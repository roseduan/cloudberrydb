#include "grammar_convert.h"
#include "utils/uri.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "catalog/dependency.h"
#include "catalog/pg_proc.h"
#include "commands/extension.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/reloptions.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "src/provider/common/config.h"

#define fmttype_is_text(c)   (c == 't')
#define fmttype_is_csv(c)    (c == 'c')
#define fmttype_is_custom(c) (c == 'b')

#define CUSTOM_PROTOCOL_GPHDFS "gphdfs"
#define CUSTOM_PROTOCOL_OSS "oss"
#define CUSTOM_PROTOCOL_DATALAKE "datalake"
#define CUSTOM_PROTOCOL_FTP "ftp"

#define IS_HDFS_PROTOCOL(uri) (pg_strncasecmp(uri, CUSTOM_PROTOCOL_GPHDFS, strlen(CUSTOM_PROTOCOL_GPHDFS)) == 0)
#define IS_OSS_PROTOCOL(uri) (pg_strncasecmp(uri, CUSTOM_PROTOCOL_OSS, strlen(CUSTOM_PROTOCOL_OSS)) == 0)
#define IS_DATALAKE_PROTOCOL(uri) (pg_strncasecmp(uri, CUSTOM_PROTOCOL_DATALAKE, strlen(CUSTOM_PROTOCOL_DATALAKE)) == 0)
#define IS_FTP_PROTOCOL(uri) (pg_strncasecmp(uri, CUSTOM_PROTOCOL_FTP, strlen(CUSTOM_PROTOCOL_FTP)) == 0)

static bool
need_convert(char *customprotocol)
{
	return IS_HDFS_PROTOCOL(customprotocol) ||
		   IS_OSS_PROTOCOL(customprotocol) ||
		   IS_DATALAKE_PROTOCOL(customprotocol);
}

// eg.
// gphdfs://ci-test-data/orc/more_file hdfs_cluster_name=paa_cluster
//                   |
//                   |
//                   |
//                   V
// gphdfs://ci-test-data/orc/more_file
static char*
get_url_with_path(const char *url)
{
	const char *delimiter = " ";
	char *options;
	int url_len;
	char *conf_url;

	options = strstr(url, delimiter);
	url_len = strlen(url);
	if (options)
		url_len = strlen(url) - strlen(options);
	conf_url = (char *) palloc(url_len + 1);
	memcpy(conf_url, url, url_len);
	conf_url[url_len] = 0;

	return conf_url;
}

static void *safePallocBuff(size_t size) {
	void *retval = NULL;

	PG_TRY();
	{
		retval = palloc0(size + 1);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	if (retval == NULL) {
		elog(ERROR, "cannot palloc memory");
	}

	return retval;
}

static char* filterString(const char* str) {
	char* out_str = (char*)safePallocBuff(strlen(str));
	int pos = 0;
	for (int i = 0; i < strlen(str); i++) {
		if (str[i] == '\n' || str[i] == '\r') {
			continue;
		} else {
			memcpy(out_str + pos, str+i, 1);
			pos+=1;
		}
	}
	return out_str;
}

static char *get_opt_oss(const char *options, const char *key) {
	const char *key_f = NULL;
	const char *key_tailing = NULL;
	const char *delimiter = " ";
	char *key_val = NULL;
	int val_len = 0;

	if (!options || !key)
		return NULL;

	char *key2search = (char *) palloc(strlen(key) + 3);
	int key_len = strlen(key);

	key2search[0] = ' ';
	memcpy(key2search + 1, key, key_len);
	key2search[key_len + 1] = '=';
	key2search[key_len + 2] = 0;

	key_f = strstr(options, key2search);
	if (key_f == NULL)
		goto FAIL;

	key_f += strlen(key2search);
	if (*key_f == ' ')
		goto FAIL;

	key_tailing = strstr(key_f, delimiter);
	val_len = 0;
	if (key_tailing)
		val_len = strlen(key_f) - strlen(key_tailing);
	else
		val_len = strlen(key_f);

	key_val = (char *) palloc(val_len + 1);

	memcpy(key_val, key_f, val_len);
	key_val[val_len] = 0;

	pfree(key2search);

	return key_val;

FAIL:
	pfree(key2search);
	return NULL;
}

/*
 * Identity of the wrapper behind gphdfs:// / oss:// / ftp:// / datalake://.
 *
 * Both halves are needed.  The extension that ships our handler also ships the
 * Iceberg catalog and volume handlers, so membership alone would accept a volume
 * server -- exactly the binding this filter exists to reject.  The C symbol
 * alone would accept an unrelated library that exports the same symbol name,
 * since a symbol only resolves within its own library.
 */
#define DATALAKE_EXTENSION_NAME "datalake_fdw"
#define DATALAKE_FDW_HANDLER_SYMBOL "datalake_fdw_handler"

/* Server options whose value names the storage address. */
#define OPTION_NAMES_ADDRESS(name) \
	(pg_strcasecmp((name), "host") == 0 || \
	 pg_strcasecmp((name), "hdfs_namenodes") == 0)

/* Result of one pass over pg_foreign_server; see find_foreign_server(). */
typedef struct ServerLookup
{
	char *serverName;			/* chosen server, NULL when none matched */
	int nmatched;				/* matching servers backed by our handler */
	int nother;					/* matching servers backed by another wrapper */
} ServerLookup;

/*
 * server_uses_datalake_handler
 *		Does this server belong to a wrapper implemented by datalake_fdw?
 *
 * Neither name in sight is usable as the key.  The wrapper name is out because
 * datalake_fdw--1.0.sql creates only the handler and validator functions, never
 * the wrapper object: each installation creates the wrapper by hand (see
 * automation/lib/sql/common_setup.sql) and may call it anything, or rename it
 * later.  The handler's SQL name is out because ALTER FUNCTION ... RENAME
 * changes it while the OID the wrapper stores stays valid.  pg_proc.probin is
 * out because this extension is relocatable and its own handlers already
 * disagree on the spelling ($libdir/datalake_fdw versus $libdir/datalake_fdw.so).
 *
 * What is left is the pair (owning extension, C symbol), and both halves carry
 * their weight: our extension also ships iceberg_catalog_fdw_handler and
 * iceberg_volume_fdw_handler, so membership alone would accept a volume server
 * -- the very binding this filter exists to reject -- while the symbol alone
 * would accept an unrelated library that exports the same symbol name, since a
 * symbol only resolves within its own library.
 */
static bool
server_uses_datalake_handler(Oid srvfdw, Oid extensionId)
{
	HeapTuple	wraptup;
	Oid			fdwhandler;
	HeapTuple	proctup;
	Datum		prosrc;
	bool		isnull;
	bool		matches;

	if (!OidIsValid(extensionId))
		return false;		/* extension not installed here */

	/*
	 * Read the wrapper straight from the syscache rather than through
	 * GetForeignDataWrapper(), which raises on a missing row.  This runs
	 * inside an open scan of pg_foreign_server, and the row we are looking at
	 * comes from our snapshot: a concurrent DROP ... CASCADE can have removed
	 * the wrapper already.  Treating that as "not our handler" keeps the
	 * caller's scan and lock cleanup on the normal path, and the server still
	 * gets counted as another wrapper's, which is what it now is.
	 */
	wraptup = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(srvfdw));
	if (!HeapTupleIsValid(wraptup))
		return false;

	fdwhandler = ((Form_pg_foreign_data_wrapper) GETSTRUCT(wraptup))->fdwhandler;
	ReleaseSysCache(wraptup);

	/* A wrapper without a handler cannot serve a scan at all. */
	if (!OidIsValid(fdwhandler))
		return false;

	if (getExtensionOfObject(ProcedureRelationId, fdwhandler) != extensionId)
		return false;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fdwhandler));
	if (!HeapTupleIsValid(proctup))
		return false;		/* handler dropped from under the wrapper */

	prosrc = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		matches = false;
	else
	{
		char	   *symbol = TextDatumGetCString(prosrc);

		matches = strcmp(symbol, DATALAKE_FDW_HANDLER_SYMBOL) == 0;
		pfree(symbol);
	}

	ReleaseSysCache(proctup);

	return matches;
}

/*
 * find_foreign_server
 *		Resolve the foreign server that backs an external table at "host".
 *
 * The external table grammar has no SERVER clause, so the server has to be
 * inferred from the address.  An option name and value do NOT identify a server
 * on their own, though, so the wrapper has to be part of the search key.
 *
 * Since 2.7 an iceberg_volume_fdw server also carries hdfs_namenodes: the volume
 * options were deliberately given the same names as the datalake_fdw HDFS SERVER
 * options and the gphdfs.conf keys, so that one vocabulary covers all three.
 * Matching on the option alone therefore binds a gphdfs:// external table to an
 * iceberg_volume_fdw server, and since that wrapper expects scan state prepared
 * by the Iceberg access method, the standard ForeignScan path later reaches
 * icebergVolumeBeginForeignScan() with a NULL fdw_state and crashes the backend.
 *
 * The plain "host" option carries the same hazard: postgres_fdw and
 * cloudberry_fdw servers accept it as a libpq option.  Filtering by wrapper here
 * is the only durable fix -- the shared vocabulary is intentional, and the option
 * names of other wrappers are not ours to constrain.
 *
 * One pass fills all of "result": the chosen server, how many of our own servers
 * matched (for the ambiguity warning) and how many servers of other wrappers
 * matched (so the caller's "no server" error can explain itself).
 */
static void
find_foreign_server(const char *host, ServerLookup *result)
{
	Relation	rel;
	HeapTuple	tuple;
	SysScanDesc scan;
	StringInfoData matched;
	Datum		datum;
	bool		isnull;
	ListCell	*lc;
	Oid			extensionId;

	result->serverName = NULL;
	result->nmatched = 0;
	result->nother = 0;

	initStringInfo(&matched);

	/*
	 * Invariant for the whole scan, so look it up once.  An invalid OID (the
	 * extension is not installed here) is not a reason to skip the scan: the
	 * servers still have to be counted in result->nother, which is what lets
	 * the caller's "no server" error say that something matched the address
	 * but belonged to another wrapper.
	 */
	extensionId = get_extension_oid(DATALAKE_EXTENSION_NAME, true);

	rel = table_open(ForeignServerRelationId, AccessShareLock);

	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_foreign_server server = (Form_pg_foreign_server) GETSTRUCT(tuple);
		List	   *options;
		bool		addressMatches = false;

		datum = heap_getattr(tuple, Anum_pg_foreign_server_srvoptions, RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		options = untransformRelOptions(datum);
		foreach(lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (OPTION_NAMES_ADDRESS(def->defname) &&
				pg_strcasecmp(defGetString(def), host) == 0)
			{
				addressMatches = true;
				break;
			}
		}

		if (!addressMatches)
			continue;

		if (!server_uses_datalake_handler(server->srvfdw, extensionId))
		{
			result->nother++;
			continue;
		}

		result->nmatched++;
		if (result->nmatched > 1)
			appendStringInfoString(&matched, ", ");
		appendStringInfo(&matched, "\"%s\"", NameStr(server->srvname));

		/*
		 * Keep the last match, which is what this function has always
		 * effectively returned: the original loop broke out of the option list
		 * only, so every later match overwrote the previous one.  The choice is
		 * arbitrary either way -- both ends of an unordered heap scan move when
		 * the catalog is rewritten -- so preserve the existing answer rather
		 * than silently rebinding tables on upgrade, and warn instead.
		 */
		if (result->serverName != NULL)
			pfree(result->serverName);
		result->serverName = pstrdup(NameStr(server->srvname));
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	/*
	 * Several of our servers describe the same address.  They can still differ
	 * in everything else -- auth method, Kerberos principal, the user mapping
	 * that supplies the credentials -- so binding to the wrong one reads the
	 * data as the wrong identity.  The grammar gives the user no way to
	 * disambiguate, so warn rather than fail, and point at the foreign table
	 * syntax that can name a server.
	 */
	if (result->nmatched > 1)
		ereport(WARNING,
				(errmsg("address \"%s\" matches %d foreign servers: %s",
						host, result->nmatched, matched.data),
				 errdetail("Server \"%s\" was selected, but the choice depends on catalog "
						   "storage order and may change after maintenance such as VACUUM FULL.",
						   result->serverName),
				 errhint("Use CREATE FOREIGN TABLE ... SERVER to name the server explicitly, "
						 "or configure a single server per address.")));

	pfree(matched.data);
}

static void
parse_options(char *url, char *old_name, char *new_name, List **foreignOptions)
{
	char *val = get_opt_oss(url, old_name);
	if (val)
	{
		*foreignOptions = lappend(*foreignOptions, makeDefElem(new_name, (Node *) makeString(val), -1));
	}
}

static char
transformFormatType(char *formatname)
{
	char		result = '\0';

	if (pg_strcasecmp(formatname, "text") == 0)
		result = 't';
	else if (pg_strcasecmp(formatname, "csv") == 0)
		result = 'c';
	else if (pg_strcasecmp(formatname, "custom") == 0)
		result = 'b';
	else if(pg_strcasecmp(formatname, "orc") == 0)
	    result = 'o';
    else if (pg_strcasecmp(formatname, "parquet") == 0)
        result = 'p';
	else if(pg_strcasecmp(formatname, "iceberg") == 0)
		result = 'i';
	else if(pg_strcasecmp(formatname, "hudi") == 0)
		result = 'h';
	else if (pg_strcasecmp(formatname, "avro") == 0)
		result = 'a';
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unsupported format '%s'", formatname),
				 errhint("Available formats for external tables are \"text\", \"csv\" and \"custom\".")));

	return result;
}

static char *
list_join(List *list, char delimiter)
{
	ListCell	   *cell;
	StringInfoData	buf;

	if (list_length(list) == 0)
		return pstrdup("");

	initStringInfo(&buf);

	foreach(cell, list)
	{
		const char *cellval;

		cellval = strVal(lfirst(cell));
		appendStringInfo(&buf, "%s%c", quote_identifier(cellval), delimiter);
	}

	/*
	 * Rather than keeping track of when we're adding the last element, trim
	 * the final delimiter to keep it simple.
	 */
	buf.data[buf.len - 1] = '\0';

	return buf.data;
}

// transform csv/text options into foreign table options
// almost copy from
// src/backend/commands/exttablecmds.c:transformFormatOpts
static List *
transformFormatOpts(char formattype, List *formatOpts, int numcols, bool iswritable)
{
	List 	   *cslist = NIL;
	ListCell   *option;
	ParseState *pstate;

	CopyFormatOptions opts;
	memset(&opts, 0, sizeof(opts));

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = NULL;

	Assert(fmttype_is_text(formattype) ||
		   fmttype_is_csv(formattype) ||
		   fmttype_is_custom(formattype));

	/* Extract options from the statement node tree */
	if (fmttype_is_text(formattype) || fmttype_is_csv(formattype))
	{
		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);

			if (strcmp(defel->defname, "delimiter") == 0 ||
				strcmp(defel->defname, "null") == 0 ||
				strcmp(defel->defname, "header") == 0 ||
				strcmp(defel->defname, "quote") == 0 ||
				strcmp(defel->defname, "escape") == 0 ||
				strcmp(defel->defname, "force_not_null") == 0 ||
				strcmp(defel->defname, "force_quote") == 0 ||
				strcmp(defel->defname, "fill_missing_fields") == 0 ||
				strcmp(defel->defname, "newline") == 0)
			{
				/* ok */
			}
			else if (strcmp(defel->defname, "formatter") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("formatter option only valid for custom formatters")));
			}
			else
				elog(ERROR, "option \"%s\" not recognized",
					 defel->defname);
		}

		/* If CSV format was chosen, make it visible to ProcessCopyOptions. */
		if (fmttype_is_csv(formattype))
		{
			formatOpts = list_copy(formatOpts);
			formatOpts = lappend(formatOpts, makeDefElem("format", (Node *) makeString("csv"), -1));

			cslist = lappend(cslist, makeDefElem("format", (Node *) makeString("csv"), -1));
		}
		else
			cslist = lappend(cslist, makeDefElem("format", (Node *) makeString("text"), -1));

		/* verify all user supplied control char combinations are legal */
		ProcessCopyOptions(pstate,
						   &opts,
						   !iswritable, /* is_from */
						   formatOpts,
						   InvalidOid);

		if (opts.delim_off)
		{
			if (numcols != 1)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("using no delimiter is only possible for a single column table")));
		}

		if (opts.header_line)
		{
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				if (!iswritable)
				{
					/* RET */
					ereport(NOTICE,
							(errmsg("HEADER means that each one of the data files has a header row")));
				}
				else
				{
					/* WET */
					ereport(ERROR,
							(errcode(ERRCODE_GP_FEATURE_NOT_YET),
							errmsg("HEADER is not yet supported for writable external tables")));
				}
			}
		}

		/* keep the same order with the original pg_exttable catalog's fmtopt field */
		cslist = lappend(cslist, makeDefElem("delimiter", (Node *) makeString(opts.delim), -1));
		cslist = lappend(cslist, makeDefElem("null", (Node *) makeString(opts.null_print), -1));
		cslist = lappend(cslist, makeDefElem("escape", (Node *) makeString(opts.escape), -1));
		if (fmttype_is_csv(formattype))
			cslist = lappend(cslist, makeDefElem("quote", (Node *) makeString(opts.quote), -1));
		if (opts.header_line)
			cslist = lappend(cslist, makeDefElem("header", (Node *) makeString("true"), -1));
		if (opts.fill_missing)
			cslist = lappend(cslist, makeDefElem("fill_missing_fields", (Node *) makeString("true"), -1));

		/* Re-construct the FORCE NOT NULL list string */
		if (opts.force_notnull)
			cslist = lappend(cslist, makeDefElem("force_not_null", (Node *) makeString(list_join(opts.force_notnull, ',')), -1));

		/* Re-construct the FORCE QUOTE list string */
		if (opts.force_quote)
			cslist = lappend(cslist, makeDefElem("force_quote", (Node *) makeString(list_join(opts.force_quote, ',')), -1));
		else if (opts.force_quote_all)
			cslist = lappend(cslist, makeDefElem("force_quote", (Node *) makeString("*"), -1));

		if (opts.eol_str)
			cslist = lappend(cslist, makeDefElem("newline", (Node *) makeString(opts.eol_str), -1));
	}
	else
	{
		bool		found = false;
		foreach(option, formatOpts)
		{
			DefElem    *defel = (DefElem *) lfirst(option);

			if (strcmp(defel->defname, "formatter") == 0)
			{
				if (found)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("redundant formatter option")));

				found = true;
			}
		}
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("no formatter function specified")));

		cslist = list_copy(formatOpts);
	}

	return cslist;
}

CreateForeignTableStmt*
ConvertExternalTableStmt(CreateExternalStmt *createExtStmt)
{
	CreateForeignTableStmt	*foreignStmt = NULL;
	CreateStmt				*createStmt;
	Oid						userid = GetUserId();
	ExtTableTypeDesc		*exttypeDesc;
	char 					logerrors = LOG_ERRORS_DISABLE;
	bool					log_persistent_option = false;
	bool 					iswritable	= createExtStmt->iswritable;
	SingleRowErrorDesc		*singlerowerrorDesc = NULL;
	int						rejectlimit = -1;
	char					rejectlimittype = '\0';
	int						encoding = -1;
	DefElem					*dencoding = NULL;
	ListCell				*option;

	exttypeDesc = (ExtTableTypeDesc *) createExtStmt->exttypedesc;
	if (exttypeDesc->exttabletype == EXTTBL_TYPE_LOCATION)
	{
		Uri		*uri;
		char	*uri_str = strVal(linitial(exttypeDesc->location_list));
		uri = ParseExternalTableUri(uri_str);
		if (uri->protocol == URI_CUSTOM || uri->protocol == URI_FTP)
		{
			/*
			 * Only convert three custom protocols in hashdata 3X: gphdfs, oss, ftp and datalake.
			 */
			if (uri->protocol != URI_FTP && !need_convert(uri->customprotocol))
			{
				return NULL;
			}

			/* FIXME: do oss protocol convert in the next version */
			if (uri->customprotocol && IS_OSS_PROTOCOL(uri->customprotocol))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("oss protocol is not supported")));
			}

			/* Convert the external table to a foreign table */
			foreignStmt = makeNode(CreateForeignTableStmt);

			/* base stmt */
			createStmt = &foreignStmt->base;
			createStmt->relation = createExtStmt->relation;
			createStmt->tableElts = createExtStmt->tableElts;
			createStmt->inhRelations = NIL;
			createStmt->constraints = NIL;
			createStmt->oncommit = ONCOMMIT_NOOP;
			createStmt->tablespacename = NULL;
			createStmt->distributedBy = createExtStmt->distributedBy; /* policy was set in transform */
			createStmt->ownerid = userid;
			createStmt->tags = createExtStmt->tags;

			/*
			 * get necessary configurations according to the original hashdata 3X,
			 * see https://code.hashdata.xyz/hashdata/database/hashdata/-/blob/v3.13.x/gpcontrib/gpossext/src/ossConf.c
			 */
			char* url = filterString(uri_str);
			List *foreignOptions = NIL;
			char *foreignServerName = NULL;
			/*
			 * Address that was looked up, kept so the "no server" error can name
			 * it, together with the lookup counters behind that diagnosis.
			 */
			const char *searchedAddress = NULL;
			ServerLookup serverLookup = {NULL, 0, 0};

			// 1. get filePath
			char *url_with_path = get_url_with_path(url);
			char *filePath = NULL;
			if (IS_FTP_URI(uri_str))
			{
				// eg. ftp://ftp.example.com/pub/README --> /pub/README
				char *ftp_prefix = strstr(url_with_path, "ftp://");
				ftp_prefix += strlen("ftp://");
				filePath = strstr(ftp_prefix, "/");
				if (!filePath)
					elog(ERROR, "invalid oss path");

				// get ftp host
				char *ftp_host = (char *) palloc(filePath - ftp_prefix + 1);
				memcpy(ftp_host, ftp_prefix, filePath - ftp_prefix);
				ftp_host[filePath - ftp_prefix] = '\0';

				// get ftp server name
				searchedAddress = ftp_host;
				find_foreign_server(ftp_host, &serverLookup);
				foreignServerName = serverLookup.serverName;
				if (!foreignServerName)
					return NULL;
			}
			else if (IS_HDFS_PROTOCOL(uri->customprotocol))
			{
				// eg. gphdfs://ci-test-data/orc/more_file --> /ci-test-data/orc/more_file
				filePath = strstr(url_with_path, "gphdfs://");
				filePath += (strlen("gphdfs://") - 1);
			}
			else if (IS_OSS_PROTOCOL(uri->customprotocol))
			{
				// eg. oss://hashdata-test-public.pek3a.qingstor.com/icg/read/badformatted --> icg/read/badformatted
				char *oss_prefix = strstr(url_with_path, "oss://");
				oss_prefix += strlen("oss://");
				filePath = strstr(oss_prefix, "/");
				if (!filePath)
					elog(ERROR, "invalid oss path");

				// get oss host
				char *oss_host = (char *) palloc(filePath - oss_prefix + 1);
				memcpy(oss_host, oss_prefix, filePath - oss_prefix);
				oss_host[filePath - oss_prefix] = '\0';

				// get oss server name
				/*
				 * Unreachable today: the oss:// branch is rejected earlier with
				 * "oss protocol is not supported".  Kept in step with the other
				 * protocols so the conversion is correct once that FIXME is
				 * lifted, rather than reintroducing the wrong-wrapper binding.
				 */
				searchedAddress = oss_host;
				find_foreign_server(oss_host, &serverLookup);
				foreignServerName = serverLookup.serverName;
			}
			else if (IS_DATALAKE_PROTOCOL(uri->customprotocol))
			{
				filePath = strstr(url_with_path, "datalake://");
				filePath += strlen("datalake://");
				if (!filePath)
					elog(ERROR, "invalid datalake path");

				// datalake catalog_type
				parse_options(url, "catalog_type", "catalog_type", &foreignOptions);
				// datalake server_name
				parse_options(url, "server_name", "server_name", &foreignOptions);
				// datalake table_identifier
				parse_options(url, "table_identifier", "table_identifier", &foreignOptions);
				// datalake split_size
				parse_options(url, "split_size", "split_size", &foreignOptions);
				// datalake query_type
				parse_options(url, "query_type", "query_type", &foreignOptions);
				// datalake metadata_table_enable
				parse_options(url, "metadata_table_enable", "metadata_table_enable", &foreignOptions);
			}

			if (!filePath)
				elog(ERROR, "invalid file path");
			foreignOptions = lappend(foreignOptions, makeDefElem("filepath", (Node *) makeString(filePath), -1));
			// 2. get format
			char fmttype = transformFormatType(createExtStmt->format);
			// csv and text format will be added in transformFormatOpts
			if (!(fmttype_is_csv(fmttype) || fmttype_is_text(fmttype)))
				foreignOptions = lappend(foreignOptions, makeDefElem("format", (Node *) makeString(createExtStmt->format), -1));
			// 3. get compress
			parse_options(url, "compression", "compression", &foreignOptions);
			// 4. get file_size_limit
			parse_options(url, "file_size_limit", "filesizelimit", &foreignOptions);
			// 5. get partition_keys
			parse_options(url, "partition_keys", "partitionkeys", &foreignOptions);
			// 5.1 get partition_values
			parse_options(url, "partition_value", "partitionvalue", &foreignOptions);
			// 6. get cache
			parse_options(url, "cache", "enablecache", &foreignOptions);
			// 7. get transactional
			parse_options(url, "transactional", "transactional", &foreignOptions);
			// 7.1 get hive_cluster_name 
			parse_options(url, "hive_cluster_name", "hive_cluster_name", &foreignOptions);
			// 7.2 get datasource
			parse_options(url, "datasource", "datasource", &foreignOptions);

			// 8. parse text/csv options
			if (fmttype_is_csv(fmttype) || fmttype_is_text(fmttype) || fmttype_is_custom(fmttype))
			{
				List *formatOpts = transformFormatOpts(fmttype,
													   createExtStmt->formatOpts,
													   list_length(createExtStmt->tableElts),
													   iswritable);
				foreignOptions = list_concat(foreignOptions, formatOpts);
			}
			// 9. parse single row error handling info if available
			singlerowerrorDesc = (SingleRowErrorDesc *) createExtStmt->sreh;
			if (singlerowerrorDesc)
			{
				Assert(!iswritable);

				logerrors = singlerowerrorDesc->log_error_type;
				if (IS_LOG_ERRORS_ENABLE(logerrors) && log_persistent_option)
				{
					logerrors = LOG_ERRORS_PERSISTENTLY;
					singlerowerrorDesc->log_error_type = LOG_ERRORS_PERSISTENTLY;
				}

				/* get reject limit, and reject limit type */
				rejectlimit = singlerowerrorDesc->rejectlimit;
				rejectlimittype = (singlerowerrorDesc->is_limit_in_rows ? 'r' : 'p');
				VerifyRejectLimit(rejectlimittype, rejectlimit);

				// logerrors
				foreignOptions = lappend(foreignOptions, makeDefElem("logerrors", (Node *) makeString(psprintf("%c", logerrors)), -1));
				// rejectlimit
				foreignOptions = lappend(foreignOptions, makeDefElem("rejectlimit", (Node *) makeString(psprintf("%d", rejectlimit)), -1));
				// rejectlimittype
				char *rejectlimit_val = NULL;
				if (rejectlimittype == 'r')
					rejectlimit_val = "rows";
				else
					rejectlimit_val = "percent";
				foreignOptions = lappend(foreignOptions, makeDefElem("rejectlimittype", (Node *) makeString(rejectlimit_val), -1));
			}
			// 10. parse external table data encoding
			foreach(option, createExtStmt->encoding)
			{
				DefElem    *defel = (DefElem *) lfirst(option);

				Assert(strcmp(defel->defname, "encoding") == 0);

				if (dencoding)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("conflicting or redundant ENCODING specification")));
				dencoding = defel;
			}

			if (dencoding && dencoding->arg)
			{
				const char *encoding_name;

				if (IsA(dencoding->arg, Integer))
				{
					encoding = intVal(dencoding->arg);
					encoding_name = pg_encoding_to_char(encoding);
					if (strcmp(encoding_name, "") == 0 ||
						pg_valid_client_encoding(encoding_name) < 0)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								errmsg("%d is not a valid encoding code", encoding)));
				}
				else if (IsA(dencoding->arg, String))
				{
					encoding_name = strVal(dencoding->arg);
					if (pg_valid_client_encoding(encoding_name) < 0)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								errmsg("%s is not a valid encoding name",
										encoding_name)));
					encoding = pg_char_to_encoding(encoding_name);
				}
				else
					elog(ERROR, "unrecognized node type: %d",
						nodeTag(dencoding->arg));
			}

			/* If encoding is defaulted, use database encoding */
			if (encoding < 0)
				encoding = pg_get_client_encoding();

			const char *enc_name = get_encoding_name_for_icu(encoding);
			if (!enc_name)
			{
				/* Handle encodings not supported by ICU but valid in PostgreSQL */
				switch (encoding)
				{
					case 0:  /* PG_SQL_ASCII */
						enc_name = "US-ASCII";
						break;
					case 5:  /* PG_EUC_JIS_2004 */
						enc_name = "EUC-JP";  /* Use EUC-JP as fallback */
						break;
					case 7:  /* PG_MULE_INTERNAL */
						enc_name = "UTF-8";   /* Use UTF-8 as fallback */
						break;
					case 18: /* PG_LATIN10 */
						enc_name = "ISO-8859-16"; /* ISO-8859-16 Latin10 */
						break;
					case 21: /* PG_WIN874 */
						enc_name = "CP874";   /* Windows-874 */
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_OBJECT),
								errmsg("encoding %d is not supported", encoding)));
				}
			}
			foreignOptions = lappend(foreignOptions, makeDefElem("encoding", (Node *) makeString(psprintf("%s", enc_name)), -1));

			// find forenign server by hdfs cluster name
			if (uri->customprotocol &&
					(IS_DATALAKE_PROTOCOL(uri->customprotocol) || IS_HDFS_PROTOCOL(uri->customprotocol)))
			{
				char *hdfs_cluster_name = get_opt_oss(url, "hdfs_cluster_name");
				/*
				 * When the LOCATION does not specify a cluster name, fall back
				 * to the optional top-level "default" key in gphdfs.conf.
				 */
				if (hdfs_cluster_name == NULL)
					hdfs_cluster_name = datalakeGetDefaultHdfsCluster("gphdfs.conf");
				if (hdfs_cluster_name)
				{
					DatalakeHdfsConfigInfo *hdfs = datalakeParseHdfsConfig("gphdfs.conf", hdfs_cluster_name);
					if (hdfs && hdfs->namenodeHost)
					{
						searchedAddress = hdfs->namenodeHost;
						find_foreign_server(hdfs->namenodeHost, &serverLookup);
						foreignServerName = serverLookup.serverName;
					}
				}
			}

			/* copy extra options in CreateExternalStmt */
			foreignOptions = list_concat(foreignOptions, list_copy(createExtStmt->extOptions));
			foreignStmt->options = foreignOptions;

			if (foreignServerName == NULL)
			{
				if (searchedAddress == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("cannot find foreign server")));

				/*
				 * Name the address and say how many servers of other wrappers
				 * describe it.  A bare "cannot find foreign server" leaves the
				 * user nothing to act on, which is especially unhelpful right
				 * after an upgrade: the setup worked before iceberg_volume_fdw
				 * started carrying hdfs_namenodes, and the server they created
				 * for this cluster is still there -- just not usable here.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("no %s foreign server describes address \"%s\"",
								DATALAKE_EXTENSION_NAME, searchedAddress),
						 serverLookup.nother > 0 ?
						 errdetail_plural("%d foreign server matches this address but belongs "
										  "to a different foreign data wrapper, which cannot "
										  "back an external table.",
										  "%d foreign servers match this address but belong "
										  "to different foreign data wrappers, which cannot "
										  "back an external table.",
										  serverLookup.nother, serverLookup.nother) : 0,
						 errhint("Create a foreign server for this address on a wrapper whose "
								 "HANDLER is the %s.%s function.",
								 DATALAKE_EXTENSION_NAME, DATALAKE_FDW_HANDLER_SYMBOL)));
			}
			foreignStmt->servername = foreignServerName;

			ereport(NOTICE,
					(errcode(ERRCODE_SUCCESSFUL_COMPLETION),
					 errmsg("external table syntax will be deprecated in the future HashData version, "
					 		 "please use foreign table instead.")));
			return foreignStmt;
		}
	}
	return NULL;
}
