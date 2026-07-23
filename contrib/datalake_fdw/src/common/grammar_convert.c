#include "grammar_convert.h"
#include "utils/uri.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "catalog/pg_foreign_server.h"
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

static char*
find_foreign_server(char *host)
{
	Relation	rel;
	HeapTuple	tuple;
	SysScanDesc scan;
	char		*serverName = NULL;
	Datum		datum;
	bool		isnull;
	ListCell	*lc;

	rel = table_open(ForeignServerRelationId, AccessShareLock);

	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_foreign_server server = (Form_pg_foreign_server) GETSTRUCT(tuple);
		datum = heap_getattr(tuple, Anum_pg_foreign_server_srvoptions, RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		List *options = untransformRelOptions(datum);
		foreach(lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);
			if (pg_strcasecmp(def->defname, "host") == 0
				|| pg_strcasecmp(def->defname, "hdfs_namenodes") == 0)
			{
				char *hostName = defGetString(def);
				if (pg_strcasecmp(hostName, host) == 0)
				{
					serverName = pstrdup(NameStr(server->srvname));
					break;
				}
			}
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return serverName;
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
				foreignServerName = find_foreign_server(ftp_host);
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
				foreignServerName = find_foreign_server(oss_host);
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
						foreignServerName = find_foreign_server(hdfs->namenodeHost);
					}
				}
			}

			/* copy extra options in CreateExternalStmt */
			foreignOptions = list_concat(foreignOptions, list_copy(createExtStmt->extOptions));
			foreignStmt->options = foreignOptions;

			if (foreignServerName == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot find foreign server")));
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
