/*-------------------------------------------------------------------------
 *
 * iceberg_site_config.c
 *    Reader for the iceberg volume site configuration files, s3.conf and
 *    gphdfs.conf.
 *
 * The agent's SiteConfigLoader is the oracle this file reproduces, so that
 * the QD can resolve a volume section itself and ship the result to QEs
 * that cannot see those files.  Two of the loader's traits are easy to miss
 * and both are load-bearing here:
 *
 * 1. SnakeYAML types plain scalars before the loader sees them, and the
 *    loader then calls toString().  That layer lives in
 *    iceberg_site_scalar.c; everything below simply asks it what a scalar
 *    means.
 *
 * 2. The loader is deliberately intolerant: legacy Hadoop-style spellings
 *    are hard errors that name their replacement.  Those messages are part
 *    of the contract and are reproduced verbatim.
 *
 * The full behavioural contract, including the places where we deliberately
 * reject input the agent accepts, is .router/plans/410/W1-equivalence-spec.md.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/iceberg_site_config.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <yaml.h>

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/json.h"

#include "iceberg_site_config.h"
#include "iceberg_site_scalar.h"

#define ICEBERG_SITE_FILE_S3	"s3.conf"
#define ICEBERG_SITE_FILE_HDFS	"gphdfs.conf"

/*
 * Legacy spelling -> the SQL OPTION name that replaced it.  Order matters
 * twice over: it is the order replacements appear in the error message, and
 * for the prefix tables it is the order they are tried in.  Mirrors the
 * LinkedHashMaps in SiteConfigLoader.
 */
typedef struct SiteLegacyKey
{
	const char *key;
	const char *replacement;
} SiteLegacyKey;

static const SiteLegacyKey s3LegacyKeys[] = {
	{"fs.gopher.ufs_type", "type"},
	{"fs.s3a.endpoint", "endpoint"},
	{"fs.s3a.access.key", "access_key_id"},
	{"fs.s3a.secret.key", "secret_access_key"},
	{"fs.s3a.endpoint.region", "region"},
	{"fs.s3a.path.style.access", "path_style_access"}
};

static const SiteLegacyKey s3LegacyPrefixes[] = {
	{"fs.s3a.", "the matching SQL OPTION name"},
	{"fs.gopher.", "the matching SQL OPTION name"}
};

static const SiteLegacyKey hdfsLegacyKeys[] = {
	{"hdfs_namenode_host", "hdfs_namenodes"},
	{"hdfs_namenode_port", "hdfs_port"},
	{"dfs.nameservices", "dfs_nameservices"},
	{"dfs.ha.namenodes", "dfs_ha_namenodes"},
	{"dfs.namenode.rpc-address", "dfs_namenode_rpc_address"}
};

static const SiteLegacyKey hdfsLegacyPrefixes[] = {
	{"dfs.client.failover.proxy.provider", "dfs_client_failover_proxy_provider"},
	{"dfs.", "the underscore spelling, e.g. \"dfs_nameservices\""}
};

static const char *const s3TypedKeys[] = {
	"type", "endpoint", "region", "bucket_name", "path_style_access",
	"username", "access_key_id", "secret_access_key"
};

/*
 * Note the absence of data_transfer_protection: the agent reads it into a
 * typed field but leaves it out of isHdfsTypedKey(), so it also lands in
 * extraProperties.  That duplication is reproduced, not fixed -- fixing it
 * on one side only would make the two sides disagree.
 */
static const char *const hdfsTypedKeys[] = {
	"hdfs_namenodes", "hdfs_port", "hdfs_auth_method", "krb_principal",
	"krb_principal_keytab", "hadoop_rpc_protection", "data_transfer_protocol",
	"is_ha_supported", "dfs_nameservices", "dfs_ha_namenodes",
	"dfs_namenode_rpc_address", "dfs_client_failover_proxy_provider",
	"dfs_client_use_datanode_hostname", "username"
};

/* One key/value pair of the matched section, in document order. */
typedef struct SiteEntry
{
	char	   *key;
	IcebergSiteScalarTag tag;
	char	   *value;			/* NULL iff tag is NULL */
	const char *unsupportedKind;
} SiteEntry;


static bool
stringInList(const char *s, const char *const *list, int nlist)
{
	int			i;

	for (i = 0; i < nlist; i++)
	{
		if (strcmp(s, list[i]) == 0)
			return true;
	}

	return false;
}

/*
 * Resolve a scalar node the way SnakeYAML would and render it the way
 * asString() would.  Quoting suppresses resolution entirely, which is why
 * "yes" and "\"yes\"" mean different things.  Never raises: the caller
 * decides what an unsupported resolution means in its position, because a
 * section name may legitimately be one while a value may not.
 */
static SiteEntry
resolveScalarNode(const yaml_node_t *node)
{
	SiteEntry	out;
	char	   *text = pnstrdup((const char *) node->data.scalar.value,
								node->data.scalar.length);
	size_t		bufsz = ICEBERG_SITE_SCALAR_BUFSZ(strlen(text));
	char	   *buf = palloc(bufsz);

	out.key = NULL;
	out.value = text;
	out.unsupportedKind = NULL;

	if (node->data.scalar.style != YAML_PLAIN_SCALAR_STYLE)
	{
		out.tag = ICEBERG_SITE_SCALAR_STR;
		pfree(buf);
		return out;
	}

	out.tag = icebergSiteResolveScalar(text, buf, bufsz,
									   &out.unsupportedKind);

	switch (out.tag)
	{
		case ICEBERG_SITE_SCALAR_NULL:
		case ICEBERG_SITE_SCALAR_UNSUPPORTED:
			out.value = NULL;
			pfree(buf);
			break;
		case ICEBERG_SITE_SCALAR_BOOL:
		case ICEBERG_SITE_SCALAR_INT:
			out.value = buf;
			break;
		case ICEBERG_SITE_SCALAR_STR:
			pfree(buf);
			break;
	}

	return out;
}

/*
 * Collect the section's pairs in document order.  A repeated key follows
 * LinkedHashMap.put(): the last value wins but keeps the first occurrence's
 * position, which is what the legacy-key error and the extras list expose.
 */
static List *
readSectionEntries(yaml_document_t *document, yaml_node_t *section,
				   const char *configFile, const char *serverName)
{
	List	   *entries = NIL;
	yaml_node_pair_t *pair;

	for (pair = section->data.mapping.pairs.start;
		 pair < section->data.mapping.pairs.top;
		 pair++)
	{
		yaml_node_t *keyNode = yaml_document_get_node(document, pair->key);
		yaml_node_t *valueNode = yaml_document_get_node(document, pair->value);
		SiteEntry	keyScalar;
		SiteEntry	valueScalar;
		SiteEntry  *entry = NULL;
		ListCell   *lc;

		/*
		 * The agent walks the section's keys as Strings, so a key YAML
		 * resolves to anything else -- "on:", "8020:", "~:" -- makes it
		 * throw before it reads a single value.
		 */
		if (keyNode == NULL || keyNode->type != YAML_SCALAR_NODE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file %s, server \"%s\": section keys must be strings",
							configFile, serverName)));

		keyScalar = resolveScalarNode(keyNode);

		if (keyScalar.tag != ICEBERG_SITE_SCALAR_STR)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file %s, server \"%s\": section keys must be strings",
							configFile, serverName),
					 errhint("Quote the key to keep it a string.")));

		if (valueNode == NULL || valueNode->type != YAML_SCALAR_NODE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file %s, server \"%s\": value of key \"%s\" must be a scalar",
							configFile, serverName, keyScalar.value)));

		valueScalar = resolveScalarNode(valueNode);

		if (valueScalar.tag == ICEBERG_SITE_SCALAR_UNSUPPORTED)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file %s, server \"%s\": value of key \"%s\" is a %s",
							configFile, serverName, keyScalar.value,
							valueScalar.unsupportedKind),
					 errhint("Quote the value so it is read as text.")));

		foreach(lc, entries)
		{
			SiteEntry  *existing = (SiteEntry *) lfirst(lc);

			if (strcmp(existing->key, keyScalar.value) == 0)
			{
				entry = existing;
				break;
			}
		}

		if (entry == NULL)
		{
			entry = (SiteEntry *) palloc0(sizeof(SiteEntry));
			entry->key = keyScalar.value;
			entries = lappend(entries, entry);
		}

		entry->tag = valueScalar.tag;
		entry->value = valueScalar.value;
	}

	return entries;
}

/*
 * Fail on every legacy spelling at once, so a file is migrated in one pass
 * rather than error by error.  The message matches the agent's byte for byte.
 */
static void
rejectLegacyKeys(List *entries, const char *configFile, const char *serverName,
				 const SiteLegacyKey *exact, int nexact,
				 const SiteLegacyKey *prefixes, int nprefixes)
{
	StringInfoData offending;
	ListCell   *lc;

	initStringInfo(&offending);

	foreach(lc, entries)
	{
		SiteEntry  *entry = (SiteEntry *) lfirst(lc);
		const char *replacement = NULL;
		int			i;

		for (i = 0; i < nexact; i++)
		{
			if (strcmp(entry->key, exact[i].key) == 0)
			{
				replacement = exact[i].replacement;
				break;
			}
		}

		if (replacement == NULL)
		{
			for (i = 0; i < nprefixes; i++)
			{
				if (strncmp(entry->key, prefixes[i].key,
							strlen(prefixes[i].key)) == 0)
				{
					replacement = prefixes[i].replacement;
					break;
				}
			}
		}

		if (replacement == NULL)
			continue;

		if (offending.len > 0)
			appendStringInfoString(&offending, ", ");

		appendStringInfo(&offending, "\"%s\"", entry->key);

		/* a replacement that reads as prose is spliced in without quotes */
		if (strncmp(replacement, "the ", 4) == 0)
			appendStringInfo(&offending, " (use %s)", replacement);
		else
			appendStringInfo(&offending, " (use \"%s\")", replacement);
	}

	if (offending.len > 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("configuration file %s, server \"%s\": legacy key(s) no longer supported: %s; keys must match the SQL OPTION names",
						configFile, serverName, offending.data)));

	pfree(offending.data);
}

/* The section's value for key, or NULL when absent or resolved to null. */
static char *
entryValue(List *entries, const char *key)
{
	ListCell   *lc;

	foreach(lc, entries)
	{
		SiteEntry  *entry = (SiteEntry *) lfirst(lc);

		if (strcmp(entry->key, key) == 0)
			return entry->value;
	}

	return NULL;
}

/*
 * parseBoolean(): a resolved Boolean is itself, anything else is compared
 * against "true" case-insensitively.  Since a resolved Boolean is rendered
 * as "true"/"false", one comparison covers both.
 */
static bool
entryBoolean(const char *value)
{
	return (pg_strcasecmp(value, "true") == 0);
}

static void
collectExtras(IcebergSiteVolumeInfo *info, List *entries,
			  const char *const *typedKeys, int ntyped)
{
	ListCell   *lc;

	foreach(lc, entries)
	{
		SiteEntry  *entry = (SiteEntry *) lfirst(lc);
		IcebergSiteExtra *extra;

		if (entry->value == NULL || stringInList(entry->key, typedKeys, ntyped))
			continue;

		extra = (IcebergSiteExtra *) palloc0(sizeof(IcebergSiteExtra));
		extra->key = pstrdup(entry->key);
		extra->value = pstrdup(entry->value);
		info->extras = lappend(info->extras, extra);
	}
}

/*
 * A namenode spliced as "host:port" wins over a separate hdfs_port, which is
 * then dropped entirely.  No port default is applied: the agent has none,
 * and inventing 8020 here would hand the QEs a port the agent would never
 * have used.  (The legacy gphdfs:// reader does default it, which is one of
 * the reasons this is a separate parser and not a flag on that one.)
 */
static void
applyHdfsNamenodes(IcebergSiteVolumeInfo *info, char *namenodes, char *port)
{
	char	   *colon;

	if (namenodes != NULL && (colon = strrchr(namenodes, ':')) != NULL)
	{
		info->hdfs_namenode_host = pnstrdup(namenodes, colon - namenodes);
		info->hdfs_namenode_port = pstrdup(colon + 1);
		return;
	}

	if (namenodes != NULL)
		info->hdfs_namenode_host = namenodes;
	if (port != NULL)
		info->hdfs_namenode_port = port;
}

char *
icebergSiteUnescapeString(const char *path)
{
	char	   *out = palloc(strlen(path) + 1);

	icebergSiteUnescape(path, out);
	return out;
}

void
icebergSiteParseBucketAndPrefix(const char *location, char **bucketName,
								char **prefix)
{
	char	   *bucket = palloc(strlen(location) + 1);
	char	   *rest = palloc(strlen(location) + 1);

	if (!icebergSiteSplitBucketAndPrefix(location, bucket, rest))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid parse bucket and prefix from location format: %s",
						location)));

	*bucketName = bucket;
	*prefix = rest;
}

IcebergSiteVolumeInfo *
icebergSiteLoadVolume(const char *baseDir, const char *serverName,
					  const char *sqlVolumeType, const char *location)
{
	IcebergSiteVolumeInfo *info;
	IcebergSiteVolumeKind kind;
	const char *configFile;
	char	   *configPath;
	FILE	   *fp;
	yaml_parser_t parser;
	yaml_document_t document;
	yaml_node_t *root;
	yaml_node_t *section = NULL;
	yaml_node_pair_t *pair;
	List	   *entries;

	/* Without a server name the agent opens no file at all */
	if (serverName == NULL || serverName[0] == '\0')
		return NULL;

	kind = (sqlVolumeType != NULL && pg_strcasecmp(sqlVolumeType, "hdfs") == 0)
		? ICEBERG_SITE_VOLUME_HDFS : ICEBERG_SITE_VOLUME_S3;
	configFile = (kind == ICEBERG_SITE_VOLUME_HDFS)
		? ICEBERG_SITE_FILE_HDFS : ICEBERG_SITE_FILE_S3;
	configPath = psprintf("%s/%s", baseDir != NULL ? baseDir : ".", configFile);

	fp = fopen(configPath, "rb");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("unable to read configuration for server \"%s\" from \"%s\": %m",
						serverName, configFile)));

	if (!yaml_parser_initialize(&parser))
	{
		fclose(fp);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not create yaml parser for \"%s\"", configFile)));
	}

	yaml_parser_set_input_file(&parser, fp);
	if (!yaml_parser_load(&parser, &document))
	{
		char	   *detail = psprintf("%s at line %lu, column %lu",
									  parser.problem != NULL ? parser.problem
									  : "parse error",
									  (unsigned long) parser.problem_mark.line + 1,
									  (unsigned long) parser.problem_mark.column + 1);

		yaml_parser_delete(&parser);
		fclose(fp);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not parse configuration file \"%s\"", configFile),
				 errdetail("%s", detail)));
	}
	fclose(fp);

	root = yaml_document_get_root_node(&document);

	/*
	 * An empty file, or one holding nothing but a null, leaves the agent
	 * with no map to look in; it logs and carries on with an empty result
	 * rather than failing.
	 */
	if (root == NULL ||
		(root->type == YAML_SCALAR_NODE &&
		 resolveScalarNode(root).tag == ICEBERG_SITE_SCALAR_NULL))
	{
		yaml_document_delete(&document);
		yaml_parser_delete(&parser);
		elog(LOG, "site config file %s is empty", configFile);
		return NULL;
	}

	if (root->type != YAML_MAPPING_NODE)
	{
		yaml_document_delete(&document);
		yaml_parser_delete(&parser);
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("configuration file %s must contain a mapping of server names",
						configFile)));
	}

	/*
	 * Section names match case-sensitively, and only against keys YAML
	 * resolves to strings -- a section headed "no:" or "8020:" is a boolean
	 * or an integer to the agent and can never be found by name.  A repeated
	 * name resolves to its last occurrence.
	 */
	for (pair = root->data.mapping.pairs.start;
		 pair < root->data.mapping.pairs.top;
		 pair++)
	{
		yaml_node_t *keyNode = yaml_document_get_node(&document, pair->key);
		SiteEntry	keyScalar;

		if (keyNode == NULL || keyNode->type != YAML_SCALAR_NODE)
			continue;

		keyScalar = resolveScalarNode(keyNode);
		if (keyScalar.tag != ICEBERG_SITE_SCALAR_STR)
			continue;

		if (strcmp(keyScalar.value, serverName) == 0)
			section = yaml_document_get_node(&document, pair->value);
	}

	PG_TRY();
	{
		if (section == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("server \"%s\" not found in %s",
							serverName, configFile)));

		if (section->type != YAML_MAPPING_NODE)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file %s, server \"%s\": section must be a mapping",
							configFile, serverName)));

		entries = readSectionEntries(&document, section, configFile, serverName);

		if (kind == ICEBERG_SITE_VOLUME_HDFS)
			rejectLegacyKeys(entries, configFile, serverName,
							 hdfsLegacyKeys, lengthof(hdfsLegacyKeys),
							 hdfsLegacyPrefixes, lengthof(hdfsLegacyPrefixes));
		else
			rejectLegacyKeys(entries, configFile, serverName,
							 s3LegacyKeys, lengthof(s3LegacyKeys),
							 s3LegacyPrefixes, lengthof(s3LegacyPrefixes));
	}
	PG_CATCH();
	{
		yaml_document_delete(&document);
		yaml_parser_delete(&parser);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Every value in entries is a copy, so the libyaml allocations -- which
	 * are malloc'd and would outlive a context reset -- can go now, before
	 * anything else has a chance to raise.
	 */
	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	info = (IcebergSiteVolumeInfo *) palloc0(sizeof(IcebergSiteVolumeInfo));
	info->kind = kind;
	info->server_name = pstrdup(serverName);

	if (kind == ICEBERG_SITE_VOLUME_HDFS)
	{
		char	   *haFlag;
		char	   *useHostname;

		info->volume_server_type = pstrdup("hdfs");

		applyHdfsNamenodes(info,
						   entryValue(entries, "hdfs_namenodes"),
						   entryValue(entries, "hdfs_port"));

		info->hdfs_auth_method = entryValue(entries, "hdfs_auth_method");
		info->krb_principal = entryValue(entries, "krb_principal");
		info->krb_principal_keytab = entryValue(entries, "krb_principal_keytab");
		info->hadoop_rpc_protection = entryValue(entries, "hadoop_rpc_protection");
		info->data_transfer_protocol = entryValue(entries, "data_transfer_protocol");
		info->data_transfer_protection = entryValue(entries, "data_transfer_protection");
		info->username = entryValue(entries, "username");

		haFlag = entryValue(entries, "is_ha_supported");
		if (haFlag != NULL)
		{
			info->is_ha_supported = entryBoolean(haFlag);
			info->is_ha_supported_set = true;
		}

		info->dfs_nameservices = entryValue(entries, "dfs_nameservices");
		info->dfs_ha_namenodes = entryValue(entries, "dfs_ha_namenodes");
		info->dfs_namenode_rpc_address = entryValue(entries, "dfs_namenode_rpc_address");
		info->dfs_client_failover_proxy_provider =
			entryValue(entries, "dfs_client_failover_proxy_provider");

		useHostname = entryValue(entries, "dfs_client_use_datanode_hostname");
		if (useHostname != NULL)
		{
			info->dfs_client_use_datanode_hostname = entryBoolean(useHostname);
			info->dfs_client_use_datanode_hostname_set = true;
		}

		collectExtras(info, entries, hdfsTypedKeys, lengthof(hdfsTypedKeys));
	}
	else
	{
		char	   *pathStyle;

		info->volume_server_type = entryValue(entries, "type");
		info->endpoint = entryValue(entries, "endpoint");
		info->access_key_id = entryValue(entries, "access_key_id");
		info->secret_access_key = entryValue(entries, "secret_access_key");
		info->region = entryValue(entries, "region");
		info->username = entryValue(entries, "username");

		pathStyle = entryValue(entries, "path_style_access");
		if (pathStyle != NULL)
		{
			info->path_style_access = entryBoolean(pathStyle);
			info->path_style_access_set = true;
		}

		info->bucket_name = entryValue(entries, "bucket_name");

		/*
		 * Dead in the iceberg path -- the request carries no location, so
		 * the agent never reaches this either.  Production callers pass
		 * NULL; anything else would invent a bucket the agent never had.
		 */
		if (info->bucket_name == NULL && location != NULL && location[0] != '\0')
		{
			char	   *bucket;
			char	   *bucketPrefix;

			icebergSiteParseBucketAndPrefix(icebergSiteUnescapeString(location),
											&bucket, &bucketPrefix);
			info->bucket_name = bucket;
		}

		collectExtras(info, entries, s3TypedKeys, lengthof(s3TypedKeys));
	}

	return info;
}


/*-------------------------------------------------------------------------
 * Differential-test surface
 *
 * pg_iceberg_site_config() renders what this file parsed as canonical JSON,
 * so it can be compared field by field against the same section read by the
 * agent's SiteConfigLoader.  The field order below is VolumeInfo's own
 * declaration order, which keeps the two dumpers auditable side by side;
 * extras are sorted by key because the agent holds them in a HashMap and
 * their order carries no information.
 *
 * Superuser-only: base_dir would otherwise let any user point the backend
 * at an arbitrary YAML file and read parts of it back.
 *-------------------------------------------------------------------------
 */

static int
extraKeyCompare(const void *a, const void *b)
{
	const IcebergSiteExtra *ea = *(const IcebergSiteExtra *const *) a;
	const IcebergSiteExtra *eb = *(const IcebergSiteExtra *const *) b;

	return strcmp(ea->key, eb->key);
}

static void
appendJsonString(StringInfo buf, const char *name, const char *value)
{
	appendStringInfo(buf, "\"%s\":", name);
	if (value == NULL)
		appendStringInfoString(buf, "null");
	else
		escape_json(buf, value);
	appendStringInfoChar(buf, ',');
}

static void
appendJsonBool(StringInfo buf, const char *name, bool value, bool isSet)
{
	appendStringInfo(buf, "\"%s\":%s,", name,
					 !isSet ? "null" : (value ? "true" : "false"));
}

PG_FUNCTION_INFO_V1(pg_iceberg_site_config);

Datum
pg_iceberg_site_config(PG_FUNCTION_ARGS)
{
	char	   *serverName;
	char	   *volumeType;
	char	   *baseDir = NULL;
	IcebergSiteVolumeInfo *info;
	StringInfoData buf;
	IcebergSiteExtra **sorted;
	int			nextras;
	int			i;
	ListCell   *lc;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to read site configuration files")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	volumeType = text_to_cstring(PG_GETARG_TEXT_PP(1));
	if (!PG_ARGISNULL(2))
		baseDir = text_to_cstring(PG_GETARG_TEXT_PP(2));

	info = icebergSiteLoadVolume(baseDir, serverName, volumeType, NULL);

	/*
	 * "Nothing came from the file" is an all-null result on the agent side --
	 * loadS3Site() hands back an empty VolumeInfo rather than null -- so it
	 * has to render as one here too, or the two dumps would differ over a
	 * case where the two parsers actually agree.
	 */
	if (info == NULL)
		info = (IcebergSiteVolumeInfo *) palloc0(sizeof(IcebergSiteVolumeInfo));

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');

	appendJsonString(&buf, "volumeServerType", info->volume_server_type);
	appendJsonString(&buf, "serverName", info->server_name);
	appendJsonString(&buf, "volumeEndpoint", info->endpoint);
	appendJsonString(&buf, "volumeRegion", info->region);
	appendJsonString(&buf, "bucketName", info->bucket_name);
	appendJsonBool(&buf, "pathStyleAccess", info->path_style_access,
				   info->path_style_access_set);
	appendJsonString(&buf, "accessKeyId", info->access_key_id);
	appendJsonString(&buf, "secretAccessKey", info->secret_access_key);
	appendJsonString(&buf, "username", info->username);
	appendJsonString(&buf, "hdfsNamenodeHost", info->hdfs_namenode_host);
	appendJsonString(&buf, "hdfsNamenodePort", info->hdfs_namenode_port);
	appendJsonBool(&buf, "isHaSupported", info->is_ha_supported,
				   info->is_ha_supported_set);
	appendJsonString(&buf, "dfsNameservices", info->dfs_nameservices);
	appendJsonString(&buf, "dfsHaNamenodes", info->dfs_ha_namenodes);
	appendJsonString(&buf, "dfsNamenodeRpcAddress", info->dfs_namenode_rpc_address);
	appendJsonString(&buf, "dfsClientFailoverProxyProvider",
					 info->dfs_client_failover_proxy_provider);
	appendJsonBool(&buf, "dfsClientUseDatanodeHostname",
				   info->dfs_client_use_datanode_hostname,
				   info->dfs_client_use_datanode_hostname_set);
	appendJsonString(&buf, "hdfsAuthMethod", info->hdfs_auth_method);
	appendJsonString(&buf, "krbPrincipal", info->krb_principal);
	appendJsonString(&buf, "krbPrincipalKeytab", info->krb_principal_keytab);
	appendJsonString(&buf, "hadoopRpcProtection", info->hadoop_rpc_protection);
	appendJsonString(&buf, "dataTransferProtocol", info->data_transfer_protocol);
	appendJsonString(&buf, "dataTransferProtection", info->data_transfer_protection);

	nextras = list_length(info->extras);
	sorted = (IcebergSiteExtra **) palloc(sizeof(IcebergSiteExtra *) *
										  (nextras > 0 ? nextras : 1));
	i = 0;
	foreach(lc, info->extras)
		sorted[i++] = (IcebergSiteExtra *) lfirst(lc);
	if (nextras > 1)
		qsort(sorted, nextras, sizeof(IcebergSiteExtra *), extraKeyCompare);

	appendStringInfoString(&buf, "\"extraProperties\":{");
	for (i = 0; i < nextras; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		escape_json(&buf, sorted[i]->key);
		appendStringInfoChar(&buf, ':');
		escape_json(&buf, sorted[i]->value);
	}
	appendStringInfoString(&buf, "}}");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
