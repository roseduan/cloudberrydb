#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include <yaml.h>
#include <stdio.h>
#include "config.h"
// #include "datalake_extension.h"

#define GOPHERMETA_FOLDER "gophermeta"

extern bool disableCacheFile;

typedef struct
{
	const char *optname;
	int			offset;
} config_elt;

static const config_elt configElts[] = {
	{"hdfs_namenode_host", offsetof(DatalakeHdfsConfigInfo, namenodeHost)},
	{"hdfs_namenode_port", offsetof(DatalakeHdfsConfigInfo, namenodePort)},
	/*
	 * Aliases for the new conf format whose keys equal the SQL OPTION names
	 * (see the iceberg guide 4.3 conf-file mode). This legacy reader stays
	 * dual-track: old spellings keep working, files migrated for the
	 * iceberg path keep working too.
	 */
	{"hdfs_namenodes", offsetof(DatalakeHdfsConfigInfo, namenodeHost)},
	{"hdfs_port", offsetof(DatalakeHdfsConfigInfo, namenodePort)},
	{"hdfs_auth_method", offsetof(DatalakeHdfsConfigInfo, authMethod)},
	{"krb_principal", offsetof(DatalakeHdfsConfigInfo, krbPrincipal)},
	{"krb_principal_keytab", offsetof(DatalakeHdfsConfigInfo, krbPrincipalKeytab)},
	{"hadoop_rpc_protection", offsetof(DatalakeHdfsConfigInfo, hadoopRpcProtection)},
	{"data_transfer_protocol", offsetof(DatalakeHdfsConfigInfo, dataTransferProtocol)},
	{"data_transfer_protection", offsetof(DatalakeHdfsConfigInfo, dataTransferProtection)},
	{"is_ha_supported", offsetof(DatalakeHdfsConfigInfo, enableHa)}
};

/*
 * New-format HA keys (underscore, equal to the SQL OPTION names) translated
 * back to the dotted Hadoop keys this path forwards to gopher verbatim.
 */
static const struct
{
	const char *newKey;
	const char *legacyKey;
} haKeyAliases[] = {
	{"dfs_nameservices", "dfs.nameservices"},
	{"dfs_ha_namenodes", "dfs.ha.namenodes"},
	{"dfs_namenode_rpc_address", "dfs.namenode.rpc-address"},
};

static int
lookupConfigEntry(const char *keyName)
{
	int  i;

	for (i = 0; i < lengthof(configElts); i++)
	{
		if (pg_strcasecmp(configElts[i].optname, keyName) == 0)
			return i;
	}

	return -1;
}

static void
GetGopherMetaPath(char *dest)
{
	sprintf(dest, "%s/%s", DataDir, GOPHERMETA_FOLDER);
}

/*
 * Post-process a parsed section so the new conf format behaves like the
 * legacy one downstream:
 *
 * - hdfs_namenodes may splice "host:port"; split it, the spliced port
 *   winning over a separate hdfs_port key.  Default the port to 8020 when
 *   only a host was given (the documented hdfs_port default).
 * - translate the underscore dfs_* HA keys to the dotted Hadoop spellings
 *   gopher expects, the failover provider regaining its per-nameservice
 *   suffix.
 *
 * Legacy-format sections come through unchanged: their keys match none of
 * the rewrites below.
 */
static void
datalakeNormalizeHdfsConfig(DatalakeHdfsConfigInfo *hci)
{
	ListCell   *lc;
	const char *nameServices = NULL;

	if (hci->namenodeHost != NULL)
	{
		char *colon = strrchr(hci->namenodeHost, ':');

		if (colon != NULL)
		{
			hci->namenodePort = pstrdup(colon + 1);
			*colon = '\0';
		}
		else if (hci->namenodePort == NULL)
			hci->namenodePort = pstrdup("8020");
	}

	foreach(lc, hci->haEntries)
	{
		DatalakeHdfsHAConfEntry *ent = (DatalakeHdfsHAConfEntry *) lfirst(lc);
		int			i;

		for (i = 0; i < lengthof(haKeyAliases); i++)
		{
			if (pg_strcasecmp(ent->key, haKeyAliases[i].newKey) == 0)
			{
				pfree(ent->key);
				ent->key = pstrdup(haKeyAliases[i].legacyKey);
				break;
			}
		}

		if (pg_strcasecmp(ent->key, "dfs.nameservices") == 0)
			nameServices = ent->value;
	}

	foreach(lc, hci->haEntries)
	{
		DatalakeHdfsHAConfEntry *ent = (DatalakeHdfsHAConfEntry *) lfirst(lc);

		if (pg_strcasecmp(ent->key, "dfs_client_failover_proxy_provider") == 0 &&
			nameServices != NULL)
		{
			pfree(ent->key);
			ent->key = psprintf("dfs.client.failover.proxy.provider.%s", nameServices);
		}
	}
}

void
datalakeFormKrbCCName(DatalakeHdfsConfigInfo *config)
{
	int i;
	int len;
	char gopherPath[MAXPGPATH];
	char ccName[MAXPGPATH];
	char *krb5ccStr;

	GetGopherMetaPath(gopherPath);
	snprintf(ccName, MAXPGPATH, "%s/krb5cc_%s", gopherPath, config->krbPrincipal);

	krb5ccStr = strstr(ccName, "krb5cc");
	len = strlen(krb5ccStr);
	for (i = 0; i < len; i++)
	{
		if (!isalnum(krb5ccStr[i]))
			krb5ccStr[i] = '_';
	}

	config->krb5CCName = pstrdup(ccName);
	config->gopherPath = pstrdup(gopherPath);
}

/*
 * datalakeGetDefaultHdfsCluster
 *
 * Read the optional top-level "default" key from gphdfs.conf, which names the
 * fallback HDFS cluster to use when an external table's LOCATION does not
 * specify hdfs_cluster_name.  Return a palloc'd copy of the cluster name, or
 * NULL when the file cannot be opened, has no "default" key, or that key's
 * value is not a scalar.  A missing or unreadable file is intentionally not an
 * error here: absence of a default just means there is no fallback cluster.
 */
char *
datalakeGetDefaultHdfsCluster(const char *configFile)
{
	FILE             *fp;
	yaml_parser_t     parser;
	yaml_document_t   document;
	yaml_node_t      *root;
	yaml_node_pair_t *tnp;
	char             *result = NULL;

	fp = fopen(configFile, "rb");
	if (fp == NULL)
		return NULL;

	if (!yaml_parser_initialize(&parser))
	{
		fclose(fp);
		return NULL;
	}

	yaml_parser_set_input_file(&parser, fp);
	if (!yaml_parser_load(&parser, &document))
	{
		yaml_parser_delete(&parser);
		fclose(fp);
		return NULL;
	}
	fclose(fp);

	root = yaml_document_get_root_node(&document);
	if (root != NULL && root->type == YAML_MAPPING_NODE)
	{
		for (tnp = root->data.mapping.pairs.start; tnp < root->data.mapping.pairs.top; tnp++)
		{
			yaml_node_t *key = yaml_document_get_node(&document, tnp->key);
			yaml_node_t *value;

			if (key == NULL || key->type != YAML_SCALAR_NODE)
				continue;
			if (pg_strcasecmp((const char *) key->data.scalar.value, "default") != 0)
				continue;

			value = yaml_document_get_node(&document, tnp->value);
			if (value != NULL && value->type == YAML_SCALAR_NODE)
				result = pnstrdup((const char *) value->data.scalar.value,
								  value->data.scalar.length);
			break;
		}
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}

DatalakeHdfsConfigInfo *
datalakeParseHdfsConfig(const char *configFile, const char *serverName)
{
	int               i;
	FILE             *fp;
	yaml_parser_t     parser;
	yaml_document_t   document;
	yaml_node_t      *root;
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	DatalakeHdfsConfigInfo   *result = NULL;
	char             *eltPos;

	if (!yaml_parser_initialize(&parser))
		elog(ERROR, "could not create yaml parser \"%s\": out of memory", configFile);

	fp = fopen(configFile, "rb");
	if (fp == NULL)
		elog(ERROR, "could not open file \"%s\": %m", configFile);

	yaml_parser_set_input_file(&parser, fp);
	if (!yaml_parser_load(&parser, &document))
	{
		fclose(fp);
		elog(ERROR, "failed to load yaml \"%s\": %s [%lu, %lu]",
				configFile,
				parser.problem,
				parser.problem_mark.line,
				parser.problem_mark.column + 1);
	}
	fclose(fp);

	root = yaml_document_get_root_node(&document);
	if (!root)
		elog(ERROR, "failed to parse \"%s\": no root node", configFile);

	if (root->type != YAML_MAPPING_NODE)
		elog(ERROR, "failed to parse \"%s\": root node must be mapping node", configFile);

	for (tnp = root->data.mapping.pairs.start; tnp < root->data.mapping.pairs.top; tnp++)
	{
		DatalakeHdfsConfigInfo *hci;

		node = yaml_document_get_node(&document, tnp->key);
		if (pg_strcasecmp((const char *) node->data.scalar.value, serverName) != 0)
			continue;

		hci = palloc0(sizeof(DatalakeHdfsConfigInfo));
		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, (const char *) node->data.scalar.value);

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{
			yaml_node_t *nodeKey;
			yaml_node_t *nodeValue;

			nodeKey = yaml_document_get_node(&document, nnp->key);
			nodeValue = yaml_document_get_node(&document, nnp->value);
			if (nodeValue->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of key \"%s\" must be scalar node",
						configFile, (const char *) nodeKey->data.scalar.value);

			i = lookupConfigEntry((const char *) nodeKey->data.scalar.value);
			if (i >= 0)
			{
				eltPos = ((char *) hci) + configElts[i].offset;
				*(char **) eltPos = pnstrdup((const char *) nodeValue->data.scalar.value, nodeValue->data.scalar.length);
			}
			else
			{
				DatalakeHdfsHAConfEntry *ent = palloc0(sizeof(DatalakeHdfsHAConfEntry));

				ent->key = pnstrdup((const char *) nodeKey->data.scalar.value, nodeKey->data.scalar.length);
				ent->value = pnstrdup((const char *) nodeValue->data.scalar.value, nodeValue->data.scalar.length);
				hci->haEntries = lappend(hci->haEntries, ent);
			}
		}

		datalakeNormalizeHdfsConfig(hci);

		result = hci;
		break;
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}

#ifdef USE_GOPHER
gopherConfig *
datalakeGopherCreateConfig(DatalakeHdfsConfigInfo *hdfsConf)
{
	gopherConfig *config = (gopherConfig *) palloc0(sizeof(gopherConfig));

	config->connect_path = hdfsConf->gopherPath;
	config->cache_strategy = GOPHER_CACHE;
	config->ufs_type = HDFS;
	config->name_node = hdfsConf->namenodeHost;
	config->port = atoi(hdfsConf->namenodePort);
	config->auth_method = hdfsConf->authMethod;
	config->krb_principal = hdfsConf->krbPrincipal;
	config->krb_server_key_file = hdfsConf->krbPrincipalKeytab;
	config->krb5_ticket_cache_path = hdfsConf->krb5CCName;
	config->hadoop_rpc_protection = hdfsConf->hadoopRpcProtection;

	if (disableCacheFile)
		config->cache_strategy = GOPHER_NOT_CACHE;

	if (hdfsConf->enableHa && pg_strcasecmp(hdfsConf->enableHa, "true") == 0)
		config->is_ha_supported = true;

	if (hdfsConf->dataTransferProtocol && pg_strcasecmp(hdfsConf->dataTransferProtocol, "true") == 0)
		config->data_transfer_protocol = true;

	config->data_transfer_protection = hdfsConf->dataTransferProtection;

	config->hdfs_ha_configs_num = list_length(hdfsConf->haEntries);
	if (config->hdfs_ha_configs_num > 0)
	{
		int i = 0;
		ListCell *lc = NULL;

		config->hdfs_ha_configs =
				(HdfsHAConfig*) palloc0(sizeof(HdfsHAConfig) * config->hdfs_ha_configs_num);

		foreach_with_count(lc, hdfsConf->haEntries, i)
		{
			DatalakeHdfsHAConfEntry *entry = (DatalakeHdfsHAConfEntry *) lfirst(lc);

			config->hdfs_ha_configs[i].key = entry->key;
			config->hdfs_ha_configs[i].value = entry->value;
		}
	}

	return config;
}

void
datalakeGopherConfigDestroy(gopherConfig *conf)
{
	int i;

	pfree(conf->connect_path);

	if (conf->name_node != NULL)
		pfree(conf->name_node);

	if (conf->auth_method != NULL)
		pfree(conf->auth_method);

	if (conf->krb5_ticket_cache_path != NULL)
		pfree(conf->krb5_ticket_cache_path);

	if (conf->krb_server_key_file != NULL)
		pfree(conf->krb_server_key_file);

	if (conf->krb_principal != NULL)
		pfree(conf->krb_principal);

	if (conf->hadoop_rpc_protection != NULL)
		pfree(conf->hadoop_rpc_protection);

	if (conf->data_transfer_protection != NULL)
		pfree(conf->data_transfer_protection);

	if (conf->hdfs_ha_configs_num > 0)
	{
		for (i = 0; i < conf->hdfs_ha_configs_num; i++)
		{
			if (conf->hdfs_ha_configs[i].key)
				pfree(conf->hdfs_ha_configs[i].key);
			if (conf->hdfs_ha_configs[i].value)
				pfree(conf->hdfs_ha_configs[i].value);
		}
	}

	if (conf->hdfs_ha_configs != NULL)
		pfree(conf->hdfs_ha_configs);

	pfree(conf);
}
#endif /* USE_GOPHER */
