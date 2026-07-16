#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include <stdio.h>
#include <yaml.h>
#include "config_parser.h"
#include "hive_helper.h"
#include "strings_util.h"

typedef struct
{
	const char *optname;
	int			offset;
} config_elt;

/*
 * The iceberg conf-file format renamed the site-file keys to the SQL OPTION
 * names ("url" instead of "uris", "hdfs_namenodes"/"hdfs_port" instead of
 * "hdfs_namenode_host"/"hdfs_namenode_port", compact underscore dfs_* HA
 * keys).  These readers stay dual-track so a shared gphive.conf/gphdfs.conf
 * migrated for iceberg keeps working here.
 */
static const config_elt configElts[] = {
	{"uris", offsetof(ConfigItem, uris)},
	{"url", offsetof(ConfigItem, uris)},
	{"auth_method", offsetof(ConfigItem, authMethod)},
	{"krb_service_principal", offsetof(ConfigItem, servicePrincipal)},
	{"krb_client_principal", offsetof(ConfigItem, clientPrincipal)},
	{"krb_client_keytab", offsetof(ConfigItem, clientKeytabFile)},
	{"hadoop_rpc_protection", offsetof(ConfigItem, rpcProtection)},
	{"debug", offsetof(ConfigItem, debug)}
};

static const config_elt hdfsConfigElts[] = {
	{"hdfs_namenode_host", offsetof(HdfsConfigItem, host)},
	{"hdfs_namenode_port", offsetof(HdfsConfigItem, port)},
	{"hdfs_namenodes", offsetof(HdfsConfigItem, host)},
	{"hdfs_port", offsetof(HdfsConfigItem, port)},
	{"hdfs_auth_method", offsetof(HdfsConfigItem, authMethod)},
	{"krb_principal", offsetof(HdfsConfigItem, krbPrincipal)},
	{"krb_principal_keytab", offsetof(HdfsConfigItem, krbKeytab)},
	{"hadoop_rpc_protection", offsetof(HdfsConfigItem, hadoopRpcProtection)},
	{"data_transfer_protocol", offsetof(HdfsConfigItem, dataTransferProtocol)},
	{"data_transfer_protection", offsetof(HdfsConfigItem, dataTransferProtection)},
	{"krb_service_principal", offsetof(HdfsConfigItem, krbServicePrincipal)},
	{"is_ha_supported", offsetof(HdfsConfigItem, enableHA)},
};

static HdfsHAConfEntry *
makeHaEntry(char *key, char *value)
{
	HdfsHAConfEntry *ent = palloc0(sizeof(HdfsHAConfEntry));

	ent->key = key;
	ent->value = value;
	return ent;
}

/*
 * Post-process a parsed gphdfs.conf section written in the new format (keys
 * equal to the SQL OPTION names) into the shapes the helpers downstream
 * expect:
 *
 * - hdfs_namenodes may splice "host:port"; split it, the spliced port
 *   winning over a separate hdfs_port key.  Default the port to 8020 when
 *   only a host was given in non-HA mode.
 * - the compact underscore HA keys (dfs_nameservices: ns,
 *   dfs_ha_namenodes: nn1,nn2, dfs_namenode_rpc_address: addr1,addr2,
 *   dfs_client_failover_proxy_provider) expand to the suffixed dotted
 *   entries extractServiceName/extractNameNodes/extractRpcAddr look up
 *   (dfs.ha.namenodes.<ns>, dfs.namenode.rpc-address.<ns>.<nn>, ...).
 *
 * Legacy-format sections come through unchanged: their keys match none of
 * the rewrites.
 */
static void
normalizeHdfsConfigItem(HdfsConfigItem *hci)
{
	ListCell   *lc;
	char	   *nameServices = NULL;
	char	   *haNamenodes = NULL;
	char	   *rpcAddrs = NULL;
	char	   *failoverProvider = NULL;
	List	   *kept = NIL;
	bool		isHa = hci->enableHA && strcmp(hci->enableHA, "true") == 0;

	if (hci->host != NULL)
	{
		char	   *colon = strrchr(hci->host, ':');

		/*
		 * port is declared int64_t but holds a string everywhere (the parse
		 * loop stores pnstrdup() through a char** cast and the SQL builder
		 * prints it with %s); follow the same convention.
		 */
		if (colon != NULL)
		{
			*(char **) &hci->port = pstrdup(colon + 1);
			*colon = '\0';
		}
		else if (hci->port == 0 && !isHa)
			*(char **) &hci->port = pstrdup("8020");
	}

	foreach(lc, hci->haEntries)
	{
		HdfsHAConfEntry *ent = (HdfsHAConfEntry *) lfirst(lc);

		if (strcmp(ent->key, "dfs_nameservices") == 0)
			nameServices = ent->value;
		else if (strcmp(ent->key, "dfs_ha_namenodes") == 0)
			haNamenodes = ent->value;
		else if (strcmp(ent->key, "dfs_namenode_rpc_address") == 0)
			rpcAddrs = ent->value;
		else if (strcmp(ent->key, "dfs_client_failover_proxy_provider") == 0)
			failoverProvider = ent->value;
		else
			kept = lappend(kept, ent);
	}

	if (nameServices == NULL)
		return;					/* legacy format (or no HA config at all) */

	kept = lappend(kept, makeHaEntry(pstrdup("dfs.nameservices"), nameServices));

	if (haNamenodes != NULL)
		kept = lappend(kept, makeHaEntry(
				psprintf("dfs.ha.namenodes.%s", nameServices), haNamenodes));

	if (haNamenodes != NULL && rpcAddrs != NULL)
	{
		List	   *names = splitString_(haNamenodes, ',', '\0');
		List	   *addrs = splitString_(rpcAddrs, ',', '\0');
		ListCell   *nameCell;
		ListCell   *addrCell;

		if (list_length(names) != list_length(addrs))
			elog(ERROR, "dfs_namenode_rpc_address lists %d address(es) but dfs_ha_namenodes lists %d namenode(s)",
				 list_length(addrs), list_length(names));

		forboth(nameCell, names, addrCell, addrs)
		{
			kept = lappend(kept, makeHaEntry(
					psprintf("dfs.namenode.rpc-address.%s.%s",
							 nameServices, (char *) lfirst(nameCell)),
					pstrdup((char *) lfirst(addrCell))));
		}
	}

	if (failoverProvider != NULL)
		kept = lappend(kept, makeHaEntry(
				psprintf("dfs.client.failover.proxy.provider.%s", nameServices),
				failoverProvider));

	hci->haEntries = kept;
}

void hiveConfCheck(ConfigItem *conf)
{
    if (!conf->authMethod)
    {
        conf->authMethod = pstrdup("simple");
    }
    if (!conf->uris)
    {
        elog(ERROR, "Missing option uris in config %s", conf->name);
    }
    if (!strcmp(conf->authMethod, "kerberos") && (!conf->servicePrincipal || !conf->clientPrincipal || !conf->clientKeytabFile))
    {
        elog(ERROR, "Missing option kerberos in config %s", conf->name);
    }
}

void hdfsConfCheck(HdfsConfigItem *conf)
{
    if (!conf->enableHA)
    {
        conf->enableHA = pstrdup("false");
    }
    if (!conf->authMethod)
    {
        conf->authMethod = pstrdup("simple");
    }
    if (!conf->host)
    {
        elog(ERROR, "Missing option namenode_host in config %s", conf->name);
    }
    if (!strcmp(conf->authMethod, "kerberos") && (!conf->krbKeytab || !conf->krbPrincipal))
    {
        elog(ERROR, "Missing option kerberos in config %s", conf->name);
    }
    if (!strcmp(conf->enableHA, "true"))
    {
        const char *serviceName         = extractServiceName(conf);
        const char *nodeNames           = extractNameNodes(conf, serviceName);
              char *rpcAddrs            = extractRpcAddr(conf, serviceName, nodeNames);
        pfree(rpcAddrs);
    }
	else if (!conf->port)
	{
		elog(ERROR, "Missing option namenode_port");
	}
}

List *
parseConf(const char *configFile, bool isFullMode)
{
	int               i;
	bool              found;
	char              key[1024];
	FILE              *fp;
	yaml_parser_t 	  parser;
	yaml_document_t   document;
	yaml_node_t      *root = NULL;
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	List             *result = NIL;
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
		ConfigItem *hci = palloc0(sizeof(ConfigItem));

		node = yaml_document_get_node(&document, tnp->key);
		if (node->data.scalar.length <= 0)
			continue;

		hci->name = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);

		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, hci->name);

		if (!isFullMode)
		{
			result = lappend(result, hci);
			continue;
		}

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{
			node = yaml_document_get_node(&document, nnp->key);
			if (node->data.scalar.length <= 0)
				continue;

			found = false;
			strncpy(key, (const char *)node->data.scalar.value, sizeof(key) -1);
			for (i = 0; i < lengthof(configElts); i++)
			{
				if (!strcmp(configElts[i].optname, (const char *)node->data.scalar.value))
				{
					found = true;
					break;
				}
			}

			if (!found)
				elog(ERROR, "failed to parse \"%s\": unrecognized configuration parameter \"%s\" for server \"%s\"",
							configFile, key, hci->name);

			node = yaml_document_get_node(&document, nnp->value);
			if (node->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of \"%s\" must be scalar node",
						configFile, key);

			eltPos = ((char *) hci) + configElts[i].offset;
			*(char **) eltPos = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);
		}
		result = lappend(result, hci);
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}

List *
parseHdfsConf(const char *configFile, bool isFullMode)
{
	int               i;
	bool              found;
	char              key[1024];
	FILE             *fp;
	yaml_parser_t 	  parser;
	yaml_document_t   document;
	yaml_node_t      *root = NULL;		
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	List             *result = NIL;
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
		HdfsConfigItem *hci = palloc0(sizeof(HdfsConfigItem));

		node = yaml_document_get_node(&document, tnp->key);
		if (node->data.scalar.length <= 0)
			continue;

		hci->name = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);

		/*
		 * Skip the reserved top-level "default" key, a scalar naming the
		 * fallback cluster for callers that omit the cluster name.  It is not
		 * a cluster section, so it has no mapping value.
		 */
		if (pg_strcasecmp(hci->name, "default") == 0)
			continue;

		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, hci->name);

		if (!isFullMode)
		{
			result = lappend(result, hci);
			continue;
		}

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{

			yaml_node_t   *nodeKey;
			yaml_node_t   *nodeVal;
			nodeKey = yaml_document_get_node(&document, nnp->key);
			nodeVal = yaml_document_get_node(&document, nnp->value);

			if (nodeKey->data.scalar.length <= 0)
				continue;

			if (nodeVal->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of \"%s\" must be scalar node",
						configFile, key);

			found = false;
			strncpy(key, (const char *)nodeKey->data.scalar.value, sizeof(key) -1);
			for (i = 0; i < lengthof(hdfsConfigElts); i++)
			{
				if (!strcmp(hdfsConfigElts[i].optname, (const char *)nodeKey->data.scalar.value))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				HdfsHAConfEntry *ent = palloc0(sizeof(HdfsHAConfEntry));
				ent->key = pnstrdup((const char *) nodeKey->data.scalar.value, nodeKey->data.scalar.length);
				ent->value = pnstrdup((const char *) nodeVal->data.scalar.value, nodeVal->data.scalar.length);
				hci->haEntries = lappend(hci->haEntries, ent);
			}
			else
			{
				eltPos = ((char *) hci) + hdfsConfigElts[i].offset;
				*(char **) eltPos = pnstrdup((const char *)nodeVal->data.scalar.value, nodeVal->data.scalar.length);
			}
		}
		normalizeHdfsConfigItem(hci);
		result = lappend(result, hci);
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}