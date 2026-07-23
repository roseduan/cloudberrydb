#ifndef GOPHER_CONFIG_H
#define GOPHER_CONFIG_H

#include "postgres.h"
#include "nodes/pg_list.h"

typedef struct DatalakeHdfsHAConfEntry
{
	char *key;
	char *value;
} DatalakeHdfsHAConfEntry;

typedef struct DatalakeHdfsConfigInfo
{
	char *gopherPath;
	char *namenodeHost;
	char *namenodePort;
	char *authMethod;
	char *krbPrincipal;
	char *krbPrincipalKeytab;
	char *hadoopRpcProtection;
	char *dataTransferProtocol;
	char *dataTransferProtection;
	char *krb5CCName;
	char *enableHa;
	List *haEntries;
} DatalakeHdfsConfigInfo;

DatalakeHdfsConfigInfo *datalakeParseHdfsConfig(const char *configFile, const char *serverName);
char *datalakeGetDefaultHdfsCluster(const char *configFile);
void datalakeFormKrbCCName(DatalakeHdfsConfigInfo *config);

#ifdef USE_GOPHER
#include "gopher/gopher.h"
gopherConfig *datalakeGopherCreateConfig(DatalakeHdfsConfigInfo *hdfsConf);
void datalakeGopherConfigDestroy(gopherConfig *conf);
#endif

#endif /* GOPHER_CONFIG_H */
