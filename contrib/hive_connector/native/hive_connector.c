#include "postgres.h"

#include "access/xact.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"

#include <unistd.h>
#include "libhms/src/hmsclient.h"
#include "config_parser.h"
#include "hive_helper.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);
extern void _PG_fini(void);
extern Datum sync_hive_table(PG_FUNCTION_ARGS);
extern Datum sync_hive_database(PG_FUNCTION_ARGS);
extern Datum create_foreign_server(PG_FUNCTION_ARGS);

enum InitStage
{
	FORMLESS_VOID,
	GUCS_REGISTERED,
	COMPLETE
};

static char *classPath;
static enum InitStage initStage = FORMLESS_VOID;

static bool
checkClassPath(char **newval, void **extra, GucSource source)
{
	if ( initStage < COMPLETE )
		return true;
	if ( classPath == *newval )
		return true;
	if ( classPath && *newval && 0 == strcmp(classPath, *newval) )
		return true;
	GUC_check_errmsg(
		"too late to change \"hive_connector.classpath\" setting");
	GUC_check_errdetail(
		"Changing the setting has no effect after "
		"Hive connector has started the Java virtual machine.");
	GUC_check_errhint(
		"To try a different value, exit this session and start a new one.");
	return false;
}

void
_PG_init(void)
{
	int             result;
	char           *javaHome;
	StringInfoData  jvmPath;
	char            errMessage[1024];

	initStringInfo(&jvmPath);
	if ((javaHome = getenv("JAVA_HOME")) == NULL)
	{
#if ((defined(__X86__) || defined(__i386__) || defined(i386) || defined(_M_IX86) || defined(__386__) || defined(__x86_64__) || defined(_M_X64)))
		javaHome = "/usr/lib/jvm/java-8-openjdk-amd64";
#else
		javaHome = "/usr/lib/jvm/java-8-openjdk-arm64";
#endif
	}

#if ((defined(__X86__) || defined(__i386__) || defined(i386) || defined(_M_IX86) || defined(__386__) || defined(__x86_64__) || defined(_M_X64)))
	appendStringInfo(&jvmPath, "%s/jre/lib/amd64/server/libjvm.so", javaHome);
#else
	appendStringInfo(&jvmPath, "%s/jre/lib/aarch64/server/libjvm.so", javaHome);
#endif

	if (initStage == FORMLESS_VOID)
	{
		StringInfoData libhmsPath;
		const char *gpHome;
		initStringInfo(&libhmsPath);
		if ((gpHome = getenv("GPHOME")) == NULL)
			gpHome = "/home/gpadmin/install/cbdb";
		appendStringInfo(&libhmsPath, "%s", gpHome);
		appendStringInfo(&libhmsPath, "/lib/postgresql/java/libhms-1.0.0-jar-with-dependencies.jar");
		
		DefineCustomStringVariable("hive_connector.classpath",
							   "Class path to be used by the JVM",
							   NULL,
							   &classPath,
							   libhmsPath.data,
							   PGC_USERSET,
							   0,
							   checkClassPath,
							   NULL,
							   NULL);
		pfree(libhmsPath.data);

		DefineCustomBoolVariable("hive_connector.enable_cache_file",
								 "Enable cache files",
								 NULL,
								 &hiveEnableCacheFile,
								 true,
								 PGC_USERSET,
								 0,
								 NULL,
								 NULL,
								 NULL);

		DefineCustomBoolVariable("hive_connector.enable_debug",
							"Enable cache files",
							NULL,
							&hiveEnableDebug,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

		initStage = GUCS_REGISTERED;
	}

	if (access(classPath, F_OK) != 0)
	{
		ereport(ERROR,
				(errmsg("could not load \"%s\"", classPath),
				 errhint("Please make sure you have the correct access rights and the jar exists.")));
	}

	result = HmsInitialize(jvmPath.data, classPath, errMessage);

	if (result == -1)
		ereport(ERROR,
				(errmsg("failed to initialize libhms: %s", errMessage)));

	pfree(jvmPath.data);
	initStage = COMPLETE;
}

static void
execCommentStatment(HmsHandle *hms, const char *tableName)
{
	int    i;
	char   statement[4096];
	char **statements = HmsTableGetColumnComments(hms);

	for (i = 0; statements[i] != NULL; i++)
	{
		snprintf(statement, sizeof(statement), statements[i], tableName);
		spiExecUtility(statement);
	}
}

void
_PG_fini(void)
{
	HmsDestroy();
}

static void
sync_partition_table(HmsHandle *hms,
					  const char *serverName,
					  const char *hiveDbName,
					  const char *hiveTableName,
					  const char *hdfsClusterName,
					  const char *destTableName,
					  const char *hiveClusterName,
					  bool forceSync,
					  bool logerrors,
					  int rejectlimit,
					  const char *islimitinrows)
{
	int partNum;
	char *location;
	char *createStmt;
	char **partKeys;
	char *field;
	char **commentStatement;

	if (!validateMetaData(hms, hiveDbName, hiveTableName, &partKeys, &field))
		return;

	location = HmsTableGetLocation(hms);

	createStmt = formCreateStmt2(hms,
								 destTableName,
								 serverName,
								 location,
								 field,
								 hdfsClusterName,
								 tableFormatConversion(hms),
								 hiveClusterName,
								 hiveDbName,
								 hiveTableName,
								 partKeys,
								 logerrors,
								 rejectlimit,
								 islimitinrows);


	if (forceSync)
		dropTable(destTableName);

	spiExecUtility(createStmt);

	execCommentStatment(hms, destTableName);

	pfree(field);
	pfree(location);
	pfree(createStmt);
	freeArray(partKeys);
}

static void
sync_normal_table(HmsHandle *hms,
				  const char *serverName,
				  const char *hiveDbName,
				  const char *hiveTableName,
				  const char *hdfsClusterName,
				  const char *destTableName,
				  bool forceSync,
				  bool logerrors,
				  int rejectlimit,
				  const char *islimitinrows)
{
	char *createStmt = NULL;
	char *location 	= NULL;
	char *field 	= NULL;

	location = HmsTableGetLocation(hms);
	if (!location)
		elog(ERROR, "failed to get location of table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));

	field = HmsTableGetField(hms);
	if (!field || !strlen(field))
		elog(ERROR, "failed to get columns of table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));

	createStmt = formCreateStmt(hms,
							destTableName,
							serverName,
							location,
							field,
							hdfsClusterName,
							tableFormatConversion(hms),
							logerrors,
							rejectlimit,
							islimitinrows);

	if (forceSync)
		dropTable(destTableName);

	spiExecUtility(createStmt);

	execCommentStatment(hms, destTableName);

	pfree(location);
	pfree(field);
	pfree(createStmt);
}

static bool
validateTable(HmsHandle *hms,
			  const char *hiveDbName,
			  const char *hiveTableName,
			  bool suppressError)
{
	char *tableType = NULL;
	char *format = NULL;
	char *serialLib = NULL;

	if (HmsGetTableMetaData(hms, hiveDbName, hiveTableName))
	{
		elog(suppressError ? WARNING : ERROR, "failed to sync table \"%s.%s\": %s",
				hiveDbName, hiveTableName, HmsGetError(hms));
		return false;
	}

	tableType = HmsTableGetTableType(hms);
	/*
	 * Hive 4.0 changed the default for `CREATE TABLE` from MANAGED_TABLE to
	 * EXTERNAL_TABLE, so a populator that worked on Hive 3.x now leaves
	 * EXTERNAL_TABLE entries behind.  Both shapes read the same way through
	 * the metastore (storage descriptor, partition columns, location) -- the
	 * difference only matters for DROP semantics, which sync_hive_table
	 * doesn't perform.  Accept both kinds so users do not have to chase the
	 * Hive 3 legacy flag.
	 */
	if (pg_strcasecmp(tableType, "MANAGED_TABLE") != 0 &&
		pg_strcasecmp(tableType, "EXTERNAL_TABLE") != 0)
	{
		elog(suppressError ? WARNING : ERROR, "failed to sync table \"%s.%s\": \"%s\" table is not supported",
				hiveDbName, hiveTableName, tableType);
		pfree(tableType);
		return false;
	}
	pfree(tableType);

	format = HmsTableGetFormat(hms);
	if ((pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat") != 0) &&
		(pg_strcasecmp(format, "org.apache.hadoop.mapred.TextInputFormat") != 0) &&
		(pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") != 0) &&
		(pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat") != 0))
	{
		elog(suppressError ? WARNING : ERROR, "failed to sync table \"%s.%s\": \"%s\" is not supported",
				hiveTableName, hiveTableName, format);
		pfree(format);
		return false;
	}

	serialLib = HmsTableGetSerialLib(hms);
	if (pg_strcasecmp(format, "org.apache.hadoop.mapred.TextInputFormat") == 0)
	{
		if ((pg_strcasecmp(serialLib, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) &&
			(pg_strcasecmp(serialLib, "org.apache.hadoop.hive.serde2.OpenCSVSerde")))
		{
			elog(suppressError ? WARNING : ERROR, "failed to sync table \"%s.%s\": \"%s\" serdeLib is not supported",
					hiveTableName, hiveTableName, serialLib);
			pfree(format);
			pfree(serialLib);
			return false;
		}
	}

	pfree(format);
	pfree(serialLib);
	return true;
}

static void
sync_hive_table_(HmsHandle *hms,
				 const char *serverName,
				 const char *hiveDbName,
				 const char *hiveTableName,
				 const char *hdfsClusterName,
				 const char *destTableName,
				 const char *hiveClusterName,
				 bool suppressError,
				 bool forceSync,
				 bool logerrors,
				 int rejectlimit,
				 const char *islimitinrows)
{
	bool  isPartTable;

	PG_TRY();
	{
		if (validateTable(hms, hiveDbName, hiveTableName, suppressError))
		{
			isPartTable = HmsTableIsPartitionTable(hms);
			if (isPartTable)
			{
				sync_partition_table(hms,
									  serverName,
									  hiveDbName,
									  hiveTableName,
									  hdfsClusterName,
									  destTableName,
									  hiveClusterName,
									  forceSync,
									  logerrors,
									  rejectlimit,
									  islimitinrows);
			}
			else
			{
				sync_normal_table(hms,
								serverName,
								hiveDbName,
								hiveTableName,
								hdfsClusterName,
								destTableName,
								forceSync,
								logerrors,
								rejectlimit,
								islimitinrows);
			}
		}

		HmsReleaseTableMetaData(hms);
	}
	PG_CATCH();
	{
		HmsReleaseTableMetaData(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
sync_hive_database_(HmsHandle *hms,
					const char *serverName,
					const char *hiveDbName,
					const char *hdfsClusterName,
					const char *destSchemaName,
					const char *hiveClusterName,
					bool forceSync,
					bool logerrors,
					int rejectlimit,
					const char *islimitinrows)
{
	int i;
	char **tables;
	StringInfoData nameBuf;

	tables = HmsGetTables(hms, hiveDbName);
	if (!tables)
		elog(ERROR, "failed to call hive_get_tables: %s", HmsGetError(hms));

	initStringInfo(&nameBuf);

	for (i = 0; tables[i] != NULL; i++)
	{
		char *hiveTableName = tables[i];

		CHECK_FOR_INTERRUPTS();

		appendStringInfo(&nameBuf, "%s.%s", destSchemaName, hiveTableName);

		sync_hive_table_(hms, serverName, hiveDbName, hiveTableName,
				hdfsClusterName, nameBuf.data, hiveClusterName, true, forceSync, logerrors, rejectlimit, islimitinrows);

		resetStringInfo(&nameBuf);
	}

	pfree(nameBuf.data);
	freeArray(tables);
}

static HmsHandle *
initializeHms(const char *hiveClusterName,
			  const char *hdfsClusterName)
{
	List       *hiveConf = NIL;
	List       *hdfsConf = NIL;
	ConfigItem *conf;
	bool        connected;
	char        errMsg[512];
	HmsHandle  *hms;
	char path[1024] = {0};
	sprintf(path, "%s/gphive.conf", DataDir);
	hiveConf = parseConf(path, true);
	if (!serverHiveConfExists(hiveConf, hiveClusterName))
		elog(ERROR, "didn't find \"%s\" in gphive.conf", hiveClusterName);

	if (hdfsClusterName != NULL)
	{
		sprintf(path, "%s/gphdfs.conf", DataDir);
		hdfsConf = parseHdfsConf("gphdfs.conf", false);
		if (!serverHdfsConfExists(hdfsConf, hdfsClusterName))
			elog(ERROR, "didn't find \"%s\" in gphdfs.conf", hdfsClusterName);
	}

	conf = getHiveServerConf(hiveConf, hiveClusterName);
	Assert(conf != NULL);

	hms = HmsCreateHandle(errMsg);
	if (!hms)
		elog(ERROR, "failed to create hms handle: %s", errMsg);

	if (!conf->authMethod || (conf->authMethod && !strcasecmp(conf->authMethod, "simple")))
		connected = HmsOpenConnection(hms, conf->uris, conf->debug ? 1 : 0);
	else if (conf->authMethod && !strcasecmp(conf->authMethod, "kerberos"))
		connected = HmsOpenConnectionWithKerberos(hms,
				conf->uris,
				conf->servicePrincipal,
				conf->clientPrincipal,
				conf->clientKeytabFile,
				conf->rpcProtection == NULL ? "authentication" : conf->rpcProtection,
				conf->debug ? 1 : 0);
	else
		elog(ERROR, "invalid auth_method \"%s\" for \"%s\": please use \"simple\",\"kerberos\"",
				conf->authMethod, conf->name);

	if (!connected)
		elog(ERROR, "failed to connect to hms: %s", HmsGetError(hms));

	list_free_deep(hiveConf);
	list_free_deep(hdfsConf);

	return hms;
}

PG_FUNCTION_INFO_V1(sync_hive_table);
Datum
sync_hive_table(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *hdfsClusterName;
	const char *destTableName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_ARGISNULL(5))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(5));

	if (PG_NARGS() == 7)
	{
		if (PG_ARGISNULL(6))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(6);
	}

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_table_(hms, serverName, hiveDbName, hiveTableName,
				hdfsClusterName, destTableName, hiveClusterName, false, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_database);
Datum
sync_hive_database(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hdfsClusterName;
	const char *destSchemaName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "dest schema name cannot be NULL");
	destSchemaName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_NARGS() == 6)
	{
		if (PG_ARGISNULL(5))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(5);
	} 

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_database_(hms, serverName, hiveDbName,
				hdfsClusterName, destSchemaName, hiveClusterName, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}






static void
create_foreign_server_(const char *serverName,
					   const char *userMapName,
					   const char *dataWrapName,
					   const HdfsConfigItem *hdfsConf)
{
	char *serverStmt = formServerCreateStmt(serverName, dataWrapName, hdfsConf);
	char *userStmt = formUserStmt(userMapName, serverName);

	spiExecUtility(serverStmt);
	spiExecUtility(userStmt);

	pfree(serverStmt);
	pfree(userStmt);
}

PG_FUNCTION_INFO_V1(create_foreign_server);
Datum create_foreign_server(PG_FUNCTION_ARGS)
{
	const char *serverName;
	const char *userMappingName;
	const char *dataWrapperName;
	const char *hdfsClusterName;
	bool result = false;
	HdfsConfigItem *hdfsConf;
	List *hdfsConfigList;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "User mapping name cannot be NULL");
	userMappingName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Data wrapper name cannot be NULL");
	dataWrapperName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	hdfsConfigList = parseHdfsConf("gphdfs.conf", true);
	if (!serverHdfsConfExists(hdfsConfigList, hdfsClusterName))
	{
		elog(ERROR, "didn't find \"%s\" in gphdfs.conf", hdfsClusterName);
	}
	hdfsConf = getHdfsServerConf(hdfsConfigList, hdfsClusterName);

	PG_TRY();
	{
		create_foreign_server_(serverName, userMappingName, dataWrapperName, hdfsConf);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}


PG_FUNCTION_INFO_V1(sync_hive_table_with_logerror);
Datum
sync_hive_table_with_logerror(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *hdfsClusterName;
	const char *destTableName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_ARGISNULL(5))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(5));

	if (PG_ARGISNULL(6))
		elog(ERROR, "logerrors cannot be NULL");
	/* LOG ERRORS PERSISTENTLY for COPY is not allowed for now. so set logerror is boolean. */
	logerrors = PG_GETARG_BOOL(6);

	if (PG_ARGISNULL(7))
		elog(ERROR, "rejectlimit cannot be NULL");
	rejectlimit = PG_GETARG_INT32(7);
	if (rejectlimit < 0)
	{
		elog(ERROR, "rejectlimit value %d no less than 0 is allowed", rejectlimit);
	}

	if (PG_ARGISNULL(8))
		elog(ERROR, "rejectlimit cannot be NULL");
	islimitinrows = text_to_cstring(PG_GETARG_TEXT_PP(8));
	if ((pg_strcasecmp(islimitinrows, "rows") != 0) &&
		(pg_strcasecmp(islimitinrows, "percent") != 0))
	{
		elog(ERROR, "islimitinrows option 'rows' or 'percent' unkonw  %s", islimitinrows);
	}

	if (PG_NARGS() == 10)
	{
		if (PG_ARGISNULL(9))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(9);
	}

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_table_(hms, serverName, hiveDbName, hiveTableName,
				hdfsClusterName, destTableName, hiveClusterName, false, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_database_with_logerror);
Datum
sync_hive_database_with_logerror(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hdfsClusterName;
	const char *destSchemaName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "dest schema name cannot be NULL");
	destSchemaName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_ARGISNULL(5))
		elog(ERROR, "logerrors cannot be NULL");
	/* LOG ERRORS PERSISTENTLY for COPY is not allowed for now. so set logerror is boolean. */
	logerrors = PG_GETARG_BOOL(5);

	if (PG_ARGISNULL(6))
		elog(ERROR, "rejectlimit cannot be NULL");
	rejectlimit = PG_GETARG_INT32(6);
	if (rejectlimit < 0)
	{
		elog(ERROR, "rejectlimit value %d no less than 0 is allowed", rejectlimit);
	}

	if (PG_ARGISNULL(7))
		elog(ERROR, "rejectlimit cannot be NULL");
	islimitinrows = text_to_cstring(PG_GETARG_TEXT_PP(7));
	if ((pg_strcasecmp(islimitinrows, "rows") != 0) &&
		(pg_strcasecmp(islimitinrows, "percent") != 0))
	{
		elog(ERROR, "islimitinrows option 'row' or 'percent' unkonw  %s", islimitinrows);
	}

	if (PG_NARGS() == 9)
	{
		if (PG_ARGISNULL(8))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(8);
	} 

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_database_(hms, serverName, hiveDbName,
				hdfsClusterName, destSchemaName, hiveClusterName, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}

//Compatibility3X
static void
sync_partition_table_compatibility3x(HmsHandle *hms,
					 const char *hiveDbName,
					 const char *hiveTableName,
					 const char *hdfsClusterName,
					 const char *destTableName,
					 const char *hiveClusterName,
					 bool forceSync,
					 const char *specifyMaxPartitionValue,
					 bool logerrors,
					 int rejectlimit,
					 const char *islimitinrows)
{
	char *location;
	char *createStmt;
	char **partKeys;
	char *field;
	char **commentStatement;

	if (!validateMetaDataCompatibility3X(hms, hiveDbName, hiveTableName, &partKeys, &field))
		return;

	location = HmsTableGetLocation(hms);

	if (specifyMaxPartitionValue)
	{
		createStmt = formCreateStmt3Compatibility3X(hms,
								destTableName,
								location,
								field,
								hdfsClusterName,
								tableFormatConversion(hms),
								hiveDbName,
								hiveTableName,
								hiveClusterName,
								partKeys,
								specifyMaxPartitionValue,
								logerrors,
								rejectlimit,
								islimitinrows);
	}
	else
	{
		createStmt = formCreateStmt2Compatibility3X(hms,
								destTableName,
								location,
								field,
								hdfsClusterName,
								tableFormatConversion(hms),
								hiveDbName,
								hiveTableName,
								hiveClusterName,
								partKeys,
								logerrors,
								rejectlimit,
								islimitinrows);
	}

	if (forceSync)
		dropTableCompatibility3X(destTableName, true);

	spiExec(createStmt);

	execCommentStatment(hms, destTableName);	

	pfree(field);
	pfree(location);
	pfree(createStmt);
	freeArray(partKeys);
}

static void
sync_normal_table_compatibility3x(HmsHandle *hms,
				  const char *hiveDbName,
				  const char *hiveTableName,
				  const char *hdfsClusterName,
				  const char *destTableName,
				  bool forceSync,
				  bool logerrors,
				  int rejectlimit,
				  const char *islimitinrows)
{
	char *sql;
	char *location = NULL;
	char *field = NULL;

	location = HmsTableGetLocation(hms);
	if (!location)
		elog(ERROR, "failed to get location of table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));

	field = HmsTableGetField(hms);
	if (!field || !strlen(field))
		elog(ERROR, "failed to get columns of table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));

	sql = formCreateStmtCompatibility3X(hms,
			destTableName,
			location,
			field,
			hdfsClusterName,
			tableFormatConversion(hms),
			logerrors,
			rejectlimit,
			islimitinrows);

	if (forceSync)
		dropTableCompatibility3X(destTableName, true);

	spiExec(sql);
	execCommentStatment(hms, destTableName);	

	pfree(location);
	pfree(field);
	pfree(sql);
}

static void
sync_hive_table_compatibility3x_(HmsHandle *hms,
				 const char *hiveDbName,
				 const char *hiveTableName,
				 const char *hdfsClusterName,
				 const char *destTableName,
				 const char *hiveClusterName,
				 bool suppressError,
				 bool forceSync,
				 const char *specifyMaxPartitionValue,
				 bool logerrors,
				 int rejectlimit,
				 const char *islimitinrows)
{
	bool  isPartTable;

	PG_TRY();
	{
		if (validateTable(hms, hiveDbName, hiveTableName, suppressError))
		{
			isPartTable = HmsTableIsPartitionTable(hms);
			if (isPartTable)
			{
				sync_partition_table_compatibility3x(hms,
									 hiveDbName,
									 hiveTableName,
									 hdfsClusterName,
									 destTableName,
									 hiveClusterName,
									 forceSync,
									 specifyMaxPartitionValue,
									 logerrors,
									 rejectlimit,
									 islimitinrows);
			}
			else
			{
				sync_normal_table_compatibility3x(hms,
					  hiveDbName,
					  hiveTableName,
					  hdfsClusterName,
					  destTableName,
					  forceSync,
					  logerrors,
					  rejectlimit,
					  islimitinrows);
			}
		}

		HmsReleaseTableMetaData(hms);
	}
	PG_CATCH();
	{
		HmsReleaseTableMetaData(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
sync_hive_database_compatibility3x_(HmsHandle *hms,
					const char *hiveDbName,
					const char *hdfsClusterName,
					const char *destSchemaName,
					const char *hiveClusterName,
					bool forceSync,
					bool logerrors,
					int rejectlimit,
					const char *islimitinrows)
{
	int i;
	char **tables;
	StringInfoData nameBuf;

	tables = HmsGetTables(hms, hiveDbName);
	if (!tables)
		elog(ERROR, "failed to call hive_get_tables: %s", HmsGetError(hms));

	initStringInfo(&nameBuf);

	for (i = 0; tables[i] != NULL; i++)
	{
		char *hiveTableName = tables[i];

		CHECK_FOR_INTERRUPTS();

		appendStringInfo(&nameBuf, "%s.%s", destSchemaName, hiveTableName);

		sync_hive_table_compatibility3x_(hms, hiveDbName, hiveTableName,
				hdfsClusterName, nameBuf.data, hiveClusterName, true, forceSync, NULL, logerrors, rejectlimit, islimitinrows);

		resetStringInfo(&nameBuf);
	}

	pfree(nameBuf.data);
	freeArray(tables);
}



PG_FUNCTION_INFO_V1(sync_hive_table_3x);
Datum
sync_hive_table_3x(PG_FUNCTION_ARGS)
{
	bool result = false;
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *hdfsClusterName;
	const char *destTableName;
	HmsHandle *volatile hms;
	bool forceSync = false;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (!PG_ARGISNULL(5))
		forceSync = PG_GETARG_BOOL(5);

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_table_compatibility3x_(hms, hiveDbName, hiveTableName,
				hdfsClusterName, destTableName, hiveClusterName, false, forceSync, NULL, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_partition_table_3x);
Datum
sync_hive_partition_table_3x(PG_FUNCTION_ARGS)
{
	bool result = false;
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *hdfsClusterName;
	const char *destTableName;
	const char *specifyMaxPartitionValue = NULL;
	HmsHandle *volatile hms;
	bool forceSync = false;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hive partition value cannot be NULL");
	specifyMaxPartitionValue = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_ARGISNULL(5))
		elog(ERROR, "dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(5));

	if (!PG_ARGISNULL(6))
		forceSync = PG_GETARG_BOOL(6);

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_table_compatibility3x_(hms, hiveDbName, hiveTableName,
				hdfsClusterName, destTableName, hiveClusterName, false, forceSync, specifyMaxPartitionValue, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_database_3x);
Datum
sync_hive_database_3x(PG_FUNCTION_ARGS)
{
	bool result = false;
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hdfsClusterName;
	const char *destSchemaName;
	HmsHandle *volatile hms;
	bool forceSync = false;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "dest schema name cannot be NULL");
	destSchemaName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (!PG_ARGISNULL(4))
		forceSync = PG_GETARG_BOOL(4);

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_database_compatibility3x_(hms, hiveDbName,
				hdfsClusterName, destSchemaName, hiveClusterName, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_table_with_logerror_3x);
Datum
sync_hive_table_with_logerror_3x(PG_FUNCTION_ARGS)
{
	bool result = false;
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *hdfsClusterName;
	const char *destTableName;
	HmsHandle *volatile hms;
	bool forceSync = false;
	bool tempTable = false;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_ARGISNULL(5))
		elog(ERROR, "logerrors cannot be NULL");
	logerrors = PG_GETARG_BOOL(5);

	if (PG_ARGISNULL(6))
		elog(ERROR, "rejectlimit cannot be NULL");
	rejectlimit = PG_GETARG_INT32(6);
	if (rejectlimit < 0)
	{
		elog(ERROR, "rejectlimit value %d no less than 0 is allowed", rejectlimit);
	}

	if (PG_ARGISNULL(7))
		elog(ERROR, "rejectlimit cannot be NULL");
	islimitinrows = text_to_cstring(PG_GETARG_TEXT_PP(7));
	if ((pg_strcasecmp(islimitinrows, "rows") != 0) &&
		(pg_strcasecmp(islimitinrows, "percent") != 0))
	{
		elog(ERROR, "islimitinrows option 'rows' or 'percent' unkonw  %s", islimitinrows);
	}

	if (!PG_ARGISNULL(8))
		forceSync = PG_GETARG_BOOL(8);

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_table_compatibility3x_(hms, hiveDbName, hiveTableName,
				hdfsClusterName, destTableName, hiveClusterName, false,
				forceSync, NULL, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_database_with_logerror_3x);
Datum
sync_hive_database_with_logerror_3x(PG_FUNCTION_ARGS)
{
	bool result = false;
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hdfsClusterName;
	const char *destSchemaName;
	HmsHandle *volatile hms;
	bool forceSync = false;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hdfs cluster name cannot be NULL");
	hdfsClusterName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "dest schema name cannot be NULL");
	destSchemaName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "logerrors cannot be NULL");
	logerrors = PG_GETARG_BOOL(4);

	if (PG_ARGISNULL(5))
		elog(ERROR, "rejectlimit cannot be NULL");
	rejectlimit = PG_GETARG_INT32(5);
	if (rejectlimit < 0)
	{
		elog(ERROR, "rejectlimit value %d no less than 0 is allowed", rejectlimit);
	}

	if (PG_ARGISNULL(6))
		elog(ERROR, "rejectlimit cannot be NULL");
	islimitinrows = text_to_cstring(PG_GETARG_TEXT_PP(6));
	if ((pg_strcasecmp(islimitinrows, "rows") != 0) &&
		(pg_strcasecmp(islimitinrows, "percent") != 0))
	{
		elog(ERROR, "islimitinrows option 'row' or 'percent' unkonw  %s", islimitinrows);
	}

	if (!PG_ARGISNULL(7))
		forceSync = PG_GETARG_BOOL(7);

	hms = initializeHms(hiveClusterName, hdfsClusterName);

	PG_TRY();
	{
		sync_hive_database_compatibility3x_(hms, hiveDbName,
				hdfsClusterName, destSchemaName, hiveClusterName, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;

	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_table_with_s3_storage);
Datum
sync_hive_table_with_s3_storage(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *hiveTableName;
	const char *destTableName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "Hive table name cannot be NULL");
	hiveTableName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Dest table name cannot be NULL");
	destTableName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_ARGISNULL(4))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (PG_NARGS() == 6)
	{
		if (PG_ARGISNULL(5))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(5);
	}

	hms = initializeHms(hiveClusterName, NULL);

	PG_TRY();
	{
		sync_hive_table_(hms, serverName, hiveDbName, hiveTableName,
				NULL, destTableName, hiveClusterName, false, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(sync_hive_database_with_s3_storage);
Datum
sync_hive_database_with_s3_storage(PG_FUNCTION_ARGS)
{
	const char *hiveClusterName;
	const char *hiveDbName;
	const char *destSchemaName;
	const char *serverName;
	bool result 	= false;
	bool forceSync 	= false;
	HmsHandle *volatile hms;
	bool logerrors = false;
	int rejectlimit = -1;
	const char *islimitinrows = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "Hive cluster name cannot be NULL");
	hiveClusterName = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
		elog(ERROR, "Hive database name cannot be NULL");
	hiveDbName = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (PG_ARGISNULL(2))
		elog(ERROR, "dest schema name cannot be NULL");
	destSchemaName = text_to_cstring(PG_GETARG_TEXT_PP(2));

	if (PG_ARGISNULL(3))
		elog(ERROR, "Server name cannot be NULL");
	serverName = text_to_cstring(PG_GETARG_TEXT_PP(3));

	if (PG_NARGS() == 5)
	{
		if (PG_ARGISNULL(4))
			elog(ERROR, "Force Sync flag cannot be NULL");
		forceSync = PG_GETARG_BOOL(4);
	} 

	hms = initializeHms(hiveClusterName, NULL);

	PG_TRY();
	{
		sync_hive_database_(hms, serverName, hiveDbName,
				NULL, destSchemaName, hiveClusterName, forceSync, logerrors, rejectlimit, islimitinrows);
		HmsDestroyHandle(hms);
	}
	PG_CATCH();
	{
		HmsDestroyHandle(hms);
		PG_RE_THROW();
	}
	PG_END_TRY();

	result = true;
	PG_RETURN_BOOL(result);
}