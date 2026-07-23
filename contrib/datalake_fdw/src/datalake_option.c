/*
 * dataLake_option.c
 *		  Foreign-data wrapper option handling for DATA LAKE (Platform Extension Framework)
 *
 */
#include "datalake_option.h"
#include "common/partition_selector.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "foreign/foreign.h"
#include "access/table.h"
#include "postmaster/postmaster.h"
#include "datalake_def.h"
#include "common/util.h"



struct datalakeFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

static const struct datalakeFdwOption valid_oss_server_options[] = {
	{DATALAKE_OPTION_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HOST, ForeignServerRelationId},
	{DATALAKE_OPTION_PORT, ForeignServerRelationId},
	{DATALAKE_OPTION_ISVIRTUAL, ForeignServerRelationId},
	{DATALAKE_OPTION_ISHTTPS, ForeignServerRelationId},
	{DATALAKE_OPTION_LISTV2, ForeignServerRelationId},
	{DATALAKE_OPTION_REGION, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_hdfs_server_options[] = {
	{DATALAKE_OPTION_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_NAMENODE, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_PORT, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_KRB_PRINCIPAL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTECTION, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_KRB_SERVICE_PRINCIPAL, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_polaris_server_options[] = {
	{DATALAKE_OPTION_POLARIS_SERVER_URL, ForeignServerRelationId},
	{DATALAKE_OPTION_POLARIS_SERVER_REALM, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_ftp_server_options[] = {
	{DATALAKE_OPTION_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HOST, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_foreign_options[] = {
	{DATALAKE_OPTION_COMPRESS, ForeignTableRelationId},
	{DATALAKE_OPTION_FILEPATH, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_TEXT, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_CSV, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_ORC, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_PARQUET, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_AVRO, ForeignTableRelationId},
	{DATALAKE_OPTION_FILE_SIZE_LIMIT, ForeignTableRelationId},
	{DATALAKE_OPTION_ENABLE_CACHE, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_DATASOURCE, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_PARTITIONKEY, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_TRANSACTIONAL, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_CLUSTER_NAME, ForeignTableRelationId},
	{DATALAKE_OPTION_HDFS_CLUSTER_NAME, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORMAT, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_HEADER, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_DELIMITER, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_QUOTE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_ESCAPE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_NULL, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_ENCODING, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_NEWLINE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORCE_NOT_NULL, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORCE_NULL, ForeignTableRelationId},
	{DATALAKE_COPY_OPTIION_LOGERRORS, ForeignTableRelationId},
	{DATALAKE_COPY_OPTIION_REJECTLIMIT, ForeignTableRelationId},
	{DATALAKE_COPY_OPTIION_REJECTLIMITTYPE, ForeignTableRelationId},
	{DATALAKE_OPTION_TABLE_IDENTIFIER, ForeignTableRelationId},
	{DATALAKE_OPTION_SERVER_NAME, ForeignTableRelationId},
	{DATALAKE_OPTION_CATALOG_TYPE, ForeignTableRelationId},
	{DATALAKE_OPTION_SPLIT_SIZE, ForeignTableRelationId},
	{DATALAKE_OPTION_CACHE_ENABLED, ForeignTableRelationId},
	{DATALAKE_OPTION_QUERY_TYPE, ForeignTableRelationId},
	{DATALAKE_OPTION_METADATA_TABLE_ENABLE, ForeignTableRelationId},
	{DATALAKE_OPTION_PARTITION_VALUE, ForeignTableRelationId},
	{DATALAKE_OPTION_CATALOG_DEFAULT_IMPL, ForeignTableRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_usermaping_options[] = {
	{DATALAKE_OPTION_ACCESKEY, UserMappingRelationId},
	{DATALAKE_OPTION_SECRETKEY, UserMappingRelationId},
	{DATALAKE_OPTION_USER, UserMappingRelationId},
	{DATALAKE_OPTION_PASSWORD, UserMappingRelationId},
	{DATALAKE_OPTION_CLIENT_ID, UserMappingRelationId},
	{DATALAKE_OPTION_CLIENT_SECRET, UserMappingRelationId},
	{DATALAKE_OPTION_SCOPE, UserMappingRelationId},
	{NULL, InvalidOid}
};

struct FdwTableFormatOption
{
	const char *table_name;
	DLTblFmt table;
};

static const struct FdwTableFormatOption table_format_mapping[] = {
	{DATALAKE_OPTION_FORMAT_TEXT, DL_TEXT_TABLE},
	{DATALAKE_OPTION_FORMAT_CSV, DL_CSV_TABLE},
	{DATALAKE_OPTION_FORMAT_CUSTOM, DL_CUSTOM_TABLE},
	{DATALAKE_OPTION_FORMAT_ORC, DL_ORC_TABLE},
	{DATALAKE_OPTION_FORMAT_PARQUET, DL_PARQUET_TABLE},
	{DATALAKE_OPTION_FORMAT_AVRO, DL_AVRO_TABLE},
	{DATALAKE_OPTION_FORMAT_HUDI, DL_HUDI_TABLE},
	{DATALAKE_OPTION_FORMAT_ICEBERG, DL_ICEBERG_TABLE},
};

static DLTblFmt datalakeGetTableFormat(const char* table)
{
	if (!table)
	{
		return DL_INVALID_TABLE_FORMAT;
	}
	for (int i = 0; i < sizeof(table_format_mapping) / sizeof(table_format_mapping[0]); i++)
	{
		if (pg_strcasecmp(table, table_format_mapping[i].table_name) == 0)
		{
			return table_format_mapping[i].table;
		}
	}
	return DL_INVALID_TABLE_FORMAT;
}

struct FdwProtocolOption
{
	const char *protocol_name;
	DLProt protocol;
};

static const struct FdwProtocolOption protocol_mapping[] = {
	{DATALAKE_HDFS_PROTOCOL, DL_HDFS_PROTOCOL},
	{DATALAKE_FTP_PROTOCOL, DL_FTP_PROTOCOL},
	{DATALAKE_OSS_PROTOCOL_S3, DL_OSS_PROTOCOL_S3A},
	{DATALAKE_OSS_PROTOCOL_S3B, DL_OSS_PROTOCOL_S3AV2},
	{DATALAKE_OSS_PROTOCOL_ALI, DL_OSS_PROTOCOL_ALI},
	{DATALAKE_OSS_PROTOCOL_COS, DL_OSS_PROTOCOL_COS},
	{DATALAKE_OSS_PROTOCOL_QINGSTORE, DL_OSS_PROTOCOL_QINGSTORE},
	{DATALAKE_OSS_PROTOCOL_HUAWEI, DL_OSS_PROTOCOL_HUAWEI},
	{DATALAKE_OSS_PROTOCOL_KS3, DL_OSS_PROTOCOL_KS3},
	{DATALAKE_OSS_PROTOCOL_AZURE, DL_OSS_PROTOCOL_AZURE},
	{DATALAKE_OSS_PROTOCOL_GCS, DL_OSS_PROTOCOL_GCS},
};

static DLProt datalakeGetProtocol(const char* protocol)
{
	if (!protocol)
	{
		return DL_INVALID_PROTOCOL;
	}

	for (int i = 0; i < sizeof(protocol_mapping) / sizeof(protocol_mapping[0]); i++)
	{
		if (pg_strcasecmp(protocol, protocol_mapping[i].protocol_name) == 0)
		{
			return protocol_mapping[i].protocol;
		}
	}
	return DL_INVALID_PROTOCOL;
}

struct FdwCompressionOption
{
	const char *compression_name;
	CompressType compress;
};

static const struct FdwCompressionOption compression_mapping[] = {
	{DATALAKE_COMPRESS_UNCOMPRESS, UNCOMPRESS},
	{DATALAKE_COMPRESS_NONE, UNCOMPRESS},
	{DATALAKE_COMPRESS_BROTLI, BROTLI},
	{DATALAKE_COMPRESS_ZLIB, ZLIB},
	{DATALAKE_COMPRESS_GZIP, GZIP},
	{DATALAKE_COMPRESS_ZIP, ZIP},
	{DATALAKE_COMPRESS_SNAPPY, SNAPPY},
	{DATALAKE_COMPRESS_LZ4, LZ4},
	{DATALAKE_COMPRESS_ZSTD, ZSTD},
};

CompressType datalakeGetCompression(const char* compress)
{
	if (!compress)
	{
		return UNSUPPORTCOMPRESS;
	}
	for (int i = 0; i < sizeof(compression_mapping) / sizeof(compression_mapping[0]); i++)
	{
		if (pg_strcasecmp(compress, compression_mapping[i].compression_name) == 0)
		{
			return compression_mapping[i].compress;
		}
	}
	return UNSUPPORTCOMPRESS;
}

char* getOptionFromList(List *options, const char *option)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, option) == 0)
		{
			return defGetString(def);
		}
	}
	return NULL;
}

extern Datum datalake_fdw_validator(PG_FUNCTION_ARGS);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(datalake_fdw_validator);


/*
 * Helper functions
 */
static bool IsValidForeignOption(const char *option, Oid context);

static bool IsValidHdfsServerOption(const char *option, Oid context);

static bool IsValidFtpServerOption(const char *option, Oid context);

static bool IsValidPolarisServerOption(const char *option, Oid context);

static bool IsValidOSSServerOption(const char *option, Oid context);

static bool IsValidUserMappingOption(const char *option, Oid context);

static void check_foreign_option(List *options_list, Oid catalog);

static void check_server_option(List *options_list, Oid catalog);

static void checkHdfsCombin(List *options_list, Oid catalog);

static void check_user_mapping_option(List *options_list, Oid catalog);

static void checkForeignDataWrapper(ForeignDataWrapper *wrapper);

/*
 * Parser functions
 */
static void parserUri(dataLakeOptions *opt);

static void parserHdfsServerOption(dataLakeOptions *datalakeopt, List *options);

static void parserFtpServerOption(dataLakeOptions *opt, List *options);

static void parserFtpUserMappingOption(dataLakeOptions *opt, List *options);

static void parseForeignTableOptions(dataLakeOptions* opt, List *options);

static void parseHdfsUserMappingOption(dataLakeOptions* opt, List *options);

static void parseOssUserMappingOptions(dataLakeOptions* opt, List *options);

static void parseOssServerOption(dataLakeOptions* opt, List *options);

static void parsePolarisUserMappingOptions(dataLakeOptions* opt, List *options);

static void parsePolarisServerOption(dataLakeOptions* opt, List *options);

static void configurePolarisOptions(dataLakeOptions *options);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 *
 */
Datum
datalake_fdw_validator(PG_FUNCTION_ARGS)
{
	Oid			catalog = PG_GETARG_OID(1);
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));

	check_server_option(options_list, catalog);
	check_foreign_option(options_list, catalog);
	check_user_mapping_option(options_list, catalog);
	PG_RETURN_VOID();
}

void parseOssServerOption(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* server options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HOST) == 0)
		{
			opt->gopher->host = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PORT) == 0)
		{
			opt->gopher->port = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ISVIRTUAL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->useVirtualHost = true;
			}
			else
			{
				opt->gopher->useVirtualHost = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_REGION) == 0)
		{
			opt->gopher->region = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			opt->gopher->gopherType = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ISHTTPS) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->useHttps = true;
			}
			else
			{
				opt->gopher->useHttps = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_LISTV2) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->useListV2 = true;
			}
			else
			{
				opt->gopher->useListV2 = false;
			}
		}
	}
}

void parseOssUserMappingOptions(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* user mapping options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ACCESKEY) == 0)
		{
			opt->gopher->accessKey = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_SECRETKEY) == 0)
		{
			opt->gopher->secretKey = pstrdup(defGetString(def));
		}
	}
}

void parseHdfsUserMappingOption(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_USER) == 0)
		{
			opt->gopher->hdfs_user = pstrdup(defGetString(def));
		}
	}
}

void parseForeignTableOptions(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* foreign table options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FILEPATH) == 0)
		{
			opt->filePath = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FORMAT) == 0)
		{
			opt->format = datalakeGetTableFormat(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_COMPRESS) == 0)
		{
			opt->compress = datalakeGetCompression(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ENABLE_CACHE) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->enableCache = true;
			}
			else
			{
				opt->gopher->enableCache = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FILE_SIZE_LIMIT) == 0)
		{
			int64_t filesize = atoll(defGetString(def));
			if (filesize == 0 || filesize < -1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("datalake_fdw: file size limit must be greater than 0 or be -1.")));
			}
			opt->fileSizeLimit = filesize > 0 ? filesize * 1024 * 1024 : -1;
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_DATASOURCE) == 0)
		{
			opt->hiveOption->datasource = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_PARTITIONKEY) == 0)
		{
			opt->hiveOption->hivePartitionKey = datalakeSplitString2(defGetString(def), ',', '/');
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_TRANSACTIONAL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->hiveOption->transactional = true;
			}
			else
			{
				opt->hiveOption->transactional = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PARTITION_VALUE) == 0)
		{
			opt->hiveOption->specifyMaxPartitonValue = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_CLUSTER_NAME) == 0)
		{
			opt->hive_cluster_name = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_CLUSTER_NAME) == 0)
		{
			opt->hdfs_cluster_name = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_CATALOG_TYPE) == 0)
		{
			opt->catalog_type = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_SPLIT_SIZE) == 0)
		{
			opt->split_size = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_SERVER_NAME) == 0)
		{
			opt->server_name = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_TABLE_IDENTIFIER) == 0)
		{
			opt->table_identifier = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_CACHE_ENABLED) == 0)
		{
			opt->cache_enabled = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_QUERY_TYPE) == 0)
		{
			opt->query_type = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_METADATA_TABLE_ENABLE) == 0)
		{
			opt->metadata_table_enable = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_CATALOG_DEFAULT_IMPL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->set_catalog_default_impl = true;
			}
			else
			{
				opt->set_catalog_default_impl = false;
			}
		}
	}
	opt->hiveOption->partitiontable = IS_PARTITION_TABLE(opt->hiveOption->hivePartitionKey, opt->hiveOption->datasource)? true : false;
}

void checkForeignDataWrapper(ForeignDataWrapper *wrapper)
{
	if (wrapper->exec_location != FTEXECLOCATION_ALL_SEGMENTS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("datalake only support data wrapper mpp_execute set \"all segments\".")));
	}
}

dataLakeOptions *datalakeGetOptions(Oid foreigntableid)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	UserMapping *user;
	DLProt protocol;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * file_fdw doesn't have any options that can be specified there.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	user = GetUserMapping(GetUserId(), server->serverid);

	/* ForeignDataWrapper */
	checkForeignDataWrapper(wrapper);

	dataLakeOptions *opt = (dataLakeOptions*)palloc0(sizeof(dataLakeOptions));
	opt->gopher = (storageOptions*)palloc0(sizeof(storageOptions));
	opt->hiveOption = (hiveOptions*)palloc0(sizeof(hiveOptions));

	char connect_path[1024] = {0};
	DatalakeGetGopherSocketPath(connect_path);
	opt->gopher->connect_path = pstrdup(connect_path);

	char connect_plasma_path[1024] = {0};
	DatalakeGetGopherPlasmaSocketPath(connect_plasma_path);
	opt->gopher->connect_plasma_path = pstrdup(connect_plasma_path);

	char connect_worker_path[1024] = {0};
	DatalakeGetGopherMetaPath(connect_worker_path);
	opt->gopher->worker_path = pstrdup(connect_worker_path);

	opt->fileSizeLimit = 128 * 1024 * 1024;

	{
		const char *protoStr = getOptionFromList(server->options, DATALAKE_OPTION_PROTOCOL);
		protocol = datalakeGetProtocol(protoStr);
		opt->protocol = protocol;
		/*
		 * Also persist the canonical protocol string into storageOptions so
		 * the BackendRegistry (consulted by fileSystemWrapper at query time,
		 * including on segments) can dispatch on it. Without this, segments
		 * saw opt->gopher->protocol = NULL because only opt->protocol (enum)
		 * was populated by this function.
		 */
		opt->gopher->protocol = (protoStr ? pstrdup(protoStr) : NULL);
	}

	if (protocol == DL_HDFS_PROTOCOL)
	{
		parserHdfsServerOption(opt, server->options);
		parseHdfsUserMappingOption(opt, user->options);
		parseForeignTableOptions(opt, table->options);
		opt->prefix = pstrdup(opt->filePath);
	}
	else if (protocol == DL_FTP_PROTOCOL)
	{
		parserFtpServerOption(opt, server->options);
		parserFtpUserMappingOption(opt, user->options);
		parseForeignTableOptions(opt, table->options);
		opt->gopher->ftp_path = pstrdup(opt->filePath);
	}
	else
	{
		parseOssServerOption(opt, server->options);
		parseOssUserMappingOptions(opt, user->options);
		parseForeignTableOptions(opt, table->options);
		parserUri(opt);

		configurePolarisOptions(opt);
	}

	return opt;
}

List* datalakeGetCopyOptions(Oid foreigntableid)
{
	List *copyOptions = NIL;
	ForeignTable *table;
	table = GetForeignTable(foreigntableid);
	List *options = NIL;
	options = list_concat(options, table->options);
	ListCell   *lc;

	/* copy options */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORMAT) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_HEADER) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_DELIMITER) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_QUOTE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_ESCAPE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_NULL) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_ENCODING) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_NEWLINE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORCE_NOT_NULL) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORCE_NULL) == 0)
		{
			copyOptions = lappend(copyOptions, def);
		}
	}

	return copyOptions;
}

List* datalakeGetCustomOptions(Oid foreigntableid)
{
	ForeignTable *table;
	table = GetForeignTable(foreigntableid);
	List *options = NIL;
	options = list_concat(options, table->options);
	/* We don't know what the option will set up in custom format.
	So the best way is to pass the option of fdw in the past. */

	return options;
}

void datalakeGetCopyLogerrorOptions(Oid foreigntableid, int *rejectlimit,
			   bool *islimitinrows, char *logerrors)
{
	ForeignTable *table;
	table = GetForeignTable(foreigntableid);
	List *options = NIL;
	options = list_concat(options, table->options);
	ListCell   *lc;

	/* get copy log error option */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTIION_LOGERRORS) == 0)
		{
			*logerrors = *defGetString(def);
		}
		else if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTIION_REJECTLIMIT) == 0)
		{
			*rejectlimit = atoi(defGetString(def));
		}
		else if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTIION_REJECTLIMITTYPE) == 0)
		{
			char* values = defGetString(def);
			if (pg_strcasecmp(values, "row") == 0)
			{
				*islimitinrows = true;
			}
			else
			{
				*islimitinrows = false;
			}
		}
	}
}

void datalakeGetUriFromOptions(Oid foreigntableid, char** uri)
{
	ForeignTable *table;
	table = GetForeignTable(foreigntableid);
	List *options = NIL;
	options = list_concat(options, table->options);
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FILEPATH) == 0)
		{
			*uri = defGetString(def);
		}
	}
}

void parserUri(dataLakeOptions *opt)
{
	if (opt->filePath)
	{
		/* get bucket */
		char* uri = opt->filePath + 1;
		char* delimit = pstrdup("/");
		char* bucket = strstr(uri, delimit);
		int len = bucket - uri;
		if (bucket == NULL || len <= 0)
		{
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("datalake filePath %s not found bucket or "
				"For the bucket the path needs '/' to end.", opt->filePath)));
			return;
		}
		char* buf = (char*)palloc0(len + 1);
		memcpy(buf, uri, len);
		opt->gopher->bucket = buf;

		/* get prefix */
		uri = bucket + 1;
		len = strlen(uri);
		if (len <= 0)
		{
			opt->prefix = pstrdup("");
			return;
		}
		char* prefix = (char*)palloc0(len + 1);
		memcpy(prefix, uri, len);
		opt->prefix = prefix;

		pfree(delimit);
	}
}

void parserHdfsServerOption(dataLakeOptions *datalakeopt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			datalakeopt->gopher->gopherType = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_NAMENODE) == 0)
		{
			datalakeopt->gopher->hdfs_namenode_host = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_PORT) == 0)
		{
			datalakeopt->gopher->hdfs_namenode_port = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD) == 0)
		{
			datalakeopt->gopher->hdfs_auth_method = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRB_PRINCIPAL) == 0)
		{
			datalakeopt->gopher->krb_principal = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB) == 0)
		{
			datalakeopt->gopher->krb_principal_keytab = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION) == 0)
		{
			datalakeopt->gopher->hadoop_rpc_protection = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				datalakeopt->gopher->data_transfer_protocol = true;
			}
			else
			{
				datalakeopt->gopher->data_transfer_protocol = false;
			}

		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTECTION) == 0)
		{
			datalakeopt->gopher->data_transfer_protection = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				datalakeopt->gopher->is_ha_supported = true;
			}
			else
			{
				datalakeopt->gopher->is_ha_supported = false;
			}
		}
	}

	/* parser hdfs ha config */
	if (datalakeopt->gopher->is_ha_supported)
	{
		foreach(lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES) == 0)
			{
				datalakeopt->gopher->dfs_name_services = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE) == 0)
			{
				datalakeopt->gopher->dfs_ha_namenodes = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR) == 0)
			{
				datalakeopt->gopher->dfs_ha_namenode_rpc_addr = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER) == 0)
			{
				datalakeopt->gopher->dfs_client_failover = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRB_SERVICE_PRINCIPAL) == 0)
			{
				datalakeopt->gopher->krb_service_principal = pstrdup(defGetString(def));
			}
		}
	}
}

void parserFtpServerOption(dataLakeOptions *opt, List *options)
{
	ListCell	*lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* server options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			opt->gopher->gopherType = pstrdup(defGetString(def));
		}
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HOST) == 0)
		{
			opt->gopher->host = pstrdup(defGetString(def));
		}
	}
}

void parserFtpUserMappingOption(dataLakeOptions *opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* user mapping options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_USER) == 0)
		{
			opt->gopher->ftp_username = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PASSWORD) == 0)
		{
			opt->gopher->ftp_password = pstrdup(defGetString(def));
		}
	}
}

void datalakeFreeDatalakeOptions(dataLakeOptions *options)
{
	if (options->gopher)
	{
		if (options->gopher->connect_path)
		{
			pfree(options->gopher->connect_path);
			options->gopher->connect_path = NULL;
		}

		if (options->gopher->gopherType)
		{
			pfree(options->gopher->gopherType);
			options->gopher->gopherType = NULL;
		}

		if (options->gopher->bucket)
		{
			pfree(options->gopher->bucket);
			options->gopher->bucket = NULL;
		}

		if (options->gopher->protocol)
		{
			pfree(options->gopher->protocol);
			options->gopher->protocol = NULL;
		}

		if (options->gopher->accessKey)
		{
			pfree(options->gopher->accessKey);
			options->gopher->accessKey = NULL;
		}

		if (options->gopher->secretKey)
		{
			pfree(options->gopher->secretKey);
			options->gopher->secretKey = NULL;
		}

		if (options->gopher->host)
		{
			pfree(options->gopher->host);
			options->gopher->host = NULL;
		}

		if (options->gopher->region)
		{
			pfree(options->gopher->region);
			options->gopher->region = NULL;
		}

		/* hdfs */
		if (options->gopher->hdfs_namenode_host)
		{
			pfree(options->gopher->hdfs_namenode_host);
			options->gopher->hdfs_namenode_host = NULL;
		}

		if (options->gopher->hdfs_auth_method)
		{
			pfree(options->gopher->hdfs_auth_method);
			options->gopher->hdfs_auth_method = NULL;
		}

		if (options->gopher->krb_principal)
		{
			pfree(options->gopher->krb_principal);
			options->gopher->krb_principal = NULL;
		}

		if (options->gopher->krb_principal_keytab)
		{
			pfree(options->gopher->krb_principal_keytab);
			options->gopher->krb_principal_keytab = NULL;
		}

		if (options->gopher->krb5_ccname)
		{
			pfree(options->gopher->krb5_ccname);
			options->gopher->krb5_ccname = NULL;
		}

		if (options->gopher->hadoop_rpc_protection)
		{
			pfree(options->gopher->hadoop_rpc_protection);
			options->gopher->hadoop_rpc_protection = NULL;
		}

		if (options->gopher->dfs_name_services)
		{
			pfree(options->gopher->dfs_name_services);
			options->gopher->dfs_name_services = NULL;
		}

		if (options->gopher->dfs_ha_namenodes)
		{
			pfree(options->gopher->dfs_ha_namenodes);
			options->gopher->dfs_ha_namenodes = NULL;
		}

		if (options->gopher->dfs_ha_namenode_rpc_addr)
		{
			pfree(options->gopher->dfs_ha_namenode_rpc_addr);
			options->gopher->dfs_ha_namenode_rpc_addr = NULL;
		}

		if (options->gopher->dfs_client_failover)
		{
			pfree(options->gopher->dfs_client_failover);
			options->gopher->dfs_client_failover = NULL;
		}

		if (options->gopher->krb_service_principal)
		{
			pfree(options->gopher->krb_service_principal);
			options->gopher->krb_service_principal = NULL;
		}

		if (options->gopher->hdfs_user)
		{
			pfree(options->gopher->hdfs_user);
			options->gopher->hdfs_user = NULL;
		}

		pfree(options->gopher);
		options->gopher = NULL;
	}

	if (options->hiveOption)
	{
		if (options->hiveOption->hivePartitionKey)
		{
			ListCell *cell;
			foreach(cell, options->hiveOption->hivePartitionKey)
			{
				char *def = (char *) lfirst(cell);
				if (def)
				{
					pfree(def);
				}
			}
			options->hiveOption->hivePartitionKey = NIL;
		}

		if (options->hiveOption->hivePartitionValues)
		{
			ListCell *cell;
			foreach(cell, options->hiveOption->hivePartitionValues)
			{
				List *values = (List *) lfirst(cell);
				ListCell *cell2;
				foreach(cell2, values)
				{
					char *partValue = (char *) lfirst(cell2);
					if (partValue)
					{
						pfree(partValue);
					}
				}
			}
			options->hiveOption->hivePartitionValues = NIL;
		}

		if (options->hiveOption->hivePartitionConstraints)
		{
			list_free(options->hiveOption->hivePartitionConstraints);
			options->hiveOption->hivePartitionConstraints = NIL;
		}

		if (options->hiveOption->constraints)
		{
			list_free(options->hiveOption->constraints);
			options->hiveOption->constraints = NIL;
		}

		if (options->hiveOption->storePartitionInfo)
		{
			pfree(options->hiveOption->storePartitionInfo);
			options->hiveOption->storePartitionInfo = NULL;
		}

		if (options->hiveOption->orignPrefix)
		{
			pfree(options->hiveOption->orignPrefix);
			options->hiveOption->orignPrefix = NULL;
		}

		if (options->hiveOption->datasource)
		{
			pfree(options->hiveOption->datasource);
			options->hiveOption->datasource = NULL;
		}

		if (options->hiveOption->specifyMaxPartitonValue)
		{
			pfree(options->hiveOption->specifyMaxPartitonValue);
			options->hiveOption->specifyMaxPartitonValue = NULL;
		}
	}

	if (options)
	{
		if (options->filePath)
		{
			pfree(options->filePath);
			options->filePath = NULL;
		}

		if (options->prefix)
		{
			pfree(options->prefix);
			options->prefix = NULL;
		}

		if (options->hive_cluster_name)
		{
			pfree(options->hive_cluster_name);
			options->hive_cluster_name = NULL;
		}

		if (options->hdfs_cluster_name)
		{
			pfree(options->hdfs_cluster_name);
			options->hdfs_cluster_name = NULL;
		}

		if (options->client_id)
		{
			pfree(options->client_id);
			options->client_id = NULL;
		}

		if (options->client_secret)
		{
			pfree(options->client_secret);
			options->client_secret = NULL;
		}

		if (options->scope)
		{
			pfree(options->scope);
			options->scope = NULL;
		}

		if (options->polaris_server_url)
		{
			pfree(options->polaris_server_url);
			options->polaris_server_url = NULL;
		}

		if (options->polaris_server_realm)
		{
			pfree(options->polaris_server_realm);
			options->polaris_server_realm = NULL;
		}

		pfree(options);
		options = NULL;
	}
}

void check_server_option(List *options_list, Oid catalog)
{
	char *protocolStr = NULL;
	DLProt protocol;
	ListCell *cell;

	if (catalog != ForeignServerRelationId)
		return;

	protocolStr = getOptionFromList(options_list, DATALAKE_OPTION_PROTOCOL);
	if (!protocolStr)
	{
		if (getOptionFromList(options_list, DATALAKE_OPTION_POLARIS_SERVER_URL))
		{
			foreach(cell, options_list)
			{
				DefElem *def = (DefElem *) lfirst(cell);
				if (!IsValidPolarisServerOption(def->defname, catalog))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid polaris option \"%s\".", def->defname)));
				}
			}

			return;
		}
	}

	protocol = datalakeGetProtocol(protocolStr);
	switch (protocol)
	{
		case DL_INVALID_PROTOCOL:
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid protocol \"%s\". datalake support hdfs, ftp, and oss protocol"
					 " ali, cos, qs, s3, s3b, huawei, ks3, azure, gcs", protocolStr)));
			break;
		}
		case DL_HDFS_PROTOCOL:
		{
			foreach(cell, options_list)
			{
				DefElem *def = (DefElem *) lfirst(cell);
				if (!IsValidHdfsServerOption(def->defname, catalog))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid hdfs option \"%s\".",
									def->defname)));
				}
			}
			checkHdfsCombin(options_list, catalog);
			break;
		}
		case DL_FTP_PROTOCOL:
		{
			foreach(cell, options_list)
			{
				DefElem *def = (DefElem *) lfirst(cell);
				if (!IsValidFtpServerOption(def->defname, catalog))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid ftp option \"%s\".",
									def->defname)));
				}
			}
			break;
		}
		default:
		{
			foreach(cell, options_list)
			{
				DefElem *def = (DefElem *) lfirst(cell);
				if (!IsValidOSSServerOption(def->defname, catalog))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid oss option \"%s\".",
									def->defname)));
				}
			}
		}
	}
}

void check_foreign_option(List *options_list, Oid catalog)
{
	ListCell *cell;
	char *tableFmtStr;
	DLTblFmt format;
	char* compressionStr = NULL;
	CompressType compress = UNCOMPRESS;

	if (catalog != ForeignTableRelationId)
		return;

	tableFmtStr = getOptionFromList(options_list, DATALAKE_OPTION_FORMAT);
	format = datalakeGetTableFormat(tableFmtStr);
	if (format == DL_INVALID_TABLE_FORMAT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid foreign format option \"%s\". "
					 "datalake support csv, text, custom, parquet, orc, avro, iceberg, hudi.",
					 tableFmtStr)));
	}

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (!FORMAT_IS_CUSTOM(format))
		{
			if (!IsValidForeignOption(def->defname, catalog))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid foreign option \"%s\".",
								def->defname)));
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_COMPRESS) == 0)
		{
			compressionStr = defGetString(def);
			compress = datalakeGetCompression(compressionStr);
		}

		//check logerror
		if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTIION_REJECTLIMIT) == 0)
		{
			int logerror = atoi(defGetString(def));
			if (logerror < 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign option \"%s\" value %d no less than 0 is allowed.",
							def->defname, logerror)));
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTIION_REJECTLIMITTYPE) == 0)
		{
			char* values = defGetString(def);
			if (pg_strcasecmp(values, "rows") != 0 &&
				pg_strcasecmp(values, "percent") != 0 )
			{
				ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("foreign option \"%s\" 'rows' or 'percent' unknow value %s.",
							def->defname, values)));
			}
		}
	}

	if (FORMAT_IS_TEXT(format) || FORMAT_IS_CSV(format))
	{
		if (!TEXT_SUPPORT_COMPRESS(compress))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign compression options \"%s\". "
							"datalake text/csv support gzip, zip.",
							compressionStr)));
		}
	}

	if (FORMAT_IS_ORC(format))
	{
		/* just not support orc compress */
	}

	if (FORMAT_IS_PARQUET(format))
	{
		if (!PARQUET_SUPPORT_COMPRESS(compress))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign compression options \"%s\". "
							"datalake parquet support snappy, gzip, zstd, lz4.",
							compressionStr)));
		}
	}

	if (FORMAT_IS_AVRO(format))
	{
		if (!AVRO_SUPPORT_COMPRESS(compress))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign compression options \"%s\". "
							"datalake avro support snappy.",
							compressionStr)));
		}
	}
}

void check_user_mapping_option(List *options_list, Oid catalog)
{
	ListCell *cell;

	if (catalog != UserMappingRelationId)
		return;

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (!IsValidUserMappingOption(def->defname, catalog))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid user mapping option \"%s\".",
							def->defname)));
		}
	}
}

bool IsValidOSSServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_oss_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidHdfsServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_hdfs_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidFtpServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_ftp_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

static bool
IsValidPolarisServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_polaris_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidForeignOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_foreign_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidUserMappingOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_usermaping_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

void checkHdfsCombin(List *options_list, Oid catalog)
{
	ListCell *cell;
	char* hdfs_namenodes = NULL;
	char* hdfs_port = NULL;
	char* hdfs_auth_method = NULL;
	char* krb_principal = NULL;
	char* krb_principal_keytab = NULL;
	char* hadoop_rpc_protection = NULL;
	char* data_transfer_protocol = NULL;
	char* data_transfer_protection = NULL;
	bool is_ha_supported = false;
	char* dfs_nameservices = NULL;
	char* dfs_ha_namenodes = NULL;
	char* dfs_namenode_rpc_address = NULL;
	char* dfs_client_failover_proxy_provider = NULL;

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_NAMENODE) == 0)
		{
			hdfs_namenodes = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_PORT) == 0)
		{
			hdfs_port = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD) == 0)
		{
			hdfs_auth_method = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRB_PRINCIPAL) == 0)
		{
			krb_principal = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB) == 0)
		{
			krb_principal_keytab = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION) == 0)
		{
			hadoop_rpc_protection = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL) == 0)
		{
			data_transfer_protocol = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTECTION) == 0)
		{
			data_transfer_protection = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				is_ha_supported = true;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES) == 0)
		{
			dfs_nameservices = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE) == 0)
		{
			dfs_ha_namenodes = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR) == 0)
		{
			dfs_namenode_rpc_address = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER) == 0)
		{
			dfs_client_failover_proxy_provider = defGetString(def);
		}
	}

	if (pg_strcasecmp(hdfs_auth_method, DATALAKE_OPTION_HDFS_AUTH_KERBEROS) == 0)
	{
		if ((krb_principal == NULL))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs kerberos options need specify \"krb_principal\".")));
		}
		if (krb_principal_keytab == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs kerberos options need specify \"krb_principal_keytab\".")));
		}
	}
	else if (pg_strcasecmp(hdfs_auth_method, DATALAKE_OPTION_HDFS_AUTH_SIMPLE) == 0)
	{
		if ((krb_principal != NULL))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs simple options no need specify \"krb_principal\".")));
		}
		if (krb_principal_keytab != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs simple options no need specify \"krb_principal_keytab\".")));
		}
	}

	if (is_ha_supported)
	{
		if (hdfs_port != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha no need specify options \"hdfs_port\". "
						"datalake visit hdfs cluster is "
						"through dfs_namenode_rpc_address.")));
		}
		if (dfs_nameservices == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_nameservices\".")));
		}
		if (dfs_ha_namenodes == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_ha_namenodes\".")));
		}
		if (dfs_namenode_rpc_address == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_namenode_rpc_address\".")));
		}
		if (dfs_client_failover_proxy_provider == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_client_failover_proxy_provider\".")));
		}
	}

	if (data_transfer_protection != NULL &&
		pg_strcasecmp(data_transfer_protection, "authentication") != 0 &&
		pg_strcasecmp(data_transfer_protection, "integrity") != 0 &&
		pg_strcasecmp(data_transfer_protection, "privacy") != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid value \"%s\" for option \"data_transfer_protection\", "
						"valid values are: authentication, integrity, privacy.",
						data_transfer_protection)));
	}
}

void datalakeCheckValidRecordBatchOpt(dataLakeOptions *options)
{
	if (FORMAT_IS_ORC(options->format))
	{

		if (options->hiveOption->transactional)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("vectorization not support orc transactional table.")));
		}
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("vectorization only support format orc.")));
	}
}

char *datalakeGetCompressionName(CompressType compress)
{
	switch (compress) {
		case UNCOMPRESS:
			return "uncompress";
		case BROTLI:
			return "brotli";
		case ZLIB:
			return "zlib";
		case GZIP:
			return "gzip";
		case ZIP:
			return "zip";
		case SNAPPY:
			return "snappy";
		case LZ4:
			return "lz4";
		case ZSTD:
			return "zstd";
		case LZJB:
			return "lzjb";
		default:
			return "not support compression";
	}
}

static void
parsePolarisUserMappingOptions(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		/* Polaris credential options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_CLIENT_ID) == 0)
		{
			opt->client_id = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_CLIENT_SECRET) == 0)
		{
			opt->client_secret = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_SCOPE) == 0)
		{
			opt->scope = pstrdup(defGetString(def));
		}
	}
}

static void
parsePolarisServerOption(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		/* Polaris server options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_POLARIS_SERVER_URL) == 0)
		{
			opt->polaris_server_url = pstrdup(defGetString(def));
		}
		else if (pg_strcasecmp(def->defname, DATALAKE_OPTION_POLARIS_SERVER_REALM) == 0)
		{
			opt->polaris_server_realm = pstrdup(defGetString(def));
		}
	}
}

static void
configurePolarisOptions(dataLakeOptions *options)
{
	if (options->catalog_type == NULL || pg_strcasecmp(options->catalog_type, "polaris") != 0)
		return;

	if (options->server_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("datalake polaris server name is required.")));

	ForeignServer *polaris_server = GetForeignServerByName(options->server_name, true);
	if (polaris_server == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("datalake polaris server %s not found.", options->server_name)));

	UserMapping *polaris_user = GetUserMapping(GetUserId(), polaris_server->serverid);
	if (polaris_user == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("datalake polaris user %s not found.", options->server_name)));

	parsePolarisServerOption(options, polaris_server->options);
	parsePolarisUserMappingOptions(options, polaris_user->options);
}
