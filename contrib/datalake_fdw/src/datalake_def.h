#ifndef DATALAKE_DEF_H
#define DATALAKE_DEF_H

#include "postgres.h"
#include "access/htup.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "commands/copy.h"
#include "common/exttable.h"
#include "common/datalake_resowner.h"

#include "src/provider/providerWrapper.h"
#include "datalake_type.h"

/* server protocl type */
#define DATALAKE_OPTION_PROTOCOL "protocol"
#define DATALAKE_OSS_PROTOCOL_ALI "ali"
#define DATALAKE_OSS_PROTOCOL_COS "cos"
#define DATALAKE_OSS_PROTOCOL_QINGSTORE "qs"
#define DATALAKE_OSS_PROTOCOL_S3 "s3"
#define DATALAKE_OSS_PROTOCOL_S3B "s3b"
#define DATALAKE_OSS_PROTOCOL_HUAWEI "huawei"
#define DATALAKE_OSS_PROTOCOL_KS3 "ks3"
#define DATALAKE_OSS_PROTOCOL_AZURE "azure"
#define DATALAKE_OSS_PROTOCOL_GCS "gcs"
#define DATALAKE_HDFS_PROTOCOL "hdfs"
#define DATALAKE_FTP_PROTOCOL "ftp"

/* server oss options */
#define DATALAKE_OPTION_HOST "host"
#define DATALAKE_OPTION_PORT "port"
#define DATALAKE_OPTION_ISVIRTUAL "isvirtual"
#define DATALAKE_OPTION_ISHTTPS "ishttps"
#define DATALAKE_OPTION_LISTV2 "listv2"
#define DATALAKE_OPTION_REGION "region"

/* server hdfs options */
#define DATALAKE_OPTION_HDFS_NAMENODE "hdfs_namenodes"
#define DATALAKE_OPTION_HDFS_PORT "hdfs_port"
#define DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD "hdfs_auth_method"
#define DATALAKE_OPTION_HDFS_KRB_PRINCIPAL "krb_principal"
#define DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB "krb_principal_keytab"
#define DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION "hadoop_rpc_protection"
#define DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL "data_transfer_protocol"
#define DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTECTION "data_transfer_protection"
#define DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED "is_ha_supported"
#define DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES "dfs_nameservices"
#define DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE "dfs_ha_namenodes"
#define DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR "dfs_namenode_rpc_address"
#define DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER "dfs_client_failover_proxy_provider"
#define DATALAKE_OPTION_HDFS_KRB_SERVICE_PRINCIPAL "krb_service_principal"

#define DATALAKE_OPTION_HDFS_AUTH_SIMPLE "simple"
#define DATALAKE_OPTION_HDFS_AUTH_KERBEROS "kerberos"

/* polaris server options */
#define DATALAKE_OPTION_POLARIS_SERVER_URL "polaris_server_url"
#define DATALAKE_OPTION_POLARIS_SERVER_REALM "polaris_server_realm"

/* user mapping options */
#define DATALAKE_OPTION_ACCESKEY "accesskey"
#define DATALAKE_OPTION_SECRETKEY "secretkey"
#define DATALAKE_OPTION_USER "user"
#define DATALAKE_OPTION_PASSWORD "password"

/* polaris credential options */
#define DATALAKE_OPTION_CLIENT_ID "client_id"
#define DATALAKE_OPTION_CLIENT_SECRET "client_secret"
#define DATALAKE_OPTION_SCOPE "scope"

/* foreign table options */
#define DATALAKE_OPTION_COMPRESS "compression"
#define DATALAKE_OPTION_FILEPATH "filepath"
#define DATALAKE_OPTION_FORMAT "format"
#define DATALAKE_OPTION_FORMAT_TEXT "text"
#define DATALAKE_OPTION_FORMAT_CSV "csv"
#define DATALAKE_OPTION_FORMAT_ORC "orc"
#define DATALAKE_OPTION_FORMAT_HUDI "hudi"
#define DATALAKE_OPTION_FORMAT_ICEBERG "iceberg"
#define DATALAKE_OPTION_FORMAT_PARQUET "parquet"
#define DATALAKE_OPTION_FORMAT_AVRO "avro"
#define DATALAKE_OPTION_FILE_SIZE_LIMIT "filesizelimit"
#define DATALAKE_OPTION_ENABLE_CACHE "enablecache"
#define DATALAKE_OPTION_HIVE_DATASOURCE "datasource"
#define DATALAKE_OPTION_HIVE_PARTITIONKEY "partitionkeys"
#define DATALAKE_OPTION_HIVE_TRANSACTIONAL "transactional"
#define DATALAKE_OPTION_HIVE_CLUSTER_NAME "hive_cluster_name"
#define DATALAKE_OPTION_HDFS_CLUSTER_NAME "hdfs_cluster_name"
#define DATALAKE_OPTION_PARTITION_VALUE "partitionvalue"

/* foreign table custom options */
#define DATALAKE_OPTION_FORMAT_CUSTOM "custom"
#define DATALAKE_OPTION_FORMAT_FORMATTER "formatter"

/* foreign table options compression */
#define DATALAKE_COMPRESS_UNCOMPRESS "uncompress"
#define DATALAKE_COMPRESS_NONE "none"
#define DATALAKE_COMPRESS_SNAPPY "snappy"
#define DATALAKE_COMPRESS_GZIP "gzip"
#define DATALAKE_COMPRESS_ZSTD "zstd"
#define DATALAKE_COMPRESS_LZ4 "lz4"
#define DATALAKE_COMPRESS_ZIP "zip"
#define DATALAKE_COMPRESS_BROTLI "brotli"
#define DATALAKE_COMPRESS_ZLIB "zlib"

/* copy options */
#define DATALAKE_COPY_OPTION_FORMAT "format"
#define DATALAKE_COPY_OPTION_HEADER "header"
#define DATALAKE_COPY_OPTION_DELIMITER "delimiter"
#define DATALAKE_COPY_OPTION_QUOTE "quote"
#define DATALAKE_COPY_OPTION_ESCAPE "escape"
#define DATALAKE_COPY_OPTION_NULL "null"
#define DATALAKE_COPY_OPTION_ENCODING "encoding"
#define DATALAKE_COPY_OPTION_NEWLINE "newline"
#define DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS "fill_missing_fields"
#define DATALAKE_COPY_OPTION_FORCE_NOT_NULL "force_not_null"
#define DATALAKE_COPY_OPTION_FORCE_NULL "force_null"

/* copy log error */
#define DATALAKE_COPY_OPTIION_LOGERRORS "logerrors"
#define DATALAKE_COPY_OPTIION_REJECTLIMIT "rejectlimit"
#define DATALAKE_COPY_OPTIION_REJECTLIMITTYPE "rejectlimittype"

/* custom options */
#define DATALAKE_CUSTOM_OPTION_LINE_DELIM "line_delim"
#define DATALAKE_CUSTOM_OPTION_ENTRY_DELIM "entry_delim"
#define DATALAKE_CUSTOM_OPTION_TAIL_DELIM "tail_delim"
#define DATALAKE_CUSTOM_OPTION_FIX_FLAG "fix_flag"

/* iceberg & hudi options */
#define DATALAKE_OPTION_TABLE_IDENTIFIER "table_identifier"
#define DATALAKE_OPTION_SERVER_NAME "server_name"
#define DATALAKE_OPTION_CATALOG_TYPE "catalog_type"
#define DATALAKE_OPTION_SPLIT_SIZE "split_size"
#define DATALAKE_OPTION_CACHE_ENABLED "cache_enabled"
#define DATALAKE_OPTION_QUERY_TYPE "query_type"
#define DATALAKE_OPTION_METADATA_TABLE_ENABLE "metadata_table_enable"
#define DATALAKE_OPTION_CATALOG_DEFAULT_IMPL "catalog_default_impl"


#define FORMAT_IS_CSV(format) (format == DL_CSV_TABLE)

#define FORMAT_IS_TEXT(format) (format == DL_TEXT_TABLE)

#define FORMAT_IS_ORC(format) (format == DL_ORC_TABLE)

#define FORMAT_IS_PARQUET(format) (format == DL_PARQUET_TABLE)

#define FORMAT_IS_AVRO(format) (format == DL_AVRO_TABLE)

#define FORMAT_IS_HUDI(format) (format == DL_HUDI_TABLE)

#define FORMAT_IS_ICEBERG(format) (format == DL_ICEBERG_TABLE)

#define FORMAT_IS_CUSTOM(format) (format == DL_CUSTOM_TABLE)

#define PROTOCOL_IS_HDFS(protocol) (protocol == DL_HDFS_PROTOCOL)

#define PROTOCOL_IS_S3(protocol) (protocol == DL_OSS_PROTOCOL_S3A)

#define PROTOCOL_IS_OSS(protocol) (protocol == DL_OSS_PROTOCOL_ALI || \
	protocol == DL_OSS_PROTOCOL_COS || \
	protocol == DL_OSS_PROTOCOL_QINGSTORE || \
	protocol == DL_OSS_PROTOCOL_S3AV2 || \
	protocol == DL_OSS_PROTOCOL_HUAWEI || \
	protocol == DL_OSS_PROTOCOL_KS3 || \
	protocol == DL_OSS_PROTOCOL_AZURE || \
	protocol == DL_OSS_PROTOCOL_GCS || \
	protocol == DL_OSS_PROTOCOL_S3A)

#define PARQUET_SUPPORT_COMPRESS(compress) (compress == UNCOMPRESS || \
	compress == SNAPPY || compress == GZIP || \
	compress == ZSTD || compress == LZ4)

#define TEXT_SUPPORT_COMPRESS(compress) (compress == UNCOMPRESS || \
	compress == ZIP || compress == GZIP)

#define AVRO_SUPPORT_COMPRESS(compress) (compress == UNCOMPRESS || \
	compress == SNAPPY)

#define IS_PARTITION_TABLE(partitionkey, datasource) \
	(partitionkey != NULL) && (datasource != NULL) \





/* iceberg table options */
#define DATALAKE_ICEBERG_TABLE_NAME "table_name"
#define DATALAKE_ICEBERG_TABLE_NAMESPCE_NAME "namespace_name"
#define DATALAKE_ICEBERG_TABLE_BASE_LOCATION "base_location"

/*
 * datalake_fdw private BeginForeignModify flag.
 * Enable QE-local metadata collection for vacuum rewrite path.
 */
#define DATALAKE_FDW_EFLAG_COLLECT_QE_METADATA 0x40000000

enum datalakePrivatePartitionDataIndex
{
	PrivatePartionString = 0,
	PrivatePartionStringLength,
};

enum datalakePrivatePartitionIndex
{
	PrivatePartitionData = 2,
	/* hive partition fragment lists */
	PrivatePartitionFragmentLists
};

enum datalakeFdwScanPrivateIndex
{
	FdwScanHdfsConfig,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* List of fragments to be processed by the segments */
	FdwScanPrivateFragmentList,
};

enum FdwModifyPrivateIndex
{
	FdwModifyFileDir,
	/*
	 * Iceberg UPDATE/DELETE only: complete fragment list from the planner
	 * (output of datalakeGetExternalFragmentList()).  Dispatched to every
	 * QE so the global file-ID map can be populated even on writer-only
	 * QEs whose slice contains no Iceberg ForeignScan (issue #333).
	 * NIL for INSERT and for non-Iceberg formats.
	 */
	FdwModifyAllFragments,
};

/*
 * Structure to store the datalake options */
typedef struct pg_HdfsHAConfig
{
    char *key;
    char *value;
}pg_HdfsHAConfig;

typedef struct storageOptions
{
	/* Gopher config */
	char*	worker_path;
	char*	connect_path;
	char*	connect_plasma_path;
	char*	gopherType;
	char*	bucket;
	bool	useVirtualHost;
	bool	useHttps;
	bool	useListV2;
	char*	protocol;
	char*	accessKey;
	char*	secretKey;
	int		port;
	char*	host;
	bool	enableCache;
	char*	region;
	/* hdfs config */
	char* 	hdfs_namenode_host;
	int 	hdfs_namenode_port;
	char*	hdfs_auth_method;
	char*	krb_principal;
	char*	krb_principal_keytab;
	char*	krb5_ccname;
	char*	hadoop_rpc_protection;
	bool	data_transfer_protocol;
	/* SASL QOP for DataNode reads/writes: authentication/integrity/privacy */
	char*	data_transfer_protection;
	char*	hdfs_user;

	/* hdfs ha config  */
	bool	is_ha_supported;
	char*	dfs_name_services;
	char*	dfs_ha_namenodes;
	char*	dfs_ha_namenode_rpc_addr;
	char*	dfs_client_failover;
	char*	krb_service_principal;
	int		hdfs_ha_configs_num;

	/* ftp config */
	char*	ftp_path;
	char*	ftp_username;
	char*	ftp_password;

	/* database install dir */
	// database_install_dir for datalake_agent load libgopherClient.so used
	char*	database_install_dir;
}storageOptions;

/* Backward-compatible alias: legacy callers still reference gopherOptions. */
typedef storageOptions gopherOptions;

typedef struct hiveOptions
{
	/* hive partition table key */
	List	*hivePartitionKey;
	/* hive partition table values */
	List	*hivePartitionValues;
	/* store json partition values for segment, seg used it need parse json */
	char	*storePartitionInfo;
	/* store json partition values buffer size */
	int		storePartitionInfoLen;
	/* hive orc transaction table */
	bool	transactional;
	/* partition constraints */
	List 	*hivePartitionConstraints;
	List 	*attNums;
	List 	*constraints;
	int 	curPartition;
	char 	*orignPrefix;
	/* hive table name */
	char	*datasource;
	/* whether hive table is partition table */
	bool	partitiontable;
	char	*specifyMaxPartitonValue;
}hiveOptions;

typedef struct dataLakeOptions
{
	DLProt		protocol;
	storageOptions* gopher;
	/* sync hive options */
	hiveOptions* hiveOption;
	char		*hive_cluster_name;
	char		*hdfs_cluster_name;
	/* get FOREIGN TABLE option filePath */
	char		*filePath;
	DLTblFmt	format;
	CompressType compress;
	/*
	 * Parquet write compression level for codecs that support it (zstd, gzip).
	 * Values <= 0 mean "unset" and the writer uses the codec's default level.
	 */
	int			compressLevel;
	/*
	 * Iceberg PARTITION BY columns (comma-separated names, declaration order).
	 * NULL/empty for unpartitioned tables; consumed by the fanout writer.
	 */
	char		*partition_by;
	/* parser filePath get prefix */
	char		*prefix;
	int64_t		fileSizeLimit;
	bool		vectorization;
	/* iceberg & hudi options */
	char		*table_identifier;
	char		*server_name;
	char		*catalog_type;
	int			split_size;
	char		*cache_enabled;
	char		*query_type;
	char		*metadata_table_enable;
	char		*client_id;
	char		*client_secret;
	char		*scope;
	char		*polaris_server_url;
	char		*polaris_server_realm;
	/* iceberg catalog default impl */
	bool		set_catalog_default_impl;
	int			nJunkInfo;

	/*
	 * Time travel: the snapshot schema's real Iceberg field-ids, parallel to
	 * the scan tuple descriptor (entry i belongs to attribute i+1).  NULL for
	 * ordinary scans.  When set, the parquet reader matches data-file columns
	 * by these ids instead of deriving one from the attribute number -- a
	 * snapshot whose history holds a DROP COLUMN has holes in its id space,
	 * so a column's position is NOT its id (see iceberg_snapshot_scan).
	 */
	int		   *iceberg_snapshot_field_ids;
	int			n_iceberg_snapshot_field_ids;
} dataLakeOptions;

typedef struct dataLakeFdwPlanState
{
	List	   *retrieved_attrs;
	Bitmapset  *attrs_used;
}dataLakeFdwPlanState;

typedef struct dataLakeCopyState
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate_scan;
	CopyState	cstate_modify;
#else
	CopyFromState	cstate_scan;
	CopyToState		cstate_modify;
#endif
}dataLakeCopyState;

typedef struct dataLakeCustomState
{
	datalake_exttable_fdw_state *fdw_state;
	DatalakeExternalInsertDesc insert_state;
} dataLakeCustomState;

typedef struct dataLakeModifyState
{
	TupleTableSlot	*us_slot;
	providerWrapper	us_provider;
	AttrNumber		us_ctid_no;
} dataLakeModifyState;

/*
 * Execution state of a foreign scan using datalake_fdw.
 */
typedef struct dataLakeFdwScanState
{
	dataLakeOptions 		*options;
	providerWrapper 		provider;
	Relation				rel;
	List 					*fragments;
	Bitmapset  				*attrs_used;     /* attributes actually used in query */
	List					*retrieved_attrs;
	MemoryContext			rowcontext;
	MemoryContext			initcontext;
	List  					*quals;
	dataLakeCopyState		cstate;
	List					*selected_segments;
	dataLakeCustomState 	customState;
	datalake_context_handle_t *datalake_handle_t;
	TupleDesc				scan_tupdesc;
	CmdType					cmd;
	dataLakeModifyState		*modify_state;
	bool					collect_qe_metadata;	/* QE local meta collection */
	List					*local_meta_list;		/* QE: collected FileFragment list */

	/*
	 * Fast-path scan cache: direct pointer to innermost row reader,
	 * bypassing all C++ provider layers per row.  Set after first
	 * successful read in iterateScanStatus().
	 */
	void					*fastScanReader;	/* DatalakeRowReader* */
} dataLakeFdwScanState;

/* iceberg update */
#define DATALAKE_ICEBERG_JUNK_FILE_OFFSET (0)
#define DATALAKE_ICEBERG_JUNK_POS_OFFSET (1)
#define DATALAKE_ICEBERG_JUNK_NUM (2)
#define DATALAKE_ICEBERG_SYSATT_NUM (2)
typedef struct IcebergJunkInfo
{
	AttrNumber	attno;
	char		*name;
	Oid			type;
} IcebergJunkInfo;

extern IcebergJunkInfo datalake_iceberg_junk_info[DATALAKE_ICEBERG_JUNK_NUM];

typedef struct IcebergFileIndexMap IcebergFileIndexMap;  /* Forward declaration for Iceberg file index */

/* Global file index map for Iceberg update/delete operations */
extern IcebergFileIndexMap *datalake_iceberg_file_index_map;

/* GUC variables */
extern char *datalake_agent_server_url;

/*
 * Global reference to the full fragment list from BeginForeignScan.
 * Used by BeginForeignModify to eagerly populate the file index map with
 * ALL files across all segments, ensuring globally consistent file IDs.
 * This is needed because Redistribute Motion can send rows from one segment
 * to another, and the file ID encoded in ctid must be valid on any segment.
 */
extern List *datalake_iceberg_all_fragments;

typedef struct datalakeFragmentData
{
	char* filePath;
	int64_t length;
	bool directory;
}datalakeFragmentData;

typedef struct IcebergTableStatistics
{
	int64		recordCount;
	int64		bytesInDataFile;
} IcebergTableStatistics;

#endif							/* DATALAKE_DEF_H */
