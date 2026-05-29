#ifndef ICEBERG_CATALOG_OPTION_H
#define ICEBERG_CATALOG_OPTION_H

#include "postgres.h"
#include "nodes/pg_list.h"

/* Structure for Iceberg catalog server options */
typedef struct IcebergCatalogServerOptions
{
	char *server_type;				/* DATALAKE_ICEBERG_CATALOG_SERVER_TYPE */
	char *server_name;				/* config file section name (e.g. hive.conf segment) */
	char *hive_metastore_uri;		/* DATALAKE_ICEBERG_CATALOG_HIVE_METASTORE_URI */
	char *polaris_server_url;		/* DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_URL */
} IcebergCatalogServerOptions;

/* Hive user mapping options */
typedef struct HiveUserMappingOptions
{
	char *username;
	char *auth_method;
	char *krb_service_principal;
	char *krb_client_principal;
	char *krb_client_keytab;
} HiveUserMappingOptions;

/* Polaris user mapping options */
typedef struct PolarisUserMappingOptions
{
	char *client_id;
	char *client_secret;
	char *scope;
} PolarisUserMappingOptions;

/* Structure for Iceberg catalog user mapping options */
typedef struct IcebergCatalogUserMappingOptions
{
	HiveUserMappingOptions hive;
	PolarisUserMappingOptions polaris;
} IcebergCatalogUserMappingOptions;

/* Structure for Iceberg foreign catalog options */
typedef struct IcebergForeignCatalogOptions
{
	char *catalog_name;			 /* DATALAKE_ICEBERG_CATALOG_NAME */
	char *default_namespace;		/* DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE */
	bool enable_metadata_cache;	 /* DATALAKE_ICEBERG_CATALOG_ENABLE_METADATA_CACHE */
	int metadata_cache_ttl;		 /* DATALAKE_ICEBERG_CATALOG_METADATA_CACHE_TTL */
	bool auto_refresh_metadata;	 /* DATALAKE_ICEBERG_CATALOG_AUTO_REFRESH_METADATA */
	char *warehouse_location_prefix; /* DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX */
	int total_segment;
	int split_size;
	char *filter_string;
} IcebergForeignCatalogOptions;

typedef struct IcebergCatalogOptions
{
	/* Catalog related options */
	IcebergCatalogServerOptions catalog_server;
	IcebergCatalogUserMappingOptions catalog_user;
	IcebergForeignCatalogOptions foreign_catalog;

} IcebergCatalogOptions;

#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE "type"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE "hive"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS "polaris"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HADOOP "hadoop"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_S3 "s3"

IcebergCatalogOptions *getIcebergCatalogOptions(const char* catalogServerName, const char* catalogName);

IcebergCatalogOptions*
getIcebergPolarisCatalogOptions(const char* catalogServerName, const char* catalogName, List* catalogOptions);

bool checkIsPolarisCatalog(const char* catalogServerName, const char* catalogName);
bool checkIsBuiltinCatalog(const char* catalogServerName, const char* catalogName);
#endif