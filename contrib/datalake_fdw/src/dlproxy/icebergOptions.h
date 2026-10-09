#ifndef DATALAKE_ICEBERG_OPTIONS_H
#define DATALAKE_ICEBERG_OPTIONS_H

#include "src/datalake_def.h"

/* Structure for Iceberg catalog server options */
typedef struct IcebergCatalogServerOptions
{
	char *server_type;				/* DATALAKE_ICEBERG_CATALOG_SERVER_TYPE */
	char *hive_metastore_uri;		/* DATALAKE_ICEBERG_CATALOG_HIVE_METASTORE_URI */
} IcebergCatalogServerOptions;

/* Structure for Iceberg catalog user mapping options */
typedef struct IcebergCatalogUserMappingOptions
{
	char *username;					/* DATALAKE_ICEBERG_CATALOG_USERNAME */
	char *auth_method;				/* DATALAKE_ICEBERG_CATALOG_AUTH_METHOD */
	char *krb_service_principal;	/* DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL */
	char *krb_client_principal;		/* DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL */
	char *krb_client_keytab;		/* DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB */
} IcebergCatalogUserMappingOptions;

/* Structure for Iceberg foreign catalog options */
typedef struct IcebergForeignCatalogOptions
{
	char *catalog_name;			 /* DATALAKE_ICEBERG_CATALOG_NAME */
	char *default_namespace;		/* DATALAKE_ICEBERG_CATALOG_DEFAULT_NAMESPACE */
	int metadata_cache;			 /* DATALAKE_ICEBERG_CATALOG_METADATA_CACHE;
								  * minutes, 0 = no caching */
	char *warehouse_location_prefix; /* DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX */
} IcebergForeignCatalogOptions;

/* Structure for S3 volume server options */
typedef struct IcebergVolumeServerOptions
{
	char *server_type;			  /* DATALAKE_ICEBERG_VOLUME_SERVER_TYPE */
	char *endpoint;				 /* DATALAKE_ICEBERG_VOLUME_ENDPOINT */
	char *region;				   /* DATALAKE_ICEBERG_VOLUME_REGION */
	char *bucket_name;			  /* DATALAKE_ICEBERG_VOLUME_BUCKET_NAME */
	bool path_style_access;		 /* DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS */
} IcebergVolumeServerOptions;

/* Structure for S3 user mapping options */
typedef struct IcebergVolumeUserMappingOptions
{
	char *username;				 /* DATALAKE_ICEBERG_USERNAME (reused from catalog) */
	char *aws_access_key_id;		/* DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID */
	char *aws_secret_access_key;	/* DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY */
} IcebergVolumeUserMappingOptions;

/* Structure for foreign volume options */
typedef struct IcebergForeignVolumeOptions
{
	char *base_path;				/* DATALAKE_ICEBERG_VOLUME_BASE_PATH */
	bool enable_caching;			/* DATALAKE_ICEBERG_VOLUME_ENABLE_CACHING */
	bool allow_writes;				/* DATALAKE_ICEBERG_VOLUME_ALLOW_WRITES */
} IcebergForeignVolumeOptions;

/* Structure for Iceberg table options */
typedef struct IcebergTableOptions
{
	char *table_name;				/* DATALAKE_ICEBERG_TABLE_NAME */
	char *namespace_name;			/* DATALAKE_ICEBERG_TABLE_NAMESPCE_NAME (note: typo in macro) */
	char *base_location;			/* DATALAKE_ICEBERG_TABLE_BASE_LOCATION */
} IcebergTableOptions;

/* Combined structure for all Iceberg options */
typedef struct IcebergOptions
{
	/* Catalog related options */
	IcebergCatalogServerOptions catalog_server;
	IcebergCatalogUserMappingOptions catalog_user;
	IcebergForeignCatalogOptions foreign_catalog;

	/* Volume related options */
	IcebergVolumeServerOptions volume_server;
	IcebergVolumeUserMappingOptions volume_user;
	IcebergForeignVolumeOptions foreign_volume;

	/* Table options */
	IcebergTableOptions table;

} IcebergOptions;



/* Function prototypes */
extern IcebergOptions *getIcebergConfigV2Options(Oid foreigntableid);
extern void freeIcebergConfigV2Options(IcebergOptions *options);

#endif /* DATALAKE_ICEBERG_OPTIONS_H */