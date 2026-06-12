#ifndef ICEBERG_VOLUME_OPTION_H
#define ICEBERG_VOLUME_OPTION_H

#include "postgres.h"

/* Structure for S3 volume server options */
typedef struct IcebergVolumeServerOptions
{
	char *server_type;			  /* DATALAKE_ICEBERG_VOLUME_SERVER_TYPE */
	char *server_name;			  /* config file section name (e.g. s3.conf segment) */
	char *endpoint;				 /* DATALAKE_ICEBERG_VOLUME_ENDPOINT */
	char *region;				   /* DATALAKE_ICEBERG_VOLUME_REGION */
	char *bucket_name;			  /* DATALAKE_ICEBERG_VOLUME_BUCKET_NAME */
	bool path_style_access;		 /* DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS */
	bool path_style_access_set;	 /* user actually wrote path_style_access */
	
	/* AWS specific options */
	char *role_arn;
	char *external_id;
	char *user_arn;
	char *current_kms_key;
	char *allowed_kms_keys;
	char *sts_endpoint;
	bool sts_unavailable;
	char *endpoint_internal;
	
	/* Azure specific options */
	char *tenant_id;
	char *multi_tenant_app_name;
	char *consent_url;
	bool hierarchical;
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
	char *fileIOConfig;
	char *table_identifier;			/* table identifier(polaris catalog.namespace.tablename) */
} IcebergForeignVolumeOptions;

typedef struct IcebergVolumeOptions
{
	/* Volume related options */
	IcebergVolumeServerOptions volume_server;
	IcebergVolumeUserMappingOptions volume_user;
	IcebergForeignVolumeOptions foreign_volume;
} IcebergVolumeOptions;

IcebergVolumeOptions* getIcebergVolumeOptions(const char* volumeServerName, const char* volumeName);
char* buildVolumeBasePath(IcebergVolumeOptions *volumeOption);
#endif