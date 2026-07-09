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

	/*
	 * HDFS specific options. Names match the datalake_fdw hdfs SERVER
	 * options (datalake_def.h) and the gphdfs.conf keys -- one vocabulary
	 * everywhere. Booleans stay strings here so absence (NULL) is
	 * distinguishable from an explicit value; the agent merges SQL options
	 * over the server_name conf section per key.
	 */
	char *hdfs_namenodes;			/* host or host:port; HA: nameservice name */
	char *hdfs_port;
	char *hdfs_auth_method;			/* simple / kerberos */
	char *krb_principal;
	char *krb_principal_keytab;
	char *krb_service_principal;
	char *hadoop_rpc_protection;
	char *data_transfer_protocol;	/* "true" / "false" */
	char *data_transfer_protection;	/* authentication / integrity / privacy */
	char *is_ha_supported;			/* "true" / "false" */
	char *dfs_nameservices;
	char *dfs_ha_namenodes;
	char *dfs_namenode_rpc_address;
	char *dfs_client_failover_proxy_provider;
	char *dfs_client_use_datanode_hostname;	/* "true" / "false" */
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