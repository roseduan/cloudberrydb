#include "iceberg_volume_option.h"
#include "src/common/parser_option.h"
#include "src/common/iceberg_constants.h"
#include "postgres.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "cdb/cdbutil.h"
#include "utils/syscache.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "utils/lsyscache.h"
#include "catalog/pg_foreign_volume.h"
#include "utils/array.h"
#include "access/reloptions.h"
#include "iceberg_volume_option.h"

/* S3 volume server options */
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE "type"
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3 "s3"
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3V2 "s3v2"
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_ABFSS "abfss"
#define DATALAKE_ICEBERG_VOLUME_ENDPOINT "endpoint"
#define DATALAKE_ICEBERG_VOLUME_REGION "region"
#define DATALAKE_ICEBERG_VOLUME_BUCKET_NAME "bucket_name"
#define DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS "path_style_access"

/* hdfs volume server options */
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS "hdfs"

/* Polaris aws server option */
#define DATALAKE_ICEBERG_VOLUME_ROLE_ARN "role_arn"
#define DATALAKE_ICEBERG_VOLUME_EXTERNAL_ID "external_id"
#define DATALAKE_ICEBERG_VOLUME_USER_ARN "user_arn"
#define DATALAKE_ICEBERG_VOLUME_CURRENT_KMS_KEY "current_kms_key"
#define DATALAKE_ICEBERG_VOLUME_ALLOWED_KMS_KEYS "allowed_kms_keys"
#define DATALAKE_ICEBERG_VOLUME_STS_ENDPOINT "sts_endpoint"
#define DATALAKE_ICEBERG_VOLUME_STS_UNAVAILABLE "sts_unavailable"
#define DATALAKE_ICEBERG_VOLUME_ENDPOINT_INTERNAL "endpoint_internal"

/* Polaris azure server option */
#define DATALAKE_ICEBERG_VOLUME_TENANT_ID "tenant_id"
#define DATALAKE_ICEBERG_VOLUME_MULTI_TENANT_APP_NAME "multi_tenant_app_name"
#define DATALAKE_ICEBERG_VOLUME_CONSENT_URL "consent_url"
#define DATALAKE_ICEBERG_VOLUME_HIERARCHICAL "hierarchical"

/* S3 user mapping options */
#define DATALAKE_ICEBERG_VOLUME_USERNAME "username"
#define DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID "access_key_id"
#define DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY "secret_access_key"

/* Foreign volume options */
#define DATALAKE_ICEBERG_VOLUME_BASE_PATH "base_path"
#define DATALAKE_ICEBERG_VOLUME_ENABLE_CACHING "enable_caching"
#define DATALAKE_ICEBERG_VOLUME_ALLOW_WRITES "allow_writes"
#define DATALAKE_ICEBERG_CATALOG_FILEIOCONFIG "file_io_config"
#define DATALAKE_ICEBERG_VOLUME_TABLE_IDENTIFIER "table_identifier"

static void parseIcebergVolumeServerOptions(IcebergVolumeServerOptions *options, List *server_options)
{
    options->server_type = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE);
    options->server_name = getStringOption(server_options, "server_name");
    options->endpoint = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_ENDPOINT);
    options->region = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_REGION);
    options->bucket_name = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_BUCKET_NAME);
    options->path_style_access = getBoolOptionEx(server_options, DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS, false,
                                                 &options->path_style_access_set);

    /*
     * Reject the deprecated s3a / s3av2 aliases at OPTION parse time. These
     * names were a leak of Hadoop's fs.s3a URI scheme into the volume_server
     * type vocabulary; the gopher native client now accepts plain s3 / s3v2
     * directly, and everywhere downstream (Java parser, JSON wire shape)
     * assumes s3 / s3v2 only.
     */
    if (options->server_type != NULL &&
        (pg_strcasecmp(options->server_type, "s3a") == 0 ||
         pg_strcasecmp(options->server_type, "s3av2") == 0)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("volume server type '%s' is no longer supported",
                        options->server_type),
                 errhint("Use 'type=%s' instead.",
                         pg_strcasecmp(options->server_type, "s3a") == 0
                             ? "s3" : "s3v2")));
    }

    if (options->server_type != NULL && pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3) == 0) {
        /* AWS specific options */
        options->role_arn = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_ROLE_ARN);
        options->external_id = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_EXTERNAL_ID);
        options->user_arn = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_USER_ARN);
        options->current_kms_key = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_CURRENT_KMS_KEY);
        options->allowed_kms_keys = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_ALLOWED_KMS_KEYS);
        options->sts_endpoint = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_STS_ENDPOINT);
        options->sts_unavailable = getBoolOption(server_options, DATALAKE_ICEBERG_VOLUME_STS_UNAVAILABLE, false);
        options->endpoint_internal = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_ENDPOINT_INTERNAL);
    }
    else if (options->server_type != NULL && pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_ABFSS) == 0) {
        /* Azure specific options */
        options->tenant_id = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_TENANT_ID);
        options->multi_tenant_app_name = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_MULTI_TENANT_APP_NAME);
        options->consent_url = getStringOption(server_options, DATALAKE_ICEBERG_VOLUME_CONSENT_URL);
        options->hierarchical = getBoolOption(server_options, DATALAKE_ICEBERG_VOLUME_HIERARCHICAL, false);
    }
    else if (options->server_type != NULL && pg_strcasecmp(options->server_type, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS) == 0) {
        /*
         * HDFS specific options. Option names match the datalake_fdw hdfs
         * SERVER options and the gphdfs.conf keys. All optional: anything
         * left NULL falls back to the server_name conf section on the agent
         * side (SQL OPTIONS win per key).
         */
        options->hdfs_namenodes = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_HDFS_NAMENODES);
        options->hdfs_port = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_HDFS_PORT);
        options->hdfs_auth_method = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_HDFS_AUTH_METHOD);
        options->krb_principal = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_KRB_PRINCIPAL);
        options->krb_principal_keytab = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_KRB_PRINCIPAL_KEYTAB);
        options->krb_service_principal = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_KRB_SERVICE_PRINCIPAL);
        options->hadoop_rpc_protection = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_HADOOP_RPC_PROTECTION);
        options->data_transfer_protocol = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DATA_TRANSFER_PROTOCOL);
        options->data_transfer_protection = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DATA_TRANSFER_PROTECTION);
        options->is_ha_supported = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_IS_HA_SUPPORTED);
        options->dfs_nameservices = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DFS_NAMESERVICES);
        options->dfs_ha_namenodes = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DFS_HA_NAMENODES);
        options->dfs_namenode_rpc_address = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DFS_NAMENODE_RPC_ADDRESS);
        options->dfs_client_failover_proxy_provider = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DFS_CLIENT_FAILOVER_PROXY_PROVIDER);
        options->dfs_client_use_datanode_hostname = getStringOption(server_options, DATALAKEFDW_ICEBERG_KEY_DFS_CLIENT_USE_DATANODE_HOSTNAME);
    }
}

static void parseIcebergVolumeUserMappingOptions(IcebergVolumeUserMappingOptions *options, List *user_options)
{
    options->username = getStringOption(user_options, DATALAKE_ICEBERG_VOLUME_USERNAME);
    options->aws_access_key_id = getStringOption(user_options, DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID);
    options->aws_secret_access_key = getStringOption(user_options, DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY);

    /*
     * Diagnostic trace for the OSS credential / connection chain:
     *   C OPTIONS parse  -> JSON volumeConfig  -> Java VolumeInfo
     *   -> resolver gopher.*  -> emitS3Inline  -> GopherFileIO ctor
     *   -> GopherClientFactories SWIG  -> gophermeta OssWorker.
     *
     * Stays at DEBUG: silent in production, recoverable by bumping
     * client_min_messages / log_min_messages to DEBUG1 + dlagent logger to
     * DEBUG. Kept after the bug hunt because the same drop signature
     * (gophermeta cache poisoning, dropped resolver props) is easy to hit
     * again and walking every layer otherwise costs a full rebuild cycle.
     */
    elog(DEBUG1, "[trace_ak] step2 parseVolUserMap: access_key_id len=%d, secret_access_key len=%d",
         options->aws_access_key_id ? (int) strlen(options->aws_access_key_id) : -1,
         options->aws_secret_access_key ? (int) strlen(options->aws_secret_access_key) : -1);
}

static void parseIcebergForeignVolumeOptions(IcebergForeignVolumeOptions *options, List *foreign_options)
{
    options->base_path = getStringOption(foreign_options, DATALAKE_ICEBERG_VOLUME_BASE_PATH);
    options->enable_caching = getBoolOption(foreign_options, DATALAKE_ICEBERG_VOLUME_ENABLE_CACHING, false);
    options->allow_writes = getBoolOption(foreign_options, DATALAKE_ICEBERG_VOLUME_ALLOW_WRITES, false);
    options->fileIOConfig = getStringOption(foreign_options, DATALAKE_ICEBERG_CATALOG_FILEIOCONFIG);
    options->table_identifier = getStringOption(foreign_options, DATALAKE_ICEBERG_VOLUME_TABLE_IDENTIFIER);
}

IcebergVolumeOptions* getIcebergVolumeOptions(const char* volumeServerName, const char* volumeName)
{

    IcebergVolumeOptions *options = (IcebergVolumeOptions *) palloc0(sizeof(IcebergVolumeOptions));

    /* Get foreign server */
    ForeignServer *server = GetForeignServerByName(volumeServerName, false);

    /* Get user mapping */
    UserMapping *user = GetUserMapping(GetUserId(), server->serverid);

    ForeignVolume* fvolume = GetForeignVolumeByName(volumeServerName, volumeName, false);

    parseIcebergVolumeServerOptions(&options->volume_server, server->options);
    parseIcebergVolumeUserMappingOptions(&options->volume_user, user->options);
    parseIcebergForeignVolumeOptions(&options->foreign_volume, fvolume->options);

    return options;
}

/*
 * Build the volume-level base path URI for Iceberg builtin catalog.
 *
 * Build the volume base path URI from volume options.
 * For object storage (s3a, s3, gs, ...): returns "<type>://bucket_name/base_path/"
 * For HDFS: returns "hdfs:///base_path/"
 * Returns a palloc'd string.
 */
char*
buildVolumeBasePath(IcebergVolumeOptions *volumeOption)
{
    StringInfoData result;
    const char *server_type;
    const char *bucket;
    const char *base_path;
    bool        is_hdfs;

    Assert(volumeOption != NULL);

    server_type = volumeOption->volume_server.server_type;
    bucket = volumeOption->volume_server.bucket_name;
    base_path = volumeOption->foreign_volume.base_path;

    if (server_type == NULL || *server_type == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("volume server type is not specified")));

    is_hdfs = (pg_strcasecmp(server_type, "hdfs") == 0);

    initStringInfo(&result);

    if (is_hdfs)
    {
        /*
         * HDFS path: hdfs://<base_path>/
         * No bucket concept; base_path is the warehouse directory.
         */
        appendStringInfoString(&result, "hdfs://");

        if (base_path != NULL && *base_path != '\0')
        {
            /* Ensure leading slash */
            if (*base_path != '/')
                appendStringInfoChar(&result, '/');
            appendStringInfoString(&result, base_path);
        }
    }
    else
    {
        /*
         * Object storage: <scheme>://bucket/base_path/
         *
         * Map the user-facing volume type to a Hadoop URI scheme:
         *   s3 / s3v2 → s3a (Hadoop only ships the fs.s3a:// connector)
         *   others    → pass through as-is
         *
         * This rewrite is purely the Hadoop URI scheme convention; the
         * gopher UFS type (sent to iceberg-gopher) is set elsewhere to
         * the plain user-facing value (s3, s3v2, ...).
         */
        const char *scheme = server_type;
        if (pg_strcasecmp(server_type, "s3") == 0 ||
            pg_strcasecmp(server_type, "s3v2") == 0)
            scheme = "s3a";

        if (bucket == NULL || *bucket == '\0')
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("bucket_name is required for volume server type \"%s\"",
                            server_type)));

        appendStringInfo(&result, "%s://%s", scheme, bucket);

        if (base_path != NULL && *base_path != '\0')
        {
            const char *clean = base_path;

            while (*clean == '/')
                clean++;

            if (*clean != '\0')
                appendStringInfo(&result, "/%s", clean);
        }
    }

    /* Ensure trailing slash */
    if (result.len == 0 || result.data[result.len - 1] != '/')
        appendStringInfoChar(&result, '/');

    return result.data;
}
