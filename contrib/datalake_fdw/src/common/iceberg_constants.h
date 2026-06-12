/*
 * iceberg_constants.h
 *   Unified constants for Iceberg FDW (catalog and volume)
 */

#ifndef ICEBERG_CONSTANTS_H
#define ICEBERG_CONSTANTS_H

/* ========== Catalog Config Keys ========== */
#define DATALAKEFDW_ICEBERG_KEY_SERVER_TYPE              "server_type"
#define DATALAKEFDW_ICEBERG_KEY_HIVE_METASTORE_URI       "hive_metastore_uri"
#define DATALAKEFDW_ICEBERG_KEY_AUTH_METHOD              "auth_method"
#define DATALAKEFDW_ICEBERG_KEY_WAREHOUSE_LOCATION       "warehouse_location_prefix"
#define DATALAKEFDW_ICEBERG_KEY_POLARIS_SERVER_URL       "polaris_server_url"
#define DATALAKEFDW_ICEBERG_KEY_POLARIS_SERVER_REALM     "polaris_server_realm"
#define DATALAKEFDW_ICEBERG_KEY_CLIENT_ID                "client_id"
#define DATALAKEFDW_ICEBERG_KEY_CLIENT_SECRET            "client_secret"
#define DATALAKEFDW_ICEBERG_KEY_CATALOG_NAME             "catalog_name"
#define DATALAKEFDW_ICEBERG_KEY_SCOPE                    "scope"

/* ========== Volume Config Keys ========== */
#define DATALAKEFDW_ICEBERG_KEY_VOLUME_SERVER_TYPE       "volume_server_type"
#define DATALAKEFDW_ICEBERG_KEY_VOLUME_ENDPOINT          "volume_endpoint"
#define DATALAKEFDW_ICEBERG_KEY_VOLUME_REGION            "volume_region"
#define DATALAKEFDW_ICEBERG_KEY_BUCKET_NAME              "bucket_name"
#define DATALAKEFDW_ICEBERG_KEY_PATH_STYLE_ACCESS        "path_style_access"
#define DATALAKEFDW_ICEBERG_KEY_ACCESS_KEY_ID            "access_key_id"
#define DATALAKEFDW_ICEBERG_KEY_SECRET_ACCESS_KEY        "secret_access_key"
#define DATALAKEFDW_ICEBERG_OPTION_LOCATION              "location"

/* ========== Request Keys ========== */
#define DATALAKEFDW_ICEBERG_KEY_NAME                     "name"
#define DATALAKEFDW_ICEBERG_KEY_SCHEMA                   "schema"
#define DATALAKEFDW_ICEBERG_KEY_PROPERTIES               "properties"
#define DATALAKEFDW_ICEBERG_KEY_FRAGMENTS                "fragments"
#define DATALAKEFDW_ICEBERG_KEY_FILE_PATH                "file_path"
#define DATALAKEFDW_ICEBERG_KEY_RESULT                   "result"
#define DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG            "IcebergConfig"
#define DATALAKEFDW_ICEBERG_KEY_TOTALSEGMENT             "totalSegment"
#define DATALAKEFDW_ICEBERG_KEY_SPLITSIZE                "splitSize"
#define DATALAKEFDW_ICEBERG_KEY_FILTERSTRING             "filterString"
#define DATALAKEFDW_ICEBERG_KEY_FILEIOCONFIG             "fileIOConfig"
#define DATALAKEFDW_ICEBERG_KEY_TABLE_IDENTIFIER         "tableIdentifier"
#define DATALAKEFDW_ICEBERG_KEY_DEFAULT_NAMESPACE        "public"
#define DATALAKEFDW_ICEBERG_KEY_DEFAULTFILEIO            "default"
#define DATALAKEFDW_ICEBERG_KEY_CUSTOMFILEIO             "customFileIO"
#define DATALAKEFDW_ICEBERG_KEY_GOPHERFILEIO             "gopherFileIO"
#define DATALAKEFDW_ICEBERG_KEY_IMPLCLASS                "impl_class"
#define DATALAKEFDW_ICEBERG_KEY_PROPERTIES               "properties"
#define DATALAKEFDW_ICEBERG_KEY_ICEBERG_CATALOG_CONFIG   "IcebergCatalogConfig"
#define DATALAKEFDW_ICEBERG_KEY_ICEBERG_VOLUME_CONFIG    "IcebergVolumeConfig"
#define DATALAKEFDW_ICEBERG_KEY_ICEBERG_ADDITIONALCONFIG "IcebergAdditionalConfig"
#define DATALAKEFDW_ICEBERG_KEY_BUILDIN_TABLE_EXISTS     "buildInCatalog.table_exists"
#define DATALAKEFDW_ICEBERG_KEY_METADATALOCATION         "buildInCatalog.metadata_location"
#define DATALAKEFDW_ICEBERG_KEY_DEFERRED_METADATA_LOCATION  "metadata_location"
#define DATALAKEFDW_ICEBERG_KEY_NAMESPACE                "namespace"
#define DATALAKEFDW_ICEBERG_KEY_PURGEREQUESTED           "purgeRequested"

#define DATALAKEFDW_ICEBERG_KEY_UPDATEFRAGMENTS          "updateFragments"
#define DATALAKEFDW_ICEBERG_KEY_FRAGMENTS                "fragments"
#define DATALAKEFDW_ICEBERG_KEY_REWRITTENFRAGMENTS       "rewrittenFragments"

/* ========== Schema Keys ========== */
#define DATALAKEFDW_ICEBERG_KEY_TYPE                     "type"
#define DATALAKEFDW_ICEBERG_KEY_FIELDS                   "fields"
#define DATALAKEFDW_ICEBERG_KEY_ID                       "id"
#define DATALAKEFDW_ICEBERG_KEY_REQUIRED                 "required"
#define DATALAKEFDW_ICEBERG_KEY_DOC                      "doc"
#define DATALAKEFDW_ICEBERG_KEY_SCHEMAID                 "schema-id"

/* ========== Type Values ========== */
#define DATALAKEFDW_ICEBERG_TYPE_STRUCT                  "struct"
#define DATALAKEFDW_ICEBERG_TYPE_BOOLEAN                 "boolean"
#define DATALAKEFDW_ICEBERG_TYPE_INT                     "int"
#define DATALAKEFDW_ICEBERG_TYPE_LONG                    "long"
#define DATALAKEFDW_ICEBERG_TYPE_FLOAT                   "float"
#define DATALAKEFDW_ICEBERG_TYPE_DOUBLE                  "double"
#define DATALAKEFDW_ICEBERG_TYPE_DECIMAL                 "decimal"
#define DATALAKEFDW_ICEBERG_TYPE_STRING                  "string"
#define DATALAKEFDW_ICEBERG_TYPE_DATE                    "date"
#define DATALAKEFDW_ICEBERG_TYPE_TIMESTAMP               "timestamp"
#define DATALAKEFDW_ICEBERG_TYPE_BINARY                  "binary"

/* ========== Server Type Values ========== */
#define DATALAKEFDW_ICEBERG_SERVER_BUILTIN               "builtin"

/* ========== Operation Types ========== */
#define DATALAKEFDW_ICEBERG_OP_READ                      "read"
#define DATALAKEFDW_ICEBERG_OP_APPEND                    "append"
#define DATALAKEFDW_ICEBERG_OP_UPDATE                    "update"
#define DATALAKEFDW_ICEBERG_OP_SCAN                      "scan"

/* ========== Boolean Values ========== */
#define DATALAKEFDW_ICEBERG_VALUE_TRUE                   "true"
#define DATALAKEFDW_ICEBERG_VALUE_FALSE                  "false"
#define DATALAKEFDW_ICEBERG_VALUE_NULL                   "null"

/* ========== FDW Names ========== */
#define DATALAKEFDW_ICEBERG_FDW_CATALOG                  "iceberg_catalog_fdw"
#define DATALAKEFDW_ICEBERG_FDW_VOLUME                   "iceberg_volume_fdw"

/* ========== Catalog Names ========== */
#define DATALAKEFDW_ICEBERG_CATALOG_PG                   "pg_catalog"

#endif /* ICEBERG_CONSTANTS_H */
