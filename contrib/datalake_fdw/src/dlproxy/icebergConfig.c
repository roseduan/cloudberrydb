/*
 * dlproxy iceberg config JSON emitter (legacy protocol).
 *
 * Emits the historical top-level shape
 *   { "gopher": { ...pre-translated gopher.* fields... },
 *     "iceberg_config_version": "v1",
 *     "set_catalog_default_impl": <bool> }
 * which the dlagent accepts in BaseConfigurationFactory.initIcebergConfigFormJson
 * via a legacy-shape branch.
 *
 * The newer iceberg REST endpoints (see iceberg_catalog_fdw.c) emit the
 * nested IcebergConfig schema and connection info travels through
 * IcebergVolumeConfig, translated to gopher.* keys server-side by
 * GopherPropertiesResolver. This file pre-translates client-side because
 * the dlproxy protocol predates that resolver and the wire shape cannot be
 * changed without coordinating with deployed gphadoop servers.
 *
 * When deprecating dlproxy, the agent-side legacy branch in
 * initIcebergConfigFormJson and this whole file can be removed together.
 */
#include "icebergConfig.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "src/datalake_option.h"
#include <jansson.h>

/* Mapping structure for Gopher config keys to gopherOptions fields */
typedef struct GopherConfigMapping
{
    const char* config_key;     /* Key in configuration */
    const char* json_key;       /* Key in JSON output */
    enum {
        GOPHER_TYPE_STRING,     /* String type */
        GOPHER_TYPE_INT,        /* Integer type */
        GOPHER_TYPE_BOOL        /* Boolean type */
    } value_type;               /* Type of the value */
    size_t struct_offset;       /* Offset of the field in gopherOptions struct */
} GopherConfigMapping;

/*
 * Marker for mapping entries with no gopherOptions field.  Zero cannot serve
 * as the marker: worker_path is the FIRST member of gopherOptions, so its
 * offsetof() is 0, and using 0 as the sentinel silently dropped
 * gopher.worker_path from every emitted config -- the agent then failed to
 * create its GopherClient with "Gopher Worker path is required" (issue #844).
 */
#define GOPHER_FIELD_NONE ((size_t) -1)

/* Mapping table for Gopher configuration */
static const GopherConfigMapping gopher_config_mapping[] = {
    /* Basic Gopher configuration */
    {"gopher.worker_path", "worker_path", GOPHER_TYPE_STRING, offsetof(gopherOptions, worker_path)},
    {"gopher.connect_path", "connect_path", GOPHER_TYPE_STRING, offsetof(gopherOptions, connect_path)},
    {"gopher.connect_plasma_path", "connect_plasma_path", GOPHER_TYPE_STRING, offsetof(gopherOptions, connect_plasma_path)},
    {"gopher.ufs_type", "ufs_type", GOPHER_TYPE_STRING, offsetof(gopherOptions, gopherType)},
    {"gopher.uriPrefix", "uri_prefix", GOPHER_TYPE_STRING, offsetof(gopherOptions, protocol)},
    /*
     * cache_strategy / gopher_mode / log_level / liboss2_log_severity have no
     * gopherOptions source field.  They used to alias gopherType, which
     * stamped the protocol name (e.g. "s3") into all four keys; emit nothing
     * and let the gopher library defaults apply instead.
     */
    {"gopher.cache_strategy", "cache_strategy", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.gopher_mode", "gopher_mode", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.logLevel", "log_level", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.liboss2LogSeverity", "liboss2_log_severity", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.cache_predict_num", "cache_predict_num", GOPHER_TYPE_INT, GOPHER_FIELD_NONE},
    {"gopher.local_path", "local_path", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},

    /* Gopher OSS configuration */
    {"gopher.bucket", "bucket", GOPHER_TYPE_STRING, offsetof(gopherOptions, bucket)},
    {"gopher.access_key", "access_key", GOPHER_TYPE_STRING, offsetof(gopherOptions, accessKey)},
    {"gopher.secret_key", "secret_key", GOPHER_TYPE_STRING, offsetof(gopherOptions, secretKey)},
    {"gopher.region", "region", GOPHER_TYPE_STRING, offsetof(gopherOptions, region)},
    {"gopher.endpoint", "endpoint", GOPHER_TYPE_STRING, offsetof(gopherOptions, host)},
    {"gopher.useVirtualHost", "use_virtual_host", GOPHER_TYPE_BOOL, offsetof(gopherOptions, useVirtualHost)},
    {"gopher.useHttps", "use_https", GOPHER_TYPE_BOOL, offsetof(gopherOptions, useHttps)},
    {"gopher.useListV2", "use_list_v2", GOPHER_TYPE_BOOL, offsetof(gopherOptions, useListV2)},
    {"gopher.max_read_connection", "max_read_connection", GOPHER_TYPE_INT, GOPHER_FIELD_NONE},
    {"gopher.maxHttpRetry", "max_http_retry", GOPHER_TYPE_INT, GOPHER_FIELD_NONE},
    {"gopher.oss_min_delay_time", "oss_min_delay_time", GOPHER_TYPE_INT, GOPHER_FIELD_NONE},

    /* Gopher HDFS configuration */
    {"gopher.name_node", "name_node", GOPHER_TYPE_STRING, offsetof(gopherOptions, hdfs_namenode_host)},
    {"gopher.port", "port", GOPHER_TYPE_INT, offsetof(gopherOptions, hdfs_namenode_port)},
    {"gopher.auth_method", "auth_method", GOPHER_TYPE_STRING, offsetof(gopherOptions, hdfs_auth_method)},
    {"gopher.krb_delegation_token", "krb_delegation_token", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.krb5_ticket_cache_path", "krb5_ticket_cache_path", GOPHER_TYPE_STRING, offsetof(gopherOptions, krb5_ccname)},
    {"gopher.krb_server_key_file", "krb_server_key_file", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.krb_principal", "krb_principal", GOPHER_TYPE_STRING, offsetof(gopherOptions, krb_principal)},
    {"gopher.hadoop_rpc_protection", "hadoop_rpc_protection", GOPHER_TYPE_STRING, offsetof(gopherOptions, hadoop_rpc_protection)},
    {"gopher.is_ha_supported", "is_ha_supported", GOPHER_TYPE_BOOL, offsetof(gopherOptions, is_ha_supported)},
    {"gopher.data_transfer_protocol", "data_transfer_protocol", GOPHER_TYPE_BOOL, offsetof(gopherOptions, data_transfer_protocol)},
    {"gopher.hdfs_ha_configs_num", "hdfs_ha_configs_num", GOPHER_TYPE_INT, offsetof(gopherOptions, hdfs_ha_configs_num)},
    {"gopher.hdfs_tbds_secureid", "hdfs_tbds_secureid", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.hdfs_tbds_securekey", "hdfs_tbds_securekey", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.hdfs_tbds_username", "hdfs_tbds_username", GOPHER_TYPE_STRING, GOPHER_FIELD_NONE},
    {"gopher.hdfs_username", "hdfs_username", GOPHER_TYPE_STRING, offsetof(gopherOptions, hdfs_user)},
    {"gopher.dfs_client_use_datanode_hostname", "dfs_client_use_datanode_hostname", GOPHER_TYPE_BOOL, GOPHER_FIELD_NONE},

    /* HDFS HA configuration */
    {"gopher.dfs_nameservices", "dfs_nameservices", GOPHER_TYPE_STRING, offsetof(gopherOptions, dfs_name_services)},
    {"gopher.dfs_ha_namenodes", "dfs_ha_namenodes", GOPHER_TYPE_STRING, offsetof(gopherOptions, dfs_ha_namenodes)},
    {"gopher.dfs_namenode_rpc_address", "dfs_namenode_rpc_address", GOPHER_TYPE_STRING, offsetof(gopherOptions, dfs_ha_namenode_rpc_addr)},
    {"gopher.dfs_client_failover_proxy_provider", "dfs_client_failover_proxy_provider", GOPHER_TYPE_STRING, offsetof(gopherOptions, dfs_client_failover)},

    /* End of mapping */
    {NULL, NULL, 0, 0}
};

/*
 * Convert gopherOptions to a JSON object with a "gopher" sub-object
 * containing all mapped fields.
 */
static json_t *
convertIcebergConfigToJsonObject(gopherOptions *gopher)
{
    json_t *root = json_object();

    if (gopher == NULL)
        return root;

    /* Add Gopher configuration */
    json_t *gopherObj = json_object();

    /* Process all mapped fields */
    for (int i = 0; gopher_config_mapping[i].config_key != NULL; i++) {
        const GopherConfigMapping *mapping = &gopher_config_mapping[i];
        void *field_ptr = NULL;

        /* Skip fields that don't have a direct mapping in the struct */
        if (mapping->struct_offset == GOPHER_FIELD_NONE)
            continue;

        /* Calculate pointer to the field in the struct */
        field_ptr = (void*)((char*)gopher + mapping->struct_offset);

        /* Add field to JSON based on its type */
        switch (mapping->value_type) {
            case GOPHER_TYPE_STRING:
                {
                    char **str_ptr = (char**)field_ptr;
                    if (*str_ptr != NULL && **str_ptr != '\0')
                        json_object_set_new(gopherObj, mapping->json_key, json_string(*str_ptr));
                }
                break;

            case GOPHER_TYPE_INT:
                {
                    int *int_ptr = (int*)field_ptr;
                    if (*int_ptr != 0)
                        json_object_set_new(gopherObj, mapping->json_key, json_integer(*int_ptr));
                }
                break;

            case GOPHER_TYPE_BOOL:
                {
                    bool *bool_ptr = (bool*)field_ptr;
                    json_object_set_new(gopherObj, mapping->json_key, json_boolean(*bool_ptr));
                }
                break;
        }
    }

    /* Add the Gopher object to the root */
    json_object_set_new(root, "gopher", gopherObj);

    return root;
}

/*
 * Convert dataLakeOptions to a JSON string containing gopher configuration,
 * iceberg config version, and flags.
 */
static char*
convertIcebergConfigToJson(dataLakeOptions* options)
{
    if (options->gopher == NULL)
    {
        return NULL;
    }

    json_t *root = convertIcebergConfigToJsonObject(options->gopher);

    /* Add set_catalog_default_impl flag to determine whether to use native catalog */
    json_object_set_new(root, "set_catalog_default_impl", json_boolean(!options->set_catalog_default_impl));
    json_object_set_new(root, "iceberg_config_version", json_string("v1"));

    char *result = NULL;

    /* Convert to string */
    result = json_dumps(root, JSON_COMPACT);
    json_decref(root);

    /* Convert to memory context */
    if (result)
    {
        char *pgResult = pstrdup(result);
        free(result);
        return pgResult;
    }

    return NULL;
}

/*
 * Build the complete Iceberg config JSON string for a given foreign table.
 * Extracts all options from the SQL definitions (CREATE SERVER, USER MAPPING,
 * FOREIGN TABLE) and serializes them to JSON for the dlagent.
 */
char* getIcebergConfigJsonString(Oid foreigntableid)
{
	dataLakeOptions* options = datalakeGetOptions(foreigntableid);
    char* jsonString = NULL;

    if (options)
    {
        jsonString = convertIcebergConfigToJson(options);
    }

    return jsonString;
}
