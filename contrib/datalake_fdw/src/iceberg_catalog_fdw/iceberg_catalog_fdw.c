/*
 * iceberg_catalog_fdw.c
 *	  Iceberg catalog foreign data wrapper implementation
 */

/* For FDW interface */
#include "iceberg_catalog_fdw.h"
#include "src/components/agent_cli/c_interface/agent_c_api.h"
#include "src/datalake_def.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"

/* For access to server/user catalogs */
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_volume.h"
#include "catalog/pg_user_mapping.h"
#include "utils/syscache.h"

/* For various utility functions */
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/laketablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* For optimizer/planner functions */
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "utils/resowner.h"
#include "utils/guc.h"

#include "src/iceberg_volume_fdw/iceberg_volume_option.h"
#include "src/components/agent_cli/c_interface/agent_cjson_builder.hpp"
#include "src/common/iceberg_constants.h"
#include "src/common/util.h"

/* Wrapper function for CHECK_FOR_INTERRUPTS */
static bool
iceberg_catalog_interrupt_callback(void)
{
    if (!InterruptPending)
        return false;

    if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
        return false;

    return true;
}
/* Remove duplicate struct definitions - now in agent_cli_wrapper.h */

typedef struct IcebergCatalogState
{
    /* Agent CLI wrapper handle */
    AgentCliHandle *agentHandle;
    IcebergCatalogOptions *catalogOption;
    IcebergVolumeOptions  *volumeOption;
} IcebergCatalogState;

/* Remove old resource management variables - now handled by wrapper */

/*
 * SQL functions
 */
extern Datum iceberg_catalog_fdw_handler(PG_FUNCTION_ARGS);
extern Datum iceberg_catalog_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(iceberg_catalog_fdw_handler);
PG_FUNCTION_INFO_V1(iceberg_catalog_fdw_validator);

/**
 * helpful function
 */
static void
icebergCatalogBeginForeiginInsert(ModifyTableState *mtstate,
                                  ResultRelInfo *rinfo);

/* Remove old resource management function declarations - now handled by wrapper */

/* Remove unused function declaration */

const char *
createCreateRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option,  IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);

const char*
createLoadTableRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);


static AgentCliHandle *
initAgentCliHandle(IcebergCatalogFdwState *fdwState, IcebergCatalogState *catalogState);

static void
checkIcebergCatalogFdwState(ResultRelInfo *rinfo);

static void
copyResponseToFdwState(IcebergCatalogFdwState *fdwState, AgentCliHandle *handle);

static char*
normalizePathComponents(const char* protocol, const char* bucket, const char* path);

static void
formatBuiltinWarehouseLocationPrefix(IcebergCatalogOptions* catalogOption, IcebergVolumeOptions* volumeOption);

static void
icebergCatalogEndForeignInsert(EState *estate,
                               ResultRelInfo *rinfo);

static void
icebergCatalogBeginForeignScan(ForeignScanState *node,
							   int eflags);

static void
icebergCatalogEndForeignScan(ForeignScanState *node);

static TupleTableSlot*
icebergCatalogExecForeignInsert(EState *estate,
                                ResultRelInfo *rinfo,
                                TupleTableSlot *slot,
                                TupleTableSlot *planSlot);

/*
 * Helper functions for JSON creation
 */
static agentcli_cJSON* createIcebergCatalogConfig(IcebergCatalogOptions *option);
static agentcli_cJSON* createIcebergVolumeConfig(IcebergVolumeOptions *volumeOpt);
static agentcli_cJSON* createIcebergAdditionalConfig(IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt);
static agentcli_cJSON* createIcebergConfig(IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt);
static agentcli_cJSON* createSchemaFromRequest(IcebergCatalogRequest req);
static agentcli_cJSON* createBuildInProperties(IcebergCatalogOperation opration, IcebergCatalogOptions *option, IcebergCatalogRequest req);
static const char* mapPostgresToIcebergType(Oid pgType, int32 typemod);
const char * createAppendRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createUpdateRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createDropTableRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createListCatalogsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createListNamespacesRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createPlanFileGroupsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
const char * createCommitFileGroupsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req);
/*
 * iceberg_catalog_fdw_handler
 *	  FDW handler function
 */
Datum iceberg_catalog_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /*
     * foreign table scan support
     */

    // iceberg create table operate
    fdwroutine->BeginForeignInsert = icebergCatalogBeginForeiginInsert;
    fdwroutine->EndForeignInsert = icebergCatalogEndForeignInsert;

    // iceberg append/update/delete operate
    fdwroutine->ExecForeignInsert = icebergCatalogExecForeignInsert;

    // iceberg get fragment or load table operate
    fdwroutine->BeginForeignScan = icebergCatalogBeginForeignScan;
    fdwroutine->EndForeignScan = icebergCatalogEndForeignScan;


    PG_RETURN_POINTER(fdwroutine);
}

/*
 * iceberg_catalog_fdw_validator
 *	  Validate options given to the FDW
 */
Datum iceberg_catalog_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}

/* Old resource management functions removed - now handled by agent_cli_wrapper */

/*
 * Normalize path components to prevent double slashes
 * Returns a palloc'd string that should be freed by caller
 *
 * TODO: Remove this function once iceberg-am passes location to FDW.
 * This is only used as a fallback when request.location is NULL.
 */
static char*
normalizePathComponents(const char* protocol, const char* bucket, const char* path)
{
    StringInfoData normalized_path;
    const char *scheme;

    initStringInfo(&normalized_path);

    /*
     * Map volume server type to Hadoop-compatible URI scheme.
     * Hadoop only has s3a:// filesystem, not s3://.
     */
    if (protocol && pg_strcasecmp(protocol, "s3") == 0)
        scheme = "s3a";
    else
        scheme = protocol;

    /* Add scheme and bucket */
    appendStringInfo(&normalized_path, "%s://%s", scheme, bucket);

    /* Handle path component */
    if (path != NULL && *path != '\0')
    {
        const char* clean_path = path;

        /* Skip leading slashes */
        while (*clean_path == '/')
            clean_path++;

        /* Add path if not empty after cleaning */
        if (*clean_path != '\0')
            appendStringInfo(&normalized_path, "/%s", clean_path);
    }

    /* Ensure trailing slash */
    if (normalized_path.data[normalized_path.len - 1] != '/')
        appendStringInfoChar(&normalized_path, '/');

    return normalized_path.data;
}

/*
 * TODO: Remove this function once iceberg-am passes location to FDW.
 * This is only used as a fallback when request.location is NULL.
 */
static void
formatBuiltinWarehouseLocationPrefix(IcebergCatalogOptions* catalogOption, IcebergVolumeOptions* volumeOption)
{
    /*
     * Generate warehouse location prefix from volume options if not already set.
     * This applies to all catalog types (builtin, hive, polaris) when a
     * volume is configured — the volume base path determines the warehouse.
     */
    if (catalogOption->foreign_catalog.warehouse_location_prefix == NULL &&
        volumeOption->volume_server.bucket_name != NULL)
    {
        catalogOption->foreign_catalog.warehouse_location_prefix =
            normalizePathComponents(
                volumeOption->volume_server.server_type,
                volumeOption->volume_server.bucket_name,
                volumeOption->foreign_volume.base_path);
    }
}

/*
 * Initialize IcebergCatalogState with catalog and volume options
 */
static IcebergCatalogState *
initCatalogState(IcebergCatalogFdwState *fdwState)
{
    IcebergCatalogState *catalogState = (IcebergCatalogState *)palloc0(sizeof(IcebergCatalogState));

    catalogState->catalogOption = getIcebergCatalogOptions(
            fdwState->catalogInfo.catalog_server_name,
            fdwState->catalogInfo.catalog_name);

    if (fdwState->catalogInfo.volumn_server_name != NULL &&
        fdwState->catalogInfo.volumn_name != NULL)
    {
        catalogState->volumeOption = getIcebergVolumeOptions(
            fdwState->catalogInfo.volumn_server_name,
            fdwState->catalogInfo.volumn_name);
    }
    else
    {
        /* Polaris dispatch — no local volume, catalog agent manages storage */
        catalogState->volumeOption = NULL;
    }

    if (fdwState->request.location != NULL)
    {
        /* Use pre-formatted location from AM layer */
        catalogState->catalogOption->foreign_catalog.warehouse_location_prefix =
            pstrdup(fdwState->request.location);
    }
    else if (catalogState->volumeOption != NULL)
    {
        /*
         * Fallback: compute warehouse prefix from volume options.
         * Only applicable when a local volume is specified (builtin/hive).
         */
        formatBuiltinWarehouseLocationPrefix(catalogState->catalogOption,
                                             catalogState->volumeOption);
    }
    /* else: Polaris dispatch — warehouse prefix managed by catalog agent */

    /* Initialize agent handle using wrapper */
    catalogState->agentHandle = initAgentCliHandle(fdwState, catalogState);

    return catalogState;
}

/*
 * Initialize and configure AgentCliHandle
 */
static AgentCliHandle *
initAgentCliHandle(IcebergCatalogFdwState *fdwState, IcebergCatalogState *catalogState)
{
    const char *server_url = fdwState->request.agentServerUrl;
    /* Use empty string if namespace is NULL (e.g., list_catalog operation) */
    const char *prefix = fdwState->request.nameSpace ? fdwState->request.nameSpace : "";
    const char *namespace_name = fdwState->request.nameSpace ? fdwState->request.nameSpace : "";

    /* Create agent handle using wrapper */
    AgentCliHandle *handle = agent_cli_wrapper_create(server_url, prefix, namespace_name);

    /* Set interrupt callback */
    agent_cli_wrapper_set_interrupt_callback(handle, iceberg_catalog_interrupt_callback);

    return handle;
}

/*
 * Mask sensitive data in JSON string for logging
 */
static char *
maskSensitiveDataInJson(const char *jsonString)
{
    if (!jsonString)
        return NULL;

    agentcli_cJSON *parsed = agentcli_cJSON_Parse(jsonString);
    if (!parsed)
        return pstrdup(jsonString);

    agentcli_cJSON *icebergConfig = agentcli_cJSON_GetObjectItem(parsed, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG);
    if (icebergConfig)
    {
        agentcli_cJSON *volumeConfig = agentcli_cJSON_GetObjectItem(icebergConfig, DATALAKEFDW_ICEBERG_KEY_ICEBERG_VOLUME_CONFIG);
        if (volumeConfig)
        {
            if (agentcli_cJSON_GetObjectItem(volumeConfig, DATALAKEFDW_ICEBERG_KEY_ACCESS_KEY_ID))
                agentcli_cJSON_ReplaceItemInObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_ACCESS_KEY_ID, agentcli_cJSON_CreateString("**"));

            if (agentcli_cJSON_GetObjectItem(volumeConfig, DATALAKEFDW_ICEBERG_KEY_SECRET_ACCESS_KEY))
                agentcli_cJSON_ReplaceItemInObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_SECRET_ACCESS_KEY, agentcli_cJSON_CreateString("**"));
        }
    }

    char *masked_json = agentcli_cJSON_PrintUnformatted(parsed);
    char *result = pstrdup(masked_json);
    free(masked_json);
    agentcli_cJSON_Delete(parsed);

    return result;
}

/*
 * Execute scan operations (get_fragment, load)
 */
static void
executeScanOperation(IcebergCatalogFdwState *fdwState,
                    IcebergCatalogState *catalogState)
{
    const char *jsonString = NULL;

    elog(DEBUG1, "executeScanOperation: operation=%d", fdwState->catalogOperation);

    switch (fdwState->catalogOperation)
    {
        case ICEBERG_GET_FRAGMENT:
            jsonString = createCreateRequestJson(fdwState, catalogState->catalogOption,
                                                catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_get_fragment(catalogState->agentHandle,
                                           fdwState->request.tableName, jsonString);
            break;
        case ICEBERG_LOAD_TABLE:
            jsonString = createLoadTableRequestJson(fdwState, catalogState->catalogOption,
                                                   catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_load_table(catalogState->agentHandle,
                                         fdwState->request.tableName, jsonString);
            break;
        case ICEBERG_RESTCATALOG_LISTCATALOG:
            jsonString = createListCatalogsRequestJson(fdwState, catalogState->catalogOption,
                                                      catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_list_catalogs(catalogState->agentHandle, jsonString);
            break;
        case ICEBERG_RESTCATALOG_LISTNAMESPACE:
            jsonString = createListNamespacesRequestJson(fdwState, catalogState->catalogOption,
                                                        catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_list_namespaces(catalogState->agentHandle, jsonString);
            break;
        case ICEBERG_GET_STATISTICS:
            jsonString = createLoadTableRequestJson(fdwState, catalogState->catalogOption,
                                                    catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_get_statistics(catalogState->agentHandle,
                                             fdwState->request.tableName, jsonString);
            break;
		case ICEBERG_PLAN_FILE_GROUPS:
			jsonString = createPlanFileGroupsRequestJson(fdwState, catalogState->catalogOption,
														 catalogState->volumeOption, fdwState->request);
			agent_cli_wrapper_plan_file_groups(catalogState->agentHandle,
											   fdwState->request.tableName, jsonString);
			break;
        default:
            elog(ERROR, "Unsupported scan operation: %d", fdwState->catalogOperation);
            break;
    }

    if (client_min_messages <= DEBUG1 && jsonString)
    {
        char *masked_json = maskSensitiveDataInJson(jsonString);
        elog(LOG, "Iceberg catalog scan request JSON: %s", masked_json);
        pfree(masked_json);
    }

    elog(DEBUG1, "executeScanOperation: completed successfully");
    copyResponseToFdwState(fdwState, catalogState->agentHandle);
}

/*
 * Execute modify operations (create, append, update, delete)
 */
static void
executeModifyOperation(IcebergCatalogFdwState *fdwState,
                      IcebergCatalogState *catalogState)
{
    const char *jsonString = NULL;

    elog(DEBUG1, "executeModifyOperation: operation=%d", fdwState->catalogOperation);

    switch (fdwState->catalogOperation)
    {
        case ICEBERG_CREATE_TABLE:
            jsonString = createCreateRequestJson(fdwState, catalogState->catalogOption,
                                                catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_create_table(catalogState->agentHandle,
                                           fdwState->request.tableName, jsonString);
            break;
        case ICEBERG_APPEND:
            jsonString = createAppendRequestJson(fdwState, catalogState->catalogOption,
                                                catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_append_table(catalogState->agentHandle,
                                           fdwState->request.tableName, jsonString);
            break;
        case ICEBERG_UPDATE:
        case ICEBERG_DELETE:
            jsonString = createUpdateRequestJson(fdwState, catalogState->catalogOption,
                                                catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_update_table(catalogState->agentHandle,
                                           fdwState->request.tableName, jsonString);
            break;
        case ICEBERG_DROPTABLE:
            jsonString = createDropTableRequestJson(fdwState, catalogState->catalogOption,
                                                   catalogState->volumeOption, fdwState->request);
            agent_cli_wrapper_drop_table(catalogState->agentHandle,
                                        fdwState->request.tableName, jsonString);
            break;
		case ICEBERG_COMMIT_FILE_GROUPS:
			jsonString = createCommitFileGroupsRequestJson(fdwState, catalogState->catalogOption,
														   catalogState->volumeOption, fdwState->request);
			agent_cli_wrapper_commit_file_groups(catalogState->agentHandle,
												  fdwState->request.tableName, jsonString);
			break;
		case ICEBERG_COMMIT_APPEND:
			jsonString = createAppendRequestJson(fdwState, catalogState->catalogOption,
												 catalogState->volumeOption, fdwState->request);
			agent_cli_wrapper_commit_append(catalogState->agentHandle,
											fdwState->request.tableName, jsonString);
			break;
		case ICEBERG_COMMIT_UPDATE:
		case ICEBERG_COMMIT_DELETE:
			jsonString = createUpdateRequestJson(fdwState, catalogState->catalogOption,
												 catalogState->volumeOption, fdwState->request);
			agent_cli_wrapper_commit_update(catalogState->agentHandle,
											fdwState->request.tableName, jsonString);
			break;
		case ICEBERG_COMMIT_REWRITE:
			jsonString = createCommitFileGroupsRequestJson(fdwState, catalogState->catalogOption,
														   catalogState->volumeOption, fdwState->request);
			agent_cli_wrapper_commit_rewrite(catalogState->agentHandle,
											  fdwState->request.tableName, jsonString);
			break;
        default:
            elog(ERROR, "Unsupported modify operation: %d", fdwState->catalogOperation);
            break;
    }

    if (client_min_messages <= DEBUG1 && jsonString)
    {
        char *masked_json = maskSensitiveDataInJson(jsonString);
        elog(LOG, "Iceberg catalog modify request JSON: %s", masked_json);
        pfree(masked_json);
    }

    elog(DEBUG1, "executeModifyOperation: completed successfully");
    copyResponseToFdwState(fdwState, catalogState->agentHandle);
}



/* Old resource callback removed - now handled by agent_cli_wrapper */

const char*
createLoadTableRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add namespace
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    // Add buildin properties if needed
    if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
        agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_LOAD_TABLE, option, req);
        agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);
    }

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

const char *
createPlanFileGroupsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
	agentcli_cJSON *request = agentcli_cJSON_CreateObject();

	// Add namespace
	agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

	// Add IcebergConfig
	agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
	agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

	// Add vacuum parameters
	agentcli_cJSON_AddNumberToObject(request, "minInputFiles", req.minInputFiles);
	agentcli_cJSON_AddNumberToObject(request, "targetFileSizeMb", req.targetFileSizeMb);

	// Add buildin properties if needed
	if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
		agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_PLAN_FILE_GROUPS, option, req);
		agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);
	}

	char *json_string = agentcli_cJSON_PrintUnformatted(request);
	agentcli_cJSON_Delete(request);
	return json_string;
}

const char *
createCreateRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add namespace
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    // Add schema - use real schema if available, otherwise use default
    agentcli_cJSON *schema = createSchemaFromRequest(req);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_SCHEMA, schema);

    // Add table name
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAME, fdwState->request.tableName);

    // Build properties
    agentcli_cJSON *properties = NULL;

    // Builtin catalog properties
    if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
        properties = createBuildInProperties(ICEBERG_CREATE_TABLE, option, req);
    }

    // Add metadata_location for deferred commit RYOW (all catalog types)
    if (req.metadataLocation != NULL) {
        if (properties == NULL)
            properties = agentcli_cJSON_CreateObject();
        agentcli_cJSON_AddStringToObject(properties,
            DATALAKEFDW_ICEBERG_KEY_DEFERRED_METADATA_LOCATION,
            req.metadataLocation);
    }

    if (properties != NULL) {
        agentcli_cJSON_AddItemToObject(request,
            DATALAKEFDW_ICEBERG_KEY_PROPERTIES, properties);
    }

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

static agentcli_cJSON* createAppendFromRequest(IcebergCatalogRequest req)
{
    if (!req.appendJson || strlen(req.appendJson) == 0) {
        elog(ERROR, "appendJson is null or empty");
        return NULL;
    }

    /* Parse the appendJson string and extract fragments array */
    agentcli_cJSON *parsed = agentcli_cJSON_Parse(req.appendJson);
    if (!parsed) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("Failed to parse appendJson: %s", req.appendJson)));
        return NULL;
    }

    /* Extract fragments array from parsed JSON */
    agentcli_cJSON *fragments = agentcli_cJSON_GetObjectItem(parsed, DATALAKEFDW_ICEBERG_KEY_FRAGMENTS);
    if (!fragments || !agentcli_cJSON_IsArray(fragments)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("fragments not found or not an array in appendJson")));
        agentcli_cJSON_Delete(parsed);
        return NULL;
    }

    /* Duplicate the fragments array to return */
    agentcli_cJSON *result = agentcli_cJSON_Duplicate(fragments, 1);
    agentcli_cJSON_Delete(parsed);

    return result;
}

static agentcli_cJSON* createUpdateFromRequest(IcebergCatalogRequest req)
{
    if (!req.appendJson || strlen(req.appendJson) == 0) {
        elog(ERROR, "appendJson is null or empty");
        return NULL;
    }

    /* Parse the appendJson string and extract fragments array */
    agentcli_cJSON *parsed = agentcli_cJSON_Parse(req.appendJson);
    if (!parsed) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("Failed to parse appendJson: %s", req.appendJson)));
        return NULL;
    }

    /* Extract fragments array from parsed JSON */
    agentcli_cJSON *fragments = agentcli_cJSON_GetObjectItem(parsed, DATALAKEFDW_ICEBERG_KEY_UPDATEFRAGMENTS);
    if (!fragments || !agentcli_cJSON_IsArray(fragments)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("fragments not found or not an array in appendJson")));
        agentcli_cJSON_Delete(parsed);
        return NULL;
    }

    /* Duplicate the fragments array to return */
    agentcli_cJSON *result = agentcli_cJSON_Duplicate(fragments, 1);
    agentcli_cJSON_Delete(parsed);


    return result;
}

const char *
createAppendRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add namespace
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    // Add append json
    agentcli_cJSON *appendjson = createAppendFromRequest(req);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_FRAGMENTS, appendjson);

    // Add buildin properties if needed
    if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
        agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_APPEND, option, req);
        agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);
    }

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

static agentcli_cJSON* createRewrittenFromRequest(IcebergCatalogRequest req)
{
	if (!req.appendJson || strlen(req.appendJson) == 0) {
		elog(ERROR, "appendJson is null or empty");
		return NULL;
	}

	agentcli_cJSON *parsed = agentcli_cJSON_Parse(req.appendJson);
	if (!parsed) {
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("Failed to parse appendJson: %s", req.appendJson)));
		return NULL;
	}

	agentcli_cJSON *rewritten = agentcli_cJSON_GetObjectItem(parsed, DATALAKEFDW_ICEBERG_KEY_REWRITTENFRAGMENTS);
	if (!rewritten || !agentcli_cJSON_IsArray(rewritten)) {
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("rewrittenFragments not found or not an array in appendJson")));
		agentcli_cJSON_Delete(parsed);
		return NULL;
	}

	agentcli_cJSON *result = agentcli_cJSON_Duplicate(rewritten, 1);
	agentcli_cJSON_Delete(parsed);
	return result;
}

const char *
createCommitFileGroupsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
	agentcli_cJSON *request = agentcli_cJSON_CreateObject();

	// Add namespace
	agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

	// Add IcebergConfig
	agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
	agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

	// Add new fragments (files to add)
	agentcli_cJSON *fragments = createAppendFromRequest(req);
	agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_FRAGMENTS, fragments);

	// Add rewritten fragments (old files to remove)
	agentcli_cJSON *rewritten = createRewrittenFromRequest(req);
	agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_REWRITTENFRAGMENTS, rewritten);

	// Add buildin properties if needed
	if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
		agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_COMMIT_FILE_GROUPS, option, req);
		agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);
	}

	char *json_string = agentcli_cJSON_PrintUnformatted(request);
	agentcli_cJSON_Delete(request);
	return json_string;
}

const char *
createUpdateRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add namespace
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    // Add append json
    agentcli_cJSON *appendjson = createUpdateFromRequest(req);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_UPDATEFRAGMENTS, appendjson);

    // Add buildin properties if needed
    if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) == 0) {
        agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_UPDATE, option, req);
        agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);
    }

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

const char *
createDropTableRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    // Only buildin catalog supports drop table
    if (pg_strcasecmp(option->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) != 0) {
        elog(ERROR, "DROP TABLE is only supported for buildin catalog");
    }

    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add namespace
    agentcli_cJSON_AddStringToObject(request, DATALAKEFDW_ICEBERG_KEY_NAMESPACE, req.nameSpace);

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    // Add purgeRequested flag (default true)
    agentcli_cJSON_AddBoolToObject(request, DATALAKEFDW_ICEBERG_KEY_PURGEREQUESTED, true);

    // Add buildin properties
    agentcli_cJSON *buildInProp = createBuildInProperties(ICEBERG_DROPTABLE, option, req);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_PROPERTIES, buildInProp);

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

const char *
createListCatalogsRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

const char *
createListNamespacesRequestJson(IcebergCatalogFdwState* fdwState, IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt, IcebergCatalogRequest req)
{
    agentcli_cJSON *request = agentcli_cJSON_CreateObject();

    // Add IcebergConfig
    agentcli_cJSON *icebergConfig = createIcebergConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(request, DATALAKEFDW_ICEBERG_KEY_ICEBERGCONFIG, icebergConfig);

    char *json_string = agentcli_cJSON_PrintUnformatted(request);
    agentcli_cJSON_Delete(request);
    return json_string;
}

/*
 * Create IcebergCatalogConfig section
 */
static void
addHiveCatalogConfig(agentcli_cJSON *catalogConfig, IcebergCatalogOptions *option)
{
    if (option->catalog_server.hive_metastore_uri && strlen(option->catalog_server.hive_metastore_uri) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_HIVE_METASTORE_URI, option->catalog_server.hive_metastore_uri);

    if (option->catalog_user.hive.auth_method && strlen(option->catalog_user.hive.auth_method) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_AUTH_METHOD, option->catalog_user.hive.auth_method);
}

static void
addPolarisCatalogConfig(agentcli_cJSON *catalogConfig, IcebergCatalogOptions *option)
{
    if (option->catalog_server.polaris_server_url && strlen(option->catalog_server.polaris_server_url) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_POLARIS_SERVER_URL, option->catalog_server.polaris_server_url);

    if (option->catalog_user.polaris.client_id && strlen(option->catalog_user.polaris.client_id) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_CLIENT_ID, option->catalog_user.polaris.client_id);

    if (option->catalog_user.polaris.client_secret && strlen(option->catalog_user.polaris.client_secret) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_CLIENT_SECRET, option->catalog_user.polaris.client_secret);

    if (option->catalog_user.polaris.scope && strlen(option->catalog_user.polaris.scope) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_SCOPE, option->catalog_user.polaris.scope);

    if (option->foreign_catalog.catalog_name && strlen(option->foreign_catalog.catalog_name) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_CATALOG_NAME, option->foreign_catalog.catalog_name);

}

static agentcli_cJSON* createIcebergCatalogConfig(IcebergCatalogOptions *option)
{
    agentcli_cJSON *catalogConfig = agentcli_cJSON_CreateObject();

    if (option->catalog_server.server_type && strlen(option->catalog_server.server_type) > 0)
    {
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_SERVER_TYPE, option->catalog_server.server_type);

        if (pg_strcasecmp(option->catalog_server.server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0)
        {
            addHiveCatalogConfig(catalogConfig, option);
        }
        else if (pg_strcasecmp(option->catalog_server.server_type, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
        {
            addPolarisCatalogConfig(catalogConfig, option);
        }
    }

    if (option->catalog_server.server_name && strlen(option->catalog_server.server_name) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, "server_name", option->catalog_server.server_name);

    if (option->foreign_catalog.warehouse_location_prefix && strlen(option->foreign_catalog.warehouse_location_prefix) > 0)
        agentcli_cJSON_AddStringToObject(catalogConfig, DATALAKEFDW_ICEBERG_KEY_WAREHOUSE_LOCATION, option->foreign_catalog.warehouse_location_prefix);

    return catalogConfig;
}

/*
 * Create IcebergVolumeConfig section
 */
static agentcli_cJSON* createIcebergVolumeConfig(IcebergVolumeOptions *volumeOpt)
{
    agentcli_cJSON *volumeConfig = agentcli_cJSON_CreateObject();

    if (volumeOpt->volume_server.server_type && strlen(volumeOpt->volume_server.server_type) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_VOLUME_SERVER_TYPE, volumeOpt->volume_server.server_type);

    if (volumeOpt->volume_server.endpoint && strlen(volumeOpt->volume_server.endpoint) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_VOLUME_ENDPOINT, volumeOpt->volume_server.endpoint);

    if (volumeOpt->volume_server.region && strlen(volumeOpt->volume_server.region) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_VOLUME_REGION, volumeOpt->volume_server.region);

    if (volumeOpt->volume_server.bucket_name && strlen(volumeOpt->volume_server.bucket_name) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_BUCKET_NAME, volumeOpt->volume_server.bucket_name);

    agentcli_cJSON_AddBoolToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_PATH_STYLE_ACCESS, volumeOpt->volume_server.path_style_access);

    if (volumeOpt->volume_user.aws_access_key_id && strlen(volumeOpt->volume_user.aws_access_key_id) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_ACCESS_KEY_ID, volumeOpt->volume_user.aws_access_key_id);

    if (volumeOpt->volume_user.aws_secret_access_key && strlen(volumeOpt->volume_user.aws_secret_access_key) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, DATALAKEFDW_ICEBERG_KEY_SECRET_ACCESS_KEY, volumeOpt->volume_user.aws_secret_access_key);

    if (volumeOpt->volume_server.server_name && strlen(volumeOpt->volume_server.server_name) > 0)
        agentcli_cJSON_AddStringToObject(volumeConfig, "server_name", volumeOpt->volume_server.server_name);

    return volumeConfig;
}

/*
 * Build the FileIOConfig payload sent to dlagent.
 *
 * The current iceberg-openapi schema (post-refactor) is flat:
 *
 *   fileIOConfig:
 *     gopherConfig:
 *       common: { worker_path, connect_path, connect_plasma_path,
 *                 cache_strategy, log_level, liboss2_log_severity }
 *     properties: { <arbitrary extension keys for the non-gopher path> }
 *
 * The agent's FileIO implementation is selected by gopher.enabled on the
 * agent side (application.properties), not by any impl_class field here.
 * Connection info (endpoint / bucket / credentials / region /
 * path_style_access / ufs_type) is exclusively carried by
 * IcebergVolumeConfig and translated to gopher.* keys by the agent.
 *
 * We still accept the legacy user-facing fileIOConfig shapes
 * (impl_class + simpleFileIOConfig / gopherFileIOConfig wrappers) for
 * backward compatibility with foreign-table DDL written against older
 * extension versions. Whatever shape the user wrote, we always emit the
 * flat schema above to the agent.
 */
static agentcli_cJSON* createIcebergFileIOconfig(IcebergVolumeOptions *volumeOpt)
{
    agentcli_cJSON *parsed = agentcli_cJSON_Parse(volumeOpt->foreign_volume.fileIOConfig);
    if (!parsed) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("Failed to parse fileIOConfig: %s", volumeOpt->foreign_volume.fileIOConfig)));
        return NULL;
    }

    agentcli_cJSON *config = agentcli_cJSON_CreateObject();

    /* Pull gopherConfig.common from one of two source shapes (in priority):
     *  - new flat:    parsed.gopherConfig
     *  - legacy:      parsed.gopherFileIOConfig.gopherConfig
     */
    agentcli_cJSON *gopherCfg = agentcli_cJSON_GetObjectItem(parsed, "gopherConfig");
    if (!gopherCfg) {
        agentcli_cJSON *wrapper = agentcli_cJSON_GetObjectItem(parsed, "gopherFileIOConfig");
        if (wrapper)
            gopherCfg = agentcli_cJSON_GetObjectItem(wrapper, "gopherConfig");
    }
    if (gopherCfg)
        agentcli_cJSON_AddItemToObject(config, "gopherConfig", agentcli_cJSON_Duplicate(gopherCfg, 1));

    /* Pull extension `properties` from one of three source shapes (in priority):
     *  - new flat:    parsed.properties
     *  - legacy:      parsed.simpleFileIOConfig.properties
     *  - legacy:      parsed.gopherFileIOConfig.properties
     */
    agentcli_cJSON *props = agentcli_cJSON_GetObjectItem(parsed, "properties");
    if (!props) {
        agentcli_cJSON *simpleWrap = agentcli_cJSON_GetObjectItem(parsed, "simpleFileIOConfig");
        if (simpleWrap)
            props = agentcli_cJSON_GetObjectItem(simpleWrap, "properties");
    }
    if (!props) {
        agentcli_cJSON *gopherWrap = agentcli_cJSON_GetObjectItem(parsed, "gopherFileIOConfig");
        if (gopherWrap)
            props = agentcli_cJSON_GetObjectItem(gopherWrap, "properties");
    }
    if (props)
        agentcli_cJSON_AddItemToObject(config, "properties", agentcli_cJSON_Duplicate(props, 1));

    agentcli_cJSON_Delete(parsed);
    return config;
}

/*
 * Inject the dlproxy-controlled gopher socket paths into
 * fileIOConfig.gopherConfig.common. Creates the gopherConfig/common
 * subobjects as needed. These keys are always required, regardless of
 * whether the user's fileIOConfig already carried a gopherConfig.
 */
static void addGopherRuntimeSocketPaths(agentcli_cJSON *fileIOConfig)
{
    agentcli_cJSON *gopherCfg = agentcli_cJSON_GetObjectItem(fileIOConfig, "gopherConfig");
    if (!gopherCfg) {
        gopherCfg = agentcli_cJSON_CreateObject();
        agentcli_cJSON_AddItemToObject(fileIOConfig, "gopherConfig", gopherCfg);
    }
    agentcli_cJSON *common = agentcli_cJSON_GetObjectItem(gopherCfg, "common");
    if (!common) {
        common = agentcli_cJSON_CreateObject();
        agentcli_cJSON_AddItemToObject(gopherCfg, "common", common);
    }

    char path_buf[1024] = {0};

    DatalakeGetGopherSocketPath(path_buf);
    agentcli_cJSON_AddStringToObject(common, "connect_path", path_buf);

    memset(path_buf, 0, sizeof(path_buf));
    DatalakeGetGopherPlasmaSocketPath(path_buf);
    agentcli_cJSON_AddStringToObject(common, "connect_plasma_path", path_buf);

    memset(path_buf, 0, sizeof(path_buf));
    DatalakeGetGopherMetaPath(path_buf);
    agentcli_cJSON_AddStringToObject(common, "worker_path", path_buf);
}

/*
 * Create IcebergAdditionalConfig section.
 *
 * Always emits a fileIOConfig object so that gopher socket paths (which the
 * dlproxy controls per-cluster) reach the agent under the standard
 * fileIOConfig.gopherConfig.common nesting. Any user-supplied fileIOConfig
 * fields are translated into the flat schema first; socket paths are then
 * merged in last.
 */
static agentcli_cJSON* createIcebergAdditionalConfig(IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt)
{
    agentcli_cJSON *additionalConfig = agentcli_cJSON_CreateObject();

    if (option->foreign_catalog.total_segment > 0)
        agentcli_cJSON_AddNumberToObject(additionalConfig, DATALAKEFDW_ICEBERG_KEY_TOTALSEGMENT, option->foreign_catalog.total_segment);

    if (option->foreign_catalog.split_size > 0)
        agentcli_cJSON_AddNumberToObject(additionalConfig, DATALAKEFDW_ICEBERG_KEY_SPLITSIZE, option->foreign_catalog.split_size);

    if (option->foreign_catalog.filter_string && strlen(option->foreign_catalog.filter_string) > 0)
        agentcli_cJSON_AddStringToObject(additionalConfig, DATALAKEFDW_ICEBERG_KEY_FILTERSTRING, option->foreign_catalog.filter_string);

    agentcli_cJSON *fileIOConfig;
    if (volumeOpt != NULL &&
        volumeOpt->foreign_volume.fileIOConfig &&
        strlen(volumeOpt->foreign_volume.fileIOConfig) > 0)
    {
        fileIOConfig = createIcebergFileIOconfig(volumeOpt);
    }
    else
    {
        fileIOConfig = agentcli_cJSON_CreateObject();
    }

    addGopherRuntimeSocketPaths(fileIOConfig);
    agentcli_cJSON_AddItemToObject(additionalConfig, DATALAKEFDW_ICEBERG_KEY_FILEIOCONFIG, fileIOConfig);

    return additionalConfig;
}

/*
 * Create complete IcebergConfig section
 */
static agentcli_cJSON* createIcebergConfig(IcebergCatalogOptions *option, IcebergVolumeOptions *volumeOpt)
{
    agentcli_cJSON *icebergConfig = agentcli_cJSON_CreateObject();

    agentcli_cJSON *catalogConfig = createIcebergCatalogConfig(option);
    agentcli_cJSON_AddItemToObject(icebergConfig, DATALAKEFDW_ICEBERG_KEY_ICEBERG_CATALOG_CONFIG, catalogConfig);

    if (volumeOpt != NULL)
    {
        agentcli_cJSON *volumeConfig = createIcebergVolumeConfig(volumeOpt);
        agentcli_cJSON_AddItemToObject(icebergConfig, DATALAKEFDW_ICEBERG_KEY_ICEBERG_VOLUME_CONFIG, volumeConfig);
    }

    agentcli_cJSON *additionalConfig = createIcebergAdditionalConfig(option, volumeOpt);
    agentcli_cJSON_AddItemToObject(icebergConfig, DATALAKEFDW_ICEBERG_KEY_ICEBERG_ADDITIONALCONFIG, additionalConfig);

    /*
     * Gopher socket paths are already attached to
     * additionalConfig.fileIOConfig.gopherConfig.common by
     * createIcebergAdditionalConfig -> addGopherRuntimeSocketPaths. The
     * previous top-level icebergConfig.gopherConfig placement was a
     * misnesting against the OpenAPI schema and is no longer emitted.
     */

    /* Determine config file name(s) based on volume/catalog type for server_name lookup */
    {
        StringInfoData config_files;
        bool has_config = false;
        initStringInfo(&config_files);

        if (volumeOpt != NULL && volumeOpt->volume_server.server_name)
        {
            const char *vtype = volumeOpt->volume_server.server_type;
            if (vtype && (pg_strcasecmp(vtype, "s3") == 0 || pg_strcasecmp(vtype, "s3a") == 0))
            {
                appendStringInfoString(&config_files, "s3.conf");
                has_config = true;
            }
            else if (vtype && pg_strcasecmp(vtype, "hdfs") == 0)
            {
                appendStringInfoString(&config_files, "gphdfs.conf");
                has_config = true;
            }
        }

        if (option->catalog_server.server_name)
        {
            const char *ctype = option->catalog_server.server_type;
            if (ctype && pg_strcasecmp(ctype, "hive") == 0)
            {
                if (has_config)
                    appendStringInfoChar(&config_files, '0'); /* delimiter */
                appendStringInfoString(&config_files, "gphive.conf");
                has_config = true;
            }
        }

        if (has_config)
            agentcli_cJSON_AddStringToObject(icebergConfig, "config_files", config_files.data);

        pfree(config_files.data);
    }

    return icebergConfig;
}

static const char* mapPostgresToIcebergType(Oid pgType, int32 typemod)
{
    switch (pgType) {
        case BOOLOID:
            return DATALAKEFDW_ICEBERG_TYPE_BOOLEAN;
        case INT2OID:
        case INT4OID:
            return DATALAKEFDW_ICEBERG_TYPE_INT;
        case INT8OID:
            return DATALAKEFDW_ICEBERG_TYPE_LONG;
        case FLOAT4OID:
            return DATALAKEFDW_ICEBERG_TYPE_FLOAT;
        case FLOAT8OID:
            return DATALAKEFDW_ICEBERG_TYPE_DOUBLE;
        case NUMERICOID:
        {
            int precision = 38;
            int scale = 9;
            if (typemod >= (int32) (VARHDRSZ))
            {
                precision = ((typemod - VARHDRSZ) >> 16) & 0xffff;
                scale = (typemod - VARHDRSZ) & 0xffff;
            }
            if (precision > 38)
                elog(ERROR, "Iceberg decimal precision %d exceeds maximum supported precision 38", precision);
            if (scale > precision)
                elog(ERROR, "Iceberg decimal scale %d cannot exceed precision %d", scale, precision);
            char *result = psprintf("decimal(%d,%d)", precision, scale);
            return result;
        }
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            /*
             * Iceberg has no fixed-length character type; CHAR(N) is mapped to
             * string, matching Snowflake / Spark / Trino / PrestoDB.  Trailing
             * space and right-padding semantics of PG CHAR(N) are NOT preserved
             * on the Iceberg side -- see contrib/datalake_fdw/README.md.
             */
            return DATALAKEFDW_ICEBERG_TYPE_STRING;
        case DATEOID:
            return DATALAKEFDW_ICEBERG_TYPE_DATE;
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return DATALAKEFDW_ICEBERG_TYPE_TIMESTAMP;
        case BYTEAOID:
            return DATALAKEFDW_ICEBERG_TYPE_BINARY;
        default:
            elog(WARNING, "Unsupported PostgreSQL type OID: %u, using string", pgType);
            return DATALAKEFDW_ICEBERG_TYPE_STRING;
    }
}

/*
 * Create schema from request - use real schema if available, otherwise default
 */
static agentcli_cJSON* createSchemaFromRequest(IcebergCatalogRequest req)
{
    agentcli_cJSON *schema = agentcli_cJSON_CreateObject();
    agentcli_cJSON_AddStringToObject(schema, DATALAKEFDW_ICEBERG_KEY_TYPE, DATALAKEFDW_ICEBERG_TYPE_STRUCT);
    agentcli_cJSON_AddNumberToObject(schema, DATALAKEFDW_ICEBERG_KEY_SCHEMAID, 0);

    agentcli_cJSON *fields = agentcli_cJSON_CreateArray();

    if (req.schema && req.schema->columns) {
        ListCell *lc;
        int fieldId = 1;

        foreach(lc, req.schema->columns) {
            IcebergColumnDef *colDef = (IcebergColumnDef*)lfirst(lc);

            agentcli_cJSON *field = agentcli_cJSON_CreateObject();
            agentcli_cJSON_AddNumberToObject(field, DATALAKEFDW_ICEBERG_KEY_ID, fieldId++);
            agentcli_cJSON_AddStringToObject(field, DATALAKEFDW_ICEBERG_KEY_NAME, colDef->columnName);
            agentcli_cJSON_AddBoolToObject(field, DATALAKEFDW_ICEBERG_KEY_REQUIRED, !colDef->isNullable);
            agentcli_cJSON_AddStringToObject(field, DATALAKEFDW_ICEBERG_KEY_TYPE,
                mapPostgresToIcebergType(colDef->dataType, colDef->typeModifier));

            if (colDef->comment && strlen(colDef->comment) > 0) {
                agentcli_cJSON_AddStringToObject(field, DATALAKEFDW_ICEBERG_KEY_DOC, colDef->comment);
            }

            agentcli_cJSON_AddItemToArray(fields, field);
        }
    } else {
        elog(ERROR, "schema data is null, get iceberg table schema failed!");
    }

    agentcli_cJSON_AddItemToObject(schema, DATALAKEFDW_ICEBERG_KEY_FIELDS, fields);
    return schema;
}

/*
 * Create buildin properties section
 */
static agentcli_cJSON* createBuildInProperties(IcebergCatalogOperation opration, IcebergCatalogOptions *option, IcebergCatalogRequest req)
{
    agentcli_cJSON *buildInProp = agentcli_cJSON_CreateObject();

    if (req.buildInCatalog.tableExists) {
        agentcli_cJSON_AddStringToObject(buildInProp, DATALAKEFDW_ICEBERG_KEY_BUILDIN_TABLE_EXISTS, DATALAKEFDW_ICEBERG_VALUE_TRUE);
    } else {
        agentcli_cJSON_AddStringToObject(buildInProp, DATALAKEFDW_ICEBERG_KEY_BUILDIN_TABLE_EXISTS, DATALAKEFDW_ICEBERG_VALUE_FALSE);
    }

    if (req.buildInCatalog.metadataLocation) {
        agentcli_cJSON_AddStringToObject(buildInProp, DATALAKEFDW_ICEBERG_KEY_METADATALOCATION, req.buildInCatalog.metadataLocation);
    } else if (opration != ICEBERG_CREATE_TABLE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("internal error, metadataLocation is required for builtin catalog")));
    }

    return buildInProp;
}

/* Removed unused initAgentConfig function */

static void
checkIcebergCatalogFdwState(ResultRelInfo *rinfo)
{
    Assert(rinfo != NULL && rinfo->ri_FdwState != NULL);
}

static void
copyResponseToFdwState(IcebergCatalogFdwState *fdwState, AgentCliHandle *handle)
{
    MemoryContext oldContext;

    if (!fdwState || !handle || !handle->currentResponse)
        return;

    elog(DEBUG2, "copyResponseToFdwState: HTTP Status: %d, CURL Code: %ld, Response Size: %zu",
         handle->currentResponse->http_status,
         handle->currentResponse->curl_code,
         handle->currentResponse->response_size);

    if (handle->currentResponse->response_body)
        elog(DEBUG2, "copyResponseToFdwState: Response Body: %s", handle->currentResponse->response_body);

    /* Switch to CurTransactionContext to ensure data persists beyond EndForeignInsert but gets cleaned up at transaction end */
    oldContext = MemoryContextSwitchTo(CurTransactionContext);

    fdwState->response.httpStatus = handle->currentResponse->http_status;
    fdwState->response.curlCode = handle->currentResponse->curl_code;
    fdwState->response.responseBody = handle->currentResponse->response_body ? pstrdup(handle->currentResponse->response_body) : NULL;
    fdwState->response.responseSize = handle->currentResponse->response_size;
    fdwState->response.errorMessage = handle->currentResponse->error_message ? pstrdup(handle->currentResponse->error_message) : NULL;
    fdwState->response.totalTime = handle->currentResponse->total_time;
    fdwState->response.retryCount = handle->currentResponse->retry_count;

    switch (handle->lastStatus)
    {
        case AGENT_CLI_SUCCESS:
            fdwState->lastStatus = ICEBERG_SUCCESS;
            break;
        case AGENT_CLI_CREATETABLE_ALREADY_EXSITS:
            fdwState->lastStatus = ICEBERG_CREAT_TABLE_ALREADY_EXISTS;
            break;
        case AGENT_CLI_ERROR_INVALID_PARAM:
            fdwState->lastStatus = ICEBERG_ERROR_INVALID_PARAM;
            break;
        case AGENT_CLI_ERROR_NETWORK:
            fdwState->lastStatus = ICEBERG_ERROR_NETWORK;
            break;
        case AGENT_CLI_ERROR_JSON_PARSE:
            fdwState->lastStatus = ICEBERG_ERROR_JSON_PARSE;
            break;
        case AGENT_CLI_ERROR_HTTP:
            fdwState->lastStatus = ICEBERG_ERROR_HTTP;
            break;
        case AGENT_CLI_ERROR_MEMORY:
            fdwState->lastStatus = ICEBERG_ERROR_MEMORY;
            break;
        case AGENT_CLI_ERROR_CURL:
            fdwState->lastStatus = ICEBERG_ERROR_TRANSPORT;
            break;
        case AGENT_CLI_ERROR_TIMEOUT:
            fdwState->lastStatus = ICEBERG_ERROR_TIMEOUT;
            break;
        default:
            fdwState->lastStatus = ICEBERG_ERROR_INVALID_PARAM;
            break;
    }

    elog(DEBUG2, "copyResponseToFdwState: Mapped status from %d to %d", handle->lastStatus, fdwState->lastStatus);

    /* Switch back to original context */
    MemoryContextSwitchTo(oldContext);
}

static void
icebergCatalogBeginForeiginInsert(ModifyTableState *mtstate,
                                  ResultRelInfo *rinfo)
{
    checkIcebergCatalogFdwState(rinfo);

    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState *)rinfo->ri_FdwState;

    elog(DEBUG1, "icebergCatalogBeginForeiginInsert: operation=%d", fdwState->catalogOperation);

    MemoryContext oldcontext;
    /* Create memory context */
    MemoryContext fdw_context = AllocSetContextCreate(CurrentMemoryContext,
                                                      "IcebergCatalogFdwContext",
                                                      ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(fdw_context);
    fdwState->fdwContext = fdw_context;

    IcebergCatalogState *catalogState = initCatalogState(fdwState);

    // Only execute for create table operation
    if (fdwState->catalogOperation == ICEBERG_CREATE_TABLE)
    {
        executeModifyOperation(fdwState, catalogState);
    }

    /* Save state */
    fdwState->catalogHandle = (void *)catalogState;
    rinfo->ri_FdwState = (void *)fdwState;
    MemoryContextSwitchTo(oldcontext);

    elog(DEBUG1, "icebergCatalogBeginForeiginInsert: completed");
}

static TupleTableSlot*
icebergCatalogExecForeignInsert(EState *estate,
                                ResultRelInfo *rinfo,
                                TupleTableSlot *slot,
                                TupleTableSlot *planSlot)
{
    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState *)rinfo->ri_FdwState;
    if (!fdwState)
    {
        elog(ERROR, "IcebergCatalogFdwState is null");
    }

    /* Get catalog state and handle from fdwState */
    IcebergCatalogState *catalogState = (IcebergCatalogState *)fdwState->catalogHandle;
    if (!catalogState || !catalogState->agentHandle)
    {
        elog(ERROR, "IcebergCatalogState or agentHandle is null");
    }

    // Use modify operation for append/update/delete
    executeModifyOperation(fdwState, catalogState);

    return slot;
}

static void
icebergCatalogEndForeignInsert(EState *estate,
                               ResultRelInfo *rinfo)
{
    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState *)rinfo->ri_FdwState;
    if (!fdwState || fdwState->catalogHandle == NULL)
    {
        return;
    }

    /*
     * Destroy the C++ AgentCliHandle before deleting the palloc context.
     * MemoryContextDelete only frees palloc'd bytes; the C++ AgentContext /
     * AgentClient / HttpClient (including persistent CURL handles and the
     * libcurl connection cache) live on the C++ heap and must be released
     * explicitly. Missing this call caused linear RSS + FD growth (~1-5 MB
     * and ~3 sockets per INSERT) under JDBC executeBatch workloads. See
     * issue #323.
     */
    IcebergCatalogState *catalogState = (IcebergCatalogState *)fdwState->catalogHandle;
    if (catalogState->agentHandle)
    {
        /*
         * Null-before-destroy: agent_cli_wrapper_destroy() reaches into the
         * C++ cleanup path (and pfree), either of which can ereport.  A
         * longjmp out of destroy with the field still set would let an
         * error-recovery re-entry call destroy on the same handle a second
         * time -- a double free.  Detach the pointer first, then destroy.
         */
        AgentCliHandle *handle = catalogState->agentHandle;
        catalogState->agentHandle = NULL;
        agent_cli_wrapper_destroy(handle);
    }

    /*
     * Destroy memory context.  catalogState is palloc'd inside fdwContext,
     * so deleting the context frees it; null catalogHandle in the same
     * step so a second call (e.g. an error-recovery retry) hits the entry
     * guard instead of dereferencing freed memory.
     */
    if (fdwState->fdwContext)
    {
        MemoryContextDelete(fdwState->fdwContext);
        fdwState->fdwContext = NULL;
        fdwState->catalogHandle = NULL;
    }
}

static void
icebergCatalogBeginForeignScan(ForeignScanState *node,
							   int eflags)
{
    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState *)node->fdw_state;
    MemoryContext oldcontext;
    /* Create memory context */
    MemoryContext fdw_context = AllocSetContextCreate(CurrentMemoryContext,
                                                      "IcebergCatalogFdwContext",
                                                      ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(fdw_context);
    fdwState->fdwContext = fdw_context;

    IcebergCatalogState *catalogState = initCatalogState(fdwState);

    // Use scan operation for get_fragment and load table
    executeScanOperation(fdwState, catalogState);

    /* Save state */
    fdwState->catalogHandle = (void *)catalogState;
    node->fdw_state = (void *)fdwState;
    MemoryContextSwitchTo(oldcontext);
}

static void
icebergCatalogEndForeignScan(ForeignScanState *node)
{
    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState *)node->fdw_state;
    if (!fdwState || fdwState->catalogHandle == NULL)
    {
        return;
    }

    /* See icebergCatalogEndForeignInsert for the rationale (including the
     * null-before-destroy ordering). */
    IcebergCatalogState *catalogState = (IcebergCatalogState *)fdwState->catalogHandle;
    if (catalogState->agentHandle)
    {
        AgentCliHandle *handle = catalogState->agentHandle;
        catalogState->agentHandle = NULL;
        agent_cli_wrapper_destroy(handle);
    }

    if (fdwState->fdwContext)
    {
        MemoryContextDelete(fdwState->fdwContext);
        fdwState->fdwContext = NULL;
        fdwState->catalogHandle = NULL;
    }
}

void check_catalog_fdw_exec_error(IcebergCatalogFdwState *fdwState, const char *error_prefix)
{
    StringInfoData errorBuf;

	if (fdwState->lastStatus != ICEBERG_SUCCESS)
	{
		initStringInfo(&errorBuf);

		appendStringInfo(&errorBuf,
						 "%s: HTTP Status: %d, CURL Code: %ld",
						 error_prefix,
						 fdwState->response.httpStatus,
						 fdwState->response.curlCode);

		if (fdwState->response.errorMessage)
			appendStringInfo(&errorBuf, ", Error: %s", fdwState->response.errorMessage);

		if (fdwState->response.responseBody)
		{
			/* Extract error details from JSON response */
			char *codeStart = strstr(fdwState->response.responseBody, "\"code\":");
			if (codeStart)
			{
				codeStart += 7;
				while (*codeStart == ' ') codeStart++;
				char *codeEnd = strchr(codeStart, ',');
				if (codeEnd)
					appendStringInfo(&errorBuf, "\nError Code: %.*s", (int)(codeEnd - codeStart), codeStart);
			}

			char *typeStart = strstr(fdwState->response.responseBody, "\"type\":\"");
			if (typeStart)
			{
				typeStart += 8;
				char *typeEnd = strchr(typeStart, '"');
				if (typeEnd)
					appendStringInfo(&errorBuf, "\nError Type: %.*s", (int)(typeEnd - typeStart), typeStart);
			}

			char *msgStart = strstr(fdwState->response.responseBody, "\"message\":\"");
			if (msgStart)
			{
				msgStart += 11;
				char *msgEnd = strstr(msgStart, "\",\"");
				if (!msgEnd) msgEnd = strstr(msgStart, "\"}");
				if (msgEnd)
					appendStringInfo(&errorBuf, "\nMessage: %.*s", (int)(msgEnd - msgStart), msgStart);
			}

			/* Extract and format stack trace */
			char *stackStart = strstr(fdwState->response.responseBody, "\"stack\":\"");
			if (stackStart && client_min_messages <= LOG)
			{
				stackStart += 9;
				char *stackEnd = strstr(stackStart, "\",\"");
				if (!stackEnd) stackEnd = strstr(stackStart, "\"}");
				if (stackEnd)
				{
					appendStringInfo(&errorBuf, "\n\nStack Trace:\n");
					char *src = stackStart;
					while (src < stackEnd)
					{
						if (src[0] == '\\' && src[1] == 'n')
						{
							appendStringInfoChar(&errorBuf, '\n');
							src += 2;
						}
						else if (src[0] == '\\' && src[1] == 't')
						{
							appendStringInfoChar(&errorBuf, '\t');
							src += 2;
						}
						else if (src[0] == '\\' && src[1] == '"')
						{
							appendStringInfoChar(&errorBuf, '"');
							src += 2;
						}
						else
						{
							appendStringInfoChar(&errorBuf, *src);
							src++;
						}
					}
				}
			}
		}

		elog(ERROR, "%s", errorBuf.data);
	}

	if (fdwState->response.responseBody == NULL || fdwState->response.responseBody[0] == '\0')
		elog(ERROR, "%s: No response body", error_prefix);
}


/*
 * Parse IcebergCatalogOptions from CreateForeignCatalogStmt
 */
static IcebergCatalogOptions*
parseIcebergCatalogOptionsFromStmt(CreateForeignCatalogStmt *createCatalogStmt)
{
	return getIcebergPolarisCatalogOptions(createCatalogStmt->servername, createCatalogStmt->catalogname, createCatalogStmt->options);
}

/*
 * Build storageConfigInfo from volume options
 */
static agentcli_cJSON*
build_storage_config_info(IcebergVolumeOptions *volumeOpt)
{
	if (!volumeOpt)
		return NULL;

	agentcli_cJSON *storageInfo = agentcli_cJSON_CreateObject();

	/* Map and validate storageType according to Polaris spec: S3, GCS, AZURE, FILE */
	if (volumeOpt->volume_server.server_type)
	{
		const char *storageType = NULL;
		
		if (pg_strcasecmp(volumeOpt->volume_server.server_type, "s3") == 0 ||
			pg_strcasecmp(volumeOpt->volume_server.server_type, "s3a") == 0)
			storageType = "S3";
		else if (pg_strcasecmp(volumeOpt->volume_server.server_type, "gcs") == 0 ||
				 pg_strcasecmp(volumeOpt->volume_server.server_type, "gs") == 0)
			storageType = "GCS";
		else if (pg_strcasecmp(volumeOpt->volume_server.server_type, "azure") == 0 ||
				 pg_strcasecmp(volumeOpt->volume_server.server_type, "abfs") == 0 ||
				 pg_strcasecmp(volumeOpt->volume_server.server_type, "abfss") == 0)
			storageType = "AZURE";
		else if (pg_strcasecmp(volumeOpt->volume_server.server_type, "file") == 0)
			storageType = "FILE";
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported storage type: %s", volumeOpt->volume_server.server_type),
					 errhint("Supported types: S3, GCS, AZURE, FILE")));
		
		agentcli_cJSON_AddStringToObject(storageInfo, "storageType", storageType);
	}

	/* Build allowedLocations as protocol://bucket/ */
	if (volumeOpt->volume_server.bucket_name)
	{
		char allowedLocation[512];
		const char *protocol = "s3";

		/* Determine protocol from server_type if available */
		if (volumeOpt->volume_server.server_type)
		{
			if (pg_strcasecmp(volumeOpt->volume_server.server_type, "s3") == 0 ||
				pg_strcasecmp(volumeOpt->volume_server.server_type, "s3a") == 0)
				protocol = "s3";
		}

		snprintf(allowedLocation, sizeof(allowedLocation), "%s://%s/",
				 protocol, volumeOpt->volume_server.bucket_name);
		
		agentcli_cJSON *allowedLocations = agentcli_cJSON_CreateArray();
		agentcli_cJSON_AddItemToArray(allowedLocations, agentcli_cJSON_CreateString(allowedLocation));
		agentcli_cJSON_AddItemToObject(storageInfo, "allowedLocations", allowedLocations);
	}

	/* AWS specific fields for Polaris */
	if (volumeOpt->volume_server.role_arn)
		agentcli_cJSON_AddStringToObject(storageInfo, "roleArn", volumeOpt->volume_server.role_arn);

	if (volumeOpt->volume_server.user_arn)
		agentcli_cJSON_AddStringToObject(storageInfo, "userArn", volumeOpt->volume_server.user_arn);

	if (volumeOpt->volume_server.region)
		agentcli_cJSON_AddStringToObject(storageInfo, "region", volumeOpt->volume_server.region);

	/* Optional endpoint fields */
	if (volumeOpt->volume_server.endpoint)
	{
		agentcli_cJSON_AddStringToObject(storageInfo, "endpoint", volumeOpt->volume_server.endpoint);
		/* Use endpoint as endpointInternal if not specified */
		if (volumeOpt->volume_server.endpoint_internal)
			agentcli_cJSON_AddStringToObject(storageInfo, "endpointInternal", volumeOpt->volume_server.endpoint_internal);
		else
			agentcli_cJSON_AddStringToObject(storageInfo, "endpointInternal", volumeOpt->volume_server.endpoint);
	}

	agentcli_cJSON_AddBoolToObject(storageInfo, "pathStyleAccess", volumeOpt->volume_server.path_style_access);

	return storageInfo;
}

/*
 * Build JSON for catalog creation
 */
static char*
build_create_catalog_json(IcebergCatalogOptions *options, IcebergVolumeOptions *volumeOpt)
{
	agentcli_cJSON *root = agentcli_cJSON_CreateObject();
	agentcli_cJSON *icebergConfig = agentcli_cJSON_CreateObject();
	agentcli_cJSON *catalogConfig = createIcebergCatalogConfig(options);

	agentcli_cJSON_AddItemToObject(icebergConfig, "IcebergCatalogConfig", catalogConfig);
	agentcli_cJSON_AddItemToObject(root, "IcebergConfig", icebergConfig);

	/* Add catalog metadata */
	agentcli_cJSON *catalog = agentcli_cJSON_CreateObject();
	agentcli_cJSON_AddStringToObject(catalog, "name", options->foreign_catalog.catalog_name);
	agentcli_cJSON_AddStringToObject(catalog, "type", "INTERNAL");
	agentcli_cJSON_AddBoolToObject(catalog, "readOnly", false);

	if (volumeOpt && volumeOpt->volume_server.bucket_name)
	{
		agentcli_cJSON *properties = agentcli_cJSON_CreateObject();

		/* Build default-base-location as protocol://bucket/path */
		char baseLocation[1024];
		const char *protocol;

		/*
		 * Map volume server type to Hadoop-compatible URI scheme.
		 * Hadoop only has s3a:// filesystem, not s3://.
		 */
		if (volumeOpt->volume_server.server_type &&
			(strcasecmp(volumeOpt->volume_server.server_type, "hdfs") == 0))
			protocol = "hdfs";
		else
			protocol = "s3a";

		snprintf(baseLocation, sizeof(baseLocation), "%s://%s/",
					 protocol, volumeOpt->volume_server.bucket_name);

		agentcli_cJSON_AddStringToObject(properties, "default-base-location", baseLocation);
		agentcli_cJSON_AddItemToObject(catalog, "properties", properties);

		agentcli_cJSON *storageInfo = build_storage_config_info(volumeOpt);
		if (storageInfo)
			agentcli_cJSON_AddItemToObject(catalog, "storageConfigInfo", storageInfo);
	}

	agentcli_cJSON_AddItemToObject(root, "catalog", catalog);

	char *json_string = agentcli_cJSON_PrintUnformatted(root);
	agentcli_cJSON_Delete(root);

	return json_string;
}

/*
 * Handle CREATE FOREIGN CATALOG statement
 */
void
iceberg_catalog_create_catalog(CreateForeignCatalogStmt *createCatalogStmt)
{
    /* Only Polaris catalog type needs to call agent to create catalog */
    if (!checkIsPolarisCatalog(createCatalogStmt->servername, createCatalogStmt->catalogname))
    {
        return;
    }
	IcebergCatalogOptions *options = parseIcebergCatalogOptionsFromStmt(createCatalogStmt);
	IcebergVolumeOptions *volumeOpt = NULL;

	elog(DEBUG1, "iceberg_catalog_create_catalog: catalog_name=%s, default_volume=%s",
		 options->foreign_catalog.catalog_name,
		 iceberg_default_volume ? iceberg_default_volume : "(null)");

	/* Get volume options from iceberg_default_volume GUC */
	if (iceberg_default_volume && iceberg_default_volume[0] != '\0')
	{
		/* Query pg_foreign_volume to get server name */
		Oid volumeOid = get_foreign_volume_oid(iceberg_default_volume, NULL, true);

		if (OidIsValid(volumeOid))
		{
			HeapTuple tuple = SearchSysCache1(FOREIGNVOLUMEOID, ObjectIdGetDatum(volumeOid));
			if (HeapTupleIsValid(tuple))
			{
				Form_pg_foreign_volume volumeForm = (Form_pg_foreign_volume) GETSTRUCT(tuple);
				ForeignServer *server = GetForeignServer(volumeForm->fvserver);

				elog(DEBUG1, "iceberg_catalog_create_catalog: loading volume %s.%s",
					 server->servername, iceberg_default_volume);

				volumeOpt = getIcebergVolumeOptions(server->servername, iceberg_default_volume);
				ReleaseSysCache(tuple);
			}
		}
	}

	AgentCliHandle *handle = agent_cli_wrapper_create(
		datalake_agent_server_url,
		"iceberg",
		options->foreign_catalog.default_namespace ? options->foreign_catalog.default_namespace : DATALAKEFDW_ICEBERG_KEY_DEFAULT_NAMESPACE
	);

	if (!handle)
		elog(ERROR, "Failed to initialize agent CLI for catalog creation");

	char *catalog_json = build_create_catalog_json(options, volumeOpt);

	elog(DEBUG1, "iceberg_catalog_create_catalog: JSON request:\n%s", catalog_json);

	PG_TRY();
	{
		agent_cli_wrapper_create_catalog(handle, catalog_json);

		if (!agent_cli_wrapper_is_success(handle))
			agent_cli_wrapper_check_exec_error_json(handle, "Failed to create catalog");

		elog(DEBUG1, "iceberg_catalog_create_catalog: catalog created successfully");
	}
	PG_CATCH();
	{
		agent_cli_wrapper_destroy(handle);
		PG_RE_THROW();
	}
	PG_END_TRY();

	agent_cli_wrapper_destroy(handle);
}
