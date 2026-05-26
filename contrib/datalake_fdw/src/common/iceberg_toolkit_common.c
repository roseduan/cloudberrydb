/*
 * iceberg_toolkit_common.c
 *      Common functions and utilities for Iceberg toolkit FDWs
 */

#include "iceberg_toolkit_common.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "foreign/foreign.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "access/table.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "parser/parse_relation.h"
#include "nodes/makefuncs.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "../common/iceberg_constants.h"
#include "../iceberg_catalog_fdw/iceberg_catalog_option.h"
#include "../am_iceberg/include/iceberg_oids.h"
#include "../am_iceberg/include/pg_iceberg_metadata.h"

/*
 * Get catalog FDW routine
 */
FdwRoutine* iceberg_get_catalog_fdw_routine(void)
{
    ForeignDataWrapper *fdw = GetForeignDataWrapperByName(ICEBERG_CATALOG_FDW_NAME, false);
    return GetFdwRoutine(fdw->fdwhandler);
}

/*
 * Get volume FDW routine
 */
FdwRoutine* iceberg_get_volume_fdw_routine(void)
{
    ForeignDataWrapper *fdw = GetForeignDataWrapperByName(ICEBERG_VOLUME_FDW_NAME, false);
    return GetFdwRoutine(fdw->fdwhandler);
}

/*
 * Create column definition
 */
IcebergColumnDef* iceberg_make_column_def(const char* name, Oid dataType,
                                         int32 typemod, bool nullable, const char* comment)
{
    IcebergColumnDef* colDef = palloc0(sizeof(IcebergColumnDef));
    colDef->columnName = pstrdup(name);
    colDef->dataType = dataType;
    colDef->typeModifier = typemod;
    colDef->isNullable = nullable;
    colDef->comment = comment ? pstrdup(comment) : NULL;
    return colDef;
}

/*
 * Build schema from iceberg table name
 */
IcebergTableSchema* iceberg_build_schema_from_table_name(const char* tableName)
{
    if (!tableName || strlen(tableName) == 0) {
        return NULL;
    }

    /* Convert table name to relation OID */
    RangeVar *rv = makeRangeVar(NULL, (char*)tableName, -1);
    Oid relationOid = RangeVarGetRelid(rv, NoLock, false);

    /* Open the relation */
    Relation rel = table_open(relationOid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);

    /* Create schema structure */
    IcebergTableSchema* schema = palloc0(sizeof(IcebergTableSchema));
    schema->columns = NIL;
    schema->partitionColumns = NIL;

    /* Process each column */
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (attr->attisdropped) continue;

        IcebergColumnDef* colDef = iceberg_make_column_def(
            NameStr(attr->attname),    /* column name */
            attr->atttypid,            /* data type */
            attr->atttypmod,           /* type modifier */
            !attr->attnotnull,         /* nullable */
            NULL                       /* comment */
        );

        schema->columns = lappend(schema->columns, colDef);
    }

    table_close(rel, AccessShareLock);
    return schema;
}

/*
 * Format success response JSON
 */
void iceberg_format_success_response(StringInfo result, const char* operation,
                                    const char* nameSpace, const char* tableName,
                                    const char* additionalData)
{
    if (additionalData) {
        appendStringInfo(result, "{\"status\":\"success\",\"operation\":\"%s\",\"namespace\":\"%s\",\"table\":\"%s\",%s}",
                        operation, nameSpace, tableName, additionalData);
    } else {
        appendStringInfo(result, "{\"status\":\"success\",\"operation\":\"%s\",\"namespace\":\"%s\",\"table\":\"%s\"}",
                        operation, nameSpace, tableName);
    }
}

/*
 * Format error response JSON
 */
void iceberg_format_error_response(StringInfo result, const char* operation,
                                  const char* nameSpace, const char* tableName,
                                  IcebergCatalogFdwState* fdwState)
{
    /* If we have a response body with error details, parse and format it */
    if (fdwState->response.responseBody &&
        strstr(fdwState->response.responseBody, "\"error\"")) {

        StringInfoData errorMsg;
        initStringInfo(&errorMsg);

        /* Extract error code */
        char *codeStart = strstr(fdwState->response.responseBody, "\"code\":");
        if (codeStart) {
            codeStart += 7; /* skip "code": */
            while (*codeStart == ' ') codeStart++; /* skip spaces */
            char *codeEnd = strchr(codeStart, ',');
            if (codeEnd) {
                appendStringInfo(&errorMsg, "Error Code: %.*s\n", (int)(codeEnd - codeStart), codeStart);
            }
        }

        /* Extract error type */
        char *typeStart = strstr(fdwState->response.responseBody, "\"type\":\"");
        if (typeStart) {
            typeStart += 8; /* skip "type":" */
            char *typeEnd = strchr(typeStart, '"');
            if (typeEnd) {
                appendStringInfo(&errorMsg, "Error Type: %.*s\n", (int)(typeEnd - typeStart), typeStart);
            }
        }

        /* Extract message */
        char *msgStart = strstr(fdwState->response.responseBody, "\"message\":\"");
        if (msgStart) {
            msgStart += 11; /* skip "message":" */
            char *msgEnd = strstr(msgStart, "\",\"");
            if (!msgEnd) msgEnd = strstr(msgStart, "\"}");
            if (msgEnd) {
                appendStringInfo(&errorMsg, "Message: %.*s\n", (int)(msgEnd - msgStart), msgStart);
            }
        }

        /* Extract and format stack trace */
        char *stackStart = strstr(fdwState->response.responseBody, "\"stack\":\"");
        if (stackStart && client_min_messages <= LOG) {
            stackStart += 14; /* skip "stack":" */
            char *stackEnd = strstr(stackStart, "\",\"");
            if (!stackEnd) stackEnd = strstr(stackStart, "\"}");
            if (stackEnd) {
                appendStringInfo(&errorMsg, "\nStack Trace:\n");

                /* Replace \\n with actual newlines and \\t with tabs */
                char *src = stackStart;
                while (src < stackEnd) {
                    if (src[0] == '\\' && src[1] == 'n') {
                        appendStringInfoChar(&errorMsg, '\n');
                        src += 2;
                    } else if (src[0] == '\\' && src[1] == 't') {
                        appendStringInfoChar(&errorMsg, '\t');
                        src += 2;
                    } else if (src[0] == '\\' && src[1] == '"') {
                        appendStringInfoChar(&errorMsg, '"');
                        src += 2;
                    } else {
                        appendStringInfoChar(&errorMsg, *src);
                        src++;
                    }
                }
            }
        }

        if (client_min_messages <= LOG) {
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg("Iceberg %s operation failed for %s.%s", operation, nameSpace, tableName),
                 errdetail("%s", errorMsg.data)));
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    errmsg("Iceberg %s operation failed for %s.%s. Check server logs or run 'SET client_min_messages=LOG;' for detailed stack trace.", operation, nameSpace, tableName),
                    errdetail("%s", errorMsg.data)));
        }

    }

    /* For other errors, format as before */
    StringInfoData errorDetails;
    initStringInfo(&errorDetails);

    appendStringInfo(&errorDetails, "HTTP Status: %d, CURL Code: %ld, Status: %d",
                    fdwState->response.httpStatus,
                    fdwState->response.curlCode,
                    fdwState->lastStatus);

    if (fdwState->response.errorMessage) {
        appendStringInfo(&errorDetails, "\nError: %s", fdwState->response.errorMessage);
    }

    appendStringInfo(&errorDetails, "\nTotal Time: %.3f, Retries: %d",
                    fdwState->response.totalTime,
                    fdwState->response.retryCount);

    ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
             errmsg("Iceberg %s operation failed for %s.%s", operation, nameSpace, tableName),
             errdetail("%s", errorDetails.data)));
}

/*
 * Setup common FDW state for catalog operations
 */
static IcebergCatalogFdwState* setup_catalog_fdw_state(IcebergCatalogOperation operation,
                                                       const char* nameSpace,
                                                       const char* tableName,
                                                       const char* catalogServer,
                                                       const char* catalogTable,
                                                       const char* volumeServer,
                                                       const char* volumeTable,
                                                       const char* appendJson)
{
    IcebergCatalogFdwState *fdwState = (IcebergCatalogFdwState*)palloc0(sizeof(IcebergCatalogFdwState));
    iceberg_setup_catalog_fdw_state(fdwState, operation, nameSpace, tableName,
                                   catalogServer, catalogTable, volumeServer, volumeTable);
    if (operation == ICEBERG_CREATE_TABLE || operation == ICEBERG_GET_FRAGMENT) {
        fdwState->request.schema = iceberg_build_schema_from_table_name(tableName);
	} else if (operation == ICEBERG_APPEND || operation == ICEBERG_COMMIT_FILE_GROUPS ||
			   operation == ICEBERG_COMMIT_APPEND || operation == ICEBERG_COMMIT_REWRITE) {
        fdwState->request.appendJson = pstrdup(appendJson);
	} else if (operation == ICEBERG_LOAD_TABLE || operation == ICEBERG_DROPTABLE ||
			   operation == ICEBERG_GET_STATISTICS || operation == ICEBERG_PLAN_FILE_GROUPS ||
			   operation == ICEBERG_RESTCATALOG_LISTCATALOG || operation == ICEBERG_RESTCATALOG_LISTNAMESPACE) {
		// nothing to setup
	} else if (operation == ICEBERG_UPDATE || operation == ICEBERG_COMMIT_UPDATE || operation == ICEBERG_COMMIT_DELETE) {
		fdwState->request.appendJson = pstrdup(appendJson);
    } else {
        const char* operation_name;
        switch (operation) {
            case ICEBERG_CREATE_TABLE: operation_name = "CREATE_TABLE"; break;
            case ICEBERG_APPEND: operation_name = "APPEND"; break;
            case ICEBERG_GET_FRAGMENT: operation_name = "GET_FRAGMENT"; break;
            case ICEBERG_LOAD_TABLE: operation_name = "LOAD_TABLE"; break;
            case ICEBERG_RESTCATALOG_LISTNAMESPACE: operation_name = "LIST_NAMESPCE";break;
            case ICEBERG_RESTCATALOG_LISTCATALOG: operation_name = "LIST_CATALOG";break;
            case ICEBERG_GET_STATISTICS: operation_name = "GET_STATISTICS"; break;
            default: operation_name = "UNKNOWN"; break;
        }
        elog(ERROR, "catalog operation not support: %s (%d)", operation_name, operation);
    }

    return fdwState;
}

/*
 * Execute CREATE TABLE operation
 */
static IcebergCatalogFdwState* execute_create_table_operation(const char* nameSpace,
                                                             const char* tableName,
                                                             const char* catalogServer,
                                                             const char* catalogTable,
                                                             const char* volumeServer,
                                                             const char* volumeTable)
{
    FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
    
    /* Use ModifyTableState for INSERT operations */
    ModifyTableState *mtstate = makeNode(ModifyTableState);
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    EState *estate = CreateExecutorState();

    resultRelInfo->ri_FdwRoutine = fdwRoutine;
    resultRelInfo->ri_RelationDesc = NULL;

    /* Setup FDW state */
    IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_CREATE_TABLE, nameSpace, tableName,
                                                              catalogServer, catalogTable, volumeServer, volumeTable, NULL);
    resultRelInfo->ri_FdwState = (void*)fdwState;

    mtstate->ps.plan = NULL;
    mtstate->ps.state = estate;
    mtstate->operation = CMD_INSERT;
    mtstate->mt_nrels = 1;
    mtstate->resultRelInfo = resultRelInfo;
    mtstate->rootResultRelInfo = resultRelInfo;

    /* Execute operation */
    if (fdwRoutine->BeginForeignInsert != NULL) {
        fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
    }

    if (fdwState->lastStatus != ICEBERG_SUCCESS) {
        return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
    }

    if (fdwRoutine->EndForeignInsert != NULL) {
        fdwRoutine->EndForeignInsert(estate, resultRelInfo);
    }

    return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute GET FRAGMENT operation
 */
static IcebergCatalogFdwState* execute_scan_operation(IcebergCatalogOperation operation,
                                                      const char* nameSpace,
                                                      const char* tableName,
                                                      const char* catalogServer,
                                                      const char* catalogTable,
                                                      const char* volumeServer,
                                                      const char* volumeTable)
{
    FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();

    /* Use ForeignScanState for SELECT operations */
    ForeignScanState *scanState = makeNode(ForeignScanState);
    EState *estate = CreateExecutorState();
    scanState->ss.ps.state = estate;

    /* Setup FDW state */
    IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(operation, nameSpace, tableName,
                                                              catalogServer, catalogTable, volumeServer, volumeTable, NULL);
    scanState->fdw_state = (void*)fdwState;

    /* Execute operation */
    if (fdwRoutine->BeginForeignScan != NULL) {
        fdwRoutine->BeginForeignScan(scanState, 0);
    }

    if (fdwRoutine->EndForeignScan != NULL) {
        fdwRoutine->EndForeignScan(scanState);
    }

    return (IcebergCatalogFdwState*)scanState->fdw_state;
}

/*
 * Execute APPEND operation
 */
static IcebergCatalogFdwState* execute_append_operation(const char* nameSpace,
                                                       const char* tableName,
                                                       const char* catalogServer,
                                                       const char* catalogTable,
                                                       const char* volumeServer,
                                                       const char* volumeTable,
                                                       const char* appendJsonStr)
{
    FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
    /* Use ModifyTableState for INSERT operations */
    ModifyTableState *mtstate = makeNode(ModifyTableState);
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    EState *estate = CreateExecutorState();

    resultRelInfo->ri_FdwRoutine = fdwRoutine;
    resultRelInfo->ri_RelationDesc = NULL;

    /* Setup FDW state */
    IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_APPEND, nameSpace, tableName,
                                                              catalogServer, catalogTable, volumeServer, volumeTable, appendJsonStr);
    resultRelInfo->ri_FdwState = (void*)fdwState;

    mtstate->ps.plan = NULL;
    mtstate->ps.state = estate;
    mtstate->operation = CMD_INSERT;
    mtstate->mt_nrels = 1;
    mtstate->resultRelInfo = resultRelInfo;
    mtstate->rootResultRelInfo = resultRelInfo;

    /* Execute operation */
    if (fdwRoutine->BeginForeignInsert != NULL) {
        fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
    }

    if (fdwState->lastStatus != ICEBERG_SUCCESS) {
        return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
    }

    /* Execute append operation */
    if (fdwRoutine->ExecForeignInsert != NULL) {
        fdwRoutine->ExecForeignInsert(estate, resultRelInfo, NULL, NULL);
    }

    if (fdwRoutine->EndForeignInsert != NULL) {
        fdwRoutine->EndForeignInsert(estate, resultRelInfo);
    }

    return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute Update operation
 */
static IcebergCatalogFdwState* execute_update_operation(const char* nameSpace,
                                                       const char* tableName,
                                                       const char* catalogServer,
                                                       const char* catalogTable,
                                                       const char* volumeServer,
                                                       const char* volumeTable,
                                                       const char* appendJsonStr)
{
    FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
    /* Use ModifyTableState for INSERT operations */
    ModifyTableState *mtstate = makeNode(ModifyTableState);
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    EState *estate = CreateExecutorState();

    resultRelInfo->ri_FdwRoutine = fdwRoutine;
    resultRelInfo->ri_RelationDesc = NULL;

    /* Setup FDW state */
    IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_UPDATE, nameSpace, tableName,
                                                              catalogServer, catalogTable, volumeServer, volumeTable, appendJsonStr);
    resultRelInfo->ri_FdwState = (void*)fdwState;

    mtstate->ps.plan = NULL;
    mtstate->ps.state = estate;
    mtstate->operation = CMD_INSERT;
    mtstate->mt_nrels = 1;
    mtstate->resultRelInfo = resultRelInfo;
    mtstate->rootResultRelInfo = resultRelInfo;

    /* Execute operation */
    if (fdwRoutine->BeginForeignInsert != NULL) {
        fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
    }

    if (fdwState->lastStatus != ICEBERG_SUCCESS) {
        return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
    }

    /* Execute update operation */
    if (fdwRoutine->ExecForeignInsert != NULL) {
        fdwRoutine->ExecForeignInsert(estate, resultRelInfo, NULL, NULL);
    }

    if (fdwRoutine->EndForeignInsert != NULL) {
        fdwRoutine->EndForeignInsert(estate, resultRelInfo);
    }

    return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute Drop operation
 */
static IcebergCatalogFdwState* execute_drop_operation(const char* nameSpace,
                                                       const char* tableName,
                                                       const char* catalogServer,
                                                       const char* catalogTable,
                                                       const char* volumeServer,
                                                       const char* volumeTable,
                                                       const char* dropJsonStr)
{
    FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
    /* Use ModifyTableState for INSERT operations */
    ModifyTableState *mtstate = makeNode(ModifyTableState);
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    EState *estate = CreateExecutorState();

    resultRelInfo->ri_FdwRoutine = fdwRoutine;
    resultRelInfo->ri_RelationDesc = NULL;

    /* Setup FDW state */
    IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_DROPTABLE, nameSpace, tableName,
                                                              catalogServer, catalogTable, volumeServer, volumeTable, dropJsonStr);
    resultRelInfo->ri_FdwState = (void*)fdwState;

    mtstate->ps.plan = NULL;
    mtstate->ps.state = estate;
    mtstate->operation = CMD_INSERT;
    mtstate->mt_nrels = 1;
    mtstate->resultRelInfo = resultRelInfo;
    mtstate->rootResultRelInfo = resultRelInfo;

    /* Execute operation */
    if (fdwRoutine->BeginForeignInsert != NULL) {
        fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
    }

    if (fdwState->lastStatus != ICEBERG_SUCCESS) {
        return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
    }

    /* Execute update operation */
    if (fdwRoutine->ExecForeignInsert != NULL) {
        fdwRoutine->ExecForeignInsert(estate, resultRelInfo, NULL, NULL);
    }

    if (fdwRoutine->EndForeignInsert != NULL) {
        fdwRoutine->EndForeignInsert(estate, resultRelInfo);
    }

    return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute COMMIT FILE GROUPS operation
 */
static IcebergCatalogFdwState* execute_commit_file_groups_operation(
													   const char* nameSpace,
													   const char* tableName,
													   const char* catalogServer,
													   const char* catalogTable,
													   const char* volumeServer,
													   const char* volumeTable,
													   const char* appendJsonStr)
{
	FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
	/* Use ModifyTableState for INSERT operations */
	ModifyTableState *mtstate = makeNode(ModifyTableState);
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	EState *estate = CreateExecutorState();

	resultRelInfo->ri_FdwRoutine = fdwRoutine;
	resultRelInfo->ri_RelationDesc = NULL;

	/* Setup FDW state */
	IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_COMMIT_FILE_GROUPS, nameSpace, tableName,
															  catalogServer, catalogTable, volumeServer, volumeTable, appendJsonStr);
	resultRelInfo->ri_FdwState = (void*)fdwState;

	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->mt_nrels = 1;
	mtstate->resultRelInfo = resultRelInfo;
	mtstate->rootResultRelInfo = resultRelInfo;

	/* Execute operation */
	if (fdwRoutine->BeginForeignInsert != NULL) {
		fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
	}

	if (fdwState->lastStatus != ICEBERG_SUCCESS) {
		return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
	}

	/* Execute commit file groups operation */
	if (fdwRoutine->ExecForeignInsert != NULL) {
		fdwRoutine->ExecForeignInsert(estate, resultRelInfo, NULL, NULL);
	}

	if (fdwRoutine->EndForeignInsert != NULL) {
		fdwRoutine->EndForeignInsert(estate, resultRelInfo);
	}

	return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute PLAN FILE GROUPS operation
 */
static IcebergCatalogFdwState* execute_plan_file_groups_operation_internal(
													  const char* nameSpace,
													  const char* tableName,
													  const char* catalogServer,
													  const char* catalogTable,
													  const char* volumeServer,
													  const char* volumeTable,
													  int minInputFiles,
													  int targetFileSizeMb)
{
	FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();

	/* Use ForeignScanState for scan operations */
	ForeignScanState *scanState = makeNode(ForeignScanState);
	EState *estate = CreateExecutorState();
	scanState->ss.ps.state = estate;

	/* Setup FDW state */
	IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(ICEBERG_PLAN_FILE_GROUPS, nameSpace, tableName,
															  catalogServer, catalogTable, volumeServer, volumeTable, NULL);
	fdwState->request.minInputFiles = minInputFiles;
	fdwState->request.targetFileSizeMb = targetFileSizeMb;
	scanState->fdw_state = (void*)fdwState;

	/* Execute operation */
	if (fdwRoutine->BeginForeignScan != NULL) {
		fdwRoutine->BeginForeignScan(scanState, 0);
	}

	if (fdwRoutine->EndForeignScan != NULL) {
		fdwRoutine->EndForeignScan(scanState);
	}

	return (IcebergCatalogFdwState*)scanState->fdw_state;
}

/*
 * Public wrapper for plan file groups operation
 */
IcebergCatalogFdwState* iceberg_execute_plan_file_groups_operation(
	const char* nameSpace, const char* tableName,
	const char* catalogServer, const char* catalogTable,
	const char* volumeServer, const char* volumeTable,
	int minInputFiles, int targetFileSizeMb)
{
	return execute_plan_file_groups_operation_internal(nameSpace, tableName,
		catalogServer, catalogTable, volumeServer, volumeTable,
		minInputFiles, targetFileSizeMb);
}

/*
 * Execute a generic modify operation with a specified operation type.
 * Used for COMMIT_APPEND, COMMIT_UPDATE, COMMIT_REWRITE which reuse
 * the same JSON formats as APPEND/UPDATE/COMMIT_FILE_GROUPS but dispatch
 * to different agent endpoints.
 */
static IcebergCatalogFdwState* execute_generic_modify_operation(
													IcebergCatalogOperation operation,
													const char* nameSpace,
													const char* tableName,
													const char* catalogServer,
													const char* catalogTable,
													const char* volumeServer,
													const char* volumeTable,
													const char* appendJsonStr)
{
	FdwRoutine *fdwRoutine = iceberg_get_catalog_fdw_routine();
	ModifyTableState *mtstate = makeNode(ModifyTableState);
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
	EState *estate = CreateExecutorState();

	resultRelInfo->ri_FdwRoutine = fdwRoutine;
	resultRelInfo->ri_RelationDesc = NULL;

	IcebergCatalogFdwState *fdwState = setup_catalog_fdw_state(operation, nameSpace, tableName,
															   catalogServer, catalogTable, volumeServer, volumeTable, appendJsonStr);
	resultRelInfo->ri_FdwState = (void*)fdwState;

	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->mt_nrels = 1;
	mtstate->resultRelInfo = resultRelInfo;
	mtstate->rootResultRelInfo = resultRelInfo;

	if (fdwRoutine->BeginForeignInsert != NULL) {
		fdwRoutine->BeginForeignInsert(mtstate, resultRelInfo);
	}

	if (fdwState->lastStatus != ICEBERG_SUCCESS) {
		return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
	}

	if (fdwRoutine->ExecForeignInsert != NULL) {
		fdwRoutine->ExecForeignInsert(estate, resultRelInfo, NULL, NULL);
	}

	if (fdwRoutine->EndForeignInsert != NULL) {
		fdwRoutine->EndForeignInsert(estate, resultRelInfo);
	}

	return (IcebergCatalogFdwState*)resultRelInfo->ri_FdwState;
}

/*
 * Execute catalog operation and return result state
 */
IcebergCatalogFdwState* iceberg_execute_catalog_operation(IcebergCatalogOperation operation,
                                                          const char* nameSpace,
                                                          const char* tableName,
                                                          const char* catalogServer,
                                                          const char* catalogTable,
                                                          const char* volumeServer,
                                                          const char* volumeTable,
                                                          const char* appendJson)
{
    switch (operation) {
        case ICEBERG_CREATE_TABLE:
            return execute_create_table_operation(nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable);
        case ICEBERG_GET_FRAGMENT:
            return execute_scan_operation(operation, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable);
        case ICEBERG_APPEND:
            return execute_append_operation(nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
        case ICEBERG_LOAD_TABLE:
            return execute_scan_operation(operation, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable);
        case ICEBERG_UPDATE:
            return execute_update_operation(nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
        case ICEBERG_RESTCATALOG_LISTNAMESPACE:
        case ICEBERG_RESTCATALOG_LISTCATALOG:
            return execute_scan_operation(operation, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable);
        case ICEBERG_DROPTABLE:
            return execute_drop_operation(nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
        case ICEBERG_GET_STATISTICS:
            return execute_scan_operation(operation, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable);
		case ICEBERG_COMMIT_FILE_GROUPS:
			return execute_commit_file_groups_operation(nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
		case ICEBERG_COMMIT_APPEND:
			return execute_generic_modify_operation(ICEBERG_COMMIT_APPEND, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
		case ICEBERG_COMMIT_UPDATE:
			return execute_generic_modify_operation(ICEBERG_COMMIT_UPDATE, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
		case ICEBERG_COMMIT_DELETE:
			return execute_generic_modify_operation(ICEBERG_COMMIT_DELETE, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
		case ICEBERG_COMMIT_REWRITE:
			return execute_generic_modify_operation(ICEBERG_COMMIT_REWRITE, nameSpace, tableName, catalogServer, catalogTable, volumeServer, volumeTable, appendJson);
        default:
            elog(ERROR, "Unsupported catalog operation: %d", operation);
            return NULL;
    }
}

/*
 * Setup catalog FDW state with common parameters
 */
static void setup_buildin_catalog_metadata(IcebergCatalogFdwState* fdwState,
                                           IcebergCatalogOptions *catalogOption,
                                           const char* tableName)
{
    /* Only for builtin catalog */
    if (pg_strcasecmp(catalogOption->catalog_server.server_type, DATALAKEFDW_ICEBERG_SERVER_BUILTIN) != 0) {
        fdwState->request.buildInCatalog.tableExists = false;
        fdwState->request.buildInCatalog.metadataLocation = NULL;
        return;
    }

    Oid relid = RelnameGetRelid(tableName);
    
    if (!OidIsValid(relid)) {
        fdwState->request.buildInCatalog.tableExists = false;
        fdwState->request.buildInCatalog.metadataLocation = NULL;
        return;
    }

    /* Iceberg native catalog: OIDs are pinned at initdb (iceberg-cdbinit). */
    Oid metadata_relid = ICEBERG_METADATA_RELID;
    Oid index_oid = ICEBERG_METADATA_PKEY_OID;

    /* Query pg_iceberg_metadata */
    Relation metadataRel = table_open(metadata_relid, AccessShareLock);
    ScanKeyData key;
    SysScanDesc scan;
    
    ScanKeyInit(&key, 1, /* relid column is first */
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    
    scan = systable_beginscan(metadataRel, index_oid, true, NULL, 1, &key);
    
    HeapTuple tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        bool isnull;
        Datum metadataLocationDatum = heap_getattr(tuple, 2, /* metadata_location is 2nd column */
                                                   RelationGetDescr(metadataRel), &isnull);
        
        fdwState->request.buildInCatalog.tableExists = true;
        fdwState->request.buildInCatalog.metadataLocation = isnull ? NULL : 
            pstrdup(TextDatumGetCString(metadataLocationDatum));
    } else {
        fdwState->request.buildInCatalog.tableExists = false;
        fdwState->request.buildInCatalog.metadataLocation = NULL;
    }
    
    systable_endscan(scan);
    table_close(metadataRel, AccessShareLock);
}

void iceberg_setup_catalog_fdw_state(IcebergCatalogFdwState* fdwState,
                                     IcebergCatalogOperation operation,
                                     const char* nameSpace, const char* tableName,
                                     const char* catalogServer, const char* catalogTable,
                                     const char* volumeServer, const char* volumeTable)
{
    fdwState->catalogOperation = operation;
    fdwState->request.tableName = tableName ? pstrdup(tableName) : NULL;
    fdwState->request.nameSpace = nameSpace ? pstrdup(nameSpace) : NULL;
    fdwState->request.agentServerUrl = pstrdup("http://localhost:3888");
    fdwState->lastStatus = ICEBERG_SUCCESS;
    fdwState->catalogInfo.catalog_name = catalogTable ? pstrdup(catalogTable) : NULL;
    fdwState->catalogInfo.catalog_server_name = catalogServer ? pstrdup(catalogServer) : NULL;
    fdwState->catalogInfo.volumn_server_name = volumeServer ? pstrdup(volumeServer) : NULL;
    fdwState->catalogInfo.volumn_name = volumeTable ? pstrdup(volumeTable) : NULL;

    /* Setup buildin catalog metadata if applicable */
    IcebergCatalogOptions *catalogOption = getIcebergCatalogOptions(catalogServer, catalogTable);
    setup_buildin_catalog_metadata(fdwState, catalogOption, tableName);
}
