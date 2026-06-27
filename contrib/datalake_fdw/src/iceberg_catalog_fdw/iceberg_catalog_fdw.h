/*
 * iceberg_catalog_fdw.h
 *      Header file for Iceberg catalog foreign data wrapper
 */

#ifndef ICEBERG_CATALOG_FDW_H
#define ICEBERG_CATALOG_FDW_H

#include "iceberg_catalog_option.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

/*
 * Forward declarations
 */
typedef struct IcebergColumnDef IcebergColumnDef;
typedef struct IcebergTableSchema IcebergTableSchema;
typedef enum {
    ICEBERG_CREAT_TABLE_ALREADY_EXISTS = 1,
    ICEBERG_SUCCESS = 0,
    ICEBERG_ERROR_INVALID_PARAM = -1,
    ICEBERG_ERROR_NETWORK = -2,
    ICEBERG_ERROR_JSON_PARSE = -3,
    ICEBERG_ERROR_HTTP = -4,
    ICEBERG_ERROR_MEMORY = -5,
    ICEBERG_ERROR_TRANSPORT = -6,
    ICEBERG_ERROR_TIMEOUT = -7,
    ICEBERG_ERROR_THREAD = -8,
    ICEBERG_ERROR_AUTH = -9,
    ICEBERG_ERROR_NOT_FOUND = -10,
    ICEBERG_ERROR_CONFLICT = -11
} IcebergCatalogStatus;

typedef enum {
    ICEBERG_CREATE_TABLE = 1,
    ICEBERG_APPEND,
    ICEBERG_GET_FRAGMENT,
    ICEBERG_LOAD_TABLE,
    ICEBERG_UPDATE,
    ICEBERG_DELETE,
    ICEBERG_DROPTABLE,
    ICEBERG_RESTCATALOG_LISTNAMESPACE,
    ICEBERG_RESTCATALOG_LISTCATALOG,
	ICEBERG_GET_STATISTICS,
	ICEBERG_PLAN_FILE_GROUPS,
	ICEBERG_COMMIT_FILE_GROUPS,
	ICEBERG_COMMIT_APPEND,    /* PRE_COMMIT append (AppendFiles + commit) */
	ICEBERG_COMMIT_UPDATE,    /* PRE_COMMIT update (RowDelta + commit) */
	ICEBERG_COMMIT_DELETE,    /* PRE_COMMIT delete (RowDelta + commit) */
	ICEBERG_COMMIT_REWRITE,   /* VACUUM commit (RewriteFiles + commit) */
	ICEBERG_TRUNCATE          /* TRUNCATE: metadata-only delete of all rows */
} IcebergCatalogOperation;

typedef enum {
	ICEBERG_FILETYPE_DATA = 0,
	ICEBERG_FILETYPE_POSITION_DELETES,
	ICEBERG_FILETYPE_EQUALITY_DELETES,
	ICEBERG_FILETYPE_DELTA_LOG
} IcebergFileType;

/*
 * Catalog operation response
 */
typedef struct IcebergCatalogResponse {
    int httpStatus;
    long curlCode;
    char* responseBody;
    size_t responseSize;
    char* errorMessage;
    double totalTime;
    int retryCount;
} IcebergCatalogResponse;

/*
 * BuildIn Catalog info
 */
typedef struct IcebergBuildInCatalogRequest {
    bool tableExists;
    const char* metadataLocation;
} IcebergBuildInCatalogRequest;

typedef struct IcebergColumnDef {
    char* columnName;
    Oid   dataType;
    int32 typeModifier;
    bool  isNullable;
    char* comment;
} IcebergColumnDef;

typedef struct IcebergTableSchema {
    List* columns;          // List of IcebergColumnDef*
    //TODO: need to support partiton table
    List* partitionColumns; // List of char* (partiton column name)
} IcebergTableSchema;

/*
 * Catalog table request
 */
typedef struct IcebergCatalogRequest {
    const char* tableName;
    const char* nameSpace;
    IcebergTableSchema* schema;
    const char* agentServerUrl;
    const char* appendJson;
    IcebergBuildInCatalogRequest buildInCatalog;
    IcebergFileType fileType;
	int minInputFiles;
	int targetFileSizeMb;
	const char* location;  /* pre-formatted location from AM layer, must be non-NULL */
	const char* metadataLocation;  /* deferred commit temp metadata location for RYOW */
	const char* pushdownFilter;  /* serialized dlproxy scan filter for data-file pruning (get-fragment only) */
} IcebergCatalogRequest;

typedef struct IcebergCatalogInfo {
    const char* catalog_name;
    const char* catalog_server_name;
    const char* volumn_name;
    const char* volumn_server_name;
} IcebergCatalogInfo;

typedef struct IcebergCatalogFdwState {
    /* Resource management */
    void* catalogHandle;

    /* Operation context */
    IcebergCatalogOperation catalogOperation;
    IcebergCatalogInfo catalogInfo;
    IcebergCatalogRequest request;
    IcebergCatalogResponse response;

    /* Memory context for cleanup */
    MemoryContext fdwContext;

    /* State information */
    IcebergCatalogStatus lastStatus;
} IcebergCatalogFdwState;

void check_catalog_fdw_exec_error(IcebergCatalogFdwState *fdwState, const char *error_prefix);

/* Catalog management functions */
void iceberg_catalog_create_catalog(CreateForeignCatalogStmt *createCatalogStmt);

#endif /* ICEBERG_CATALOG_FDW_H */
