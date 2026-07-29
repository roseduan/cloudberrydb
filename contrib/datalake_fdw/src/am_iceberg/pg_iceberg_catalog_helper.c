/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog_helper.c
 *    Client implementation for Iceberg catalog operations
 *
 * This file contains the client-side implementation for executing Iceberg
 * catalog operations through the catalog FDW infrastructure.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_catalog_helper.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"

#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_catalog_utils.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_rewrite_plan.h"
#include "include/pg_iceberg_deletion_queue.h"
#include "include/pg_iceberg_am_handler.h"
#include "include/pg_iceberg_metadata_tracker.h"
#include "utils/timestamp.h"

/*
 * pg_iceberg_validate_object_name
 *		Reject iceberg table / namespace names that are unsafe across catalog
 *		backends.  See header for the rationale; the rule mirrors a Spark
 *		regular identifier (and is a subset of Hive's [a-zA-Z_0-9]+):
 *		non-empty, every byte in [A-Za-z0-9_], and not all digits.
 */
void
pg_iceberg_validate_object_name(const char *name, const char *kind)
{
	const unsigned char *p;
	bool		has_nondigit = false;

	if (name == NULL || name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("iceberg %s name must not be empty", kind)));

	for (p = (const unsigned char *) name; *p != '\0'; p++)
	{
		unsigned char c = *p;

		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_')
			has_nondigit = true;
		else if (c >= '0' && c <= '9')
			 /* digit: allowed, but cannot be the only kind of character */ ;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("iceberg %s name \"%s\" is not allowed", kind, name),
					 errhint("only ASCII letters, digits and underscores are allowed, and the name cannot be all digits")));
	}

	if (!has_nondigit)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("iceberg %s name \"%s\" is not allowed", kind, name),
				 errhint("only ASCII letters, digits and underscores are allowed, and the name cannot be all digits")));
}

/*
 * Three-tier iceberg namespace resolver.  See header for contract.
 *
 * The previous code path picked one source per callsite (PG schema for
 * internal tables, opts->namespace for external) and never consulted the
 * catalog's default_namespace.  That made the foreign catalog option
 * dead code, and prevented internal tables from overriding the namespace
 * at all.  Funneling every callsite through this helper keeps the three
 * sources at known precedence and ensures the resolved value is the only
 * thing that reaches the FDW request payload / agent.
 */
const char *
pg_iceberg_resolve_namespace(const char *options_namespace,
							 const char *catalog_server_name,
							 const char *catalog_name,
							 Oid schema_oid)
{
	char	   *nspname;

	/* Tier 1: explicit table OPTIONS namespace. */
	if (options_namespace != NULL && options_namespace[0] != '\0')
		return pstrdup(options_namespace);

	/* Tier 2: foreign catalog default_namespace. */
	if (catalog_server_name != NULL && catalog_name != NULL)
	{
		IcebergCatalogOptions *cat = getIcebergCatalogOptions(catalog_server_name,
															  catalog_name);
		if (cat != NULL &&
			cat->foreign_catalog.default_namespace != NULL &&
			cat->foreign_catalog.default_namespace[0] != '\0')
			return pstrdup(cat->foreign_catalog.default_namespace);
	}

	/*
	 * Tier 3: PG schema name behind schema_oid. A caller that has no schema
	 * at hand passes InvalidOid because it always expects tier 1 to win.
	 * Raise an ereport in that case rather than inventing a namespace: a
	 * hardcoded "public" here would silently route data of a table living in
	 * some other schema into the wrong iceberg namespace, which is strictly
	 * worse than a loud failure.
	 */
	if (!OidIsValid(schema_oid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg namespace cannot be resolved: "
						"no OPTIONS namespace, no catalog default_namespace, "
						"and no PG schema for fallback")));

	nspname = get_namespace_name(schema_oid);
	if (nspname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("iceberg namespace cannot be resolved: "
						"schema with OID %u no longer exists", schema_oid)));

	return nspname;
}

static FdwRoutine *
get_catalog_fdw_routine(void)
{
	ForeignDataWrapper *fdw;

	fdw = GetForeignDataWrapperByName("iceberg_catalog_fdw", false);
	return GetFdwRoutine(fdw->fdwhandler);
}

static IcebergCatalogFdwState *
create_catalog_fdw_state(IcebergCatalogOperation operation,
						 const char *catalogName,
						 const char *nameSpace,
						 const char *tableName,
						 const char *catalogServer,
						 const char *foreignCatalogName,
						 const char *volumeServer,
						 const char *volumeName,
						 IcebergTableSchema *schema)
{
	IcebergCatalogFdwState *fdwState;

	fdwState = (IcebergCatalogFdwState *) palloc0(sizeof(IcebergCatalogFdwState));

	fdwState->catalogOperation = operation;
	//fdwState->request.catalogName = catalogName;
	fdwState->request.tableName = tableName;
	fdwState->request.nameSpace = nameSpace;
	fdwState->request.agentServerUrl = "http://localhost:3888";
	fdwState->catalogInfo.catalog_name = foreignCatalogName;
	fdwState->catalogInfo.catalog_server_name = catalogServer;
	fdwState->catalogInfo.volumn_server_name = volumeServer;
	fdwState->catalogInfo.volumn_name = volumeName;
	fdwState->request.schema = schema;

	return fdwState;
}

static IcebergCatalogFdwState *
execute_create_table_via_fdw(const char *catalogName,
							 const char *nameSpace,
							 const char *tableName,
							 const char *catalogServer,
							 const char *foreignCatalogName,
							 const char *volumeServer,
							 const char *volumeName,
							 const char *location,
							 IcebergTableSchema *schema)
{
	FdwRoutine		   *fdwRoutine;
	ResultRelInfo	   *resultRelInfo;

	resultRelInfo = makeNode(ResultRelInfo);

	fdwRoutine = get_catalog_fdw_routine();
	resultRelInfo->ri_FdwState = create_catalog_fdw_state(ICEBERG_CREATE_TABLE,
														  catalogName,
														  nameSpace,
														  tableName,
														  catalogServer,
														  foreignCatalogName,
														  volumeServer,
														  volumeName,
														  schema);
	((IcebergCatalogFdwState *) resultRelInfo->ri_FdwState)->request.location = location;

	fdwRoutine->BeginForeignInsert(NULL, resultRelInfo);
	fdwRoutine->EndForeignInsert(NULL, resultRelInfo);

	return resultRelInfo->ri_FdwState;
}

static IcebergCatalogFdwState *
execute_get_fragments_via_fdw(const char *metadata_location,
							  bool is_internal,
							  const char *pushdown_filter,
							  const char *catalogName,
							  const char *nameSpace,
							  const char *tableName,
							  const char *catalogServer,
							  const char *foreignCatalogName,
							  const char *volumeServer,
							  const char *volumeName,
							  IcebergTableSchema *schema,
							  int64 snapshot_id)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_GET_FRAGMENT,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	/*
	 * Issue #338: always populate request.metadataLocation so
	 * createCreateRequestJson() emits the deferred-RYOW "metadata_location"
	 * property for every catalog server type.  Without this, an Iceberg AM
	 * table created on a non-builtin catalog (e.g. type='s3') with no
	 * explicit OPTIONS clause has is_internal=true AND server_type !=
	 * builtin, so the JSON sent to the agent has no metadata_location key,
	 * the agent falls back to the external catalog's current pointer (still
	 * pre-this-statement inside a multi-statement transaction), and same-tx
	 * Read-Your-Own-Writes silently breaks.  buildInCatalog.metadataLocation
	 * is also kept for the builtin-catalog properties path.
	 */
	fdwState->request.metadataLocation = metadata_location;
	if (is_internal)
	{
		fdwState->request.buildInCatalog.metadataLocation = metadata_location;
		fdwState->request.buildInCatalog.tableExists = true;
	}

	/* Carry the serialized scan filter so the agent can prune data files. */
	fdwState->request.pushdownFilter = pushdown_filter;

	/* Time travel: the target snapshot (0 = HEAD) for the agent to useSnapshot. */
	fdwState->request.snapshotId = snapshot_id;

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

static IcebergCatalogFdwState *
execute_get_statistics_via_fdw(const char *metadata_location,
							   bool is_internal,
							   const char *catalogName,
							   const char *nameSpace,
							   const char *tableName,
							   const char *catalogServer,
							   const char *foreignCatalogName,
							   const char *volumeServer,
							   const char *volumeName,
							   IcebergTableSchema *schema)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_GET_STATISTICS,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	/* Issue #338: see execute_get_fragments_via_fdw for the rationale.
	 * Statistics during a multi-statement transaction must reflect the
	 * uncommitted metadata location too, or planner cardinality goes stale
	 * between the first DML and the next. */
	fdwState->request.metadataLocation = metadata_location;
	if (is_internal)
	{
		fdwState->request.buildInCatalog.metadataLocation = metadata_location;
		fdwState->request.buildInCatalog.tableExists = true;
	}

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

/* Time travel: resolve a specific snapshot's schema (getSnapshotSchema). */
static IcebergCatalogFdwState *
execute_get_snapshot_schema_via_fdw(const char *metadata_location,
									int64 snapshot_id,
									const char *catalogName,
									const char *nameSpace,
									const char *tableName,
									const char *catalogServer,
									const char *foreignCatalogName,
									const char *volumeServer,
									const char *volumeName,
									IcebergTableSchema *schema)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_GET_SNAPSHOT_SCHEMA,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	fdwState->request.metadataLocation = metadata_location;
	fdwState->request.buildInCatalog.metadataLocation = metadata_location;
	/*
	 * The pure-builtin catalog resolves the table from this flag +
	 * metadata_location; without it the agent derives a bogus location from
	 * the warehouse root.  Catalogs with a warehouse prefix resolve by name
	 * and ignore it, which is how its absence went unnoticed.
	 */
	fdwState->request.buildInCatalog.tableExists = true;
	fdwState->request.snapshotId = snapshot_id;

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

/* Time travel: list the snapshots of the pinned metadata (getSnapshots). */
static IcebergCatalogFdwState *
execute_get_snapshots_via_fdw(const char *metadata_location,
							  const char *catalogName,
							  const char *nameSpace,
							  const char *tableName,
							  const char *catalogServer,
							  const char *foreignCatalogName,
							  const char *volumeServer,
							  const char *volumeName,
							  IcebergTableSchema *schema)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_GET_SNAPSHOTS,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	fdwState->request.metadataLocation = metadata_location;
	fdwState->request.buildInCatalog.metadataLocation = metadata_location;
	/* See execute_get_snapshot_schema_via_fdw: required by pure-builtin. */
	fdwState->request.buildInCatalog.tableExists = true;

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

static IcebergCatalogFdwState *
execute_plan_file_groups_via_fdw(const char *metadata_location,
								 bool is_internal,
								 int min_input_files,
								 int target_file_size_mb,
								 const char *catalogName,
								 const char *nameSpace,
								 const char *tableName,
								 const char *catalogServer,
								 const char *foreignCatalogName,
								 const char *volumeServer,
								 const char *volumeName,
								 IcebergTableSchema *schema)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_PLAN_FILE_GROUPS,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	if (is_internal)
	{
		fdwState->request.buildInCatalog.metadataLocation = metadata_location;
		fdwState->request.buildInCatalog.tableExists = true;
	}
	else
	{
		fdwState->request.metadataLocation = metadata_location;
	}

	fdwState->request.minInputFiles = min_input_files;
	fdwState->request.targetFileSizeMb = target_file_size_mb;

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

static IcebergCatalogFdwState *
execute_catalog_op_via_fdw(IcebergCatalogOperation op,
						   const char *data_locations,
						   const char *metadata_location,
						   bool is_internal,
						   const char *catalogName,
						   const char *nameSpace,
						   const char *tableName,
						   const char *catalogServer,
						   const char *foreignCatalogName,
						   const char *volumeServer,
						   const char *volumeName,
						   IcebergTableSchema *schema)

{
	IcebergCatalogFdwState *fdwState;
	FdwRoutine *fdwRoutine = get_catalog_fdw_routine();
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);

	fdwState = create_catalog_fdw_state(op,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										schema);

	if (is_internal)
	{
		fdwState->request.buildInCatalog.metadataLocation = metadata_location;
		fdwState->request.buildInCatalog.tableExists = true;
	}
	else
	{
		fdwState->request.metadataLocation = metadata_location;
	}

	fdwState->request.appendJson = data_locations;

	resultRelInfo->ri_FdwState = fdwState;

	fdwRoutine->BeginForeignInsert(NULL, resultRelInfo);
	fdwRoutine->ExecForeignInsert(NULL, resultRelInfo, NULL, NULL);
	fdwRoutine->EndForeignInsert(NULL, resultRelInfo);

	return (IcebergCatalogFdwState*) resultRelInfo->ri_FdwState;
}

static IcebergCatalogFdwState *
execute_load_table_via_fdw(const char *catalogName,
						   const char *nameSpace,
						   const char *tableName,
						   const char *catalogServer,
						   const char *foreignCatalogName,
						   const char *volumeServer,
						   const char *volumeName)
{
	FdwRoutine		   *fdwRoutine;
	ForeignScanState   *scanstate;
	IcebergCatalogFdwState *fdwState;

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_LOAD_TABLE,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										NULL);

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);

	return (IcebergCatalogFdwState *) scanstate->fdw_state;
}

static char *
extract_location_from_fdw_state(IcebergCatalogFdwState *fdwState)
{
	check_fdw_execution_error(fdwState, "Failed to create Iceberg table");
	return parse_metadata_location(fdwState->response.responseBody);
}

static IcebergLoadTableResult *
extract_load_table_result_from_fdw_state(IcebergCatalogFdwState *fdwState)
{
	/* Table not found is not an error — return NULL so callers can create it */
	if (is_table_not_found_error(fdwState))
		return NULL;

	check_fdw_execution_error(fdwState, "Failed to load Iceberg table");
	return parse_load_table_response(fdwState->response.responseBody);
}

static char *
extract_response_body_from_fdw_state(IcebergCatalogFdwState *fdwState,
									 const char *what)
{
	check_fdw_execution_error(fdwState, what);
	return fdwState->response.responseBody;
}

static IcebergTableStatistics *
extract_statistics_from_fdw_state(IcebergCatalogFdwState *fdwState)
{
	check_fdw_execution_error(fdwState, "Failed to get Iceberg table statistics");
	return parse_statistics_response(fdwState->response.responseBody);
}

/*
 * Extract the new metadata-location from a catalog-op response and, when
 * written_out is non-NULL, also the "written-metadata-files" array (the
 * metadata-layer files the agent newly wrote) for transaction-level orphan
 * cleanup (issue #399).
 */
static char *
extract_locations_and_written_from_fdw_state(IcebergCatalogFdwState *fdwState,
											  List **written_out)
{
	check_fdw_execution_error(fdwState, "Failed to catalog operation");
	return parse_metadata_location_ex(fdwState->response.responseBody,
									  written_out);
}

char *
pg_iceberg_catalog_op_ex(Relation relation,
						 IcebergCatalogOperation op,
						 const char *catalogName,
						 const char *nameSpace,
						 const char *tableName,
						 const char *data_locations,
						 const char *metadata_location,
						 bool is_internal,
						 const char *catalogServer,
						 const char *foreignCatalogName,
						 const char *volumeServer,
						 const char *volumeName,
						 List **written_metadata_files)
{
	IcebergCatalogFdwState *fdwState;
	IcebergTableSchema	   *schema = NULL;

	if (written_metadata_files != NULL)
		*written_metadata_files = NIL;

	if (relation != NULL)
		schema = build_schema_from_pg_table(relation);

	fdwState = execute_catalog_op_via_fdw(op,
										  data_locations,
										  metadata_location,
										  is_internal,
										  catalogName,
										  nameSpace,
										  tableName,
										  catalogServer,
										  foreignCatalogName,
										  volumeServer,
										  volumeName,
										  schema);
	if (schema != NULL)
		free_schema_info(schema);

	return extract_locations_and_written_from_fdw_state(fdwState,
														written_metadata_files);
}

char *
pg_iceberg_catalog_op(Relation relation,
					  IcebergCatalogOperation op,
					  const char *catalogName,
					  const char *nameSpace,
					  const char *tableName,
					  const char *data_locations,
					  const char *metadata_location,
					  bool is_internal,
					  const char *catalogServer,
					  const char *foreignCatalogName,
					  const char *volumeServer,
					  const char *volumeName)
{
	return pg_iceberg_catalog_op_ex(relation,
									op,
									catalogName,
									nameSpace,
									tableName,
									data_locations,
									metadata_location,
									is_internal,
									catalogServer,
									foreignCatalogName,
									volumeServer,
									volumeName,
									NULL);
}

/*
 * Fire an ICEBERG_UPDATE_SCHEMA op (ALTER TABLE schema evolution, builtin only,
 * issue #401).  Mirrors execute_catalog_op_via_fdw but carries the schema-op list
 * (List of IcebergSchemaOp*) instead of data locations, and always runs as the
 * builtin/internal path so the agent loads the exact current table by metadata.
 */
static IcebergCatalogFdwState *
execute_update_schema_via_fdw(const char *metadata_location,
							  List *schemaOps,
							  const char *catalogName,
							  const char *nameSpace,
							  const char *tableName,
							  const char *catalogServer,
							  const char *foreignCatalogName,
							  const char *volumeServer,
							  const char *volumeName)
{
	IcebergCatalogFdwState *fdwState;
	FdwRoutine *fdwRoutine = get_catalog_fdw_routine();
	ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);

	fdwState = create_catalog_fdw_state(ICEBERG_UPDATE_SCHEMA,
										catalogName,
										nameSpace,
										tableName,
										catalogServer,
										foreignCatalogName,
										volumeServer,
										volumeName,
										NULL /* no schema needed; agent loads by metadata */);

	fdwState->request.buildInCatalog.metadataLocation = metadata_location;
	fdwState->request.buildInCatalog.tableExists = true;
	fdwState->request.schemaOps = schemaOps;

	resultRelInfo->ri_FdwState = fdwState;

	fdwRoutine->BeginForeignInsert(NULL, resultRelInfo);
	fdwRoutine->ExecForeignInsert(NULL, resultRelInfo, NULL, NULL);
	fdwRoutine->EndForeignInsert(NULL, resultRelInfo);

	return (IcebergCatalogFdwState *) resultRelInfo->ri_FdwState;
}

char *
pg_iceberg_update_schema_op(const char *catalogName,
							const char *nameSpace,
							const char *tableName,
							const char *metadata_location,
							const char *catalogServer,
							const char *foreignCatalogName,
							const char *volumeServer,
							const char *volumeName,
							List *schemaOps)
{
	IcebergCatalogFdwState *fdwState;

	fdwState = execute_update_schema_via_fdw(metadata_location,
											 schemaOps,
											 catalogName,
											 nameSpace,
											 tableName,
											 catalogServer,
											 foreignCatalogName,
											 volumeServer,
											 volumeName);

	return extract_location_from_fdw_state(fdwState);
}

IcebergLoadTableResult *
pg_iceberg_load_table(const char *catalogName,
					  const char *nameSpace,
					  const char *tableName,
					  const char *catalogServer,
					  const char *foreignCatalogName,
					  const char *volumeServer,
					  const char *volumeName)
{
	IcebergCatalogFdwState *fdwState;

	fdwState = execute_load_table_via_fdw(catalogName,
										  nameSpace,
										  tableName,
										  catalogServer,
										  foreignCatalogName,
											  volumeServer,
											  volumeName);
	return extract_load_table_result_from_fdw_state(fdwState);
}

void
pg_iceberg_free_load_table_result(IcebergLoadTableResult *result)
{
	if (result == NULL)
		return;

	if (result->metadata_location != NULL)
		pfree(result->metadata_location);
	if (result->catalog_properties != NULL)
		pfree(result->catalog_properties);
	if (result->location != NULL)
		pfree(result->location);
	if (result->partition_spec_summary != NULL)
		pfree(result->partition_spec_summary);
	pfree(result);
}

/*
 * pg_iceberg_load_metadata_json
 *		Fetch a builtin table's metadata.json verbatim through dlagent, using
 *		the volume's own object storage credentials.  Exists so the
 *		datalake_rest_catalog gateway does not need object storage
 *		credentials of its own (see pg_iceberg_metadata_reader.c).
 *
 *		Both out parameters are palloc'd in the caller's context.
 *
 *		The metadata location is NEVER taken from the caller: it is always
 *		derived from the catalog registration for this relid.  Letting a
 *		caller name the path would turn this into an arbitrary-fetch
 *		primitive.
 */
void
pg_iceberg_load_metadata_json(Oid relid,
							  char **metadata_location,
							  char **metadata_json)
{
	IcebergTableInfo		   *table_info;
	IcebergCatalogFdwState	   *fdwState;
	FdwRoutine				   *fdwRoutine;
	ForeignScanState		   *scanstate;
	Relation					rel;
	const char				   *nameSpace;
	char					   *tableName;
	char					   *location;
	Size						json_len;

	Assert(metadata_location != NULL && metadata_json != NULL);

	/*
	 * Caller MUST run this on the QD: it consults the local
	 * pg_iceberg_metadata catalog (which is QD-only populated) via
	 * pg_iceberg_tracker_get_scan_metadata_location(), the same constraint
	 * documented on pg_iceberg_list_data_fragments_json() (see
	 * pg_iceberg_am.c).  A QE has no rows there, so fail loudly here rather
	 * than risk a wrong or empty metadata document being returned from a
	 * segment.  This check is the ONLY guard: EXECUTE ON COORDINATOR is
	 * rejected for functions that are not set-returning, so the SQL
	 * declaration cannot carry it.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_iceberg_load_metadata_json() must be executed on the coordinator"),
				 errdetail("It reads the QD-only Iceberg metadata catalog and cannot run on a segment.")));

	rel = relation_open(relid, AccessShareLock);

	if (!is_iceberg_rel(rel))
	{
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not an iceberg table",
						get_rel_name(relid))));
	}

	table_info = pg_iceberg_get_table_info(relid);

	/*
	 * Deliberately a different message from the one above: "not an iceberg
	 * table at all" and "an iceberg table registered on an external catalog"
	 * are different situations for the caller, and only the second one is
	 * worth retrying against that catalog's own REST endpoint.  Callers that
	 * resolve tables through pg_ext_aux.iceberg_visible_tables() never reach
	 * this branch -- that accessor filters to builtin-catalog tables for
	 * exactly this reason.
	 */
	if (!pg_iceberg_is_builtin_catalog(table_info->catalog_server_name))
	{
		char *catalog_server_name = pstrdup(table_info->catalog_server_name);

		pg_iceberg_free_table_info(table_info);
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("iceberg table \"%s\" is not registered on the builtin catalog",
						get_rel_name(relid)),
				 errdetail("Its catalog server \"%s\" is an external catalog.",
						   catalog_server_name),
				 errhint("Only builtin-catalog tables can have their metadata document read through the database.")));
	}

	/*
	 * Authoritative accessor, the same one the scan path uses
	 * (pg_iceberg_am.c): when the current transaction has uncommitted DML on
	 * this table it returns the rebased location, so the document handed out
	 * here matches what a SELECT in the same transaction would read.  Reading
	 * pg_iceberg_get_metadata_info() directly would silently diverge.
	 */
	location = pg_iceberg_tracker_get_scan_metadata_location(relid);

	nameSpace = pg_iceberg_resolve_namespace(table_info->opts ? table_info->opts->namespace : NULL,
											 table_info->catalog_server_name,
											 table_info->catalog_name,
											 RelationGetNamespace(rel));
	tableName = pstrdup(RelationGetRelationName(rel));

	fdwRoutine = get_catalog_fdw_routine();
	scanstate = makeNode(ForeignScanState);
	fdwState = create_catalog_fdw_state(ICEBERG_LOAD_METADATA_JSON,
										NULL,	/* builtin: no external catalog name */
										nameSpace,
										tableName,
										table_info->catalog_server_name,
										table_info->catalog_name,
										table_info->volume_server_name,
										table_info->volume_name,
										NULL);
	fdwState->request.buildInCatalog.metadataLocation = location;
	fdwState->request.buildInCatalog.tableExists = true;

	scanstate->fdw_state = fdwState;
	fdwRoutine->BeginForeignScan(scanstate, 0);
	fdwRoutine->EndForeignScan(scanstate);
	fdwState = (IcebergCatalogFdwState *) scanstate->fdw_state;

	check_fdw_execution_error(fdwState, "Failed to load Iceberg metadata document");

	/*
	 * Bound what a single call can pull through the coordinator.  A
	 * metadata.json does not grow with row count, but it does grow with
	 * snapshot and schema history, and the document is buffered whole several
	 * times on its way out (agent response, the copy below, the returned
	 * datum).  A table whose history has run away should produce a
	 * diagnosable error, not an OOM on the coordinator.
	 */
	json_len = strlen(fdwState->response.responseBody);
	if (json_len > ICEBERG_METADATA_JSON_MAX_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("iceberg metadata document for relation \"%s\" is " UINT64_FORMAT " bytes, exceeding the %d byte limit",
						get_rel_name(relid), (uint64) json_len,
						ICEBERG_METADATA_JSON_MAX_BYTES),
				 errhint("Expire old snapshots to shrink the metadata document.")));

	*metadata_location = pstrdup(location);
	*metadata_json = pstrdup(fdwState->response.responseBody);

	pfree(location);
	pg_iceberg_free_table_info(table_info);
	relation_close(rel, AccessShareLock);
}

char *
pg_iceberg_create_table(Relation relation,
						const char *catalogName,
						const char *nameSpace,
						const char *tableName,
						const char *catalogServer,
						const char *foreignCatalogName,
						const char *volumeServer,
						const char *volumeName,
						const char *location,
						char **partition_spec_summary_out)
{
	IcebergTableSchema	   *schema;
	IcebergCatalogFdwState *fdwState;
	char				   *result;

	schema = build_schema_from_pg_table(relation);

	fdwState = execute_create_table_via_fdw(catalogName,
											nameSpace,
											tableName,
											catalogServer,
											foreignCatalogName,
											volumeServer,
											volumeName,
											location,
											schema);
	free_schema_info(schema);

	/*
	 * Extract the metadata location first: this raises the real create error
	 * (e.g. an agent HTTP failure) for a failed create.  Only parse the rest
	 * of the response once we know the create succeeded.
	 */
	result = extract_location_from_fdw_state(fdwState);

	/*
	 * The agent's create response also carries "partition-spec-summary" for
	 * the table it actually created.  Surface it (when requested) so the
	 * caller can verify it matches the declared PARTITION BY without a second
	 * round-trip -- a builtin-catalog load_table needs a metadata location we
	 * do not have yet, which is why the builtin path cannot simply refresh.
	 */
	if (partition_spec_summary_out != NULL)
	{
		IcebergLoadTableResult *created;

		*partition_spec_summary_out = NULL;
		created = parse_load_table_response(fdwState->response.responseBody);
		if (created != NULL)
		{
			if (created->partition_spec_summary != NULL)
				*partition_spec_summary_out =
					pstrdup(created->partition_spec_summary);
			pg_iceberg_free_load_table_result(created);
		}
	}

	return result;
}

char *
pg_iceberg_get_fragments(Relation relation,
						 const char *catalogName,
						 const char *nameSpace,
						 const char *tableName,
						 const char *metadata_location,
						 bool is_internal,
						 const char *pushdown_filter,
						 const char *catalogServer,
						 const char *foreignCatalogName,
						 const char *volumeServer,
						 const char *volumeName,
						 int64 snapshot_id)
{
	IcebergTableSchema	   *schema;
	IcebergCatalogFdwState *fdwState;

	schema = build_schema_from_pg_table(relation);

	fdwState = execute_get_fragments_via_fdw(metadata_location,
											 is_internal,
											 pushdown_filter,
											 catalogName,
											 nameSpace,
											 tableName,
											 catalogServer,
											 foreignCatalogName,
											 volumeServer,
											 volumeName,
											 schema,
											 snapshot_id);

	free_schema_info(schema);
	return extract_response_body_from_fdw_state(fdwState,
												"Failed to get Iceberg table fragments");
}

IcebergTableStatistics *
pg_iceberg_get_statistics(Relation relation,
						  const char *catalogName,
						  const char *nameSpace,
						  const char *tableName,
						  const char *metadata_location,
						  bool is_internal,
						  const char *catalogServer,
						  const char *foreignCatalogName,
						  const char *volumeServer,
						  const char *volumeName)
{
	IcebergTableSchema	   *schema;
	IcebergCatalogFdwState *fdwState;

	schema = build_schema_from_pg_table(relation);

	fdwState = execute_get_statistics_via_fdw(metadata_location,
											  is_internal,
											  catalogName,
											  nameSpace,
											  tableName,
											  catalogServer,
											  foreignCatalogName,
											  volumeServer,
											  volumeName,
											  schema);

	free_schema_info(schema);
	return extract_statistics_from_fdw_state(fdwState);
}

char *
pg_iceberg_get_snapshot_schema(Relation relation,
							   const char *catalogName,
							   const char *nameSpace,
							   const char *tableName,
							   const char *metadata_location,
							   int64 snapshot_id,
							   const char *catalogServer,
							   const char *foreignCatalogName,
							   const char *volumeServer,
							   const char *volumeName)
{
	IcebergTableSchema	   *schema;
	IcebergCatalogFdwState *fdwState;

	schema = build_schema_from_pg_table(relation);

	fdwState = execute_get_snapshot_schema_via_fdw(metadata_location,
												   snapshot_id,
												   catalogName,
												   nameSpace,
												   tableName,
												   catalogServer,
												   foreignCatalogName,
												   volumeServer,
												   volumeName,
												   schema);

	free_schema_info(schema);
	return extract_response_body_from_fdw_state(fdwState,
												"Failed to get Iceberg snapshot schema");
}

char *
pg_iceberg_get_snapshots(Relation relation,
						 const char *catalogName,
						 const char *nameSpace,
						 const char *tableName,
						 const char *metadata_location,
						 const char *catalogServer,
						 const char *foreignCatalogName,
						 const char *volumeServer,
						 const char *volumeName)
{
	IcebergTableSchema	   *schema;
	IcebergCatalogFdwState *fdwState;

	schema = build_schema_from_pg_table(relation);

	fdwState = execute_get_snapshots_via_fdw(metadata_location,
											 catalogName,
											 nameSpace,
											 tableName,
											 catalogServer,
											 foreignCatalogName,
											 volumeServer,
											 volumeName,
											 schema);

	free_schema_info(schema);
	return extract_response_body_from_fdw_state(fdwState,
												"Failed to list Iceberg snapshots");
}

char *
pg_iceberg_get_rewrite_plan(Relation rel,
							const char *metadata_location,
							bool is_internal,
							struct VacuumParams *params,
							int min_input_files,
							int target_file_size_mb,
							const char *catalogName,
							const char *nameSpace,
							const char *tableName,
							const char *catalogServer,
							const char *foreignCatalogName,
							const char *volumeServer,
							const char *volumeName)
{
	IcebergTableSchema *schema;
	IcebergCatalogFdwState *fdwState;

	(void) params;

	schema = build_schema_from_pg_table(rel);

	fdwState = execute_plan_file_groups_via_fdw(metadata_location,
												is_internal,
												min_input_files,
												target_file_size_mb,
												catalogName,
												nameSpace,
												tableName,
												catalogServer,
												foreignCatalogName,
												volumeServer,
												volumeName,
												schema);

	free_schema_info(schema);
	return extract_response_body_from_fdw_state(fdwState,
												"Failed to plan Iceberg rewrite file groups");
}

void
pg_iceberg_commit_rewrite(Relation rel, List *all_private_results)
{
	ListCell *qe_cell;
	int qe_idx = 0;
	int total_result_count = 0;
	StringInfoData added_fragments;
	StringInfoData rewritten_fragments;
	char *metadata_location;
	bool is_internal;
	IcebergTableInfo *table_info;
	char *new_metadata_location;
	char *data_locations;
	const char *nameSpace;
	const char *tableName;
	const char *catalogName;
	bool has_added_fragments = false;
	bool has_rewritten_fragments = false;

	metadata_location = NULL;
	table_info = NULL;
	data_locations = NULL;

	initStringInfo(&added_fragments);
	appendStringInfoString(&added_fragments, "[");
	initStringInfo(&rewritten_fragments);
	appendStringInfoString(&rewritten_fragments, "[");

	foreach(qe_cell, all_private_results)
	{
		List *qe_results = (List *) lfirst(qe_cell);
		ListCell *result_cell;
		int qe_result_count = 0;

		qe_idx++;

		if (qe_results == NIL)
		{
			elog(DEBUG1,
				 "pg_iceberg_commit_rewrite: relation=%u, qe_idx=%d, result_count=0",
				 RelationGetRelid(rel), qe_idx);
			continue;
		}

		foreach(result_cell, qe_results)
		{
			Node *node = (Node *) lfirst(result_cell);
			char *result_json;
			char *fragments_json = NULL;
			char *rewritten_json = NULL;

			if (node == NULL || !IsA(node, String))
				continue;

			result_json = strVal(node);
			pg_iceberg_extract_rewrite_result_arrays(result_json,
													 &fragments_json,
													 &rewritten_json);

			if (fragments_json != NULL && fragments_json[0] != '\0')
			{
				if (has_added_fragments)
					appendStringInfoString(&added_fragments, ",");
				appendStringInfoString(&added_fragments, fragments_json);
				has_added_fragments = true;
			}

			if (rewritten_json != NULL && rewritten_json[0] != '\0')
			{
				if (has_rewritten_fragments)
					appendStringInfoString(&rewritten_fragments, ",");
				appendStringInfoString(&rewritten_fragments, rewritten_json);
				has_rewritten_fragments = true;
			}

			if (fragments_json != NULL)
				pfree(fragments_json);
			if (rewritten_json != NULL)
				pfree(rewritten_json);

			qe_result_count++;
			total_result_count++;
		}

		elog(DEBUG1,
			 "pg_iceberg_commit_rewrite: relation=%u, qe_idx=%d, result_count=%d",
			 RelationGetRelid(rel),
			 qe_idx,
			 qe_result_count);
	}

	appendStringInfoString(&added_fragments, "]");
	appendStringInfoString(&rewritten_fragments, "]");

	elog(DEBUG1,
		 "pg_iceberg_commit_rewrite: relation=%u, qe_list_count=%d, total_worker_results=%d, has_rewritten=%s",
		 RelationGetRelid(rel),
		 list_length(all_private_results),
		 total_result_count,
		 has_rewritten_fragments ? "true" : "false");

	if (total_result_count <= 0 || !has_added_fragments)
		goto cleanup;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	is_internal =
		(table_info->opts == NULL || table_info->opts->table == NULL);
	metadata_location = pg_iceberg_get_latest_metadata_location(
		RelationGetRelid(rel), table_info);

	nameSpace = pg_iceberg_resolve_namespace(
		table_info->opts ? table_info->opts->namespace : NULL,
		table_info->catalog_server_name,
		table_info->catalog_name,
		RelationGetNamespace(rel));

	if (table_info->opts == NULL || table_info->opts->table == NULL)
	{
		tableName = pstrdup(RelationGetRelationName(rel));
		catalogName = NULL;
	}
	else
	{
		tableName = table_info->opts->table;
		catalogName = table_info->opts->catalog;
	}

	/*
	 * Keep vacuum rewrite payload in grouped JSON shape:
	 *   {"fragments":[...], "rewrittenFragments":[...]}
	 * and commit through the dedicated COMMIT_FILE_GROUPS operation.
	 */
	data_locations = psprintf("{\"fragments\":%s,\"rewrittenFragments\":%s}",
							  added_fragments.data,
							  rewritten_fragments.data);
	new_metadata_location = pg_iceberg_catalog_op(rel,
									  ICEBERG_COMMIT_FILE_GROUPS,
									  catalogName,
									  nameSpace,
									  tableName,
									  data_locations,
									  metadata_location,
									  is_internal,
									  table_info->catalog_server_name,
									  table_info->catalog_name,
									  table_info->volume_server_name,
									  table_info->volume_name);

	if (new_metadata_location != NULL)
	{
		pg_iceberg_update_metadata(RelationGetRelid(rel), new_metadata_location);

		/*
		 * VACUUM space reclamation: the compaction's rewritten OLD files are
		 * now superseded by the new compacted files in the current snapshot.
		 * Enqueue each for async deletion (DELETION_TYPE_FILE) so the
		 * autovacuum consumer removes them from object storage.  Only for
		 * internal (builtin-managed) tables -- external-catalog files are owned
		 * by that catalog.  The enqueue is transactional, so an aborted VACUUM
		 * keeps the old files.  The exact files to delete are known from the
		 * rewrite inputs, so no snapshot diffing is needed.
		 */
		if (is_internal && has_rewritten_fragments)
		{
			List	   *old_paths =
				pg_iceberg_collect_fragment_paths(rewritten_fragments.data);

			if (old_paths != NIL)
			{
				char	   *owner_username = GetUserNameFromId(GetUserId(), false);
				char	   *nspname = get_namespace_name(rel->rd_rel->relnamespace);
				const char *relname = NameStr(rel->rd_rel->relname);
				char	   *table_qname = nspname ?
					psprintf("%s.%s", nspname, relname) : psprintf("%s", relname);
				ListCell   *pc;

				foreach(pc, old_paths)
					pg_iceberg_deletion_queue_insert((char *) lfirst(pc),
													 RelationGetRelid(rel),
													 table_info->volume_name,
													 table_info->volume_server_name,
													 owner_username,
													 table_qname,
													 GetCurrentTimestamp(),
													 DELETION_TYPE_FILE);

				if (nspname)
					pfree(nspname);
				pfree(table_qname);
			}
		}
	}

	elog(DEBUG1,
		 "pg_iceberg_commit_rewrite: relation=%u, committed_results=%d",
		 RelationGetRelid(rel),
		 total_result_count);

cleanup:
	if (metadata_location != NULL)
		pfree(metadata_location);
	if (data_locations != NULL)
		pfree(data_locations);
	if (table_info != NULL)
		pg_iceberg_free_table_info(table_info);
	pfree(added_fragments.data);
	pfree(rewritten_fragments.data);
}
