/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog_helper.h
 *    Client interface for Iceberg catalog operations
 *
 * IDENTIFICATION
 *	  contrib/pg_iceberg/include/pg_iceberg_catalog_helper.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_CATALOG_HELPER_H__
#define __PG_ICEBERG_CATALOG_HELPER_H__

#include "utils/rel.h"
#include "nodes/pg_list.h"
#include "commands/vacuum.h"
#include "pg_iceberg_rewrite_plan.h"
#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"

typedef struct IcebergTableStatistics IcebergTableStatistics;

/*
 * IcebergLoadTableResult - result of loading an Iceberg table from catalog
 *
 * metadata_location: The location of the table metadata file
 *     e.g. "s3://bucket/warehouse/db/table/metadata/00001-xxx.metadata.json"
 *
 * catalog_properties: JSON string containing 'config', 'storage-credentials',
 *     and optional 'table-location' extracted from the catalog agent response. Example:
 *     {
 *       "config": {
 *         "s3.path-style-access": "true",
 *         "s3.endpoint": "http://192.168.197.4:8002",
 *         "client.region": "us-east-1"
 *       },
 *       "storage-credentials": [
 *         {
 *           "prefix": "s3://bucket/warehouse/db/table",
 *           "config": {
 *             "s3.access-key-id": "***",
 *             "s3.secret-access-key": "***"
 *           }
 *         }
 *       ],
 *       "table-location": "s3://bucket/warehouse/db/table"
 *     }
 */
typedef struct IcebergLoadTableResult
{
	char *metadata_location;
	char *catalog_properties;
	char *location;
} IcebergLoadTableResult;

/*
 * Resolve the iceberg namespace for a CREATE / DML / metadata-lookup
 * operation.  Precedence (highest first):
 *   1. table OPTIONS namespace   -- per-table override (opts->namespace)
 *   2. catalog default_namespace -- per-catalog default
 *   3. PG schema name of rel     -- final fallback
 *
 * All three sources are user-visible PG state, no external lookup.
 * Returns a palloc'd string in the current memory context; never returns
 * NULL.
 *
 * Parameter nullability:
 *   catalog_server_name / catalog_name  may be NULL when no foreign
 *      catalog is associated; tier (2) is skipped in that case.
 *   rel                                  may be NULL on callsites that
 *      always expect tier (1) to win (e.g. external-table commit paths
 *      that never need PG schema fallback). If rel is NULL AND tiers (1)
 *      + (2) both come up empty, ereport(ERROR) is raised so the failure
 *      is diagnosable rather than a NULL dereference.
 */
extern const char *pg_iceberg_resolve_namespace(const char *options_namespace,
												const char *catalog_server_name,
												const char *catalog_name,
												Relation rel);

extern char *pg_iceberg_create_table(Relation relation,
									 const char *catalogName,
									 const char *nameSpace,
									 const char *tableName,
									 const char *catalogServer,
									 const char *foreignCatalogName,
									 const char *volumeServer,
									 const char *volumeName,
									 const char *location);

extern IcebergLoadTableResult *pg_iceberg_load_table(const char *catalogName,
									   const char *nameSpace,
									   const char *tableName,
									   const char *catalogServer,
									   const char *foreignCatalogName,
									   const char *volumeServer,
								   const char *volumeName);
extern void pg_iceberg_free_load_table_result(IcebergLoadTableResult *result);

extern char *pg_iceberg_get_fragments(Relation relation,
									  const char *catalogName,
									  const char *nameSpace,
									  const char *tableName,
									  const char *metadata_location,
									  bool is_internal,
									  const char *pushdown_filter,
									  const char *catalogServer,
									  const char *foreignCatalogName,
									  const char *volumeServer,
									  const char *volumeName);

extern IcebergTableStatistics *pg_iceberg_get_statistics(Relation relation,
														 const char *catalogName,
														 const char *nameSpace,
														 const char *tableName,
														 const char *metadata_location,
														 bool is_internal,
														 const char *catalogServer,
														 const char *foreignCatalogName,
														 const char *volumeServer,
														 const char *volumeName);

extern char *pg_iceberg_catalog_op(Relation relation,
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
									 const char *volumeName);

/*
 * Vacuum rewrite functions
 *
 * pg_iceberg_get_rewrite_plan:
 *   QD-side planner. Returns a rewrite plan JSON payload containing
 *   combinedTasks for worker scheduling.
 *
 * pg_iceberg_execute_rewrite:
 *   QE-side executor. Consumes one structured vacuum am_private and
 *   returns one execution result JSON:
 *     {"fragments":[...], "rewrittenFragments":[...]}
 *
 * pg_iceberg_commit_rewrite:
 *   QD-side combiner/committer. Aggregates QE private results, merges
 *   fragments and commits through catalog operation
 *   ICEBERG_COMMIT_FILE_GROUPS.
 */
extern char *pg_iceberg_get_rewrite_plan(Relation rel,
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
										 const char *volumeName);
extern char *pg_iceberg_execute_rewrite(Relation rel,
										 List *vacuum_am_private);
extern void pg_iceberg_commit_rewrite(Relation rel,
									 List *all_private_results);

#endif /* __PG_ICEBERG_CATALOG_HELPER_H__ */
