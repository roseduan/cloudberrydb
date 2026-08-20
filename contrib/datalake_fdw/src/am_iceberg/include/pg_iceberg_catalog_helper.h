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

	/*
	 * Comma-separated partition spec summary from the agent
	 * ("partition-spec-summary" in the load-table response): one entry per
	 * partition field in spec order, identity fields as the bare column name,
	 * other transforms as "transform(column)".  NULL when the table is
	 * unpartitioned (or the agent predates the field).
	 */
	char *partition_spec_summary;
} IcebergLoadTableResult;

/*
 * Resolve the iceberg namespace for a CREATE / DML / metadata-lookup
 * operation.  Precedence (highest first):
 *   1. table OPTIONS namespace   -- per-table override (opts->namespace)
 *   2. catalog default_namespace -- per-catalog default
 *   3. PG schema name of schema_oid -- final fallback
 *
 * All three sources are user-visible PG state, no external lookup.
 * Returns a palloc'd string in the current memory context; never returns
 * NULL.
 *
 * Tier (3) takes the schema OID rather than a Relation because the resolver
 * also runs at transaction PRE_COMMIT (issue #411), where the tracker holds
 * only relid + namespace_oid and deliberately does not table_open() the
 * relation -- taking a fresh lock at the end of a transaction is something
 * we want to avoid.  The OID is all tier (3) ever needed.
 *
 * Parameter nullability:
 *   catalog_server_name / catalog_name  may be NULL when no foreign
 *      catalog is associated; tier (2) is skipped in that case.
 *   schema_oid                           may be InvalidOid on callsites that
 *      always expect tier (1) to win (e.g. external-table commit paths
 *      that never need PG schema fallback). If it is InvalidOid AND tiers
 *      (1) + (2) both come up empty, ereport(ERROR) is raised so the
 *      failure is diagnosable rather than a silent wrong namespace.
 */
extern const char *pg_iceberg_resolve_namespace(const char *options_namespace,
												const char *catalog_server_name,
												const char *catalog_name,
												Oid schema_oid);

/*
 * Validate an iceberg object name (table or namespace) against a strict
 * whitelist before it is sent to the catalog.
 *
 * The name is also used verbatim to build the warehouse storage path
 * ({warehouse}/{namespace}/{table}) by the builtin/hive/hadoop/s3 catalogs
 * and is embedded in REST URL paths, neither of which is escaped at the
 * backend.  We therefore restrict names to the minimal cross-catalog subset
 * that is also a Spark regular identifier: ASCII letters, digits and
 * underscore ([A-Za-z0-9_]), and not consisting entirely of digits.  Anything
 * else (spaces, '/', '#', '?', '%', '"', '\\', '.', non-ASCII/CJK, ...) is
 * rejected with ERROR.  'kind' is used in the error message ("table" /
 * "namespace").
 */
extern void pg_iceberg_validate_object_name(const char *name, const char *kind);

extern char *pg_iceberg_create_table(Relation relation,
									 const char *catalogName,
									 const char *nameSpace,
									 const char *tableName,
									 const char *catalogServer,
									 const char *foreignCatalogName,
									 const char *volumeServer,
									 const char *volumeName,
									 const char *location,
									 char **partition_spec_summary_out);

extern IcebergLoadTableResult *pg_iceberg_load_table(const char *catalogName,
									   const char *nameSpace,
									   const char *tableName,
									   const char *catalogServer,
									   const char *foreignCatalogName,
									   const char *volumeServer,
								   const char *volumeName);
extern void pg_iceberg_free_load_table_result(IcebergLoadTableResult *result);

/*
 * Result of fetching a builtin table's metadata.json verbatim (REST catalog
 * gateway, #382/#935).  Both fields are palloc'd in the caller's context.
 */
typedef struct IcebergMetadataJsonResult
{
	char	   *metadata_location;
	char	   *metadata_json;
} IcebergMetadataJsonResult;

extern IcebergMetadataJsonResult *pg_iceberg_load_metadata_json(Oid relid);
extern void pg_iceberg_free_metadata_json_result(IcebergMetadataJsonResult *result);

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
 * Like pg_iceberg_catalog_op, but when written_metadata_files is non-NULL it
 * is set to the List of metadata-layer files (cstrings) the agent call newly
 * wrote to object storage, so the transaction tracker can clean them up on
 * ROLLBACK / COMMIT (issue #399).  Set to NIL when the response has none.
 */
extern char *pg_iceberg_catalog_op_ex(Relation relation,
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
									  List **written_metadata_files);

/*
 * ALTER TABLE schema evolution (builtin only, issue #401): fire an
 * ICEBERG_UPDATE_SCHEMA op carrying a List of IcebergSchemaOp*, returning the
 * new metadata-location.
 */
extern char *pg_iceberg_update_schema_op(const char *catalogName,
										 const char *nameSpace,
										 const char *tableName,
										 const char *metadata_location,
										 const char *catalogServer,
										 const char *foreignCatalogName,
										 const char *volumeServer,
										 const char *volumeName,
										 List *schemaOps);

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
