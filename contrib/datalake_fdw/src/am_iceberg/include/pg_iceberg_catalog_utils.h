/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog_utils.h
 *    Internal utilities for Iceberg catalog operations
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_catalog_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_CATALOG_UTILS_H__
#define __PG_ICEBERG_CATALOG_UTILS_H__

#include "postgres.h"
#include "utils/rel.h"
#include "../iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "pg_iceberg_catalog_helper.h"

/* Schema building and management */
extern IcebergTableSchema *build_schema_from_pg_table(Relation relation);
extern void free_schema_info(IcebergTableSchema *schema);

/* JSON parsing for Iceberg responses */
extern char *parse_metadata_location(char *json_response);
/*
 * Like parse_metadata_location, but also returns the "written-metadata-files"
 * array (metadata-layer files the agent call newly wrote) when
 * written_files_out is non-NULL.  Used by the transaction tracker to clean up
 * orphaned metadata files on ROLLBACK / COMMIT (issue #399).
 */
extern char *parse_metadata_location_ex(char *json_response,
										List **written_files_out);
extern IcebergLoadTableResult *parse_load_table_response(char *json_response);
extern IcebergTableStatistics *parse_statistics_response(char *json_response);
extern List *pg_iceberg_collect_fragment_paths(const char *json);

/* Error handling */
extern void check_fdw_execution_error(IcebergCatalogFdwState *fdwState, const char *error_prefix);
extern bool is_table_not_found_error(IcebergCatalogFdwState *fdwState);

#endif /* __PG_ICEBERG_CATALOG_UTILS_H__ */
