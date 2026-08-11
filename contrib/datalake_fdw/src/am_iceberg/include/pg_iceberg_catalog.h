/*-------------------------------------------------------------------------
 *
 * pg_iceberg_catalog.h
 *
 *
 * IDENTIFICATION
 *	  contrib/pg_iceberg/include/pg_iceberg_catalog.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_CATALOG_H__
#define __PG_ICEBERG_CATALOG_H__

#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"

#include "pg_iceberg_metadata.h"
#include "pg_iceberg_options.h"

struct VacuumParams;
typedef struct IcebergTableStatistics IcebergTableStatistics;

/*
 * IcebergTableInfo - stores catalog and volume information for an Iceberg table
 *
 * This structure contains the names and server names for both catalog and volume,
 * which are necessary for accessing Iceberg table data.
 */
typedef struct IcebergTableInfo
{
	char *catalog_name;          /* Name of the catalog */
	char *catalog_server_name;   /* Foreign server name for catalog */
	char *volume_name;           /* Name of the volume */
	char *volume_server_name;    /* Foreign server name for volume */
	IcebergTableOptions *opts;   /* Parsed table options (NULL for internal tables) */
} IcebergTableInfo;

/* Helper to determine whether a catalog server is builtin-backed */
extern bool pg_iceberg_is_builtin_catalog(const char *catalog_server_name);

/* Function to create iceberg table with metadata from lake table catalog */
extern char *pg_iceberg_create_table_with_catalog(Relation rel, bool *is_internal);

extern char *pg_iceberg_get_fragments_with_catalog(Relation rel,
													IcebergTableInfo *table_info,
													const char *metadata_location,
													bool is_internal,
													const char *pushdown_filter);

extern IcebergTableStatistics *pg_iceberg_get_statistics_with_catalog(Relation rel,
																	  IcebergTableInfo *table_info,
																	  const char *metadata_location,
																	  bool is_internal);

extern char *pg_iceberg_get_rewrite_plan_with_catalog(Relation rel,
													   IcebergTableInfo *table_info,
													   const char *metadata_location,
													   bool is_internal,
													   struct VacuumParams *params,
													   int min_input_files,
													   int target_file_size_mb);

/*
 * written_metadata_files (out, nullable): when non-NULL, set to the List of
 * metadata-layer files the agent newly wrote, for transaction-level orphan
 * cleanup on ROLLBACK / COMMIT (issue #399).
 */
extern char *pg_iceberg_modify_data_with_catalog(Relation rel,
												 IcebergTableInfo *table_info,
												 const char *data_locations,
												 const char *metadata_location,
												 bool is_internal,
												 CmdType operation,
												 List **written_metadata_files);

extern char *pg_iceberg_truncate_with_catalog(Relation rel,
											   IcebergTableInfo *table_info,
											   const char *metadata_location);

extern char *pg_iceberg_update_schema_with_catalog(Relation rel,
												   IcebergTableInfo *table_info,
												   const char *metadata_location,
												   List *schemaOps);

extern char *pg_iceberg_commit_data_with_catalog(Relation rel,
												 IcebergTableInfo *table_info,
												 const char *data_locations,
												 const char *metadata_location,
												 CmdType operation);

extern char *pg_iceberg_get_latest_metadata_location(Oid relid,
													 IcebergTableInfo *table_info);
extern char *pg_iceberg_get_latest_metadata_and_mode(Oid relid,
													 bool *is_internal_out);

/* Helper to retrieve iceberg related table information */
extern IcebergTableInfo *pg_iceberg_get_table_info(Oid relid);

/* Helper to free memory allocated in IcebergTableInfo */
extern void pg_iceberg_free_table_info(IcebergTableInfo *info);

#endif /* __PG_ICEBERG_CATALOG_H__ */
