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

/*
 * Time travel: parsed getSnapshotSchema response.  Columns are the snapshot's
 * schema in field order; type strings are Iceberg's col.type().toString().
 */
typedef struct IcebergSnapshotSchema
{
	int			snapshot_schema_id;
	int			current_schema_id;
	int			ncols;
	char	  **colnames;
	char	  **coltypes;
	/*
	 * Iceberg field-id of each column, parallel to colnames.  The parquet
	 * reader matches data-file columns by these ids (a snapshot whose history
	 * holds a DROP has holes, so a column's position is NOT its id).  0 when
	 * the agent response predates the fieldId key.
	 */
	int		   *fieldids;
} IcebergSnapshotSchema;

extern void pg_iceberg_parse_snapshot_schema_response(char *json_response,
													  IcebergSnapshotSchema *out);

/*
 * Time travel: parsed getSnapshots response -- the snapshots recorded in the
 * pinned metadata_location, in metadata (chronological) order.  Backs
 * iceberg_toolkit.snapshots() and the AS OF TIMESTAMP resolution.
 *
 * timestamp_ms is Iceberg's commit time in milliseconds since the Unix epoch
 * (not PostgreSQL's 2000-01-01 origin -- callers must convert).  summary_json
 * is the snapshot summary map already serialized by the agent, so this parser
 * only ever sees scalars.
 */
typedef struct IcebergSnapshotEntry
{
	int64		snapshot_id;
	int64		timestamp_ms;
	char	   *operation;
	int			schema_id;	/* -1 when the snapshot records none */
	int64		parent_id;	/* 0 when the snapshot has no parent */
	char	   *summary_json;
} IcebergSnapshotEntry;

typedef struct IcebergSnapshotList
{
	int64		current_snapshot_id;	/* 0 when the table has no snapshot */
	int			nsnapshots;
	IcebergSnapshotEntry *snapshots;
} IcebergSnapshotList;

extern void pg_iceberg_parse_snapshots_response(char *json_response,
												IcebergSnapshotList *out);

/*
 * Look up an integer-valued key ("total-records", "total-delete-files", ...) in
 * an IcebergSnapshotEntry.summary_json map.  Returns false and leaves *out
 * untouched when the key is absent or not integral; never throws.
 */
extern bool pg_iceberg_summary_int64(const char *summary_json, const char *key,
									 int64 *out);

/* Error handling */
extern void check_fdw_execution_error(IcebergCatalogFdwState *fdwState, const char *error_prefix);
extern bool is_table_not_found_error(IcebergCatalogFdwState *fdwState);

#endif /* __PG_ICEBERG_CATALOG_UTILS_H__ */
