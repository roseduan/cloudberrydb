/*-------------------------------------------------------------------------
 *
 * pg_iceberg_guc.h
 * 		Routines for pg_iceberg GUCs.
 *
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_guc.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_GUC_H__
#define __PG_ICEBERG_GUC_H__

#include "utils/guc.h"

/* GUC variables */

/* Vacuum (Autovacuum) related - Migrated from pg_lake_iceberg */
extern bool pg_iceberg_autovacuum_enabled;
extern int pg_iceberg_autovacuum_naptime;
extern int pg_iceberg_autovacuum_log_min_duration;
extern int pg_iceberg_max_snapshot_age;

/* Vacuum (Compaction and Removal limits) related - Migrated from pg_lake_table */
extern int pg_iceberg_vacuum_compact_min_input_files;
extern int pg_iceberg_vacuum_rewrite_target_file_size_mb;
extern int pg_iceberg_max_file_removals_per_vacuum;
extern int pg_iceberg_max_compactions_per_vacuum;

/* Scan pushdown */
extern bool pg_iceberg_enable_predicate_pushdown;

extern void pg_iceberg_init_gucs(void);

#endif /* __PG_ICEBERG_GUC_H__ */
