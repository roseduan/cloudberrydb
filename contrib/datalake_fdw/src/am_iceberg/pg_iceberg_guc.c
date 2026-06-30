/*-------------------------------------------------------------------------
 *
 * pg_iceberg_guc.c
 *
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_guc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "miscadmin.h"			/* DataDir */
#include "lib/stringinfo.h"

#include <limits.h>
#include <stdint.h>

#include "include/pg_iceberg_guc.h"

/* Default values */
#define DEFAULT_MAX_SNAPSHOT_AGE (3600 * 24 * 5)
#define DEFAULT_MIN_INPUT_FILES 5

/* Vacuum related variables */
bool pg_iceberg_autovacuum_enabled = true;
int pg_iceberg_autovacuum_naptime = 10 * 60;
int pg_iceberg_autovacuum_log_min_duration = 600000;
int pg_iceberg_max_snapshot_age = DEFAULT_MAX_SNAPSHOT_AGE;

int pg_iceberg_vacuum_compact_min_input_files = DEFAULT_MIN_INPUT_FILES;
int pg_iceberg_vacuum_rewrite_target_file_size_mb = 512;
int pg_iceberg_max_file_removals_per_vacuum = 100000;
int pg_iceberg_max_compactions_per_vacuum = 100;

/* Scan pushdown */
bool pg_iceberg_enable_predicate_pushdown = true;

void
pg_iceberg_init_gucs(void)
{
	DefineCustomBoolVariable("datalake.iceberg_enable_predicate_pushdown",
							 "Push Iceberg AM scan quals to the datalake agent for "
							 "manifest-based data-file pruning.",
							 NULL,
							 &pg_iceberg_enable_predicate_pushdown,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/* 1. Autovacuum configuration */
	DefineCustomBoolVariable("datalake.iceberg_autovacuum",
							 "Global switch for the iceberg autovacuum process.",
							 NULL,
							 &pg_iceberg_autovacuum_enabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("datalake.iceberg_autovacuum_naptime",
							"Naptime between iceberg autovacuum runs.",
							NULL,
							&pg_iceberg_autovacuum_naptime,
							10 * 60, 1, INT_MAX / 1000,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	/* 2. Logging and snapshot expiration */
	DefineCustomIntVariable("datalake.iceberg_log_autovacuum_min_duration",
							"Minimum duration to log iceberg autovacuum operations.",
							NULL,
							&pg_iceberg_autovacuum_log_min_duration,
							600000, -1, INT_MAX,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("datalake.iceberg_max_snapshot_age",
							"The maximum age of snapshots in seconds to retain.",
							NULL,
							&pg_iceberg_max_snapshot_age,
							DEFAULT_MAX_SNAPSHOT_AGE,
							0,
							INT32_MAX,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	/* 3. Compaction and removal limits */
	DefineCustomIntVariable("datalake.iceberg_vacuum_compact_min_input_files",
							"Minimum input files to trigger compaction during vacuum.",
							NULL,
							&pg_iceberg_vacuum_compact_min_input_files,
							DEFAULT_MIN_INPUT_FILES,
							1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("datalake.iceberg_max_file_removals_per_vacuum",
							"Maximum number of files to remove during a single vacuum operation.",
							NULL,
							&pg_iceberg_max_file_removals_per_vacuum,
							100000, 0, INT_MAX,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("datalake.iceberg_max_compactions_per_vacuum",
							"Maximum number of compactions during a single vacuum operation.",
							NULL,
							&pg_iceberg_max_compactions_per_vacuum,
							100, 1, INT_MAX,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("datalake.iceberg_vacuum_rewrite_target_file_size_mb",
							"Target size of the rewritten data files (in MB).",
							NULL,
							&pg_iceberg_vacuum_rewrite_target_file_size_mb,
							512, 1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
}
