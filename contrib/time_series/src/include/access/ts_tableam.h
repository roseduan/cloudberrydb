/*-------------------------------------------------------------------------
 *
 * ts_tableam.h
 *    Access-layer surface for the time_series Table AM: data shapes,
 *    fork-numbering constants, reloption / hook / catalog / advisory-lock
 *    APIs, and the entry points _PG_init calls via ts_tableam_init.
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/access/ts_tableam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_TABLEAM_H
#define TS_TABLEAM_H

#include "postgres.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "common/relpath.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "optimizer/pathnode.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/lock.h"
#include "utils/rel.h"

/*
 * First fork number used by time_series chunks — immediately above
 * the system forks (MAIN/FSM/VM/INIT).
 */
#define TS_FIRST_CHUNKNUM		(MAX_FORKNUM + 1)

/*
 * Maximum extension fork number that can persist.  Extension fork
 * numbers are serialised as uint16 in WAL, so anything above
 * UINT16_MAX cannot be replayed after crash.  INSERT routing raises
 * ERRCODE_PROGRAM_LIMIT_EXCEEDED when a row's chunk would land above.
 */
#define TS_MAX_CHUNK_FORKNUM	((ForkNumber) UINT16_MAX)
#define TS_MAX_CHUNKS_PER_TABLE	(TS_MAX_CHUNK_FORKNUM - TS_FIRST_CHUNKNUM + 1)

/*
 * Reloption struct stored in rd_options for time_series tables.
 * The prefix matches StdRdOptions so core code (vacuum.c etc.) can
 * cast rd_options → StdRdOptions* safely; time_series-specific
 * fields follow.  String fields are byte offsets into the palloc'd
 * options block, per the build_reloptions() convention.
 */
typedef struct TSRelOptions
{
	int32		vl_len_;			/* varlena header, must be first */
	int			fillfactor;
	int			toast_tuple_target;
	AutoVacOpts autovacuum;
	bool		user_catalog_table;
	int			parallel_workers;
	StdRdOptIndexCleanup vacuum_index_cleanup;
	bool		vacuum_truncate;
	int			blocksize;
	int			compresslevel;
	char		compresstype[NAMEDATALEN];
	bool		checksum;
	/* time_series-specific */
	int			ts_column;
	int			ts_interval;
	int			ts_origin;
	int			ts_n_total_chunks;
	int			ts_n_compressed_chunks;
} TSRelOptions;

/*
 * Parsed time-series configuration derived from TSRelOptions.  All
 * time values in microseconds so arithmetic stays in integer space.
 */
typedef struct TSConfig
{
	AttrNumber	ts_attnum;
	int64		interval_usec;
	int64		origin_usec;
} TSConfig;

/*
 * Result of a range scan on ts_chunk: parallel arrays sized by n.
 */
typedef struct TSChunkList
{
	ForkNumber *chunks;
	int16	   *statuses;
	int			n;
} TSChunkList;

/* Custom reloption kind, registered during ts_reloptions_init. */
extern relopt_kind ts_relopt_kind;

/* GUC: time_series.enable_chunk_append */
extern bool ts_enable_chunk_append;

/* GUC: time_series.chunk_append_max_chunks */
extern int	ts_chunk_append_max_chunks;

/* Register the AM and bring up its access-layer dependencies. */
extern void ts_tableam_init(void);

extern bool RelationIsTimeSeries(Relation rel);

/*
 * Scan a chunk's heap fork, calling heap_toast_delete on tuples with
 * external attributes.  Must run before smgrtruncate so pg_toast rows
 * are cleaned up; VACUUM cannot find them otherwise (MAIN fork empty).
 */
extern void ts_heap_fork_cleanup_toast(Relation rel, ForkNumber forknum);

/*
 * Refresh cached chunk counts in pg_class.reloptions.  Dispatched
 * count(*) on ts_chunk aggregated at QD, no table lock upgrade.
 */
extern void ts_refresh_chunk_stats(Oid relid);

/* Register the time_series reloptions (ts_partition_column, ...). */
extern void ts_reloptions_init(void);

/* Install ProcessUtility hook for CREATE / DROP / TRUNCATE handling. */
extern void ts_hooks_init(void);

/* Parse rel's reloptions into *config; false if not a time_series rel. */
extern bool ts_get_config(Relation rel, TSConfig *config);

/*
 * Map a timestamp to a fork number.  Same formula used by write and
 * scan paths.
 */
extern ForkNumber ts_calculate_chunk(int64 ts_usec, int64 origin_usec,
									 int64 interval_usec);

extern int64 ts_parse_interval_usec(const char *interval_str);
extern int64 ts_parse_origin_usec(const char *origin_str);

/* ChunkScan CustomScan provider. */
extern void ts_scan_scan_init(void);
extern void ts_scan_pathlist_init(void);
extern void ts_scan_planner_init(void);

/* ChunkAppend CustomScan provider — wraps N per-chunk subpaths. */
extern void ts_chunk_append_init(void);

extern CustomPath *ts_build_chunkscan_path_for_chunk(PlannerInfo *root,
													 RelOptInfo *rel,
													 RangeTblEntry *rte,
													 int32 chunk_num,
													 int64 ts_min,
													 int64 ts_max,
													 bool min_inclusive,
													 bool max_inclusive,
													 double rows_estimate,
													 Cost startup_cost,
													 Cost total_cost);

extern Path *ts_chunk_append_try_build_path(PlannerInfo *root,
											RelOptInfo *rel,
											Index rti,
											RangeTblEntry *rte,
											TSConfig *config,
											int64 ts_min,
											int64 ts_max,
											bool min_inclusive,
											bool max_inclusive);

/*
 * ts_chunk catalog access — direct heap operations on the local
 * segment's ts_chunk relation.  No SPI, no distributed dispatch.
 */
extern void ts_chunk_catalog_insert(Oid table_oid, ForkNumber fork,
									int64 range_start_usec,
									int64 range_end_usec);
extern bool ts_chunk_catalog_exists_cached(Oid table_oid,
										   int32 chunk_number);
extern List *ts_chunk_catalog_get_chunks(Oid table_oid);
extern TSChunkList ts_chunk_catalog_get_chunks_with_status(Oid table_oid,
														   ForkNumber min_chunk,
														   ForkNumber max_chunk);
extern void ts_chunk_catalog_delete(Oid table_oid);
extern bool ts_chunk_catalog_has_any(Oid table_oid);
extern bool ts_chunk_catalog_is_compressed(Oid table_oid,
										   int32 chunk_number);
extern bool ts_chunk_catalog_lock_if_compressed(Oid table_oid,
												int32 chunk_number);
extern void ts_chunk_catalog_update_status(Oid table_oid,
										   int32 chunk_number,
										   int16 new_status);

/*
 * Per-chunk advisory lock keyed on (table_oid, chunk_number).
 * Xact-scoped.  INSERT / ChunkScan take Share; compress / reclaim
 * take Exclusive.
 */
extern void ts_chunk_lock(Oid table_oid, int32 chunk_number,
						  LOCKMODE mode);

#endif							/* TS_TABLEAM_H */
