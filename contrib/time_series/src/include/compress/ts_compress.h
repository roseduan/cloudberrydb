/*-------------------------------------------------------------------------
 *
 * ts_compress.h
 *    Types, PAX file-layout macros, and function declarations for the
 *    time_series chunk compression subsystem.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/compress/ts_compress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_COMPRESS_H
#define TS_COMPRESS_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/*
 * Chunk lifecycle states stored in ts_chunk.status (smallint).
 * Values are persisted in the catalog and must stay stable across
 * releases.
 */
#define TS_CHUNK_ACTIVE       0	/* heap holds all rows; no PAX file */
#define TS_CHUNK_COMPRESSED   1	/* PAX authoritative; heap empty
								 * (post-reclaim) or redundant copy
								 * (pre-reclaim) */
#define TS_CHUNK_PARTIAL      2	/* PAX holds pre-compress rows; heap
								 * holds new rows; reader merges */

/* Maximum segmentby + orderby columns. */
#define TS_MAX_COMPRESS_COLS  32

/*
 * PAX sidecar file layout:
 *
 *     <data_root>/base/<dbid>/ts_compressed/<relid>/chunk_<N>.pax[.seg<M>]
 *
 * The `ts_compressed` directory sits alongside per-database
 * `base/<dbid>/`, one subdirectory per time_series relation, one file
 * per chunk.  segXX suffix is appended by smgr for >1GB segments.
 * Macros centralise the layout so ts_ddl.c / ts_scan.c / ts_compress.c
 * agree on paths.
 */
#define TS_COMPRESSED_SUBDIR		"ts_compressed"
#define TS_PAX_RELDIR_FMT			"base/%u/" TS_COMPRESSED_SUBDIR "/%u"
#define TS_PAX_RELFILE_FMT			TS_PAX_RELDIR_FMT "/chunk_%d.pax"
#define TS_PAX_DBDIR_ABS_FMT		"%s/base/%u/" TS_COMPRESSED_SUBDIR
#define TS_PAX_RELDIR_ABS_FMT		"%s/base/%u/" TS_COMPRESSED_SUBDIR "/%u"
#define TS_PAX_SEGFILE_ABS_FMT		TS_PAX_RELDIR_ABS_FMT "/chunk_%d.pax.seg%d"
/*
 * Basename only (no directory) — what the PAX WAL records carry, since
 * redo re-derives the directory from dbid + relid.
 */
#define TS_PAX_SEGFILE_BASE_FMT		"chunk_%d.pax.seg%d"

/* Maximum rows per PAX group (matches pax_max_tuples_per_group default). */
#define TS_MAX_TUPLES_PER_GROUP  131072

/*
 * Minimum rows before a segmentby boundary triggers a mid-group flush.
 * Without a floor, every distinct segmentby value gets its own ORC
 * stripe — TSBS-cpu-shaped data (100 tags × 38 rows/slice) produces
 * 100 micro-stripes per chunk, each smaller than ZSTD's effective
 * block size.  1024 is a starting point; retune with a
 * speed/ratio sweep.
 */
#define TS_MIN_GROUP_ROWS_FOR_SEGMENTBY_FLUSH  1024

/* Attribute numbers (1-based, must match the SQL DDL). */

/* time_series.ts_chunk */
#define Anum_ts_chunk_table_oid		1
#define Anum_ts_chunk_chunk_number	2
#define Anum_ts_chunk_range_start	3
#define Anum_ts_chunk_range_end		4
#define Anum_ts_chunk_creation_time	5
#define Anum_ts_chunk_status		6
#define Natts_ts_chunk				6

/* time_series.ts_compress_config */
#define Anum_cc_table_oid			1
#define Anum_cc_segmentby			2
#define Anum_cc_orderby				3
#define Anum_cc_orderby_desc		4
#define Anum_cc_orderby_nullsfirst	5
#define Natts_cc					5

/* time_series.ts_compressed_chunk */
#define Anum_cchunk_table_oid			1
#define Anum_cchunk_chunk_number		2
#define Anum_cchunk_range_start			3
#define Anum_cchunk_range_end			4
#define Anum_cchunk_pax_file			5
#define Anum_cchunk_compressed_at		6
#define Anum_cchunk_numrows				7
#define Anum_cchunk_num_groups			8
#define Anum_cchunk_compressed_size		9
#define Anum_cchunk_uncompressed_size	10
#define Natts_cchunk					10

/* Parsed compression configuration, built from ts_compress_config. */
typedef struct TSCompressConfig
{
	Oid			table_oid;
	int			n_segmentby;
	int			n_orderby;
	AttrNumber	segmentby_attnums[TS_MAX_COMPRESS_COLS];
	Oid			segmentby_types[TS_MAX_COMPRESS_COLS];
	AttrNumber	orderby_attnums[TS_MAX_COMPRESS_COLS];
	Oid			orderby_types[TS_MAX_COMPRESS_COLS];
	bool		orderby_desc[TS_MAX_COMPRESS_COLS];
	bool		orderby_nullsfirst[TS_MAX_COMPRESS_COLS];
} TSCompressConfig;

/* Metadata for one compressed chunk, matching ts_compressed_chunk. */
typedef struct TSCompressedChunkMeta
{
	Oid			table_oid;
	int32		chunk_number;
	TimestampTz range_start;
	TimestampTz range_end;
	char	   *pax_file;
	TimestampTz compressed_at;
	int64		numrows;
	int32		num_groups;
	int64		compressed_size;
	int64		uncompressed_size;
} TSCompressedChunkMeta;

/* SQL-callable functions. */
extern Datum ts_set_compress_config(PG_FUNCTION_ARGS);
extern Datum ts_compress_chunks(PG_FUNCTION_ARGS);
extern Datum ts_compressed_chunk_info(PG_FUNCTION_ARGS);

/* Internal APIs. */
extern bool ts_compress_config_load(Oid table_oid, TSCompressConfig *config);
extern void ts_compress_config_delete(Oid table_oid);
extern void ts_compressed_chunk_delete(Oid table_oid);

/*
 * Drop the PAX sidecar directory immediately (non-transactional
 * rmtree).  Callers in transactional paths must use
 * ts_pax_register_pending_removal so ROLLBACK preserves the dir.
 */
extern void ts_pax_remove_reldir(Oid dbid, Oid relid);

/*
 * Schedule a PAX sidecar directory removal that fires at xact end.
 *
 *   atCommit = true  → rmtree iff xact COMMITs (DROP / TRUNCATE)
 *   atCommit = false → rmtree iff xact ABORTs (CREATE-in-xact rollback)
 *
 * Savepoints (subtransactions) are NOT supported — entries registered
 * inside a savepoint fire at the outer xact's commit/abort regardless
 * of any intervening ROLLBACK TO SAVEPOINT.
 */
extern void ts_pax_register_pending_removal(Oid dbid, Oid relid,
											bool atCommit);

extern bool ts_compressed_chunk_has_any(Oid table_oid);

/*
 * Row counts for COMPRESSED chunks, indexed like chunk_list[].
 * Slots for non-COMPRESSED chunks are zero.  Used by the count-only
 * scan shortcut.
 */
extern int64 *ts_compressed_chunk_load_numrows(Oid table_oid,
											   ForkNumber *chunk_list,
											   int16 *chunk_status,
											   int nchunks);

/*
 * PAX writer bridge (ts_compress_pax.cc).  Opaque handle to the C++
 * OrcWriter, usable from C code.
 *
 *   minmax_col_idxs: 0-based column indexes for which the writer
 *     tracks per-group min/max statistics (used by PaxFilter's sparse
 *     filter for group-level pruning at scan time).  NULL / n_minmax=0
 *     disables stats.  Typical callers pass only segmentby + orderby
 *     columns.
 *   dbid + relid: identify the PAX sidecar directory so the WAL
 *     wrapper (TsWalRecordingFile) can emit records keyed on the
 *     logical relation, not the transient filepath.
 */
typedef void *TSPaxWriter;

/*
 * toast_filepath: sibling temp path for PAX's own external-toast sidecar
 * (e.g. filepath="chunk_4.pax.seg0.new" -> toast_filepath should be
 * "chunk_4.pax.seg0.toast.new").  Caller supplies it explicitly rather
 * than having ts_pax_writer_open derive it by stripping filepath's
 * ".new" suffix -- see the "keep '.new' as a literal, caller-owned
 * suffix, never reconstruct a live name by string surgery" rule that
 * XLOG_TS_PAX_WRITE's basename bug (fixed in ts_wal_pax_rename) exists
 * to enforce.  Always opened (kWriteWithTruncMode): OrcWriter asserts
 * a toast_file_ handle whenever the schema has any varlena column,
 * even if no row actually externalises a value this run.
 */
extern TSPaxWriter ts_pax_writer_open(const char *filepath,
									  const char *toast_filepath,
									  TupleDesc tupdesc,
									  Oid dbid, Oid relid,
									  const int *minmax_col_idxs,
									  int n_minmax);
extern void ts_pax_writer_write_tuple(TSPaxWriter w, TupleTableSlot *slot);
extern void ts_pax_writer_flush(TSPaxWriter w);
extern int64 ts_pax_writer_close(TSPaxWriter w, int64 *out_ntuples,
								 int32 *out_ngroups);
/*
 * ts_pax_writer_abort
 *		Release a writer's resources without finalising the file.
 *		Callers must invoke this from a PG_CATCH around any sequence
 *		that calls ts_pax_writer_write_tuple / ts_pax_writer_flush --
 *		those calls are not individually PG_TRY-protected (would be
 *		too costly per-row), so an elog(ERROR) raised from inside PAX
 *		mid-write reaches the caller's own PG_CATCH with the writer
 *		handle still open.  Skips Close()'s finalisation (footer
 *		write etc., which assumes a consistent, fully-written file --
 *		not a safe thing to attempt while already unwinding an
 *		error) and only releases the writer's own resources (open fds
 *		for the main + toast files).  Safe to call with NULL.
 */
extern void ts_pax_writer_abort(TSPaxWriter w);

/*
 * PAX reader bridge (ts_compress_pax.cc).  Opaque handle to read TSCP
 * compressed files from C code.
 */
typedef void *TSPaxReader;

extern TSPaxReader ts_pax_reader_open(const char *filepath,
									  TupleDesc tupdesc);
/* proj_bitmap[i] = true means materialise column i. */
extern TSPaxReader ts_pax_reader_open_with_proj(const char *filepath,
												TupleDesc tupdesc,
												const bool *proj_bitmap);
/*
 * Projection + sparse filter (group pruning via min/max stats).  quals
 * is the raw List * from plan->qual; PAX parses it into its internal
 * PFTNode AST and skips groups whose per-column min/max cannot satisfy
 * the predicate.  Pass NIL to disable the sparse filter.
 */
extern TSPaxReader ts_pax_reader_open_filtered(const char *filepath,
											   Relation rel,
											   const bool *proj_bitmap,
											   List *quals);
extern bool ts_pax_reader_next(TSPaxReader r, TupleTableSlot *slot);
extern void ts_pax_reader_close(TSPaxReader r);
/*
 * ts_pax_reader_abort
 *		Release a reader's resources on an error path -- see
 *		ts_pax_writer_abort's comment; same rationale for
 *		ts_pax_reader_next not being individually PG_TRY-protected.
 *		Safe to call with NULL.
 */
extern void ts_pax_reader_abort(TSPaxReader r);

/*
 * Full-scan PAX iterator (no projection, no qual push-down).  Used by
 * callers that have no PlanState / ExprContext to feed the projection
 * + sparse-filter path in ts_chunk_scan_pax_next — Table AM
 * scan_getnextslot, ANALYZE sampling, COPY OUT.
 *
 * Lifecycle:
 *   TsPaxIter iter = {0};
 *   while (chunk available) {
 *       while (ts_pax_iter_next(&iter, rel, chunk, slot)) { ... }
 *       ts_pax_iter_reset_chunk(&iter);     // close PAX before next chunk
 *   }
 *   ts_pax_iter_end(&iter);                 // release internal slot
 *
 * PAX data is logically immutable, so MVCC visibility is skipped —
 * every PAX row is returned.
 */
typedef struct TsPaxIter
{
	TSPaxReader		reader;			/* NULL = no PAX open for current chunk */
	TupleTableSlot *internal_slot;	/* NULL = not yet allocated (lazy) */
} TsPaxIter;

extern bool ts_pax_iter_next(TsPaxIter *iter, Relation rel,
							 ForkNumber chunk, TupleTableSlot *out_slot);
extern void ts_pax_iter_reset_chunk(TsPaxIter *iter);
extern void ts_pax_iter_end(TsPaxIter *iter);

/* Register _exit() callback to prevent C++ atexit double-free. */
extern void ts_pax_register_exit_hook(void);

/*
 * Per-backend on_proc_exit registration so the inherited atexit hook
 * forwards the real proc_exit() code instead of hard-coding 0.  Call
 * from each bgworker's main entry function.
 */
extern void ts_pax_register_per_backend_exit_capture(void);

#endif							/* TS_COMPRESS_H */
