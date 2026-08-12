/*-------------------------------------------------------------------------
 *
 * ts_wal.h
 *    Public surface of the time_series custom WAL rmgr: chunk-fork
 *    heap emitters (called from the AM's INSERT / TRUNCATE paths) and
 *    plain-C PAX helpers called from the C++ compression code.
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/access/ts_wal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_WAL_H
#define TS_WAL_H

#include "postgres.h"
#include "access/htup.h"
#include "common/relpath.h"
#include "storage/buf.h"
#include "storage/off.h"
#include "utils/rel.h"

/* Register the time_series WAL resource manager. */
extern void ts_wal_init(void);

/*
 * Place a tuple on an exclusively-locked chunk-fork page.  Counterpart
 * to upstream RelationPutHeapTuple: PageAddItem + t_self stamping only,
 * no WAL, no MarkBufferDirty, no critical section.  Caller wraps this
 * in START_CRIT_SECTION / MarkBufferDirty / ts_wal_insert / END.
 */
extern OffsetNumber ts_relation_put_heap_tuple(Buffer buf, HeapTuple tup,
											   bool token);

/*
 * Emit an incremental WAL record for one tuple already placed by
 * ts_relation_put_heap_tuple.  Buffer must be exclusively locked and
 * inside a critical section opened by the caller.
 */
extern void ts_wal_insert(Relation rel, Buffer buf, HeapTuple tup,
						  OffsetNumber offnum, bool init_page);

/* Same as ts_wal_insert but covers multiple tuples in one record. */
extern void ts_wal_multi_insert(Relation rel, Buffer buf,
								HeapTuple *tuples, int ntuples,
								OffsetNumber *offnums, bool init_page);

/*
 * WAL record that truncates forknum of rel to nblocks on replay.
 * XLogFlush'd so a following non-transactional smgrtruncate cannot
 * outrun durable WAL.
 */
extern void ts_wal_fork_truncate(Relation rel, ForkNumber forknum,
								 BlockNumber nblocks);

/*
 * PAX sidecar directory / file WAL — replaces PAX's own rmgr for the
 * time_series compression path.  Called from C++ (ts_pax_wal.cc,
 * ts_compress_pax.cc) via "extern \"C\"" — signatures stay plain-C.
 */
extern void ts_wal_pax_create_dir(Oid dbid, Oid relid);
extern void ts_wal_pax_write(Oid dbid, Oid relid, const char *filename,
							 int64 offset, const void *payload,
							 size_t payload_len);

/*
 * Promote "<filename>.new" to "<filename>" on replay, and -- when
 * toast_filename is non-NULL -- promote "<toast_filename>.new" to
 * "<toast_filename>" too, from the SAME WAL record.  Both filenames are
 * FINAL basenames.  Packing both renames into one record is deliberate:
 * two separate records could be split by a crash landing between them
 * (only one flushed), leaving a live main file paired with a toast
 * sidecar from a different generation -- silently wrong offsets rather
 * than the self-correcting "orphan .new" outcome documented on
 * ts_wal_pax_rename's definition. Call only after the PAX writer closed
 * successfully, immediately before the primary's own rename(2) calls.
 */
extern void ts_wal_pax_rename(Oid dbid, Oid relid, const char *filename,
							  const char *toast_filename);
extern void ts_wal_pax_remove_dir(Oid dbid, Oid relid);

#endif							/* TS_WAL_H */
