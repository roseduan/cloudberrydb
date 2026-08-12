/*-------------------------------------------------------------------------
 *
 * ts_tableam.c
 *    time_series Table Access Method: wraps standard heap AM to route
 *    INSERT data to time-partitioned chunk files.
 *
 *    At module load time, ts_tableam_init() copies every callback from
 *    the standard heap TableAmRoutine and then overwrites only the
 *    handful that need chunk-aware behaviour (insert, scan, size,
 *    amoptions).  This avoids maintaining dozens of thin delegation
 *    wrappers for callbacks that pass straight through to heapam.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_tableam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage.h"
#include "storage/proc.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "common/relpath.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/spccache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/compress/ts_compress.h"
#include "../include/storage/ts_fork_name.h"
#include "../include/storage/ts_smgr.h"
#include "../include/access/ts_wal.h"
#include "catalog/index.h"

/*
 *		Heap AM pointer
 *
 *		Cached pointer to the standard heap TableAmRoutine, populated
 *		once during ts_tableam_init().  Used by custom callbacks that
 *		need to delegate individual operations back to heap (e.g. the
 *		non-ts fallback path in tuple_insert, or non-MAIN fork sizes).
 */
static const TableAmRoutine *heap_am = NULL;

/*
 * ts_heap_methods is populated at runtime by ts_tableam_init():
 * memcpy from heap_am, then overwrite the custom callbacks.
 */
static TableAmRoutine ts_heap_methods;

/*
 * ChunkScanDescData
 *		Extends TableScanDescData with the state needed to iterate
 *		over every chunk of a time_series relation.  Returned by
 *		ts_heap_scan_begin, consumed by the scan_* callbacks below.
 */
typedef struct ChunkScanDescData
{
	TableScanDescData	base;		/* must be first */
	Relation			rel;		/* scanned relation */
	Snapshot			snapshot;	/* visibility snapshot */
	/*
	 * Parallel arrays from ts_chunk_catalog_get_chunks_with_status.
	 * cur_chunk_idx walks 0..n_chunks-1; the status drives whether we
	 * read the chunk's heap fork, its PAX sidecar, or both (PARTIAL).
	 * Without statuses, this AM path silently dropped any COMPRESSED
	 * chunk's rows because the heap fork is empty or stale after
	 * post-compress reclaim — see TS_CHUNK_COMPRESSED comment in
	 * ts_compress.h.
	 */
	ForkNumber		   *chunks;
	int16			   *statuses;
	int					n_chunks;
	int					cur_chunk_idx;
	/* Per-chunk heap-fork iteration state */
	BlockNumber			cur_block;		/* current block number */
	OffsetNumber		cur_offset;		/* current line pointer */
	Buffer				cur_buffer;		/* pinned buffer or Invalid */
	/* Per-chunk PAX iteration state (zero-init = closed/no slot) */
	TsPaxIter			pax;
	bool				pax_exhausted;	/* true once cur chunk's PAX EOF */
	/*
	 * Optional bulk-read access strategy.  Allocated at scan_begin when
	 * SO_ALLOW_STRAT is set on the flags; passed to ReadBufferExtended in
	 * getnextslot to keep the working set out of the main buffer ring.
	 */
	BufferAccessStrategy	rs_strategy;
	/*
	 * Page-mode visibility cache.  Populated by ts_heap_loadpage when
	 * SO_ALLOW_PAGEMODE is set on the scan: we scan every itemid on the
	 * page under share-lock, collect the offnums that pass MVCC, then
	 * drop the lock and walk the offnums without holding it (mirrors
	 * heapgetpage / heapgettup_pagemode).  rs_cindex advances within
	 * rs_vistuples; rs_ntuples is the populated length.
	 */
	int					rs_ntuples;
	int					rs_cindex;
	OffsetNumber		rs_vistuples[MaxHeapTuplesPerPage];
	/*
	 * Long-lived HeapTupleData backing the current returned tuple.
	 * ExecStoreBufferHeapTuple stores the POINTER (not a copy) into the
	 * slot, so the HeapTuple must outlive the getnextslot call.
	 * Mirrors HeapScanDescData.rs_ctup.
	 */
	HeapTupleData		rs_ctup;
} ChunkScanDescData;

typedef ChunkScanDescData *ChunkScanDesc;

typedef struct TSChunkInfoCtx
{
	Oid			table_oid;
	TSChunkList	list;
	int			cur_idx;
	Relation	rel;
} TSChunkInfoCtx;

PG_FUNCTION_INFO_V1(ts_chunk_info);
PG_FUNCTION_INFO_V1(ts_tableam_handler);

/*
 * RelationIsTimeSeries
 *		Return true if the relation uses the time_series table access method.
 *		Mirrors the spirit of RelationIsHeap / RelationIsAoRows: identify the
 *		AM by comparing the TableAmRoutine pointer rather than inspecting
 *		reloptions.  rd_tableam is NULL for relkinds that have no storage
 *		(views, partitioned tables, indexes, ...), so the NULL check below
 *		makes this safe to call on any Relation.
 */
bool
RelationIsTimeSeries(Relation rel)
{
	return rel->rd_tableam == &ts_heap_methods;
}

/*
 * ts_chunk_get_insert_buffer
 *		Return an exclusively-locked buffer in the given chunk that has
 *		at least tuplen bytes of free space (including the line pointer).
 *		Tries the last block first; if it is full, extends the chunk with
 *		P_NEW and initialises the new page.
 */
/*
 * Per-fork cached last block number to avoid smgrnblocks() per row.
 *
 * Single-entry cache keyed by (relid, fork, xid).  Invalidated automatically
 * at transaction boundaries via xid mismatch — mirrors the per-transaction
 * lifecycle of heap's BulkInsertState.
 *
 * Catalog registration is checked independently by ts_chunk_catalog_exists_cached()
 * which has its own per-transaction invalidation.
 */
static BlockNumber		ts_ins_blkno = InvalidBlockNumber;
static ForkNumber		ts_ins_fork = InvalidForkNumber;
static Oid				ts_ins_relid = InvalidOid;
static TransactionId	ts_ins_xid = InvalidTransactionId;

/*
 * ts_read_buffer_bi
 *		Mirror PG's ReadBufferBI (hio.c:86): if `bistate->current_buf`
 *		already points at `targetBlock`, hand the caller a fresh
 *		refcount instead of re-walking the buffer hash table; otherwise
 *		release the stale cached pin and read the target normally
 *		(using bistate->strategy so BAS_BULKWRITE keeps the bulk-write
 *		ring instead of trashing shared_buffers).  bistate->current_buf
 *		retains one extra pin between calls and is released by the
 *		caller via FreeBulkInsertState — that explicit teardown happens
 *		BEFORE the commit-time ResourceOwnerRelease, so the cached pin
 *		never outlives its ResourceOwner.
 *
 *		bistate must not be NULL.  Non-bistate callers go through the
 *		plain ReadBufferExtended branch in ts_chunk_get_insert_buffer.
 */
static Buffer
ts_read_buffer_bi(Relation rel, ForkNumber forknum, BlockNumber targetBlock,
				  ReadBufferMode mode, struct BulkInsertStateData *bistate)
{
	Buffer	buf;

	if (bistate->current_buf != InvalidBuffer)
	{
		if (BufferGetBlockNumber(bistate->current_buf) == targetBlock)
		{
			IncrBufferRefCount(bistate->current_buf);
			return bistate->current_buf;
		}
		ReleaseBuffer(bistate->current_buf);
		bistate->current_buf = InvalidBuffer;
	}

	buf = ReadBufferExtended(rel, forknum, targetBlock, mode, bistate->strategy);

	IncrBufferRefCount(buf);
	bistate->current_buf = buf;

	return buf;
}

static Buffer
ts_chunk_get_insert_buffer(Relation rel, ForkNumber forknum, Size tuplen,
						   struct BulkInsertStateData *bistate)
{
	BufferAccessStrategy	strategy = bistate ? bistate->strategy : NULL;
	Buffer			buf;
	Page			page;
	Size			needed;
	Size			saveFreeSpace;
	BlockNumber		target;

	/*
	 * Reserve the fillfactor-derived free space so user-set fillfactor
	 * < 100 is respected (extend a new page once a block falls below
	 * the target, instead of stuffing it to the brim).  Mirrors
	 * RelationGetBufferForTuple's internal use of
	 * RelationGetTargetPageFreeSpace.
	 */
	saveFreeSpace = RelationGetTargetPageFreeSpace(rel, HEAP_DEFAULT_FILLFACTOR);
	needed = tuplen + sizeof(ItemIdData) + saveFreeSpace;

	/*
	 * Fast path: use cached block number. Avoids smgrnblocks per row.
	 *
	 * The xid check forces re-validation on xact boundaries.  TRUNCATE
	 * (or any other operation that shortens the fork) between xacts
	 * leaves the static ts_ins_blkno pointing past EOF; without the
	 * xid gate the next xact's first row would ReadBufferExtended at
	 * a non-existent block and raise "could not read block N: read
	 * only 0 of 32768 bytes".  One smgrnblocks per (xact, fork) is
	 * cheap; subsequent rows in the same xact still hit the fast path.
	 */
	if (ts_ins_fork == forknum &&
		ts_ins_relid == RelationGetRelid(rel) &&
		ts_ins_xid == GetCurrentTransactionIdIfAny() &&
		ts_ins_blkno != InvalidBlockNumber)
	{
		target = ts_ins_blkno;
	}
	else
	{
		SMgrRelation	smgr;
		BlockNumber		nblocks;

		RelationOpenSmgr(rel);
		smgr = rel->rd_smgr;
		nblocks = smgrnblocks(smgr, forknum);

		if (nblocks == 0)
			goto extend;

		target = nblocks - 1;
	}

	if (bistate)
		buf = ts_read_buffer_bi(rel, forknum, target, RBM_NORMAL, bistate);
	else
		buf = ReadBufferExtended(rel, forknum, target, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);
	if (PageGetFreeSpace(page) >= needed)
	{
		ts_ins_blkno = target;
		ts_ins_fork = forknum;
		ts_ins_relid = RelationGetRelid(rel);
		ts_ins_xid = GetCurrentTransactionIdIfAny();
		return buf;
	}
	UnlockReleaseBuffer(buf);

extend:
	/*
	 * Two backends calling P_NEW on the same fork without a relation-level
	 * extension lock both see smgrnblocks=N and both try to extend to block
	 * N — whichever loses the smgrextend race finds the winner's BM_VALID
	 * page in shared buffers and aborts with "unexpected data beyond EOF"
	 * (bufmgr.c:1039).  Stock heap (RelationGetBufferForTuple) serialises
	 * the same way; mirror it here.
	 *
	 * Use the plain ReadBufferExtended for P_NEW (mirrors PG hio.c
	 * RelationGetBufferForTuple's extend branch — ReadBufferBI is only
	 * used on the targetBlock fast path).  Pass strategy through so
	 * bulk-write extends still use BAS_BULKWRITE.
	 */
	LockRelationForExtension(rel, ExclusiveLock);
	buf = ReadBufferExtended(rel, forknum, P_NEW, RBM_ZERO_AND_LOCK, strategy);
	page = BufferGetPage(buf);
	PageInit(page, BLCKSZ, 0);
	UnlockRelationForExtension(rel, ExclusiveLock);

	/*
	 * Drop bistate's cached pin if any: it pointed at an old block and
	 * we just opened a new one.  Caller's pin is retained (return value).
	 */
	if (bistate && bistate->current_buf != InvalidBuffer)
	{
		ReleaseBuffer(bistate->current_buf);
		bistate->current_buf = InvalidBuffer;
	}
	if (bistate)
	{
		IncrBufferRefCount(buf);
		bistate->current_buf = buf;
	}

	ts_ins_blkno = BufferGetBlockNumber(buf);
	ts_ins_fork = forknum;
	ts_ins_relid = RelationGetRelid(rel);
	ts_ins_xid = GetCurrentTransactionIdIfAny();
	return buf;
}

/*
 *		Page-level tuple placement
 *
 *		Counterpart to upstream RelationPutHeapTuple (hio.c).  Plain
 *		PageAddItem plus t_self stamping — no WAL, no MarkBufferDirty,
 *		no critical section.  Caller is responsible for those.
 */
OffsetNumber
ts_relation_put_heap_tuple(Buffer buf, HeapTuple tup, bool token)
{
	Page			page = BufferGetPage(buf);
	OffsetNumber	offnum;

	offnum = PageAddItem(page, (Item) tup->t_data, tup->t_len,
						 InvalidOffsetNumber, false, true);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add tuple to time_series page");

	ItemPointerSet(&tup->t_self, BufferGetBlockNumber(buf), offnum);

	/*
	 * Mirror PG's RelationPutHeapTuple (hio.c): the on-disk tuple
	 * header's t_ctid must point to itself for a newly inserted row.
	 * t_self is only known after PageAddItem returns the offnum, so
	 * the write must happen here rather than in the prepare-insert
	 * path that stamps the rest of the header.  Without this, the
	 * t_ctid invariant ("an unmodified tuple's t_ctid points to
	 * itself") breaks, which would mis-lead HOT-chain followers and
	 * heap_get_latest_tid the moment UPDATE / VACUUM-driven access
	 * paths are reintroduced for time_series.
	 */
	if (!token)
	{
		ItemId			itemId = PageGetItemId(page, offnum);
		HeapTupleHeader item = (HeapTupleHeader) PageGetItem(page, itemId);
		item->t_ctid = tup->t_self;
	}

	return offnum;
}

/*
 * ts_heap_fork_cleanup_toast
 *		Walk every heap tuple in the chunk fork and call heap_toast_delete
 *		on those that carry external (TOAST'd) attributes.  Required before
 *		smgrtruncate'ing the fork to 0 blocks: the user table's TOAST
 *		relation holds the actual large values, and once the heap fork is
 *		truncated those rows have nothing pointing at them.  PG VACUUM
 *		can't reclaim them either, because the user table's MAIN_FORKNUM
 *		is empty (we use extension forks 4+) so VACUUM finds no dead
 *		tuples to drive the cleanup.
 *
 *		Buffer SHARE lock is enough because heap_toast_delete only reads
 *		the source tuple's attributes; the actual deletes run against the
 *		TOAST relation, which heap_toast_delete locks internally.
 *
 *		Idempotent: if a tuple has no external attributes (all values
 *		inline), HeapTupleHasExternal returns false and we skip it
 *		cheaply.  Safe to call on a fork that has never had wide rows.
 *
 *		Abort behaviour: if the enclosing xact aborts after this function
 *		runs but before commit, heap_toast_delete is rolled back (the
 *		TOAST rows' xmax flips back to invalid → they appear live again),
 *		while the caller's smgrtruncate (which is non-transactional)
 *		persists.  Net result: empty heap fork + live-but-orphaned TOAST
 *		rows — the same orphan state as if cleanup_toast had never run.
 *		Aborted reclaim/auto-truncate is already lossy by design (the
 *		non-tx smgrtruncate makes it so), so this is not a regression.
 */
void
ts_heap_fork_cleanup_toast(Relation rel, ForkNumber forknum)
{
	SMgrRelation	smgr;
	BlockNumber		nblocks;
	BlockNumber		blkno;

	RelationOpenSmgr(rel);
	smgr = rel->rd_smgr;
	if (!smgrexists(smgr, forknum))
		return;

	nblocks = smgrnblocks(smgr, forknum);
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer			buf;
		Page			page;
		OffsetNumber	maxoff;
		OffsetNumber	off;

		buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (PageIsNew(page) || PageIsEmpty(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId			itemid = PageGetItemId(page, off);
			HeapTupleData	loctup;

			if (!ItemIdIsNormal(itemid))
				continue;

			loctup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			loctup.t_len = ItemIdGetLength(itemid);
			loctup.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&loctup.t_self, blkno, off);

			if (HeapTupleHasExternal(&loctup))
				heap_toast_delete(rel, &loctup, false);
		}
		UnlockReleaseBuffer(buf);
	}
}

/*
 * ts_heap_amoptions
 *		Parse time_series-specific reloptions (ts_partition_column, ts_chunk_interval,
 *		ts_chunk_origin) using the standard build_reloptions() machinery.
 */
static bytea *
ts_heap_amoptions(Datum reloptions, char relkind, bool validate)
{
	static const relopt_parse_elt ts_elems[] = {
		{"ts_partition_column", RELOPT_TYPE_STRING,
			offsetof(TSRelOptions, ts_column)},
		{"ts_chunk_interval", RELOPT_TYPE_STRING,
			offsetof(TSRelOptions, ts_interval)},
		{"ts_chunk_origin", RELOPT_TYPE_STRING,
			offsetof(TSRelOptions, ts_origin)},
		{"ts_n_total_chunks", RELOPT_TYPE_INT,
			offsetof(TSRelOptions, ts_n_total_chunks)},
		{"ts_n_compressed_chunks", RELOPT_TYPE_INT,
			offsetof(TSRelOptions, ts_n_compressed_chunks)},
	};
	TSRelOptions *opts;

	opts = (TSRelOptions *) build_reloptions(reloptions, validate, ts_relopt_kind,
											 sizeof(TSRelOptions), ts_elems,
											 lengthof(ts_elems));

	/*
	 * Initialise the StdRdOptions-compatible prefix with safe defaults.
	 * Core code (vacuum.c) may blindly cast rd_options to StdRdOptions*
	 * to read vacuum_index_cleanup / vacuum_truncate before calling any
	 * AM callback.  Without valid defaults here, the read hits garbage
	 * and crashes.
	 */
	if (opts != NULL)
	{
		opts->fillfactor = HEAP_DEFAULT_FILLFACTOR;
		opts->toast_tuple_target = TOAST_TUPLE_TARGET;
		memset(&opts->autovacuum, 0, sizeof(AutoVacOpts));
		opts->user_catalog_table = false;
		opts->parallel_workers = -1;
		opts->vacuum_index_cleanup = STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO;
		opts->vacuum_truncate = true;
		opts->blocksize = 0;
		opts->compresslevel = 0;
		memset(opts->compresstype, 0, NAMEDATALEN);
		opts->checksum = false;
	}

	return (bytea *) opts;
}

/*
 * ts_heap_route_to_chunk
 *		Extract the partition-column timestamp from slot, translate it
 *		to a chunk fork number, and validate that the fork is within
 *		the per-table cap.  Reports user-facing ERRORs for NULL ts,
 *		out-of-range origin, and chunk-count overflow.
 */
static ForkNumber
ts_heap_route_to_chunk(Relation rel, TupleTableSlot *slot, TSConfig *config)
{
	Datum		ts_datum;
	bool		ts_isnull;
	int64		ts_usec;
	ForkNumber	forknum;

	ts_datum = slot_getattr(slot, config->ts_attnum, &ts_isnull);
	if (ts_isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("time-series column cannot be NULL")));

	/*
	 * Convert the partition-column value to int64 microseconds (PG epoch
	 * 2000-01-01) so the routing math stays in a single representation.
	 *
	 * TZ semantics — must NOT depend on the session GUC.  TSDB's
	 * ts_time_value_to_internal() solves this by treating naive
	 * TIMESTAMP / DATE as if the wall-clock value were UTC; mirror that.
	 * Otherwise a row inserted under `SET timezone='UTC'` and a row
	 * inserted under `SET timezone='America/New_York'` would route to
	 * different chunks for the same TIMESTAMP value, and `\copy from`
	 * runs at the user's session TZ would be irreproducible.
	 *
	 * Supports:
	 *   - TIMESTAMPTZ: already int64 usec since PG epoch (UTC); take as-is.
	 *   - TIMESTAMP  : also int64 usec since PG epoch but tz-less; the
	 *                  bit pattern is interpretable as if the wall clock
	 *                  is UTC — just cast.  Equivalent to TSDB's
	 *                  ts_pg_timestamp_to_unix_microseconds path which
	 *                  is identical for both TIMESTAMP and TIMESTAMPTZ.
	 *   - DATE       : int32 days; widen via date_timestamp() which
	 *                  returns midnight UTC as a TIMESTAMP (no TZ shift),
	 *                  then cast.  date_timestamptz() (what we used to
	 *                  call) applies the session TZ — wrong for our goal.
	 *
	 * NB: ts_chunk_origin is parsed via timestamptz_in() at table creation
	 * which DOES use the session TZ at that moment for naive strings.
	 * Users should pin the origin with an explicit offset
	 * (e.g. '2019-01-01 00:00:00+00') for deterministic routing across
	 * sessions.  Documented in the reloption help text.
	 */
	switch (TupleDescAttr(rel->rd_att, config->ts_attnum - 1)->atttypid)
	{
		case TIMESTAMPTZOID:
			ts_usec = DatumGetTimestampTz(ts_datum);
			break;
		case TIMESTAMPOID:
			/* Treat naive TIMESTAMP bits as UTC microseconds-since-epoch. */
			ts_usec = DatumGetInt64(ts_datum);
			break;
		case DATEOID:
			/* Midnight UTC, no session-TZ shift. */
			ts_usec = DatumGetInt64(
						DirectFunctionCall1(date_timestamp, ts_datum));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported time-series partition column type "
							"(OID %u)",
							TupleDescAttr(rel->rd_att,
										  config->ts_attnum - 1)->atttypid),
					 errhint("Supported types: timestamp, timestamptz, date.")));
	}
	forknum = ts_calculate_chunk(ts_usec, config->origin_usec,
								 config->interval_usec);

	if (forknum == InvalidForkNumber)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("timestamp is before the chunk origin"),
				 errhint("Use a ts_chunk_origin that is at or before the earliest timestamp in your data.")));

	if (forknum > TS_MAX_CHUNK_FORKNUM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("time_series table \"%s\" has reached the per-segment chunk limit (%u)",
						RelationGetRelationName(rel), TS_MAX_CHUNKS_PER_TABLE),
				 errhint("Increase ts_chunk_interval or move ts_chunk_origin closer to the data's time range.")));

	return forknum;
}

/*
 * ts_chunk_prepare_for_insert
 *		Bring the (rel, forknum) chunk into a state where rows can be
 *		written: fork file exists, catalog row exists, and the chunk
 *		is in PARTIAL state (transitioning from COMPRESSED if needed).
 *
 *		Gated by a per-backend cache keyed on (xact, relid, fork) — only
 *		the first row per (xact, fork) pays the smgr/catalog cost.
 *
 *		Slow-path gate intentionally does NOT include ts_ins_blkno:
 *		conflating "buffer cache stale" with "catalog setup needed"
 *		causes a re-entry bug.  The auto-truncate branch invalidates
 *		ts_ins_blkno (heap was truncated to 0 blocks); under READ
 *		COMMITTED the status UPDATE below isn't visible to the same
 *		statement's later snapshot reads, so the next row would re-enter
 *		the slow path, re-take the auto-truncate branch, and fail
 *		update_status with TM_SelfModified on the same ts_chunk row.
 *		ts_chunk_get_insert_buffer handles blkno-Invalid on its own
 *		(via smgrnblocks resync).
 */
static void
ts_chunk_prepare_for_insert(Relation rel, ForkNumber forknum,
							TSConfig *config, TransactionId xid)
{
	SMgrRelation	smgr;
	Oid				relid = RelationGetRelid(rel);

	if (ts_ins_fork == forknum && ts_ins_relid == relid && ts_ins_xid == xid)
		return;	/* fast path: already prepared for this (xact, fork) */

	/*
	 * Per-chunk advisory ShareLock.  Conflicts with the ExclusiveLock
	 * that compress / reclaim take on the same chunk, so an in-flight
	 * compress on this chunk blocks this INSERT until it commits.
	 *
	 * Held until xact end; subsequent rows that hit the cache skip
	 * re-acquire because lockmgr already has our entry (and dedups
	 * defensively if it doesn't).
	 */
	ts_chunk_lock(relid, (int32) forknum, ShareLock);

	RelationOpenSmgr(rel);
	smgr = rel->rd_smgr;
	if (!smgrexists(smgr, forknum))
	{
		int64	range_start;
		int64	range_end;

		/*
		 * Serialise smgrcreate against concurrent inserters: two
		 * backends both reading !smgrexists would each call
		 * smgrcreate(false) and the loser fails with "File exists".
		 * Brief LockRelationForExtension dedups; re-check after
		 * acquiring in case another backend already created.
		 */
		LockRelationForExtension(rel, ExclusiveLock);
		if (!smgrexists(smgr, forknum))
			smgrcreate(smgr, forknum, false);
		UnlockRelationForExtension(rel, ExclusiveLock);

		range_start = config->origin_usec +
			(int64)(forknum - TS_FIRST_CHUNKNUM) * config->interval_usec;
		range_end = range_start + config->interval_usec;

		ts_chunk_catalog_insert(relid, forknum, range_start, range_end);
	}
	else if (!ts_chunk_catalog_exists_cached(relid, (int32) forknum))
	{
		/* Fork exists on disk but catalog row missing (rollback recovery). */
		int64	range_start;
		int64	range_end;

		range_start = config->origin_usec +
			(int64)(forknum - TS_FIRST_CHUNKNUM) * config->interval_usec;
		range_end = range_start + config->interval_usec;

		ts_chunk_catalog_insert(relid, forknum, range_start, range_end);
	}

	/*
	 * Transition COMPRESSED → PARTIAL.  Before flipping status we
	 * truncate the heap fork to 0 blocks to enforce the PARTIAL
	 * invariant "heap holds only rows INSERTed since the last compress".
	 *
	 * lock_if_compressed atomically checks status and takes a row-level
	 * exclusive lock on the ts_chunk tuple, so concurrent INSERTs in
	 * different xacts serialise on the COMPRESSED→PARTIAL flip without
	 * "tuple concurrently updated" errors.
	 *
	 * Abort behaviour: heap truncation persists (smgrtruncate is non-
	 * transactional) but ts_compressed_chunk / PAX are unchanged, so
	 * COMPRESSED scans still return authoritative PAX content — no
	 * user-visible data loss.
	 */
	if (ts_chunk_catalog_lock_if_compressed(relid, (int32) forknum))
	{
		ForkNumber		tforks[1] = { forknum };
		BlockNumber		tblocks[1] = { 0 };

		if (smgrexists(smgr, forknum) && smgrnblocks(smgr, forknum) > 0)
		{
			/*
			 * Cascade-delete TOAST rows referenced by tuples we're
			 * about to discard; otherwise they'd orphan in the user
			 * table's TOAST relation.
			 */
			ts_heap_fork_cleanup_toast(rel, forknum);

			Assert(!MyProc->delayChkptEnd);
			MyProc->delayChkptEnd = true;

			if (RelationNeedsWAL(rel))
				ts_wal_fork_truncate(rel, forknum, 0);

			smgrtruncate(smgr, tforks, 1, tblocks);

			MyProc->delayChkptEnd = false;
		}

		ts_chunk_catalog_update_status(relid, (int32) forknum, TS_CHUNK_PARTIAL);
		ts_ins_blkno = InvalidBlockNumber;	/* heap is empty; force refresh */
	}

	/*
	 * Mark this (xact, fork) as prepared.  Reset blkno so the next
	 * ts_chunk_get_insert_buffer call resyncs via smgrnblocks.
	 */
	ts_ins_blkno = InvalidBlockNumber;
	ts_ins_fork = forknum;
	ts_ins_relid = relid;
	ts_ins_xid = xid;
}

/*
 * ts_heap_prepare_insert
 *		Counterpart to upstream heap_prepare_insert.  Materialises the
 *		tuple from slot, stamps the header (xmin/cmin/infomask, table
 *		OID, FROZEN if requested), and applies TOAST.  Returns the
 *		tuple to actually store (== input tup if no TOAST was needed,
 *		else a fresh palloc'd tuple).
 *
 *		The returned tuple may differ from the input — caller should
 *		track shouldFree so it can pfree both the slot-owned tuple
 *		(if any) and the TOAST'd tuple.
 *
 *		Pre-stamping the header before TOAST is required: heap_toast_
 *		insert_or_update writes pg_toast rows whose visibility is tied
 *		to the parent tuple's xmin/cmin, so those fields must already
 *		be set.
 */
static HeapTuple
ts_heap_prepare_insert(Relation rel, TupleTableSlot *slot,
					   TransactionId xid, CommandId cid, int options,
					   bool *shouldFree)
{
	HeapTuple	tup;

	tup = ExecFetchSlotHeapTuple(slot, true, shouldFree);

	Assert(HeapTupleHeaderGetNatts(tup->t_data) <=
		   RelationGetNumberOfAttributes(rel));

	slot->tts_tableOid = RelationGetRelid(rel);
	tup->t_tableOid = slot->tts_tableOid;

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(tup->t_data);

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, InvalidTransactionId);

	/*
	 * TOAST: push oversized / external-attribute columns out.  Without
	 * this a row whose post-MAXALIGN size exceeds MaxHeapTupleSize
	 * PANICs in PageAddItem inside ts_wal_insert's critical section.
	 *
	 * Time-series relations are always RELKIND_RELATION (enforced at
	 * CREATE TABLE), so unlike heap_prepare_insert we don't need the
	 * MATVIEW / catalog-table escape hatch.
	 */
	if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
	{
		HeapTuple	toasted = heap_toast_insert_or_update(rel, tup, NULL, options);

		if (toasted != tup)
		{
			if (*shouldFree)
				heap_freetuple(tup);
			tup = toasted;
			*shouldFree = true;
		}
	}

	return tup;
}

/*
 * ts_heap_tuple_insert
 *		Single-row INSERT.  Mirrors upstream heap_insert structure:
 *		route → chunk-prepare → tuple-prepare → SSI check → get-buffer
 *		→ put-tuple-and-WAL → set-TID → release.  Non-ts relations
 *		delegate straight to heap.
 */
static void
ts_heap_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
					 int options, struct BulkInsertStateData *bistate)
{
	TSConfig		config;
	TransactionId	xid;
	ForkNumber		forknum;
	HeapTuple		tup;
	bool			shouldFree = false;
	Buffer			buf;
	OffsetNumber	offnum;

	if (!RelationIsTimeSeries(rel))
	{
		heap_am->tuple_insert(rel, slot, cid, options, bistate);
		return;
	}

	/*
	 * INSERT ... ON CONFLICT isn't yet wired through the chunk-fork WAL
	 * (no speculative-insert flag on the WAL record, no companion
	 * abort/confirm records).  Reject early — a partial implementation
	 * would leak speculative tokens through crash recovery.
	 */
	if (options & HEAP_INSERT_SPECULATIVE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT ... ON CONFLICT is not supported on time_series tables")));

	if (!ts_get_config(rel, &config))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("time_series table missing configuration")));

	xid = GetCurrentTransactionId();

	/* Route the row to its chunk fork. */
	forknum = ts_heap_route_to_chunk(rel, slot, &config);

	/* Ensure fork + catalog are ready; flip COMPRESSED → PARTIAL. */
	ts_chunk_prepare_for_insert(rel, forknum, &config, xid);

	/* Stamp header, TOAST. */
	tup = ts_heap_prepare_insert(rel, slot, xid, cid, options, &shouldFree);

	/*
	 * SSI conflict check.  Matches upstream heap_insert; only relevant
	 * if any predicate locks have been taken on this relation.  Cheap
	 * when there are none.
	 */
	CheckForSerializableConflictIn(rel, NULL, InvalidBlockNumber);

	/*
	 * Get exclusively-locked buffer with room for the tuple.  Single
	 * tuple_insert has no BulkInsertState (matching PG's heap_insert
	 * which also passes NULL).  Our internal cache pin still gives
	 * per-row hash-lookup avoidance even without bistate.
	 */
	buf = ts_chunk_get_insert_buffer(rel, forknum, MAXALIGN(tup->t_len), NULL);

	/*
	 * Mirror upstream heap_insert: take the critical section
	 * around put + dirty + WAL.  init_page is decided from page
	 * state BEFORE PageAddItem — a freshly extended page reports
	 * max-offset == 0 / Invalid here, which tells redo it can
	 * re-init the page instead of reading it from disk.
	 */
	{
		Page		page = BufferGetPage(buf);
		bool		init_page = (PageGetMaxOffsetNumber(page) == 0 ||
								 PageGetMaxOffsetNumber(page) == InvalidOffsetNumber);

		START_CRIT_SECTION();

		offnum = ts_relation_put_heap_tuple(buf, tup,
											(options & HEAP_INSERT_SPECULATIVE) != 0);
		MarkBufferDirty(buf);
		ts_wal_insert(rel, buf, tup, offnum, init_page);

		END_CRIT_SECTION();
	}

	UnlockReleaseBuffer(buf);

	/*
	 * Mirror heap_insert tail: pgstat counter first (inside heap_insert
	 * itself, post-release), then heapam_tuple_insert wrapper does the
	 * slot t_self copy and shouldFree.
	 */
	pgstat_count_heap_insert(rel, 1);

	ItemPointerCopy(&tup->t_self, &slot->tts_tid);

	if (shouldFree)
		pfree(tup);
}

/*
 * ts_heap_multi_insert
 *		Bulk INSERT for COPY and INSERT...SELECT.  Mirrors the two-phase
 *		structure of upstream heap_multi_insert:
 *
 *		  phase 1 (prepare loop):
 *		      for each slot: route to chunk, ts_heap_prepare_insert
 *		      (header stamp + TOAST).  Buffers are NOT locked here, so
 *		      TOAST writes are safe.
 *		  phase 2 (SSI):
 *		      one CheckForSerializableConflictIn for the whole batch.
 *		  phase 3 (flush loop):
 *		      for each maximal same-fork page-sized run: chunk-prepare,
 *		      get-buffer, critical section { put-loop + MarkBufferDirty
 *		      + ts_wal_multi_insert }, stamp TIDs, release.
 *
 *		Because all TOAST work is done up front, the flush phase can
 *		batch by exact post-TOAST sizes — no page-fit pre-check, no
 *		risk of orphaned pg_toast rows.
 *
 *		Scratch arrays palloc'd at nslots; caller-side batching keeps
 *		that small (CopyMultiInsertBuffer ≈ 1000).
 */
static void
ts_heap_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
					 CommandId cid, int options,
					 struct BulkInsertStateData *bistate)
{
	TSConfig		config;
	TransactionId	xid;
	HeapTuple	   *tuples;
	bool		   *shouldfree;
	ForkNumber	   *forks;
	OffsetNumber   *offnums;
	int				i;

	if (!RelationIsTimeSeries(rel))
	{
		heap_am->multi_insert(rel, slots, nslots, cid, options, bistate);
		return;
	}

	/* See ts_heap_tuple_insert: speculative insert path isn't WAL-safe yet. */
	if (options & HEAP_INSERT_SPECULATIVE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT ... ON CONFLICT is not supported on time_series tables")));

	if (!ts_get_config(rel, &config))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("time_series table missing configuration")));

	xid = GetCurrentTransactionId();

	tuples = (HeapTuple *) palloc(sizeof(HeapTuple) * nslots);
	shouldfree = (bool *) palloc(sizeof(bool) * nslots);
	forks = (ForkNumber *) palloc(sizeof(ForkNumber) * nslots);
	offnums = (OffsetNumber *) palloc(sizeof(OffsetNumber) * nslots);

	/* Route + prepare every tuple before touching any buffer. */
	for (i = 0; i < nslots; i++)
	{
		forks[i] = ts_heap_route_to_chunk(rel, slots[i], &config);
		shouldfree[i] = false;
		tuples[i] = ts_heap_prepare_insert(rel, slots[i], xid, cid, options,
										   &shouldfree[i]);
	}

	/* SSI conflict check — once per call, matching heap_multi_insert. */
	CheckForSerializableConflictIn(rel, NULL, InvalidBlockNumber);

	/*
	 * Flush.  Mirrors the heap_multi_insert loop exactly — size buffer
	 * for the first tuple, put it unconditionally
	 * (RelationGetBufferForTuple has ensured it fits), then walk
	 * nthispage = 1, 2, ... packing as many subsequent same-fork
	 * tuples as the page's remaining free space accepts.
	 */
	{
		int		ndone = 0;
		Size	saveFreeSpace = RelationGetTargetPageFreeSpace(rel,
															   HEAP_DEFAULT_FILLFACTOR);

		while (ndone < nslots)
		{
			ForkNumber	batch_fork = forks[ndone];
			int			nthispage;
			Buffer		buf;
			Page		page;
			bool		init_page;

			CHECK_FOR_INTERRUPTS();

			/* Ensure fork + catalog ready; flip COMPRESSED → PARTIAL. */
			ts_chunk_prepare_for_insert(rel, batch_fork, &config, xid);

			/*
			 * Size the buffer for the first tuple — analogous to
			 * RelationGetBufferForTuple(heaptuples[ndone]->t_len).
			 */
			buf = ts_chunk_get_insert_buffer(rel, batch_fork,
											 MAXALIGN(tuples[ndone]->t_len),
											 bistate);
			page = BufferGetPage(buf);
			init_page = (PageGetMaxOffsetNumber(page) == 0 ||
						 PageGetMaxOffsetNumber(page) == InvalidOffsetNumber);

			START_CRIT_SECTION();

			/* First tuple fits by construction. */
			offnums[ndone] = ts_relation_put_heap_tuple(buf, tuples[ndone], false);

			/* Pack as many subsequent same-fork tuples as fit on the page. */
			for (nthispage = 1; ndone + nthispage < nslots; nthispage++)
			{
				int		next = ndone + nthispage;

				if (forks[next] != batch_fork)
					break;
				if (PageGetHeapFreeSpace(page) <
					MAXALIGN(tuples[next]->t_len) + saveFreeSpace)
					break;

				offnums[next] = ts_relation_put_heap_tuple(buf, tuples[next], false);
			}

			MarkBufferDirty(buf);
			ts_wal_multi_insert(rel, buf, &tuples[ndone], nthispage,
								&offnums[ndone], init_page);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(buf);
			ndone += nthispage;
		}
	}

	/*
	 * Second SSI check — mirrors heap_multi_insert's post-loop check.
	 * Catches rw-conflicts where a concurrent scan locked the table
	 * between our before-check and the buffer locks: without this the
	 * scan would observe neither the predicate lock nor the new rows.
	 */
	CheckForSerializableConflictIn(rel, NULL, InvalidBlockNumber);

	/*
	 * Copy t_self into all caller slots; if a row directory is
	 * active, allocate a row_seq per tuple, record into the rowdir
	 * accumulator, and stamp the encoded TID instead of the raw one.
	 */
	for (i = 0; i < nslots; i++)
		slots[i]->tts_tid = tuples[i]->t_self;

	pgstat_count_heap_insert(rel, nslots);

	for (i = 0; i < nslots; i++)
	{
		if (shouldfree[i])
			pfree(tuples[i]);
	}

	pfree(tuples);
	pfree(shouldfree);
	pfree(forks);
	pfree(offnums);
}

/*
 * ts_heap_loadpage
 *		Mirror of PG's heapgetpage for our chunk-fork layout.  Releases
 *		any previously-pinned scan page, pins the requested (forknum,
 *		blkno), then — when SO_ALLOW_PAGEMODE is set — share-locks the
 *		buffer, runs heap_page_prune_opt + TestForOldSnapshot, scans
 *		every itemid for MVCC visibility under a small one-tuple
 *		(xmin,cid) cache, invokes HeapCheckForSerializableConflictOut
 *		on each result, collects the visible offnums into
 *		scan->rs_vistuples, and unlocks.  Non-page-mode callers (rare —
 *		non-MVCC snapshots) get just the pinned buffer back and lock /
 *		walk it themselves to match heapgettup.
 */
static void
ts_heap_loadpage(ChunkScanDesc scan, ForkNumber forknum, BlockNumber blkno)
{
	Buffer			buffer;
	Page			dp;
	Snapshot		snapshot;
	int				lines;
	int				ntup;
	OffsetNumber	lineoff;
	ItemId			lpp;
	bool			all_visible;
	TransactionId	t_xmin = 0;
	CommandId		t_cid = 0;

	/* Release the previous scan page, mirroring heapgetpage:399-404. */
	if (BufferIsValid(scan->cur_buffer))
	{
		ReleaseBuffer(scan->cur_buffer);
		scan->cur_buffer = InvalidBuffer;
	}

	/*
	 * Cancellation hook: a sequential scan across many chunk forks would
	 * otherwise be uninterruptible inside the chunk loop.  Matches the
	 * per-page check in heapgetpage:411 / heapgettup:751.
	 */
	CHECK_FOR_INTERRUPTS();

	buffer = ReadBufferExtended(scan->rel, forknum, blkno, RBM_NORMAL,
								scan->rs_strategy);
	scan->cur_buffer = buffer;
	scan->cur_block = blkno;
	scan->rs_ntuples = 0;
	scan->rs_cindex = 0;

	/*
	 * For non-MVCC snapshots SO_ALLOW_PAGEMODE was cleared in scan_begin;
	 * such callers walk the page under share-lock themselves so we just
	 * hand back the pinned buffer.
	 */
	if (!(scan->base.rs_flags & SO_ALLOW_PAGEMODE))
		return;

	snapshot = scan->snapshot;

	/* Opportunistic HOT prune: takes its own lock, harmless if nothing to do. */
	heap_page_prune_opt(scan->rel, buffer);

	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	TestForOldSnapshot(snapshot, scan->rel, dp);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;

	/*
	 * time_series does not maintain a visibility map (its forks live
	 * outside the standard MAIN_FORKNUM that VM is keyed on), so we cannot
	 * take the PageIsAllVisible fast path from heapgetpage:464.  Always
	 * per-tuple.
	 */
	all_visible = false;

	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		if (ItemIdIsNormal(lpp))
		{
			HeapTupleData	loctup;
			bool			valid;
			HeapTupleHeader	theader = (HeapTupleHeader) PageGetItem(dp, lpp);

			loctup.t_tableOid = RelationGetRelid(scan->rel);
			loctup.t_data = theader;
			loctup.t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&loctup.t_self, blkno, lineoff);

			if (all_visible)
			{
				valid = true;
			}
			else
			{
				/*
				 * Single-entry (xmin, cid) cache shared across the page,
				 * mirroring the GPDB-specific block at heapgetpage:495-520:
				 * batch-inserted rows on the same page share xmin+cid and
				 * skip the repeated HeapTupleSatisfiesVisibility recompute.
				 * Disable the cache once xmax is non-trivial — the rules
				 * for locked-only / multi-XID tuples diverge.
				 */
				bool	use_cache;

				if ((theader->t_infomask & HEAP_XMAX_INVALID) != 0 ||
					HEAP_XMAX_IS_LOCKED_ONLY(theader->t_infomask))
					use_cache = true;
				else
					use_cache = false;

				if (use_cache &&
					t_xmin == HeapTupleHeaderGetXmin(theader) &&
					t_cid == HeapTupleHeaderGetRawCommandId(theader))
				{
					valid = true;
				}
				else
				{
					valid = HeapTupleSatisfiesVisibility(scan->rel,
														 &loctup, snapshot,
														 buffer);
					if (valid && use_cache)
					{
						t_xmin = HeapTupleHeaderGetXmin(loctup.t_data);
						t_cid = HeapTupleHeaderGetRawCommandId(loctup.t_data);
					}
				}
			}

			HeapCheckForSerializableConflictOut(valid, scan->rel,
												&loctup, buffer, snapshot);

			if (valid)
				scan->rs_vistuples[ntup++] = lineoff;
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}

/*
 * ts_heap_scan_begin
 *		Open a table scan on a time_series table.  Probes the storage
 *		manager for existing time-series chunks within the expected
 *		range (origin to now + interval, with a safety margin) and
 *		builds the chunk list that scan_getnextslot will iterate.
 *		Delegates to heap for non-ts tables.
 */
static TableScanDesc
ts_heap_scan_begin(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
				   ParallelTableScanDesc pscan, uint32 flags)
{
	ChunkScanDesc	scan;
	TSChunkList		list;

	if (!RelationIsTimeSeries(rel))
		return heap_am->scan_begin(rel, snapshot, nkeys, key, pscan, flags);

	/*
	 * Parallel scans are not supported.  The inherited heap parallel
	 * descriptor seeds itself from RelationGetNumberOfBlocks(MAIN), which
	 * is always 0 for time_series (data lives in forks >= 4), so workers
	 * would scan nothing.  Reject explicitly rather than return wrong
	 * results.
	 */
	if (pscan != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("time_series tables do not support parallel scans")));

	/*
	 * Mirror heap_beginscan: a non-MVCC snapshot cannot safely use page
	 * mode.  We do not honor SO_ALLOW_PAGEMODE in getnextslot today, but
	 * keep the flag accurate so EXPLAIN and any future page-mode path see
	 * the same value heap would compute.
	 */
	if (!(snapshot && IsMVCCSnapshot(snapshot)))
		flags &= ~SO_ALLOW_PAGEMODE;

	scan = (ChunkScanDesc) palloc0(sizeof(ChunkScanDescData));
	scan->base.rs_rd = rel;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = pscan;
	scan->base.rs_nkeys = nkeys;
	scan->rel = rel;
	scan->snapshot = snapshot;
	scan->cur_buffer = InvalidBuffer;

	/* Copy the scan key, if any (matches heap_beginscan's allocation). */
	if (nkeys > 0)
	{
		scan->base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->base.rs_key, key, sizeof(ScanKeyData) * nkeys);
	}
	else
		scan->base.rs_key = NULL;

	/*
	 * Pin the relcache entry so a concurrent invalidation cannot free it
	 * out from under the scan.  Paired with RelationDecrementReferenceCount
	 * in scan_end.
	 */
	RelationIncrementReferenceCount(rel);

	/*
	 * SSI: SEQSCAN / SAMPLESCAN under serializable isolation must take a
	 * relation-level predicate lock so concurrent inserts conflict.  This
	 * matches heap_beginscan and complements the per-TID PredicateLockTID
	 * in ts_heap_fetch_row_version.
	 */
	if (scan->base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
	{
		Assert(snapshot);
		PredicateLockRelation(rel, snapshot);
	}

	/*
	 * Bulk-read strategy.  Time_series relations are typically large and
	 * cold-cache scans pollute the shared buffer ring; honor SO_ALLOW_STRAT
	 * unconditionally rather than gating on a per-fork block count (which
	 * would require opening smgr for every chunk just to decide).
	 */
	if (scan->base.rs_flags & SO_ALLOW_STRAT)
		scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
	else
		scan->rs_strategy = NULL;

	/* Stats counter for sequential scans, same as heap. */
	if (scan->base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(rel);

	/*
	 * TableAM seq scan entry point (used by ANALYZE / VACUUM / COPY OUT
	 * and any caller that opens a scan via table_beginscan).  No WHERE
	 * clause is available here, so we have to scan every chunk the
	 * catalog knows about.  Time-based chunk pruning happens in the
	 * Custom Scan path (ts_scan.c) which receives min/max bounds from
	 * set_rel_pathlist_hook.
	 *
	 * Load statuses too: COMPRESSED chunks live in the PAX sidecar (heap
	 * fork is stale/empty); a simple heap-fork walk would silently drop
	 * those rows.  See ts_compress.h for the chunk-status state machine.
	 */
	list = ts_chunk_catalog_get_chunks_with_status(
		RelationGetRelid(rel), TS_FIRST_CHUNKNUM, TS_MAX_CHUNK_FORKNUM);
	scan->chunks = list.chunks;
	scan->statuses = list.statuses;
	scan->n_chunks = list.n;
	scan->cur_chunk_idx = 0;
	scan->cur_block = 0;
	scan->cur_offset = FirstOffsetNumber;
	scan->pax_exhausted = false;	/* pax / internal_slot zero-init via palloc0 */

	return (TableScanDesc) scan;
}

/*
 * ts_heap_scan_end
 *		Release resources held by a ts_heap scan descriptor.
 */
static void
ts_heap_scan_end(TableScanDesc sscan)
{
	ChunkScanDesc	scan = (ChunkScanDesc) sscan;

	/* If this was delegated to heap, delegate end too. */
	if (!RelationIsTimeSeries(sscan->rs_rd))
	{
		heap_am->scan_end(sscan);
		return;
	}

	/*
	 * cur_buffer is pin-only between getnextslot calls (we unlock right
	 * after copying the tuple), so use ReleaseBuffer rather than
	 * UnlockReleaseBuffer to match heap_endscan's contract.
	 */
	if (BufferIsValid(scan->cur_buffer))
	{
		ReleaseBuffer(scan->cur_buffer);
		scan->cur_buffer = InvalidBuffer;
	}
	ts_pax_iter_end(&scan->pax);
	if (scan->chunks)
		pfree(scan->chunks);
	if (scan->statuses)
		pfree(scan->statuses);

	/* Pair with scan_begin allocations. */
	if (scan->base.rs_key)
		pfree(scan->base.rs_key);
	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	RelationDecrementReferenceCount(scan->base.rs_rd);

	/*
	 * If the caller transferred snapshot ownership to the scan (e.g. via
	 * table_beginscan_strat with a copied snapshot), drop the registration
	 * here.  Mirrors heap_endscan.
	 */
	if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->base.rs_snapshot);

	pfree(scan);
}

/*
 * ts_heap_scan_rescan
 *		Reset the scan position to the beginning of the chunk list.
 */
static void
ts_heap_scan_rescan(TableScanDesc sscan, struct ScanKeyData *key,
					bool set_params, bool allow_strat,
					bool allow_sync, bool allow_pagemode)
{
	ChunkScanDesc	scan = (ChunkScanDesc) sscan;

	if (!RelationIsTimeSeries(sscan->rs_rd))
	{
		heap_am->scan_rescan(sscan, key, set_params, allow_strat,
							 allow_sync, allow_pagemode);
		return;
	}

	/*
	 * Update the strat/sync/pagemode flags from rescan arguments, matching
	 * heap_rescan.  SO_ALLOW_SYNC is tracked for API parity even though we
	 * do not implement syncscan startblock coordination across chunks.
	 */
	if (set_params)
	{
		if (allow_strat)
			scan->base.rs_flags |= SO_ALLOW_STRAT;
		else
			scan->base.rs_flags &= ~SO_ALLOW_STRAT;

		if (allow_sync)
			scan->base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->base.rs_flags &= ~SO_ALLOW_SYNC;

		if (allow_pagemode && scan->base.rs_snapshot &&
			IsMVCCSnapshot(scan->base.rs_snapshot))
			scan->base.rs_flags |= SO_ALLOW_PAGEMODE;
		else
			scan->base.rs_flags &= ~SO_ALLOW_PAGEMODE;

		/* Mirror initscan: re-acquire or release the bulk-read strategy. */
		if (allow_strat)
		{
			if (scan->rs_strategy == NULL)
				scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
		}
		else if (scan->rs_strategy != NULL)
		{
			FreeAccessStrategy(scan->rs_strategy);
			scan->rs_strategy = NULL;
		}
	}

	/* See note in scan_end on ReleaseBuffer vs UnlockReleaseBuffer. */
	if (BufferIsValid(scan->cur_buffer))
	{
		ReleaseBuffer(scan->cur_buffer);
		scan->cur_buffer = InvalidBuffer;
	}

	/*
	 * Refresh the scan key, if one was supplied.  Callers that pass a new
	 * key on rescan (e.g. parameterized inner scans) would otherwise see
	 * the old key silently.
	 */
	if (key != NULL && scan->base.rs_nkeys > 0)
		memcpy(scan->base.rs_key, key,
			   sizeof(ScanKeyData) * scan->base.rs_nkeys);

	ts_pax_iter_reset_chunk(&scan->pax);
	scan->pax_exhausted = false;
	scan->cur_chunk_idx = 0;
	scan->cur_block = 0;
	scan->cur_offset = FirstOffsetNumber;
}

/*
 * ts_heap_scan_getnextslot
 *		Fetch the next visible tuple from the chunk list.  Iterates
 *		blocks within each chunk, checking MVCC visibility for every
 *		tuple.  Copies the tuple off the buffer before returning so
 *		the buffer lock can be released immediately.
 */
static bool
ts_heap_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ChunkScanDesc	scan = (ChunkScanDesc) sscan;
	SMgrRelation	smgr;
	Oid				tableoid;
	bool			pagemode;

	if (!RelationIsTimeSeries(sscan->rs_rd))
		return heap_am->scan_getnextslot(sscan, direction, slot);

	/*
	 * Forward-only.  Chunk forks, the chunk-list iterator, and the
	 * per-page offset cursor are all advanced monotonically by this
	 * routine; reverse traversal would require parallel logic that v1
	 * does not implement.  Reject the call rather than silently scan
	 * the wrong way.
	 */
	if (!ScanDirectionIsForward(direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("time_series tables do not support backward scans")));

	ExecClearTuple(slot);

	pagemode = (sscan->rs_flags & SO_ALLOW_PAGEMODE) != 0;
	tableoid = RelationGetRelid(scan->rel);

	RelationOpenSmgr(scan->rel);
	smgr = scan->rel->rd_smgr;

	while (scan->cur_chunk_idx < scan->n_chunks)
	{
		ForkNumber	forknum = scan->chunks[scan->cur_chunk_idx];
		int16		status = scan->statuses[scan->cur_chunk_idx];
		BlockNumber	nblocks;

		/*
		 * PAX sidecar first (COMPRESSED is PAX-only; PARTIAL has
		 * pre-compress rows in PAX + post-compress rows in heap;
		 * ACTIVE has no PAX file).  ts_pax_iter_next lazily opens the
		 * reader on first call and auto-closes at EOF.
		 */
		if (!scan->pax_exhausted &&
			(status == TS_CHUNK_COMPRESSED || status == TS_CHUNK_PARTIAL))
		{
			if (ts_pax_iter_next(&scan->pax, scan->rel, forknum, slot))
			{
				slot->tts_tableOid = tableoid;
				return true;
			}
			scan->pax_exhausted = true;	/* PAX done for this chunk */
		}

		/*
		 * Heap fork next.  Skipped for COMPRESSED chunks because the
		 * heap there either is empty (post-reclaim) or carries a
		 * redundant copy of the PAX rows (pre-reclaim) that we already
		 * returned above — reading it would double-count.
		 */
		if (status != TS_CHUNK_ACTIVE && status != TS_CHUNK_PARTIAL)
			goto advance_chunk;
		nblocks = smgrnblocks(smgr, forknum);

		while (scan->cur_block < nblocks)
		{
			Page		page;

			scan->rs_ctup.t_tableOid = tableoid;

			if (pagemode)
			{
				/*
				 * Page mode (the common case for MVCC snapshots): load
				 * the next page via the helper if our pinned buffer is
				 * stale, then walk the pre-collected visible offnums
				 * without the buffer lock.  Each returned tuple is
				 * stored pin-only via ExecStoreBufferHeapTuple — the
				 * slot retains its own pin so the executor can read the
				 * tuple after we return.
				 */
				if (!BufferIsValid(scan->cur_buffer) ||
					BufferGetBlockNumber(scan->cur_buffer) != scan->cur_block)
				{
					ts_heap_loadpage(scan, forknum, scan->cur_block);
				}

				page = BufferGetPage(scan->cur_buffer);

				while (scan->rs_cindex < scan->rs_ntuples)
				{
					OffsetNumber	lineoff = scan->rs_vistuples[scan->rs_cindex++];
					ItemId			lpp = PageGetItemId(page, lineoff);

					Assert(ItemIdIsNormal(lpp));

					scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
					scan->rs_ctup.t_len = ItemIdGetLength(lpp);
					ItemPointerSet(&scan->rs_ctup.t_self, scan->cur_block, lineoff);

					/*
					 * Honor scankey qualifiers: heap_beginscan copies
					 * them into rs_key for us; heap_getnextslot runs
					 * HeapKeyTest in heapgettup_pagemode.  Skipping this
					 * silently returned rows that didn't match.
					 */
					if (sscan->rs_nkeys > 0 && sscan->rs_key != NULL)
					{
						bool	valid;

						HeapKeyTest(&scan->rs_ctup, RelationGetDescr(scan->rel),
									sscan->rs_nkeys, sscan->rs_key, valid);
						if (!valid)
							continue;
					}

					pgstat_count_heap_getnext(scan->rel);

					slot->tts_tableOid = tableoid;
					ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->cur_buffer);
					return true;
				}

				/*
				 * Page exhausted — advance to next block; loadpage drops
				 * the old pin and pins the new block on the next pass.
				 */
				scan->cur_block++;
				scan->cur_offset = FirstOffsetNumber;
				scan->rs_cindex = 0;
				scan->rs_ntuples = 0;
				continue;
			}
			else
			{
				/*
				 * Non-page-mode (non-MVCC snapshot): heapgettup-style
				 * loop.  We hold the buffer share-lock while scanning
				 * itemids and drop it before returning a valid tuple,
				 * re-acquiring on the next call.  Per-tuple visibility
				 * and SSI conflict-out checks run under the lock so the
				 * tuple data does not shift beneath us.
				 */
				int		maxoff;

				if (!BufferIsValid(scan->cur_buffer) ||
					BufferGetBlockNumber(scan->cur_buffer) != scan->cur_block)
				{
					ts_heap_loadpage(scan, forknum, scan->cur_block);
					scan->cur_offset = FirstOffsetNumber;
				}

				LockBuffer(scan->cur_buffer, BUFFER_LOCK_SHARE);
				page = BufferGetPage(scan->cur_buffer);
				TestForOldSnapshot(scan->snapshot, scan->rel, page);
				maxoff = PageGetMaxOffsetNumber(page);

				while (scan->cur_offset <= maxoff)
				{
					ItemId			itemid;
					OffsetNumber	lineoff = scan->cur_offset++;
					bool			valid;

					itemid = PageGetItemId(page, lineoff);

					if (!ItemIdIsNormal(itemid))
						continue;

					scan->rs_ctup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
					scan->rs_ctup.t_len = ItemIdGetLength(itemid);
					ItemPointerSet(&scan->rs_ctup.t_self, scan->cur_block, lineoff);

					valid = HeapTupleSatisfiesVisibility(scan->rel, &scan->rs_ctup,
														 scan->snapshot,
														 scan->cur_buffer);

					HeapCheckForSerializableConflictOut(valid, scan->rel,
														&scan->rs_ctup,
														scan->cur_buffer,
														scan->snapshot);

					if (!valid)
						continue;

					if (sscan->rs_nkeys > 0 && sscan->rs_key != NULL)
					{
						HeapKeyTest(&scan->rs_ctup, RelationGetDescr(scan->rel),
									sscan->rs_nkeys, sscan->rs_key, valid);
						if (!valid)
							continue;
					}

					LockBuffer(scan->cur_buffer, BUFFER_LOCK_UNLOCK);
					pgstat_count_heap_getnext(scan->rel);

					slot->tts_tableOid = tableoid;
					ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->cur_buffer);
					return true;
				}

				LockBuffer(scan->cur_buffer, BUFFER_LOCK_UNLOCK);
				scan->cur_block++;
				scan->cur_offset = FirstOffsetNumber;
			}
		}

advance_chunk:
		/* Release the chunk's last-used page pin before moving on. */
		if (BufferIsValid(scan->cur_buffer))
		{
			ReleaseBuffer(scan->cur_buffer);
			scan->cur_buffer = InvalidBuffer;
		}
		ts_pax_iter_reset_chunk(&scan->pax);
		scan->pax_exhausted = false;
		scan->cur_chunk_idx++;
		scan->cur_block = 0;
		scan->cur_offset = FirstOffsetNumber;
		scan->rs_ntuples = 0;
		scan->rs_cindex = 0;
	}

	return false;
}

/*
 * sample_consider_live_tuple
 *		Shared reservoir-sample decision used by both the heap-fork and
 *		PAX phases below.  Caller has already determined the row is
 *		live; we increment liverows, then either drop the row into the
 *		reservoir (numrows < targrows) or replace a random slot under
 *		Vitter's Algorithm Z.  The HeapTuple passed in must be palloc'd
 *		(we either keep it or free it here).
 */
static inline void
sample_consider_live_tuple(HeapTuple tup, HeapTuple *rows, int targrows,
							int *numrows, double *liverows,
							double *rowstoskip, ReservoirState rstate)
{
	*liverows += 1;
	if (*numrows < targrows)
	{
		rows[(*numrows)++] = tup;
		return;
	}
	if (*rowstoskip < 0)
		*rowstoskip = reservoir_get_next_S(rstate, *liverows - 1, targrows);
	if (*rowstoskip <= 0)
	{
		int		k = (int) (targrows * sampler_random_fract(rstate->randstate));

		Assert(k >= 0 && k < targrows);
		heap_freetuple(rows[k]);
		rows[k] = tup;
	}
	else
	{
		heap_freetuple(tup);
	}
	*rowstoskip -= 1;
}

/*
 * compare_rows
 *		qsort_arg comparator that orders sampled rows by their physical
 *		(block, offset) position.  Matches analyze.c:compare_rows so the
 *		downstream attribute-correlation statistic (computed by std_typanalyze)
 *		sees rows in scan order, the same expectation it has for heap.
 */
static int
ts_compare_rows(const void *a, const void *b, void *arg)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber	ba;
	OffsetNumber oa;
	BlockNumber	bb;
	OffsetNumber ob;

	/*
	 * PAX-source tuples carry InvalidBlockNumber in t_self because they
	 * are heap_form_tuple'd from a virtual slot without a physical home.
	 * Skip comparing them — matches analyze.c:compare_rows which short-
	 * circuits the same way for AO/CO tables.
	 */
	if (!BlockNumberIsValid(ItemPointerGetBlockNumberNoCheck(&ha->t_self)) ||
		!BlockNumberIsValid(ItemPointerGetBlockNumberNoCheck(&hb->t_self)))
		return 0;

	ba = ItemPointerGetBlockNumber(&ha->t_self);
	oa = ItemPointerGetOffsetNumber(&ha->t_self);
	bb = ItemPointerGetBlockNumber(&hb->t_self);
	ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}

/*
 * ts_locate_fork_for_vblock
 *		Given a virtual block index (0..total_heap_blocks) and a prefix-sum
 *		array `offsets` of size nforks+1 such that offsets[i+1] = offsets[i]
 *		+ nblocks_of_fork_i, return the fork index that owns vblk via
 *		binary search.  The local block within the fork is
 *		vblk - offsets[lo].
 */
static int
ts_locate_fork_for_vblock(BlockNumber vblk, BlockNumber *offsets, int nforks)
{
	int		lo = 0;
	int		hi = nforks;

	while (lo < hi - 1)
	{
		int		mid = (lo + hi) / 2;

		if (offsets[mid] <= vblk)
			lo = mid;
		else
			hi = mid;
	}
	return lo;
}

/*
 * ts_heap_acquire_sample_rows
 *		Collect a random sample of rows from all chunks using reservoir
 *		sampling.  This replaces the standard 2-stage block sampling which
 *		fails for ts_heap because RelationGetNumberOfBlocks reads MAIN
 *		fork (always 0 blocks for ts_heap).
 *
 *		The approach: iterate all blocks in all chunks, examine every tuple
 *		for MVCC visibility, count live/dead rows, and use Vitter's
 *		reservoir algorithm (Algorithm Z) to select a uniform random sample
 *		of at most targrows tuples.
 */
static int
ts_heap_acquire_sample_rows(Relation onerel, int elevel,
							HeapTuple *rows, int targrows,
							double *totalrows, double *totaldeadrows)
{
	int				numrows = 0;
	double			liverows = 0;
	double			deadrows = 0;
	double			rowstoskip = -1;
	ReservoirStateData rstate;
	TSChunkList		clist;
	int				i;
	SMgrRelation	smgr;
	TransactionId	OldestXmin;
	TsPaxIter		pax = {0};
	TupleTableSlot *pax_out_slot;
	BlockSamplerData bs;
	long			randseed;
	BlockNumber		total_heap_blocks = 0;
	BlockNumber		blksdone = 0;
	int				n_heap_forks = 0;
	ForkNumber	   *heap_forks = NULL;
	BlockNumber	   *heap_fork_off = NULL;	/* prefix-sum array, size n_heap_forks+1 */
	double			heap_live = 0;
	double			pax_live = 0;
#ifdef USE_PREFETCH
	int				prefetch_maximum = 0;
	BlockSamplerData prefetch_bs;
#endif

	Assert(targrows > 0);

	OldestXmin = GetOldestNonRemovableTransactionId(onerel);

	/*
	 * Load chunks AND statuses.  A heap-only walk would silently drop
	 * COMPRESSED chunks (whose rows live in the PAX sidecar) and skew
	 * reltuples downward, breaking planner estimates on any compressed
	 * relation.
	 */
	clist = ts_chunk_catalog_get_chunks_with_status(
		RelationGetRelid(onerel), TS_FIRST_CHUNKNUM, TS_MAX_CHUNK_FORKNUM);
	if (clist.n == 0)
	{
		*totalrows = 0;
		*totaldeadrows = 0;
		return 0;
	}

	RelationOpenSmgr(onerel);
	smgr = onerel->rd_smgr;
	/* Virtual slot we feed to PAX; rows are heap_form_tuple'd before sampling. */
	pax_out_slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel),
											&TTSOpsVirtual);

	reservoir_init_selection_state(&rstate, targrows);

	/*
	 * Build a prefix-sum index over heap-fork sizes for ACTIVE / PARTIAL
	 * chunks.  Heap-fork blocks form a single virtual block space of
	 * total_heap_blocks; a BlockSampler over that space picks ~targrows
	 * blocks regardless of how many chunks there are.  COMPRESSED chunks
	 * carry no heap data and are excluded.
	 */
	heap_forks = (ForkNumber *) palloc(sizeof(ForkNumber) * clist.n);
	heap_fork_off = (BlockNumber *) palloc(sizeof(BlockNumber) * (clist.n + 1));
	heap_fork_off[0] = 0;
	for (i = 0; i < clist.n; i++)
	{
		ForkNumber	f = clist.chunks[i];
		int16		status = clist.statuses[i];
		BlockNumber	n = 0;

		if (status == TS_CHUNK_ACTIVE || status == TS_CHUNK_PARTIAL)
		{
			if (smgrexists(smgr, f))
				n = smgrnblocks(smgr, f);
		}
		if (n == 0)
			continue;
		heap_forks[n_heap_forks] = f;
		total_heap_blocks += n;
		heap_fork_off[n_heap_forks + 1] = total_heap_blocks;
		n_heap_forks++;
	}

	if (total_heap_blocks > 0)
	{
		randseed = random();
		(void) BlockSampler_Init(&bs, total_heap_blocks, targrows, randseed);

		pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
									 (int64) total_heap_blocks);

#ifdef USE_PREFETCH
		prefetch_maximum =
			get_tablespace_maintenance_io_concurrency(onerel->rd_rel->reltablespace);
		if (prefetch_maximum)
		{
			(void) BlockSampler_Init(&prefetch_bs, total_heap_blocks,
									 targrows, randseed);

			/* Seed the prefetch ring. */
			for (i = 0; i < prefetch_maximum; i++)
			{
				BlockNumber pvblk;
				int			pidx;

				if (!BlockSampler_HasMore(&prefetch_bs))
					break;
				pvblk = BlockSampler_Next(&prefetch_bs);
				pidx = ts_locate_fork_for_vblock(pvblk, heap_fork_off,
												 n_heap_forks);
				PrefetchBuffer(onerel, heap_forks[pidx],
							   pvblk - heap_fork_off[pidx]);
			}
		}
#endif
	}

	/*
	 * Sample heap-fork blocks via Vitter's algorithm.  BlockSampler
	 * returns block indices in ascending order, so within any one
	 * chunk fork we hit its blocks in ascending block-number order —
	 * matching heap's per-relation analyze scan order.
	 */
	while (total_heap_blocks > 0 && BlockSampler_HasMore(&bs))
	{
		BlockNumber	vblk;
		ForkNumber	forknum;
		BlockNumber	blk;
		int			fidx;
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offnum;

		vacuum_delay_point();

		vblk = BlockSampler_Next(&bs);
		fidx = ts_locate_fork_for_vblock(vblk, heap_fork_off, n_heap_forks);
		forknum = heap_forks[fidx];
		blk = vblk - heap_fork_off[fidx];

#ifdef USE_PREFETCH
		if (prefetch_maximum && BlockSampler_HasMore(&prefetch_bs))
		{
			BlockNumber pvblk = BlockSampler_Next(&prefetch_bs);
			int			pidx = ts_locate_fork_for_vblock(pvblk, heap_fork_off,
														 n_heap_forks);

			PrefetchBuffer(onerel, heap_forks[pidx],
						   pvblk - heap_fork_off[pidx]);
		}
#endif

		buf = ReadBufferExtended(onerel, forknum, blk, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
		{
			ItemId			itemid;
			HeapTupleData	targtuple;
			HTSV_Result		htsv;

			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
				continue;

			targtuple.t_tableOid = RelationGetRelid(onerel);
			targtuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			targtuple.t_len = ItemIdGetLength(itemid);
			ItemPointerSet(&targtuple.t_self, blk, offnum);

			htsv = HeapTupleSatisfiesVacuum(onerel, &targtuple, OldestXmin, buf);

			switch (htsv)
			{
				case HEAPTUPLE_LIVE:
					sample_consider_live_tuple(heap_copytuple(&targtuple),
												rows, targrows,
												&numrows, &liverows,
												&rowstoskip, &rstate);
					heap_live += 1;
					break;

				case HEAPTUPLE_DEAD:
				case HEAPTUPLE_RECENTLY_DEAD:
					deadrows += 1;
					break;

				case HEAPTUPLE_INSERT_IN_PROGRESS:
					if (TransactionIdIsCurrentTransactionId(
							HeapTupleHeaderGetRawXmin(targtuple.t_data)))
					{
						liverows += 1;
						heap_live += 1;
					}
					break;

				case HEAPTUPLE_DELETE_IN_PROGRESS:
					liverows += 1;
					heap_live += 1;
					break;

				default:
					break;
			}
		}

		UnlockReleaseBuffer(buf);

		blksdone++;
		pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE,
									 (int64) blksdone);
	}

	/*
	 * Walk PAX rows for COMPRESSED + PARTIAL chunks.  PAX has no
	 * block-level layout that BlockSampler can target, so we walk
	 * every row; vacuum_delay_point() keeps autovacuum responsive on
	 * large compressed relations.  PAX row counts feed the reservoir
	 * and the totals as exact values, not extrapolated.
	 */
	for (i = 0; i < clist.n; i++)
	{
		ForkNumber	forknum = clist.chunks[i];
		int16		status = clist.statuses[i];

		if (status != TS_CHUNK_COMPRESSED && status != TS_CHUNK_PARTIAL)
			continue;

		while (ts_pax_iter_next(&pax, onerel, forknum, pax_out_slot))
		{
			HeapTuple	tup;

			CHECK_FOR_INTERRUPTS();
			vacuum_delay_point();
			slot_getallattrs(pax_out_slot);
			tup = heap_form_tuple(RelationGetDescr(onerel),
								   pax_out_slot->tts_values,
								   pax_out_slot->tts_isnull);
			sample_consider_live_tuple(tup, rows, targrows,
										&numrows, &liverows,
										&rowstoskip, &rstate);
			pax_live += 1;
		}
		ts_pax_iter_reset_chunk(&pax);
	}

	/*
	 * Sort the reservoir by (block, offset) so std_typanalyze sees rows
	 * in physical order — required for the attribute correlation
	 * statistic.  PAX tuples carry InvalidBlockNumber TIDs and sort
	 * before heap tuples; that's fine because they originated from a
	 * distinct fork and were never expected to share a TID space with
	 * heap tuples.  Matches analyze.c:1938 qsort_interruptible step.
	 */
	if (numrows == targrows)
		qsort_arg((void *) rows, numrows, sizeof(HeapTuple),
				  ts_compare_rows, NULL);

	/*
	 * Extrapolate heap-fork row counts: BlockSampler scanned bs.m of
	 * total_heap_blocks, so the seen rows-per-block ratio scaled by
	 * total_heap_blocks approximates the population.  PAX is counted
	 * exactly because the PAX walk visited every row.
	 */
	{
		double	heap_total = 0;
		double	heap_dead_total = 0;

		if (total_heap_blocks > 0 && bs.m > 0)
		{
			heap_total = floor((heap_live / bs.m) * total_heap_blocks + 0.5);
			heap_dead_total = floor((deadrows / bs.m) * total_heap_blocks + 0.5);
		}
		*totalrows = heap_total + pax_live;
		*totaldeadrows = heap_dead_total;
	}

	ereport(elevel,
			(errmsg("time_series: \"%s\": sampled %u of %u heap blocks "
					"(%.0f live, %.0f dead), walked %.0f pax rows; "
					"reservoir holds %d rows, estimated total %.0f",
					RelationGetRelationName(onerel),
					blksdone, total_heap_blocks,
					heap_live, deadrows, pax_live,
					numrows, *totalrows)));

	ts_pax_iter_end(&pax);
	ExecDropSingleTupleTableSlot(pax_out_slot);

	if (heap_forks)
		pfree(heap_forks);
	if (heap_fork_off)
		pfree(heap_fork_off);
	if (clist.chunks)
		pfree(clist.chunks);
	if (clist.statuses)
		pfree(clist.statuses);
	return numrows;
}

/*
 * ts_heap_estimate_rel_size
 *		Estimate the size of a ts_heap relation for planner purposes.
 *		Sums block counts across all chunks and uses tuple density from
 *		pg_class (if available) or estimates from attribute widths.
 */
static void
ts_heap_estimate_rel_size(Relation rel, int32 *attr_widths,
						  BlockNumber *pages, double *tuples, double *allvisfrac)
{
	BlockNumber		curpages = 0;
	BlockNumber		relpages;
	double			reltuples;
	double			density;
	List		   *chunks;
	ListCell	   *lc;
	SMgrRelation	smgr;

	if (!RelationIsTimeSeries(rel))
	{
		heap_am->relation_estimate_size(rel, attr_widths, pages, tuples, allvisfrac);
		return;
	}

	chunks = ts_chunk_catalog_get_chunks(RelationGetRelid(rel));
	if (chunks != NIL)
	{
		RelationOpenSmgr(rel);
		smgr = rel->rd_smgr;
		foreach(lc, chunks)
		{
			ForkNumber f = (ForkNumber) lfirst_int(lc);
			if (smgrexists(smgr, f))
				curpages += smgrnblocks(smgr, f);
		}
		list_free(chunks);
	}

	/*
	 * Apply the same heuristic as heap: if the table has never been analyzed
	 * yet (reltuples < 0), use at least 10 pages to avoid bad plans for
	 * newly-created tables.
	 */
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;

	if (curpages < 10 && reltuples < 0 && !rel->rd_rel->relhassubclass)
		curpages = 10;

	*pages = curpages;

	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/* Estimate tuple count from previous density or attribute widths */
	if (reltuples >= 0 && relpages > 0)
	{
		density = reltuples / (double) relpages;
	}
	else
	{
		int32	tuple_width;
		Size	overhead = MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData);
		Size	usable = BLCKSZ - SizeOfPageHeaderData;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += (int32) overhead;
		density = (double) usable / (double) tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	/* ts_heap doesn't track all-visible pages, so report 0 */
	*allvisfrac = 0;
}

/*
 * ts_heap_relation_nontransactional_truncate
 *		Truncate all chunk forks to 0 blocks and remove their catalog
 *		entries.  Also truncate the MAIN fork (which is normally empty
 *		but may have been extended by heap fallback paths).
 *
 *		For non-ts tables, delegates to the standard heap truncate.
 */
static void
ts_heap_relation_nontransactional_truncate(Relation rel)
{
	List	   *chunks;
	ListCell   *lc;
	SMgrRelation smgr;

	if (!RelationIsTimeSeries(rel))
	{
		heap_am->relation_nontransactional_truncate(rel);
		return;
	}

	/*
	 * Invalidate the cached block hint BEFORE we truncate the forks.
	 * The blkno would otherwise point past EOF.
	 */
	ts_ins_blkno = InvalidBlockNumber;

	chunks = ts_chunk_catalog_get_chunks(RelationGetRelid(rel));

	if (chunks != NIL)
	{
		RelationOpenSmgr(rel);
		smgr = rel->rd_smgr;

		/*
		 * Mirror RelationTruncate's (storage.c) pending-sync handling
		 * once for the whole relation; per-fork bookkeeping below.
		 */
		RelationPreTruncate(rel);

		foreach(lc, chunks)
		{
			ForkNumber	f = (ForkNumber) lfirst_int(lc);
			ForkNumber	forks[1] = { f };
			BlockNumber	blocks[1] = { 0 };
			if (!smgrexists(smgr, f))
				continue;

			/*
			 * Reset the per-fork nblocks cache (kernel slot for standard
			 * forks, extension sidecar entry for chunk forks) so any
			 * subsequent smgrnblocks() picks the post-truncate length
			 * off the disk instead of returning the stale value.
			 */
			ts_smgr_invalidate_nblocks(smgr, f);

			/*
			 * Order matches RelationTruncate at storage.c:357-403:
			 * delay checkpoint completion across (WAL emit, smgrtruncate)
			 * so a concurrent checkpoint cannot land a record that lets
			 * recovery start from a point AFTER our WAL truncate but
			 * BEFORE the file shrink.
			 */
			Assert(!MyProc->delayChkptEnd);
			MyProc->delayChkptEnd = true;

			if (RelationNeedsWAL(rel))
				ts_wal_fork_truncate(rel, f, 0);

			smgrtruncate(smgr, forks, 1, blocks);

			MyProc->delayChkptEnd = false;
		}

		list_free(chunks);
	}

	/* Remove all chunk catalog entries */
	ts_chunk_catalog_delete(RelationGetRelid(rel));

	/* Truncate MAIN fork via heap */
	RelationTruncate(rel, 0);
}

/*
 * ts_heap_relation_set_new_filenode
 *		Create new storage for the relation (used by TRUNCATE and
 *		ALTER TABLE).  After the new filenode is created, we must
 *		delete the old chunk catalog entries because the old forks
 *		are gone.
 *
 *		For non-ts tables, delegates to the standard heap implementation.
 */
static void
ts_heap_relation_set_new_filenode(Relation rel,
								 const RelFileNode *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	bool	is_ts = RelationIsTimeSeries(rel);
	Oid		old_relid;

	/*
	 * Remember the old relid for catalog cleanup.  Must do this before
	 * the heap callback changes the underlying storage.
	 */
	old_relid = RelationGetRelid(rel);

	/* Delegate to heap for the actual filenode creation */
	heap_am->relation_set_new_filenode(rel, newrnode, persistence,
									   freezeXid, minmulti);

	/*
	 * For time_series tables, purge the chunk catalog entries that referred
	 * to forks in the old filenode.  New chunks will be created lazily
	 * on the next INSERT.
	 *
	 * Compressed-chunk catalog (ts_compressed_chunk) and the PAX sidecar
	 * directory (ts_compressed/<old_relid>/) for the OLD relfilenode also
	 * need to go, otherwise:
	 *   - ts_compressed_chunk rows from the old generation would shadow
	 *     reads on the fresh relfilenode (post-TRUNCATE the table has
	 *     no data, but stale rows make readers think it does)
	 *   - the PAX files leak on disk forever
	 *
	 * ts_compress_config is INTENTIONALLY left untouched — TRUNCATE
	 * means "delete data", not "reset table configuration".  The user's
	 * declared compression rule survives the TRUNCATE and any subsequent
	 * INSERT can be re-compressed via the existing policy.
	 *
	 * The PAX dir removal goes through the deferred (xact-callback)
	 * mechanism so a ROLLBACK after TRUNCATE leaves the old generation
	 * untouched on disk — matching the catalog rollback semantics.
	 */
	if (is_ts)
	{
		ts_chunk_catalog_delete(old_relid);
		ts_compressed_chunk_delete(old_relid);
		ts_pax_register_pending_removal(MyDatabaseId, old_relid, true);

		/*
		 * The per-backend ts_ins cache remembers the last (fork, relid,
		 * xid, blkno) an INSERT touched so subsequent rows in the same
		 * xact skip the smgrcreate + ts_chunk_catalog_insert dance.
		 * TRUNCATE (transactional), REFRESH MATERIALIZED VIEW, and
		 * ALTER-that-rewrites all route through this callback and
		 * substitute a fresh relfilenode -- but the cache triple still
		 * matches on (fork, relid, xid), so a follow-up INSERT in the
		 * same xact would skip smgrcreate + catalog_insert against the
		 * empty new storage AND use the stale blkno hint pointing past
		 * the new fork's EOF.  Reset all four fields; the next INSERT
		 * rediscovers the state via the slow path.
		 */
		ts_ins_relid = InvalidOid;
		ts_ins_fork  = InvalidForkNumber;
		ts_ins_xid   = InvalidTransactionId;
		ts_ins_blkno = InvalidBlockNumber;
	}
}

/*
 *		DML guards: ts_heap is append-only
 *
 *		DELETE, UPDATE, and row-level LOCK are not supported because
 *		ts_heap stores data across multiple chunk forks and the TID
 *		alone does not encode which fork a tuple lives in.  Raising
 *		an explicit ERROR here prevents the inherited heap callbacks
 *		from silently operating on the wrong fork (MAIN_FORKNUM).
 */

static TM_Result
ts_heap_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
					 Snapshot snapshot, Snapshot crosscheck, bool wait,
					 TM_FailureData *tmfd, bool changingPart)
{
	if (RelationIsTimeSeries(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot delete from a time_series table"),
				 errhint("time_series tables are append-only")));

	return heap_am->tuple_delete(rel, tid, cid, snapshot, crosscheck,
								 wait, tmfd, changingPart);
}

static TM_Result
ts_heap_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
					 CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					 bool wait, TM_FailureData *tmfd,
					 LockTupleMode *lockmode, bool *update_indexes)
{
	if (RelationIsTimeSeries(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot update a time_series table"),
				 errhint("time_series tables are append-only")));

	return heap_am->tuple_update(rel, otid, slot, cid, snapshot, crosscheck,
								 wait, tmfd, lockmode, update_indexes);
}

static TM_Result
ts_heap_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
				   TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, uint8 flags,
				   TM_FailureData *tmfd)
{
	if (RelationIsTimeSeries(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot lock rows in a time_series table"),
				 errhint("time_series tables are append-only and do not support row-level locks")));

	return heap_am->tuple_lock(rel, tid, snapshot, slot, cid, mode,
							   wait_policy, flags, tmfd);
}

/*
 *		tuple_fetch_row_version: chunk-aware TID fetch
 *
 *		Called by EvalPlanQual (READ COMMITTED recheck), foreign key
 *		validation, and trigger visibility checks.  The standard heap
 *		implementation reads from MAIN_FORKNUM, which is empty for
 *		ts_heap.  We search all chunk forks for the given TID.
 *
 *		This is correct but not optimal — it scans all chunks.  In
 *		practice this path is rarely hot: ts_heap does not support
 *		DELETE/UPDATE (so EvalPlanQual won't recheck ts_heap rows),
 *		and FK references to time_series tables are uncommon.
 */

static bool
ts_heap_fetch_row_version(Relation rel, ItemPointer tid,
						  Snapshot snapshot, TupleTableSlot *slot)
{
	List	   *chunks;
	ListCell   *lc;
	BlockNumber	blk;
	OffsetNumber offnum;
	SMgrRelation smgr;
	BufferHeapTupleTableSlot *bslot;

	if (tid == NULL || !ItemPointerIsValid(tid))
		return false;

	if (!RelationIsTimeSeries(rel))
		return heap_am->tuple_fetch_row_version(rel, tid, snapshot, slot);

	/*
	 * Caller (PG executor) guarantees a buffer-heap slot for this AM
	 * callback (see heapam_fetch_row_version); the zero-copy ExecStore
	 * path below requires it.
	 */
	Assert(TTS_IS_BUFFERTUPLE(slot));
	bslot = (BufferHeapTupleTableSlot *) slot;

	blk = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);

	chunks = ts_chunk_catalog_get_chunks(RelationGetRelid(rel));
	if (chunks == NIL)
		return false;

	RelationOpenSmgr(rel);
	smgr = rel->rd_smgr;

	foreach(lc, chunks)
	{
		ForkNumber	forknum = (ForkNumber) lfirst_int(lc);
		Buffer		buf;
		Page		page;
		ItemId		itemid;
		HeapTupleData htup;
		bool		visible;
		if (!smgrexists(smgr, forknum))
			continue;
		if (blk >= smgrnblocks(smgr, forknum))
			continue;

		buf = ReadBufferExtended(rel, forknum, blk, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		/*
		 * Snapshot-too-old guard.  Mirrors heap_fetch_extended; harmless
		 * when old_snapshot_threshold is disabled (-1, the CB default).
		 */
		TestForOldSnapshot(snapshot, rel, page);

		/*
		 * Lower + upper offset bounds.  Heap checks both; we previously
		 * only guarded the upper bound.
		 */
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		htup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		htup.t_len = ItemIdGetLength(itemid);
		htup.t_tableOid = RelationGetRelid(rel);
		ItemPointerCopy(tid, &htup.t_self);

		visible = HeapTupleSatisfiesVisibility(rel, &htup, snapshot, buf);

		/*
		 * SSI predicate-lock + serializable-conflict-out, mirroring
		 * heap_fetch_extended.  Must run while the buffer is still
		 * pinned (PredicateLockTID needs the BlockNumber resolvable,
		 * and HeapCheckForSerializableConflictOut reads the tuple
		 * header).  Buffer remains SHARE-locked at this point.
		 */
		if (visible)
			PredicateLockTID(rel, &htup.t_self, snapshot,
							 HeapTupleHeaderGetXmin(htup.t_data));
		HeapCheckForSerializableConflictOut(visible, rel, &htup, buf, snapshot);

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (visible)
		{
			/*
			 * Zero-copy pin transfer: hand the still-pinned buffer to
			 * the slot.  Matches heapam_fetch_row_version's call to
			 * ExecStorePinnedBufferHeapTuple, avoiding the
			 * heap_copytuple() palloc + memcpy on every fetch.  Slot is
			 * now responsible for ReleaseBuffer.
			 */
			bslot->base.tupdata = htup;
			ExecStorePinnedBufferHeapTuple(&bslot->base.tupdata, slot, buf);
			slot->tts_tableOid = RelationGetRelid(rel);

			list_free(chunks);
			return true;
		}

		/* Not visible: release this fork's buffer, try the next chunk. */
		ReleaseBuffer(buf);
	}

	list_free(chunks);
	return false;
}

/*
 * ts_heap_relation_copy_data
 *		Blocks ALTER TABLE ... SET TABLESPACE on time_series tables.
 *		The heap implementation copies only MAIN fork data, which
 *		would silently discard all chunk data.
 */
static void
ts_heap_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	if (RelationIsTimeSeries(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change tablespace of a time_series table"),
				 errhint("time_series tables store data in chunk forks that cannot be relocated")));

	heap_am->relation_copy_data(rel, newrnode);
}

/*
 * ts_heap_relation_copy_for_cluster
 *		Blocks VACUUM FULL and CLUSTER on time_series tables.
 *		Both commands rewrite the table via this callback, but the
 *		heap implementation only copies MAIN fork, losing chunk data.
 */
static void
ts_heap_relation_copy_for_cluster(Relation NewTable, Relation OldTable,
								  Relation OldIndex, bool use_sort,
								  TransactionId OldestXmin,
								  TransactionId *xid_cutoff,
								  MultiXactId *multi_cutoff,
								  double *num_tuples,
								  double *tups_vacuumed,
								  double *tups_recently_dead)
{
	if (RelationIsTimeSeries(OldTable))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot run VACUUM FULL or CLUSTER on a time_series table"),
				 errhint("time_series tables are append-only and do not require compaction")));

	heap_am->relation_copy_for_cluster(NewTable, OldTable, OldIndex, use_sort,
									   OldestXmin, xid_cutoff, multi_cutoff,
									   num_tuples, tups_vacuumed, tups_recently_dead);
}

/*
 * ts_heap_relation_vacuum
 *		For time_series tables, VACUUM is a no-op: there are no dead tuples
 *		because DELETE and UPDATE are not supported.  The inherited heap
 *		implementation would scan the empty MAIN fork, which is harmless
 *		but wasteful.  We skip it entirely.
 *
 *		For non-ts tables, delegates to the standard heap vacuum.
 */
static void
ts_heap_relation_vacuum(Relation rel, struct VacuumParams *params,
						BufferAccessStrategy bstrategy)
{
	if (RelationIsTimeSeries(rel))
	{
		/* Nothing to do — ts_heap is append-only, no dead tuples */
		return;
	}

	heap_am->relation_vacuum(rel, params, bstrategy);
}

/*
 * ts_heap_index_build_range_scan
 *		CREATE INDEX / REINDEX entry for time_series tables.
 *
 *		Indexes on time_series tables are not supported in this build.
 *		The logical-TID + rowdir machinery was removed in favor of a
 *		ChunkScan-only data path; index support is tracked on the
 *		`logical-tid-index` branch.
 *
 *		Non-ts tables delegate to the standard heap implementation.
 */
static double
ts_heap_index_build_range_scan(Relation table_rel, Relation index_rel,
							   struct IndexInfo *index_info,
							   bool allow_sync, bool anyvisible,
							   bool progress,
							   BlockNumber start_blockno,
							   BlockNumber numblocks,
							   IndexBuildCallback callback,
							   void *callback_state,
							   TableScanDesc scan)
{
	if (!RelationIsTimeSeries(table_rel))
		return heap_am->index_build_range_scan(table_rel, index_rel, index_info,
											   allow_sync, anyvisible, progress,
											   start_blockno, numblocks,
											   callback, callback_state, scan);

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("indexes on time_series tables are not supported in this build"),
			 errhint("Index access methods are temporarily disabled; "
					 "this restriction will be lifted in a future release.")));
	return 0;	/* unreachable */
}

/*
 * ts_heap_scan_bitmap_next_block / ts_heap_scan_bitmap_next_tuple
 *		Block bitmap index scans on time_series tables.  The heap callbacks
 *		use block numbers relative to MAIN fork, which is empty.
 */
static bool
ts_heap_scan_bitmap_next_block(TableScanDesc scan,
							   struct TBMIterateResult *tbmres)
{
	if (RelationIsTimeSeries(scan->rs_rd))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bitmap scan is not supported on time_series tables")));

	return heap_am->scan_bitmap_next_block(scan, tbmres);
}

static bool
ts_heap_scan_bitmap_next_tuple(TableScanDesc scan,
							   struct TBMIterateResult *tbmres,
							   TupleTableSlot *slot)
{
	if (RelationIsTimeSeries(scan->rs_rd))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("bitmap scan is not supported on time_series tables")));

	return heap_am->scan_bitmap_next_tuple(scan, tbmres, slot);
}

/*
 * ts_heap_scan_sample_next_block / ts_heap_scan_sample_next_tuple
 *		Block TABLESAMPLE scans on time_series tables.  The heap callbacks
 *		sample blocks from MAIN fork, which is empty.
 */
static bool
ts_heap_scan_sample_next_block(TableScanDesc scan,
							   struct SampleScanState *scanstate)
{
	if (RelationIsTimeSeries(scan->rs_rd))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TABLESAMPLE is not supported on time_series tables")));

	return heap_am->scan_sample_next_block(scan, scanstate);
}

static bool
ts_heap_scan_sample_next_tuple(TableScanDesc scan,
							   struct SampleScanState *scanstate,
							   TupleTableSlot *slot)
{
	if (RelationIsTimeSeries(scan->rs_rd))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TABLESAMPLE is not supported on time_series tables")));

	return heap_am->scan_sample_next_tuple(scan, scanstate, slot);
}

/*
 * ts_heap_relation_toast_am
 *		TOAST tables for time_series relations must use plain heap, not the
 *		time_series AM.  The inherited heapam_relation_toast_am returns
 *		rel->rd_rel->relam which would incorrectly propagate the time_series
 *		AM OID to the TOAST table.
 */
static Oid
ts_heap_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/*
 * ts_heap_relation_size
 *		When asked for the MAIN fork size, also sum the sizes of all
 *		time-series chunks so that pg_relation_size() and VACUUM
 *		report a realistic total.  Non-MAIN fork queries (FSM, VM)
 *		are delegated directly to heap.
 */
static uint64
ts_heap_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64			size;
	SMgrRelation	smgr;

	/* For non-MAIN forks, delegate to heap if the fork file exists */
	if (forkNumber != MAIN_FORKNUM)
	{
		RelationOpenSmgr(rel);
		if (!smgrexists(rel->rd_smgr, forkNumber))
			return 0;
		return heap_am->relation_size(rel, forkNumber);
	}

	/* MAIN fork size from heap */
	size = heap_am->relation_size(rel, MAIN_FORKNUM);

	/* Sum all ts chunk sizes from the catalog */
	{
		List	   *chunks = ts_chunk_catalog_get_chunks(RelationGetRelid(rel));
		ListCell   *lc;

		RelationOpenSmgr(rel);
		smgr = rel->rd_smgr;

		foreach(lc, chunks)
		{
			ForkNumber f = (ForkNumber) lfirst_int(lc);
			if (smgrexists(smgr, f))
				size += (uint64) smgrnblocks(smgr, f) * BLCKSZ;
		}

		list_free(chunks);
	}

	return size;
}

/*
 * ts_tableam_handler
 *		SQL-callable handler that returns a pointer to the ts_heap
 *		TableAmRoutine.  Referenced by CREATE ACCESS METHOD.
 */
Datum
ts_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&ts_heap_methods);
}

/*
 *		ts_chunk_info SRF
 *
 *		User-facing introspection over chunk forks: yields one row per
 *		chunk with (segment_id, chunk_number, nblocks, range_start,
 *		range_end, status).  Lives here because chunks are the table-AM
 *		storage unit — same file as the heap-routine override that
 *		treats fork ≥ 4 as a chunk.
 */
Datum
ts_chunk_info(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	TSChunkInfoCtx *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		Oid				relid = PG_GETARG_OID(0);
		TupleDesc		tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Chunk metadata (time ranges, row counts, status) reveals a
		 * table's storage layout.  Restrict to table owner.
		 */
		if (!pg_class_ownercheck(relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relid));

		ctx = (TSChunkInfoCtx *) palloc0(sizeof(TSChunkInfoCtx));
		ctx->table_oid = relid;
		ctx->rel = table_open(relid, AccessShareLock);

		/* Unbounded range — caller wants every chunk. */
		ctx->list = ts_chunk_catalog_get_chunks_with_status(relid,
															TS_FIRST_CHUNKNUM,
															TS_MAX_CHUNK_FORKNUM);
		ctx->cur_idx = 0;

		tupdesc = CreateTemplateTupleDesc(6);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segment_id", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "chunk_number", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "nblocks", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "range_start", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "range_end", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "status", INT2OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = ctx;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = (TSChunkInfoCtx *) funcctx->user_fctx;

	if (ctx->cur_idx < ctx->list.n)
	{
		ForkNumber		forknum = ctx->list.chunks[ctx->cur_idx];
		SMgrRelation	smgr;
		BlockNumber		nblocks = 0;
		Datum			values[6];
		bool			nulls[6] = {false, false, false, false, false, false};
		HeapTuple		tuple;
		TSConfig		config;

		RelationOpenSmgr(ctx->rel);
		smgr = ctx->rel->rd_smgr;
		if (smgrexists(smgr, forknum))
			nblocks = smgrnblocks(smgr, forknum);

		values[0] = Int32GetDatum(GpIdentity.segindex);
		values[1] = Int32GetDatum((int32) forknum);
		values[2] = Int64GetDatum((int64) nblocks);

		/* Calculate range from chunk number and config */
		if (ts_get_config(ctx->rel, &config))
		{
			int64	range_start = config.origin_usec +
				(int64)(forknum - TS_FIRST_CHUNKNUM) *
				config.interval_usec;
			int64	range_end = range_start + config.interval_usec;

			values[3] = TimestampTzGetDatum((TimestampTz) range_start);
			values[4] = TimestampTzGetDatum((TimestampTz) range_end);
		}
		else
		{
			nulls[3] = true;
			nulls[4] = true;
		}

		values[5] = Int16GetDatum(ctx->list.statuses ? ctx->list.statuses[ctx->cur_idx] : 0);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		ctx->cur_idx++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	table_close(ctx->rel, AccessShareLock);
	if (ctx->list.chunks)
		pfree(ctx->list.chunks);
	if (ctx->list.statuses)
		pfree(ctx->list.statuses);
	SRF_RETURN_DONE(funcctx);
}

/*
 * ts_heap_methods_init
 *		Populate the ts_heap TableAmRoutine — a copy of the heap AM
 *		with the callbacks that need chunk-aware behaviour overridden.
 *		Every callback we don't touch keeps the heap implementation by
 *		identity, avoiding thin delegation wrappers.
 */
static void
ts_heap_methods_init(void)
{
	heap_am = GetHeapamTableAmRoutine();

	/* Start from a complete copy of the heap AM */
	memcpy(&ts_heap_methods, heap_am, sizeof(TableAmRoutine));

	/* Custom scan: chunk-aware begin/end/rescan/getnextslot */
	ts_heap_methods.scan_begin = ts_heap_scan_begin;
	ts_heap_methods.scan_end = ts_heap_scan_end;
	ts_heap_methods.scan_rescan = ts_heap_scan_rescan;
	ts_heap_methods.scan_getnextslot = ts_heap_scan_getnextslot;

	/* Custom insert: route tuples to time-partitioned chunks */
	ts_heap_methods.tuple_insert = ts_heap_tuple_insert;
	ts_heap_methods.multi_insert = ts_heap_multi_insert;

	/* Custom relation_size: include ts chunk sizes */
	ts_heap_methods.relation_size = ts_heap_relation_size;

	/* Custom ANALYZE: chunk-aware sampling and size estimation */
	ts_heap_methods.relation_acquire_sample_rows = ts_heap_acquire_sample_rows;
	ts_heap_methods.relation_estimate_size = ts_heap_estimate_rel_size;

	/* Custom TRUNCATE: truncate all chunk forks and catalog entries */
	ts_heap_methods.relation_nontransactional_truncate = ts_heap_relation_nontransactional_truncate;
	ts_heap_methods.relation_set_new_filenode = ts_heap_relation_set_new_filenode;

	/* DML guards: reject DELETE/UPDATE/LOCK on time_series tables */
	ts_heap_methods.tuple_delete = ts_heap_tuple_delete;
	ts_heap_methods.tuple_update = ts_heap_tuple_update;
	ts_heap_methods.tuple_lock = ts_heap_tuple_lock;

	/* Chunk-aware TID fetch for EvalPlanQual / FK checks */
	ts_heap_methods.tuple_fetch_row_version = ts_heap_fetch_row_version;

	/* Guards: block operations that would lose data or produce wrong results */
	ts_heap_methods.relation_copy_data = ts_heap_relation_copy_data;
	ts_heap_methods.relation_copy_for_cluster = ts_heap_relation_copy_for_cluster;
	ts_heap_methods.relation_vacuum = ts_heap_relation_vacuum;
	ts_heap_methods.index_build_range_scan = ts_heap_index_build_range_scan;
	ts_heap_methods.scan_bitmap_next_block = ts_heap_scan_bitmap_next_block;
	ts_heap_methods.scan_bitmap_next_tuple = ts_heap_scan_bitmap_next_tuple;
	ts_heap_methods.scan_sample_next_block = ts_heap_scan_sample_next_block;
	ts_heap_methods.scan_sample_next_tuple = ts_heap_scan_sample_next_tuple;

	/* Custom reloptions: ts_partition_column, ts_chunk_interval, ts_chunk_origin */
	ts_heap_methods.amoptions = ts_heap_amoptions;

	/* TOAST tables must use plain heap, not time_series */
	ts_heap_methods.relation_toast_am = ts_heap_relation_toast_am;
}

/*
 * ts_tableam_init
 *		Bring up every access-layer / storage-layer subsystem
 *		time_series needs before any DDL can reference the AM.
 *
 *		Everything below is idempotent per-backend and safe to call
 *		once from _PG_init.  Ordering is load-bearing where noted;
 *		everything else could be reordered.
 */
void
ts_tableam_init(void)
{
	/*
	 * relpath_hook must be installed before smgr opens any chunk
	 * fork, or the standard path-builder would try to name our forks
	 * itself and mdsyncfiletag's fallback branch would not find them.
	 */
	ts_fork_name_init();

	/*
	 * Custom reloptions must be registered before any relcache lookup
	 * of a time_series table, otherwise rd_options ends up NULL and
	 * ts_get_config returns false.
	 */
	ts_reloptions_init();

	/*
	 * smgr sidecar so opening chunk forks >= TS_FIRST_CHUNKNUM does
	 * not overflow the fixed-size kernel per-fork arrays.
	 */
	ts_smgr_init();

	/*
	 * PAX process-exit hook.  libpaxformat.so registers C++ static
	 * destructors that double-free memory PG's proc_exit already
	 * released; the hook installs on_proc_exit + atexit callbacks
	 * that call _exit() before those destructors run.  Must be
	 * registered during _PG_init because the startup process loads
	 * the .so but never touches PAX functions lazily.
	 */
	ts_pax_register_exit_hook();

	/* The AM callback table itself. */
	ts_heap_methods_init();

	/*
	 * ChunkScan / ChunkAppend Custom Scan providers plus the two
	 * planner hooks that inject their paths.  Must be registered on
	 * QD and every QE so custom-scan plan nodes deserialise
	 * cluster-wide after dispatch.
	 */
	ts_scan_scan_init();
	ts_chunk_append_init();
	ts_scan_pathlist_init();
	ts_scan_planner_init();

	/* ProcessUtility hook: CREATE TABLE validation + DROP/TRUNCATE cleanup. */
	ts_hooks_init();

	/* Custom WAL resource manager for chunk-fork writes + PAX. */
	ts_wal_init();
}
