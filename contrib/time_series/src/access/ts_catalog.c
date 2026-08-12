/*-------------------------------------------------------------------------
 *
 * ts_catalog.c
 *    Direct heap operations on the time_series.ts_chunk table.
 *
 *    Each QE segment opens ts_chunk via table_open() and operates on
 *    its local heap files — no SPI, no distributed dispatch.
 *
 *    ts_chunk is a regular extension table, not a system catalog.
 *    We use plain heap sequential scans (table_beginscan / heap_getnext)
 *    with ScanKey filtering.  The table is small (one row per chunk per
 *    table per segment), so a sequential scan is efficient enough.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/access/ts_catalog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "libpq-fe.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/int8.h"
#include "utils/resowner.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/proc.h"

#include "../include/time_series.h"
#include "../include/access/ts_tableam.h"
#include "../include/compress/ts_compress.h"

/* Cached OID for the ts_chunk relation */
static Oid	ts_chunk_relid = InvalidOid;

/*
 * Cached OID for the composite (table_oid, chunk_number) btree index.
 * Resolved lazily on first use by ts_chunk_ensure_idx_oid().  InvalidOid
 * means "not yet resolved".
 */
static Oid	ts_chunk_oid_chunknum_idx_relid = InvalidOid;

/*
 * Both OIDs above are "sticky for the backend lifetime" only as long as
 * the underlying tables never disappear out from under them.  That
 * assumption breaks across DROP EXTENSION time_series [CASCADE];
 * CREATE EXTENSION time_series; in the SAME backend session: the
 * extension owns its schema (time_series.control: schema =
 * 'time_series'), so DROP EXTENSION drops the whole schema and every
 * table in it, and CREATE EXTENSION recreates ts_chunk and its index
 * under brand-new OIDs.  Without invalidation, ts_chunk_ensure_oid()/
 * ts_chunk_ensure_idx_oid() would keep returning the stale, now-dropped
 * OIDs for the rest of the session -- table_open() on a dropped OID
 * normally fails loudly, but if Postgres's OID counter has wrapped and
 * reassigned that exact OID to an unrelated object in the meantime
 * (plausible on a long-lived backend with heavy DDL churn), catalog
 * writes would silently land in the wrong relation.
 *
 * ts_chunk_ensure_oid()'s own comment documents that a per-call
 * SearchSysCacheExists1 revalidation was tried and reverted (crash
 * correlation under CAGG fault injection), so we use the same relcache
 * callback pattern already proven for extension_proxy_oid in
 * time_series.c: reset on either a whole-cache invalidation
 * (relid == InvalidOid) or a hit on one of our two cached OIDs
 * specifically.  Registered lazily on first resolution so this file
 * doesn't need its own _PG_init hook.
 */
static bool	ts_chunk_oid_callback_registered = false;

static void
ts_chunk_oid_relcache_callback(Datum arg, Oid relid)
{
	if (!OidIsValid(relid) ||
		relid == ts_chunk_relid ||
		relid == ts_chunk_oid_chunknum_idx_relid)
	{
		ts_chunk_relid = InvalidOid;
		ts_chunk_oid_chunknum_idx_relid = InvalidOid;
	}
}

static void
ts_chunk_oid_callback_ensure_registered(void)
{
	if (ts_chunk_oid_callback_registered)
		return;
	CacheRegisterRelcacheCallback(ts_chunk_oid_relcache_callback, (Datum) 0);
	ts_chunk_oid_callback_registered = true;
}

/*
 * Transaction-level cache of known chunk (table_oid, chunk_number) pairs.
 * Avoids repeated catalog lookups on every INSERT row within the same
 * top-level transaction.  Automatically cleared at transaction end
 * (commit or abort) so that TRUNCATE and ROLLBACK invalidate it.
 */
#define TS_CHUNK_CACHE_SIZE 256

typedef struct TSChunkCacheEntry
{
	Oid		table_oid;
	int32	chunk_number;
} TSChunkCacheEntry;

static TSChunkCacheEntry ts_chunk_cache[TS_CHUNK_CACHE_SIZE];
static int	ts_chunk_cache_count = 0;
static TransactionId ts_chunk_cache_xid = InvalidTransactionId;

/*
 * (chunk_number, status) pair carried through the List used by
 * ts_chunk_catalog_get_chunks_with_status — the pointer-list variant
 * for the path that needs both fields.  Local to this file.
 */
typedef struct ChunkEntry
{
	ForkNumber	chunk;
	int16		status;
} ChunkEntry;

/*
 * High-bit marker that puts our advisory locks in a private namespace
 * disjoint from user pg_advisory_lock() keys.  See ts_chunk_lock for
 * the full rationale.
 */
#define TS_CHUNK_LOCK_MAGIC		0x80000000U

/*
 * Ensure the cache belongs to the current transaction.
 * If the transaction changed (new xid), clear the cache.
 */
static void
ts_chunk_cache_check_xid(void)
{
	TransactionId	cur_xid = GetTopTransactionIdIfAny();

	if (cur_xid != ts_chunk_cache_xid)
	{
		ts_chunk_cache_count = 0;
		ts_chunk_cache_xid = cur_xid;
	}
}

/*
 * ts_chunk_cache_invalidate
 *		Drop every cache entry recorded for table_oid.  Must be called
 *		by ts_chunk_catalog_delete() before it removes the real ts_chunk
 *		rows: without this, a chunk_number that was cached as
 *		"known to exist" earlier in the SAME xact (e.g. by an INSERT
 *		right before a same-xact TRUNCATE) stays cached as existing
 *		even after the row is deleted.  A later ts_chunk_catalog_insert()
 *		for that same chunk_number (e.g. the INSERT following the
 *		TRUNCATE) then hits the stale positive-cache fast path at the
 *		top of ts_chunk_catalog_insert() and returns immediately
 *		*without* re-inserting the catalog row -- even though the row
 *		it should have pointed at was just deleted.  The row's bytes
 *		land in a freshly created chunk-fork file with no ts_chunk
 *		entry to reference it, so every scan (which discovers chunks
 *		by walking ts_chunk) skips that fork forever: silent, permanent
 *		data loss reachable via `BEGIN; INSERT; TRUNCATE; INSERT;
 *		COMMIT;` whenever the post-TRUNCATE INSERT reuses a chunk
 *		number the table already had before the TRUNCATE.
 */
static void
ts_chunk_cache_invalidate(Oid table_oid)
{
	int	i;
	int	kept = 0;

	ts_chunk_cache_check_xid();

	for (i = 0; i < ts_chunk_cache_count; i++)
	{
		if (ts_chunk_cache[i].table_oid != table_oid)
			ts_chunk_cache[kept++] = ts_chunk_cache[i];
	}
	ts_chunk_cache_count = kept;
}

/*
 * ts_chunk_catalog_exists_cached
 *		Fast check: is this (table_oid, chunk_number) known to be in the
 *		catalog?  Uses the transaction-level cache — false means "unknown,
 *		need to check the actual catalog".
 */
bool
ts_chunk_catalog_exists_cached(Oid table_oid, int32 chunk_number)
{
	int		i;

	ts_chunk_cache_check_xid();

	for (i = 0; i < ts_chunk_cache_count; i++)
	{
		if (ts_chunk_cache[i].table_oid == table_oid &&
			ts_chunk_cache[i].chunk_number == chunk_number)
			return true;
	}
	return false;
}

static void
ts_chunk_cache_add(Oid table_oid, int32 chunk_number)
{
	ts_chunk_cache_check_xid();

	if (ts_chunk_cache_count >= TS_CHUNK_CACHE_SIZE)
	{
		/* Evict oldest half when full */
		int keep = TS_CHUNK_CACHE_SIZE / 2;

		memmove(ts_chunk_cache, ts_chunk_cache + keep,
				sizeof(TSChunkCacheEntry) * keep);
		ts_chunk_cache_count = keep;
	}

	ts_chunk_cache[ts_chunk_cache_count].table_oid = table_oid;
	ts_chunk_cache[ts_chunk_cache_count].chunk_number = chunk_number;
	ts_chunk_cache_count++;
}

/*
 * ts_chunk_ensure_oid
 *		Populate the cached OID for the ts_chunk relation.
 *
 *		NOTE: an *active*, per-call revalidation via
 *		SearchSysCacheExists1 was tried and reverted — under CAGG
 *		fault-injected concurrent REFRESH (cagg_threshold_race
 *		isolation2 test) it correlated with a SIGSEGV in another
 *		session.  That is a different mechanism from the *passive*
 *		CacheRegisterRelcacheCallback registered at the top of this
 *		file: the callback only fires on an actual relcache
 *		invalidation event, does no catalog access itself (same
 *		constraint extension_relcache_callback in time_series.c
 *		already follows), and simply resets the two OIDs below to
 *		InvalidOid so the next ensure_*_oid() call re-resolves them —
 *		it does not re-introduce the synchronous SearchSysCacheExists1
 *		call that correlated with the crash.
 */
static bool
ts_chunk_ensure_oid(void)
{
	Oid		ns_oid;

	ts_chunk_oid_callback_ensure_registered();

	if (OidIsValid(ts_chunk_relid))
		return true;

	ns_oid = ht_get_namespace_oid_cached();
	if (!OidIsValid(ns_oid))
		return false;

	ts_chunk_relid = get_relname_relid("ts_chunk", ns_oid);
	if (!OidIsValid(ts_chunk_relid))
		return false;

	return true;
}

/*
 * ts_chunk_ensure_idx_oid
 *		Lazily resolve and cache the OID of the (table_oid, chunk_number)
 *		composite btree index.  The index is created by
 *		time_series--1.0.sql, so its absence is treated as a hard error —
 *		callers depend on it for O(log N) catalog probes and have no
 *		correctness fallback.
 */
static Oid
ts_chunk_ensure_idx_oid(void)
{
	Oid		ns_oid;

	ts_chunk_oid_callback_ensure_registered();

	if (OidIsValid(ts_chunk_oid_chunknum_idx_relid))
		return ts_chunk_oid_chunknum_idx_relid;

	ns_oid = ht_get_namespace_oid_cached();
	if (!OidIsValid(ns_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("time_series schema not found"),
				 errhint("Is the time_series extension installed?")));

	ts_chunk_oid_chunknum_idx_relid =
		get_relname_relid("ts_chunk_oid_chunknum_idx", ns_oid);

	if (!OidIsValid(ts_chunk_oid_chunknum_idx_relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("index time_series.ts_chunk_oid_chunknum_idx not found"),
				 errhint("The time_series extension appears to be partially installed; "
						 "expected CREATE INDEX from time_series--1.0.sql to have run.")));

	return ts_chunk_oid_chunknum_idx_relid;
}

/*
 * ts_chunk_catalog_insert
 *		Insert a chunk record into the local segment's ts_chunk heap.
 *
 *		Scans the heap first — if a row for (table_oid, chunk_number)
 *		already exists, the insert is silently skipped.
 */
void
ts_chunk_catalog_insert(Oid table_oid, ForkNumber fork,
						int64 range_start_usec, int64 range_end_usec)
{
	Relation	rel;
	TupleDesc	tupdesc;
	SysScanDesc hscan;
	ScanKeyData skey[2];
	HeapTuple	existing;
	Datum		values[Natts_ts_chunk];
	bool		nulls[Natts_ts_chunk];
	HeapTuple	tup;
	Oid			idx_relid;

	/*
	 * Fast path: if this chunk was already registered in this session,
	 * skip the expensive catalog lookup entirely.
	 */
	if (ts_chunk_catalog_exists_cached(table_oid, (int32) fork))
		return;

	if (!ts_chunk_ensure_oid())
		return;		/* ts_chunk table not yet created */

	rel = table_open(ts_chunk_relid, RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	/*
	 * Existence check.  We use SnapshotSelf so the same xact creating
	 * multiple chunks doesn't try to insert duplicates across multiple
	 * slow-path entries.  Concurrent xacts may still race us and produce
	 * duplicate rows — that is intentional under the "duplicate-tolerant"
	 * model.  Update / lock paths handle multiple matching rows correctly;
	 * reads sort+dedup by chunk_number.
	 *
	 * No SUEL on user_rel here: holding a xact-scoped relation lock for
	 * the duration of bulk INSERT/COPY caused cross-segment deadlock in
	 * MPP — each segment's local lockmgr formed independent waits that
	 * combined into a cycle the global deadlock detector couldn't see.
	 *
	 * The lookup runs via systable_beginscan with the composite
	 * (table_oid, chunk_number) btree index for an O(log N) probe.
	 * Heap-seqscan fallback (indexOK=true automatically degrades when
	 * idx_relid is InvalidOid) keeps the path safe during CREATE
	 * EXTENSION before the CREATE INDEX has dispatched.  At 65K chunks
	 * the index path avoids the O(N) seqscan that previously degraded
	 * INSERT throughput ~9x.
	 */
	idx_relid = ts_chunk_ensure_idx_oid();

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));
	ScanKeyInit(&skey[1],
				Anum_ts_chunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) fork));

	hscan = systable_beginscan(rel, idx_relid,
							   true /* indexOK */,
							   SnapshotSelf, 2, skey);
	existing = systable_getnext(hscan);

	if (HeapTupleIsValid(existing))
	{
		/*
		 * Row already exists (either by us or a committed concurrent
		 * xact) — cache + skip.
		 */
		systable_endscan(hscan);
		table_close(rel, RowExclusiveLock);
		ts_chunk_cache_add(table_oid, (int32) fork);
		return;
	}
	systable_endscan(hscan);

	/* Build the tuple */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_ts_chunk_table_oid - 1] = ObjectIdGetDatum(table_oid);
	values[Anum_ts_chunk_chunk_number - 1] = Int32GetDatum((int32) fork);
	values[Anum_ts_chunk_range_start - 1] = TimestampTzGetDatum((TimestampTz) range_start_usec);
	values[Anum_ts_chunk_range_end - 1] = TimestampTzGetDatum((TimestampTz) range_end_usec);
	values[Anum_ts_chunk_creation_time - 1] = TimestampTzGetDatum(GetCurrentTimestamp());
	values[Anum_ts_chunk_status - 1] = Int16GetDatum(TS_CHUNK_ACTIVE);

	tup = heap_form_tuple(tupdesc, values, nulls);

	CatalogTupleInsert(rel, tup);

	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);

	/* Add to session cache so subsequent rows skip catalog lookup */
	ts_chunk_cache_add(table_oid, (int32) fork);
}

/*
 * ts_chunk_catalog_has_any
 *		Cheap existence probe: returns true on the first ts_chunk row
 *		seen for table_oid.  Used by DDL guards that need to gate on
 *		"any chunks created" without paying for a full list.
 */
bool
ts_chunk_catalog_has_any(Oid table_oid)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;
	bool			has_any;

	if (!ts_chunk_ensure_oid())
		return false;

	rel = table_open(ts_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);
	tup = heap_getnext(hscan, ForwardScanDirection);
	has_any = (tup != NULL);
	table_endscan(hscan);

	table_close(rel, AccessShareLock);
	return has_any;
}

/*
 * Comparator for list_sort over int-typed cells (ForkNumber stored
 * as int).  Used by ts_chunk_catalog_get_chunks.
 */
static int
chunk_list_cmp(const ListCell *a, const ListCell *b)
{
	int		va = lfirst_int(a);
	int		vb = lfirst_int(b);

	return (va > vb) - (va < vb);
}

static int
chunk_entry_cmp(const ListCell *a, const ListCell *b)
{
	const ChunkEntry   *ea = (const ChunkEntry *) lfirst(a);
	const ChunkEntry   *eb = (const ChunkEntry *) lfirst(b);

	return (ea->chunk > eb->chunk) - (ea->chunk < eb->chunk);
}

/*
 * ts_chunk_catalog_get_chunks
 *		Return every chunk number registered for table_oid in the
 *		local ts_chunk heap, sorted ascending and deduplicated
 *		(distributed tables may carry one row per segment).
 *		Returns NIL if no chunks exist or the catalog table hasn't
 *		been created yet.
 */
List *
ts_chunk_catalog_get_chunks(Oid table_oid)
{
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[1];
	HeapTuple		tup;
	List		   *forks = NIL;
	List		   *unique;
	ListCell	   *cell;
	int				prev = -1;
	bool			first = true;

	if (!ts_chunk_ensure_oid())
		return NIL;

	rel = table_open(ts_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);

	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
	{
		Datum	val;
		bool	isnull;

		val = heap_getattr(tup, Anum_ts_chunk_chunk_number,
						   RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;
		forks = lappend_int(forks, DatumGetInt32(val));
	}

	table_endscan(hscan);
	table_close(rel, AccessShareLock);

	if (forks == NIL)
		return NIL;

	/* Sort + deduplicate using PG List APIs (no qsort). */
	list_sort(forks, chunk_list_cmp);

	unique = NIL;
	foreach(cell, forks)
	{
		int	v = lfirst_int(cell);

		if (first || v != prev)
		{
			unique = lappend_int(unique, v);
			prev = v;
			first = false;
		}
	}
	list_free(forks);
	return unique;
}

/*
 * ts_chunk_catalog_get_chunks_with_status
 *		Return chunks of `table_oid` filtered to [min_chunk, max_chunk]
 *		inclusive, with their per-row status, as a TSChunkList struct.
 *		Pass TS_FIRST_CHUNKNUM for "no lower bound" and TS_MAX_CHUNK_FORKNUM
 *		for "no upper bound" — those sentinels skip emitting the
 *		corresponding ScanKey so heapam doesn't pay for an always-true
 *		comparison.
 *
 *		Caller is responsible for pfree() on chunks + statuses (only
 *		when n>0; both are NULL when n==0).
 */
TSChunkList
ts_chunk_catalog_get_chunks_with_status(Oid table_oid,
										ForkNumber min_chunk,
										ForkNumber max_chunk)
{
	TSChunkList		list = { NULL, NULL, 0 };
	Relation		rel;
	TableScanDesc	hscan;
	ScanKeyData		skey[3];
	int				nkeys;
	HeapTuple		tup;
	List		   *entries = NIL;
	ListCell	   *lc;
	int				n = 0;
	ForkNumber		prev = InvalidForkNumber;

	if (!ts_chunk_ensure_oid())
		return list;

	rel = table_open(ts_chunk_relid, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));
	nkeys = 1;

	if (min_chunk > TS_FIRST_CHUNKNUM)
	{
		ScanKeyInit(&skey[nkeys],
					Anum_ts_chunk_chunk_number,
					BTGreaterEqualStrategyNumber, F_INT4GE,
					Int32GetDatum((int32) min_chunk));
		nkeys++;
	}
	if (max_chunk < TS_MAX_CHUNK_FORKNUM)
	{
		ScanKeyInit(&skey[nkeys],
					Anum_ts_chunk_chunk_number,
					BTLessEqualStrategyNumber, F_INT4LE,
					Int32GetDatum((int32) max_chunk));
		nkeys++;
	}

	hscan = table_beginscan(rel, SnapshotSelf, nkeys, skey);

	/*
	 * Collect (chunk, status) pairs into a PG List — avoids manual
	 * geometric resize of two parallel arrays.  Sort + dedup happen
	 * after the scan completes.
	 */
	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
	{
		Datum			val;
		bool			isnull;
		ChunkEntry	   *entry;
		int32			forknum;
		int16			status;

		val = heap_getattr(tup, Anum_ts_chunk_chunk_number,
						   RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;

		forknum = DatumGetInt32(val);

		val = heap_getattr(tup, Anum_ts_chunk_status,
						   RelationGetDescr(rel), &isnull);
		status = isnull ? 0 : DatumGetInt16(val);

		entry = (ChunkEntry *) palloc(sizeof(ChunkEntry));
		entry->chunk = (ForkNumber) forknum;
		entry->status = status;
		entries = lappend(entries, entry);
	}

	table_endscan(hscan);
	table_close(rel, AccessShareLock);

	if (entries == NIL)
		return list;

	list_sort(entries, chunk_entry_cmp);

	/*
	 * Walk the sorted list once, copying into output arrays and skipping
	 * adjacent duplicates in the same pass.  Over-allocate to
	 * list_length(entries) — the post-dedup count is bounded by it and
	 * the slack is a few bytes per dup (negligible vs the alternative
	 * of a separate "count distinct" pass).
	 */
	list.chunks = (ForkNumber *) palloc(sizeof(ForkNumber) * list_length(entries));
	list.statuses = (int16 *) palloc(sizeof(int16) * list_length(entries));

	foreach(lc, entries)
	{
		ChunkEntry *entry = (ChunkEntry *) lfirst(lc);

		if (n == 0 || entry->chunk != prev)
		{
			list.chunks[n] = entry->chunk;
			list.statuses[n] = entry->status;
			prev = entry->chunk;
			n++;
		}
	}
	list.n = n;

	list_free_deep(entries);
	return list;
}

/*
 * ts_chunk_catalog_is_compressed
 *		Returns true if the given chunk has status = TS_CHUNK_COMPRESSED.
 *
 *		Uses SnapshotSelf so the INSERT path's auto-truncate flow can see
 *		its own uncommitted UPDATE done earlier in the same statement.
 *		Without this, a single INSERT visiting the same chunk twice (e.g.
 *		after the auto-truncate path resets the buffer cache, or any INSERT
 *		whose row stream alternates between chunks) re-fires the auto-
 *		truncate branch on the second visit and update_status fails with
 *		TM_SelfModified ("tuple already updated by self").
 *
 *		For the compress segment caller (do_compress_one_chunk) SnapshotSelf
 *		gives the same answer as MVCC: that callsite never has own
 *		uncommitted ts_chunk writes pending when it runs.
 */
bool
ts_chunk_catalog_is_compressed(Oid table_oid, int32 chunk_number)
{
	Relation	 rel;
	SysScanDesc	 hscan;
	ScanKeyData	 skey[2];
	HeapTuple	 tup;
	bool		 compressed = false;
	Oid			 idx_relid;

	if (!ts_chunk_ensure_oid())
		return false;

	rel = table_open(ts_chunk_relid, AccessShareLock);
	idx_relid = ts_chunk_ensure_idx_oid();

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));
	ScanKeyInit(&skey[1],
				Anum_ts_chunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(chunk_number));

	hscan = systable_beginscan(rel, idx_relid,
							   true,
							   SnapshotSelf, 2, skey);

	while ((tup = systable_getnext(hscan)) != NULL)
	{
		Datum	d_status;
		bool	isnull;

		d_status = heap_getattr(tup, Anum_ts_chunk_status,
								RelationGetDescr(rel), &isnull);
		if (!isnull && DatumGetInt16(d_status) == TS_CHUNK_COMPRESSED)
		{
			compressed = true;
			break;
		}
	}

	systable_endscan(hscan);
	table_close(rel, AccessShareLock);

	return compressed;
}

/*
 * ts_chunk_catalog_update_status
 *		Update the status column of all rows matching (table_oid,
 *		chunk_number).  Under the duplicate-tolerant model, a chunk may
 *		have multiple ts_chunk rows (one per concurrent first-time
 *		INSERTer that raced past each other's snapshots).  We update
 *		every matching row so status stays consistent regardless of
 *		which row a reader hits first.
 */
void
ts_chunk_catalog_update_status(Oid table_oid, int32 chunk_number, int16 new_status)
{
	Relation	 rel;
	SysScanDesc	 hscan;
	ScanKeyData	 skey[2];
	HeapTuple	 tup;
	Oid			 idx_relid;

	if (!ts_chunk_ensure_oid())
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("time_series.ts_chunk table not found")));

	rel = table_open(ts_chunk_relid, RowExclusiveLock);
	idx_relid = ts_chunk_ensure_idx_oid();

	ScanKeyInit(&skey[0], Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(table_oid));
	ScanKeyInit(&skey[1], Anum_ts_chunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk_number));

	/* GetTransactionSnapshot (NOT SnapshotSelf) here: this is a SCAN+UPDATE
	 * pattern (we iterate tuples and CatalogTupleUpdate each).  SnapshotSelf
	 * may return tuples whose physical ctid was HOT-pruned/freed by a
	 * concurrent backend; heap_update on those errors with "attempted to
	 * update invisible tuple".  Standard catalog updates use the txn
	 * snapshot for this reason.  Distributed-snapshot visibility for this
	 * write side is fine: we only flip status for chunks whose existence
	 * was already established via the per-chunk advisory lock in the
	 * caller (ts_chunk_catalog_lock_if_compressed uses SnapshotSelf to
	 * see the latest commit). */
	hscan = systable_beginscan(rel, idx_relid,
							   true,
							   GetTransactionSnapshot(), 2, skey);

	while ((tup = systable_getnext(hscan)) != NULL)
	{
		Datum		values[Natts_ts_chunk];
		bool		nulls[Natts_ts_chunk];
		bool		replaces[Natts_ts_chunk];
		HeapTuple	newtup;

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		replaces[Anum_ts_chunk_status - 1] = true;
		values[Anum_ts_chunk_status - 1] = Int16GetDatum(new_status);

		newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
		CatalogTupleUpdate(rel, &newtup->t_self, newtup);
		heap_freetuple(newtup);
	}

	systable_endscan(hscan);
	table_close(rel, RowExclusiveLock);
}

/*
 * ts_chunk_catalog_lock_if_compressed
 *		Atomically check that the chunk is still COMPRESSED and acquire a
 *		row-level exclusive lock on the ts_chunk tuple.  Returns true if
 *		the row was found with status = COMPRESSED and is now locked, false
 *		otherwise (row missing or status changed).  The lock is held for
 *		the remainder of the current transaction — ts_chunk_catalog_update_status
 *		from a concurrent backend will block until we commit.
 *
 *		Used by reclaim_chunk_heaps to serialise against INSERT's
 *		COMPRESSED → PARTIAL status flip before truncating the heap fork.
 */
bool
ts_chunk_catalog_lock_if_compressed(Oid table_oid, int32 chunk_number)
{
	Relation		rel;
	SysScanDesc		hscan;
	ScanKeyData		skey[2];
	HeapTuple		tup;
	HeapTupleData	canon_tup;
	bool			canon_found = false;
	int16			canon_status = 0;
	bool			locked = false;
	Oid				idx_relid;

	if (!ts_chunk_ensure_oid())
		return false;

	rel = table_open(ts_chunk_relid, RowExclusiveLock);
	idx_relid = ts_chunk_ensure_idx_oid();

	ScanKeyInit(&skey[0], Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(table_oid));
	ScanKeyInit(&skey[1], Anum_ts_chunk_chunk_number,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk_number));

	/*
	 * Under the duplicate-tolerant model, several rows may exist for the
	 * same (table_oid, chunk_number).  We pick the canonical row by the
	 * smallest ctid so every concurrent caller agrees on the same row
	 * and serialises on its tuple lock.  Without this, two callers might
	 * lock different rows and both proceed past the lock-gate.
	 *
	 * The (table_oid, chunk_number) composite index is used when
	 * available; called per-row in the INSERT hot path, so an index
	 * lookup vs heap seqscan is the difference between O(log N) and
	 * O(N) per row at 65K chunks.
	 */
	/*
	 * SnapshotSelf, NOT GetTransactionSnapshot(): under Cloudberry the
	 * distributed snapshot is fixed at command start.  reclaim acquires the
	 * per-chunk advisory ExclusiveLock (see ts_reclaim_chunk_heaps_segment)
	 * immediately before calling us and may block there while a concurrent
	 * INSERT into the same chunk commits its COMPRESSED -> PARTIAL flip.
	 * That flip is NOT visible through the (older) transaction snapshot, so
	 * a scan with GetTransactionSnapshot() reports the chunk as still
	 * COMPRESSED; reclaim then smgrtruncate()s the heap fork and silently
	 * drops the just-inserted rows.  No invalidation is emitted for that
	 * loss, so a continuous aggregate over the table is left permanently
	 * ahead of the source.  SnapshotSelf bypasses the distributed snapshot
	 * and observes the latest committed status (the chunk is PARTIAL, so we
	 * return false and reclaim skips the truncate).  ts_chunk_catalog_delete
	 * uses SnapshotSelf for exactly the same Cloudberry-visibility reason.
	 */
	hscan = systable_beginscan(rel, idx_relid,
							   true,
							   SnapshotSelf, 2, skey);
	while ((tup = systable_getnext(hscan)) != NULL)
	{
		bool	isnull;
		Datum	d = heap_getattr(tup, Anum_ts_chunk_status,
								 RelationGetDescr(rel), &isnull);

		if (!canon_found ||
			ItemPointerCompare(&tup->t_self, &canon_tup.t_self) < 0)
		{
			canon_tup.t_self = tup->t_self;
			canon_status = isnull ? -1 : DatumGetInt16(d);
			canon_found = true;
		}
	}
	systable_endscan(hscan);

	if (canon_found && canon_status == TS_CHUNK_COMPRESSED)
	{
		TM_FailureData	tmfd;
		TM_Result		res;
		Buffer			buf = InvalidBuffer;

		res = heap_lock_tuple(rel, &canon_tup,
							  GetCurrentCommandId(true),
							  LockTupleExclusive,
							  LockWaitBlock,
							  false, &buf, &tmfd);
		if (BufferIsValid(buf))
			ReleaseBuffer(buf);
		if (res == TM_Ok)
			locked = true;
	}
	/*
	 * NoLock: keep both the relation-level RowExclusiveLock and the
	 * tuple-level lock acquired above.  The immediate caller almost always
	 * follows up with ts_chunk_catalog_update_status which re-opens the
	 * relation; keeping the lock avoids a release/re-acquire cycle.  Both
	 * locks are released at xact end via the resource owner.
	 */
	table_close(rel, NoLock);
	return locked;
}

/*
 * ts_chunk_catalog_delete
 *		Remove all chunk records for the given table_oid from the local
 *		segment's ts_chunk heap.
 *
 *		Uses SnapshotSelf rather than the transaction snapshot.  Under
 *		Cloudberry, the distributed snapshot for the enclosing DROP
 *		TABLE statement is fixed at command start, so a compress that
 *		commits during DROP's wait on the user-relation lock is not
 *		visible through GetTransactionSnapshot() — the scan would
 *		return the pre-compress tuple TIDs and simple_heap_delete on
 *		those raises "tuple concurrently updated" against the new
 *		versions written by compress.  SnapshotSelf bypasses the
 *		distributed snapshot, sees the latest committed state, and
 *		hands DELETE the current TIDs.
 */
void
ts_chunk_catalog_delete(Oid table_oid)
{
	Relation	 rel;
	TableScanDesc hscan;
	ScanKeyData	 skey[1];
	HeapTuple	 tup;

	if (!ts_chunk_ensure_oid())
		return;

	/*
	 * Must run before the actual deletion below: see the comment on
	 * ts_chunk_cache_invalidate for the same-xact TRUNCATE+INSERT data
	 * loss this prevents.
	 */
	ts_chunk_cache_invalidate(table_oid);

	rel = table_open(ts_chunk_relid, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_ts_chunk_table_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(table_oid));

	hscan = table_beginscan(rel, SnapshotSelf, 1, skey);

	while ((tup = heap_getnext(hscan, ForwardScanDirection)) != NULL)
	{
		simple_heap_delete(rel, &tup->t_self);
	}

	table_endscan(hscan);
	table_close(rel, RowExclusiveLock);
}

/*
 *		PER-CHUNK ADVISORY LOCK
 *
 *	Custom mutex keyed on (table_oid, chunk_number) using PG's advisory
 *	lockmgr.  Provides per-chunk synchronisation between:
 *
 *	  INSERT     (Share)     — writes rows into the heap fork.
 *	  ChunkScan  (Share)     — reads rows from the heap fork (taken
 *	                            only on ACTIVE / PARTIAL chunks;
 *	                            COMPRESSED chunks read from the
 *	                            immutable PAX file and need no lock).
 *	  compress   (Exclusive) — writes PAX, flips status.
 *	  reclaim    (Exclusive) — smgrtruncate of the heap fork.
 *
 *	reclaim's smgrtruncate is non-transactional, so without the
 *	reader-side Share a reader whose MVCC snapshot predates compress
 *	would observe heap fork blocks vanish mid-scan (mdread past EOF
 *	or silent 0 rows when nblocks is lazily recomputed to 0).
 *
 *	Compress is also Exclusive on this lock, so long-running scans
 *	delay compress.  Accepted: maintenance ops yield to in-flight
 *	queries.
 *
 *	Tag layout:
 *	  field1 = MyDatabaseId
 *	  field2 = table_oid ^ TS_CHUNK_LOCK_MAGIC
 *	  field3 = chunk_number
 *	  field4 = 2  (two-int4 advisory key)
 *
 *	The MAGIC bit XOR-toggles bit 31 of table_oid, shifting our keyspace
 *	into a region unlikely to be hit by user-level pg_advisory_xact_lock()
 *	calls (which typically use small positive int4 values that leave bit
 *	31 clear).  XOR (rather than OR) preserves the bijection between
 *	table_oid and the tag field: two different Oids always map to two
 *	different tags.  OR would collide any Oid `a` with `a | 0x80000000`
 *	(reachable in principle once Oid allocation crosses 2^31 on a long-
 *	lived system), giving cross-relation advisory-lock aliasing that
 *	silently serialises unrelated chunks.
 *
 *	TS_CHUNK_LOCK_MAGIC is defined at the top of this file alongside the
 *	other file-level constants.
 */

void
ts_chunk_lock(Oid table_oid, int32 chunk_number, LOCKMODE mode)
{
	LOCKTAG		tag;

	SET_LOCKTAG_ADVISORY(tag,
						 MyDatabaseId,
						 (uint32) table_oid ^ TS_CHUNK_LOCK_MAGIC,
						 (uint32) chunk_number,
						 2);
	(void) LockAcquire(&tag, mode, false /* sessionLock */, false /* dontWait */);
}

/*
 * chunk_stats_dispatch_counts
 *		Run `count(DISTINCT chunk_number)` + `... FILTER (WHERE status=1)`
 *		on every segment via CdbDispatchCommand and sum the results into
 *		*n_total / *n_compressed.  Best-effort: returns false on dispatch
 *		failure (and leaves both outputs unchanged) so the caller can
 *		skip the rest of the refresh rather than aborting the xact.
 */
static bool
chunk_stats_dispatch_counts(Oid relid, int *n_total, int *n_compressed)
{
	CdbPgResults	res = {NULL, 0};
	StringInfoData	sql;
	int				i;
	MemoryContext	oldcontext;
	ResourceOwner	oldowner;
	bool			dispatch_ok = true;

	*n_total = 0;
	*n_compressed = 0;

	initStringInfo(&sql);
	appendStringInfo(&sql,
		"SELECT count(DISTINCT chunk_number)::int8, "
		"       count(DISTINCT chunk_number) FILTER (WHERE status = 1)::int8 "
		"FROM time_series.ts_chunk WHERE table_oid = %u",
		relid);

	/*
	 * Wrap the fanout in an internal subtransaction so a dispatch failure
	 * rolls back cleanly (releasing the gang, resource-owner slot, and
	 * any resources CdbDispatchCommand acquired) instead of leaving the
	 * outer xact in a half-torn-down state.  A bare PG_CATCH +
	 * FlushErrorState swallows the error text but leaves the gang
	 * leaked -- subsequent statements in the same xact then fail with
	 * "cannot execute in a transaction that has performed a dispatch".
	 */
	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	BeginInternalSubTransaction(NULL);

	PG_TRY();
	{
		CdbDispatchCommand(sql.data, DF_WITH_SNAPSHOT, &res);
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		/* Discard the error message and roll the subtransaction back. */
		MemoryContextSwitchTo(oldcontext);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		dispatch_ok = false;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	if (!dispatch_ok)
	{
		cdbdisp_clearCdbPgResults(&res);
		pfree(sql.data);
		return false;
	}
	pfree(sql.data);

	for (i = 0; i < res.numResults; i++)
	{
		PGresult   *pgres = res.pg_results[i];
		int64		v_total = 0;
		int64		v_compressed = 0;

		if (PQresultStatus(pgres) != PGRES_TUPLES_OK)
			continue;
		if (PQntuples(pgres) != 1 || PQnfields(pgres) != 2)
			continue;

		(void) scanint8(PQgetvalue(pgres, 0, 0), false, &v_total);
		(void) scanint8(PQgetvalue(pgres, 0, 1), false, &v_compressed);
		*n_total += (int) v_total;
		*n_compressed += (int) v_compressed;
	}
	cdbdisp_clearCdbPgResults(&res);
	return true;
}

/*
 * Build a DefElem("name" = "value") with the value formatted from `v`.
 */
static DefElem *
make_int_reloption(const char *name, int v)
{
	char	buf[32];

	snprintf(buf, sizeof(buf), "%d", v);
	return makeDefElem(pstrdup(name), (Node *) makeString(pstrdup(buf)), -1);
}

/*
 * chunk_stats_merge_reloptions
 *		Walk the existing reloption list, replace any pre-existing
 *		`ts_n_total_chunks` / `ts_n_compressed_chunks` entries with the
 *		new counts, append fresh entries for any that were missing, and
 *		keep all other reloptions verbatim.  Returns the new list (newly
 *		palloc'd in the current memory context).
 */
static List *
chunk_stats_merge_reloptions(List *old_opts, int n_total, int n_compressed)
{
	List	   *new_opts = NIL;
	ListCell   *lc;
	bool		saw_total = false;
	bool		saw_comp = false;

	foreach(lc, old_opts)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "ts_n_total_chunks") == 0)
		{
			new_opts = lappend(new_opts, make_int_reloption("ts_n_total_chunks", n_total));
			saw_total = true;
		}
		else if (strcmp(def->defname, "ts_n_compressed_chunks") == 0)
		{
			new_opts = lappend(new_opts,
							   make_int_reloption("ts_n_compressed_chunks", n_compressed));
			saw_comp = true;
		}
		else
		{
			new_opts = lappend(new_opts,
							   makeDefElem(pstrdup(def->defname),
										   def->arg ? (Node *) copyObject(def->arg) : NULL,
										   -1));
		}
	}
	if (!saw_total)
		new_opts = lappend(new_opts, make_int_reloption("ts_n_total_chunks", n_total));
	if (!saw_comp)
		new_opts = lappend(new_opts, make_int_reloption("ts_n_compressed_chunks", n_compressed));
	return new_opts;
}

/*
 * ts_refresh_chunk_stats
 *
 *		Dispatch a COUNT(*) over ts_chunk to all segments and aggregate
 *		into (n_total, n_compressed).  Then update the hypertable's
 *		pg_class.reloptions via CatalogTupleUpdate so the values are
 *		visible to QD planner via the relcache.
 *
 *		Called from compress_chunks tail and from the ANALYZE tableam
 *		callback.  Both are QD-side entry points.  We only hold
 *		RowExclusiveLock on pg_class; the target relation's existing
 *		lock (AccessShare from compress_chunks etc.) is undisturbed.
 *
 *		Implementation is split across two small helpers immediately
 *		below: chunk_stats_dispatch_counts() runs the segment fanout
 *		and sums into (n_total, n_compressed); chunk_stats_merge_reloptions()
 *		rewrites the reloption list with the new counts.  See bottom
 *		of file for ts_refresh_chunk_stats() itself.
 */
void
ts_refresh_chunk_stats(Oid relid)
{
	int				n_total = 0;
	int				n_compressed = 0;
	Relation		classRel;
	HeapTuple		oldTup;
	HeapTuple		newTup;
	Datum			cur_opts;
	bool			isnull = true;
	List		   *old_opts = NIL;
	List		   *new_opts;
	Datum			repl_val[Natts_pg_class];
	bool			repl_null[Natts_pg_class];
	bool			repl_repl[Natts_pg_class];
	Datum			new_reloptions;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	/* Dispatch count(*) to segments — bail quietly on dispatch failure. */
	if (!chunk_stats_dispatch_counts(relid, &n_total, &n_compressed))
		return;

	/* Rewrite pg_class.reloptions in place */
	classRel = table_open(RelationRelationId, RowExclusiveLock);
	oldTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(oldTup))
	{
		table_close(classRel, RowExclusiveLock);
		return;
	}

	cur_opts = SysCacheGetAttr(RELOID, oldTup, Anum_pg_class_reloptions, &isnull);
	if (!isnull)
		old_opts = untransformRelOptions(cur_opts);
	new_opts = chunk_stats_merge_reloptions(old_opts, n_total, n_compressed);

	new_reloptions = transformRelOptions((Datum) 0, new_opts, NULL, NULL, false, false);

	memset(repl_repl, false, sizeof(repl_repl));
	repl_repl[Anum_pg_class_reloptions - 1] = true;
	if (PointerIsValid(DatumGetPointer(new_reloptions)))
	{
		repl_val[Anum_pg_class_reloptions - 1] = new_reloptions;
		repl_null[Anum_pg_class_reloptions - 1] = false;
	}
	else
		repl_null[Anum_pg_class_reloptions - 1] = true;

	newTup = heap_modify_tuple(oldTup, RelationGetDescr(classRel),
							   repl_val, repl_null, repl_repl);

	/*
	 * Reloptions can grow when we add our two int values for the first time,
	 * so heap_inplace_update (same-length replacement) won't do.
	 * CatalogTupleUpdate is the standard pattern for pg_class row mutations
	 * from extension code (ANALYZE itself uses this via vac_update_relstats).
	 * We only hold RowExclusiveLock on pg_class, never escalating the lock
	 * on the target relation — compress_chunks's existing AccessShareLock
	 * stays undisturbed.
	 */
	CatalogTupleUpdate(classRel, &newTup->t_self, newTup);

	heap_freetuple(newTup);
	heap_freetuple(oldTup);
	table_close(classRel, RowExclusiveLock);

	CommandCounterIncrement();

	/* Make sure other backends + the local relcache see the new values. */
	CacheInvalidateRelcacheByRelid(relid);
}
