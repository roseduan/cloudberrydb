-- ============================================================
-- compress_insert_visibility_loss.sql (isolation2)
--
-- Regression spec for the compress-vs-INSERT DATA-LOSS race.
--
-- do_compress_one_chunk scans the chunk's heap fork and writes every
-- VISIBLE tuple into the PAX file, then flips the chunk to COMPRESSED;
-- compress_and_reclaim later truncates the heap fork.  The heap scan
-- filtered tuples with HeapTupleSatisfiesVisibility() against the
-- snapshot returned by GetTransactionSnapshot().
--
-- The bug: compress takes the per-chunk advisory ExclusiveLock before the
-- scan, so an in-flight INSERT into the chunk must commit before compress
-- proceeds -- but under Cloudberry the dispatched command's distributed
-- snapshot is fixed BEFORE compress blocks on that lock.  A row that
-- commits while compress is waiting for the lock is therefore invisible to
-- the (stale) transaction snapshot; the visibility check skips it, it is
-- never written to PAX, and the heap-fork reclaim then truncates it away.
-- The committed row is silently lost -- and because compression emits no
-- invalidation, a continuous aggregate over the table is left permanently
-- ahead of the source.
--
-- Fix: scan_heap_to_pax_{sorted,unsorted} scan with SnapshotSelf, which
-- consults clog directly and sees every committed row (under the
-- ExclusiveLock that is exactly the set to compress).  Same SnapshotSelf
-- rationale already used by ts_chunk_catalog_is_compressed /
-- ts_chunk_catalog_delete for Cloudberry distributed-snapshot visibility.
--
-- Deterministic interleaving (no fault injection needed -- the per-chunk
-- advisory lock orders the participants):
--   (1) one ACTIVE chunk with 200 rows.
--   (2) S2: BEGIN; INSERT a sentinel row -> takes the advisory ShareLock,
--       writes the sentinel into the heap fork (uncommitted).
--   (3) S3: compress_chunks -> its snapshot is fixed now (does NOT see the
--       sentinel), then it blocks on the advisory ExclusiveLock held by S2.
--   (4) S2: COMMIT -> the sentinel is now committed.
--   (5) S3 unblocks and scans the heap.  Buggy: the stale snapshot skips
--       the sentinel, so it is not written to PAX.  Fixed: SnapshotSelf
--       sees it and writes it.
--   (6) reclaim truncates the heap fork.
-- Assertion: the sentinel survives -- total = 201, sentinel = 1.  Before
-- the fix: total = 200, sentinel = 0 (the COMPRESSED chunk serves only PAX,
-- which lost the row).
-- ============================================================

1: SET optimizer = off;
1: SET max_parallel_workers_per_gather = 0;
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: SET search_path TO public, time_series;

-- Single 1 h chunk; origin well before the data so tz interpretation can
-- never push origin past it.  DISTRIBUTED BY (device) with a single device
-- value pins every row to one segment, so the race runs on one QE.
1: CREATE TABLE vis_tbl (ts TIMESTAMPTZ NOT NULL, device INT NOT NULL, val FLOAT8)
   USING time_series WITH (
     ts_partition_column = 'ts',
     ts_chunk_interval   = '1 hour',
     ts_chunk_origin     = '2020-01-01'
   )
   DISTRIBUTED BY (device);
1: SELECT set_compress_config('vis_tbl', 'device', 'ts');

1: INSERT INTO vis_tbl
   SELECT '2024-01-01'::timestamptz + (i * interval '15 sec'), 1, i * 0.5
   FROM generate_series(1, 200) i;
1: SELECT count(*) AS total_before FROM vis_tbl;
-- Chunk is ACTIVE (status 0): nothing compressed yet.
1: SELECT chunk_number, status FROM time_series.ts_chunk
   WHERE table_oid = 'vis_tbl'::regclass
   GROUP BY chunk_number, status ORDER BY chunk_number;

-- ============================================================
-- S2: INSERT a sentinel row into the ACTIVE chunk and hold the
-- transaction open -- takes the per-chunk advisory ShareLock and writes
-- the sentinel into the heap fork (uncommitted).
-- ============================================================
2: SET optimizer = off;
2: SET search_path TO public, time_series;
2: BEGIN;
2: INSERT INTO vis_tbl VALUES ('2024-01-01 00:10:07'::timestamptz, 1, 999999.0);

-- ============================================================
-- S3: compress.  Its (distributed) snapshot is fixed now -- S2 is
-- uncommitted, so the snapshot does NOT include the sentinel -- and it then
-- blocks on the advisory ExclusiveLock that S2 holds in Share mode.
-- ============================================================
3: SET optimizer = off;
3: SET search_path TO public, time_series;
3&: SELECT time_series.compress_chunks('vis_tbl');

-- ============================================================
-- S2 COMMIT: releases the ShareLock; the sentinel is now committed.  S3
-- unblocks and scans the heap fork to build the PAX file.
-- ============================================================
2: COMMIT;
3<:

-- Reclaim the heap fork (the txn2 half of compress_and_reclaim).  After
-- this the chunk's rows live only in PAX.
1: SELECT time_series.reclaim_chunk_heaps('vis_tbl');

-- ASSERTION: the sentinel row must survive.  Before the fix compress's
-- stale snapshot skipped it, so it never reached PAX and the heap reclaim
-- truncated it: total = 200, sentinel = 0.  After the fix: total = 201,
-- sentinel = 1.
1: SELECT count(*) AS total_after FROM vis_tbl;
1: SELECT count(*) AS sentinel_rows FROM vis_tbl WHERE val = 999999.0;
1: SELECT chunk_number, status FROM time_series.ts_chunk
   WHERE table_oid = 'vis_tbl'::regclass
   GROUP BY chunk_number, status ORDER BY chunk_number;

-- Cleanup
1: DROP TABLE vis_tbl;

-- Stop the BGW scheduler before DROP EXTENSION to avoid a catalog-lock
-- ABBA against check_for_stopped_and_timed_out_jobs()'s BGWH_STOPPED
-- path (unbounded lock wait; observed as "deadlock detected").
-- Consistent with every other iso2 test that ends in DROP EXTENSION --
-- see compress_insert_no_deadlock.sql.
1: SELECT time_series.stop_background_workers();

1: DROP EXTENSION time_series CASCADE;
1q:
2q:
3q:
