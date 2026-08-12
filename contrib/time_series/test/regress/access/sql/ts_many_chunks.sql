-- ts_many_chunks.sql — multi-chunk smoke test (~200 chunks, 100K rows)
--
-- Verifies the read/write pipeline at scale.  After deleting the index
-- AM and refactoring ts_chunk_scan_exec, ChunkScan is the only read
-- path; this fixture exercises it across 200 chunks for every state
-- (ACTIVE / COMPRESSED / reclaimed-COMPRESSED / PARTIAL).
--
-- Covers in one run:
--   - INSERT spreading rows across 200 chunks
--   - ChunkScan range scan, point lookup, aggregate across many chunks
--   - Batch compress_chunks (200 chunks at once)
--   - Batch reclaim_chunk_heaps (200 chunks at once)
--   - INSERT into 5 already-reclaimed chunks → status flips to PARTIAL
--   - Mixed PAX + heap read on PARTIAL chunks
--   - test.ts_check_consistency at every state transition
--
-- Targets installcheck (< 30 sec total runtime on a 6-segment gpdemo).

\i sql/include/setup.sql
\i sql/include/consistency.sql

-- ======================================================================
-- Setup: 200 chunks × 500 rows = 100K rows
-- ======================================================================

CREATE TABLE many_t (ts timestamptz, k int, v text) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 hour',
      ts_chunk_origin     = '2025-01-01')
DISTRIBUTED BY (k);

INSERT INTO many_t
SELECT '2025-01-01'::timestamptz + (h || ' hours')::interval,
       h * 1000 + i,
       'v' || (h * 1000 + i)
FROM generate_series(0, 199) h, generate_series(0, 499) i;

SELECT count(*) AS total_rows FROM many_t;
SELECT count(DISTINCT chunk_number) AS distinct_chunks
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass;
SELECT test.ts_check_consistency('many_t'::regclass) AS state_active;

-- ======================================================================
-- Reads across many chunks
-- ======================================================================

-- Range scan: hours 24..95 (3 days starting from 2025-01-02), 72 chunks
SELECT count(*) AS range_rows FROM many_t
WHERE ts >= '2025-01-02 00:00+00' AND ts < '2025-01-05 00:00+00';

-- Point lookup: single chunk, 500 rows
SELECT count(*) AS point_rows FROM many_t
WHERE ts = '2025-01-01 00:00+00';

-- Aggregate over all 200 chunks
SELECT min(ts) AS min_ts, max(ts) AS max_ts FROM many_t;

-- ======================================================================
-- Phase 1: batch compress all chunks → status = COMPRESSED (1)
-- ======================================================================

SELECT time_series.set_compress_config('many_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('many_t'::regclass) > 0 AS phase1_compressed;

SELECT count(DISTINCT chunk_number) AS chunks_compressed
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass AND status = 1;
SELECT count(DISTINCT chunk_number) AS chunks_still_active
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass AND status = 0;

SELECT count(*) AS rows_after_compress FROM many_t;
SELECT count(*) AS range_rows_via_pax FROM many_t
WHERE ts >= '2025-01-02 00:00+00' AND ts < '2025-01-05 00:00+00';
SELECT test.ts_check_consistency('many_t'::regclass) AS state_compressed;

-- ======================================================================
-- Phase 2: batch reclaim heap forks
-- ======================================================================

SELECT time_series.reclaim_chunk_heaps('many_t'::regclass) > 0 AS phase2_reclaimed;

SELECT count(DISTINCT chunk_number) AS still_compressed_after_reclaim
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass AND status = 1;
SELECT count(*) AS rows_after_reclaim FROM many_t;
SELECT test.ts_check_consistency('many_t'::regclass) AS state_reclaimed;

-- ======================================================================
-- Phase 3: INSERT into 5 already-reclaimed chunks → status flips to PARTIAL
-- ======================================================================

INSERT INTO many_t
SELECT '2025-01-01'::timestamptz + (h || ' hours')::interval + interval '30 min',
       1000000 + h,
       'late'
FROM generate_series(50, 54) h;

-- Topology-agnostic checks below.  Per-segment ts_chunk rows can diverge:
-- a late INSERT only updates the row on the segment that received the
-- routed tuple, so chunk N may appear with status=1 (COMPRESSED) on
-- some segments and status=2 (PARTIAL) on others.  The DISTINCT counts
-- therefore use "any segment with status=2" semantics, and the union
-- across both statuses always covers the full chunk set.

-- 5 chunks touched by the late INSERT
SELECT count(DISTINCT chunk_number) AS chunks_with_partial_row
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass AND status = 2;

-- Union of COMPRESSED-or-PARTIAL = all 200 chunks (no segment-count dependence)
SELECT count(DISTINCT chunk_number) AS chunks_in_compressed_or_partial
FROM time_series.ts_chunk WHERE table_oid = 'many_t'::regclass AND status IN (1, 2);

-- Row-level invariants (purely data, no catalog-shape dependence)
SELECT count(*) AS rows_after_late_insert FROM many_t;
SELECT count(*) AS late_rows_visible FROM many_t WHERE k >= 1000000;
SELECT test.ts_check_consistency('many_t'::regclass) AS state_partial;

-- ======================================================================
-- Cleanup (DROP TABLE does not cascade to compress catalogs)
-- ======================================================================

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid = 'many_t'::regclass;
DELETE FROM time_series.ts_compress_config WHERE table_oid = 'many_t'::regclass;
DROP TABLE many_t;

-- ======================================================================
-- Phase 4: chunk forknum boundary
--   Validates that:
--     (a) a single INSERT routing to chunk_idx near TS_MAX_CHUNK_FORKNUM
--         (=UINT16_MAX) succeeds — exercises the sidecar's lazy-grow path
--         to a one-shot ~1MB allocation per SMgrRelation.
--     (b) chunk_idx that overflows forknum > UINT16_MAX is rejected with
--         a clean ERROR (no PANIC, no segfault).
--     (c) the relation is still usable after the failed INSERT, and
--         DROP TABLE walks the sparse sidecar [min..max] range without
--         crashing on the unpopulated slots in between.
--
--   Origin is pinned with "+00" to make raw chunk_idx values match the
--   ts_calculate_chunk math regardless of the session timezone the test
--   happens to run under.  TS_FIRST_CHUNKNUM = MAX_FORKNUM+1 = 4, so the
--   allowed raw chunk_idx ceiling is UINT16_MAX - 4 = 65531.
-- ======================================================================

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

CREATE TABLE chunk_limit_t (ts timestamp, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;

-- Sparse insert at chunk_idx 30000 succeeds; sidecar repalloc grows to
-- ~30005 slots in one step (not power-of-2 doubled).
INSERT INTO chunk_limit_t
  VALUES ('2025-01-01 00:00:00'::timestamp + interval '30000 minutes', 1);

-- chunk_idx = 65531 → forknum = TS_MAX_CHUNK_FORKNUM = UINT16_MAX, last
-- legal slot.  Sidecar allocates BlockNumber/int/MdfdVec* arrays of
-- ~65536 entries (~1 MB) on a single ensure_capacity call.
INSERT INTO chunk_limit_t
  VALUES ('2025-01-01 00:00:00'::timestamp + interval '65531 minutes', 2);

-- chunk_idx = 65532 → forknum = UINT16_MAX + 1; must error gracefully.
INSERT INTO chunk_limit_t
  VALUES ('2025-01-01 00:00:00'::timestamp + interval '65532 minutes', 3);

-- Relation is still queryable after the failed INSERT.
SELECT count(*) AS rows_after_limit_error FROM chunk_limit_t;
SELECT min(k) AS min_k, max(k) AS max_k FROM chunk_limit_t;

-- DROP walks the sparse sidecar range [forknum 30004 .. 65535], hitting
-- only 2 populated slots and NULL guards for the rest.  Must not crash.
DROP TABLE chunk_limit_t;
