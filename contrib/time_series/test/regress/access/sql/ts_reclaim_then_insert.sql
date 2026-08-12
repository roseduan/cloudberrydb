-- ts_reclaim_then_insert.sql
--
-- Regression covering the chunk-state transitions around heap reclaim:
--   ACTIVE  → compress      → COMPRESSED
--   COMPRESSED → reclaim    → COMPRESSED (heap fork truncated)
--   COMPRESSED → INSERT     → PARTIAL    (heap fork extended again)
--   PARTIAL    → ROLLBACK   → COMPRESSED (status reverts with the xact)
--   PARTIAL    → compress   → COMPRESSED (re-compression flushes new rows)
--
-- The non-obvious case is "INSERT into a chunk whose heap fork was just
-- truncated to 0 blocks by reclaim_chunk_heaps".  Storage code that
-- assumes "fork file exists ⇒ has live data" or that caches block counts
-- across the truncate boundary tends to break here.
--
-- Each transition is followed by ts_check_consistency() to catch silent
-- catalog drift.

\i sql/include/setup.sql
\i sql/include/consistency.sql

-- ======================================================================
-- Setup: one table, two chunks (200 rows total, evenly split)
-- ======================================================================

CREATE TABLE ts_rri (ts timestamptz, k int, v text) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 day',
      ts_chunk_origin     = '2025-01-01')
DISTRIBUTED BY (k);

INSERT INTO ts_rri
SELECT '2025-03-01 12:00+00'::timestamptz + (i || ' seconds')::interval,
       i, 'v' || i
FROM generate_series(1, 100) i;

INSERT INTO ts_rri
SELECT '2025-03-02 12:00+00'::timestamptz + (i || ' seconds')::interval,
       100 + i, 'v' || (100 + i)
FROM generate_series(1, 100) i;

SELECT count(*) AS rows_after_insert FROM ts_rri;
SELECT count(DISTINCT chunk_number) AS chunks_after_insert
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_active;

-- ======================================================================
-- Phase 1: compress_chunks → status = COMPRESSED (1)
-- ======================================================================

SELECT time_series.set_compress_config('ts_rri'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('ts_rri'::regclass) > 0 AS phase1_compressed;

SELECT count(DISTINCT chunk_number) AS compressed_chunks
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 1;  -- COMPRESSED

SELECT count(*) AS rows_after_compress FROM ts_rri;
SELECT count(*) AS pax_only_first_chunk FROM ts_rri WHERE k <= 100;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_compressed;

-- ======================================================================
-- Phase 2: reclaim_chunk_heaps → heap forks truncated, status stays COMPRESSED
-- ======================================================================

SELECT time_series.reclaim_chunk_heaps('ts_rri'::regclass) > 0 AS phase2_reclaimed;

SELECT count(DISTINCT chunk_number) AS compressed_after_reclaim
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 1;  -- COMPRESSED

SELECT count(*) AS rows_after_reclaim FROM ts_rri;
SELECT count(*) AS pax_only_after_reclaim FROM ts_rri WHERE k <= 100;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_reclaimed;

-- ======================================================================
-- Phase 3: INSERT into an already-reclaimed chunk → status flips to PARTIAL (2)
--
-- The 2025-03-01 chunk's heap fork was truncated to 0 blocks in Phase 2.
-- This INSERT extends it again — the storage path must handle
-- "extend a truncated fork file" without assuming the file is fresh.
-- ======================================================================

INSERT INTO ts_rri VALUES
    ('2025-03-01 18:00+00', 1001, 'late1'),
    ('2025-03-01 19:00+00', 1002, 'late2');

-- status code 2 = PARTIAL
SELECT count(DISTINCT chunk_number) AS partial_chunks
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 2;
-- status code 1 = COMPRESSED
SELECT count(DISTINCT chunk_number) AS still_compressed
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 1;

SELECT count(*) AS rows_after_late_insert FROM ts_rri;
SELECT count(*) AS late_rows_visible FROM ts_rri WHERE k >= 1001;
SELECT count(*) AS pax_rows_in_partial FROM ts_rri WHERE k <= 100;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_partial;

-- ======================================================================
-- Phase 4: ROLLBACK semantics — INSERT inside a transaction, then rollback
--
-- The status flip (COMPRESSED → PARTIAL) and the new heap rows must both
-- revert with the transaction.
-- ======================================================================

BEGIN;
INSERT INTO ts_rri VALUES
    ('2025-03-02 18:00+00', 2001, 'rollback1'),
    ('2025-03-02 19:00+00', 2002, 'rollback2');

-- Inside txn: the new rows are visible
SELECT count(*) AS rows_inside_txn FROM ts_rri;

ROLLBACK;

-- After rollback: gone
SELECT count(*) AS rows_after_rollback FROM ts_rri;
SELECT count(*) AS late_rows_2002_after_rollback FROM ts_rri WHERE k >= 2001;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_after_rollback;

-- ======================================================================
-- Phase 5: Re-compress the PARTIAL chunk → back to COMPRESSED, no row loss
-- ======================================================================

SELECT time_series.compress_chunk(
    'ts_rri'::regclass,
    (SELECT chunk_number FROM time_series.ts_chunk
     WHERE table_oid = 'ts_rri'::regclass AND status = 2  -- PARTIAL
     ORDER BY chunk_number LIMIT 1),
    if_not_compressed => false) > 0 AS phase5_recompressed;

-- status code 2 = PARTIAL
SELECT count(DISTINCT chunk_number) AS partial_after_recompress
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 2;
-- status code 1 = COMPRESSED
SELECT count(DISTINCT chunk_number) AS compressed_after_recompress
FROM time_series.ts_chunk WHERE table_oid = 'ts_rri'::regclass AND status = 1;

SELECT count(*) AS rows_after_recompress FROM ts_rri;
SELECT count(*) AS recompressed_first_chunk FROM ts_rri WHERE k <= 100 OR k >= 1001;
SELECT test.ts_check_consistency('ts_rri'::regclass) AS state_after_recompress;

-- ======================================================================
-- Cleanup (DROP TABLE does not cascade to compress catalogs)
-- ======================================================================

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid = 'ts_rri'::regclass;
DELETE FROM time_series.ts_compress_config WHERE table_oid = 'ts_rri'::regclass;
DROP TABLE ts_rri;
