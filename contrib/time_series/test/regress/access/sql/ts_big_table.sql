-- Test: large table stress test
-- NOT part of regular installcheck — run via: make bigcheck
--
-- Verifies that time_series works correctly when a single chunk
-- exceeds the 1GB RELSEG_SIZE file split boundary.
-- Tables are NOT dropped at the end for post-test inspection.

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: Create wide table for large chunk test
-- ======================================================================

CREATE TABLE ts_big (
    ts          timestamptz NOT NULL,
    region      text        NOT NULL,
    device_id   integer     NOT NULL,
    val1        double precision,
    val2        double precision,
    val3        double precision,
    payload     text
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2025-01-01 00:00:00+00'
) DISTRIBUTED BY (device_id);

-- ======================================================================
-- Section 2: Insert data in batches to exceed 1GB per segment
-- Each row ~200 bytes, need ~5M rows per segment to hit 1GB.
-- With 3 segments, insert ~18M rows total.
-- ======================================================================

\echo === batch 1: 6M rows ===
INSERT INTO ts_big
SELECT
    '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 millisecond'),
    'region_' || (i % 10),
    i % 100,
    random() * 100,
    random() * 100,
    random() * 100,
    'payload_data_' || (i % 1000)
FROM generate_series(1, 6000000) i;

\echo === batch 2: 6M rows ===
INSERT INTO ts_big
SELECT
    '2025-01-01 00:00:00+00'::timestamptz + ((6000000 + i) * interval '1 millisecond'),
    'region_' || (i % 10),
    i % 100,
    random() * 100,
    random() * 100,
    random() * 100,
    'payload_data_' || (i % 1000)
FROM generate_series(1, 6000000) i;

\echo === batch 3: 6M rows ===
INSERT INTO ts_big
SELECT
    '2025-01-01 00:00:00+00'::timestamptz + ((12000000 + i) * interval '1 millisecond'),
    'region_' || (i % 10),
    i % 100,
    random() * 100,
    random() * 100,
    random() * 100,
    'payload_data_' || (i % 1000)
FROM generate_series(1, 6000000) i;

-- ======================================================================
-- Section 3: Verify row count
-- ======================================================================

SELECT count(*) AS total_rows FROM ts_big;

-- ======================================================================
-- Section 4: Verify chunk info (single chunk, all data in one day)
-- ======================================================================

SELECT DISTINCT chunk_number, range_start::date, range_end::date
FROM time_series.ts_chunk_info('ts_big'::regclass)
ORDER BY chunk_number;

-- Per-segment block count
SELECT segment_id, chunk_number, nblocks,
       pg_size_pretty(nblocks * 8192::bigint) AS chunk_size
FROM time_series.ts_chunk_info('ts_big'::regclass)
ORDER BY segment_id, chunk_number;

-- ======================================================================
-- Section 5: Verify data spans across RELSEG_SIZE boundary
-- Query data from early part (in first segment file) and
-- late part (in second segment file .1)
-- ======================================================================

SELECT count(*) AS early_rows
FROM ts_big
WHERE ts < '2025-01-01 02:00:00+00';

SELECT count(*) AS late_rows
FROM ts_big
WHERE ts > '2025-01-01 04:00:00+00';

-- Total should match
SELECT count(*) AS cross_boundary_total FROM ts_big;

-- ======================================================================
-- Section 6: Aggregation across file boundary
-- ======================================================================

SELECT region, count(*), round(avg(val1)::numeric, 0) AS avg_val1
FROM ts_big
GROUP BY region
ORDER BY region;

-- ======================================================================
-- Section 7: Time-pruned scan (should still work with large chunk)
-- ======================================================================

EXPLAIN (COSTS OFF) SELECT count(*) FROM ts_big
WHERE ts >= '2025-01-01 02:00:00+00' AND ts < '2025-01-01 03:00:00+00';

SELECT count(*) AS pruned_count FROM ts_big
WHERE ts >= '2025-01-01 02:00:00+00' AND ts < '2025-01-01 03:00:00+00';

-- ======================================================================
-- Section 8: Table size (should show > 1GB total)
-- ======================================================================

SELECT pg_size_pretty(pg_relation_size('ts_big')) AS table_size;

-- ======================================================================
-- Section 9: ts_btree index with large data (B-tree split regression test)
-- Verifies that internal node splits work correctly when the B-tree
-- exceeds 2 levels (~1M+ index entries per chunk fork).
-- Previously this corrupted the tree structure (fddc54a fix).
--
-- Index AM (ts_btree) is disabled in this build; section parked here
-- for re-enable.  Uncomment together with re-enabling the AM:
-- ======================================================================

-- CREATE TABLE ts_btree_big (
--     ts timestamptz NOT NULL,
--     val integer
-- ) USING time_series WITH (
--     ts_partition_column = 'ts',
--     ts_chunk_interval = '1 day',
--     ts_chunk_origin = '2025-01-01 00:00:00+00'
-- ) DISTRIBUTED BY (val);
--
-- CREATE INDEX ts_btree_big_idx ON ts_btree_big USING ts_btree (ts);
--
-- -- Same session, multiple batches to trigger multi-level B-tree splits
-- INSERT INTO ts_btree_big
-- SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 millisecond'), i
-- FROM generate_series(1, 5000000) i;
--
-- INSERT INTO ts_btree_big
-- SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 millisecond'), i
-- FROM generate_series(1, 5000000) i;
--
-- INSERT INTO ts_btree_big
-- SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 millisecond'), i
-- FROM generate_series(1, 5000000) i;
--
-- -- All 15M rows should be accessible
-- SELECT count(*) AS btree_big_count FROM ts_btree_big;
--
-- -- Index scan should work correctly
-- SET enable_seqscan = off;
-- SELECT count(*) AS btree_idx_scan FROM ts_btree_big
-- WHERE ts >= '2025-01-01 01:00:00+00' AND ts < '2025-01-01 02:00:00+00';
-- RESET enable_seqscan;
--
-- DROP TABLE ts_btree_big;

-- ======================================================================
-- Cleanup
-- ======================================================================

DROP TABLE ts_big;

\echo === big table test complete ===

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
