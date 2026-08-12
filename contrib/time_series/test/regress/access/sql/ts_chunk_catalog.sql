-- Test: ts_chunk catalog table
-- Verifies that the ts_chunk table is maintained correctly during
-- INSERT and DROP, and that scan paths use the catalog.

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: ts_chunk table exists after extension creation
-- ======================================================================

-- 1a: Snapshot the pre-existing ts_chunk row count from earlier
--     tests in the schedule.  The three "count(*) FROM ts_chunk"
--     assertions in this file all report deltas against this baseline
--     instead of absolute totals — otherwise leakage from upstream
--     tests (ts_compress*, ts_big_table, ...) shifts the expected
--     numbers every time their fixtures evolve.
SELECT count(*) AS baseline_chunks FROM time_series.ts_chunk \gset

-- 1a: This test has not created any time_series tables yet, so its
--     contribution to ts_chunk should be exactly 0 above baseline.
SELECT count(*) - :baseline_chunks AS chunk_rows_added FROM time_series.ts_chunk;

-- 1b: Verify table structure (all 5 columns)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'time_series' AND table_name = 'ts_chunk'
ORDER BY ordinal_position;

-- 1c: Verify primary key constraint exists
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'time_series.ts_chunk'::regclass
ORDER BY conname;

-- 1d: Verify indexes on ts_chunk exist
SELECT indexname FROM pg_indexes
WHERE schemaname = 'time_series' AND tablename = 'ts_chunk'
ORDER BY indexname;

-- ======================================================================
-- Section 2: Basic catalog population during INSERT
-- ======================================================================

-- Use DISTRIBUTED REPLICATED so all rows go to all segments,
-- making catalog entry counts deterministic for testing.
CREATE TABLE ts_cat_daily (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- 2a: No chunks before any INSERT
SELECT count(*) AS chunks_before
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass;

-- 2b: Insert data spanning 3 days
INSERT INTO ts_cat_daily VALUES
    ('2025-01-01 12:00:00+00', 1),
    ('2025-01-02 12:00:00+00', 2),
    ('2025-01-03 12:00:00+00', 3);

-- 2c: 3 distinct chunks created
SELECT count(DISTINCT chunk_number) AS unique_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass;

-- 2d: Verify chunk numbers and time ranges are correct
SELECT DISTINCT chunk_number, range_start, range_end
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass
ORDER BY chunk_number;

-- 2e: creation_time is populated (not null) for all entries
SELECT count(*) AS has_creation_time
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass
AND creation_time IS NOT NULL;

-- ======================================================================
-- Section 3: Duplicate INSERT — same chunk, no new catalog entry
-- ======================================================================

-- 3a: Insert another row into day 1 (same chunk)
INSERT INTO ts_cat_daily VALUES ('2025-01-01 18:00:00+00', 4);

-- 3b: Still only 3 distinct chunks
SELECT count(DISTINCT chunk_number) AS unique_chunks_after
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass;

-- 3c: 4 rows in the table
SELECT count(*) AS total_rows FROM ts_cat_daily;

-- ======================================================================
-- Section 4: Chunk boundary conditions
-- ======================================================================

-- 4a: Timestamp exactly at interval boundary (midnight = start of new chunk)
INSERT INTO ts_cat_daily VALUES ('2025-01-04 00:00:00+00', 10);

-- 4b: 1 microsecond before boundary (still in previous chunk)
INSERT INTO ts_cat_daily VALUES ('2025-01-04 23:59:59.999999+00', 11);

-- 4c: 1 microsecond after next boundary (new chunk)
INSERT INTO ts_cat_daily VALUES ('2025-01-05 00:00:00.000001+00', 12);

-- 4d: Verify chunk count — should be 5 now (days 1,2,3,4,5)
SELECT count(DISTINCT chunk_number) AS chunks_after_boundary
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass;

-- 4e: Verify boundary rows landed in correct chunks
-- Day 4: chunk 7 (4 + 3 = chunk for day 4: origin + 3 days)
-- Day 5: chunk 8
SELECT DISTINCT chunk_number, range_start, range_end
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_daily'::regclass
AND chunk_number >= 7
ORDER BY chunk_number;

-- 4f: Scan for day 4 should return both rows (midnight + 1µs-before-midnight)
SELECT val FROM ts_cat_daily
WHERE ts >= '2025-01-04 00:00:00+00' AND ts < '2025-01-05 00:00:00+00'
ORDER BY val;

-- ======================================================================
-- Section 5: Empty table and no-match queries
-- ======================================================================

CREATE TABLE ts_cat_empty (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 hour',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- 5a: SELECT from empty table returns no rows
SELECT count(*) AS empty_count FROM ts_cat_empty;

-- 5b: No catalog entries for empty table
SELECT count(*) AS empty_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_empty'::regclass;

-- 5c: Pruned scan on empty table
SELECT val FROM ts_cat_empty
WHERE ts >= '2025-01-01 00:00:00+00' AND ts < '2025-01-02 00:00:00+00';

DROP TABLE ts_cat_empty;

-- ======================================================================
-- Section 6: Query for time range with no data (no matching chunks)
-- ======================================================================

-- 6a: Query for a month with no data at all
SELECT count(*) AS no_data
FROM ts_cat_daily
WHERE ts >= '2025-06-01 00:00:00+00' AND ts < '2025-07-01 00:00:00+00';

-- 6b: Query for time before any data
SELECT count(*) AS before_data
FROM ts_cat_daily
WHERE ts < '2024-01-01 00:00:00+00';

-- ======================================================================
-- Section 7: Open-ended range queries
-- ======================================================================

-- 7a: Lower bound only — everything from day 3 onward
SELECT val FROM ts_cat_daily
WHERE ts >= '2025-01-03 00:00:00+00'
ORDER BY val;

-- 7b: Upper bound only — everything before day 3
SELECT val FROM ts_cat_daily
WHERE ts < '2025-01-03 00:00:00+00'
ORDER BY val;

-- ======================================================================
-- Section 8: ts_chunk_info with EXECUTE ON ALL SEGMENTS
-- ======================================================================

-- 8a: Per-segment chunk info (REPLICATED table: all segments identical)
SELECT segment_id, chunk_number, nblocks, range_start, range_end
FROM time_series.ts_chunk_info('ts_cat_daily'::regclass)
ORDER BY segment_id, chunk_number;

-- 8b: Summary: distinct chunks across cluster
SELECT DISTINCT chunk_number, range_start, range_end
FROM time_series.ts_chunk_info('ts_cat_daily'::regclass)
ORDER BY chunk_number;

-- ======================================================================
-- Section 9: ChunkScan EXPLAIN output
-- ======================================================================

-- 8a: Full scan
EXPLAIN (COSTS OFF) SELECT * FROM ts_cat_daily;

-- 8b: Pruned scan
EXPLAIN (COSTS OFF) SELECT * FROM ts_cat_daily
WHERE ts >= '2025-01-02 00:00:00+00' AND ts < '2025-01-03 00:00:00+00';

-- 8c: Non-time predicate still uses ChunkScan
EXPLAIN (COSTS OFF) SELECT * FROM ts_cat_daily WHERE val = 1;

-- ======================================================================
-- Section 9: Bulk INSERT...SELECT generates multiple chunks
-- ======================================================================

CREATE TABLE ts_cat_bulk (
    ts timestamptz NOT NULL,
    seq integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- Generate 100 rows spanning ~4 days (0..99 hours)
INSERT INTO ts_cat_bulk
    SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i
    FROM generate_series(0, 99) AS i;

-- 9a: All rows inserted
SELECT count(*) AS bulk_count FROM ts_cat_bulk;

-- 9b: 5 distinct chunks created (day 1,2,3,4,5 — 100 hours spans into day 5)
SELECT count(DISTINCT chunk_number) AS bulk_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_bulk'::regclass;

-- 9c: Per-day counts match expectations (24 rows per full day)
SELECT date_trunc('day', ts)::date AS day, count(*)
FROM ts_cat_bulk
WHERE ts < '2025-01-04'
GROUP BY 1
ORDER BY 1;

-- 9d: Pruned scan on bulk data
SELECT count(*) AS day3_count
FROM ts_cat_bulk
WHERE ts >= '2025-01-03 00:00:00+00' AND ts < '2025-01-04 00:00:00+00';

DROP TABLE ts_cat_bulk;

-- ======================================================================
-- Section 10: Index build and scan with catalog — disabled along with
-- the ts_btree index AM.  Re-enable together with the AM:
-- ======================================================================
-- -- 10a: Create index on existing data
-- CREATE INDEX ON ts_cat_daily USING ts_btree (ts);
--
-- -- 10b: Index scan with chunk pruning
-- SET enable_seqscan = off;
-- EXPLAIN (COSTS OFF) SELECT val FROM ts_cat_daily
-- WHERE ts >= '2025-01-02 00:00:00+00' AND ts < '2025-01-03 00:00:00+00';
-- SELECT val FROM ts_cat_daily
-- WHERE ts >= '2025-01-02 00:00:00+00' AND ts < '2025-01-03 00:00:00+00';
-- RESET enable_seqscan;
--
-- -- 10c: Insert new data after index creation — new chunk, index maintained
-- INSERT INTO ts_cat_daily VALUES ('2025-01-10 12:00:00+00', 100);
--
-- -- 10d: New chunk registered in catalog
-- SELECT DISTINCT chunk_number
-- FROM time_series.ts_chunk
-- WHERE table_oid = 'ts_cat_daily'::regclass
-- ORDER BY chunk_number;
--
-- -- 10e: New data queryable via index scan
-- SET enable_seqscan = off;
-- SELECT val FROM ts_cat_daily
-- WHERE ts >= '2025-01-10 00:00:00+00' AND ts < '2025-01-11 00:00:00+00';
-- RESET enable_seqscan;
--
-- -- 10f: Cross-chunk index range scan (spans multiple chunks)
-- SET enable_seqscan = off;
-- SELECT val FROM ts_cat_daily
-- WHERE ts >= '2025-01-01 00:00:00+00' AND ts < '2025-01-04 00:00:00+00'
-- ORDER BY val;
-- RESET enable_seqscan;
--
-- DROP INDEX ts_cat_daily_ts_idx;

-- ======================================================================
-- Section 11: pg_relation_size uses catalog
-- ======================================================================

-- 11a: Relation size (runs on coordinator, which may not have chunk data
-- for DISTRIBUTED REPLICATED tables — returns 0 on coordinator)
SELECT pg_relation_size('ts_cat_daily') >= 0 AS size_ok;

-- ======================================================================
-- Section 12: Different interval types with catalog
-- ======================================================================

-- 12a: Weekly interval
CREATE TABLE ts_cat_weekly (
    ts timestamptz NOT NULL,
    v int
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 week')
  DISTRIBUTED REPLICATED;

INSERT INTO ts_cat_weekly VALUES
    ('2025-01-06 10:00:00+00', 1),
    ('2025-01-13 10:00:00+00', 2),
    ('2025-01-20 10:00:00+00', 3);

SELECT count(DISTINCT chunk_number) AS weekly_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_weekly'::regclass;

-- 12b: Monthly interval
CREATE TABLE ts_cat_monthly (
    ts timestamptz NOT NULL,
    v int
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
  DISTRIBUTED REPLICATED;

INSERT INTO ts_cat_monthly VALUES
    ('2025-01-15', 1),
    ('2025-02-15', 2),
    ('2025-03-15', 3),
    ('2025-04-15', 4);

SELECT count(DISTINCT chunk_number) AS monthly_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_monthly'::regclass;

-- 12c: Monthly scan pruning works
SELECT v FROM ts_cat_monthly
WHERE ts >= '2025-02-01' AND ts < '2025-03-01'
ORDER BY v;

DROP TABLE ts_cat_weekly;
DROP TABLE ts_cat_monthly;

-- ======================================================================
-- Section 13: ROLLBACK undoes catalog entries
-- ======================================================================

CREATE TABLE ts_cat_rollback (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

BEGIN;
INSERT INTO ts_cat_rollback VALUES ('2025-01-01 12:00:00+00', 1);

-- Inside transaction: chunk should exist in catalog
SELECT count(DISTINCT chunk_number) AS chunks_in_txn
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_rollback'::regclass;

ROLLBACK;

-- After rollback: catalog entry is rolled back
SELECT count(DISTINCT chunk_number) AS chunks_after_rollback
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_rollback'::regclass;

-- Table empty after rollback
SELECT count(*) AS rows_after_rollback FROM ts_cat_rollback;

-- Insert into a DIFFERENT chunk (day 2) to avoid the orphaned chunk file
-- from the rolled-back insert (day 1 chunk file still exists on disk,
-- so smgrexists returns true and the catalog insert is skipped).
INSERT INTO ts_cat_rollback VALUES ('2025-01-02 12:00:00+00', 2);

SELECT count(*) AS rows_committed FROM ts_cat_rollback;

SELECT count(DISTINCT chunk_number) AS chunks_committed
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_rollback'::regclass;

DROP TABLE ts_cat_rollback;

-- ======================================================================
-- Section 14: DROP TABLE cleans up catalog entries
-- ======================================================================

DROP TABLE ts_cat_daily;

-- 14a: No orphaned entries should remain
SELECT count(*) AS orphaned
FROM time_series.ts_chunk
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- ======================================================================
-- Section 15: Multiple tables with independent catalogs
-- ======================================================================

CREATE TABLE ts_multi_a (ts timestamptz NOT NULL, v int)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
    DISTRIBUTED REPLICATED;
CREATE TABLE ts_multi_b (ts timestamptz NOT NULL, v int)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
    DISTRIBUTED REPLICATED;

INSERT INTO ts_multi_a VALUES ('2025-01-15', 1), ('2025-02-15', 2);
INSERT INTO ts_multi_b VALUES ('2025-01-01 12:00:00+00', 10), ('2025-01-02 12:00:00+00', 20);

-- 15a: Each table has its own catalog entries
SELECT count(DISTINCT chunk_number) AS a_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_multi_a'::regclass;

SELECT count(DISTINCT chunk_number) AS b_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_multi_b'::regclass;

-- 15b: Data correct
SELECT v FROM ts_multi_a ORDER BY v;
SELECT v FROM ts_multi_b ORDER BY v;

-- 15c: Drop one table; the other's entries remain
DROP TABLE ts_multi_a;

SELECT count(DISTINCT chunk_number) AS b_still
FROM time_series.ts_chunk
WHERE table_oid = 'ts_multi_b'::regclass;

DROP TABLE ts_multi_b;

-- ======================================================================
-- Section 16: DROP multiple tables in one statement
-- ======================================================================

CREATE TABLE ts_drop1 (ts timestamptz NOT NULL, v int)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
    DISTRIBUTED REPLICATED;
CREATE TABLE ts_drop2 (ts timestamptz NOT NULL, v int)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
    DISTRIBUTED REPLICATED;

INSERT INTO ts_drop1 VALUES ('2025-01-01 12:00:00+00', 1);
INSERT INTO ts_drop2 VALUES ('2025-01-02 12:00:00+00', 2);

-- Both tables have catalog entries
SELECT count(*) > 0 AS has_entries FROM time_series.ts_chunk
WHERE table_oid IN (
    'ts_drop1'::regclass,
    'ts_drop2'::regclass
);

-- Drop both in one statement
DROP TABLE ts_drop1, ts_drop2;

-- All catalog entries cleaned up (delta vs the schedule baseline so
-- prior-test leakage doesn't shift the expected number)
SELECT count(*) - :baseline_chunks AS remaining_after_multi_drop FROM time_series.ts_chunk;

-- ======================================================================
-- Section 17: DROP TABLE IF EXISTS (non-existent table)
-- ======================================================================

-- Should not error on catalog cleanup
DROP TABLE IF EXISTS ts_nonexistent_table;

-- ======================================================================
-- Section 18: Recreate table after DROP — catalog works for new table
-- ======================================================================

CREATE TABLE ts_recreate (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_recreate VALUES ('2025-01-01 12:00:00+00', 1);

SELECT count(DISTINCT chunk_number) AS recreate_chunks_1
FROM time_series.ts_chunk
WHERE table_oid = 'ts_recreate'::regclass;

DROP TABLE ts_recreate;

-- Recreate with same name
CREATE TABLE ts_recreate (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- New table, new OID, catalog should be empty for it
SELECT count(DISTINCT chunk_number) AS recreate_chunks_2
FROM time_series.ts_chunk
WHERE table_oid = 'ts_recreate'::regclass;

-- Insert works correctly in recreated table
INSERT INTO ts_recreate VALUES
    ('2025-01-01 12:00:00+00', 10),
    ('2025-01-02 12:00:00+00', 20);

SELECT val FROM ts_recreate ORDER BY val;

SELECT count(DISTINCT chunk_number) AS recreate_chunks_3
FROM time_series.ts_chunk
WHERE table_oid = 'ts_recreate'::regclass;

DROP TABLE ts_recreate;

-- No orphans
SELECT count(*) AS final_orphans
FROM time_series.ts_chunk
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- ======================================================================
-- Section 19: INSERT...RETURNING with catalog
-- ======================================================================

CREATE TABLE ts_cat_ret (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- Single row RETURNING
INSERT INTO ts_cat_ret VALUES ('2025-01-01 12:00:00+00', 1) RETURNING val;

-- Multi-row RETURNING
INSERT INTO ts_cat_ret VALUES
    ('2025-01-02 12:00:00+00', 2),
    ('2025-01-03 12:00:00+00', 3)
    RETURNING val;

-- Verify all rows + catalog
SELECT val FROM ts_cat_ret ORDER BY val;

SELECT count(DISTINCT chunk_number) AS ret_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_ret'::regclass;

DROP TABLE ts_cat_ret;

-- ======================================================================
-- Section 20: Subquery and CTE with catalog-based scans
-- ======================================================================

CREATE TABLE ts_cat_cte (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_cat_cte VALUES
    ('2025-01-01 12:00:00+00', 1),
    ('2025-01-02 12:00:00+00', 2),
    ('2025-01-03 12:00:00+00', 3);

-- 20a: Subquery with pruning
SELECT * FROM (
    SELECT val FROM ts_cat_cte
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02'
) sub ORDER BY val;

-- 20b: CTE with pruning
WITH jan1 AS (
    SELECT val FROM ts_cat_cte
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02'
)
SELECT count(*) AS cte_count FROM jan1;

-- 20c: UNION of pruned scans from different chunks
SELECT val FROM ts_cat_cte
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02'
UNION ALL
SELECT val FROM ts_cat_cte
    WHERE ts >= '2025-01-03' AND ts < '2025-01-04'
ORDER BY val;

-- 20d: Aggregate with chunk pruning
SELECT count(*), min(val), max(val) FROM ts_cat_cte
WHERE ts >= '2025-01-01' AND ts < '2025-01-03';

DROP TABLE ts_cat_cte;

-- ======================================================================
-- Section 21: ROLLBACK + re-INSERT catalog recovery
-- ======================================================================

CREATE TABLE ts_cat_rollback (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- INSERT then ROLLBACK — fork files left on disk, catalog rolled back
BEGIN;
INSERT INTO ts_cat_rollback
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 47) AS i;
ROLLBACK;

-- Table should be empty after rollback
SELECT count(*) AS after_rollback FROM ts_cat_rollback;

-- Re-INSERT same data — must succeed despite orphaned fork files
INSERT INTO ts_cat_rollback
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 47) AS i;

-- All 48 rows should be visible
SELECT count(*) AS after_reinsert FROM ts_cat_rollback;

-- Chunks should be in catalog
SELECT count(DISTINCT chunk_number) AS n_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_cat_rollback'::regclass;

-- Pruned scan works
SELECT count(*) AS day1 FROM ts_cat_rollback
WHERE ts >= '2025-01-01' AND ts < '2025-01-02';

DROP TABLE ts_cat_rollback;

-- ======================================================================
-- Section 22: Verify everything is clean
-- ======================================================================

SELECT count(*) - :baseline_chunks AS total_remaining FROM time_series.ts_chunk;

-- ======================================================================
-- Cleanup
-- ======================================================================

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
