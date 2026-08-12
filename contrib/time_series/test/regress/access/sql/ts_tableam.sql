-- ts_tableam.sql: Comprehensive tests for time_series Table Access Method
--
-- Covers: table creation, reloptions, INSERT routing, chunk pruning,
--         boundary conditions, data types, error handling, EXPLAIN,
--         bulk operations, concurrent tables, edge cases.

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

\i sql/include/setup.sql

CREATE OR REPLACE FUNCTION test.show_am(rel regclass)
RETURNS name AS $$
    SELECT am.amname
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_am am ON am.oid = c.relam
    WHERE c.oid = rel;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION test.show_reloptions(rel regclass)
RETURNS text[] AS $$
    SELECT reloptions FROM pg_catalog.pg_class WHERE oid = rel;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION test.show_columns(rel regclass)
RETURNS TABLE(column_name name, data_type text, nullable text) AS $$
    SELECT a.attname,
           pg_catalog.format_type(a.atttypid, a.atttypmod),
           CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = rel
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION test.results_match(query_a text, query_b text)
RETURNS boolean AS $$
DECLARE
    diff_count bigint;
BEGIN
    EXECUTE format(
        'SELECT count(*) FROM ((SELECT * FROM (%s) a EXCEPT SELECT * FROM (%s) b) UNION ALL (SELECT * FROM (%s) c EXCEPT SELECT * FROM (%s) d)) q',
        query_a, query_b, query_b, query_a
    ) INTO diff_count;
    IF diff_count <> 0 THEN
        RAISE NOTICE 'FAIL: queries differ by % rows', diff_count;
        RETURN false;
    END IF;
    RETURN true;
END;
$$ LANGUAGE plpgsql;

-- ======================================================================
-- Section 1: Access method registration
-- ======================================================================

-- 1a: time_series is registered in pg_am
SELECT amname, amtype FROM pg_am WHERE amname = 'time_series';

-- 1b: ts_btree is registered in pg_am (disabled along with the AM)
-- SELECT amname, amtype FROM pg_am WHERE amname = 'ts_btree';

-- ======================================================================
-- Section 2: Table creation and reloptions
-- ======================================================================

-- 2a: Basic table creation with monthly interval
CREATE TABLE ts_monthly (
    ts   TIMESTAMPTZ NOT NULL,
    dev  INT,
    val  DOUBLE PRECISION
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
  DISTRIBUTED BY (dev);

SELECT test.show_am('ts_monthly');
SELECT test.show_reloptions('ts_monthly');
SELECT * FROM test.show_columns('ts_monthly');

-- 2b: Table with daily interval and custom origin
CREATE TABLE ts_daily (
    ts   TIMESTAMPTZ NOT NULL,
    dev  INT,
    val  DOUBLE PRECISION
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (dev);

SELECT test.show_reloptions('ts_daily');

-- 2c: Table with weekly interval
CREATE TABLE ts_weekly (
    ts   TIMESTAMPTZ NOT NULL,
    tag  INT,
    val  FLOAT8
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 week')
  DISTRIBUTED BY (tag);

SELECT test.show_reloptions('ts_weekly');

-- 2d: Table with yearly interval
CREATE TABLE ts_yearly (
    ts   TIMESTAMPTZ NOT NULL,
    val  INT
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 year')
  DISTRIBUTED BY (val);

SELECT test.show_reloptions('ts_yearly');

-- ======================================================================
-- Section 3: Error handling — invalid table definitions
-- ======================================================================

\set ON_ERROR_STOP 0

-- 3a: Missing ts_partition_column reloption
CREATE TABLE ts_err_no_col (id INT)
    USING time_series WITH (ts_chunk_interval='1 month')
    DISTRIBUTED BY (id);

-- 3b: Non-existent ts_partition_column
CREATE TABLE ts_err_bad_col (id INT, val FLOAT8)
    USING time_series WITH (ts_partition_column='nonexistent', ts_chunk_interval='1 month')
    DISTRIBUTED BY (id);

-- 3c: Incompatible column type for ts_partition_column
CREATE TABLE ts_err_type (ts TEXT NOT NULL, val INT)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
    DISTRIBUTED BY (val);

-- 3d: NULL timestamp insert rejected
CREATE TABLE ts_null_chk (ts TIMESTAMPTZ, val INT)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
    DISTRIBUTED BY (val);
INSERT INTO ts_null_chk VALUES (NULL, 1);

-- 3e: Timestamp before origin is rejected
INSERT INTO ts_monthly VALUES ('1999-12-31 23:59:59+00', 1, 999.0);

\set ON_ERROR_STOP 1

-- ======================================================================
-- Section 4: INSERT routing — monthly interval
-- ======================================================================

-- Insert into different months to verify chunk routing
INSERT INTO ts_monthly VALUES
    ('2025-01-05 10:00:00+00', 1, 10.0),
    ('2025-01-15 12:00:00+00', 2, 20.0),
    ('2025-01-25 08:00:00+00', 3, 30.0),
    ('2025-02-10 09:00:00+00', 1, 40.0),
    ('2025-02-20 11:00:00+00', 2, 50.0),
    ('2025-03-01 00:00:00+00', 3, 60.0),
    ('2025-03-15 14:00:00+00', 1, 70.0),
    ('2025-04-01 00:00:00+00', 2, 80.0);

-- 4a: Full table scan returns all rows
SELECT count(*) AS total FROM ts_monthly;

-- 4b: Verify data integrity
SELECT dev, val FROM ts_monthly ORDER BY val;

-- ======================================================================
-- Section 5: Chunk pruning — monthly interval
-- ======================================================================

-- 5a: Single-month scan (January)
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-01-01' AND ts < '2025-02-01'
    ORDER BY val;

-- 5b: Single-month scan (February)
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-02-01' AND ts < '2025-03-01'
    ORDER BY val;

-- 5c: Single-month scan (March)
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-03-01' AND ts < '2025-04-01'
    ORDER BY val;

-- 5d: Multi-month range (Jan-Feb)
SELECT count(*) FROM ts_monthly
    WHERE ts >= '2025-01-01' AND ts < '2025-03-01';

-- 5e: Open-ended range (everything after Feb 15)
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-02-15'
    ORDER BY val;

-- 5f: No matching rows
SELECT count(*) FROM ts_monthly
    WHERE ts >= '2025-06-01' AND ts < '2025-07-01';

-- ======================================================================
-- Section 6: EXPLAIN plan verification
-- ======================================================================

-- 6a: Full scan shows ChunkScan
EXPLAIN (COSTS OFF) SELECT * FROM ts_monthly;

-- 6b: Pruned scan shows ChunkScan with chunk info
EXPLAIN (COSTS OFF) SELECT * FROM ts_monthly
    WHERE ts >= '2025-02-01' AND ts < '2025-03-01';

-- 6c: Non-time predicate still uses ChunkScan
EXPLAIN (COSTS OFF) SELECT * FROM ts_monthly WHERE dev = 1;

-- ======================================================================
-- Section 7: INSERT...RETURNING
-- ======================================================================

-- 7a: Single row RETURNING
INSERT INTO ts_monthly VALUES ('2025-05-01 00:00:00+00', 4, 90.0)
    RETURNING dev, val;

-- 7b: Multi-row RETURNING
INSERT INTO ts_monthly VALUES
    ('2025-05-15 00:00:00+00', 5, 100.0),
    ('2025-05-20 00:00:00+00', 6, 110.0)
    RETURNING dev, val;

-- 7c: Count after all inserts
SELECT count(*) AS total FROM ts_monthly;

-- ======================================================================
-- Section 8: INSERT routing — daily interval
-- ======================================================================

-- Insert into adjacent days
INSERT INTO ts_daily VALUES
    ('2025-01-01 00:00:01+00', 1, 1.0),
    ('2025-01-01 23:59:59+00', 1, 2.0),
    ('2025-01-02 00:00:00+00', 2, 3.0),
    ('2025-01-02 12:00:00+00', 2, 4.0),
    ('2025-01-03 06:00:00+00', 3, 5.0);

-- 8a: All rows present
SELECT count(*) FROM ts_daily;

-- 8b: Prune to single day
SELECT dev, val FROM ts_daily
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02'
    ORDER BY val;

-- 8c: Boundary — row exactly at midnight belongs to new day
SELECT dev, val FROM ts_daily
    WHERE ts >= '2025-01-02' AND ts < '2025-01-03'
    ORDER BY val;

-- ======================================================================
-- Section 9: Boundary conditions
-- ======================================================================

-- 9a: Timestamp exactly at interval boundary
INSERT INTO ts_monthly VALUES ('2025-06-01 00:00:00+00', 1, 200.0);
SELECT val FROM ts_monthly
    WHERE ts >= '2025-06-01' AND ts < '2025-07-01';

-- 9b: Timestamp 1 microsecond before boundary
INSERT INTO ts_monthly VALUES ('2025-06-30 23:59:59.999999+00', 2, 201.0);
SELECT count(*) FROM ts_monthly
    WHERE ts >= '2025-06-01' AND ts < '2025-07-01';

-- 9c: Timestamp 1 microsecond after boundary
INSERT INTO ts_monthly VALUES ('2025-07-01 00:00:00.000001+00', 3, 202.0);
SELECT count(*) FROM ts_monthly
    WHERE ts >= '2025-07-01' AND ts < '2025-08-01';

-- ======================================================================
-- Section 10: Bulk INSERT...SELECT
-- ======================================================================

CREATE TABLE ts_bulk (
    ts  TIMESTAMPTZ NOT NULL,
    seq INT
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (seq);

-- Generate 100 rows spanning 10 days
INSERT INTO ts_bulk
    SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i
    FROM generate_series(0, 99) AS i;

-- 10a: All rows inserted
SELECT count(*) FROM ts_bulk;

-- 10b: Rows per day (first 3 days)
SELECT date_trunc('day', ts)::date AS day, count(*)
    FROM ts_bulk
    WHERE ts < '2025-01-04'
    GROUP BY 1
    ORDER BY 1;

-- 10c: Prune to single day
SELECT count(*) FROM ts_bulk
    WHERE ts >= '2025-01-03' AND ts < '2025-01-04';

-- ======================================================================
-- Section 11: Multiple concurrent tables
-- ======================================================================

CREATE TABLE ts_t1 (ts TIMESTAMPTZ NOT NULL, v INT)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
    DISTRIBUTED BY (v);
CREATE TABLE ts_t2 (ts TIMESTAMPTZ NOT NULL, v INT)
    USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
    DISTRIBUTED BY (v);

INSERT INTO ts_t1 VALUES ('2025-01-15', 1), ('2025-02-15', 2);
INSERT INTO ts_t2 VALUES ('2025-03-15', 3), ('2025-04-15', 4);

-- 11a: Tables are independent
SELECT count(*) FROM ts_t1;
SELECT count(*) FROM ts_t2;

-- 11b: Cross-table join
SELECT t1.v AS v1, t2.v AS v2
    FROM ts_t1 t1 CROSS JOIN ts_t2 t2
    ORDER BY t1.v, t2.v;


-- ======================================================================
-- Section 12: Aggregate queries
-- ======================================================================

SELECT min(val), max(val), avg(val)::numeric(10,2) FROM ts_monthly;
SELECT dev, sum(val) AS total FROM ts_monthly GROUP BY dev ORDER BY dev;

-- ======================================================================
-- Section 13: Subquery and CTE patterns
-- ======================================================================

-- 13a: Subquery
SELECT * FROM (
    SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-01-01' AND ts < '2025-02-01'
) sub ORDER BY val;

-- 13b: CTE
WITH jan AS (
    SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-01-01' AND ts < '2025-02-01'
)
SELECT count(*) FROM jan;

-- 13c: UNION of pruned scans
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-01-01' AND ts < '2025-02-01'
UNION ALL
SELECT dev, val FROM ts_monthly
    WHERE ts >= '2025-03-01' AND ts < '2025-04-01'
ORDER BY val;

-- ======================================================================
-- Section 14: Oracle-style verification (generate_series + EXCEPT)
-- ======================================================================

-- Verify daily table data matches expected using EXCEPT
SELECT test.results_match(
    $$SELECT seq FROM ts_bulk WHERE ts >= '2025-01-01' AND ts < '2025-01-02' ORDER BY seq$$,
    $$SELECT i FROM generate_series(0, 23) AS i ORDER BY i$$
);

-- ======================================================================
-- Section 15: Weekly interval
-- ======================================================================

INSERT INTO ts_weekly VALUES
    ('2025-01-06 10:00:00+00', 1, 1.0),
    ('2025-01-07 10:00:00+00', 2, 2.0),
    ('2025-01-13 10:00:00+00', 3, 3.0),
    ('2025-01-14 10:00:00+00', 4, 4.0);

-- 15a: Full scan
SELECT tag, val FROM ts_weekly ORDER BY val;

-- 15b: Pruned scan — first week
SELECT tag, val FROM ts_weekly
    WHERE ts >= '2025-01-06' AND ts < '2025-01-13'
    ORDER BY val;

-- ======================================================================
-- Section 16: Yearly interval
-- ======================================================================

INSERT INTO ts_yearly VALUES
    ('2024-06-15', 1),
    ('2025-03-20', 2),
    ('2026-12-01', 3);

SELECT val FROM ts_yearly ORDER BY val;

-- Prune to single year
SELECT val FROM ts_yearly
    WHERE ts >= '2025-01-01' AND ts < '2026-01-01';

-- ======================================================================
-- Section 17: ANALYZE support
-- ======================================================================

CREATE TABLE ts_analyze (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

-- 17a: Before ANALYZE, reltuples is -1 (never analyzed)
SELECT relpages, reltuples FROM pg_class WHERE relname = 'ts_analyze';

-- Insert data spanning two chunks
INSERT INTO ts_analyze
SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
FROM generate_series(1, 500) i;

INSERT INTO ts_analyze
SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 500 + i
FROM generate_series(1, 500) i;

-- 17b: Run ANALYZE
ANALYZE ts_analyze;

-- 17c: After ANALYZE, reltuples should be close to 1000
SELECT relpages > 0 AS has_pages,
       reltuples BETWEEN 900 AND 1100 AS tuples_reasonable
FROM pg_class WHERE relname = 'ts_analyze';

-- 17d: Statistics are populated (check that pg_statistic has entries)
SELECT count(*) > 0 AS has_stats
FROM pg_statistic WHERE starelid = 'ts_analyze'::regclass;

-- 17e: EXPLAIN after ANALYZE should show reasonable row estimates
EXPLAIN (COSTS OFF) SELECT * FROM ts_analyze;

-- 17f: ANALYZE on empty table works
CREATE TABLE ts_analyze_empty (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

ANALYZE ts_analyze_empty;

SELECT relpages, reltuples FROM pg_class WHERE relname = 'ts_analyze_empty';


-- ======================================================================
-- Section 18: TRUNCATE support
-- ======================================================================

CREATE TABLE ts_trunc (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

-- 18a: Insert data spanning multiple chunks
INSERT INTO ts_trunc
SELECT '2025-01-01'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 50) i;

SELECT count(*) AS before_truncate FROM ts_trunc;

-- 18b: TRUNCATE clears all data
TRUNCATE ts_trunc;

SELECT count(*) AS after_truncate FROM ts_trunc;

-- 18c: Re-insert after truncate works
INSERT INTO ts_trunc
SELECT '2025-01-01'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 10) i;

SELECT count(*) AS after_reinsert FROM ts_trunc;

-- 18d: Second TRUNCATE on re-populated table
TRUNCATE ts_trunc;
SELECT count(*) AS after_second_truncate FROM ts_trunc;

-- 18e: TRUNCATE on empty table is a no-op
TRUNCATE ts_trunc;
SELECT count(*) AS after_empty_truncate FROM ts_trunc;


-- ======================================================================
-- Section 19: DML guards — DELETE/UPDATE rejected
-- ======================================================================

CREATE TABLE ts_dml (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

INSERT INTO ts_dml VALUES ('2025-01-01 12:00:00+00', 1);

\set ON_ERROR_STOP 0

-- 19a: DELETE is rejected
DELETE FROM ts_dml WHERE val = 1;

-- 19b: UPDATE is rejected
UPDATE ts_dml SET val = 2 WHERE val = 1;

\set ON_ERROR_STOP 1

-- 19c: Data is still intact after rejected DML
SELECT val FROM ts_dml;


-- ======================================================================
-- Section 20: Operation guards — VACUUM FULL, CREATE INDEX (non-ts_btree)
-- ======================================================================

CREATE TABLE ts_guard (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

INSERT INTO ts_guard VALUES ('2025-01-01 12:00:00+00', 1);

\set ON_ERROR_STOP 0

-- 20a: VACUUM FULL is rejected (would lose chunk data)
VACUUM FULL ts_guard;

-- 20b: Standard btree index is rejected
CREATE INDEX ts_guard_idx ON ts_guard USING btree (val);

\set ON_ERROR_STOP 1

-- 20c: Regular VACUUM is a no-op (no error)
VACUUM ts_guard;

-- 20d: Data is still intact after all guard checks
SELECT val FROM ts_guard;


-- ======================================================================
-- Section 21: ALTER TABLE guards — protect partition column and AM
-- ======================================================================

CREATE TABLE ts_alter (
    ts  TIMESTAMPTZ NOT NULL,
    dev TEXT,
    val DOUBLE PRECISION
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (dev);

INSERT INTO ts_alter VALUES
    ('2025-01-01 10:00:00+00', 'a', 1.0),
    ('2025-01-02 10:00:00+00', 'b', 2.0);

\set ON_ERROR_STOP 0

-- 21a: Cannot DROP the partition column
ALTER TABLE ts_alter DROP COLUMN ts;

-- 21b: Cannot RENAME the partition column
ALTER TABLE ts_alter RENAME COLUMN ts TO old_ts;

-- 21c: Cannot ALTER TYPE of partition column
ALTER TABLE ts_alter ALTER COLUMN ts TYPE timestamp;

-- 21d: Cannot SET ACCESS METHOD to something else
ALTER TABLE ts_alter SET ACCESS METHOD heap;

\set ON_ERROR_STOP 1

-- 21e: CAN drop a non-partition column (safe)
ALTER TABLE ts_alter DROP COLUMN val;

-- 21f: CAN rename a non-partition column (safe)
ALTER TABLE ts_alter RENAME COLUMN dev TO device;

-- 21g: Table still works after allowed ALTER operations
SELECT count(*) AS alter_count FROM ts_alter;
INSERT INTO ts_alter VALUES ('2025-01-03 10:00:00+00', 'c');
SELECT count(*) AS alter_count_after FROM ts_alter;


-- ======================================================================
-- Section 22: VACUUM safety — no crash on time_series tables
-- ======================================================================

CREATE TABLE ts_vacuum (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

INSERT INTO ts_vacuum VALUES
    ('2025-01-01 10:00:00+00', 1),
    ('2025-01-02 10:00:00+00', 2);

-- 22a: Standard VACUUM should be a safe no-op
VACUUM ts_vacuum;

-- 22b: VACUUM ANALYZE should work (ANALYZE part runs our custom sampling)
VACUUM ANALYZE ts_vacuum;

-- 22c: Data intact after VACUUM operations
SELECT count(*) AS vacuum_count FROM ts_vacuum;


-- ======================================================================
-- Section 23: pg_relation_size reports non-zero for populated tables
-- ======================================================================

CREATE TABLE ts_size (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

INSERT INTO ts_size VALUES ('2025-01-01 10:00:00+00', 1);

-- 23a: pg_relation_size should be > 0
SELECT pg_relation_size('ts_size'::regclass) > 0 AS has_size;


-- ======================================================================
-- Section 24: COPY roundtrip (simulates pg_dump/pg_restore data path)
-- ======================================================================

CREATE TABLE ts_copy_src (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

INSERT INTO ts_copy_src
SELECT '2025-01-01'::timestamptz + (i * interval '6 hours'), i
FROM generate_series(1, 20) i;

SELECT count(*) AS copy_src_count FROM ts_copy_src;

-- Export via COPY
COPY ts_copy_src TO '/tmp/ts_copy_test.csv' CSV;

-- Import into a fresh time_series table
CREATE TABLE ts_copy_dst (
    ts  TIMESTAMPTZ NOT NULL,
    val INTEGER
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (val);

COPY ts_copy_dst FROM '/tmp/ts_copy_test.csv' CSV;
SELECT count(*) AS copy_dst_count FROM ts_copy_dst;


-- ======================================================================
-- Section 25: INSERT-after-TRUNCATE regression
--
-- Previously ts_chunk_get_insert_buffer's fast path used the cached
-- per-backend ts_ins_blkno without re-validating against the actual
-- fork size.  TRUNCATE between xacts physically truncated the fork to
-- 0 blocks but the cache still pointed past EOF, so the next xact's
-- first INSERT raised:
--   ERROR: could not read block N in file "base/.../ext0":
--          read only 0 of 32768 bytes
-- Fix: gate the fast path on `ts_ins_xid == GetCurrentTransactionIdIfAny()`
-- so the first row of every new xact resyncs from smgrnblocks.
-- ======================================================================

CREATE TABLE ts_trunc_bug (
    ts   TIMESTAMPTZ NOT NULL,
    v    INTEGER
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='8 hour',
    ts_chunk_origin='2025-01-01'
) DISTRIBUTED REPLICATED;

-- xact A: enough rows so the chunk fork extends past block 0
INSERT INTO ts_trunc_bug
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 second'), i
FROM generate_series(1, 5000) i;

SELECT count(*) AS rows_before_truncate FROM ts_trunc_bug;

-- xact B: physically truncate all chunk forks; cache stays
TRUNCATE ts_trunc_bug;
SELECT count(*) AS rows_after_truncate FROM ts_trunc_bug;

-- xact C: single-row INSERT (ts_heap_insert path)
INSERT INTO ts_trunc_bug VALUES ('2025-01-01 01:00:00+00', 1);

-- xact D: multi-row VALUES (ts_heap_multi_insert path)
INSERT INTO ts_trunc_bug VALUES
    ('2025-01-01 01:00:01+00', 2),
    ('2025-01-01 01:00:02+00', 3),
    ('2025-01-01 01:00:03+00', 4),
    ('2025-01-01 01:00:04+00', 5),
    ('2025-01-01 01:00:05+00', 6);

-- xact E: INSERT...SELECT (also ts_heap_multi_insert)
INSERT INTO ts_trunc_bug
SELECT '2025-01-01 02:00:00+00'::timestamptz + (i * interval '1 second'),
       100 + i
FROM generate_series(1, 100) i;

SELECT count(*) AS rows_after_inserts FROM ts_trunc_bug;

-- Second cycle: ensure the cache reset fires on every xact transition,
-- not only the first.
TRUNCATE ts_trunc_bug;
INSERT INTO ts_trunc_bug VALUES ('2025-01-01 03:00:00+00', 999);
SELECT count(*) AS rows_after_second_cycle FROM ts_trunc_bug;

DROP TABLE ts_trunc_bug;


-- ======================================================================
-- Section 26: same-xact TRUNCATE + INSERT — ts_chunk_cache invalidation
--
-- Section 25 above covers cross-xact TRUNCATE + INSERT (each stmt in
-- its own auto-commit xact).  This section drives the stricter
-- same-xact scenario BEGIN; INSERT; TRUNCATE; INSERT; COMMIT.
--
-- Root cause (found while investigating a case that looked like a
-- ts_ins per-backend cache bug but wasn't): ts_chunk_catalog_insert()
-- has a process-local, xact-scoped positive-existence cache
-- (ts_chunk_cache[] in ts_catalog.c) keyed on (table_oid,
-- chunk_number).  The first INSERT below populates that cache for
-- chunk 12.  TRUNCATE deletes the real ts_chunk row for chunk 12 via
-- ts_chunk_catalog_delete(), but before the fix that delete never
-- invalidated the cache.  The second INSERT creates a brand-new
-- chunk-fork file under the new relfilenode and writes its row
-- successfully -- but ts_chunk_catalog_insert()'s cache check sees
-- the stale positive entry and returns *without* re-inserting the
-- ts_chunk catalog row.  Every scan discovers chunks by walking
-- ts_chunk, so the row becomes permanently unreachable even though
-- it is sitting in a valid heap page on disk: silent, permanent data
-- loss.  Fixed by ts_chunk_cache_invalidate(table_oid), called from
-- ts_chunk_catalog_delete() before the real rows are removed.
-- ======================================================================

CREATE TABLE ts_trunc_samex (
    ts timestamptz NOT NULL,
    v  int
) USING time_series
  WITH (ts_partition_column='ts',
        ts_chunk_interval='1 hour',
        ts_chunk_origin='2025-01-01')
  DISTRIBUTED REPLICATED;

BEGIN;
INSERT INTO ts_trunc_samex VALUES ('2025-01-01 00:30:00+00', 1);
TRUNCATE ts_trunc_samex;
-- Without ts_chunk_cache_invalidate, this row lands in a fresh
-- chunk-fork file that no ts_chunk catalog row ever points at.
INSERT INTO ts_trunc_samex VALUES ('2025-01-01 00:45:00+00', 2);
COMMIT;

SELECT count(*) AS rows_after_samex_truncate FROM ts_trunc_samex;
-- Expected: 1 (only the post-TRUNCATE INSERT survives)

-- The chunk-catalog must reflect the new fork, not the old one.
-- ts_chunk_info is EXECUTE ON ALL SEGMENTS (segment_id is its first
-- OUT column), so a REPLICATED table's one distinct chunk correctly
-- surfaces as one raw row per segment; count DISTINCT chunk_number
-- for the cluster-wide chunk count (see ts_chunk_catalog.sql's
-- "8b: Summary: distinct chunks across cluster" for the convention).
SELECT count(DISTINCT chunk_number) AS chunks_after_samex_truncate
  FROM time_series.ts_chunk_info('ts_trunc_samex');
-- Expected: 1 (a fresh chunk row from the post-TRUNCATE INSERT)

DROP TABLE ts_trunc_samex;


-- ======================================================================
-- Section 27: ALTER TABLE ... SET (ts_chunk_interval=...) after
-- TRUNCATE must take effect immediately, even in the SAME backend
-- that already cached the table's TSConfig.
--
-- ts_get_config() (ts_config.c) keeps a session-local, relid-keyed
-- cache of TSConfig (partition column + interval_usec + origin_usec)
-- that, before this fix, had NO invalidation -- the file's own
-- comment claimed invalidation was "the responsibility of relcache
-- callbacks registered elsewhere", which was impossible: the cache
-- is a file-static variable no other translation unit can reach.
--
-- ts_ddl.c permits ALTER TABLE ... SET (ts_chunk_interval / origin /
-- partition_column) as long as the table currently has zero chunks
-- (see the AT_SetRelOptions guard) -- e.g. right after a TRUNCATE.
-- Reading from or inserting into the table even once before that
-- ALTER (this test's first INSERT) warms the cache with the OLD
-- interval.  Without the fix, the INSERT below (after the ALTER)
-- would still compute its chunk_number using the OLD 1-hour interval
-- instead of the new 1-day interval -- self-consistent within this
-- one backend (so a plain row-count check can't see it: this backend
-- would read back what it itself wrote using the same stale config),
-- but observable via the chunk width ts_chunk_info() reports.
-- ======================================================================

CREATE TABLE ts_config_cache_bug (
    ts timestamptz NOT NULL,
    v  int
) USING time_series
  WITH (ts_partition_column='ts',
        ts_chunk_interval='1 hour',
        ts_chunk_origin='2025-01-01')
  DISTRIBUTED REPLICATED;

-- Warm ts_get_config's cache with the ORIGINAL (1 hour) config.
INSERT INTO ts_config_cache_bug VALUES ('2025-01-01 00:30:00+00', 1);
TRUNCATE ts_config_cache_bug;

-- Allowed: zero chunks exist right after the TRUNCATE.
ALTER TABLE ts_config_cache_bug SET (ts_chunk_interval = '1 day');

-- Without the fix, this INSERT would still use the stale 1-hour
-- config cached by the first INSERT above.
INSERT INTO ts_config_cache_bug VALUES ('2025-01-02 12:00:00+00', 2);

-- The new chunk's range must span 1 DAY (86400s), not 1 HOUR (3600s),
-- proving the post-ALTER INSERT picked up the new interval.
SELECT (EXTRACT(EPOCH FROM (range_end - range_start)) = 86400) AS chunk_width_matches_new_interval
  FROM time_series.ts_chunk_info('ts_config_cache_bug')
 LIMIT 1;
-- Expected: t

DROP TABLE ts_config_cache_bug;


-- ======================================================================
-- Cleanup
-- ======================================================================

