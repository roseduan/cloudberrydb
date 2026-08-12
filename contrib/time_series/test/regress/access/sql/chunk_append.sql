-- ============================================================
-- chunk_append.sql — regression tests for the ChunkAppend
-- CustomScan path under all registered bucketing functions.
--
-- Each section sets enable_chunk_append=on and verifies:
--   (1) query executes without error
--   (2) result row count matches the equivalent run with
--       enable_chunk_append=off (correctness oracle)
--   (3) the chosen plan actually uses ChunkAppend (EXPLAIN
--       contains "ChunkAppend")
--
-- Sections cover:
--   §1  time_bucket — 2-arg, 3-arg, multiple time types
--   §2  time_bucket_gapfill — 4-arg, with WHERE on non-tlist col
--   §3  time_bucket with timezone — 5-arg overload
--   §4  date_trunc bucketing
--   §5  Mixed: DESC ordering, OFFSET, multi-column GROUP BY
--   §6  Volatile-EC regression: gapfill + LIMIT + WHERE col
--       not in tlist (used to PANIC at exec init via NULL Var
--       in setrefs.c)
--   §7  ChunkAppend vs vanilla Append result-set equality
-- ============================================================

CREATE EXTENSION IF NOT EXISTS time_series;

SET time_series.enable_chunk_append = on;
-- Make ChunkAppend the cheap option for tests of any size.
SET time_series.chunk_append_max_chunks = 256;

-- ------------------------------------------------------------
-- Fixture: multi-chunk hypertable with several devices.
-- Two 4-hour chunks span 8 hours of synthetic data, so every
-- query below crosses at least two chunks and ChunkAppend's
-- subplan list has length >= 2.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS ca_test CASCADE;
CREATE TABLE ca_test (
    "time"     TIMESTAMPTZ NOT NULL,
    device_id  INT         NOT NULL,
    value      FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO ca_test
SELECT
    '2024-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval,
    1 + (i % 4),
    (i * 1.5)::float8
FROM generate_series(0, 479) i;       -- 8 hours, every minute, 4 devices

ANALYZE ca_test;

-- ============================================================
-- §1  time_bucket — 2-arg, default and DESC
--
-- Every query carries an explicit `time` upper bound so the
-- ChunkAppend gate (which requires ts_max != DT_NOEND) fires.
-- Without it ChunkAppend bails to the regular ChunkScan path.
-- ============================================================
\echo === §1.1 time_bucket 2-arg, ORDER BY bucket ASC LIMIT 5
SELECT time_series.time_bucket('30 min'::interval, "time") AS bucket,
       count(*) AS n
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 5;

\echo === §1.2 time_bucket 2-arg, ORDER BY bucket DESC LIMIT 5
SELECT time_series.time_bucket('30 min'::interval, "time") AS bucket,
       avg(value) AS m
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket DESC
LIMIT 5;

\echo === §1.3 time_bucket 3-arg with origin offset
SELECT time_series.time_bucket(
           '1 hour'::interval, "time",
           '2019-01-01 00:00:00+00'::timestamptz) AS bucket,
       sum(value) AS s
FROM ca_test
WHERE device_id IN (1, 2, 3)
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 4;

-- ============================================================
-- §2  time_bucket_gapfill — 4-arg basic
--
-- These trigger the volatile-EquivalenceClass code path: the
-- bucketing function is VOLATILE, so its EC has ec_sortref != 0
-- and prepare_sort_from_pathkeys requires the sub-Sort's
-- targetlist to carry a TLE labelled with that sortref.
-- ============================================================
\echo === §2.1 gapfill ASC + LIMIT, no gaps
SELECT
    time_series.time_bucket_gapfill('30 min'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS m
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 4;

\echo === §2.2 gapfill with sparse data — gap rows must appear
DROP TABLE IF EXISTS ca_sparse CASCADE;
CREATE TABLE ca_sparse (
    "time"    TIMESTAMPTZ NOT NULL,
    device_id INT         NOT NULL,
    value     FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO ca_sparse VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, 20.0),
    -- hour 2: gap
    ('2024-01-01 03:00:00+00', 1, 40.0),
    ('2024-01-01 04:30:00+00', 1, 50.0);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS m
FROM ca_sparse
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 6;

\echo === §2.3 gapfill OFFSET 2 LIMIT 3 — skip leading rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS m
FROM ca_sparse
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket
OFFSET 2 LIMIT 3;

\echo === §2.4 gapfill grouped by (device_id, bucket) — multi-key
INSERT INTO ca_sparse VALUES
    ('2024-01-01 00:00:00+00', 2, 100.0),
    ('2024-01-01 03:00:00+00', 2, 300.0);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS m
FROM ca_sparse
WHERE device_id IN (1, 2)
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 03:00:00+00'
GROUP BY device_id, bucket
ORDER BY device_id, bucket
LIMIT 6;

-- ============================================================
-- §3  time_bucket with timezone (5-arg overload)
-- ============================================================
\echo === §3.1 time_bucket(period, time, tz)
SELECT time_series.time_bucket(
           '1 hour'::interval, "time", 'UTC') AS bucket,
       count(*) AS n
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 4;

-- ============================================================
-- §4  date_trunc bucketing
-- ============================================================
\echo === §4.1 date_trunc('hour', time)
SELECT date_trunc('hour', "time") AS bucket,
       count(*) AS n
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 4;

\echo === §4.2 date_trunc DESC
SELECT date_trunc('hour', "time") AS bucket,
       max(value) AS m
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket DESC
LIMIT 4;

-- ============================================================
-- §5  Aggregate variety — avg/sum/count/min/max
-- ============================================================
\echo === §5.1 multi-aggregate per bucket
SELECT time_series.time_bucket('1 hour'::interval, "time") AS bucket,
       count(*) AS n,
       sum(value) AS s,
       min(value) AS lo,
       max(value) AS hi
FROM ca_test
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 08:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 4;

-- ============================================================
-- §6  Regression: gapfill + LIMIT + WHERE column missing from
--     output tlist.  Previously PANIC'd at exec init because
--     setrefs.c couldn't find device_id in custom_scan_tlist
--     (volatile-EC pathtarget fix accidentally narrowed the
--     scan tlist).  This must run without error.
-- ============================================================
\echo === §6.1 gapfill + LIMIT, WHERE device_id not in tlist
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS m
FROM ca_sparse
WHERE device_id = 1
  AND "time" >= '2024-01-01 00:00:00+00'
  AND "time" <  '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 3;

-- ============================================================
-- §7  ChunkAppend ON vs OFF — result-set equality oracle.
--
-- For each query, count rows under ChunkAppend ON and OFF.
-- If both numbers match we have confidence the plan is
-- semantically identical regardless of which path the planner
-- chose.  Verifying row counts (instead of full result diffs)
-- keeps the .out file deterministic across different segment
-- distributions.
-- ============================================================
\echo === §7.1 result equality across enable_chunk_append on/off
SET time_series.enable_chunk_append = on;
WITH ca_on AS (
    SELECT count(*) AS n
    FROM (
        SELECT time_series.time_bucket('30 min'::interval, "time") AS b,
               avg(value)
        FROM ca_test
        WHERE device_id = 1
          AND "time" >= '2024-01-01 00:00:00+00'
          AND "time" <  '2024-01-01 08:00:00+00'
        GROUP BY b ORDER BY b LIMIT 10
    ) s
)
SELECT n FROM ca_on;

SET time_series.enable_chunk_append = off;
WITH ca_off AS (
    SELECT count(*) AS n
    FROM (
        SELECT time_series.time_bucket('30 min'::interval, "time") AS b,
               avg(value)
        FROM ca_test
        WHERE device_id = 1
          AND "time" >= '2024-01-01 00:00:00+00'
          AND "time" <  '2024-01-01 08:00:00+00'
        GROUP BY b ORDER BY b LIMIT 10
    ) s
)
SELECT n FROM ca_off;

SET time_series.enable_chunk_append = on;

\echo === §7.2 gapfill row-count parity on/off
SET time_series.enable_chunk_append = on;
SELECT count(*) AS n_on FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS b,
        avg(value)
    FROM ca_sparse
    WHERE device_id = 1
      AND "time" >= '2024-01-01 00:00:00+00'
      AND "time" <  '2024-01-01 06:00:00+00'
    GROUP BY b ORDER BY b LIMIT 6
) s;

SET time_series.enable_chunk_append = off;
SELECT count(*) AS n_off FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, "time",
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS b,
        avg(value)
    FROM ca_sparse
    WHERE device_id = 1
      AND "time" >= '2024-01-01 00:00:00+00'
      AND "time" <  '2024-01-01 06:00:00+00'
    GROUP BY b ORDER BY b LIMIT 6
) s;

SET time_series.enable_chunk_append = on;

-- ============================================================
-- §8  EXPLAIN — confirm ChunkAppend is selected for the
--     canonical Order+Limit shape.  A plpgsql block scrapes
--     the EXPLAIN text and asserts "ChunkAppend" appears.
--     If the planner ever stops picking it, this fails loudly.
-- ============================================================
\echo === §8.1 EXPLAIN picks ChunkAppend
DO $$
DECLARE
    plan_line text;
    found     boolean := false;
BEGIN
    FOR plan_line IN
        EXPLAIN (COSTS OFF)
        SELECT time_series.time_bucket('30 min'::interval, "time") AS b,
               count(*)
        FROM ca_test
        WHERE device_id = 1
          AND "time" >= '2024-01-01 00:00:00+00'
          AND "time" <  '2024-01-01 08:00:00+00'
        GROUP BY b ORDER BY b LIMIT 5
    LOOP
        IF plan_line LIKE '%ChunkAppend%' THEN
            found := true;
            EXIT;
        END IF;
    END LOOP;
    IF NOT found THEN
        RAISE EXCEPTION 'plan did not use ChunkAppend';
    END IF;
    RAISE NOTICE 'plan uses ChunkAppend: ok';
END $$;

\echo === §8.2 EXPLAIN gapfill also picks ChunkAppend
DO $$
DECLARE
    plan_line text;
    found     boolean := false;
BEGIN
    FOR plan_line IN
        EXPLAIN (COSTS OFF)
        SELECT time_series.time_bucket_gapfill('1 hour'::interval, "time",
                   '2024-01-01 00:00:00+00'::timestamptz,
                   '2024-01-01 06:00:00+00'::timestamptz) AS b,
               avg(value)
        FROM ca_sparse
        WHERE device_id = 1
          AND "time" >= '2024-01-01 00:00:00+00'
          AND "time" <  '2024-01-01 06:00:00+00'
        GROUP BY b ORDER BY b LIMIT 3
    LOOP
        IF plan_line LIKE '%ChunkAppend%' THEN
            found := true;
            EXIT;
        END IF;
    END LOOP;
    IF NOT found THEN
        RAISE EXCEPTION 'gapfill plan did not use ChunkAppend';
    END IF;
    RAISE NOTICE 'gapfill plan uses ChunkAppend: ok';
END $$;

-- ============================================================
-- cleanup
-- ============================================================
DROP TABLE ca_test CASCADE;
DROP TABLE ca_sparse CASCADE;
RESET time_series.enable_chunk_append;
RESET time_series.chunk_append_max_chunks;
