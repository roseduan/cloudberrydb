-- Regression tests for time_series extension: gapfill / locf / interpolate
-- Clean-room implementation — not derived from upstream tests.
--
-- NOTE: GapFill Custom Scan requires PG planner. The planner_hook auto-
-- disables ORCA for gapfill queries. We still set optimizer=off here
-- for stable EXPLAIN output across all (including non-gapfill) queries.
--
-- TIMEZONE NOTE: Session timezone is explicitly set to PST8PDT at the
-- start and after every \c reconnect. All timestamptz expected output
-- assumes PST8PDT display. If running outside the standard Docker
-- environment, ensure timezone is consistent.
--
-- MPP NOTE: In CBDB, GapFill runs on each segment independently.
-- Sections 1-65 mostly use single-device queries (co-located data),
-- with a few multi-device exceptions (e.g. 5g, 6) that are safe because
-- the table is DISTRIBUTED BY (device_id) matching the GROUP BY key.
-- Sections 66-72 test multi-device GapFill across segments.
-- Sections 73-77 test MPP-specific: sort order fix, distribution key check,
-- HAVING/LIMIT interaction, and DISTRIBUTED REPLICATED tables.
-- 8 devices × 6h range = diverse gap patterns for MPP coverage.

CREATE EXTENSION time_series;
SET optimizer = off;

-- Fix session timezone so expected output is stable across environments.
-- PST8PDT matches the Docker default; all timestamptz output uses this TZ.
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 1: Data preparation
-- Creates gapfill_test table DISTRIBUTED BY (device_id).
-- 8 devices with diverse gap patterns; different device_ids land
-- on different segments for MPP testing.
-- ============================================================

CREATE TABLE gapfill_test (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

-- Device 1: hours 0,1,3,5 (gaps at 2,4) — 4 points
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, 20.0),
    ('2024-01-01 03:00:00+00', 1, 40.0),
    ('2024-01-01 05:00:00+00', 1, 60.0);

-- Device 2: hours 0,2 (gap at 1) — 2 points
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 2, 100.0),
    ('2024-01-01 02:00:00+00', 2, 300.0);

-- Device 3: hours 1,3 (gaps at 0,2) — 2 points
INSERT INTO gapfill_test VALUES
    ('2024-01-01 01:00:00+00', 3, 1000.0),
    ('2024-01-01 03:00:00+00', 3, 3000.0);

-- Device 4: all hours 0-5, no gaps — 6 points (dense baseline)
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 4, 10000.0),
    ('2024-01-01 01:00:00+00', 4, 10010.0),
    ('2024-01-01 02:00:00+00', 4, 10020.0),
    ('2024-01-01 03:00:00+00', 4, 10030.0),
    ('2024-01-01 04:00:00+00', 4, 10040.0),
    ('2024-01-01 05:00:00+00', 4, 10050.0);

-- Device 5: endpoints only, hours 0,5 — 2 points (max gap stretch)
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 5, 50000.0),
    ('2024-01-01 05:00:00+00', 5, 55000.0);

-- Device 6: hours 0,1,2,4,5 (single gap at 3) — 5 points
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 6, 60000.0),
    ('2024-01-01 01:00:00+00', 6, 60100.0),
    ('2024-01-01 02:00:00+00', 6, 60200.0),
    ('2024-01-01 04:00:00+00', 6, 60400.0),
    ('2024-01-01 05:00:00+00', 6, 60500.0);

-- Device 7: alternating hours 0,2,4 (gaps at 1,3,5) — 3 points
INSERT INTO gapfill_test VALUES
    ('2024-01-01 00:00:00+00', 7, 70000.0),
    ('2024-01-01 02:00:00+00', 7, 70200.0),
    ('2024-01-01 04:00:00+00', 7, 70400.0);

-- Device 8: single point at hour 3 — 1 point (extreme sparse)
INSERT INTO gapfill_test VALUES
    ('2024-01-01 03:00:00+00', 8, 80000.0);

-- ============================================================
-- Section 2: Basic gapfill (TIMESTAMPTZ)
-- Tests: time_bucket_gapfill() with TIMESTAMPTZ type produces gap rows
-- with NULL aggregates for missing time buckets. Verifies:
--   2a. Full range gapfill (6h, 2 gaps at hours 2,4)
--   2b. Trimmed range (start=01:00, finish=04:00)
--   2c. COALESCE on gap rows (NULL → 0)
--   2d. CASE expression on gap rows
--   2e. Constant column alongside gapfill
-- ============================================================

-- 2a. Basic time_bucket_gapfill — gap rows have NULL aggregates
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2b. Trimmed range (start=01:00, finish=04:00)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 01:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2c. Coalesce on gap rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    COALESCE(avg(value), 0) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2d. CASE expression on gap rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    CASE WHEN avg(value) IS NULL THEN -1 ELSE avg(value) END AS val
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2e. Constant column alongside gapfill
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    'sensor_a' AS label,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 3: Multi-type bucketing (using real tables for MPP)
-- Tests time_bucket_gapfill() with non-TIMESTAMPTZ types:
--   3a. INT type (bucket_width=2, range 2..10)
--   3b. DATE type (1-day buckets, 5-day range)
--   3c. TIMESTAMP without timezone (1-hour buckets)
--   3d. BIGINT type (bucket_width=10, range 10..60)
--   3e. SMALLINT type (bucket_width=5, range 5..25)
-- Each uses a dedicated table with DISTRIBUTED BY (grp).
-- ============================================================

-- 3a. INT type gapfill
CREATE TABLE gf_int (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_int VALUES (2, 1), (4, 1), (8, 1);

SELECT
    time_series.time_bucket_gapfill(2, val, 2, 10) AS bucket,
    count(*) AS cnt
FROM gf_int
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_int;

-- 3b. DATE type gapfill
CREATE TABLE gf_date (d DATE NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_date VALUES ('2024-01-01', 1), ('2024-01-03', 1), ('2024-01-05', 1);

SELECT
    time_series.time_bucket_gapfill('1 day'::interval, d,
        '2024-01-01'::date, '2024-01-06'::date) AS bucket,
    count(*) AS cnt
FROM gf_date
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_date;

-- 3c. TIMESTAMP (without timezone) gapfill
CREATE TABLE gf_ts (ts TIMESTAMP NOT NULL, grp INT NOT NULL) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (grp);
INSERT INTO gf_ts VALUES
    ('2024-01-01 00:00:00', 1),
    ('2024-01-01 02:00:00', 1),
    ('2024-01-01 04:00:00', 1);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, ts,
        '2024-01-01 00:00:00'::timestamp,
        '2024-01-01 05:00:00'::timestamp) AS bucket,
    count(*) AS cnt
FROM gf_ts
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_ts;

-- 3d. BIGINT type gapfill
-- Data values 10, 20, 40 align with bucket_width=10, start=10 boundaries.
CREATE TABLE gf_bigint (val BIGINT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_bigint VALUES (10, 1), (20, 1), (40, 1);

SELECT
    time_series.time_bucket_gapfill(10::bigint, val, 10::bigint, 60::bigint) AS bucket,
    count(*) AS cnt
FROM gf_bigint
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_bigint;

-- 3e. SMALLINT type gapfill
-- Data values 5, 10, 20 align with bucket_width=5, start=5 boundaries.
CREATE TABLE gf_smallint (val SMALLINT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_smallint VALUES (5, 1), (10, 1), (20, 1);

SELECT
    time_series.time_bucket_gapfill(5::smallint, val, 5::smallint, 25::smallint) AS bucket,
    count(*) AS cnt
FROM gf_smallint
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_smallint;

-- ============================================================
-- Section 4: LOCF tests
-- Tests locf() (Last Observation Carried Forward) gap-fill strategy.
--   4a. Basic LOCF — carries forward last non-NULL value across gaps
--   4b. LOCF with NULLs in actual data (NULL data point vs gap)
--   4c. LOCF with multiple aggregate functions (min, max)
--   4d. LOCF with out-of-boundary — gap before first data point
--   4e. LOCF with INT type gapfill
--   4f. LOCF with DATE type gapfill
-- ============================================================

-- 4a. Basic LOCF — carries forward last non-NULL value
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 4b. LOCF with NULLs in actual data
CREATE TABLE gf_locf_null (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_locf_null VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, NULL),  -- actual NULL data point
    ('2024-01-01 02:00:00+00', 1, 30.0),
    -- hour 3 missing (gap)
    ('2024-01-01 04:00:00+00', 1, 50.0);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gf_locf_null
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_locf_null;

-- 4c. LOCF with multiple aggregate functions
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.locf(min(value)) AS locf_min,
    time_series.locf(max(value)) AS locf_max
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 4d. LOCF with out-of-boundary — gap before first data point
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2023-12-31 22:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2023-12-31 22:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 4e. LOCF with INT type (using real table for MPP)
CREATE TABLE gf_locf_int (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_int VALUES (2, 1), (6, 1), (10, 1);

SELECT
    time_series.time_bucket_gapfill(2, val, 2, 12) AS bucket,
    time_series.locf(min(val)) AS locf_v
FROM gf_locf_int
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_locf_int;

-- 4f. LOCF with DATE type (using real table for MPP)
CREATE TABLE gf_locf_date (d DATE NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_date VALUES ('2024-01-01', 1), ('2024-01-04', 1);

SELECT
    time_series.time_bucket_gapfill('1 day'::interval, d,
        '2024-01-01'::date, '2024-01-06'::date) AS bucket,
    time_series.locf(min(d)) AS locf_date
FROM gf_locf_date
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_locf_date;

-- ============================================================
-- Section 5: Interpolate tests
-- Tests interpolate() linear interpolation gap-fill strategy.
--   5a. Basic interpolate (FLOAT8) — linear between known points
--   5b. Interpolate with INT column
--   5c. Interpolate with SMALLINT column
--   5d. Interpolate with BIGINT column
--   5e. Interpolate with FLOAT4 (REAL) column
--   5f. Interpolate with NULL in data
--   5g. Interpolate with multiple groups (device_id)
-- ============================================================

-- 5a. Basic interpolate (FLOAT8) — linear interpolation between known points
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 5b. Interpolate with INT column (co-located for MPP)
CREATE TABLE gf_interp_int (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    val INT
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_interp_int VALUES
    ('2024-01-01 00:00:00+00', 1, 0),
    ('2024-01-01 04:00:00+00', 1, 80);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(min(val)) AS interp_val
FROM gf_interp_int
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_int;

-- 5c. Interpolate with SMALLINT column
CREATE TABLE gf_interp_i2 (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    val INT2
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_interp_i2 VALUES
    ('2024-01-01 00:00:00+00', 1, 10::int2),
    ('2024-01-01 03:00:00+00', 1, 40::int2);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(min(val)) AS interp_val
FROM gf_interp_i2
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_i2;

-- 5d. Interpolate with BIGINT column
CREATE TABLE gf_interp_i8 (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    val INT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_interp_i8 VALUES
    ('2024-01-01 00:00:00+00', 1, 1000000::int8),
    ('2024-01-01 04:00:00+00', 1, 5000000::int8);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(min(val)) AS interp_val
FROM gf_interp_i8
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_i8;

-- 5e. Interpolate with FLOAT4 (REAL) column
CREATE TABLE gf_interp_f4 (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    val FLOAT4
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_interp_f4 VALUES
    ('2024-01-01 00:00:00+00', 1, 1.5::float4),
    ('2024-01-01 03:00:00+00', 1, 4.5::float4);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(min(val)) AS interp_val
FROM gf_interp_f4
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_f4;

-- 5f. Interpolate with NULL in data
CREATE TABLE gf_interp_null (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_interp_null VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, NULL),
    ('2024-01-01 02:00:00+00', 1, 30.0);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_val
FROM gf_interp_null
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_null;

-- 5g. Interpolate with multiple groups (device_id IN (1,2))
-- NOTE: Multi-device query, but safe in MPP because table is DISTRIBUTED BY
-- (device_id) and GROUP BY includes device_id — data stays co-located.
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.interpolate(avg(value)) AS interp_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 6: Multi-device grouping
-- Tests GROUP BY device_id + bucket with LOCF across groups.
-- Co-located data (same device_id segment) ensures correct results.
-- ============================================================

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(value)) AS locf_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 7: CTE queries
-- Tests gapfill inside and outside CTEs:
--   7a. GapFill inside CTE, queried from outside with COALESCE
--   7b. GapFill outside CTE (raw data in CTE, gapfill in outer)
-- ============================================================

-- 7a. Gap filling inside CTE, queried from outside
WITH filled AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT bucket, COALESCE(avg_value, 0) AS val
FROM filled
ORDER BY bucket;

-- 7b. Gap filling outside CTE (data in CTE, gapfill in outer query)
WITH raw_data AS (
    SELECT time, value FROM gapfill_test WHERE device_id = 1
)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM raw_data
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 8: JOIN queries
-- Tests JOINing two gapfilled subqueries on bucket column:
--   8a. INNER JOIN two gapfilled subqueries (basic avg)
--   8b. JOIN with LOCF in both subqueries
-- ============================================================

-- 8a. INNER JOIN two gapfilled subqueries
SELECT a.bucket, a.avg_val AS dev1_avg, b.avg_val AS dev2_avg
FROM (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_val
    FROM gapfill_test WHERE device_id = 1
    GROUP BY bucket
) a
INNER JOIN (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_val
    FROM gapfill_test WHERE device_id = 2
    GROUP BY bucket
) b ON a.bucket = b.bucket
ORDER BY a.bucket;

-- 8b. JOIN with LOCF
SELECT a.bucket, a.locf_val AS dev1_locf, b.locf_val AS dev2_locf
FROM (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
        time_series.locf(avg(value)) AS locf_val
    FROM gapfill_test WHERE device_id = 1
    GROUP BY bucket
) a
INNER JOIN (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
        time_series.locf(avg(value)) AS locf_val
    FROM gapfill_test WHERE device_id = 2
    GROUP BY bucket
) b ON a.bucket = b.bucket
ORDER BY a.bucket;

-- ============================================================
-- Section 9: Edge cases
-- Tests boundary conditions for gapfill:
--   9a. Empty result set (WHERE false) — all gap rows
--   9b. All data outside range — all gap rows with NULLs
--   9c. Single data point in range
--   9d. Exact bucket boundaries (data at start and finish-1)
-- ============================================================

-- 9a. Empty result set (WHERE false)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE false
GROUP BY bucket
ORDER BY bucket;

-- 9a2. Empty result via non-existent device_id (reaches GapFill Custom Scan
-- unlike WHERE false which is eliminated at planning time in CBDB MPP)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 999
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 9b. All data outside range
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2025-01-01 00:00:00+00'::timestamptz,
        '2025-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2025-01-01 00:00:00+00' AND time < '2025-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 9c. Single data point in range
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time = '2024-01-01 00:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 9d. Exact bucket boundaries (data exactly at start and finish-1)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 02:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 02:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 10: ORDER BY variants
-- Tests different ORDER BY clauses with gapfill:
--   10a. ORDER BY bucket DESC
--   10b. ORDER BY bucket, device_id (multi-column)
-- ============================================================

-- 10a. ORDER BY bucket DESC
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket DESC;

-- 10b. ORDER BY bucket, device_id
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket, device_id
ORDER BY bucket, device_id;

-- ============================================================
-- Section 11: INSERT INTO SELECT with gapfill
-- Tests materializing gapfill results into a table via INSERT..SELECT.
-- Verifies gap rows are correctly inserted with NULL aggregates.
-- ============================================================

CREATE TABLE gapfill_result (
    bucket TIMESTAMPTZ,
    avg_value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'bucket',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (bucket);

INSERT INTO gapfill_result
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

SELECT * FROM gapfill_result ORDER BY bucket;

DROP TABLE gapfill_result;

-- ============================================================
-- Section 12: LOCF and Interpolate together
-- Tests using both locf() and interpolate() in the same query.
-- Uses different aggregates (min vs avg) so they map to different
-- sub-columns, avoiding same-aggregate collision.
-- ============================================================

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.locf(min(value)) AS locf_val,
    time_series.interpolate(avg(value)) AS interp_val
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 13: Error handling
-- Tests error conditions for time_bucket_gapfill parameters:
--   13a-c. bucket_width = 0 (interval, int, date)
--   13d.   Negative bucket_width
--   13e-f. start >= finish (reversed/equal range)
--   13g.   start >= finish (int type)
--   13h-i. Missing start/finish parameters
--   13j-l. NULL bucket_width/start/finish
-- ============================================================

\set ON_ERROR_STOP 0

-- 13a. bucket_width = 0 (interval)
SELECT
    time_series.time_bucket_gapfill('0 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13b. bucket_width = 0 (int)
CREATE TABLE gf_err_int (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_int VALUES (1, 1), (5, 1);
SELECT
    time_series.time_bucket_gapfill(0, val, 1, 10) AS bucket,
    count(*)
FROM gf_err_int WHERE grp = 1
GROUP BY bucket;
DROP TABLE gf_err_int;

-- 13c. bucket_width = 0 (date)
CREATE TABLE gf_err_date (d DATE NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_date VALUES ('2024-01-01', 1);
SELECT
    time_series.time_bucket_gapfill('0 day'::interval, d,
        '2024-01-01'::date, '2024-01-06'::date) AS bucket,
    count(*)
FROM gf_err_date WHERE grp = 1
GROUP BY bucket;
DROP TABLE gf_err_date;

-- 13d. Negative bucket_width (int)
CREATE TABLE gf_err_neg (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_neg VALUES (1, 1);
SELECT
    time_series.time_bucket_gapfill(-1, val, 1, 10) AS bucket,
    count(*)
FROM gf_err_neg WHERE grp = 1
GROUP BY bucket;
DROP TABLE gf_err_neg;

-- 13e. start >= finish (timestamptz)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 06:00:00+00'::timestamptz,
        '2024-01-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13f. start = finish (timestamptz)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13g. start >= finish (int)
CREATE TABLE gf_err_range (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_range VALUES (1, 1);
SELECT
    time_series.time_bucket_gapfill(1, val, 10, 5) AS bucket,
    count(*)
FROM gf_err_range WHERE grp = 1
GROUP BY bucket;
DROP TABLE gf_err_range;

-- 13h. Missing start and finish
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13i. Missing finish only
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13j. NULL bucket_width
SELECT
    time_series.time_bucket_gapfill(NULL::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13k. NULL start
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        NULL::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- 13l. NULL finish
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        NULL::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

\set ON_ERROR_STOP 1

-- ============================================================
-- Section 15: locf/interpolate pass-through outside GapFill
-- When called directly (not inside a gapfill context), locf() and
-- interpolate() act as pass-through: return input unchanged.
-- ============================================================

-- When called directly (not inside gapfill), these are pass-through
SELECT time_series.locf(42);
SELECT time_series.locf(NULL::int);
SELECT time_series.locf(3.14);
SELECT time_series.interpolate(42);
SELECT time_series.interpolate(NULL::int);
SELECT time_series.interpolate(3.14);

-- Reconnect to reset session state before new test sections
\c :DBNAME
SET optimizer = off;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 16: Gapfill without aggregation (no GROUP BY)
-- Without GROUP BY, time_bucket_gapfill acts like time_bucket
-- (no gap filling). Verifies pass-through behavior.
-- ============================================================

-- Without GROUP BY, time_bucket_gapfill acts like time_bucket (no gap filling)
CREATE TABLE gf_noagg (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_noagg VALUES (1, 1), (2, 1);

SELECT time_series.time_bucket_gapfill(1, val, 1, 11)
FROM gf_noagg WHERE grp = 1
ORDER BY 1;

DROP TABLE gf_noagg;

-- ============================================================
-- Section 17: Non-aligned bucket start (data silently lost)
-- bucket_width=10, start=5 → gapfill slots at 5,15,25,35.
-- Data val=11,22 maps via time_bucket(10,val) to buckets 10,20 (origin=0),
-- which don't match any gapfill slot. All data is silently discarded,
-- producing all-NULL output. This documents the current behavior.
-- ============================================================

CREATE TABLE gf_nonalign (val INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_nonalign VALUES (11, 1), (22, 1);

SELECT
    time_series.time_bucket_gapfill(10, val, 5, 40) AS bucket,
    count(*) AS cnt
FROM gf_nonalign WHERE grp = 1
GROUP BY bucket ORDER BY bucket;

DROP TABLE gf_nonalign;

-- ============================================================
-- Section 18: Multiple values per bucket
-- Tests aggregation when multiple rows fall in the same bucket.
-- Verifies min() correctly picks smallest value per bucket.
-- ============================================================

CREATE TABLE gf_multi_val (val INT NOT NULL, data INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_multi_val VALUES (10, 2, 1), (11, 3, 1), (12, 4, 1), (22, 5, 1), (30, 6, 1);

SELECT
    time_series.time_bucket_gapfill(10, val, 0, 50) AS time,
    min(data) AS value
FROM gf_multi_val WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_multi_val;

-- ============================================================
-- Section 19: References to different columns
-- Tests gapfill with min/max on both time column and value column.
-- Verifies aggregates on different columns work independently.
-- ============================================================

CREATE TABLE gf_diffcol (t INT NOT NULL, v INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_diffcol VALUES (1, 3, 1), (2, 5, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS t,
    min(t), max(t), min(v), max(v)
FROM gf_diffcol WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_diffcol;

-- ============================================================
-- Section 20: Values outside boundaries
-- Tests data points outside [start, finish) range.
-- Values at t=-1 and t=6 should be excluded from gap fill range [1,6).
-- ============================================================

CREATE TABLE gf_outside (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_outside VALUES (-1, 1), (1, 1), (3, 1), (6, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t)
FROM gf_outside WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_outside;

-- ============================================================
-- Section 21: Gap fill before first row and after last row
-- Data at t=2,3,4 with range [1,6). Verifies gap rows generated
-- at t=1 (before first data) and t=5 (after last data).
-- ============================================================

CREATE TABLE gf_beforeafter (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_beforeafter VALUES (2, 1), (3, 1), (4, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t)
FROM gf_beforeafter WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_beforeafter;

-- ============================================================
-- Section 22: Complex coalesce and CASE
-- Tests COALESCE with multiple columns and different defaults,
-- and CASE expressions with various conditions on gap rows.
-- ============================================================

-- Coalesce with multiple columns
CREATE TABLE gf_coalesce (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_coalesce VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    coalesce(min(t), 0),
    coalesce(min(value), 0),
    coalesce(min(value), 7)
FROM gf_coalesce WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_coalesce;

-- CASE with various expressions
CREATE TABLE gf_case (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_case VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t),
    CASE WHEN min(t) IS NOT NULL THEN min(t) ELSE -1 END,
    CASE WHEN min(t) IS NOT NULL THEN min(t) + 7 ELSE 0 END,
    CASE WHEN 1 = 1 THEN 1 ELSE 0 END
FROM gf_case WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_case;

-- ============================================================
-- Section 23: Constants and column reordering
-- Tests constant expressions in select list alongside gapfill,
-- and gapfill column not in first position (GROUP BY 3).
-- ============================================================

-- Constants in select list
CREATE TABLE gf_const (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_const VALUES (1, 1), (2, 1), (3, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t), min(t), 4 AS c
FROM gf_const WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_const;

-- Column reordering: gapfill column not first
CREATE TABLE gf_reorder (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_reorder VALUES (1, 1), (2, 1), (3, 1);

SELECT
    1 AS c1, '2' AS c2,
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    3.0 AS c3,
    min(t), min(t), 4 AS c4
FROM gf_reorder WHERE grp = 1
GROUP BY 3 ORDER BY 3;

DROP TABLE gf_reorder;

-- ============================================================
-- Section 24: Grouping by text columns
-- Tests GROUP BY with text-type grouping column (color).
-- Also tests text grouping with empty result set (WHERE false).
-- ============================================================

CREATE TABLE gf_textgrp (t INT NOT NULL, color TEXT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_textgrp VALUES (1, 'blue', 1, 1), (2, 'red', 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    color,
    min(value) AS m
FROM gf_textgrp WHERE grp = 1
GROUP BY 1, color ORDER BY 2, 1;

DROP TABLE gf_textgrp;

-- Text grouping with no rows in result
CREATE TABLE gf_textgrp_empty (t INT NOT NULL, color TEXT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_textgrp_empty VALUES (1, 'blue', 1, 1), (2, 'red', 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    color,
    min(value) AS m
FROM gf_textgrp_empty WHERE grp = 1 AND false
GROUP BY 1, color ORDER BY 2, 1;

DROP TABLE gf_textgrp_empty;

-- ============================================================
-- Section 25: Duplicate columns in GROUP BY
-- Tests GROUP BY with the same column referenced multiple times
-- (GROUP BY 1, 2, 3 where 2 and 3 are same column).
-- ============================================================

CREATE TABLE gf_dupgrp (t INT NOT NULL, id INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_dupgrp VALUES (1, 1, 1, 1), (2, 2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    id, id,
    min(value) AS m
FROM gf_dupgrp WHERE grp = 1
GROUP BY 1, 2, 3 ORDER BY 2, 1;

DROP TABLE gf_dupgrp;

-- ============================================================
-- Section 26: Grouping columns not in resultset
-- Tests GROUP BY column (id) not in SELECT list but used in ORDER BY.
-- Verifies hidden grouping columns work with gapfill.
-- ============================================================

CREATE TABLE gf_hiddengrp (t INT NOT NULL, id INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_hiddengrp VALUES (1, 1, 1, 1), (2, 2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    min(value) AS m
FROM gf_hiddengrp WHERE grp = 1
GROUP BY 1, id ORDER BY id, 1;

DROP TABLE gf_hiddengrp;

-- ============================================================
-- Section 27: Grouping by non-time columns with no rows
-- Tests GROUP BY with non-time column when WHERE false yields
-- no input rows. Verifies empty result behavior.
-- ============================================================

CREATE TABLE gf_grp_norows (t INT NOT NULL, id INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_grp_norows VALUES (1, 1, 1, 1), (2, 2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    id,
    min(value) AS m
FROM gf_grp_norows WHERE false
GROUP BY 1, id ORDER BY 2, 1;

DROP TABLE gf_grp_norows;

-- Reconnect to reset session state before error tests
\c :DBNAME
SET optimizer = off;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 28: Edge cases and error tests
-- Tests various edge cases for unusual usage patterns.
--
-- Actual errors:
-- - time_bucket_gapfill not top-level
-- - Multiple/nested time_bucket_gapfill calls
-- - Nested locf/interpolate calls (multiple calls per column)
-- - Mixed locf(interpolate(...)) (multiple calls per column)
-- - locf/interpolate not top-level (1 + locf(...), COALESCE, round)
-- - locf inside aggregate (min(locf(t)))
-- - Window function inside locf/interpolate
-- - NULL/zero/negative bucket_width
-- - Interpolate with unsupported types (text, interval)
-- ============================================================

\set ON_ERROR_STOP 0

-- Error: time_bucket_gapfill not top level — must be a direct target entry
CREATE TABLE gf_err_toplevel (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_toplevel VALUES (1, 1), (2, 1);
SELECT
    1 + time_series.time_bucket_gapfill(1, t, 1, 11)
FROM gf_err_toplevel WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_toplevel;

-- Error: multiple time_bucket_gapfill calls
CREATE TABLE gf_err_multi (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_multi VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.time_bucket_gapfill(1, t, 1, 11)
FROM gf_err_multi WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_multi;

-- Error: nested time_bucket_gapfill calls
CREATE TABLE gf_err_nested (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_nested VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1,
        time_series.time_bucket_gapfill(1, t, 1, 11), 1, 11)
FROM gf_err_nested WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_nested;

-- Error: nested locf calls — multiple locf/interpolate per column
CREATE TABLE gf_err_nestlocf (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_nestlocf VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.locf(time_series.locf(min(t)))
FROM gf_err_nestlocf WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_nestlocf;

-- Error: nested interpolate calls — multiple locf/interpolate per column
CREATE TABLE gf_err_nestinterp (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_nestinterp VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.interpolate(time_series.interpolate(min(t)))
FROM gf_err_nestinterp WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_nestinterp;

-- Error: mixed locf(interpolate(...)) — multiple locf/interpolate per column
CREATE TABLE gf_err_mixedli (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_mixedli VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.locf(time_series.interpolate(min(t)))
FROM gf_err_mixedli WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_mixedli;

-- Error: interpolate(locf(...)) — multiple locf/interpolate per column
CREATE TABLE gf_err_interp_locf (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_interp_locf VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.interpolate(time_series.locf(min(t)))
FROM gf_err_interp_locf WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_interp_locf;

-- Error: locf not toplevel (1 + locf(...)) — locf must be top-level expression
CREATE TABLE gf_err_locftoplevel (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_locftoplevel VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    1 + time_series.locf(min(t))
FROM gf_err_locftoplevel WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_locftoplevel;

-- Error: interpolate not toplevel (1 + interpolate(...))
CREATE TABLE gf_err_interp_toplevel (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_interp_toplevel VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    1 + time_series.interpolate(min(t))
FROM gf_err_interp_toplevel WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_interp_toplevel;

-- Error: round(interpolate(...)) — interpolate must be top-level
CREATE TABLE gf_err_round_interp (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_round_interp VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    round(time_series.interpolate(min(t)))
FROM gf_err_round_interp WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_round_interp;

-- Error: COALESCE(interpolate(...), 0) — interpolate must be top-level
CREATE TABLE gf_err_coalesce_interp (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_coalesce_interp VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    COALESCE(time_series.interpolate(min(t)), 0)
FROM gf_err_coalesce_interp WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_coalesce_interp;

-- Error: locf inside COALESCE — locf must be top-level
CREATE TABLE gf_err_locf_coalesce (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_locf_coalesce VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    COALESCE(time_series.locf(min(t)), 0)
FROM gf_err_locf_coalesce WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_locf_coalesce;

-- Error: locf inside aggregate — locf must be top-level expression
CREATE TABLE gf_err_locfagg (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_locfagg VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    min(time_series.locf(t))
FROM gf_err_locfagg WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_locfagg;

-- Error: interpolate inside aggregate — interpolate must be top-level
CREATE TABLE gf_err_interp_agg (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_interp_agg VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    min(time_series.interpolate(t))
FROM gf_err_interp_agg WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_interp_agg;

-- Error: window function inside locf
CREATE TABLE gf_err_winfunc_locf (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_winfunc_locf VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.locf(count(*) OVER ())
FROM gf_err_winfunc_locf WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_winfunc_locf;

-- Error: window function inside interpolate
CREATE TABLE gf_err_winfunc_interp (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_winfunc_interp VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.interpolate(count(*) OVER ())
FROM gf_err_winfunc_interp WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_winfunc_interp;

-- Error: NULL bucket_width
CREATE TABLE gf_err_null_width (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_null_width VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(NULL::int, t, 1, 11)
FROM gf_err_null_width WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_null_width;

-- Error: zero bucket_width (executor error)
CREATE TABLE gf_err_zero_width (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_zero_width VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(0, t, 1, 11)
FROM gf_err_zero_width WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_zero_width;

-- Error: negative bucket_width (executor error)
CREATE TABLE gf_err_neg_width (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_neg_width VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(-1, t, 1, 11)
FROM gf_err_neg_width WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_neg_width;

-- Error: interpolate with unsupported type (text)
CREATE TABLE gf_err_interp_text (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_interp_text VALUES (1, 1), (2, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.interpolate(text 'text')
FROM gf_err_interp_text WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_interp_text;

-- Error: interpolate with unsupported type (interval)
CREATE TABLE gf_err_interp_interval (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_err_interp_interval VALUES (2, 1), (3, 1);
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11),
    time_series.interpolate(interval '1d')
FROM gf_err_interp_interval WHERE grp = 1
GROUP BY 1;
DROP TABLE gf_err_interp_interval;

\set ON_ERROR_STOP 1

-- ============================================================
-- Section 29: ORDER BY locf variants
-- Tests ORDER BY with locf column in various positions:
-- - ORDER BY bucket, locf
-- - ORDER BY locf NULLS FIRST
-- - ORDER BY locf NULLS LAST
-- ============================================================

CREATE TABLE gf_orderlocf (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_orderlocf VALUES (2, 1), (3, 1);

-- ORDER BY bucket, locf
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.locf(min(t)) AS locf_val
FROM gf_orderlocf WHERE grp = 1
GROUP BY 1 ORDER BY 1, 2;

-- ORDER BY locf NULLS FIRST
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.locf(min(t)) AS locf_val
FROM gf_orderlocf WHERE grp = 1
GROUP BY 1 ORDER BY 2 NULLS FIRST, 1;

-- ORDER BY locf NULLS LAST
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.locf(min(t)) AS locf_val
FROM gf_orderlocf WHERE grp = 1
GROUP BY 1 ORDER BY 2 NULLS LAST, 1;

DROP TABLE gf_orderlocf;

-- Reconnect to reset session state
\c :DBNAME
SET optimizer = off;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 30: Interpolation with extreme values (overflow test)
-- Tests interpolation between MIN/MAX type boundaries (smallint,
-- int, bigint) and ±Infinity floats. Verifies overflow handling.
-- ============================================================

CREATE TABLE gf_interp_extreme (
    t INT NOT NULL,
    s INT2,
    i INT4,
    b INT8,
    d FLOAT8,
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_interp_extreme VALUES
    (1, (-32768)::smallint, (-2147483648)::int,
     -9223372036854775807::bigint, '-Infinity'::float8, 1),
    (3, 32767::smallint, 2147483647::int,
     9223372036854775807::bigint, 'Infinity'::float8, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 4) AS time,
    time_series.interpolate(min(s)) AS "smallint",
    time_series.interpolate(min(i)) AS "int",
    time_series.interpolate(min(b)) AS "bigint",
    time_series.interpolate(min(d)) AS "double"
FROM gf_interp_extreme WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_interp_extreme;

-- ============================================================
-- Section 31: Float rounding for specific values
-- Tests interpolation with specific float values (20266.959547)
-- to verify float4 vs float8 precision differences.
-- ============================================================

CREATE TABLE gf_float_round (
    t INT NOT NULL,
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_float_round VALUES (1, 1), (10, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 11) AS bucket,
    time_series.interpolate(avg(20266.959547::float4)) AS float4,
    time_series.interpolate(avg(20266.959547::float8)) AS float8
FROM gf_float_round WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_float_round;

-- ============================================================
-- Section 32: Prepared statements
-- Tests gapfill with PREPARE/EXECUTE. Executes 6 times to trigger
-- generic plan conversion (PG switches from custom to generic plan
-- after 5 executions).
-- ============================================================

CREATE TABLE gf_prep (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_prep VALUES (1, 1, 1), (2, 2, 1);

PREPARE prep_gapfill AS
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    time_series.locf(min(value))
FROM gf_prep WHERE grp = 1
GROUP BY 1;

-- Execute 6 times to test generic plan conversion
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;
EXECUTE prep_gapfill;

DEALLOCATE prep_gapfill;
DROP TABLE gf_prep;

-- ============================================================
-- Section 33: Expressions on GROUP BY columns
-- Tests expressions applied to GROUP BY columns alongside gapfill:
-- - device_id::text (cast)
-- - 'Device ' || device_id::text (concatenation)
-- - length(color) (function on group column)
-- ============================================================

-- Expression with cast
CREATE TABLE gf_expr_cast (t INT NOT NULL, device_id INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_expr_cast VALUES (1, 1, 1), (2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    device_id::text
FROM gf_expr_cast WHERE grp = 1
GROUP BY 1, device_id ORDER BY 1;

DROP TABLE gf_expr_cast;

-- Expression with string concatenation
CREATE TABLE gf_expr_concat (t INT NOT NULL, device_id INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_expr_concat VALUES (1, 1, 1), (2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    'Device ' || device_id::text
FROM gf_expr_concat WHERE grp = 1
GROUP BY 1, device_id ORDER BY 1;

DROP TABLE gf_expr_concat;

-- Expression with length()
CREATE TABLE gf_expr_len (t INT NOT NULL, color TEXT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_expr_len VALUES (1, 'blue', 1, 1), (2, 'red', 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    time_series.locf(min(t)),
    color,
    length(color)
FROM gf_expr_len WHERE grp = 1
GROUP BY color, 1 ORDER BY 1;

DROP TABLE gf_expr_len;

-- ============================================================
-- Section 34: Interpolation with negative time values
-- Tests interpolation across negative-to-positive range (t=-40..20)
-- for all 5 numeric types (smallint, int, bigint, float4, float8).
-- NOTE: At t=0, interpolation computes -3 + (40/60)*6 = 1.0 exactly.
-- All integer and float types should produce 1 at this boundary.
-- ============================================================

CREATE TABLE gf_interp_neg (
    t INT NOT NULL,
    v1 INT2,
    v2 INT4,
    v3 INT8,
    v4 FLOAT4,
    v5 FLOAT8,
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_interp_neg VALUES
    (-40, -3::smallint, -3::int, -3::bigint, -3::float4, -3::float8, 1),
    (20, 3::smallint, 3::int, 3::bigint, 3::float4, 3::float8, 1);

SELECT time, "smallint", "int", "bigint", "float4",
       round("float8"::numeric, 10) AS "float8"
FROM (
    SELECT
        time_series.time_bucket_gapfill(10, t, -40, 30) AS time,
        time_series.interpolate(min(v1)) AS "smallint",
        time_series.interpolate(min(v2)) AS "int",
        time_series.interpolate(min(v3)) AS "bigint",
        time_series.interpolate(min(v4)) AS "float4",
        time_series.interpolate(min(v5)) AS "float8"
    FROM gf_interp_neg WHERE grp = 1
    GROUP BY 1
) sub ORDER BY 1;

DROP TABLE gf_interp_neg;

-- ============================================================
-- Section 35: Interpolate all 5 types (positive range, start=0)
-- Tests interpolation for all 5 numeric types with start=0 boundary.
-- time_bucket(10, 0) = 0 and time_bucket(10, 60) = 60 align with
-- gapfill range 0..70, producing 7 interpolated rows.
-- Also exercises the start=0 edge case (not a sentinel).
-- ============================================================

CREATE TABLE gf_interp_types (
    t INT NOT NULL,
    v1 INT2,
    v2 INT4,
    v3 INT8,
    v4 FLOAT4,
    v5 FLOAT8,
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_interp_types VALUES
    (0, -3::smallint, -3::int, -3::bigint, -3::float4, -3::float8, 1),
    (60, 3::smallint, 3::int, 3::bigint, 3::float4, 3::float8, 1);

SELECT time, "smallint", "int", "bigint", "float4",
       round("float8"::numeric, 10) AS "float8"
FROM (
    SELECT
        time_series.time_bucket_gapfill(10, t, 0, 70) AS time,
        time_series.interpolate(min(v1)) AS "smallint",
        time_series.interpolate(min(v2)) AS "int",
        time_series.interpolate(min(v3)) AS "bigint",
        time_series.interpolate(min(v4)) AS "float4",
        time_series.interpolate(min(v5)) AS "float8"
    FROM gf_interp_types WHERE grp = 1
    GROUP BY 1
) sub ORDER BY 1;

DROP TABLE gf_interp_types;

-- ============================================================
-- Section 36: LOCF with constants and expressions
-- Tests locf() with constant columns in select list, and locf()
-- applied to constants and expressions (locf(4), locf(4+min(value))).
-- ============================================================

-- locf with constants in select
CREATE TABLE gf_locf_const (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_const VALUES (1, 1, 1), (4, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    2,
    time_series.locf(min(value))
FROM gf_locf_const WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_const;

-- locf with expressions
CREATE TABLE gf_locf_expr (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_expr VALUES (1, 1, 1), (4, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.locf(min(value)),
    time_series.locf(4),
    time_series.locf(4 + min(value))
FROM gf_locf_expr WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_expr;

-- (Section 37: reserved — removed during consolidation)

-- ============================================================
-- Section 38: LOCF basic with INT gapfill
-- Tests locf() with INT-type gapfill (bucket_width=10, range 10..60).
-- Data at t=10,20,50 → gaps at t=30,40 carry forward.
-- ============================================================

CREATE TABLE gf_locf_basic (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_basic VALUES (10, 9, 1), (20, 3, 1), (50, 6, 1);

SELECT
    time_series.time_bucket_gapfill(10, t, 10, 60) AS time,
    time_series.locf(min(value)) AS value
FROM gf_locf_basic WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_basic;

-- ============================================================
-- Section 39: LOCF with NULLs in resultset (NULL data point)
-- Tests locf() when actual data contains NULL values.
-- NULL data points vs gap rows: both produce NULL in aggregates.
-- ============================================================

CREATE TABLE gf_locf_nulldata (t INT NOT NULL, value INT, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_locf_nulldata VALUES (10, 9, 1), (20, 3, 1), (30, NULL, 1), (50, 6, 1);

SELECT
    time_series.time_bucket_gapfill(10, t, 10, 60) AS time,
    time_series.locf(min(value)) AS value
FROM gf_locf_nulldata WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_nulldata;

-- ============================================================
-- Section 40: Interpolation basic with INT gapfill
-- Tests interpolate() with INT-type gapfill (bucket_width=10).
-- Data at t=10,50 → linear interpolation at t=20,30,40.
-- ============================================================

CREATE TABLE gf_interp_basic (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_interp_basic VALUES (10, 1, 1), (50, 6, 1);

SELECT
    time_series.time_bucket_gapfill(10, t, 10, 60) AS time,
    time_series.interpolate(min(value)) AS value
FROM gf_interp_basic WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_interp_basic;

-- ============================================================
-- Section 41: Interpolate with NULL values
-- Tests interpolate() when data contains NULL values.
-- NULL is not used as an interpolation anchor but does not break
-- the chain — interpolation uses the nearest non-NULL neighbors.
-- ============================================================

CREATE TABLE gf_interp_nullval (t INT NOT NULL, temp INT, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_interp_nullval VALUES (1, 0, 1), (2, NULL, 1), (5, 5, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    time_series.interpolate(min(temp)) AS temp
FROM gf_interp_nullval WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_interp_nullval;

-- ============================================================
-- Section 42: LOCF with different datatypes (text, int[])
-- Tests locf() with non-numeric types: TEXT and INT[] (array).
-- Verifies variable-length datums are correctly carried forward.
-- ============================================================

CREATE TABLE gf_locf_types (
    t INT NOT NULL,
    v1 TEXT,
    v2 INT[],
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_locf_types VALUES
    (1, 'foo', ARRAY[1,2,3], 1),
    (3, 'bar', ARRAY[3,4,5], 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    time_series.locf(min(v1)) AS "text",
    time_series.locf(min(v2)) AS "int[]"
FROM gf_locf_types WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_types;

-- ============================================================
-- Section 43: Reorder (GROUP BY 1,id ORDER BY 1,id)
-- Tests GROUP BY and ORDER BY with same columns in same order.
-- Verifies gapfill produces correct sort when id is secondary key.
-- ============================================================

CREATE TABLE gf_reorder2 (t INT NOT NULL, id INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_reorder2 VALUES (1, 1, 1, 1), (2, 2, 2, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    id,
    min(value) AS m
FROM gf_reorder2 WHERE grp = 1
GROUP BY 1, id ORDER BY 1, id;

DROP TABLE gf_reorder2;

-- ============================================================
-- Section 44: DISTINCT ON with gapfill
-- Tests DISTINCT ON (color) with gapfill. Returns first row per
-- color group from the gapfilled result.
-- ============================================================

CREATE TABLE gf_distinct (t INT NOT NULL, color TEXT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_distinct VALUES (1, 'blue', 1, 1), (2, 'red', 2, 1);

SELECT DISTINCT ON (color)
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    color,
    min(value) AS m
FROM gf_distinct WHERE grp = 1
GROUP BY 1, color ORDER BY 2, 1;

DROP TABLE gf_distinct;

-- ============================================================
-- Section 45: Column references with TIME_COLUMN last
-- Tests time_bucket_gapfill as last column in SELECT list.
-- GROUP BY color, 3 where 3 is the gapfill column.
-- ============================================================

CREATE TABLE gf_colref_last (t INT NOT NULL, color TEXT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_colref_last VALUES (1, 'blue', 1, 1), (2, 'red', 2, 1);

-- time_bucket_gapfill column is last in select list and GROUP BY
SELECT
    time_series.locf(min(t)),
    color,
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time
FROM gf_colref_last WHERE grp = 1
GROUP BY color, 3 ORDER BY 3;

DROP TABLE gf_colref_last;

-- ============================================================
-- Section 46: Month interval gapfill (TIMESTAMPTZ)
-- Tests gapfill with month-based intervals (variable-length).
-- 2-month and 1-month buckets over TIMESTAMPTZ data.
-- ============================================================

-- chunk_origin one year earlier than earliest data; '2020-01-01' (no
-- TZ) would be parsed in session timezone (PST8PDT here), making it
-- 8h LATER than UTC and rejecting the UTC '2020-01-01' INSERT below.
-- chunk_interval = '1 month' matches the 1-/2-month gapfill buckets
-- queries use below.  Using `4 hour` here would produce a 1:180 reverse
-- ratio (many tiny chunks per huge bucket) — functionally fine but
-- noisy in catalog scans.
CREATE TABLE gf_month_tstz (
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_month_tstz VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-03-01 00:00:00+00', 1, 30.0),
    ('2020-05-01 00:00:00+00', 1, 50.0);

-- 2-month gapfill: should produce buckets at Jan, Mar, May
SELECT
    time_series.time_bucket_gapfill('2 month'::interval, ts,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-07-01 00:00:00+00'::timestamptz) AS bucket,
    min(value) AS min_value
FROM gf_month_tstz
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

-- 1-month gapfill: should fill Feb, Apr gaps
SELECT
    time_series.time_bucket_gapfill('1 month'::interval, ts,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-06-01 00:00:00+00'::timestamptz) AS bucket,
    min(value) AS min_value
FROM gf_month_tstz
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_tstz;

-- ============================================================
-- Section 47: Month interval gapfill (DATE)
-- Tests month-interval gapfill with DATE type.
-- 1-month buckets from Jan to Apr with gap at Feb.
-- ============================================================

CREATE TABLE gf_month_date (
    d DATE NOT NULL,
    grp INT NOT NULL,
    value FLOAT8
) DISTRIBUTED BY (grp);

INSERT INTO gf_month_date VALUES
    ('2020-01-01', 1, 10.0),
    ('2020-03-01', 1, 30.0);

SELECT
    time_series.time_bucket_gapfill('1 month'::interval, d,
        '2020-01-01'::date, '2020-04-01'::date) AS bucket,
    min(value) AS min_value
FROM gf_month_date
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_date;

-- ============================================================
-- Section 48: Month interval gapfill (TIMESTAMP)
-- Tests month-interval gapfill with TIMESTAMP (no timezone) type.
-- 1-month buckets from Jan to May with gaps at Feb, Mar.
-- ============================================================

CREATE TABLE gf_month_ts (
    ts TIMESTAMP NOT NULL,
    grp INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (grp);

INSERT INTO gf_month_ts VALUES
    ('2020-01-01 00:00:00', 1, 10.0),
    ('2020-04-01 00:00:00', 1, 40.0);

SELECT
    time_series.time_bucket_gapfill('1 month'::interval, ts,
        '2020-01-01 00:00:00'::timestamp,
        '2020-05-01 00:00:00'::timestamp) AS bucket,
    min(value) AS min_value
FROM gf_month_ts
WHERE grp = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_ts;

-- ============================================================
-- Section 49: Timezone gapfill (non-month + timezone)
-- Tests 5-arg time_bucket_gapfill with explicit timezone parameter.
-- 1-day buckets with 'UTC' timezone over 3-day range.
-- ============================================================

CREATE TABLE gf_tz_test (
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_tz_test VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-01-03 00:00:00+00', 1, 30.0);

SELECT
    time_series.time_bucket_gapfill('1 day'::interval, ts, 'UTC',
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-01-04 00:00:00+00'::timestamptz) AS bucket,
    min(value) AS min_value
FROM gf_tz_test
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_tz_test;

-- ============================================================
-- Section 50: Month interval + timezone combination
-- Tests month-interval gapfill with explicit timezone parameter.
-- 1-month buckets with 'UTC' timezone.
-- ============================================================

CREATE TABLE gf_month_tz (
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_month_tz VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-03-01 00:00:00+00', 1, 30.0);

SELECT
    time_series.time_bucket_gapfill('1 month'::interval, ts, 'UTC',
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-04-01 00:00:00+00'::timestamptz) AS bucket,
    min(value) AS min_value
FROM gf_month_tz
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_tz;

-- ============================================================
-- Section 51: Month interval + LOCF
-- Tests locf() with month-interval gapfill. Data at Jan and Apr,
-- gaps at Feb and Mar carry forward Jan's value.
-- ============================================================

CREATE TABLE gf_month_locf (
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_month_locf VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0),
    ('2020-04-01 00:00:00+00', 1, 40.0);

SELECT
    time_series.time_bucket_gapfill('1 month'::interval, ts,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-05-01 00:00:00+00'::timestamptz) AS bucket,
    time_series.locf(min(value)) AS locf_value
FROM gf_month_locf
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_locf;

-- ============================================================
-- Section 52: Error: mixed month + day interval
-- Tests error when interval has both month and day/time components.
-- '1 month 1 day' and '1 month 1 hour' should raise errors.
-- ============================================================

\set ON_ERROR_STOP 0

-- Should error: month intervals cannot have day or time component
SELECT
    time_series.time_bucket_gapfill('1 month 1 day'::interval, time,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-04-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

-- Should error: month intervals cannot have day or time component
SELECT
    time_series.time_bucket_gapfill('1 month 1 hour'::interval, time,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-04-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value)
FROM gapfill_test
WHERE device_id = 1
GROUP BY bucket;

\set ON_ERROR_STOP 1

-- ============================================================
-- Section 53: Window functions with gapfill
-- Tests window functions (lag, lead, row_number, sum OVER) combined
-- with gapfill, locf, and interpolate:
--   53a. lag() with interpolate
--   53b. Multiple window functions with gapfill
--   53c. Window functions with constants
--   53d. Window functions with locf (lag/lead of locf)
--   53e. Window functions with interpolate (lag/lead of interpolate)
--   53f. Window functions with expressions
-- ============================================================

-- 53a. lag() with interpolate
-- Data at t=10, t=30 align with bucket_width=10, start=10 boundaries
-- time_bucket(10, 10) = 10, time_bucket(10, 30) = 30 (perfect alignment)
CREATE TABLE gf_win_lag (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_lag VALUES (10, 1), (30, 1);

SELECT
    time_series.time_bucket_gapfill(10, t, 10, 60) AS bucket,
    time_series.interpolate(min(t)) AS interp,
    lag(min(t)) OVER () AS lag_min
FROM gf_win_lag WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_lag;

-- 53b. Multiple window functions with gapfill
-- Uses separate value column for locf to avoid collision with interpolate(min(t)).
CREATE TABLE gf_win_multi (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_multi VALUES (1, 10, 1), (9, 90, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 10) AS bucket,
    time_series.interpolate(min(t)) AS interp,
    row_number() OVER () AS rn,
    time_series.locf(min(value)) AS locf,
    sum(time_series.interpolate(min(t))) OVER (ROWS 1 PRECEDING) AS sum_1,
    sum(time_series.interpolate(min(t))) OVER (ROWS 2 PRECEDING) AS sum_2
FROM gf_win_multi WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_multi;

-- 53c. Window functions with constants
CREATE TABLE gf_win_const (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_const VALUES (1, 1), (2, 1), (3, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t),
    4 AS c,
    lag(min(t)) OVER () AS lag_min
FROM gf_win_const WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_const;

-- 53d. Window functions with locf
-- Uses separate value column for locf to avoid collision with min(t).
CREATE TABLE gf_win_locf (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_locf VALUES (1, 10, 1), (2, 20, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t) AS "min",
    lag(min(t)) OVER () AS lag_min,
    lead(min(t)) OVER () AS lead_min,
    time_series.locf(min(value)) AS locf,
    lag(time_series.locf(min(value))) OVER () AS lag_locf,
    lead(time_series.locf(min(value))) OVER () AS lead_locf
FROM gf_win_locf WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_locf;

-- 53e. Window functions with interpolate
-- Uses separate value column for interpolate to avoid collision with min(t).
CREATE TABLE gf_win_interp (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_interp VALUES (1, 10, 1), (3, 30, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t) AS "min",
    lag(min(t)) OVER () AS lag_min,
    lead(min(t)) OVER () AS lead_min,
    time_series.interpolate(min(value)) AS interpolate,
    lag(time_series.interpolate(min(value))) OVER () AS lag_interpolate,
    lead(time_series.interpolate(min(value))) OVER () AS lead_interpolate
FROM gf_win_interp WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_interp;

-- 53f. Window functions with expressions
-- Uses separate value column for interpolate to avoid collision with min(t).
CREATE TABLE gf_win_expr (t INT NOT NULL, value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_expr VALUES (1, 10, 1), (3, 30, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    min(t) AS "min",
    lag(min(t)) OVER () AS lag_min,
    1 + lag(min(t)) OVER () AS lag_min_plus1,
    time_series.interpolate(min(value)) AS interpolate,
    lag(time_series.interpolate(min(value))) OVER () AS lag_interpolate,
    1 + lag(time_series.interpolate(min(value))) OVER () AS lag_interpolate_plus1
FROM gf_win_expr WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_expr;

-- ============================================================
-- Section 54: row_number/rank/ntile window functions
-- Tests ntile(2/3/5), row_number(), rank() with and without
-- ORDER BY applied to gapfilled results.
-- ============================================================

CREATE TABLE gf_win_rank (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_win_rank VALUES (1, 1), (3, 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    ntile(2) OVER () AS ntile_2,
    ntile(3) OVER () AS ntile_3,
    ntile(5) OVER () AS ntile_5,
    row_number() OVER () AS rn,
    rank() OVER () AS rank_noorder,
    rank() OVER (ORDER BY time_series.time_bucket_gapfill(1, t, 1, 6)) AS rank_ordered
FROM gf_win_rank WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_win_rank;


-- ============================================================
-- Section 57: ORDER BY interpolate variants
-- Tests ORDER BY with interpolate column in various positions:
-- - ORDER BY bucket, interpolate
-- - ORDER BY interpolate NULLS FIRST
-- - ORDER BY interpolate NULLS LAST
-- ============================================================

-- Reconnect to reset session state
\c :DBNAME
SET optimizer = off;
SET timezone = 'PST8PDT';

CREATE TABLE gf_order_interp (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_order_interp VALUES (2, 1), (3, 1);

-- ORDER BY bucket, interpolate
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.interpolate(min(t)) AS interp_val
FROM gf_order_interp WHERE grp = 1
GROUP BY 1 ORDER BY 1, 2;

-- ORDER BY interpolate NULLS FIRST
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.interpolate(min(t)) AS interp_val
FROM gf_order_interp WHERE grp = 1
GROUP BY 1 ORDER BY 2 NULLS FIRST, 1;

-- ORDER BY interpolate NULLS LAST
SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS bucket,
    time_series.interpolate(min(t)) AS interp_val
FROM gf_order_interp WHERE grp = 1
GROUP BY 1 ORDER BY 2 NULLS LAST, 1;

DROP TABLE gf_order_interp;

-- ============================================================
-- Section 58: Interpolate with multiple groupings
-- Tests interpolate() with GROUP BY device + bucket.
-- Two devices with data at t=0 and t=10, verifies per-group interpolation
-- fills the gap at t=5 independently for each device.
-- ============================================================

CREATE TABLE gf_interp_multigrp (t INT NOT NULL, device INT NOT NULL, v1 INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_interp_multigrp VALUES
    (0, 1, 0,   1), (10, 1, 100, 1),
    (0, 2, 50,  1), (10, 2, 150, 1);

SELECT
    time_series.time_bucket_gapfill(5, t, 0, 15) AS bucket,
    device,
    time_series.interpolate(min(v1)) AS interp
FROM gf_interp_multigrp WHERE grp = 1
GROUP BY 1, 2 ORDER BY 2, 1;

DROP TABLE gf_interp_multigrp;

-- ============================================================
-- Section 59: DST switching gapfill tests
-- Tests gapfill across daylight saving time transitions:
-- - Spring forward (Europe/Berlin, 2024-03-31): 1h and 30min buckets
-- - Fall back (Europe/Berlin, 2024-10-27): repeated hour handling
-- ============================================================

-- Spring forward (Europe/Berlin): clocks go from 01:59 CET to 03:00 CEST
-- on 2024-03-31. A 1h gapfill should handle the 2-hour jump correctly.
SET timezone TO 'Europe/Berlin';

SELECT time_series.time_bucket_gapfill('1h'::interval, ts, 'Europe/Berlin',
    '2024-03-31 00:00:00+01'::timestamptz,
    '2024-03-31 04:00:00+02'::timestamptz) AS bucket,
    min(val) AS val
FROM (SELECT NULL::timestamptz AS ts, NULL::float8 AS val LIMIT 0) s
GROUP BY 1 ORDER BY 1;

-- 30-minute buckets across spring forward
SELECT time_series.time_bucket_gapfill('30 minutes'::interval, ts, 'Europe/Berlin',
    '2024-03-31 00:00:00+01'::timestamptz,
    '2024-03-31 04:00:00+02'::timestamptz) AS bucket,
    min(val) AS val
FROM (SELECT NULL::timestamptz AS ts, NULL::float8 AS val LIMIT 0) s
GROUP BY 1 ORDER BY 1;

-- Fall back (Europe/Berlin): clocks go from 02:59 CEST to 02:00 CET
-- on 2024-10-27. A 1h gapfill should handle the repeated hour.
SELECT time_series.time_bucket_gapfill('1h'::interval, ts, 'Europe/Berlin',
    '2024-10-26 23:00:00+02'::timestamptz,
    '2024-10-27 03:00:00+01'::timestamptz) AS bucket,
    min(val) AS val
FROM (SELECT NULL::timestamptz AS ts, NULL::float8 AS val LIMIT 0) s
GROUP BY 1 ORDER BY 1;

RESET timezone;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 60: LOCF with different datatypes (large text / TOAST)
-- Tests locf() with TOAST-eligible large text values (2048+ bytes)
-- and INT arrays.
-- ============================================================

CREATE TABLE gf_locf_toast (
    t INT NOT NULL,
    v1 TEXT,
    v2 INT[],
    grp INT NOT NULL
) DISTRIBUTED BY (grp);

INSERT INTO gf_locf_toast VALUES
    (1, 'foo', ARRAY[1,2,3], 1),
    (3, 'bar', ARRAY[3,4,5], 1);

SELECT
    time_series.time_bucket_gapfill(1, t, 1, 6) AS time,
    time_series.locf(min(v1)) AS "text",
    time_series.locf(min(v2)) AS "int[]"
FROM gf_locf_toast WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_locf_toast;


-- ============================================================
-- Section 62: Array type as group key with gapfill (start=0)
-- Tests INT[] array column as GROUP BY key alongside gapfill.
-- Verifies locf() works with array group keys.
-- Also exercises the start=0 edge case (not a sentinel).
-- ============================================================

CREATE TABLE gf_array_grp (ts INT NOT NULL, int_arr INT[], value INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_array_grp VALUES (0, ARRAY[1,2,3,4], 500001, 1);

SELECT
    time_series.time_bucket_gapfill(5, ts, 0, 15) AS ts,
    int_arr,
    time_series.locf(min(value)) AS locf_value
FROM gf_array_grp WHERE grp = 1
GROUP BY 1, 2 ORDER BY 1;

DROP TABLE gf_array_grp;

-- ============================================================
-- Section 63: Month interval gapfill with LOCF + interpolate together
-- Tests both locf() and interpolate() in same query with month buckets.
-- Data at Jan and Apr, gaps at Feb/Mar filled by both strategies.
-- Uses separate columns (value for locf, value2 for interpolate)
-- to avoid same-aggregate collision on the underlying sub-column.
-- ============================================================

CREATE TABLE gf_month_combo (
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8,
    value2 FLOAT8
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO gf_month_combo VALUES
    ('2020-01-01 00:00:00+00', 1, 10.0, 10.0),
    ('2020-04-01 00:00:00+00', 1, 40.0, 40.0);

SELECT
    time_series.time_bucket_gapfill('1 month'::interval, ts,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-06-01 00:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_val,
    time_series.interpolate(avg(value2)) AS interp_val
FROM gf_month_combo
WHERE device_id = 1
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_combo;

-- ============================================================
-- Section 64: Year interval gapfill (epoch boundary)
-- Tests 1-year interval gapfill. Data at 2000-01-01 UTC (PG epoch)
-- with range 2000..2003 UTC — verifies year-level gap filling works
-- and that the epoch timestamp (internal value 0) is handled correctly.
-- ============================================================

-- 1-year gapfill.  Data starts at year 2000; origin must precede it.
-- Use 1-month chunks so a 1900-2099 span stays well under UINT16_MAX.
CREATE TABLE gf_year (ts TIMESTAMPTZ NOT NULL, grp INT NOT NULL, value FLOAT8) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '1900-01-01'
) DISTRIBUTED BY (grp);
INSERT INTO gf_year VALUES ('2000-01-01 00:00:00+00'::timestamptz, 1, 100.0);

SELECT
    time_series.time_bucket_gapfill('1 year'::interval, ts,
        '2000-01-01 00:00:00+00'::timestamptz, '2003-01-01 00:00:00+00'::timestamptz) AS bucket,
    min(value)
FROM gf_year WHERE grp = 1
GROUP BY 1 ORDER BY 1;

DROP TABLE gf_year;

-- ============================================================
-- Section 64b: Boundary tests for start=0 (all integer types)
-- Verifies that start=0 is a valid boundary for INT2, INT4, INT8.
-- Previously, 0 was used as "not set" sentinel — this section
-- ensures the fix works correctly for all integer widths.
-- ============================================================

-- INT4 start=0
CREATE TABLE gf_start0_int4 (t INT NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_start0_int4 VALUES (0, 1), (4, 1);
SELECT
    time_series.time_bucket_gapfill(2, t, 0, 6) AS bucket,
    min(t)
FROM gf_start0_int4 WHERE grp = 1
GROUP BY 1 ORDER BY 1;
DROP TABLE gf_start0_int4;

-- INT2 start=0 (cast bucket_width and range to smallint)
CREATE TABLE gf_start0_int2 (t INT2 NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_start0_int2 VALUES (0::smallint, 1), (4::smallint, 1);
SELECT
    time_series.time_bucket_gapfill(2::smallint, t, 0::smallint, 6::smallint) AS bucket,
    min(t)
FROM gf_start0_int2 WHERE grp = 1
GROUP BY 1 ORDER BY 1;
DROP TABLE gf_start0_int2;

-- INT8 start=0
CREATE TABLE gf_start0_int8 (t INT8 NOT NULL, grp INT NOT NULL) DISTRIBUTED BY (grp);
INSERT INTO gf_start0_int8 VALUES (0::bigint, 1), (4::bigint, 1);
SELECT
    time_series.time_bucket_gapfill(2::bigint, t, 0::bigint, 6::bigint) AS bucket,
    min(t)
FROM gf_start0_int8 WHERE grp = 1
GROUP BY 1 ORDER BY 1;
DROP TABLE gf_start0_int8;

-- TIMESTAMPTZ at PG epoch (2000-01-01 00:00:00+00 = internal 0).
-- Origin must precede 2000; use 1900 to leave headroom.
CREATE TABLE gf_start0_tstz (ts TIMESTAMPTZ NOT NULL, grp INT NOT NULL) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '1900-01-01'
) DISTRIBUTED BY (grp);
INSERT INTO gf_start0_tstz VALUES ('2000-01-01 00:00:00+00', 1), ('2000-01-01 02:00:00+00', 1);
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, ts,
        '2000-01-01 00:00:00+00'::timestamptz,
        '2000-01-01 03:00:00+00'::timestamptz) AS bucket,
    min(ts)
FROM gf_start0_tstz WHERE grp = 1
GROUP BY 1 ORDER BY 1;
DROP TABLE gf_start0_tstz;

-- ============================================================
-- Section 65: Month gapfill with different timezone settings
-- Tests 2-month gapfill with various timezone parameters:
-- - 'UTC' explicit timezone literal
-- - current_setting('timezone') — works because PG constant-folds
--   it into a Const TEXT at plan time; does NOT prove that arbitrary
--   non-Const expressions are supported as timezone parameter
-- - 'Europe/Berlin' timezone with SET timezone
-- ============================================================

-- Year-2000 data: origin must precede 2000; 1900 + 1-month chunk
-- keeps the chunk count under UINT16_MAX.
CREATE TABLE gf_month_tz2 (ts TIMESTAMPTZ NOT NULL, grp INT NOT NULL, value FLOAT8) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '1900-01-01'
) DISTRIBUTED BY (grp);
INSERT INTO gf_month_tz2 VALUES ('2000-03-01'::timestamptz, 1, 100.0);

-- With 'UTC' timezone
SELECT
    time_series.time_bucket_gapfill('2 month'::interval, ts, 'UTC',
        '2000-01-01'::timestamptz, '2001-01-01'::timestamptz) AS bucket,
    min(value)
FROM gf_month_tz2 WHERE grp = 1
GROUP BY 1 ORDER BY 1;

-- With current_setting('timezone') — PG folds this to Const at plan time
SELECT
    time_series.time_bucket_gapfill('2 month'::interval, ts, current_setting('timezone'),
        '2000-01-01'::timestamptz, '2001-01-01'::timestamptz) AS bucket,
    min(value)
FROM gf_month_tz2 WHERE grp = 1
GROUP BY 1 ORDER BY 1;

-- With Europe/Berlin
SET timezone TO 'Europe/Berlin';
SELECT
    time_series.time_bucket_gapfill('2 month'::interval, ts, 'Europe/Berlin',
        '2000-01-01'::timestamptz, '2001-01-01'::timestamptz) AS bucket,
    min(value)
FROM gf_month_tz2 WHERE grp = 1
GROUP BY 1 ORDER BY 1;
RESET timezone;

DROP TABLE gf_month_tz2;

-- Reconnect to reset session state before MPP tests
\c :DBNAME
SET optimizer = off;
SET timezone = 'PST8PDT';

-- ============================================================
-- Section 66: [MPP] Multi-group GapFill — 3 devices
-- Group-aware gapfill across 3 devices on different segments.
-- device_id is propagated to gap rows.
-- ============================================================

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2, 3)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 67: [MPP] Multi-group + LOCF — 4 devices
-- LOCF state must reset when group changes.
-- Covers dense (dev 4), sparse (dev 3), and mid-density devices.
-- ============================================================

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(value)) AS locf_value
FROM gapfill_test
WHERE device_id IN (1, 3, 4, 7)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 68: [MPP] Multi-group + Interpolate — 4 devices
-- Linear interpolation across 4 devices with diverse patterns.
-- Device 4 (dense, all hours): no interpolation needed.
-- Device 5 (endpoints only): linear interpolation across 4 gaps.
-- Device 7 (alternating): interpolation at odd hours.
-- Device 1 (standard): interpolation at hours 2,4.
-- NOTE: WHERE device_id IN (...) required in MPP — without it,
-- all segments scan the table and produce duplicate gap rows.
-- ============================================================

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.interpolate(avg(value)) AS interp_value
FROM gapfill_test
WHERE device_id IN (1, 4, 5, 7)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 70: [MPP] CTE + GapFill across segments
-- GapFill inside CTE with LOCF, queried from outside.
-- Tests that Custom Scan (GapFill) works correctly when
-- dispatched to segments via CTE.
-- Uses single-device query (multi-device CTE has MPP limitations
-- with gap row device_id propagation).
-- Device 6: 5 data points, gap at hour 3 → LOCF carries 60200.
-- ============================================================

WITH filled AS (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
           time_series.locf(avg(value)) AS locf_value
    FROM gapfill_test
    WHERE device_id = 6
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT bucket, COALESCE(locf_value, 0) AS val
FROM filled
ORDER BY bucket;

-- ============================================================
-- Section 71: [MPP] JOIN two gapfilled subqueries
-- Cross-slice query: GapFill Custom Scan dispatched to segments.
-- Joins dense device 4 with sparse device 7.
-- ============================================================

SELECT d4.bucket, d4.avg_value AS dev4_val, d7.avg_value AS dev7_val
FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 4
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
) d4
JOIN (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 7
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
) d7 ON d4.bucket = d7.bucket
ORDER BY d4.bucket;

-- ============================================================
-- Section 72: [MPP] INSERT INTO SELECT (cross-slice)
-- GapFill + Redistribute Motion to target table segments.
-- Uses single-device query (multi-device INSERT INTO SELECT
-- has MPP limitations with gap row group-key propagation).
-- Device 7: alternating hours 0,2,4 → gaps at 1,3,5.
-- ============================================================

CREATE TABLE gf_mpp_result (bucket TIMESTAMPTZ, avg_value FLOAT8) USING time_series WITH (
    ts_partition_column = 'bucket',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
)
    DISTRIBUTED BY (bucket);

INSERT INTO gf_mpp_result
SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
           '2024-01-01 00:00:00+00'::timestamptz,
           '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
       avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 7
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket;

SELECT * FROM gf_mpp_result ORDER BY bucket;

DROP TABLE gf_mpp_result;

-- ============================================================
-- Section 73: [MPP] Sort order fix — GROUP BY bucket, device_id
-- Tests planner auto-fix: when bucket comes before device_id in
-- GROUP BY and no ORDER BY is specified, planner inserts Sort to
-- guarantee (device_id, bucket ASC) order. Without this fix,
-- GapFill executor would skip rows (data loss).
-- ============================================================

-- 73a. Multi-device gapfill with bucket-first GROUP BY
-- Bucket in GROUP BY comes first. Planner must auto-fix sort.
-- Result should have all 3 devices * 4 buckets = 12 rows.
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2, 3)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- 73b. LOCF with bucket-first GROUP BY
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(value)) AS locf_value
FROM gapfill_test
WHERE device_id IN (1, 3)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 74: [MPP] Distribution key mismatch detection
-- Tests that a clear ERROR is raised when the table's
-- DISTRIBUTED BY key does not match the GROUP BY columns,
-- which would cause GapFill to produce incorrect results.
-- ============================================================

-- 74a. Create table with mismatched distribution key
CREATE TABLE gf_distkey_mismatch (
    time TIMESTAMPTZ NOT NULL,
    sensor_type INT NOT NULL,
    region_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (sensor_type);

INSERT INTO gf_distkey_mismatch VALUES
    ('2024-01-01 00:00:00+00', 1, 10, 1.0),
    ('2024-01-01 01:00:00+00', 2, 10, 2.0),
    ('2024-01-01 00:00:00+00', 1, 20, 10.0),
    ('2024-01-01 02:00:00+00', 2, 20, 30.0);

\set ON_ERROR_STOP 0

-- 74b. GapFill with mismatched distribution key should ERROR
-- DISTRIBUTED BY (sensor_type) but GROUP BY (region_id, bucket)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    region_id,
    avg(value) AS avg_value
FROM gf_distkey_mismatch
WHERE time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY region_id, bucket
ORDER BY region_id, bucket;

-- 74c. Single-group query (only bucket in GROUP BY, no group key)
-- Should ERROR: data is DISTRIBUTED BY (sensor_type) — each segment has
-- an incomplete time range, so GapFill would produce spurious gap rows
-- on segments missing data for certain time buckets.
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_distkey_mismatch
WHERE time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 74d. Same table, but filter to single region_id — still ERROR
-- because planner still redistributes by hash(region_id, bucket)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    region_id,
    avg(value) AS avg_value
FROM gf_distkey_mismatch
WHERE region_id = 10
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY region_id, bucket
ORDER BY bucket;

\set ON_ERROR_STOP 1

DROP TABLE gf_distkey_mismatch;

-- 74e. Verify correct distribution key does NOT error
-- gapfill_test is DISTRIBUTED BY (device_id) and GROUP BY (device_id, bucket)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY device_id, bucket
ORDER BY device_id, bucket;

-- ============================================================
-- Section 75: [MPP] HAVING clause with GapFill
-- Tests interaction between HAVING and gap-filled rows.
-- HAVING filters after aggregation but before GapFill gap insertion,
-- so gap rows (with NULL aggregates) are NOT filtered by HAVING.
-- ============================================================

-- 75a. HAVING avg(value) > 5 — gap rows should still appear (NULL avg)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
HAVING avg(value) > 5
ORDER BY bucket;

-- 75b. HAVING with multi-device grouping
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY device_id, bucket
HAVING avg(value) > 50
ORDER BY device_id, bucket;

-- 75c. HAVING count(*) > 0 — gap rows are NOT filtered because they are
-- inserted by GapFill AFTER GroupAggregate (and HAVING). Gap rows have
-- NULL count (not 0), since they bypass aggregation entirely.
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    count(*) AS cnt,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
HAVING count(*) > 0
ORDER BY bucket;

-- ============================================================
-- Section 76: [MPP] LIMIT and OFFSET with GapFill
-- Tests that LIMIT/OFFSET correctly truncate gap-filled results.
-- ============================================================

-- 76a. LIMIT 3 on gap-filled result
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket
LIMIT 3;

-- 76b. OFFSET 2 LIMIT 3 — skip first 2 gap-filled rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket
OFFSET 2 LIMIT 3;

-- 76c. LIMIT with multi-device grouping
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gapfill_test
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY device_id, bucket
ORDER BY device_id, bucket
LIMIT 5;

-- ============================================================
-- Section 77: [MPP] DISTRIBUTED REPLICATED table with GapFill
-- Tests gapfill with a replicated table. In CBDB, replicated tables
-- exist on all segments. GapFill should work correctly because the
-- planner uses Gather Motion to collect all data to coordinator.
-- ============================================================

CREATE TABLE gf_replicated (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED REPLICATED;

INSERT INTO gf_replicated VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, 20.0),
    ('2024-01-01 00:00:00+00', 2, 100.0),
    ('2024-01-01 02:00:00+00', 2, 300.0);

-- 77a. Single-device gapfill on replicated table
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_replicated
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 77b. Multi-device gapfill on replicated table
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gf_replicated
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY device_id, bucket
ORDER BY device_id, bucket;

-- 77c. LOCF on replicated table
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(value)) AS locf_value
FROM gf_replicated
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY device_id, bucket
ORDER BY device_id, bucket;

DROP TABLE gf_replicated;

-- ============================================================
-- Section 78: Independent oracle — generate_series + LEFT JOIN + EXCEPT
-- Validates gapfill results against an independent oracle built
-- from generate_series (expected buckets) LEFT JOIN raw aggregated data.
-- If GapFill Custom Scan is not activated or produces wrong rows,
-- the EXCEPT queries will return differences.
-- ============================================================

-- 78a. Basic gapfill oracle (Device 1: gaps at h2, h4)
-- Oracle: generate all 6 hourly buckets, LEFT JOIN actual data
WITH oracle AS (
    SELECT
        b.bucket,
        a.avg_value
    FROM generate_series(
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz,
        '1 hour'::interval) AS b(bucket)
    LEFT JOIN (
        SELECT time_series.time_bucket('1 hour'::interval, time) AS bucket,
               avg(value) AS avg_value
        FROM gapfill_test
        WHERE device_id = 1
          AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
        GROUP BY 1
    ) a ON a.bucket = b.bucket
),
gapfilled AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT 'oracle_minus_gapfill' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM oracle EXCEPT SELECT * FROM gapfilled) t
UNION ALL
SELECT 'gapfill_minus_oracle', count(*)
FROM (SELECT * FROM gapfilled EXCEPT SELECT * FROM oracle) t;

-- 78b. LOCF oracle (Device 1: locf should carry forward across gaps)
-- Oracle computes LOCF manually using a window function over the oracle base
WITH oracle_base AS (
    SELECT
        b.bucket,
        a.avg_value
    FROM generate_series(
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz,
        '1 hour'::interval) AS b(bucket)
    LEFT JOIN (
        SELECT time_series.time_bucket('1 hour'::interval, time) AS bucket,
               avg(value) AS avg_value
        FROM gapfill_test
        WHERE device_id = 1
          AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
        GROUP BY 1
    ) a ON a.bucket = b.bucket
),
oracle_locf AS (
    SELECT bucket,
           COALESCE(avg_value,
               (SELECT o2.avg_value FROM oracle_base o2
                WHERE o2.bucket < oracle_base.bucket AND o2.avg_value IS NOT NULL
                ORDER BY o2.bucket DESC LIMIT 1)
           ) AS locf_value
    FROM oracle_base
),
gapfilled_locf AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        time_series.locf(avg(value)) AS locf_value
    FROM gapfill_test
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT 'oracle_minus_gapfill' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM oracle_locf EXCEPT SELECT * FROM gapfilled_locf) t
UNION ALL
SELECT 'gapfill_minus_oracle', count(*)
FROM (SELECT * FROM gapfilled_locf EXCEPT SELECT * FROM oracle_locf) t;

-- 78c. Interpolate oracle (Device 5: endpoints only, h0=50000, h5=55000)
-- Linear interpolation: value_at_h = 50000 + (h * 1000)
WITH oracle_interp AS (
    SELECT
        b.bucket,
        50000.0 + (EXTRACT(EPOCH FROM b.bucket - '2024-01-01 00:00:00+00'::timestamptz) / 3600.0) * 1000.0 AS interp_value
    FROM generate_series(
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 05:00:00+00'::timestamptz,
        '1 hour'::interval) AS b(bucket)
),
gapfilled_interp AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        time_series.interpolate(avg(value)) AS interp_value
    FROM gapfill_test
    WHERE device_id = 5
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT 'oracle_minus_gapfill' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM oracle_interp EXCEPT SELECT * FROM gapfilled_interp) t
UNION ALL
SELECT 'gapfill_minus_oracle', count(*)
FROM (SELECT * FROM gapfilled_interp EXCEPT SELECT * FROM oracle_interp) t;

-- ============================================================
-- Section 79: No-gap data completeness — gapfill adds no extra rows
-- Device 4 has all 6 hours (0-5), no gaps. GapFill should return
-- exactly the same rows as a plain GROUP BY, no synthesized rows.
-- ============================================================

-- 79a. Row count: gapfill should return exactly 6 rows
SELECT count(*) AS gapfill_rows FROM (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 4
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
) t;

-- 79b. EXCEPT: gapfill output == plain GROUP BY output
WITH plain AS (
    SELECT time_series.time_bucket('1 hour'::interval, time) AS bucket,
           avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 4
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY 1
),
gapfilled AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
        avg(value) AS avg_value
    FROM gapfill_test
    WHERE device_id = 4
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
    GROUP BY bucket
)
SELECT 'plain_minus_gapfill' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM plain EXCEPT SELECT * FROM gapfilled) t
UNION ALL
SELECT 'gapfill_minus_plain', count(*)
FROM (SELECT * FROM gapfilled EXCEPT SELECT * FROM plain) t;

-- ============================================================
-- Section 80: Interpolation boundary — only first or last data point
-- Tests interpolate behavior when there is no "other endpoint":
--   80a. Only first point (h0) → all subsequent gaps have no next → NULL
--   80b. Only last point (h5) → all preceding gaps have no prev → NULL
--   80c. Single mid-point (Device 8, h3) → both sides NULL
-- ============================================================

CREATE TABLE gf_interp_boundary (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

-- Device A: only h0
INSERT INTO gf_interp_boundary VALUES
    ('2024-01-01 00:00:00+00', 1, 100.0);

-- Device B: only h5
INSERT INTO gf_interp_boundary VALUES
    ('2024-01-01 05:00:00+00', 2, 500.0);

-- 80a. Only first point — interpolate returns NULL for all gap rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_interp_boundary
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 80b. Only last point — interpolate returns NULL for all gap rows before it
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_interp_boundary
WHERE device_id = 2
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 80c. Single mid-point (Device 8, h3) — NULL on both sides
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 06:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gapfill_test
WHERE device_id = 8
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 06:00:00+00'
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_interp_boundary;

-- ============================================================
-- Section 81: Month-interval gapfill Oracle verification
-- advance_month_usec() is custom code (not from upstream),
-- so month boundary handling (leap year, month-end) needs
-- independent Oracle verification via generate_series + EXCEPT.
-- ============================================================

-- chunk_interval = '1 month' to match the 1-month bucket queries below
-- (see comment on gf_month_tstz for the same reasoning).
CREATE TABLE gf_month_oracle (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT8,
    grp INT NOT NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (grp);

-- Data at Jan, Mar, Jun 2020 (gaps at Feb, Apr, May)
INSERT INTO gf_month_oracle VALUES
    ('2020-01-01 00:00:00+00', 100.0, 1),
    ('2020-03-01 00:00:00+00', 300.0, 1),
    ('2020-06-01 00:00:00+00', 600.0, 1);

-- 81a. Month gapfill — verify row count and data presence
-- NOTE: generate_series('...', '...', '1 month') does NOT align to month
-- boundaries the same way time_bucket does (generate_series drifts after
-- months of different lengths), so we use direct output verification here.
-- gapfill should produce 6 monthly buckets (Jan-Jun 2020)
-- Data at Jan, Mar, Jun; gaps at Feb, Apr, May
SELECT
    time_series.time_bucket_gapfill('1 month'::interval, time,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-07-01 00:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_month_oracle
WHERE grp = 1
  AND time >= '2020-01-01 00:00:00+00' AND time < '2020-07-01 00:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 81b. Month LOCF — carry forward across month gaps
-- Jan=100, Feb=100(locf), Mar=300, Apr=300(locf), May=300(locf), Jun=600
SELECT
    time_series.time_bucket_gapfill('1 month'::interval, time,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-07-01 00:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gf_month_oracle
WHERE grp = 1
  AND time >= '2020-01-01 00:00:00+00' AND time < '2020-07-01 00:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 81c. Month interpolate — linear interpolation by timestamp microseconds
-- Feb between Jan(100) and Mar(300): not exactly 200 due to different month lengths
-- Apr,May between Mar(300) and Jun(600): proportional to actual time span
SELECT
    time_series.time_bucket_gapfill('1 month'::interval, time,
        '2020-01-01 00:00:00+00'::timestamptz,
        '2020-07-01 00:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_month_oracle
WHERE grp = 1
  AND time >= '2020-01-01 00:00:00+00' AND time < '2020-07-01 00:00:00+00'
GROUP BY bucket
ORDER BY bucket;

DROP TABLE gf_month_oracle;

-- ============================================================
-- Cleanup (basic gapfill tests)
-- ============================================================

DROP TABLE gapfill_test;

-- ============================================================
-- CAGG + Gapfill tests
-- Test: Gapfill functions (time_bucket_gapfill, locf, interpolate)
--       applied to Continuous Aggregate views.
--
-- Common user pattern: create a CAGG for fast aggregation,
-- then query it with gapfill to fill gaps for dashboards.
--
-- Covers:
--   1. Basic gapfill on CAGG (real-time mode)
--   2. Gapfill on CAGG (materialized_only mode)
--   3. LOCF on CAGG
--   4. Interpolate on CAGG
--   5. Gapfill spanning mat/live boundary
--   6. Multi-device gapfill on CAGG
--   7. Gapfill with CTE wrapping CAGG
--   8. Oracle verification (gapfill on CAGG = gapfill on source)
-- ============================================================

-- Setup: source table with intentional gaps
-- Device 1: data at hours 0,1,2, gap at 3,4, data at 5,6,7
-- Device 2: data at hours 0,1, gap at 2,3,4,5, data at 6,7
CREATE TABLE gf_src (
    time        TIMESTAMPTZ NOT NULL,
    device_id   INT         NOT NULL,
    temperature DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2019-01-01'
) DISTRIBUTED BY (device_id);

-- Device 1: hours 0,1,2,5,6,7 (gap at 3,4)
INSERT INTO gf_src VALUES
    ('2024-01-01 00:30+00', 1, 10.0),
    ('2024-01-01 01:30+00', 1, 12.0),
    ('2024-01-01 02:30+00', 1, 14.0),
    ('2024-01-01 05:30+00', 1, 20.0),
    ('2024-01-01 06:30+00', 1, 22.0),
    ('2024-01-01 07:30+00', 1, 24.0);

-- Device 2: hours 0,1,6,7 (gap at 2,3,4,5)
INSERT INTO gf_src VALUES
    ('2024-01-01 00:30+00', 2, 50.0),
    ('2024-01-01 01:30+00', 2, 52.0),
    ('2024-01-01 06:30+00', 2, 62.0),
    ('2024-01-01 07:30+00', 2, 64.0);

-- Sentinel row in bucket 08:00 so the hot bucket sits AT 08:00
-- and the test range [00:00, 08:00) lies entirely in the stable
-- region.  Without this, max(time) = 07:30 makes bucket 07:00 the
-- hot bucket, which is excluded from mat by the hot-bucket-
-- exclusion fix and would surface as NULL under materialized_only
-- (Test 91 below).  The sentinel is on a separate device_id (3)
-- so it doesn't perturb device_id IN (1, 2) result columns.
INSERT INTO gf_src VALUES ('2024-01-01 08:30+00', 3, 0.0);

-- CAGG: hourly aggregation
-- Put time_series on search_path so the rest of this section
-- can use unqualified time_bucket / time_bucket_gapfill calls.
SET search_path TO public, time_series;

CREATE MATERIALIZED VIEW gf_hourly
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         device_id,
         avg(temperature) AS avg_temp,
         count(*) AS n
  FROM gf_src
  GROUP BY bucket, device_id;

CALL time_series.refresh_continuous_aggregate('gf_hourly', NULL, NULL);

-- ============================================================
-- 90. Basic gapfill on CAGG (real-time mode, default)
--     Gap rows should have NULL for avg_temp.
-- ============================================================
\echo '=== 90: Basic gapfill on CAGG ==='
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 00:00+00'::timestamptz,
        '2024-01-01 08:00+00'::timestamptz) AS bucket,
    device_id,
    avg(avg_temp) AS avg_temp
FROM gf_hourly
WHERE device_id = 1
  AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
GROUP BY 1, device_id
ORDER BY 1;

-- ============================================================
-- 91. Gapfill on CAGG (materialized_only mode)
-- ============================================================
\echo '=== 91: Gapfill materialized_only ==='
ALTER VIEW gf_hourly SET (time_series.materialized_only = true);
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 00:00+00'::timestamptz,
        '2024-01-01 08:00+00'::timestamptz) AS bucket,
    device_id,
    avg(avg_temp) AS avg_temp
FROM gf_hourly
WHERE device_id = 1
  AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
GROUP BY 1, device_id
ORDER BY 1;
ALTER VIEW gf_hourly SET (time_series.materialized_only = false);

-- ============================================================
-- 92. LOCF on CAGG — carry forward last known value
-- ============================================================
\echo '=== 92: LOCF on CAGG ==='
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 00:00+00'::timestamptz,
        '2024-01-01 08:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(avg_temp)) AS locf_temp
FROM gf_hourly
WHERE device_id = 1
  AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
GROUP BY 1, device_id
ORDER BY 1;

-- ============================================================
-- 93. Interpolate on CAGG — linear interpolation across gaps
-- ============================================================
\echo '=== 93: Interpolate on CAGG ==='
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 00:00+00'::timestamptz,
        '2024-01-01 08:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.interpolate(avg(avg_temp)) AS interp_temp
FROM gf_hourly
WHERE device_id = 1
  AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
GROUP BY 1, device_id
ORDER BY 1;

-- ============================================================
-- 94. Gapfill spanning mat/live boundary
--     Insert data beyond watermark (not refreshed), gapfill
--     should show data from both mat and live branches.
-- ============================================================
\echo '=== 94: Gapfill across mat/live boundary ==='
INSERT INTO gf_src VALUES ('2024-01-01 10:30+00', 1, 30.0);

SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 06:00+00'::timestamptz,
        '2024-01-01 12:00+00'::timestamptz) AS bucket,
    device_id,
    avg(avg_temp) AS avg_temp
FROM gf_hourly
WHERE device_id = 1
  AND bucket >= '2024-01-01 06:00+00' AND bucket < '2024-01-01 12:00+00'
GROUP BY 1, device_id
ORDER BY 1;

-- (originally DELETE FROM gf_src WHERE time = '2024-01-01 10:30+00';
-- removed: hypertable is append-only.  The remaining row at 10:30
-- belongs to device_id=1 and only affects subsequent queries that
-- both reference device 1 AND span hour 10 — none of the following
-- sections do, so this is inert.)

-- ============================================================
-- 95. Multi-device gapfill on CAGG
--     Each device has different gap patterns.
-- ============================================================
\echo '=== 95: Multi-device gapfill ==='
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, bucket,
        '2024-01-01 00:00+00'::timestamptz,
        '2024-01-01 08:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(avg_temp)) AS locf_temp
FROM gf_hourly
WHERE bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
GROUP BY 1, device_id
ORDER BY device_id, 1;

-- ============================================================
-- 96. CTE wrapping CAGG with gapfill + COALESCE
-- ============================================================
\echo '=== 96: CTE + gapfill on CAGG ==='
WITH filled AS (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, bucket,
            '2024-01-01 00:00+00'::timestamptz,
            '2024-01-01 08:00+00'::timestamptz) AS bucket,
        device_id,
        time_series.locf(avg(avg_temp)) AS filled_temp
    FROM gf_hourly
    WHERE device_id = 1
      AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
    GROUP BY 1, device_id
)
SELECT bucket, COALESCE(filled_temp, -1) AS temp
FROM filled
ORDER BY bucket;

-- ============================================================
-- 97. Oracle verification: gapfill on CAGG = gapfill on source
--     Both must produce identical results (symmetric EXCEPT = 0).
-- ============================================================
\echo '=== 97: Oracle verification ==='
SELECT count(*) AS diff_gapfill FROM (
  (SELECT
      time_series.time_bucket_gapfill('1 hour'::interval, bucket,
          '2024-01-01 00:00+00'::timestamptz,
          '2024-01-01 08:00+00'::timestamptz) AS bucket,
      device_id,
      avg(avg_temp) AS avg_temp
  FROM gf_hourly
  WHERE device_id = 1
    AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
  GROUP BY 1, device_id
  EXCEPT
  SELECT
      time_series.time_bucket_gapfill('1 hour'::interval,
          time_series.time_bucket('1 hour'::interval, time),
          '2024-01-01 00:00+00'::timestamptz,
          '2024-01-01 08:00+00'::timestamptz) AS bucket,
      device_id,
      avg(temperature) AS avg_temp
  FROM gf_src
  WHERE device_id = 1
    AND time >= '2024-01-01 00:00+00' AND time < '2024-01-01 08:00+00'
  GROUP BY 1, device_id)
  UNION ALL
  (SELECT
      time_series.time_bucket_gapfill('1 hour'::interval,
          time_series.time_bucket('1 hour'::interval, time),
          '2024-01-01 00:00+00'::timestamptz,
          '2024-01-01 08:00+00'::timestamptz) AS bucket,
      device_id,
      avg(temperature) AS avg_temp
  FROM gf_src
  WHERE device_id = 1
    AND time >= '2024-01-01 00:00+00' AND time < '2024-01-01 08:00+00'
  GROUP BY 1, device_id
  EXCEPT
  SELECT
      time_series.time_bucket_gapfill('1 hour'::interval, bucket,
          '2024-01-01 00:00+00'::timestamptz,
          '2024-01-01 08:00+00'::timestamptz) AS bucket,
      device_id,
      avg(avg_temp) AS avg_temp
  FROM gf_hourly
  WHERE device_id = 1
    AND bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 08:00+00'
  GROUP BY 1, device_id)
) x;

-- ============================================================
-- Cleanup (CAGG gapfill tests)
-- ============================================================
DROP TABLE gf_src CASCADE;

-- ============================================================
-- Final cleanup
-- ============================================================
RESET optimizer;
DROP EXTENSION time_series CASCADE;
