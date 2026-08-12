-- Regression tests for time_series extension: GapFill MPP behavior
-- Focus: multi-device distribution across segments, group-aware gapfill,
--        EXPLAIN plans showing Gather Motion + GapFill interaction,
--        and ORCA auto-fallback for gapfill queries.
--
-- NOTE: GapFill Custom Scan requires PG planner. The planner_hook in
-- time_series automatically disables ORCA when gapfill is detected,
-- so no manual SET optimizer = off is needed.
--
-- MPP KEY INSIGHT: In CBDB, GapFill runs independently on each segment.
-- Group-aware gapfill detects GROUP BY columns before the bucket column
-- in the GroupAggregate sort order and propagates them to gap rows.

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;

-- Fix session timezone for stable expected output.
SET timezone = 'UTC';

-- ============================================================
-- Section 1: Data preparation — 3 devices with different gap patterns
-- DISTRIBUTED BY (device_id) spreads devices across segments.
-- ============================================================

CREATE TABLE gf_mpp (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (device_id);

-- Device 1: hours 0,1,3 (gap at hour 2) — values ~10s
INSERT INTO gf_mpp VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, 20.0),
    ('2024-01-01 03:00:00+00', 1, 40.0);

-- Device 2: hours 0,2 (gap at hour 1) — values ~100s
INSERT INTO gf_mpp VALUES
    ('2024-01-01 00:00:00+00', 2, 100.0),
    ('2024-01-01 02:00:00+00', 2, 300.0);

-- Device 3: hours 1,3 (gaps at hours 0,2) — values ~1000s
INSERT INTO gf_mpp VALUES
    ('2024-01-01 01:00:00+00', 3, 1000.0),
    ('2024-01-01 03:00:00+00', 3, 3000.0);

-- ============================================================
-- Section 2: Single-device GapFill (safe mode — WHERE device_id = X)
-- Each query targets one device on one segment → always correct.
-- ============================================================

-- 2a. Device 1: 4h range → 4 rows (gap at hour 2)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2b. Device 2: 3h range → 3 rows (gap at hour 1)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 2
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 2c. Device 3: 4h range → 4 rows (gaps at hours 0,2)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 3
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 3: Single-device LOCF
-- Verifies LOCF state isolation per segment executor.
-- ============================================================

-- 3a. Device 1 LOCF: 10, 20, 20(carry), 40
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 3b. Device 3 LOCF: NULL(no prior), 1000, 1000(carry), 3000
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gf_mpp
WHERE device_id = 3
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 4: Single-device Interpolate
-- ============================================================

-- 4a. Device 2 interpolate: 100, 200(interp), 300
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_mpp
WHERE device_id = 2
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 4b. Device 3 interpolate: NULL(no prev), 1000, 2000(interp), 3000
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_mpp
WHERE device_id = 3
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 5: LOCF + Interpolate together (single device)
-- Using DIFFERENT aggregates: locf on avg, interpolate on max.
-- Using the same aggregate for both is rejected (collision check).
-- ============================================================

-- 5a. LOCF + Interpolate on different aggregates — should succeed
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_avg,
    time_series.interpolate(max(value)) AS interp_max
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 5b. LOCF + Interpolate on SAME aggregate — should error
\set ON_ERROR_STOP 0
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value,
    time_series.interpolate(avg(value)) AS interp_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;
\set ON_ERROR_STOP 1

-- ============================================================
-- Section 6: Multi-group GapFill (GROUP BY device_id, bucket)
-- Group-aware gapfill: non-bucket GROUP BY columns that appear
-- before bucket in the GroupAggregate sort order are propagated
-- to gap rows. LOCF/interpolate state resets between groups.
-- ============================================================

-- 6a. Multi-group basic: device_id should appear in gap rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- 6b. Multi-group + LOCF: LOCF state must reset between groups
-- Device 1: 10, 20, 20(carry), 40
-- Device 2: 100, 100(carry from hour 0), 300, 300(carry)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    time_series.locf(avg(value)) AS locf_value
FROM gf_mpp
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- ============================================================
-- Section 7: EXPLAIN plans (uncompressed table)
-- ============================================================

-- 7a. Single-device gapfill — Gather Motion + GapFill + GroupAggregate
EXPLAIN (COSTS OFF)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 7b. Multi-device gapfill — Gather Motion N:1 + GapFill on segments
EXPLAIN (COSTS OFF)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- 7c. LOCF plan — Result → GapFill → GroupAggregate
EXPLAIN (COSTS OFF)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    time_series.locf(avg(value)) AS locf_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 8: Edge cases
-- ============================================================

-- 8a. Non-existent device_id=999 — GapFill generates all-NULL gap rows
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 03:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 999
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 8b. 15-minute buckets — sparse data across wider time range
SELECT
    time_series.time_bucket_gapfill('15 minutes'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- 8c. Single data point in range
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 2
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 01:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 9: CTE + GapFill (uncompressed, multi-device via CTE)
-- ============================================================

WITH filled_dev1 AS (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           time_series.locf(avg(value)) AS locf_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
)
SELECT bucket, COALESCE(locf_value, 0) AS val
FROM filled_dev1
ORDER BY bucket;

-- ============================================================
-- Section 10: JOIN two single-device gapfilled subqueries
-- Cross-slice query: GapFill Custom Scan dispatched to segments.
-- Requires shared_preload_libraries = 'time_series' on all segments.
-- ============================================================

SELECT d1.bucket, d1.avg_value AS dev1_val, d2.avg_value AS dev2_val
FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
) d1
JOIN (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 2
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 03:00:00+00'
    GROUP BY bucket
) d2 ON d1.bucket = d2.bucket
ORDER BY d1.bucket;

-- ============================================================
-- Section 11: INSERT INTO SELECT with gapfill (MPP)
-- Cross-slice query: GapFill + Redistribute Motion to segments.
-- ============================================================

CREATE TABLE gf_mpp_result (bucket TIMESTAMPTZ, avg_value FLOAT8) USING time_series WITH (
    ts_partition_column = 'bucket',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (bucket);

INSERT INTO gf_mpp_result
SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
           '2024-01-01 00:00:00+00'::timestamptz,
           '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
       avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket;

SELECT * FROM gf_mpp_result ORDER BY bucket;

DROP TABLE gf_mpp_result;

-- ============================================================
-- Section 12: GapFill Planner-Hook Fallback
-- Verifies that gapfill queries always use PG planner via the
-- planner_hook, regardless of whether ORCA is available.
-- Non-gapfill ORCA tests are excluded because ORCA availability
-- varies across CI environments (test_database vs test_database_orca).
-- ============================================================

SET optimizer = on;

-- 12a. GapFill query should auto-fallback to PG planner
EXPLAIN (COSTS OFF)
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    device_id,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id IN (1, 2)
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket, device_id
ORDER BY device_id, bucket;

-- 12b. GapFill in subquery — should also trigger fallback
EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
) sub
ORDER BY bucket;

-- 12c. GapFill in CTE — should also trigger fallback
EXPLAIN (COSTS OFF)
WITH filled AS (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
)
SELECT * FROM filled ORDER BY bucket;

RESET optimizer;

-- ============================================================
-- Section 13: DISTRIBUTED RANDOMLY — should be rejected
-- GapFill validates distribution key vs GROUP BY; DISTRIBUTED
-- RANDOMLY has no key, so gapfill correctly refuses the query.
-- ============================================================

CREATE TABLE gf_random (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED RANDOMLY;

INSERT INTO gf_random VALUES
    ('2024-01-01 00:00:00+00', 1, 10.0),
    ('2024-01-01 01:00:00+00', 1, 20.0),
    ('2024-01-01 03:00:00+00', 1, 40.0);

-- 13a. Gapfill on DISTRIBUTED RANDOMLY table — should error
\set ON_ERROR_STOP 0
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_random
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;
\set ON_ERROR_STOP 1

DROP TABLE gf_random;

-- ============================================================
-- Section 14: UNION ALL with gapfill subqueries
-- Known limitation: In MPP mode, each segment runs gapfill
-- independently. With UNION ALL, gap rows are generated on
-- every segment (including those without data), producing
-- duplicate gap rows. This test documents the current behavior.
-- ============================================================

-- 14a. UNION ALL of two gapfill subqueries — produces 16 rows (known MPP dup)
SELECT * FROM (
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
        1 AS device_id,
        avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
    UNION ALL
    SELECT
        time_series.time_bucket_gapfill('1 hour'::interval, time,
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
        2 AS device_id,
        avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 2
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY bucket
) combined
ORDER BY device_id, bucket;

-- ============================================================
-- Section 15: Timezone variant of time_bucket_gapfill in MPP
-- ============================================================

-- 15a. Timezone gapfill across segment boundary
SELECT
    time_series.time_bucket_gapfill('1 hour'::interval, time,
        'US/Eastern',
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
    avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY bucket
ORDER BY bucket;

-- ============================================================
-- Section 16: Nested gapfill — EXTRACT / arithmetic wrapping
--
-- time_bucket_gapfill() must be a top-level expression in the
-- target list. Wrapping it in EXTRACT(), arithmetic, or other
-- expressions is detected at planning time and raises an error
-- with a hint to use a subquery workaround.
-- ============================================================

-- 16a. EXTRACT(hour) wrapping gapfill — error: must be top-level
\set ON_ERROR_STOP 0
SELECT EXTRACT(hour FROM time_series.time_bucket_gapfill(
           '1 hour'::interval, time,
           '2024-01-01 00:00:00+00'::timestamptz,
           '2024-01-01 04:00:00+00'::timestamptz)) AS hour,
       avg(value) AS avg_value
FROM gf_mpp
WHERE device_id = 1
  AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
GROUP BY 1
ORDER BY 1;
\set ON_ERROR_STOP 1

-- Data-correctness tests using subquery workaround.
-- Reuses gf_mpp (DISTRIBUTED BY device_id) to avoid distribution issues.
-- device_id=1 has hour 0, 1, 3 (hour 2 missing); gapfill fills the gap.

-- 16c. EXTRACT(hour) via subquery — should produce 0,1,2,3
SELECT EXTRACT(hour FROM bucket) AS hour, avg_value
FROM (
    SELECT time_series.time_bucket_gapfill(
               '1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY 1
) sub
ORDER BY 1;

-- 16d. Arithmetic (bucket + interval) via subquery
SELECT bucket + interval '30 min' AS shifted, avg_value
FROM (
    SELECT time_series.time_bucket_gapfill(
               '1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY 1
) sub
ORDER BY 1;

-- 16e. EXTRACT + locf via subquery (locf must be inside gapfill subquery)
SELECT EXTRACT(hour FROM bucket) AS hour, locf_value
FROM (
    SELECT time_series.time_bucket_gapfill(
               '1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-01 04:00:00+00'::timestamptz) AS bucket,
           time_series.locf(avg(value)) AS locf_value
    FROM gf_mpp
    WHERE device_id = 1
      AND time >= '2024-01-01 00:00:00+00' AND time < '2024-01-01 04:00:00+00'
    GROUP BY 1
) sub
ORDER BY 1;

-- ============================================================
-- Section 17: Multi-chunk gapfill — 4023 rows across 12 chunks
--
-- Sections 1-16 above all use `gf_mpp` with 7 rows in a 4-hour
-- window — every row lands in the same single chunk (4-hour
-- chunk_interval, origin 2020).  Plan-shape EXPLAINs there show
-- ChunkScan but always over 1 chunk; multi-chunk behavior is
-- not exercised.
--
-- This section adds a parallel hypertable with:
--   * chunk_interval = 2 hour, 24-hour data span → 12 chunks
--   * 10 devices × diverse gap patterns (~4023 rows total,
--     ~335 rows per chunk on average) — meaningful per-chunk
--     density, not toy data
--   * gaps designed to test:
--       - dense baseline (devices 1-3): full coverage, every chunk
--         has data
--       - whole-chunk empty (devices 4-6): chunks 2-3, 5-6, 9-10
--         resp. completely empty — ChunkScan should skip these
--         entirely, GapFill should still fill the buckets
--       - sparse single-point-per-chunk (device 8): exercises
--         per-chunk minimum-density path
--       - cross-chunk LOCF/Interpolate (device 9): only 2 rows
--         at hour 0 and hour 23, separated by 11 chunks; LOCF
--         must carry forward and Interpolate must connect across
--         every intermediate chunk
--       - single-point single-chunk (device 10): edge minimal
--
-- Each assertion uses aggregated quantities (count, bool_and, sum)
-- so 4023 rows don't end up in expected output; only ~30 lines of
-- summary stats are pinned.
-- ============================================================

CREATE TABLE gf_multichunk (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '2 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (device_id);

-- Devices 1, 2, 3: dense baseline, every 2 minutes for 24 hours.
-- 3 × 720 = 2160 rows.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz + (m * interval '2 minutes'),
       dev, dev * 1000.0 + m
  FROM generate_series(1, 3) dev,
       generate_series(0, 719) m;

-- Device 4: dense but chunks 2-3 (hours 4..8) empty.  720 - 120 = 600.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz + (m * interval '2 minutes'),
       4, 4000.0 + m
  FROM generate_series(0, 719) m
 WHERE NOT (m >= 120 AND m < 240);

-- Device 5: chunks 5-6 (hours 10..14) empty.  600 rows.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz + (m * interval '2 minutes'),
       5, 5000.0 + m
  FROM generate_series(0, 719) m
 WHERE NOT (m >= 300 AND m < 420);

-- Device 6: chunks 9-10 (hours 18..22) empty.  600 rows.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz + (m * interval '2 minutes'),
       6, 6000.0 + m
  FROM generate_series(0, 719) m
 WHERE NOT (m >= 540 AND m < 660);

-- Device 7: sparse, every 30 minutes (one point per 30 min over 24h).
-- 48 rows; every chunk has 4 points.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz + (i * interval '30 minutes'),
       7, 7000.0 + i
  FROM generate_series(0, 47) i;

-- Device 8: extreme sparse, one point per chunk (every 2 hours, at the
-- middle of each chunk).  12 rows = 1 per chunk.
INSERT INTO gf_multichunk
SELECT '2024-01-01 00:00+00'::timestamptz
        + (chk * interval '2 hours')
        + interval '1 hour',
       8, 8000.0 + chk
  FROM generate_series(0, 11) chk;

-- Device 9: only hour 0 + hour 23 — gap spans 11 chunks.
INSERT INTO gf_multichunk VALUES
    ('2024-01-01 00:00+00', 9, 9000.0),
    ('2024-01-01 23:00+00', 9, 9230.0);

-- Device 10: single point at hour 12 — single chunk, single row.
INSERT INTO gf_multichunk VALUES ('2024-01-01 12:00+00', 10, 10120.0);

-- ---------- Sanity floor ----------
\echo '=== Section 17a: chunk + row count sanity ==='
-- 2-hour chunk_interval over 24h data = 12 distinct chunks.
SELECT count(DISTINCT chunk_number) = 12 AS exactly_12_chunks
  FROM time_series.ts_chunk
 WHERE table_oid = 'gf_multichunk'::regclass;

-- 2160 + 600 + 600 + 600 + 48 + 12 + 2 + 1 = 4023.
SELECT count(*) = 4023 AS exactly_4023_rows FROM gf_multichunk;

-- Per-device sanity: each device has the expected row count.
SELECT device_id, count(*) AS rows_in_dev
  FROM gf_multichunk
 GROUP BY device_id
 ORDER BY device_id;

-- ---------- Test 17b: dense baseline (devices 1-3) ----------
\echo '=== Section 17b: dense-baseline gapfill (device 1) ==='
-- Window covers all 24 hours.  Device 1 has a row every 2 minutes,
-- so every hour-bucket has 30 rows → 24 non-NULL buckets, 0 gap rows.
SELECT count(*)                 = 24    AS exactly_24_buckets,
       bool_and(avg_value IS NOT NULL)  AS no_gap_rows,
       min(extract(hour from bucket))::int = 0  AS starts_at_00,
       max(extract(hour from bucket))::int = 23 AS ends_at_23
  FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
      FROM gf_multichunk
     WHERE device_id = 1
       AND time >= '2024-01-01 00:00:00+00'::timestamptz
       AND time <  '2024-01-02 00:00:00+00'::timestamptz
     GROUP BY 1
  ) sub;

-- ---------- Test 17c: whole-chunk empty (device 4) ----------
\echo '=== Section 17c: whole-chunk-empty gapfill (device 4, hours 4-7 empty) ==='
-- Device 4 has chunks 2-3 (hours 4..8) empty.  Window covers all 24h.
-- Expect: 24 buckets total, but buckets 04..07 have NULL avg_value
-- (gap rows because the empty chunks produced no data).
SELECT count(*)                                          = 24 AS exactly_24_buckets,
       count(*) FILTER (WHERE avg_value IS NULL)         = 4  AS four_gap_rows,
       count(*) FILTER (WHERE avg_value IS NULL
                          AND extract(hour from bucket) BETWEEN 4 AND 7) = 4
                                                              AS gap_rows_in_expected_hours
  FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
           avg(value) AS avg_value
      FROM gf_multichunk
     WHERE device_id = 4
       AND time >= '2024-01-01 00:00:00+00'::timestamptz
       AND time <  '2024-01-02 00:00:00+00'::timestamptz
     GROUP BY 1
  ) sub;

-- ---------- Test 17d: cross-11-chunk LOCF (device 9) ----------
\echo '=== Section 17d: cross-chunk LOCF (device 9: hour 0 → hour 23) ==='
-- Device 9 has only hour 0 (value 9000) + hour 23 (value 9230).
-- LOCF must carry 9000 forward through hours 1..22 (crossing
-- chunks 0..10), then switch to 9230 at hour 23.
-- This tests LOCF state propagation across chunk boundaries.
SELECT count(*)                                              = 24 AS exactly_24_buckets,
       bool_and(locf_val IS NOT NULL)                              AS no_null_after_locf,
       count(*) FILTER (WHERE locf_val = 9000.0
                          AND extract(hour from bucket) BETWEEN 0 AND 22)
                                                              = 23 AS locf_carries_9000_for_23h,
       (SELECT locf_val FROM (
          SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
                     '2024-01-01 00:00:00+00'::timestamptz,
                     '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
                 time_series.locf(avg(value)) AS locf_val
            FROM gf_multichunk
           WHERE device_id = 9
             AND time >= '2024-01-01 00:00:00+00'::timestamptz
             AND time <  '2024-01-02 00:00:00+00'::timestamptz
           GROUP BY 1
        ) s2
        WHERE extract(hour from bucket) = 23) = 9230.0
                                                              AS hour_23_is_new_value
  FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
           time_series.locf(avg(value)) AS locf_val
      FROM gf_multichunk
     WHERE device_id = 9
       AND time >= '2024-01-01 00:00:00+00'::timestamptz
       AND time <  '2024-01-02 00:00:00+00'::timestamptz
     GROUP BY 1
  ) sub;

-- ---------- Test 17e: cross-11-chunk Interpolate (device 9) ----------
\echo '=== Section 17e: cross-chunk Interpolate (device 9) ==='
-- Device 9: hour 0 → 9000, hour 23 → 9230.  Linear interpolate:
--   hour H value = 9000 + (9230-9000) * H / 23 = 9000 + 10*H
-- Bucket boundaries cross 11 chunks; verify a few representative
-- buckets land on the exact expected values.
SELECT count(*)                                              = 24 AS exactly_24_buckets,
       bool_and(interp_val IS NOT NULL)                            AS no_null_after_interp,
       (SELECT interp_val FROM (
          SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
                     '2024-01-01 00:00:00+00'::timestamptz,
                     '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
                 time_series.interpolate(avg(value)) AS interp_val
            FROM gf_multichunk WHERE device_id = 9
             AND time >= '2024-01-01 00:00:00+00'::timestamptz
             AND time <  '2024-01-02 00:00:00+00'::timestamptz
           GROUP BY 1) s
        WHERE extract(hour from bucket) = 0)  = 9000.0 AS interp_hour_0,
       (SELECT interp_val FROM (
          SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
                     '2024-01-01 00:00:00+00'::timestamptz,
                     '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
                 time_series.interpolate(avg(value)) AS interp_val
            FROM gf_multichunk WHERE device_id = 9
             AND time >= '2024-01-01 00:00:00+00'::timestamptz
             AND time <  '2024-01-02 00:00:00+00'::timestamptz
           GROUP BY 1) s
        WHERE extract(hour from bucket) = 23) = 9230.0 AS interp_hour_23
  FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
           time_series.interpolate(avg(value)) AS interp_val
      FROM gf_multichunk
     WHERE device_id = 9
       AND time >= '2024-01-01 00:00:00+00'::timestamptz
       AND time <  '2024-01-02 00:00:00+00'::timestamptz
     GROUP BY 1
  ) sub;

-- ---------- Test 17f: multi-device gapfill (devices 1, 2, 3) ----------
\echo '=== Section 17f: multi-device + multi-chunk gapfill ==='
-- 3 devices × 24 buckets = 72 rows, all non-NULL (devices 1-3 are dense).
SELECT count(*)              = 72                            AS rows_72,
       count(DISTINCT bucket)= 24                            AS distinct_buckets_24,
       count(DISTINCT device_id) = 3                         AS distinct_devices_3,
       bool_and(avg_value IS NOT NULL)                       AS no_null_avg
  FROM (
    SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
               '2024-01-01 00:00:00+00'::timestamptz,
               '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
           device_id,
           avg(value) AS avg_value
      FROM gf_multichunk
     WHERE device_id IN (1, 2, 3)
       AND time >= '2024-01-01 00:00:00+00'::timestamptz
       AND time <  '2024-01-02 00:00:00+00'::timestamptz
     GROUP BY 1, 2
  ) sub;

-- ---------- Test 17g: EXPLAIN plan shape over 12 chunks ----------
\echo '=== Section 17g: EXPLAIN — multi-chunk ChunkScan + Motion + GapFill ==='
-- Verify the plan still uses Custom Scan (ChunkScan) on the
-- 12-chunk hypertable (not Seq Scan / Append over per-chunk
-- relations).  A regression that broke ChunkScan dispatch on
-- multi-chunk would surface in the plan shape.
-- Pin the aggregate strategy: the ChunkScan-usage assertion below is
-- what this test actually checks, but GroupAggregate vs HashAggregate
-- is a cost-based choice that can flip between environments/CBDB
-- versions with no functional difference, spuriously failing the
-- exact-text plan diff.  Force the historical GroupAggregate shape so
-- the diff stays sensitive only to what we actually care about here.
SET enable_hashagg = off;
EXPLAIN (COSTS OFF)
SELECT time_series.time_bucket_gapfill('1 hour'::interval, time,
           '2024-01-01 00:00:00+00'::timestamptz,
           '2024-01-02 00:00:00+00'::timestamptz) AS bucket,
       avg(value) AS avg_value
  FROM gf_multichunk
 WHERE device_id = 1
   AND time >= '2024-01-01 00:00:00+00'::timestamptz
   AND time <  '2024-01-02 00:00:00+00'::timestamptz
 GROUP BY bucket
 ORDER BY bucket;
RESET enable_hashagg;

DROP TABLE gf_multichunk;

-- ============================================================
-- Section 18: Cleanup — drop test table and extension
-- ============================================================

DROP TABLE gf_mpp;
DROP EXTENSION time_series CASCADE;
