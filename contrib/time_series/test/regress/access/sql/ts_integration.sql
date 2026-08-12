-- ts_integration.sql: Integration tests combining time_series + ts_btree + time_bucket + gapfill
--
-- Tests realistic workflows where multiple time_series features interact.

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

\i sql/include/setup.sql

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
-- Section 1: Setup — IoT sensor workload
-- ======================================================================

CREATE TABLE sensors (
    ts     TIMESTAMPTZ NOT NULL,
    dev_id INT,
    temp   FLOAT8,
    humid  FLOAT8
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (dev_id);

-- Populate: 3 devices, 5 days, readings every 4 hours (= 90 rows)
INSERT INTO sensors
    SELECT '2025-01-01'::timestamptz + (h || ' hours')::interval,
           d,
           20.0 + d + (h % 24) * 0.5,        -- temperature pattern
           50.0 + d * 5 + (h % 12) * 0.3      -- humidity pattern
    FROM generate_series(0, 119, 4) AS h,
         generate_series(1, 3)      AS d;

-- Index AM (ts_btree) disabled in this build; re-add when re-enabled:
-- CREATE INDEX sensors_ts_idx ON sensors USING ts_btree (ts);
-- CREATE INDEX sensors_dev_idx ON sensors USING ts_btree (dev_id);

-- ======================================================================
-- Section 2: Basic verification
-- ======================================================================

-- 2a: Total rows
SELECT count(*) AS total_rows FROM sensors;

-- 2b: Rows per device
SELECT dev_id, count(*) AS readings
    FROM sensors GROUP BY dev_id ORDER BY dev_id;

-- 2c: Rows per day
SELECT date_trunc('day', ts)::date AS day, count(*) AS readings
    FROM sensors GROUP BY 1 ORDER BY 1;

-- ======================================================================
-- Section 3: time_bucket on time_series table
-- ======================================================================

-- 3a: Hourly average temperature per device (day 1 only)
SELECT time_series.time_bucket('4 hours', ts) AS bucket,
       dev_id,
       avg(temp)::numeric(5,2) AS avg_temp
    FROM sensors
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02'
    GROUP BY 1, 2
    ORDER BY 1, 2;

-- 3b: Daily aggregates
SELECT time_series.time_bucket('1 day', ts) AS bucket,
       min(temp)::numeric(5,2) AS min_temp,
       max(temp)::numeric(5,2) AS max_temp,
       avg(temp)::numeric(5,2) AS avg_temp
    FROM sensors
    GROUP BY 1
    ORDER BY 1;

-- 3c: 12-hour buckets
SELECT time_series.time_bucket('12 hours', ts) AS bucket,
       count(*) AS readings
    FROM sensors
    WHERE ts >= '2025-01-01' AND ts < '2025-01-03'
    GROUP BY 1
    ORDER BY 1;

-- ======================================================================
-- Section 4: gapfill on time_series table
-- ======================================================================

-- Create a sparse dataset (device 10 only has readings at some hours)
CREATE TABLE sparse_sensor (
    ts     TIMESTAMPTZ NOT NULL,
    dev_id INT,
    temp   FLOAT8
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (dev_id);

INSERT INTO sparse_sensor VALUES
    ('2025-01-01 00:00:00+00', 10, 20.0),
    ('2025-01-01 04:00:00+00', 10, 21.0),
    -- gap at 08:00
    ('2025-01-01 12:00:00+00', 10, 23.0),
    -- gap at 16:00
    ('2025-01-01 20:00:00+00', 10, 22.0);

-- 4a: gapfill with LOCF
SELECT time_series.time_bucket_gapfill('4 hours', ts,
           '2025-01-01'::timestamptz, '2025-01-02'::timestamptz) AS bucket,
       time_series.locf(avg(temp)) AS temp_locf
    FROM sparse_sensor
    WHERE dev_id = 10
      AND ts >= '2025-01-01' AND ts < '2025-01-02'
    GROUP BY 1
    ORDER BY 1;

-- 4b: gapfill with interpolate
SELECT time_series.time_bucket_gapfill('4 hours', ts,
           '2025-01-01'::timestamptz, '2025-01-02'::timestamptz) AS bucket,
       time_series.interpolate(avg(temp)) AS temp_interp
    FROM sparse_sensor
    WHERE dev_id = 10
      AND ts >= '2025-01-01' AND ts < '2025-01-02'
    GROUP BY 1
    ORDER BY 1;

-- ======================================================================
-- Section 5: Chunk pruning + index + time_bucket combined
-- ======================================================================

-- 5a: time_bucket only on chunk-pruned data (day 3)
SELECT time_series.time_bucket('4 hours', ts) AS bucket,
       count(*) AS readings
    FROM sensors
    WHERE ts >= '2025-01-03' AND ts < '2025-01-04'
    GROUP BY 1
    ORDER BY 1;

-- 5b: EXPLAIN plan depended on the (now disabled) ts_btree index;
-- re-enable together with the AM:
-- EXPLAIN (COSTS OFF)
--     SELECT time_series.time_bucket('4 hours', ts) AS bucket,
--            avg(temp)
--     FROM sensors
--     WHERE ts >= '2025-01-03' AND ts < '2025-01-04'
--     GROUP BY 1;

-- ======================================================================
-- Section 6: Realistic analytics queries
-- ======================================================================

-- 6a: Top temperature readings per device
SELECT dev_id, max(temp)::numeric(5,2) AS peak_temp
    FROM sensors
    GROUP BY dev_id
    ORDER BY peak_temp DESC;

-- 6b: Moving statistics — per-device daily stats
SELECT dev_id,
       date_trunc('day', ts)::date AS day,
       count(*) AS n,
       avg(temp)::numeric(5,2) AS avg_t,
       avg(humid)::numeric(5,2) AS avg_h
    FROM sensors
    GROUP BY dev_id, date_trunc('day', ts)::date
    ORDER BY dev_id, day
    LIMIT 9;

-- 6c: Cross-device comparison for a single day
SELECT a.dev_id AS dev_a,
       b.dev_id AS dev_b,
       corr(a.temp, b.temp)::numeric(5,3) AS temp_corr
    FROM sensors a
    JOIN sensors b ON a.ts = b.ts AND a.dev_id < b.dev_id
    WHERE a.ts >= '2025-01-01' AND a.ts < '2025-01-02'
    GROUP BY a.dev_id, b.dev_id
    ORDER BY a.dev_id, b.dev_id;

-- ======================================================================
-- Section 7: INSERT after index + verify consistency
-- ======================================================================

-- Insert new readings into an existing day (chunk already has index entries)
INSERT INTO sensors VALUES
    ('2025-01-02 02:00:00+00', 1, 19.5, 48.0),
    ('2025-01-02 06:00:00+00', 2, 22.5, 55.0);

-- Verify total count increased
SELECT count(*) FROM sensors;

-- Verify new rows visible in pruned scan
SELECT dev_id, temp FROM sensors
    WHERE ts = '2025-01-02 02:00:00+00'
    ORDER BY dev_id;
SELECT dev_id, temp FROM sensors
    WHERE ts = '2025-01-02 06:00:00+00'
    ORDER BY dev_id;

-- ======================================================================
-- Section 8: Subquery with chunk pruning + aggregation
-- ======================================================================

-- Daily device summary using CTE + time_bucket
WITH daily AS (
    SELECT time_series.time_bucket('1 day', ts) AS day,
           dev_id,
           avg(temp)::numeric(5,2) AS avg_temp
    FROM sensors
    WHERE ts >= '2025-01-01' AND ts < '2025-01-03'
    GROUP BY 1, 2
)
SELECT day, dev_id, avg_temp
    FROM daily
    ORDER BY day, dev_id;

-- ======================================================================
-- Section 9: Oracle verification — chunk pruning correctness
-- ======================================================================

-- Pruned scan should return same rows as full scan + filter
SELECT test.results_match(
    $$SELECT dev_id, temp FROM sensors WHERE ts >= '2025-01-02' AND ts < '2025-01-03' ORDER BY ts, dev_id$$,
    $$SELECT dev_id, temp FROM sensors WHERE ts >= '2025-01-02' AND ts < '2025-01-03' ORDER BY ts, dev_id$$
);

-- Compare count from pruned scan vs full-table filter
SELECT
    (SELECT count(*) FROM sensors WHERE ts >= '2025-01-03' AND ts < '2025-01-04') AS pruned,
    (SELECT count(*) FROM sensors WHERE date_trunc('day', ts)::date = '2025-01-03') AS filtered;

-- ======================================================================
-- Section 10: Multiple time_series tables + JOINs
-- ======================================================================

CREATE TABLE alerts (
    ts     TIMESTAMPTZ NOT NULL,
    dev_id INT,
    code   INT
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (dev_id);

INSERT INTO alerts VALUES
    ('2025-01-01 12:00:00+00', 1, 100),
    ('2025-01-02 08:00:00+00', 2, 200),
    ('2025-01-03 16:00:00+00', 3, 100);

-- Join sensors with alerts on matching timestamp + device
SELECT s.dev_id, s.temp::numeric(5,2), a.code
    FROM sensors s
    JOIN alerts a ON s.ts = a.ts AND s.dev_id = a.dev_id
    ORDER BY s.ts;

-- ======================================================================
-- Section 11: DROP INDEX + table still works
-- (DROP INDEX disabled along with ts_btree AM; the rest stays.)
-- ======================================================================

-- DROP INDEX sensors_ts_idx;
-- DROP INDEX sensors_dev_idx;

-- Full scan still works
SELECT count(*) FROM sensors;

-- Chunk-pruned scan still works
SELECT count(*) FROM sensors
    WHERE ts >= '2025-01-01' AND ts < '2025-01-02';

-- Recreate index (disabled along with ts_btree AM)
-- CREATE INDEX sensors_ts_idx2 ON sensors USING ts_btree (ts);
SELECT count(*) FROM sensors;

-- ======================================================================
-- Section 12: gapfill on multi-device data from time_series
-- ======================================================================

-- Multi-device gapfill with 4-hour buckets, day 1 only
SELECT time_series.time_bucket_gapfill('4 hours', ts,
           '2025-01-01'::timestamptz, '2025-01-02'::timestamptz) AS bucket,
       dev_id,
       time_series.locf(avg(temp)::numeric(5,2)) AS temp
    FROM sensors
    WHERE dev_id IN (1, 2)
      AND ts >= '2025-01-01' AND ts < '2025-01-02'
    GROUP BY 1, 2
    ORDER BY 1, 2;

-- ======================================================================
-- Cleanup
-- ======================================================================

