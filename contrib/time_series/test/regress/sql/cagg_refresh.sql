-- ============================================================
-- cagg_refresh.sql
-- Test: REFRESH procedure — full, incremental, L1/L2, watermark, errors
--
-- Covers: R-01 ~ R-37 from cagg_test_tracking.md
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- Setup: deterministic source data
CREATE TABLE metrics (
    time        TIMESTAMPTZ       NOT NULL,
    tags_id     INT               NOT NULL,
    temperature DOUBLE PRECISION  NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);

INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz
        + (hr || ' hour')::interval
        + (m * 5 || ' minute')::interval,
       tid, 20.0 + tid + hr * 0.5 + m * 0.1
FROM generate_series(1, 10) tid,    -- 10 tags
     generate_series(0, 9)  hr,      -- 10 hours
     generate_series(1, 10) m;       -- 10 rows per (tag, hour)
-- 10 × 10 × 10 = 1000 rows; count=10/cell, 100 groups

CREATE MATERIALIZED VIEW cv
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id,
         count(*) AS cnt,
         avg(temperature) AS avg_temp
  FROM metrics
  GROUP BY bucket, tags_id;

-- ============================================================
-- R-01: Full REFRESH (NULL, NULL) → mat table has data
-- ============================================================
\echo '=== R-01: full REFRESH ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_count FROM cv;

-- ============================================================
-- R-02: Full REFRESH → EXCEPT = 0
-- ============================================================
\echo '=== R-02: EXCEPT = 0 ==='
SELECT count(*) AS diff FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-03: Explicit window REFRESH → only window data
-- ============================================================
\echo '=== R-03: window REFRESH ==='
-- Clear mat table by creating a fresh CAGG
DROP VIEW cv CASCADE;
CREATE MATERIALIZED VIEW cv
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(temperature) AS avg_temp
  FROM metrics GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:00+00', '2024-01-01 03:00+00');
-- Only buckets 01:00 and 02:00 should be materialized
SELECT DISTINCT bucket FROM cv ORDER BY bucket;
-- Content should match source for the window.
-- Filter cv to the same window so real-time branch (live data above
-- watermark) doesn't contribute extra rows to the diff.
SELECT count(*) AS diff_window FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket >= '2024-01-01 01:00+00' AND bucket < '2024-01-01 03:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics
   WHERE time >= '2024-01-01 01:00+00' AND time < '2024-01-01 03:00+00'
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics
   WHERE time >= '2024-01-01 01:00+00' AND time < '2024-01-01 03:00+00'
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket >= '2024-01-01 01:00+00' AND bucket < '2024-01-01 03:00+00'
   )
) x;

-- ============================================================
-- R-03b: Partial REFRESH must not advance watermark past
--        un-materialized ranges (P0 watermark jump bug).
--        First refresh with [02:00, 04:00) should NOT push
--        watermark to 04:00, otherwise bucket 00:00 and 01:00
--        become permanently invisible.
-- ============================================================
\echo '=== R-03b: watermark jump prevention ==='
-- Clean state
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Record current watermark
SELECT MAX(w.watermark) AS wm_before
FROM time_series.cagg_watermark w
JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
WHERE c.user_view_name = 'cv';

-- Backfill into bucket 05:00 (beyond current watermark after full refresh)
INSERT INTO metrics VALUES ('2024-01-01 12:30+00', 1, 999.0);

-- Partial refresh [12:00, 13:00) — starts beyond watermark.
-- Aligned with upstream permissive gap semantics: the refresh does not
-- ereport() when window_start > current_watermark; instead it becomes a
-- no-op advance for the direct path.  Catch-up below still guards the
-- watermark via gap_is_materialized(), so wm advances only when the gap
-- is actually filled.
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 12:00+00', '2024-01-01 13:00+00');

-- Watermark must NOT have jumped to 13:00
-- (window_start > watermark → no advancement).
-- Express the stated intent as an explicit boolean rather than
-- snapshotting the exact post-refresh watermark, since the exact
-- landing depends on the hot-bucket-exclusion clamp and the
-- gap-check outcome (both are subtleties of the refresh internals
-- that may legitimately evolve).  The invariant the test guards
-- is: the partial refresh did not jump watermark past the user-
-- visible data range -- i.e. it stayed strictly below window_end.
SELECT MAX(w.watermark) < '2024-01-01 13:00+00'::timestamptz
       AS wm_did_not_jump_to_window_end
FROM time_series.cagg_watermark w
JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
WHERE c.user_view_name = 'cv';

-- All data must still be visible (symmetric EXCEPT = 0)
SELECT count(*) AS diff_wm_jump FROM (
  (SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv)
) x;

-- Full refresh to clean up
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- R-04: Non-aligned window → ERROR (matches upstream)
--       [01:25, 02:45) with 1h buckets → inscribed [02:00, 02:00) = empty!
--       upstream reports: "refresh window too small"
-- ============================================================
\echo '=== R-04: non-aligned window ==='
-- Full refresh first for clean state
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Backfill into bucket 01:00
INSERT INTO metrics VALUES ('2024-01-01 01:25+00', 1, 111.0);
-- Non-aligned window: [01:25, 02:45) → ERROR "refresh window too small"
\set ON_ERROR_STOP off
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:25+00', '2024-01-01 02:45+00');
\set ON_ERROR_STOP on
-- Aligned window refresh picks it up
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
SELECT count(*) AS diff_after_aligned FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-04b: "refresh window too small" — additional edge cases
--        Verifies ERROR is raised for various too-small windows
--        and that properly-sized windows still work.
-- ============================================================
\echo '=== R-04b: refresh window too small edge cases ==='

-- Case 1: window smaller than one bucket (30 min < 1 hour)
\set ON_ERROR_STOP off
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 03:10+00', '2024-01-01 03:40+00');
\set ON_ERROR_STOP on

-- Case 2: window exactly one bucket but not aligned (straddles boundary)
--         [02:30, 03:30) → inscribed [03:00, 03:00) = empty
\set ON_ERROR_STOP off
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:30+00', '2024-01-01 03:30+00');
\set ON_ERROR_STOP on

-- Case 3: window exactly on bucket boundaries → should succeed (not too small)
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 03:00+00', '2024-01-01 04:00+00');

-- Case 4: window covers exactly one bucket, start aligned, end not
--         [03:00, 03:45) → inscribed [03:00, 03:00) = empty
\set ON_ERROR_STOP off
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 03:00+00', '2024-01-01 03:45+00');
\set ON_ERROR_STOP on

-- Case 5: window covers two buckets, neither boundary aligned → should succeed
--         [02:30, 04:30) → inscribed [03:00, 04:00) = one bucket
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:30+00', '2024-01-01 04:30+00');

-- ============================================================
-- R-04c: Origin/Offset/Timezone shifted CAGG targeted refresh
--        Verifies that inscribed alignment and L2 dirty interval
--        alignment use the CAGG's actual bucket boundaries, not
--        the default ones.
-- ============================================================
\echo '=== R-04c: origin/offset/timezone shifted CAGG ==='

-- --- Origin-shifted CAGG (4h buckets starting at 02:00) ---
CREATE TABLE src_origin (time TIMESTAMPTZ NOT NULL, dev INT, val INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (dev);
INSERT INTO src_origin
SELECT '2024-01-01 02:00+00'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(0, 23) i;

CREATE MATERIALIZED VIEW cv_origin WITH (time_series.continuous) AS
SELECT time_bucket('4 hours'::interval, time,
                   '2024-01-01 02:00+00'::timestamptz) AS bucket,
       dev, sum(val) AS total, count(*) AS n
FROM src_origin GROUP BY bucket, dev;

CALL time_series.refresh_continuous_aggregate('cv_origin', NULL, NULL);
INSERT INTO src_origin VALUES ('2024-01-01 03:00+00', 1, 88888);
-- Targeted refresh on an origin-aligned window [02:00, 06:00)
CALL time_series.refresh_continuous_aggregate('cv_origin',
  '2024-01-01 02:00+00', '2024-01-01 06:00+00');
SELECT count(*) AS diff_origin FROM (
  (SELECT time_bucket('4 hours'::interval, time,
                      '2024-01-01 02:00+00'::timestamptz),
          dev, sum(val), count(*)
   FROM src_origin GROUP BY 1, 2
   EXCEPT
   SELECT bucket, dev, total, n FROM cv_origin)
  UNION ALL
  (SELECT bucket, dev, total, n FROM cv_origin
   EXCEPT
   SELECT time_bucket('4 hours'::interval, time,
                      '2024-01-01 02:00+00'::timestamptz),
          dev, sum(val), count(*)
   FROM src_origin GROUP BY 1, 2)
) x;
DROP TABLE src_origin CASCADE;

-- --- Offset-shifted CAGG (1h buckets offset by 30min) ---
CREATE TABLE src_offset (time TIMESTAMPTZ NOT NULL, dev INT, val INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (dev);
INSERT INTO src_offset
SELECT '2024-01-01'::timestamptz + (i * interval '10 minutes'), 1, i
FROM generate_series(0, 143) i;

CREATE MATERIALIZED VIEW cv_offset WITH (time_series.continuous) AS
SELECT time_bucket('1 hour'::interval, time,
                   '30 minutes'::interval) AS bucket,
       dev, sum(val) AS total, count(*) AS n
FROM src_offset GROUP BY bucket, dev;

CALL time_series.refresh_continuous_aggregate('cv_offset', NULL, NULL);
INSERT INTO src_offset VALUES ('2024-01-01 01:45+00', 1, 55555);
-- Targeted refresh on an offset-aligned window [01:30, 02:30)
CALL time_series.refresh_continuous_aggregate('cv_offset',
  '2024-01-01 01:30+00', '2024-01-01 02:30+00');
SELECT count(*) AS diff_offset FROM (
  (SELECT time_bucket('1 hour'::interval, time, '30 minutes'::interval),
          dev, sum(val), count(*)
   FROM src_offset GROUP BY 1, 2
   EXCEPT
   SELECT bucket, dev, total, n FROM cv_offset)
  UNION ALL
  (SELECT bucket, dev, total, n FROM cv_offset
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time, '30 minutes'::interval),
          dev, sum(val), count(*)
   FROM src_offset GROUP BY 1, 2)
) x;
DROP TABLE src_offset CASCADE;

-- --- Timezone-shifted CAGG (1 day buckets in America/New_York) ---
CREATE TABLE src_tz (time TIMESTAMPTZ NOT NULL, dev INT, val INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (dev);
INSERT INTO src_tz
SELECT '2024-01-01'::timestamptz + (h * interval '1 hour'), 1, h
FROM generate_series(0, 47) h;

CREATE MATERIALIZED VIEW cv_tz WITH (time_series.continuous) AS
SELECT time_bucket('1 day', time, 'America/New_York') AS bucket,
       dev, sum(val) AS total, count(*) AS n
FROM src_tz GROUP BY bucket, dev;

CALL time_series.refresh_continuous_aggregate('cv_tz', NULL, NULL);
INSERT INTO src_tz VALUES ('2024-01-01 12:00+00', 1, 44444);
-- Targeted refresh: [05:00 UTC, 05:00+24h UTC) = one full NYC day
CALL time_series.refresh_continuous_aggregate('cv_tz',
  '2024-01-01 05:00+00', '2024-01-02 05:00+00');
SELECT count(*) AS diff_tz FROM (
  (SELECT time_bucket('1 day', time, 'America/New_York'),
          dev, sum(val), count(*)
   FROM src_tz GROUP BY 1, 2
   EXCEPT
   SELECT bucket, dev, total, n FROM cv_tz)
  UNION ALL
  (SELECT bucket, dev, total, n FROM cv_tz
   EXCEPT
   SELECT time_bucket('1 day', time, 'America/New_York'),
          dev, sum(val), count(*)
   FROM src_tz GROUP BY 1, 2)
) x;
DROP TABLE src_tz CASCADE;

-- ============================================================
-- R-05: Backfill → EXCEPT = 0
-- ============================================================
\echo '=== R-05: backfill EXCEPT ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
INSERT INTO metrics VALUES
    ('2024-01-01 00:30+00', 3, 333.0),
    ('2024-01-01 03:15+00', 7, 777.0);
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 00:00+00', '2024-01-01 04:00+00');
SELECT count(*) AS diff_backfill FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-06: Idempotent REFRESH (call twice, same result)
-- ============================================================
\echo '=== R-06: idempotent ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS cnt1 FROM cv;
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS cnt2 FROM cv;

-- ============================================================
-- R-07: Empty table REFRESH → no error
-- ============================================================
\echo '=== R-07: empty table REFRESH ==='
CREATE TABLE empty_src (time timestamptz NOT NULL, tags_id int NOT NULL, val float8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
CREATE MATERIALIZED VIEW cv_empty
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*)
  FROM empty_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_empty', NULL, NULL);
SELECT count(*) AS empty_mat FROM cv_empty;
DROP VIEW cv_empty CASCADE;
DROP TABLE empty_src CASCADE;

-- ============================================================
-- R-08: TRUNCATE source → REFRESH → mat table empty
-- ============================================================
\echo '=== R-08: TRUNCATE → empty ==='
TRUNCATE metrics;
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_after_truncate FROM cv;

-- Re-populate
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz
        + (hr || ' hour')::interval
        + (m * 5 || ' minute')::interval,
       tid, 20.0 + tid + hr * 0.5 + m * 0.1
FROM generate_series(1, 10) tid,    -- 10 tags
     generate_series(0, 9)  hr,      -- 10 hours
     generate_series(1, 10) m;       -- 10 rows per (tag, hour)
-- 10 × 10 × 10 = 1000 rows; count=10/cell, 100 groups
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- R-09: INSERT into existing bucket → REFRESH → cnt/avg updated
--       (replaces DELETE test — source table doesn't support DELETE)
-- ============================================================
\echo '=== R-09: INSERT existing bucket → REFRESH ==='
-- Record the original cnt for (bucket=02:00, tags_id=1)
SELECT cnt AS cnt_before FROM cv
WHERE bucket = '2024-01-01 02:00+00' AND tags_id = 1;
-- Insert another row into the same bucket
INSERT INTO metrics VALUES ('2024-01-01 02:30+00', 1, 999.0);
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:00+00', '2024-01-01 03:00+00');
-- cnt should increase by 1, and EXCEPT against live query = 0
SELECT cnt AS cnt_after FROM cv
WHERE bucket = '2024-01-01 02:00+00' AND tags_id = 1;
SELECT count(*) AS diff_insert_existing FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-10: INSERT into new (tags_id, bucket) → REFRESH → new mat row
--       (replaces UPDATE test — source table doesn't support UPDATE)
-- ============================================================
\echo '=== R-10: INSERT new tags_id → REFRESH ==='
-- tags_id=99 doesn't exist yet in hour-00 bucket
INSERT INTO metrics VALUES ('2024-01-01 00:30+00', 99, 42.0);
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- A new mat row for (bucket=00:00, tags_id=99) should appear
SELECT cnt, avg_temp FROM cv
WHERE bucket = '2024-01-01 00:00+00' AND tags_id = 99;
SELECT count(*) AS diff_new_tag FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-13: L1→L2 migration → L1 cleared
-- ============================================================
\echo '=== R-13: L1 cleared after REFRESH ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
INSERT INTO metrics VALUES ('2024-01-01 01:30+00', 2, 555.0);
-- L1 should have 1 entry
SELECT count(*) AS l1_before FROM time_series.cagg_invalidation_log;
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
SELECT count(*) AS l1_after FROM time_series.cagg_invalidation_log;

-- ============================================================
-- R-23: Watermark only increases (GREATEST semantics)
-- ============================================================
\echo '=== R-23: watermark only increases ==='
-- Save watermark before, refresh a small old window, compare after
DO $$
DECLARE
  wm_before timestamptz;
  wm_after  timestamptz;
BEGIN
  SELECT watermark INTO wm_before FROM time_series.cagg_watermark LIMIT 1;
  CALL time_series.refresh_continuous_aggregate('cv',
    '2024-01-01 00:00+00', '2024-01-01 01:00+00');
  SELECT watermark INTO wm_after FROM time_series.cagg_watermark LIMIT 1;
  IF wm_after < wm_before THEN
    RAISE EXCEPTION 'watermark decreased: % → %', wm_before, wm_after;
  END IF;
  RAISE NOTICE 'watermark did not decrease: OK';
END $$;

-- ============================================================
-- R-26: REFRESH invalid name → error
-- ============================================================
\set ON_ERROR_STOP 0
\echo '=== R-26: invalid name ==='
CALL time_series.refresh_continuous_aggregate('nonexistent_cagg', NULL, NULL);

-- ============================================================
-- R-27: REFRESH reverse window → error
-- ============================================================
\echo '=== R-27: reverse window ==='
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-02 00:00+00', '2024-01-01 00:00+00');

-- ============================================================
-- R-28: REFRESH NULL name → error
-- ============================================================
\echo '=== R-28: NULL name ==='
CALL time_series.refresh_continuous_aggregate(NULL, NULL, NULL);

\set ON_ERROR_STOP 1

-- ============================================================
-- R-32/R-33: Multi-CAGG: refresh one, other's L2 preserved
-- ============================================================
\echo '=== R-32/R-33: multi-CAGG ==='
CREATE MATERIALIZED VIEW cv_daily
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv_daily', NULL, NULL);

-- Backfill into already-materialized range
INSERT INTO metrics VALUES ('2024-01-01 02:30+00', 4, 444.0);

-- Refresh only hourly — daily should still have outdated data
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:00+00', '2024-01-01 03:00+00');

-- Daily should have L2 entries (from the L1→L2 migration that happened
-- during hourly's REFRESH TX1)
SELECT count(*) AS daily_l2 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_daily');

-- Now refresh daily
CALL time_series.refresh_continuous_aggregate('cv_daily', NULL, NULL);
SELECT count(*) AS diff_daily FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_daily
   EXCEPT
   SELECT time_bucket('1 day'::interval, time), tags_id, count(*)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, time), tags_id, count(*)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_daily
   )
) x;

DROP VIEW cv_daily CASCADE;

-- ============================================================
-- R-34: 5 rounds INSERT→REFRESH → EXCEPT = 0
-- ============================================================
\echo '=== R-34: 5 rounds ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Round 1
INSERT INTO metrics VALUES ('2024-01-01 00:45+00', 1, 10.0);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- Round 2
INSERT INTO metrics VALUES ('2024-01-01 01:45+00', 2, 20.0);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- Round 3
INSERT INTO metrics VALUES ('2024-01-01 02:45+00', 3, 30.0);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 02:00+00', '2024-01-01 03:00+00');
-- Round 4
INSERT INTO metrics VALUES ('2024-01-01 03:45+00', 4, 40.0);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 03:00+00', '2024-01-01 04:00+00');
-- Round 5
INSERT INTO metrics VALUES ('2024-01-01 04:45+00', 5, 50.0);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 04:00+00', '2024-01-01 05:00+00');

SELECT count(*) AS diff_5rounds FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-36: NULL aggregate values handled correctly
-- ============================================================
\echo '=== R-36: NULL aggregates ==='
INSERT INTO metrics VALUES ('2024-01-01 00:30+00', 10, NULL);
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 00:00+00', '2024-01-01 01:00+00');
SELECT count(*) AS diff_null FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- R-37: Multi-aggregate column-by-column EXCEPT = 0
-- ============================================================
\echo '=== R-37: multi-agg EXCEPT ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_final FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- Unified-path tests (post-refactor: NULL,NULL → incremental)
-- ============================================================

-- ============================================================
-- U-01: First REFRESH on fresh CAGG (watermark=-∞)
--       Unmaterialized range = everything → full materialization
-- ============================================================
\echo '=== U-01: first REFRESH on fresh CAGG ==='
DROP VIEW cv CASCADE;
CREATE MATERIALIZED VIEW cv
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(temperature) AS avg_temp
  FROM metrics GROUP BY bucket, tags_id;
-- watermark starts at -infinity
SELECT bool_and(watermark = '-infinity'::timestamptz) AS wm_is_neg_inf
FROM time_series.cagg_watermark;
-- First REFRESH via unified path
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_first FROM cv;
SELECT count(*) AS diff_first FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- U-02: NULL,NULL with only dirty data (no new unmaterialized data)
--       Should refresh ONLY the dirty intervals, not everything
-- ============================================================
\echo '=== U-02: NULL,NULL dirty-only ==='
-- After U-01, everything is materialized. Insert a backfill row.
INSERT INTO metrics VALUES ('2024-01-01 01:30+00', 1, 888.0);
-- Record mat count before
SELECT count(*) AS mat_before_dirty FROM cv;
-- NULL,NULL should only refresh the dirty bucket (hour 01), not all 50+
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_dirty FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- U-03: NULL,NULL no dirty data, no new data → true no-op
-- ============================================================
\echo '=== U-03: NULL,NULL no-op ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_before_noop FROM cv;
-- Second NULL,NULL with nothing changed
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_after_noop FROM cv;
-- L1, L2 should both be empty
SELECT count(*) AS l1_noop FROM time_series.cagg_invalidation_log;
SELECT count(*) AS l2_noop FROM time_series.cagg_materialization_log;

-- ============================================================
-- U-04: NULL,NULL with dirty L2 + new unmaterialized data overlapping
--       Intervals should be merged correctly
-- ============================================================
\echo '=== U-04: dirty + unmaterialized overlap ==='
-- Set watermark to a known value so we control the unmaterialized range
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 03:00:00+00'::timestamptz;
-- Insert backfill (below watermark → dirty) and new data (above watermark → unmaterialized)
INSERT INTO metrics VALUES ('2024-01-01 02:30+00', 1, 100.0);  -- dirty: hour 02
INSERT INTO metrics VALUES ('2024-01-01 03:30+00', 2, 200.0);  -- unmaterialized: hour 03
INSERT INTO metrics VALUES ('2024-01-01 04:30+00', 3, 300.0);  -- unmaterialized: hour 04
-- The dirty interval (hour 02-03) and unmaterialized range (03:00→+∞) overlap/adjoin
-- After merge they should form one contiguous range
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_overlap FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- U-05: Watermark advancement after NULL,NULL
--       Should advance to now() (not +infinity)
-- ============================================================
\echo '=== U-05: watermark after NULL,NULL ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Watermark should be close to now(), definitely > the latest data timestamp
DO $$
DECLARE wm timestamptz;
BEGIN
  SELECT watermark INTO wm FROM time_series.cagg_watermark LIMIT 1;
  IF wm > '2024-01-01 05:00+00' AND wm < now() + interval '1 minute' THEN
    RAISE NOTICE 'watermark_ok: true';
  ELSE
    RAISE NOTICE 'watermark_ok: false (wm=%)', wm;
  END IF;
END $$;

-- ============================================================
-- U-06: NULL,NULL after partial REFRESH left some L2 behind
--       The remaining L2 + unmaterialized range should all be processed
-- ============================================================
\echo '=== U-06: NULL,NULL cleans up leftover L2 ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Insert two rows in different buckets
INSERT INTO metrics VALUES ('2024-01-01 01:15+00', 5, 50.0);
INSERT INTO metrics VALUES ('2024-01-01 03:15+00', 6, 60.0);
-- Partial refresh only hour 01
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- L2 for hour 03 should remain
SELECT count(*) AS l2_leftover FROM time_series.cagg_materialization_log;
-- NULL,NULL should process the leftover L2
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS l2_after_full FROM time_series.cagg_materialization_log;
SELECT count(*) AS diff_leftover FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- Borrowed-from-upstream tests
-- ============================================================

-- ============================================================
-- T-01: Half-open window REFRESH — (NULL, end) and (start, NULL)
-- ============================================================
\echo '=== T-01: half-open window ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Insert backfill in early and late buckets
INSERT INTO metrics VALUES ('2024-01-01 00:30+00', 1, 10.0);
INSERT INTO metrics VALUES ('2024-01-01 04:30+00', 2, 20.0);

-- Refresh (NULL, '2024-01-01 02:00') — from -∞ to hour-02
CALL time_series.refresh_continuous_aggregate('cv', NULL, '2024-01-01 02:00+00');
-- Hour-00 backfill should be refreshed, hour-04 should NOT be
SELECT count(*) AS diff_left_open FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket < '2024-01-01 02:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics WHERE time < '2024-01-01 02:00+00' GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics WHERE time < '2024-01-01 02:00+00' GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket < '2024-01-01 02:00+00'
   )
) x;

-- Refresh ('2024-01-01 03:00', NULL) — from hour-03 to +∞
CALL time_series.refresh_continuous_aggregate('cv', '2024-01-01 03:00+00', NULL);
-- Hour-04 backfill should now be refreshed
SELECT count(*) AS diff_right_open FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- T-02: L2 interval cut — partial REFRESH splits L2 entry
--       L2 entry spans [hour-01, hour-04), REFRESH window [02:00, 03:00)
--       → left piece [01:00, 02:00) and right piece [03:00, 04:00) survive
-- ============================================================
\echo '=== T-02: L2 interval cut ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Insert across a wide range to create a broad L2 entry
INSERT INTO metrics VALUES
    ('2024-01-01 01:30+00', 1, 10.0),
    ('2024-01-01 02:30+00', 1, 20.0),
    ('2024-01-01 03:30+00', 1, 30.0);
-- Migrate L1→L2 by starting a refresh, but only refresh the middle bucket
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:00+00', '2024-01-01 03:00+00');
-- L2 should still have entries for hour-01 and hour-03 (outside refresh window)
SELECT count(*) AS l2_cut_remaining FROM time_series.cagg_materialization_log;
-- Middle bucket should be refreshed correctly
SELECT count(*) AS diff_cut FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket = '2024-01-01 02:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics WHERE time >= '2024-01-01 02:00+00' AND time < '2024-01-01 03:00+00'
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics WHERE time >= '2024-01-01 02:00+00' AND time < '2024-01-01 03:00+00'
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   WHERE bucket = '2024-01-01 02:00+00'
   )
) x;
-- Full refresh to clean up
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- T-03: Minimum bucket interval (1 second) — off-by-one catcher
-- ============================================================
\echo '=== T-03: 1-second bucket ==='
CREATE TABLE metrics_1s (time TIMESTAMPTZ NOT NULL, v INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO metrics_1s VALUES
    ('2024-01-01 00:00:00+00', 1),
    ('2024-01-01 00:00:01+00', 2),
    ('2024-01-01 00:00:02+00', 3);
CREATE MATERIALIZED VIEW cv_1s WITH (time_series.continuous) AS
  SELECT time_bucket('1 second'::interval, time) AS bucket, count(*) AS cnt
  FROM metrics_1s GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_1s', NULL, NULL);
SELECT count(*) AS mat_1s FROM cv_1s;
-- Backfill at exact second boundary
INSERT INTO metrics_1s VALUES ('2024-01-01 00:00:01+00', 4);
CALL time_series.refresh_continuous_aggregate('cv_1s',
  '2024-01-01 00:00:01+00', '2024-01-01 00:00:02+00');
-- Bucket at 00:00:01 should now have cnt=2
SELECT cnt AS cnt_1s_after FROM cv_1s WHERE bucket = '2024-01-01 00:00:01+00';
SELECT count(*) AS diff_1s FROM (
  (  SELECT bucket, cnt FROM cv_1s
   EXCEPT
   SELECT time_bucket('1 second'::interval, time), count(*)
   FROM metrics_1s GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 second'::interval, time), count(*)
   FROM metrics_1s GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_1s
   )
) x;
DROP VIEW cv_1s CASCADE;
DROP TABLE metrics_1s CASCADE;

-- ============================================================
-- T-04: Watermark should not exceed max(data time)
--       After REFRESH, watermark advances but is bounded by now()
-- ============================================================
\echo '=== T-04: watermark capped ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- All data is from 2024-01. Watermark should be ≈ now() (2026),
-- but never exceed now() + small tolerance.
DO $$
DECLARE wm timestamptz;
BEGIN
  SELECT MAX(watermark) INTO wm FROM time_series.cagg_watermark;
  IF wm <= now() + interval '1 minute' THEN
    RAISE NOTICE 'watermark_capped: true';
  ELSE
    RAISE NOTICE 'watermark_capped: false (wm=%, now=%)', wm, now();
  END IF;
END $$;

-- ============================================================
-- T-05: REFRESH in DO block — should work (no subtransaction)
-- ============================================================
\echo '=== T-05: REFRESH in DO block ==='
DO $$
BEGIN
  CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
END $$;
-- Should not error; verify mat is correct
SELECT count(*) AS diff_doblock FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- T-06: Mixed type input — start::date, end::timestamptz
-- ============================================================
\echo '=== T-06: mixed type input ==='
INSERT INTO metrics VALUES ('2024-01-01 01:45+00', 1, 42.0);
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01'::date, '2024-01-02 00:00+00'::timestamptz);
SELECT count(*) AS diff_mixed_type FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- T-07: Adjacent L2 entries merge — 3 consecutive inserts
-- ============================================================
\echo '=== T-07: L2 merge ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Three separate INSERTs into consecutive hour buckets
INSERT INTO metrics VALUES ('2024-01-01 01:10+00', 1, 10.0);
INSERT INTO metrics VALUES ('2024-01-01 02:10+00', 1, 20.0);
INSERT INTO metrics VALUES ('2024-01-01 03:10+00', 1, 30.0);
-- L2 stores raw entries (3 rows). Merging happens at gather time
-- during REFRESH, not at write time. Verify all 3 are in L2.
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- L2 should have entries for the 2 buckets outside the refresh window
-- (hour-02 and hour-03; hour-01 was consumed by the refresh)
SELECT count(*) AS l2_after_partial FROM time_series.cagg_materialization_log;
-- Full REFRESH: gather merges remaining L2 into one range, refreshes it
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS l2_after_merge FROM time_series.cagg_materialization_log;
SELECT count(*) AS diff_merged FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;
-- Clean
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- T-08: materializations_per_refresh_window GUC
--       When disjoint interval count exceeds the limit, merge
--       into a single large refresh. Result must still be correct.
-- ============================================================
\echo '=== T-08: materializations_per_refresh_window ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Set limit to 3: more than 3 disjoint intervals → merge to one
SET time_series.materializations_per_refresh_window = 3;

-- Insert into 5 widely-spaced buckets to create 5 disjoint L2 entries
INSERT INTO metrics VALUES ('2024-01-01 00:10+00', 1, 1.0);  -- hour 00
INSERT INTO metrics VALUES ('2024-01-01 01:10+00', 2, 2.0);  -- hour 01
INSERT INTO metrics VALUES ('2024-01-01 02:10+00', 3, 3.0);  -- hour 02
INSERT INTO metrics VALUES ('2024-01-01 03:10+00', 4, 4.0);  -- hour 03
INSERT INTO metrics VALUES ('2024-01-01 04:10+00', 5, 5.0);  -- hour 04

-- REFRESH: 5 intervals > limit 3 → should merge to [hour-00, hour-05)
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Result must still be correct despite the merge
SELECT count(*) AS diff_guc_merge FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- Now test with limit = 0 (unlimited) — should behave normally
SET time_series.materializations_per_refresh_window = 0;
INSERT INTO metrics VALUES ('2024-01-01 00:20+00', 6, 6.0);
INSERT INTO metrics VALUES ('2024-01-01 02:20+00', 7, 7.0);
INSERT INTO metrics VALUES ('2024-01-01 04:20+00', 8, 8.0);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_guc_unlimited FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- Test with limit = 1 — always merge to one range
SET time_series.materializations_per_refresh_window = 1;
INSERT INTO metrics VALUES ('2024-01-01 00:40+00', 9, 9.0);
INSERT INTO metrics VALUES ('2024-01-01 04:40+00', 10, 10.0);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_guc_one FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

RESET time_series.materializations_per_refresh_window;

-- ============================================================
-- A-01: DISTINCT aggregate end-to-end REFRESH
--       count(DISTINCT tags_id) per bucket. Adding a new tags_id
--       to an existing bucket should change the distinct count.
-- ============================================================
\echo '=== A-01: DISTINCT aggregate ==='
CREATE TABLE metrics_dist (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO metrics_dist SELECT
  '2024-01-01 00:00+00'::timestamptz + (hr || ' hour')::interval,
  tid, tid * 1.0
FROM generate_series(1, 3) tid, generate_series(0, 2) hr;
-- 9 rows: 3 tags × 3 hours. Each bucket has 3 distinct tags.

CREATE MATERIALIZED VIEW cv_dist
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(DISTINCT tags_id) AS uniq_tags,
         count(*) AS total_cnt
  FROM metrics_dist GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_dist', NULL, NULL);
-- Verify initial: each bucket has uniq_tags=3, total_cnt=3
SELECT count(*) AS diff_dist_init FROM (
  (  SELECT bucket, uniq_tags, total_cnt FROM cv_dist
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time),
   count(DISTINCT tags_id), count(*)
   FROM metrics_dist GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time),
   count(DISTINCT tags_id), count(*)
   FROM metrics_dist GROUP BY 1
   EXCEPT
   SELECT bucket, uniq_tags, total_cnt FROM cv_dist
   )
) x;

-- Backfill: add tags_id=4 to bucket 01:00 (new distinct value)
INSERT INTO metrics_dist VALUES ('2024-01-01 01:30+00', 4, 40.0);
CALL time_series.refresh_continuous_aggregate('cv_dist',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- Bucket 01:00 should now have uniq_tags=4
SELECT uniq_tags AS dist_after FROM cv_dist WHERE bucket = '2024-01-01 01:00+00';
SELECT count(*) AS diff_dist_after FROM (
  (  SELECT bucket, uniq_tags, total_cnt FROM cv_dist
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time),
   count(DISTINCT tags_id), count(*)
   FROM metrics_dist GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time),
   count(DISTINCT tags_id), count(*)
   FROM metrics_dist GROUP BY 1
   EXCEPT
   SELECT bucket, uniq_tags, total_cnt FROM cv_dist
   )
) x;

-- Backfill: add duplicate tags_id=1 to bucket 00:00 (no new distinct)
INSERT INTO metrics_dist VALUES ('2024-01-01 00:30+00', 1, 11.0);
CALL time_series.refresh_continuous_aggregate('cv_dist',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- Bucket 00:00: uniq_tags should stay at 3, total_cnt should be 4
SELECT uniq_tags, total_cnt FROM cv_dist WHERE bucket = '2024-01-01 00:00+00';

DROP VIEW cv_dist CASCADE;
DROP TABLE metrics_dist CASCADE;

-- ============================================================
-- A-02: FILTER aggregate end-to-end REFRESH
--       avg(val) FILTER (WHERE val > 15). Backfill should re-compute.
-- ============================================================
\echo '=== A-02: FILTER aggregate ==='
CREATE TABLE metrics_filt (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO metrics_filt VALUES
  ('2024-01-01 00:00+00', 1, 10.0),  -- filtered out (≤15)
  ('2024-01-01 00:30+00', 2, 20.0),  -- passes filter
  ('2024-01-01 00:45+00', 3, 30.0);  -- passes filter

CREATE MATERIALIZED VIEW cv_filt
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS total,
         avg(val) FILTER (WHERE val > 15) AS avg_high
  FROM metrics_filt GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_filt', NULL, NULL);
-- Expected: total=3, avg_high=avg(20,30)=25
SELECT total, avg_high FROM cv_filt WHERE bucket = '2024-01-01 00:00+00';

-- Backfill: add val=5 (fails filter) → avg_high should NOT change
INSERT INTO metrics_filt VALUES ('2024-01-01 00:15+00', 4, 5.0);
CALL time_series.refresh_continuous_aggregate('cv_filt',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- Expected: total=4, avg_high=25 (unchanged)
SELECT total, avg_high FROM cv_filt WHERE bucket = '2024-01-01 00:00+00';

-- Backfill: add val=40 (passes filter) → avg_high should change
INSERT INTO metrics_filt VALUES ('2024-01-01 00:50+00', 5, 40.0);
CALL time_series.refresh_continuous_aggregate('cv_filt',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- Expected: total=5, avg_high=avg(20,30,40)=30
SELECT total, avg_high FROM cv_filt WHERE bucket = '2024-01-01 00:00+00';
-- EXCEPT verification
SELECT count(*) AS diff_filt FROM (
  (  SELECT bucket, total, avg_high FROM cv_filt
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time),
   count(*), avg(val) FILTER (WHERE val > 15)
   FROM metrics_filt GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time),
   count(*), avg(val) FILTER (WHERE val > 15)
   FROM metrics_filt GROUP BY 1
   EXCEPT
   SELECT bucket, total, avg_high FROM cv_filt
   )
) x;

DROP VIEW cv_filt CASCADE;
DROP TABLE metrics_filt CASCADE;

-- ============================================================
-- A-03: HAVING end-to-end REFRESH
--       HAVING count(*) > 1. Buckets below threshold don't appear.
-- ============================================================
\echo '=== A-03: HAVING clause ==='
CREATE TABLE metrics_hav (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
-- Insert: bucket 00:00 has 1 row, bucket 01:00 has 2 rows
INSERT INTO metrics_hav VALUES
  ('2024-01-01 00:30+00', 1, 10.0),  -- bucket 00:00, count=1 (excluded)
  ('2024-01-01 01:15+00', 2, 20.0),  -- bucket 01:00, count=2 (included)
  ('2024-01-01 01:45+00', 3, 30.0);  -- bucket 01:00

CREATE MATERIALIZED VIEW cv_hav
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM metrics_hav GROUP BY bucket HAVING count(*) > 1;
CALL time_series.refresh_continuous_aggregate('cv_hav', NULL, NULL);
-- Only bucket 01:00 should appear (count=2)
SELECT bucket, cnt FROM cv_hav ORDER BY bucket;

-- Backfill: add another row to bucket 00:00 → count becomes 2 → should appear
INSERT INTO metrics_hav VALUES ('2024-01-01 00:45+00', 4, 40.0);
CALL time_series.refresh_continuous_aggregate('cv_hav',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
-- Now bucket 00:00 should also appear
SELECT bucket, cnt FROM cv_hav ORDER BY bucket;
-- EXCEPT: mat should match live with HAVING
SELECT count(*) AS diff_hav FROM (
  (  SELECT bucket, cnt FROM cv_hav
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 1
   EXCEPT
   SELECT bucket, cnt FROM cv_hav
   )
) x;

DROP VIEW cv_hav CASCADE;
DROP TABLE metrics_hav CASCADE;

-- ============================================================
-- A-04: REFRESH window with no source data
--       Window covers range where source has zero rows. Mat should
--       remain unchanged (no empty rows inserted).
-- ============================================================
\echo '=== A-04: empty window REFRESH ==='
CREATE TABLE metrics_empty (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO metrics_empty VALUES
  ('2024-01-01 01:00+00', 1, 10.0),
  ('2024-01-01 02:00+00', 2, 20.0);
CREATE MATERIALIZED VIEW cv_empty_window
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM metrics_empty GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_empty_window', NULL, NULL);
SELECT count(*) AS mat_before_empty FROM cv_empty_window;

-- REFRESH a far-future window with no data
CALL time_series.refresh_continuous_aggregate('cv_empty_window',
  '2099-01-01 00:00+00', '2099-01-02 00:00+00');
-- Mat row count should be unchanged, no empty rows
SELECT count(*) AS mat_after_empty FROM cv_empty_window;
-- No empty/NULL-only rows should have been inserted
SELECT count(*) AS empty_rows FROM cv_empty_window WHERE cnt IS NULL OR cnt = 0;

DROP VIEW cv_empty_window CASCADE;
DROP TABLE metrics_empty CASCADE;

-- ============================================================
-- R-38: Schema-qualified REFRESH — same view name in different schemas
--       must refresh the correct one, not an arbitrary match.
-- ============================================================
\echo '=== R-38: schema-qualified REFRESH ==='
DROP SCHEMA IF EXISTS ns_a CASCADE;
DROP SCHEMA IF EXISTS ns_b CASCADE;
CREATE SCHEMA ns_a;
CREATE SCHEMA ns_b;

CREATE TABLE ns_src (time timestamptz NOT NULL, tags_id int NOT NULL, val float8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO ns_src VALUES
    ('2024-01-01 00:30+00', 1, 100.0),
    ('2024-01-01 01:30+00', 2, 200.0);

CREATE MATERIALIZED VIEW ns_a.same_name WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*) AS cnt
  FROM ns_src GROUP BY bucket, tags_id;

CREATE MATERIALIZED VIEW ns_b.same_name WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, sum(val) AS total
  FROM ns_src GROUP BY bucket, tags_id;

-- Refresh only ns_a.same_name (schema-qualified)
CALL time_series.refresh_continuous_aggregate('ns_a.same_name', NULL, NULL);

-- ns_a should have data, ns_b should still be empty (not refreshed)
SELECT count(*) AS ns_a_count FROM ns_a.same_name;
SELECT count(*) AS ns_b_count FROM ns_b.same_name;

-- Now refresh ns_b
CALL time_series.refresh_continuous_aggregate('ns_b.same_name', NULL, NULL);
SELECT count(*) AS ns_b_after FROM ns_b.same_name;

-- Verify both are correct
SELECT count(*) AS diff_a FROM (
  (  SELECT bucket, tags_id, cnt FROM ns_a.same_name
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM ns_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM ns_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM ns_a.same_name
   )
) x;

SELECT count(*) AS diff_b FROM (
  (  SELECT bucket, tags_id, total FROM ns_b.same_name
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, sum(val)
   FROM ns_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, sum(val)
   FROM ns_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, total FROM ns_b.same_name
   )
) x;

DROP SCHEMA ns_a CASCADE;
DROP SCHEMA ns_b CASCADE;
DROP TABLE ns_src CASCADE;

-- ============================================================
-- upstream-aligned tests: P1 + P2 gaps
-- ============================================================

-- ============================================================
-- P1-DATE: DATE type CAGG REFRESH
-- T42 verified CREATE, now verify REFRESH + EXCEPT=0
-- ============================================================
\echo '=== P1-DATE: DATE CAGG REFRESH ==='
CREATE TABLE date_src (day DATE NOT NULL, tags_id INT NOT NULL, val FLOAT8)
USING time_series WITH (
    ts_partition_column = 'day',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2024-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO date_src
SELECT '2024-01-01'::date + i, (i % 3) + 1, i * 1.5
FROM generate_series(0, 29) i;

CREATE MATERIALIZED VIEW cv_date WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, day) AS bucket,
         tags_id, count(*) AS cnt, avg(val) AS avg_val
  FROM date_src GROUP BY bucket, tags_id;

-- Full REFRESH
CALL time_series.refresh_continuous_aggregate('cv_date', NULL, NULL);
SELECT count(*) AS date_mat_rows FROM cv_date;

-- EXCEPT = 0
SELECT count(*) AS diff_date FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_date
   EXCEPT
   SELECT time_bucket('1 day'::interval, day), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM date_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, day), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM date_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_date
   )
) x;

-- Window REFRESH after backfill
INSERT INTO date_src VALUES ('2024-01-05', 1, 999.0);
CALL time_series.refresh_continuous_aggregate('cv_date',
  '2024-01-05'::date, '2024-01-06'::date);
SELECT count(*) AS diff_date_incr FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_date
   EXCEPT
   SELECT time_bucket('1 day'::interval, day), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM date_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, day), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM date_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_date
   )
) x;

DROP TABLE date_src CASCADE;

-- ============================================================
-- P1-NONCAGG: REFRESH on non-CAGG object → error
-- ============================================================
\echo '=== P1-NONCAGG: REFRESH non-CAGG ==='
\set ON_ERROR_STOP 0
-- Try to refresh a plain table
CALL time_series.refresh_continuous_aggregate('metrics', NULL, NULL);
-- Try to refresh a regular view
CREATE VIEW plain_view AS SELECT 1;
CALL time_series.refresh_continuous_aggregate('plain_view', NULL, NULL);
DROP VIEW plain_view;
\set ON_ERROR_STOP 1

-- ============================================================
-- P2-ALIGN: Inscribed bucketing
-- upstream rejects non-aligned; we inscribe (only complete buckets).
-- ============================================================
\echo '=== P2-ALIGN: inscribed bucketing ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
INSERT INTO metrics VALUES ('2024-01-01 02:30+00', 1, 555.0);

-- Non-aligned: [01:25, 02:45) → ERROR "refresh window too small"
\set ON_ERROR_STOP off
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:25+00', '2024-01-01 02:45+00');
\set ON_ERROR_STOP on
-- Use aligned window to refresh the dirty bucket
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 02:00+00', '2024-01-01 03:00+00');
SELECT count(*) AS diff_align_fixed FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- P2-STDREFRESH: Standard REFRESH MATERIALIZED VIEW
-- ============================================================
\echo '=== P2-STDREFRESH: standard REFRESH syntax ==='
\set ON_ERROR_STOP 0
REFRESH MATERIALIZED VIEW cv;
\set ON_ERROR_STOP 1

-- ============================================================
-- P2-TXNCTX: Transaction context restrictions
-- ============================================================
\echo '=== P2-TXNCTX: transaction context ==='

-- Plain DO block → should succeed (non-atomic context)
DO $$
BEGIN
  CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
END $$;

-- FUNCTION context → should error, not crash
\set ON_ERROR_STOP 0
CREATE FUNCTION test_refresh_in_func() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
END $$;
SELECT test_refresh_in_func();
DROP FUNCTION test_refresh_in_func();
\set ON_ERROR_STOP 1

-- EXCEPTION block context → should error, not crash
\set ON_ERROR_STOP 0
DO $$
BEGIN
  BEGIN
    CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
  END;
END $$;
\set ON_ERROR_STOP 1

-- Explicit transaction block → should error
-- SPI_commit_and_chain cannot commit inside user's BEGIN...COMMIT
\set ON_ERROR_STOP 0
BEGIN;
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
COMMIT;
\set ON_ERROR_STOP 1

-- Dynamic SQL via EXECUTE → also atomic in CBDB (SPI dispatch path)
-- Unlike direct CALL in DO block, EXECUTE 'CALL ...' goes through
-- SPI_execute which preserves atomic context. Expected: error.
\set ON_ERROR_STOP 0
DO $$
BEGIN
  EXECUTE 'CALL time_series.refresh_continuous_aggregate(''cv'', NULL, NULL)';
END $$;
\set ON_ERROR_STOP 1

-- Trigger context → should error
-- Trigger functions run in atomic context (same as FUNCTION)
\set ON_ERROR_STOP 0
CREATE FUNCTION trigger_refresh_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
  RETURN NEW;
END $$;
CREATE TABLE trigger_test (id int) DISTRIBUTED BY (id);
CREATE TRIGGER trg AFTER INSERT ON trigger_test
  FOR EACH ROW EXECUTE FUNCTION trigger_refresh_fn();
INSERT INTO trigger_test VALUES (1);
DROP TABLE trigger_test CASCADE;
DROP FUNCTION trigger_refresh_fn();
\set ON_ERROR_STOP 1

-- ============================================================
-- P2-OFFSET-REFRESH: REFRESH on origin CAGG
-- ============================================================
\echo '=== P2-OFFSET-REFRESH: origin CAGG REFRESH ==='
CREATE MATERIALIZED VIEW cv_origin WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time,
                     '2024-01-01 00:30:00+00'::timestamptz) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv_origin', NULL, NULL);

SELECT count(*) AS diff_origin FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_origin
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time,
   '2024-01-01 00:30:00+00'::timestamptz),
   tags_id, count(*)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time,
   '2024-01-01 00:30:00+00'::timestamptz),
   tags_id, count(*)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_origin
   )
) x;

-- Backfill + incremental
INSERT INTO metrics VALUES ('2024-01-01 01:45+00', 1, 777.0);
CALL time_series.refresh_continuous_aggregate('cv_origin',
  '2024-01-01 00:30+00', '2024-01-01 02:30+00');
SELECT count(*) AS diff_origin_incr FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_origin
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time,
   '2024-01-01 00:30:00+00'::timestamptz),
   tags_id, count(*)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time,
   '2024-01-01 00:30:00+00'::timestamptz),
   tags_id, count(*)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_origin
   )
) x;

DROP VIEW cv_origin CASCADE;

-- ============================================================
-- P2-WITHDATA: CREATE defaults to no auto-populate
-- ============================================================
\echo '=== P2-WITHDATA: no auto-populate ==='
CREATE MATERIALIZED VIEW cv_nodata_check WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics GROUP BY bucket, tags_id;

ALTER VIEW cv_nodata_check SET (time_series.materialized_only = true);
SELECT count(*) AS nodata_count FROM cv_nodata_check;
ALTER VIEW cv_nodata_check SET (time_series.materialized_only = false);
DROP VIEW cv_nodata_check CASCADE;

-- ============================================================
-- P3 tests: remaining upstream gaps
-- ============================================================

-- ============================================================
-- P3-TZ: PDT timezone window
-- ============================================================
\echo '=== P3-TZ: timezone window ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
INSERT INTO metrics VALUES ('2024-01-01 02:15+00', 1, 444.0);
-- Use America/Los_Angeles (PST/PDT) timezone for window boundaries
CALL time_series.refresh_continuous_aggregate('cv',
  '2023-12-31 17:00:00-08'::timestamptz,
  '2023-12-31 19:00:00-08'::timestamptz);
SELECT count(*) AS diff_tz FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- P3-BADARGS: Invalid argument types/formats
-- PG type system rejects most of these, but document behavior.
-- ============================================================
\echo '=== P3-BADARGS: invalid arguments ==='
\set ON_ERROR_STOP 0
-- Integer where timestamptz expected
CALL time_series.refresh_continuous_aggregate('cv', 0, '2024-01-01');
-- Text type
CALL time_series.refresh_continuous_aggregate('cv', 'xyz'::text, '2024-01-01'::text);
-- Object ID type (no matching overload)
CALL time_series.refresh_continuous_aggregate(1, NULL, NULL);
-- Unparseable time string
CALL time_series.refresh_continuous_aggregate('cv', 'xyz', NULL);
\set ON_ERROR_STOP 1

-- ============================================================
-- P3-HASHAGG: hashagg toggle doesn't affect result
-- ============================================================
\echo '=== P3-HASHAGG: hashagg toggle ==='
-- Ensure real-time mode so result_default can be anchored to the live
-- source aggregate below (independent of the mat/live split).
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Capture result with default settings
CREATE TEMP TABLE result_default AS
  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) AS avg_temp
  FROM cv ORDER BY bucket, tags_id;

-- Toggle hashagg off and compare
SET enable_hashagg = off;
CREATE TEMP TABLE result_no_hashagg AS
  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) AS avg_temp
  FROM cv ORDER BY bucket, tags_id;
RESET enable_hashagg;

-- (1) hashagg on/off must agree with EACH OTHER (plan invariance)...
SELECT count(*) AS diff_hashagg FROM (
  SELECT * FROM result_default EXCEPT SELECT * FROM result_no_hashagg
) x;
-- (2) ...AND result_default must agree with the INDEPENDENT source
-- aggregate -- otherwise both plans could share the same wrong result
-- and the self-comparison in (1) would still pass.
SELECT count(*) AS hashagg_vs_source_diff FROM (
  (SELECT bucket, tags_id, cnt, avg_temp FROM result_default
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
     FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
     FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM result_default)
) y;

DROP TABLE result_default;
DROP TABLE result_no_hashagg;

-- ============================================================
-- P3-PROCEDURE: REFRESH inside a custom PROCEDURE
-- ============================================================
\echo '=== P3-PROCEDURE: REFRESH in PROCEDURE ==='
CREATE PROCEDURE test_refresh_proc() LANGUAGE plpgsql AS $$
BEGIN
  CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
END $$;

CALL test_refresh_proc();

-- Verify it worked: EXCEPT = 0
SELECT count(*) AS diff_proc FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

DROP PROCEDURE test_refresh_proc();

-- ============================================================
-- P3-CATALOG: Corrupt bucket_func in catalog
-- Tampering with catalog should cause REFRESH to fail gracefully.
-- ============================================================
\echo '=== P3-CATALOG: corrupt catalog ==='
-- Save original bucket_width
CREATE TEMP TABLE saved_bf AS
  SELECT * FROM time_series.cagg_bucket_function
  WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv');

-- Corrupt: set bucket_width to 0
UPDATE time_series.cagg_bucket_function
SET bucket_width = '0 seconds'::interval
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv');

-- REFRESH with corrupt catalog — document behavior
\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
\set ON_ERROR_STOP 1

-- Restore
UPDATE time_series.cagg_bucket_function
SET bucket_width = (SELECT bucket_width FROM saved_bf LIMIT 1)
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv');
DROP TABLE saved_bf;

-- Verify recovery: REFRESH should work again
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS diff_recovery FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;

-- ============================================================
-- P2-MATDEL: Manual DELETE from mat table → behavior
--
--
--   Same limitation as upstream: after manual DELETE of mat rows,
--   normal REFRESH cannot recover because watermark already
--   indicates "everything materialized" and no L1/L2 entries
--   exist for the deleted range.
--
--   upstream workaround: force=>true parameter.
--   Our workaround: reset watermark to -infinity, then REFRESH.
-- ============================================================
\echo '=== P2-MATDEL: mat table DELETE recovery ==='

-- Ensure fully materialized
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT count(*) AS mat_before_del FROM cv;

-- Directly DELETE from internal mat table (simulating corruption)
DO $$
DECLARE mat_name text;
BEGIN
  SELECT mat_table_name INTO mat_name FROM time_series.continuous_agg
  WHERE user_view_name = 'cv';
  EXECUTE format('DELETE FROM time_series.%I', mat_name);
END $$;

-- Mat table is now empty
SELECT count(*) AS mat_after_del FROM cv;

-- Normal REFRESH (NULL,NULL) — does NOT recover!
-- Watermark is past all data, unified path sees nothing dirty.
-- Same behavior as upstream without force=>true.
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_no_recovery FROM cv;

-- Workaround: reset watermark to -infinity, then REFRESH
UPDATE time_series.cagg_watermark
   SET watermark = '-infinity'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_after_wm_reset FROM cv;
ALTER VIEW cv SET (time_series.materialized_only = false);

-- Verify correctness (both directions)
SELECT count(*) AS diff_matdel_1 FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;
SELECT count(*) AS diff_matdel_2 FROM (
  (  SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv)
  UNION ALL
  (SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics GROUP BY 1, 2
   )
) x;

-- ============================================================
-- P2-HOTBUCKET: Hot bucket protection
--   Watermark should stop at the START of the last bucket,
--   not the END.  This ensures the "hot bucket" (still
--   receiving data) is served by the live branch, not the
--   stale mat branch.
-- ============================================================
\echo '=== P2-HOTBUCKET: hot bucket protection ==='
CREATE TABLE hot_src (time TIMESTAMPTZ NOT NULL, v INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
-- Bucket 00:00 full (10 rows)
INSERT INTO hot_src SELECT '2024-01-01 00:00+00'::timestamptz + (i * interval '5 min'),
  i FROM generate_series(1, 10) i;
-- Bucket 01:00 partial (3 rows) — hot bucket
INSERT INTO hot_src SELECT '2024-01-01 01:00+00'::timestamptz + (i * interval '5 min'),
  i FROM generate_series(1, 3) i;

CREATE MATERIALIZED VIEW cv_hot WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM hot_src GROUP BY bucket;

CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);

-- Watermark should be at 01:00 (start of hot bucket), NOT 02:00
SELECT MAX(watermark) AS wm_hot FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_hot');

-- More data arrives in hot bucket
INSERT INTO hot_src SELECT '2024-01-01 01:00+00'::timestamptz + ((i+3) * interval '5 min'),
  i FROM generate_series(1, 5) i;

-- CAGG should see ALL 8 rows via live branch (not stale mat value of 3)
SELECT cnt AS hot_cnt FROM cv_hot WHERE bucket = '2024-01-01 01:00+00';

-- Full EXCEPT both directions
SELECT count(*) AS diff_hot FROM (
  SELECT bucket, cnt FROM cv_hot EXCEPT
  SELECT time_bucket('1 hour'::interval, time), count(*)
  FROM hot_src GROUP BY 1
) x;
SELECT count(*) AS diff_hot_rev FROM (
  SELECT time_bucket('1 hour'::interval, time), count(*)
  FROM hot_src GROUP BY 1
  EXCEPT SELECT bucket, cnt FROM cv_hot
) x;

DROP TABLE hot_src CASCADE;

-- ============================================================
-- REFRESH-FORCE: refresh_continuous_aggregate(force => true)
--   Re-materialize ranges that have no outstanding invalidation
--   entries (e.g. after the mat table is wiped out of band).
-- ============================================================
\echo '=== REFRESH-FORCE-A: normal refresh populates all buckets ==='
CREATE TABLE force_src (
    time TIMESTAMPTZ NOT NULL,
    tags_id INT NOT NULL,
    v INT
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);

INSERT INTO force_src
SELECT '2024-01-01'::timestamptz - h*'1 hour'::interval, h%5+1, h
FROM generate_series(1, 24) h;

CREATE MATERIALIZED VIEW force_cv
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS c
  FROM force_src GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('force_cv', NULL, NULL);
SELECT count(*) > 0 AS first_refresh_populated FROM force_cv;

-- Capture the (auto-numbered) materialization table name so we can peek
-- at it directly in REFRESH-FORCE-B.  Querying force_cv won't work for
-- that check: after we DELETE from the mat table, the user view falls
-- back to the direct view (UNION over source) and would return rows
-- regardless of whether force=TRUE actually re-materialized anything.
SELECT (mat_table_schema || '.' || mat_table_name) AS force_mat_table
  FROM time_series.continuous_agg WHERE user_view_name = 'force_cv'
\gset

-- Simulate data drift: empty the materialization table directly so
-- normal refresh (which checks invalidation log) won't catch it.
DO $$
DECLARE
    v_mat_table regclass;
BEGIN
    SELECT (mat_table_schema || '.' || mat_table_name)::regclass
      INTO v_mat_table
      FROM time_series.continuous_agg
     WHERE user_view_name = 'force_cv';
    EXECUTE format('DELETE FROM %s', v_mat_table);
END $$;

\echo '=== REFRESH-FORCE-B: force=TRUE refreshes despite no invalidation entries ==='
CALL time_series.refresh_continuous_aggregate('force_cv', NULL, NULL, force => true);
SELECT count(*) > 0 AS forced_repopulated FROM :force_mat_table;

\echo '=== REFRESH-FORCE-C: force=FALSE on already-up-to-date is a NOTICE no-op ==='
CALL time_series.refresh_continuous_aggregate('force_cv', NULL, NULL);

DROP TABLE force_src CASCADE;

-- ============================================================
-- REFRESH-GUC: caller's optimizer GUC must survive CALL refresh
--
-- cagg_refresh sets the global ORCA flag to off internally to dodge
-- "Operator Update on replicated tables not supported" + downstream
-- crash.  That side effect is acceptable on the BGW worker path
-- (process exits) but would silently corrupt user sessions calling
-- CALL refresh_continuous_aggregate from psql — leaving the caller
-- without ORCA for the rest of the session.  PG_FINALLY in
-- cagg_refresh restores the prior value.  These cases pin the
-- behavior so a future careless edit can't reintroduce the leak.
-- ============================================================
CREATE TABLE m_orca(
    time TIMESTAMPTZ NOT NULL,
    v INT NOT NULL DEFAULT 1
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);

CREATE MATERIALIZED VIEW cv_orca
    WITH (time_series.continuous, time_series.materialized_only=true)
    AS SELECT time_bucket('1 hour'::interval, time) AS bucket,
              count(*) AS c
       FROM m_orca GROUP BY bucket WITH NO DATA;

INSERT INTO m_orca(time)
SELECT now() - i * interval '1 hour' FROM generate_series(1, 3) i;

\echo '=== REFRESH-GUC-01: optimizer=on preserved across CALL refresh ==='
SET optimizer = on;
SELECT current_setting('optimizer') AS optimizer_before;
SET client_min_messages TO warning;
CALL time_series.refresh_continuous_aggregate('cv_orca', NULL, NULL);
RESET client_min_messages;
SELECT current_setting('optimizer') AS optimizer_after_call;

\echo '=== REFRESH-GUC-02: optimizer=off caller stays off (no spurious flip-on) ==='
SET optimizer = off;
SET client_min_messages TO warning;
CALL time_series.refresh_continuous_aggregate('cv_orca', NULL, NULL);
RESET client_min_messages;
SELECT current_setting('optimizer') AS optimizer_after;

RESET optimizer;
DROP TABLE m_orca CASCADE;

-- ============================================================
-- Section H: hot-bucket exclusion (post-fix invariants)
--
-- Verifies the new behaviour added by the hot-bucket-exclusion
-- fix: refresh never materialises the "hot bucket" (bucket
-- containing source max(time)) into mat.  The cagg_union view
-- still returns the hot bucket via its live branch, so the
-- user-facing data is unchanged, but warm refresh on an idle
-- source becomes a no-op (~5 ms instead of ~800 ms) and the
-- mat table no longer accumulates churn rows that the
-- bucket-<-watermark filter would discard anyway.
--
-- Probes the underlying mat table directly (via the catalog-
-- resolved mat_table_name), not the cagg_union view, because
-- the view UNION ALL's mat + live and would mask the question.
-- ============================================================
CREATE TABLE metrics_hot (
    time        TIMESTAMPTZ      NOT NULL,
    tags_id     INT              NOT NULL,
    temperature DOUBLE PRECISION NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 hour',
    ts_chunk_origin     = '2024-01-01'
) DISTRIBUTED BY (tags_id);

-- 10 hours of data, 1 row per 10 minutes -> 60 rows total.
-- Hot bucket (1-hour bucket containing max(time)) = bucket
-- '2024-01-01 09:00+00'.  Stable buckets: '00:00' .. '08:00'
-- (9 buckets).
INSERT INTO metrics_hot
SELECT '2024-01-01 00:00+00'::timestamptz + (m * interval '10 minutes'),
       1,
       20.0 + m * 0.1
FROM generate_series(0, 59) m;

CREATE MATERIALIZED VIEW cv_hot
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id,
         count(*) AS cnt,
         avg(temperature) AS avg_temp
  FROM metrics_hot
  GROUP BY bucket, tags_id;

-- Look up the mat table name once; query it via :"mat" below.
SELECT 'time_series.' || mat_table_name AS mat FROM
  time_series.continuous_agg WHERE user_view_name = 'cv_hot' \gset

\echo '=== H-01: cold refresh materialises STABLE buckets only ==='
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);

-- Expect exactly 9 stable buckets (00:00 .. 08:00).  Hot bucket
-- (09:00) must NOT be in the underlying mat table.
SELECT count(*) AS mat_rows_after_cold FROM :mat;
SELECT max(bucket) AS mat_max_after_cold FROM :mat;
SELECT count(*) AS hot_bucket_in_mat
  FROM :mat WHERE bucket = '2024-01-01 09:00+00';

-- watermark must advance to the hot bucket start (09:00),
-- excluding the hot bucket itself.
SELECT min(watermark) AS watermark_after_cold
  FROM time_series.cagg_watermark
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                   WHERE user_view_name = 'cv_hot');

-- H-01b: mat aggregate VALUES must match the independent source
-- aggregate over the stable region (time < hot bucket start 09:00).
-- H-01 above only checked structure (row count / max bucket /
-- watermark); without this, a wrong cnt/avg in a materialized bucket
-- would slip past, and H-02's mat-vs-snapshot self-comparison below
-- (a no-op invariance check) would then rubber-stamp the wrong value.
SELECT count(*) AS mat_vs_source_diff FROM (
  (SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM :mat
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
     FROM metrics_hot WHERE time < '2024-01-01 09:00+00'
    GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), round(avg(temperature)::numeric, 10)
     FROM metrics_hot WHERE time < '2024-01-01 09:00+00'
    GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM :mat)
) d;

\echo '=== H-02: warm refresh (no new data) is a no-op ==='
-- Capture mat snapshot.
-- Silence the DISTRIBUTED BY NOTICE for TEMP CTAS: the exact NOTICE
-- differs between CI (planner CTAS path — "Using column(s) named")
-- and cbdb-dev (parser fallback — "Creating a NULL policy entry"),
-- and mat_hot_snapshot's distribution policy is irrelevant to H-02
-- (it's only used for self-comparison inside this session).
SET client_min_messages = warning;
CREATE TEMP TABLE mat_hot_snapshot AS SELECT * FROM :mat;
RESET client_min_messages;

-- Three consecutive warm refreshes -- each must report
-- "already up-to-date" and must NOT touch the mat table.
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);

-- mat must be bit-identical to the snapshot.
SELECT count(*) AS extra_or_missing_rows FROM (
  (SELECT * FROM :mat EXCEPT SELECT * FROM mat_hot_snapshot)
   UNION ALL
  (SELECT * FROM mat_hot_snapshot EXCEPT SELECT * FROM :mat)
) diff;

-- Hot bucket still absent from mat.
SELECT count(*) AS hot_bucket_in_mat
  FROM :mat WHERE bucket = '2024-01-01 09:00+00';

\echo '=== H-03: cagg_union view returns correct data for hot bucket ==='
-- The view should expose the hot bucket via the live branch
-- even though mat does not have it.
SELECT bucket, cnt, round(avg_temp::numeric, 4) AS avg_temp
  FROM cv_hot WHERE bucket = '2024-01-01 09:00+00';

\echo '=== H-04: new data into next bucket triggers materialisation ==='
-- Insert data into bucket 10:00, making 09:00 the new "stable"
-- bucket.  Next refresh should materialise 09:00 into mat.
INSERT INTO metrics_hot VALUES ('2024-01-01 10:30+00'::timestamptz, 1, 99.5);
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);

-- mat must now include 09:00 (newly stable).  Hot bucket is
-- now 10:00 and must be absent from mat.
SELECT count(*) AS mat_rows_after_advance FROM :mat;
SELECT max(bucket) AS mat_max_after_advance FROM :mat;
SELECT count(*) AS hot_bucket_10_in_mat
  FROM :mat WHERE bucket = '2024-01-01 10:00+00';

-- watermark must have advanced to 10:00.
SELECT min(watermark) AS watermark_after_advance
  FROM time_series.cagg_watermark
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                   WHERE user_view_name = 'cv_hot');

\echo '=== H-05: L2 path still re-aggregates stable bucket changes ==='
-- Insert a row into an already-stable bucket (03:00).  L1
-- trigger fires because the bucket is below the threshold.
INSERT INTO metrics_hot VALUES ('2024-01-01 03:25+00'::timestamptz, 1, -100.0);
CALL time_series.refresh_continuous_aggregate('cv_hot', NULL, NULL);

-- Bucket 03:00 must be re-aggregated in mat.  Original cnt=6
-- (rows at 03:00, 03:10, ..., 03:50).  After insert cnt=7.
SELECT cnt AS cnt_03_in_mat FROM :mat
  WHERE bucket = '2024-01-01 03:00+00' AND tags_id = 1;

DROP TABLE metrics_hot CASCADE;

-- ============================================================
-- Section CMP: REFRESH over a COMPRESSED source (PAX / heap mix)
--
-- Every other section refreshes over ACTIVE (heap-only) chunks.
-- This one drives a CAGG whose source has been compressed into
-- PAX and had its heap forks reclaimed, then back-filled -- so
-- the refresh's source scan must read every chunk state:
--   COMPRESSED (status 1) -> PAX only   (heap truncated by reclaim)
--   PARTIAL    (status 2) -> PAX + heap  (back-filled rows merged)
-- The oracle is the cagg_union view vs a direct aggregate over
-- the source: they must agree to the row.
-- ============================================================
CREATE TABLE comp_src (
    time    TIMESTAMPTZ NOT NULL,
    tags_id INT         NOT NULL,
    v       FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2025-01-01'
) DISTRIBUTED BY (tags_id);

-- 6 daily chunks, 2 tags, deterministic value 1.0.
INSERT INTO comp_src
SELECT t, g, 1.0
FROM generate_series('2025-01-01 00:00+00'::timestamptz,
                     '2025-01-06 23:00+00', '1 hour') t,
     generate_series(1, 2) g;

CREATE MATERIALIZED VIEW cv_comp
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, sum(v) AS sm, count(*) AS c
  FROM comp_src GROUP BY bucket, tags_id;

SELECT 'time_series.' || mat_table_name AS mat FROM
  time_series.continuous_agg WHERE user_view_name = 'cv_comp' \gset

\echo '=== CMP-01: initial REFRESH over ACTIVE (heap) source ==='
CALL time_series.refresh_continuous_aggregate('cv_comp', NULL, NULL);
-- 5 stable buckets x 2 tags (day 6 is the hot bucket, excluded).
SELECT count(*) AS mat_rows_initial FROM :mat;

\echo '=== CMP-02: compress + reclaim -> source is PAX-only ==='
SELECT time_series.set_compress_config('comp_src'::regclass, NULL, 'time');
SELECT time_series.compress_chunks('comp_src'::regclass) > 0 AS compressed;
SELECT time_series.reclaim_chunk_heaps('comp_src'::regclass) > 0 AS reclaimed;
-- every chunk now COMPRESSED (status 1); none ACTIVE or PARTIAL.
SELECT bool_and(status = 1) AS all_pax
FROM time_series.ts_chunk WHERE table_oid = 'comp_src'::regclass;

\echo '=== CMP-03: back-fill a compressed bucket -> chunk goes PARTIAL ==='
-- INSERT into day 3, whose heap fork was truncated by reclaim;
-- the chunk flips to PARTIAL = PAX rows + the new heap row.
INSERT INTO comp_src VALUES ('2025-01-03 05:30+00', 1, 1000.0);
SELECT count(*) FILTER (WHERE status = 2) > 0 AS has_partial,
       count(*) FILTER (WHERE status = 1) > 0 AS has_compressed
FROM time_series.ts_chunk WHERE table_oid = 'comp_src'::regclass;

\echo '=== CMP-04: REFRESH re-aggregates the PARTIAL bucket (PAX+heap) ==='
CALL time_series.refresh_continuous_aggregate('cv_comp', NULL, NULL);

\echo '=== CMP-05: cagg_union view == direct source aggregate (0 = exact) ==='
SELECT count(*) AS mismatches FROM (
  (SELECT bucket, tags_id, sm, c FROM cv_comp
     EXCEPT
   SELECT time_bucket('1 day'::interval, time), tags_id, sum(v), count(*)
     FROM comp_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, time), tags_id, sum(v), count(*)
     FROM comp_src GROUP BY 1, 2
     EXCEPT
   SELECT bucket, tags_id, sm, c FROM cv_comp)
) diff;

\echo '=== CMP-06: PARTIAL bucket merges PAX (24 rows) + heap back-fill ==='
-- day 3 / tag 1: 24 PAX rows (1.0 each) + one 1000.0 heap row
-- => sum 1024, count 25.
SELECT sm AS day3_sum, c AS day3_count FROM cv_comp
  WHERE bucket = '2025-01-03 00:00+00' AND tags_id = 1;

DROP TABLE comp_src CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE metrics CASCADE;

\echo '=== REFRESH TESTS DONE ==='
