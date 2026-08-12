-- ============================================================
-- cagg_invalidation.sql
-- Test: Invalidation Trigger + Threshold filtering
--
-- Covers: I-01 ~ I-20 from cagg_test_tracking.md
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- Setup: source table
CREATE TABLE metrics (
    time        TIMESTAMPTZ       NOT NULL,
    tags_id     INT               NOT NULL,
    temperature DOUBLE PRECISION  NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);

-- Insert initial data: 50 rows, 10 tags × 5 hourly buckets
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz
        + (hr || ' hour')::interval
        + (m * 5 || ' minute')::interval,
       tid, 20.0 + tid + hr * 0.5 + m * 0.1
FROM generate_series(1, 10) tid,    -- 10 tags
     generate_series(0, 9)  hr,      -- 10 hours
     generate_series(1, 10) m;       -- 10 rows per (tag, hour)
-- 10 × 10 × 10 = 1000 rows; count=10/cell, 100 groups

-- Create CAGG
CREATE MATERIALIZED VIEW cv_hourly
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id,
         count(*) AS cnt,
         avg(temperature) AS avg_temp
  FROM metrics
  GROUP BY bucket, tags_id;

-- ============================================================
-- I-12: CREATE 后 watermark = -infinity（每 segment 一行）
-- ============================================================
\echo '=== I-12: initial watermark ==='
SELECT count(*) > 0 AS has_watermark FROM time_series.cagg_watermark;

-- ============================================================
-- I-01/I-02: Before REFRESH, watermark = -infinity
--            ALL inserts are >= threshold → L1 should be EMPTY
-- ============================================================
\echo '=== I-01/I-02: L1 empty before first REFRESH ==='
INSERT INTO metrics VALUES ('2024-01-01 02:30+00', 1, 99.0);
-- watermark = -infinity → all inserts >= threshold → L1 must be empty
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- Full REFRESH to set watermark
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

\echo '=== I-13: watermark advanced after REFRESH ==='
SELECT bool_and(watermark > '-infinity'::timestamptz) AS all_wm_advanced
FROM time_series.cagg_watermark;

-- ============================================================
-- I-01: INSERT below watermark → L1 has entry with correct range
-- ============================================================
\echo '=== I-01: INSERT below watermark → L1 ==='
INSERT INTO metrics VALUES ('2024-01-01 01:15+00', 2, 88.0);
-- L1 should have exactly one entry with the inserted time
SELECT lowest_modified, greatest_modified
FROM time_series.cagg_invalidation_log
ORDER BY lowest_modified;

-- ============================================================
-- I-02: INSERT above watermark → L1 empty (skip)
-- ============================================================
\echo '=== I-02: INSERT above watermark → no L1 ==='
-- Clean L1 first via REFRESH
CALL time_series.refresh_continuous_aggregate('cv_hourly', '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- Insert data in the far future (definitely above watermark)
-- 2099 originally, but 4-hour chunks + 2020 origin can't reach that far
-- without exceeding UINT16_MAX chunk count.  2025-12 is enough above the
-- 2024-01-01 watermark this test compares against.
INSERT INTO metrics VALUES ('2025-12-31 23:00+00', 3, 77.0);
-- L1 must be empty (future data >= threshold → skip)
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- ============================================================
-- I-02b: INSERT at exact watermark boundary → should NOT trigger L1
--        (trigger condition is ts < threshold, not ts <= threshold)
-- ============================================================
\echo '=== I-02b: INSERT at exact watermark → no L1 ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Set a deterministic watermark for boundary testing
UPDATE time_series.cagg_watermark
   SET watermark = '2024-06-01 00:00:00+00'::timestamptz;
INSERT INTO metrics VALUES ('2024-06-01 00:00:00+00', 1, 50.0);
-- ts = threshold exactly → ts < threshold is FALSE → no L1
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;
-- Restore: full refresh to advance watermark back to now()
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-03a: Multiple INSERTs in one transaction → L1 accumulates
-- ============================================================
\echo '=== I-03a: multi-INSERT in txn → L1 accumulates ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
BEGIN;
INSERT INTO metrics VALUES ('2024-01-01 00:45+00', 3, 30.0);
INSERT INTO metrics VALUES ('2024-01-01 01:45+00', 4, 40.0);
INSERT INTO metrics VALUES ('2024-01-01 02:45+00', 5, 50.0);
COMMIT;
-- All 3 below watermark → 3 L1 entries with exact times
SELECT lowest_modified, greatest_modified
FROM time_series.cagg_invalidation_log
ORDER BY lowest_modified;

-- ============================================================
-- I-04a: INSERT INTO ... SELECT below watermark → L1 per row
-- ============================================================
\echo '=== I-04a: INSERT INTO SELECT → L1 per row ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz + (i || ' minute')::interval,
       i % 5 + 1, 10.0 + i
FROM generate_series(1, 5) i;
-- 5 rows, all below watermark → 5 L1 entries
SELECT lowest_modified
FROM time_series.cagg_invalidation_log
ORDER BY lowest_modified;

-- ============================================================
-- I-05a: INSERT rows spanning multiple buckets → L1 per row
--        with distinct lowest_modified values
-- ============================================================
\echo '=== I-05a: INSERT multi-bucket → distinct L1 entries ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
INSERT INTO metrics VALUES
    ('2024-01-01 00:10+00', 1, 10.0),  -- bucket 00:00
    ('2024-01-01 01:10+00', 2, 20.0),  -- bucket 01:00
    ('2024-01-01 02:10+00', 3, 30.0);  -- bucket 02:00
-- Each row produces L1 with its own time → 3 distinct values
SELECT lowest_modified, greatest_modified
FROM time_series.cagg_invalidation_log
ORDER BY lowest_modified;

-- ============================================================
-- I-05a-DATA-PLANE: end-to-end verification that I-01..I-05a's L1
-- writes actually drive a correct refresh.  Without this, the
-- preceding tests only verify L1 has plausible-looking entries
-- (judge = athlete: trigger writes L1, test reads L1).  Catches:
--   * off-by-one ranges in L1 that look right but address wrong
--     buckets (regression for commit 76072f03 cagg_trim_l2 bug)
--   * refresh skipping L1 entries silently
--   * L1 row visibility bugs across the trigger→refresh hand-off
-- ============================================================
\echo '=== I-05a-DATA-PLANE: REFRESH + mat ↔ src EXCEPT ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Full-range EXCEPT both directions.  I-02 INSERTs at 2025-12-31
-- and I-02b at 2024-06-01 deliberately land outside the main
-- [2024-01-01, 2024-01-02) window of the fixture; mat ↔ src parity
-- still has to hold across all of them.
SELECT count(*) AS i05_post_refresh_diff FROM (
    (SELECT bucket, tags_id, cnt FROM cv_hourly
     EXCEPT
     SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2)
    UNION ALL
    (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2
     EXCEPT
     SELECT bucket, tags_id, cnt FROM cv_hourly)
) x;

-- ============================================================
-- I-06: TRUNCATE → L1 has {-infinity, +infinity}
-- ============================================================
\echo '=== I-06: TRUNCATE → full-range L1 ==='
-- Build a known dataset: 20 rows, 4 tags × 5 hourly buckets → 20 mat rows
TRUNCATE metrics;
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz + (hr || ' hour')::interval,
       tid, 20.0 + tid + hr
FROM generate_series(1, 4) tid, generate_series(0, 4) hr;
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Pre-check: mat table has data
SELECT count(*) > 0 AS mat_has_data FROM cv_hourly;
-- Now TRUNCATE and verify
TRUNCATE metrics;
-- Verify L1 has exactly one full-range entry
SELECT lowest_modified = '-infinity'::timestamptz AS is_neg_inf,
       greatest_modified = 'infinity'::timestamptz AS is_pos_inf
FROM time_series.cagg_invalidation_log;

-- REFRESH to process invalidation, then verify mat table empty
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM cv_hourly) AS mat_has_rows_after_truncate;

-- ============================================================
-- Re-populate for remaining tests
-- ============================================================
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz
        + (hr || ' hour')::interval
        + (m * 5 || ' minute')::interval,
       tid, 20.0 + tid + hr * 0.5 + m * 0.1
FROM generate_series(1, 10) tid,    -- 10 tags
     generate_series(0, 9)  hr,      -- 10 hours
     generate_series(1, 10) m;       -- 10 rows per (tag, hour)
-- 10 × 10 × 10 = 1000 rows; count=10/cell, 100 groups
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-07: Transaction rollback → L1 invisible
-- ============================================================
\echo '=== I-07: rollback → no L1 ==='
BEGIN;
INSERT INTO metrics VALUES ('2024-01-01 02:45+00', 4, 55.0);
ROLLBACK;
-- L1 must be empty after rollback (MVCC)
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- ============================================================
-- I-08: Batch INSERT mixed (some above, some below watermark)
-- ============================================================
\echo '=== I-08: mixed batch INSERT ==='
INSERT INTO metrics VALUES
    ('2024-01-01 01:10+00', 5, 11.0),   -- below watermark → L1
    ('2024-01-01 03:20+00', 6, 22.0),   -- below watermark → L1
    ('2025-06-15 00:00+00', 7, 33.0);   -- above watermark → skip (was 2099)
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;
-- Verify L1 recorded the correct two timestamps (not the future one)
SELECT lowest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;

-- Clean up
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-08-DATA-PLANE: covers I-07 (rollback) + I-08 (mixed batch)
-- Verify mat ↔ src parity end-to-end.  Without this, I-07/I-08
-- only verify L1 table contents (judge = athlete: trigger writes
-- L1, test reads L1).  This pin checks that the L1 rows actually
-- drive refresh correctly: rollback didn't sneak rows into mat,
-- and the mixed batch's below-watermark INSERTs are materialized.
-- ============================================================
\echo '=== I-08-DATA-PLANE: REFRESH + mat ↔ src EXCEPT ==='
SELECT count(*) AS i08_post_refresh_diff FROM (
    (SELECT bucket, tags_id, cnt FROM cv_hourly
     EXCEPT
     SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2)
    UNION ALL
    (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2
     EXCEPT
     SELECT bucket, tags_id, cnt FROM cv_hourly)
) x;

-- ============================================================
-- I-09 / I-09b removed: NULL time-column rows are not supported on
-- hypertable sources.  ts_heap_route_to_chunk() rejects NULL with
-- ERROR "time-series column cannot be NULL" before any invalidation
-- code runs, so the heap-table semantics ("skip NULL rows silently")
-- no longer apply.  The rejection itself is exercised by basic
-- INSERT NOT NULL coverage in the access-method layer.
-- ============================================================

-- ============================================================
-- I-10: ALTER TABLE ADD COLUMN → trigger still works
-- ============================================================
\echo '=== I-10: ADD COLUMN → trigger OK ==='
ALTER TABLE metrics ADD COLUMN extra_col TEXT;
INSERT INTO metrics (time, tags_id, temperature) VALUES ('2024-01-01 01:50+00', 8, 66.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;
ALTER TABLE metrics DROP COLUMN extra_col;

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-11: ALTER TABLE DROP COLUMN (non-time) → trigger still works
-- ============================================================
\echo '=== I-11: DROP COLUMN → trigger OK ==='
ALTER TABLE metrics ADD COLUMN temp_col INT;
ALTER TABLE metrics DROP COLUMN temp_col;
INSERT INTO metrics VALUES ('2024-01-01 02:50+00', 9, 77.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-11-DATA-PLANE: covers I-10 + I-11 schema-change INSERTs.
-- L1 inspection alone could hide a bug where ADD/DROP COLUMN
-- leaves the trigger with a stale tupdesc that writes the wrong
-- time into L1 (looks right in the log but refresh materializes
-- a different bucket).  End-to-end EXCEPT catches that.
-- ============================================================
\echo '=== I-11-DATA-PLANE: REFRESH + mat ↔ src EXCEPT ==='
SELECT count(*) AS i11_post_refresh_diff FROM (
    (SELECT bucket, tags_id, cnt FROM cv_hourly
     EXCEPT
     SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2)
    UNION ALL
    (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
     FROM metrics GROUP BY 1, 2
     EXCEPT
     SELECT bucket, tags_id, cnt FROM cv_hourly)
) x;

-- ============================================================
-- I-12a (P0): DROP COLUMN bucket column (used by time_bucket in CAGG)
--             must be rejected.  If pg_depend tracking misses this,
--             user could DROP the time column, leaving the CAGG
--             pointing at a non-existent column → every future
--             INSERT triggers an invalidation trigger that fails to
--             read NEW.time → ERROR or data corruption.
-- ============================================================
\echo '=== I-12a: DROP bucket column rejected (CAGG depends on it) ==='
\set ON_ERROR_STOP 0
ALTER TABLE metrics DROP COLUMN time;
\set ON_ERROR_STOP 1

-- Source still functional?
INSERT INTO metrics (time, tags_id, temperature)
  VALUES ('2024-01-01 04:00+00', 10, 99.0);
-- CAGG still functional?
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT count(*) > 0 AS cv_hourly_still_works FROM cv_hourly;

-- ============================================================
-- I-12b (P0): DROP COLUMN aggregate column (used by avg/count in CAGG)
--             must be rejected with the same reasoning.
-- ============================================================
\echo '=== I-12b: DROP aggregate column rejected ==='
\set ON_ERROR_STOP 0
ALTER TABLE metrics DROP COLUMN temperature;
\set ON_ERROR_STOP 1

INSERT INTO metrics (time, tags_id, temperature)
  VALUES ('2024-01-01 04:05+00', 11, 100.0);
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT count(*) > 0 AS cv_hourly_still_works_2 FROM cv_hourly;

-- ============================================================
-- I-12c (P0): DROP COLUMN unrelated column must still succeed
--             (positive control — proves the dependency tracker is
--             not over-aggressive and rejecting innocent ALTERs).
-- ============================================================
\echo '=== I-12c: DROP unrelated column allowed ==='
ALTER TABLE metrics ADD COLUMN dummy_col TEXT;
ALTER TABLE metrics DROP COLUMN dummy_col;
INSERT INTO metrics (time, tags_id, temperature)
  VALUES ('2024-01-01 04:10+00', 12, 101.0);
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT count(*) > 0 AS unrelated_drop_ok FROM cv_hourly;

-- ============================================================
-- AB (P0): abort safety — L1 must NEVER leak from a failed or
--          aborted transaction.
--
-- BEFORE-trigger deferred-judgment design: the per-backend batch
-- accumulates min/max in memory; the L1 row is written only at
-- PRE_COMMIT (cagg_l1_batch_flush) and DISCARDED on ABORT
-- (cagg_l1_xact_callback, XACT_EVENT_ABORT).  So "L1 written but the
-- insert later failed" must be impossible: a failed insert aborts the
-- txn, which discards the batch before any L1 is written.
--
-- Uses a self-contained table so leftover rows cannot perturb the
-- watermark-sensitive I-14+ sections below.
-- ============================================================
\echo '=== AB-00: abort safety setup ==='
CREATE TABLE abrt_src (time TIMESTAMPTZ NOT NULL, tags_id INT, val FLOAT8)
USING time_series WITH (ts_partition_column = 'time',
                        ts_chunk_interval = '1 hour',
                        ts_chunk_origin = '2024-01-01')
DISTRIBUTED BY (tags_id);
INSERT INTO abrt_src
SELECT '2024-01-01 00:00+00'::timestamptz + (h || ' hour')::interval, 1, h
FROM generate_series(0, 5) h;
CREATE MATERIALIZED VIEW cv_abrt WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*) AS cnt
  FROM abrt_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_abrt', NULL, NULL);
-- Baseline: watermark now ~06:00, L1 empty for this source.
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
              WHERE source_table_oid = 'abrt_src'::regclass) AS l1_empty_baseline;

\echo '=== AB-01: accumulate, then a SUBSEQUENT insert fails → no L1, nothing lands ==='
-- Row 1 (below watermark) is accumulated by the BEFORE trigger; the
-- SUBSEQUENT insert (NULL time) is rejected by the tableam, aborting the
-- whole txn.  The accumulated batch must be discarded.
\set ON_ERROR_STOP 0
BEGIN;
INSERT INTO abrt_src VALUES ('2024-01-01 02:30+00', 1, 99.0);
INSERT INTO abrt_src VALUES (NULL, 1, 98.0);
COMMIT;
\set ON_ERROR_STOP 1
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
              WHERE source_table_oid = 'abrt_src'::regclass) AS l1_after_failed_insert;
SELECT count(*) AS rows_landed FROM abrt_src WHERE val IN (99.0, 98.0);

\echo '=== AB-02: cross-transaction ghost — aborted batch must not leak ==='
-- Txn A accumulates a below-watermark range, then ABORTs.
BEGIN;
INSERT INTO abrt_src VALUES ('2024-01-01 03:30+00', 1, 97.0);
ABORT;
-- Next txn on the SAME backend writes an ABOVE-watermark row (no L1 of
-- its own) and commits, forcing the PRE_COMMIT flush path.  If A's abort
-- failed to discard the per-backend batch, A's range would surface here
-- as a ghost L1 entry.  It must not.  (12:00 is above the ~06:00 watermark
-- yet within the per-segment chunk-count limit — 2099 would overflow it.)
INSERT INTO abrt_src VALUES ('2024-01-01 12:00+00', 1, 96.0);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
              WHERE source_table_oid = 'abrt_src'::regclass) AS l1_ghost_after_abort;
SELECT count(*) AS aborted_row_landed FROM abrt_src WHERE val = 97.0;

DROP TABLE abrt_src CASCADE;

-- ============================================================
-- I-14/I-15: Multiple CAGGs on same source → threshold = MAX
-- ============================================================
\echo '=== I-14/I-15: multi-CAGG threshold ==='
CREATE MATERIALIZED VIEW cv_daily
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics
  GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv_daily', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- Set deterministic watermarks: hourly = March, daily = June
-- threshold should be MAX = June
UPDATE time_series.cagg_watermark
   SET watermark = '2024-03-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_hourly');
UPDATE time_series.cagg_watermark
   SET watermark = '2024-06-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_daily');

-- Case A: INSERT between the two watermarks (April)
-- If threshold = MAX(June) → April < June → L1 ✓
-- If threshold = MIN(March) → April > March → skip ✗
\echo '=== I-14a: INSERT between two watermarks ==='
INSERT INTO metrics VALUES ('2024-04-01 00:00+00', 1, 100.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;

-- Case B: INSERT below BOTH watermarks (February) → always L1
\echo '=== I-14b: INSERT below both watermarks ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Re-set watermarks (REFRESH advanced them)
UPDATE time_series.cagg_watermark
   SET watermark = '2024-03-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_hourly');
UPDATE time_series.cagg_watermark
   SET watermark = '2024-06-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_daily');
INSERT INTO metrics VALUES ('2024-02-01 00:00+00', 2, 200.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;

-- Case C: INSERT above BOTH watermarks (July) → skip
\echo '=== I-14c: INSERT above both watermarks ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Re-set watermarks again
UPDATE time_series.cagg_watermark
   SET watermark = '2024-03-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_hourly');
UPDATE time_series.cagg_watermark
   SET watermark = '2024-06-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_daily');
INSERT INTO metrics VALUES ('2024-07-01 00:00+00', 3, 300.0);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

DROP VIEW cv_daily CASCADE;

-- Restore watermark
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- I-16: Different source tables → independent thresholds
-- ============================================================
\echo '=== I-16: different sources → independent thresholds ==='
CREATE TABLE metrics2 (
    time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO metrics2 SELECT '2024-06-01'::timestamptz + (i * interval '1 hour'), i%3+1, i
FROM generate_series(1,10) i;

CREATE MATERIALIZED VIEW cv2
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*)
  FROM metrics2 GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- Set different watermarks: cv_hourly = March, cv2 = August
UPDATE time_series.cagg_watermark
   SET watermark = '2024-03-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv_hourly');
UPDATE time_series.cagg_watermark
   SET watermark = '2024-08-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv2');

-- Case A: INSERT into metrics2, below cv2 watermark (June < August) → L1
\echo '=== I-16a: INSERT below cv2 watermark → L1 ==='
INSERT INTO metrics2 VALUES ('2024-06-01 03:30+00', 1, 999.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log ORDER BY lowest_modified;
-- Verify L1 points to metrics2, not metrics
SELECT l.source_table_oid = 'metrics2'::regclass AS is_metrics2
FROM time_series.cagg_invalidation_log l;

-- Case B: INSERT into metrics2, above cv2 watermark (Sept > August) → skip
-- This also proves cv2 uses its own watermark, not cv_hourly's
\echo '=== I-16b: INSERT above cv2 watermark → no new L1 ==='
CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);
UPDATE time_series.cagg_watermark
   SET watermark = '2024-08-01 00:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv2');
INSERT INTO metrics2 VALUES ('2024-09-01 00:00+00', 2, 888.0);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- Case C: Cross-isolation — INSERT into metrics below cv_hourly watermark
-- should NOT produce L1 with metrics2's source_table_oid
\echo '=== I-16c: INSERT into metrics → L1 only for metrics ==='
INSERT INTO metrics VALUES ('2024-01-15 00:00+00', 1, 111.0);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics2'::regclass) AS l1_has_rows;

DROP VIEW cv2 CASCADE;
DROP TABLE metrics2 CASCADE;

-- ============================================================
-- P0: TIMESTAMP (without timezone) source table
-- ============================================================
\echo '=== P0: TIMESTAMP type source ==='
CREATE TABLE metrics_ts (
    time TIMESTAMP NOT NULL, tags_id INT NOT NULL, val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO metrics_ts SELECT '2024-01-01'::timestamp + (i * interval '1 hour'), i%3+1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_ts
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*)
  FROM metrics_ts GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv_ts', NULL, NULL);
-- INSERT below watermark — tests TIMESTAMP→TIMESTAMPTZ conversion in trigger
INSERT INTO metrics_ts VALUES ('2024-01-01 03:30', 1, 999.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics_ts'::regclass ORDER BY lowest_modified;

DROP VIEW cv_ts CASCADE;
DROP TABLE metrics_ts CASCADE;

-- ============================================================
-- P0: DATE source table
-- ============================================================
\echo '=== P0: DATE type source ==='
CREATE TABLE metrics_date (
    day DATE NOT NULL, tags_id INT NOT NULL, val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'day',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2024-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO metrics_date SELECT '2024-01-01'::date + i, i%3+1, i
FROM generate_series(1, 30) i;

CREATE MATERIALIZED VIEW cv_date
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, day) AS bucket, tags_id, count(*)
  FROM metrics_date GROUP BY bucket, tags_id;

CALL time_series.refresh_continuous_aggregate('cv_date', NULL, NULL);
-- INSERT below watermark — tests DATE→TIMESTAMPTZ conversion in trigger
INSERT INTO metrics_date VALUES ('2024-01-15', 1, 999.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics_date'::regclass ORDER BY lowest_modified;

DROP VIEW cv_date CASCADE;
DROP TABLE metrics_date CASCADE;

-- ============================================================
-- P0: Large batch INSERT (1000 rows) — trigger scalability
-- ============================================================
\echo '=== P0: large batch INSERT ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz + (i || ' second')::interval,
       i % 10 + 1, i * 0.1
FROM generate_series(1, 1000) i;
SELECT count(*) AS l1_large_batch FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass; -- count is appropriate for 1000-row batch
-- All 1000 rows below watermark → expect 1000

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P1: Extreme timestamps — epoch and very old dates
--
-- Verify invalidation records very old / pre-Y2K timestamps
-- correctly to L1.  Uses a dedicated table because the project's
-- standard ts_chunk_origin = '2020-01-01' + ts_chunk_interval =
-- '1 day' can't accommodate 1970 data (origin too late) without
-- exceeding the per-table chunk-count limit (UINT16_MAX) when
-- combined with future-dated data elsewhere in the same table.
-- Solution: dedicated table with origin = 1900 + interval = 1 month
-- (1900..2099 = ~2400 chunks, well below UINT16_MAX).
-- ============================================================
\echo '=== P1: extreme timestamps ==='
CREATE TABLE metrics_extreme (
    time        TIMESTAMPTZ       NOT NULL,
    tags_id     INT               NOT NULL,
    val         DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 month',
    ts_chunk_origin     = '1900-01-01'
) DISTRIBUTED BY (tags_id);

CREATE MATERIALIZED VIEW cv_extreme
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics_extreme
  GROUP BY bucket, tags_id;

-- Establish a watermark far above the epoch dates so the L1
-- below-threshold check accepts them.
INSERT INTO metrics_extreme VALUES ('2024-01-01 00:00+00', 1, 0.0);
CALL time_series.refresh_continuous_aggregate('cv_extreme', NULL, NULL);

INSERT INTO metrics_extreme VALUES
    ('1970-01-01 00:00:00+00', 1, 0.0),   -- Unix epoch
    ('2000-01-01 00:00:00+00', 2, 1.0),   -- Y2K
    ('1999-12-31 23:59:59+00', 3, 2.0);   -- Pre-Y2K
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics_extreme'::regclass
 ORDER BY lowest_modified;
-- All 3 below watermark.  L1 is per-segment + batched within a
-- transaction (one [min, max] entry per segment), so the row
-- count reflects how MPP distributed tags_id 1/2/3, not the
-- raw INSERT count.
-- Verify the epoch value was recorded correctly
SELECT lowest_modified = '1970-01-01 00:00:00+00'::timestamptz AS has_epoch
FROM time_series.cagg_invalidation_log
WHERE lowest_modified = '1970-01-01 00:00:00+00'::timestamptz;

DROP VIEW cv_extreme CASCADE;
DROP TABLE metrics_extreme CASCADE;
-- ============================================================

-- ============================================================
-- P0: REFRESH end-to-end: INSERT → L1 → REFRESH → mat updated → L1 cleared
-- ============================================================
\echo '=== P0: REFRESH end-to-end ==='
-- Start clean: known dataset
TRUNCATE metrics;
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz + (hr || ' hour')::interval,
       tid, 20.0 + tid + hr
FROM generate_series(1, 3) tid, generate_series(0, 2) hr;
-- 9 rows: 3 tags × 3 hours → 9 mat rows
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM cv_hourly) AS mat_has_data;
-- Verify L1 is clean after REFRESH
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass) AS l1_has_rows;

-- Now INSERT new data below watermark → triggers L1
INSERT INTO metrics VALUES ('2024-01-01 01:30+00', 1, 99.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;

-- REFRESH should: move L1→L2, materialize the changed bucket, clear L1
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Mat table should still have data after re-refresh
SELECT EXISTS(SELECT 1 FROM cv_hourly) AS mat_has_data;
-- L1 must be empty after REFRESH
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass) AS l1_has_rows;
-- Verify the refreshed data is correct: tag_id=1 hour-1 bucket should have cnt=2
SELECT cnt AS tag1_hour1_cnt FROM cv_hourly
 WHERE bucket = '2024-01-01 01:00+00' AND tags_id = 1;

-- ============================================================
-- P0: Threshold cache within transaction
--     trigger caches threshold per-txn; REFRESH in same txn changes
--     watermark but cached_threshold is stale → INSERT may use old value
-- ============================================================
\echo '=== P0: threshold cache within txn ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Set watermark to a known value
UPDATE time_series.cagg_watermark
   SET watermark = '2024-06-01 00:00:00+00'::timestamptz;
-- In a single transaction: INSERT below wm → L1, then INSERT above wm → skip
BEGIN;
INSERT INTO metrics VALUES ('2024-03-01 00:00+00', 1, 10.0);  -- below June → L1
INSERT INTO metrics VALUES ('2024-09-01 00:00+00', 2, 20.0);  -- above June → skip
COMMIT;
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
-- Expect 1: March is below, September is above, both use same cached threshold

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P1: Partial REFRESH: L1→L2 moves ALL entries, but only refreshes
--     the specified window. L2 entries outside the window remain
--     for the next REFRESH to pick up.
-- ============================================================
\echo '=== P1: partial REFRESH range ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Insert into two different hour buckets
INSERT INTO metrics VALUES ('2024-01-01 01:30+00', 1, 10.0);  -- hour 01
INSERT INTO metrics VALUES ('2024-01-01 03:30+00', 2, 20.0);  -- hour 03
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
-- Partial REFRESH covering only hour 01-02 range
CALL time_series.refresh_continuous_aggregate('cv_hourly',
    '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- L1 is fully moved to L2 (all entries), so L1 = 0
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass) AS l1_has_rows;
-- L2 should still have the hour-03 entry (outside refresh window)
SELECT lowest_modified, greatest_modified FROM time_series.cagg_materialization_log ORDER BY lowest_modified;
-- Full REFRESH now should process the remaining L2 entry
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log) AS l2_has_rows;

-- ============================================================
-- P1: Double TRUNCATE (second TRUNCATE on empty table)
-- ============================================================
\echo '=== P1: double TRUNCATE ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
TRUNCATE metrics;
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
-- Second TRUNCATE on already-empty table — should not crash, still writes L1
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
TRUNCATE metrics;
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;

-- Re-populate for remaining tests
INSERT INTO metrics
SELECT '2024-01-01 00:00+00'::timestamptz
        + (hr || ' hour')::interval
        + (m * 5 || ' minute')::interval,
       tid, 20.0 + tid + hr * 0.5 + m * 0.1
FROM generate_series(1, 10) tid,    -- 10 tags
     generate_series(0, 9)  hr,      -- 10 hours
     generate_series(1, 10) m;       -- 10 rows per (tag, hour)
-- 10 × 10 × 10 = 1000 rows; count=10/cell, 100 groups
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P1: Per-segment threshold independence
--     cagg_watermark is DISTRIBUTED RANDOMLY (one row per segment per CAGG).
--     Verify that segments compute threshold from local watermark rows.
-- ============================================================
\echo '=== P1: per-segment threshold ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- All segment watermarks should be identical after full REFRESH
SELECT count(DISTINCT watermark) AS distinct_wm_values
FROM time_series.cagg_watermark;
-- INSERT below watermark → L1 count should match the number of segments
-- that received the row (exactly 1 segment for a specific tags_id hash)
INSERT INTO metrics VALUES ('2024-01-01 01:00+00', 1, 10.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P0: Time column NOT first in table — attnum lookup
--     Trigger must find the time column by name, not by position.
--     If cagg_get_time_attnum has an off-by-one, this catches it.
-- ============================================================
\echo '=== P0: time column not first ==='
CREATE TABLE metrics_late_time (
    tags_id     INT               NOT NULL,
    sensor_name TEXT              NOT NULL,
    time        TIMESTAMPTZ       NOT NULL,
    val         DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO metrics_late_time
SELECT i % 5 + 1, 'sensor_' || i,
       '2024-01-01 00:00+00'::timestamptz + (i || ' hour')::interval, i * 1.1
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_late_time
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics_late_time
  GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_late_time', NULL, NULL);

-- INSERT below watermark — time is the 3rd column
INSERT INTO metrics_late_time VALUES (1, 'test', '2024-01-01 03:30+00', 99.9);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics_late_time'::regclass ORDER BY lowest_modified;
-- Verify L1 recorded the correct time value (not tags_id or some other column)
SELECT lowest_modified = '2024-01-01 03:30:00+00'::timestamptz AS time_col_ok
FROM time_series.cagg_invalidation_log
WHERE source_table_oid = 'metrics_late_time'::regclass;

DROP VIEW cv_late_time CASCADE;
DROP TABLE metrics_late_time CASCADE;

-- ============================================================
-- P1: Multiple rows with identical timestamp
--     Each row should produce its own L1 entry (no dedup).
-- ============================================================
\echo '=== P1: duplicate timestamps ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
INSERT INTO metrics VALUES
    ('2024-01-01 01:00+00', 1, 10.0),
    ('2024-01-01 01:00+00', 2, 20.0),
    ('2024-01-01 01:00+00', 3, 30.0);
SELECT lowest_modified, greatest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
-- All 3 rows have same time but different tags → 3 separate L1 entries

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P1: Watermark monotonicity — partial REFRESH with earlier window
--     should NOT decrease watermark (GREATEST semantics).
-- ============================================================
\echo '=== P1: watermark monotonicity ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Record current watermark
SELECT watermark AS wm_before
FROM time_series.cagg_watermark LIMIT 1 \gset
-- Insert in early bucket to create L1, then partial refresh a small window
INSERT INTO metrics VALUES ('2024-01-01 01:30+00', 1, 10.0);
CALL time_series.refresh_continuous_aggregate('cv_hourly',
    '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- Watermark must NOT have decreased
SELECT bool_and(watermark >= :'wm_before'::timestamptz) AS wm_not_decreased
FROM time_series.cagg_watermark;

-- Clean
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);

-- ============================================================
-- P1: REFRESH idempotent — two consecutive REFRESHes with no new data
--     second REFRESH should be a no-op (mat unchanged, no errors).
-- ============================================================
\echo '=== P1: REFRESH idempotent ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM cv_hourly) AS mat_has_data;
-- Second REFRESH with no new inserts
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM cv_hourly) AS mat_has_data;
-- L1 and L2 both empty
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass) AS l1_has_rows;
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log) AS l2_has_rows;

-- ============================================================
-- P1: DROP VIEW (CAGG) → catalog + trigger + L1 fully cleaned
-- ============================================================
\echo '=== P1: DROP VIEW (CAGG) → full cleanup ==='
CALL time_series.refresh_continuous_aggregate('cv_hourly', NULL, NULL);
-- Inject one L1 entry
INSERT INTO metrics VALUES ('2024-01-01 01:00+00', 1, 50.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
-- DROP VIEW should trigger event trigger and clean catalog + L1
DROP VIEW cv_hourly CASCADE;
-- L1 entries for metrics should be gone
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;
-- continuous_agg catalog should be empty
SELECT count(*) AS cagg_rows_after_drop_view FROM time_series.continuous_agg;
-- Trigger on source table should be removed
SELECT count(*) AS trigger_after_drop_view FROM pg_trigger
 WHERE tgrelid = 'metrics'::regclass
   AND tgname = 'cagg_invalidation_trigger';
-- INSERT should NOT write L1 (trigger gone)
INSERT INTO metrics VALUES ('2024-01-01 02:00+00', 2, 60.0);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- ============================================================
-- P1: DROP source TABLE → cascade cleanup
-- ============================================================
\echo '=== P1: DROP TABLE → cascade cleanup ==='
-- Re-create a CAGG to test DROP TABLE path
CREATE MATERIALIZED VIEW cv_hourly2
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM metrics GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_hourly2', NULL, NULL);
INSERT INTO metrics VALUES ('2024-01-01 03:00+00', 3, 70.0);
SELECT lowest_modified FROM time_series.cagg_invalidation_log
 WHERE source_table_oid = 'metrics'::regclass ORDER BY lowest_modified;
DROP TABLE metrics CASCADE;
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;
SELECT count(*) AS cagg_rows_after_drop_table FROM time_series.continuous_agg;

-- ============================================================
-- P1-UPD removed: UPDATE on hypertable sources is rejected outright
-- (append-only).  Test cases that originally exercised
-- "UPDATE old-time → new-time produces TWO L1 entries" and
-- "UPDATE non-time column produces one L1 entry" rely on the heap-table
-- ability to mutate rows in place; that semantic no longer exists.
-- Invalidation correctness on INSERT-only workloads is covered by
-- I-01..I-08 above and P0/P1 sections below.
-- ============================================================

-- ============================================================
-- P1-TRIM: L2 trimming 5 cases
--
-- Verify the 4 explicit cases + 1 implicit case of L2 trimming:
--   A: fully inside window → DELETE
--   B: spans left boundary → UPDATE shrink to [lowest, start)
--   C: spans right boundary → UPDATE shrink to [end, greatest]
--   D: fully contains window → split into 2 rows
--   E: fully outside window → no change (implicit)
-- ============================================================
\echo '=== P1-TRIM: L2 trimming 5 cases ==='

CREATE TABLE trim_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO trim_src
SELECT '2024-01-01'::timestamptz + (i * interval '30 min'), 1, i
FROM generate_series(1, 48) i;  -- 24 hours of data

CREATE MATERIALIZED VIEW cv_trim WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM trim_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_trim', NULL, NULL);

-- Manually insert L2 entries covering different overlap patterns
-- with a REFRESH window of [06:00, 12:00)

-- First, backfill data to create L1 entries spanning various ranges
INSERT INTO trim_src VALUES
  ('2024-01-01 08:30+00', 1, 100),  -- Case A: [08:00, 09:00) fully inside [06:00, 12:00)
  ('2024-01-01 04:30+00', 1, 101),  -- Case B: [04:00, 05:00) spans left of [06:00, ...)
  ('2024-01-01 13:30+00', 1, 102),  -- Case C: [13:00, 14:00) spans right of [..., 12:00)
  ('2024-01-01 02:30+00', 1, 103),  -- Case E: [02:00, 03:00) fully outside (left)
  ('2024-01-01 20:30+00', 1, 104);  -- Case E: [20:00, 21:00) fully outside (right)

-- Move L1 → L2 via a dummy REFRESH that won't touch these ranges
-- Actually, we need to trigger L1→L2 migration first
CALL time_series.refresh_continuous_aggregate('cv_trim',
  '2024-01-01 06:00+00', '2024-01-01 12:00+00');

-- Check L2 after partial refresh — entries outside [06:00, 12:00) should remain
SELECT lowest_modified, greatest_modified FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_trim')
ORDER BY lowest_modified;

-- EXCEPT = 0 for the refreshed range
SELECT count(*) AS diff_trim FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_trim
   WHERE bucket >= '2024-01-01 06:00+00' AND bucket < '2024-01-01 12:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM trim_src
   WHERE time >= '2024-01-01 06:00+00' AND time < '2024-01-01 12:00+00'
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM trim_src
   WHERE time >= '2024-01-01 06:00+00' AND time < '2024-01-01 12:00+00'
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_trim
   WHERE bucket >= '2024-01-01 06:00+00' AND bucket < '2024-01-01 12:00+00'
   )
) x;

-- Now REFRESH the remaining ranges to clean up
CALL time_series.refresh_continuous_aggregate('cv_trim', NULL, NULL);

-- Final EXCEPT = 0 (everything materialized correctly)
SELECT count(*) AS diff_trim_final FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_trim
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM trim_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM trim_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_trim
   )
) x;

-- L2 should be empty after full refresh
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_trim')) AS l2_has_rows;

DROP TABLE trim_src CASCADE;

-- ============================================================
-- P2-MERGE: Invalidation entry merging
--
-- Adjacent/overlapping L2 entries should be merged into fewer
-- intervals during cagg_gather_dirty_intervals.
-- ============================================================
\echo '=== P2-MERGE: invalidation merging ==='

CREATE TABLE merge_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO merge_src
SELECT '2024-01-01'::timestamptz + (i * interval '30 min'), 1, i
FROM generate_series(1, 48) i;

CREATE MATERIALIZED VIEW cv_merge WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM merge_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_merge', NULL, NULL);

-- Insert scattered backfill: hours 01, 02, 03, 05 (gap at 04)
INSERT INTO merge_src VALUES
  ('2024-01-01 01:10+00', 1, 200),
  ('2024-01-01 02:10+00', 1, 201),
  ('2024-01-01 03:10+00', 1, 202),
  ('2024-01-01 05:10+00', 1, 203);

-- Partial REFRESH [00:00, 06:00) — should merge [01,02,03] into one range
-- and keep [05] as separate (or merge [01-03]+[05] depending on alignment)
CALL time_series.refresh_continuous_aggregate('cv_merge',
  '2024-01-01 00:00+00', '2024-01-01 06:00+00');

-- EXCEPT = 0 in refreshed range
SELECT count(*) AS diff_merge FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_merge
   WHERE bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 06:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM merge_src
   WHERE time >= '2024-01-01 00:00+00' AND time < '2024-01-01 06:00+00'
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM merge_src
   WHERE time >= '2024-01-01 00:00+00' AND time < '2024-01-01 06:00+00'
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_merge
   WHERE bucket >= '2024-01-01 00:00+00' AND bucket < '2024-01-01 06:00+00'
   )
) x;

-- Full EXCEPT = 0
CALL time_series.refresh_continuous_aggregate('cv_merge', NULL, NULL);
SELECT count(*) AS diff_merge_full FROM (
  SELECT bucket, tags_id, cnt FROM cv_merge EXCEPT
  SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
  FROM merge_src GROUP BY 1, 2
) x;

DROP TABLE merge_src CASCADE;

-- ============================================================
-- P2-TRUNC-MID: TRUNCATE source → CAGG still has old data
--               before REFRESH
-- ============================================================
\echo '=== P2-TRUNC-MID: TRUNCATE intermediate state ==='

CREATE TABLE trunc_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO trunc_src
SELECT '2024-01-01'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_trunc_mid WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM trunc_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_trunc_mid', NULL, NULL);

SELECT EXISTS(SELECT 1 FROM cv_trunc_mid) AS mat_has_data;

-- TRUNCATE source but do NOT refresh yet
TRUNCATE trunc_src;

-- CAGG should STILL have old data (materialized_only or mat branch)
-- Because we haven't refreshed, the mat table is stale
ALTER VIEW cv_trunc_mid SET (time_series.materialized_only = true);
SELECT EXISTS(SELECT 1 FROM cv_trunc_mid) AS mat_has_data;

-- Now REFRESH → CAGG should be empty
ALTER VIEW cv_trunc_mid SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv_trunc_mid', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM cv_trunc_mid) AS mat_has_data;

DROP TABLE trunc_src CASCADE;

-- ============================================================
-- P2-TRUNC-CAGG: TRUNCATE the CAGG user view itself
-- ============================================================
\echo '=== P2-TRUNC-CAGG: TRUNCATE user view ==='

CREATE TABLE tc_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO tc_src
SELECT '2024-01-01'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_tc WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM tc_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_tc', NULL, NULL);

SELECT EXISTS(SELECT 1 FROM cv_tc) AS mat_has_data;

-- TRUNCATE the user view (which is a regular VIEW, not a TABLE)
-- PG should reject this: "cannot truncate a view"
\set ON_ERROR_STOP 0
TRUNCATE cv_tc;
\set ON_ERROR_STOP 1

DROP TABLE tc_src CASCADE;

-- ============================================================
-- P2-DELETE removed: DELETE on hypertable sources is rejected outright
-- (append-only).  The original test exercised "DELETE source range →
-- L1 captures dirty range → incremental REFRESH removes aggregated
-- rows".  This whole flow doesn't apply when the source is immutable.
-- ============================================================

-- ============================================================
-- P2-MINBUCKET: Minimal bucket width boundary tests
--
-- Use '1 minute' as a very small bucket to test exact-boundary
-- refresh behavior.
-- ============================================================
\echo '=== P2-MINBUCKET: minimal bucket boundary ==='

CREATE TABLE mb_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);

CREATE MATERIALIZED VIEW cv_mb WITH (time_series.continuous) AS
  SELECT time_bucket('1 minute'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(val) AS avg_val
  FROM mb_src GROUP BY bucket, tags_id;

-- Insert a single row at exact minute boundary
INSERT INTO mb_src VALUES ('2024-01-01 00:05:00+00', 1, 10.0);
CALL time_series.refresh_continuous_aggregate('cv_mb', NULL, NULL);

SELECT EXISTS(SELECT 1 FROM cv_mb) AS mat_has_data;
SELECT avg_val AS mb_avg1 FROM cv_mb WHERE bucket = '2024-01-01 00:05:00+00';

-- Insert another row in the SAME minute bucket → avg should change
INSERT INTO mb_src VALUES ('2024-01-01 00:05:30+00', 1, 20.0);
CALL time_series.refresh_continuous_aggregate('cv_mb',
  '2024-01-01 00:05:00+00', '2024-01-01 00:06:00+00');

SELECT avg_val AS mb_avg2 FROM cv_mb WHERE bucket = '2024-01-01 00:05:00+00';
-- avg should be (10+20)/2 = 15

-- Insert in adjacent minute bucket
INSERT INTO mb_src VALUES ('2024-01-01 00:06:00+00', 1, 30.0);
CALL time_series.refresh_continuous_aggregate('cv_mb',
  '2024-01-01 00:06:00+00', '2024-01-01 00:07:00+00');

-- Full EXCEPT = 0
SELECT count(*) AS diff_mb FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_mb
   EXCEPT
   SELECT time_bucket('1 minute'::interval, time), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM mb_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 minute'::interval, time), tags_id,
   count(*), round(avg(val)::numeric, 10)
   FROM mb_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_mb
   )
) x;

DROP TABLE mb_src CASCADE;

-- ============================================================
-- P2-WMLIMIT: Watermark vs data boundary
--
-- Our watermark uses now() for full refresh. Verify that partial
-- REFRESH with a window beyond source data doesn't break.
-- ============================================================
\echo '=== P2-WMLIMIT: watermark beyond data ==='

CREATE TABLE wm_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO wm_src
SELECT '2024-01-01'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_wm WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM wm_src GROUP BY bucket, tags_id;

-- REFRESH beyond data range — should not crash
CALL time_series.refresh_continuous_aggregate('cv_wm',
  '2024-01-01 00:00+00', '2024-12-31 00:00+00');

-- EXCEPT = 0
SELECT count(*) AS diff_wm FROM (
  SELECT bucket, tags_id, cnt FROM cv_wm EXCEPT
  SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
  FROM wm_src GROUP BY 1, 2
) x;

-- REFRESH with start after all data — should be no-op
CALL time_series.refresh_continuous_aggregate('cv_wm',
  '2025-01-01 00:00+00', '2025-12-31 00:00+00');

-- Mat table should still have data
SELECT EXISTS(SELECT 1 FROM cv_wm) AS mat_has_data;

DROP TABLE wm_src CASCADE;

-- ============================================================
-- P3-PLPGSQL: cagg_watermark() in PL/pgSQL function
-- ============================================================
\echo '=== P3-PLPGSQL: watermark in PL/pgSQL ==='

CREATE TABLE pl_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO pl_src
SELECT '2024-01-01'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_pl WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM pl_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_pl', NULL, NULL);

-- PL/pgSQL function that calls cagg_watermark()
CREATE FUNCTION get_wm(cid int) RETURNS timestamptz
LANGUAGE plpgsql AS $$
DECLARE wm timestamptz;
BEGIN
  SELECT time_series.cagg_watermark(cid) INTO wm;
  RETURN wm;
END $$;

-- On QD, cagg_watermark() reads local heap of RANDOMLY table → no rows → returns -infinity.
-- This is expected: the function is designed for segment-level execution (via UNION ALL view).
-- Calling directly on QD returns -infinity.
SELECT get_wm((SELECT cagg_id FROM time_series.continuous_agg
               WHERE user_view_name = 'cv_pl')) AS wm_on_qd;
-- Expected: -infinity (QD has no local cagg_watermark rows)

DROP FUNCTION get_wm(int);
DROP TABLE pl_src CASCADE;

-- ============================================================
-- BATCH3: Remaining edge cases from upstream comparison
-- ============================================================

-- ============================================================
-- INV-04: Initial L2 state should be empty after CREATE
-- ============================================================
\echo '=== INV-04: initial L2 empty ==='
CREATE TABLE b3_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO b3_src SELECT '2024-01-01'::timestamptz + (i * interval '30 min'), 1, i
FROM generate_series(1, 20) i;

CREATE MATERIALIZED VIEW cv_b3 WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM b3_src GROUP BY bucket, tags_id;

-- L2 should be empty right after CREATE (no REFRESH has moved L1→L2 yet)
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3')) AS l2_has_rows;

-- L1 should also be empty (watermark = -infinity, all inserts >= threshold)
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- ============================================================
-- INV-10: After REFRESH, L1 should be completely cleared
-- ============================================================
\echo '=== INV-10: L1 cleared after REFRESH ==='
CALL time_series.refresh_continuous_aggregate('cv_b3', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;

-- ============================================================
-- INV-16~18: Multi-CAGG L2 interaction — second CAGG's L2 is
--            populated independently, non-overlapping entries handled
-- ============================================================
\echo '=== INV-16~18: multi-CAGG L2 interaction ==='
-- Create a second CAGG on the same source
CREATE MATERIALIZED VIEW cv_b3_daily WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM b3_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_b3_daily', NULL, NULL);

-- Backfill: insert rows that land in different bucket ranges
INSERT INTO b3_src VALUES
  ('2024-01-01 02:30+00', 1, 100),   -- hourly bucket: 02:00
  ('2024-01-01 05:30+00', 1, 101),   -- hourly bucket: 05:00
  ('2024-01-01 08:30+00', 1, 102);   -- hourly bucket: 08:00

-- REFRESH only cv_b3 (hourly) with partial window [02:00, 06:00)
-- This should:
--   1. Migrate L1→L2 for BOTH CAGGs (all 3 entries copied to both)
--   2. Delete L1
--   3. Trim cv_b3's L2 for [02:00, 06:00) range
--   4. cv_b3_daily's L2 should still have entries (not touched)
CALL time_series.refresh_continuous_aggregate('cv_b3',
  '2024-01-01 02:00+00', '2024-01-01 06:00+00');

-- cv_b3_daily should still have L2 entries (untouched by hourly REFRESH)
SELECT lowest_modified, greatest_modified FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3_daily')
ORDER BY lowest_modified;

-- cv_b3 should have L2 entries only outside [02:00, 06:00) — i.e., hour 08
SELECT lowest_modified, greatest_modified FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3')
ORDER BY lowest_modified;

-- Stepwise cleanup (INV-17): REFRESH the remaining range
CALL time_series.refresh_continuous_aggregate('cv_b3',
  '2024-01-01 08:00+00', '2024-01-01 09:00+00');

-- cv_b3 L2 should now be empty
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3')) AS l2_has_rows;

-- REFRESH daily to consume its L2
CALL time_series.refresh_continuous_aggregate('cv_b3_daily', NULL, NULL);
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3_daily')) AS l2_has_rows;

-- Both EXCEPT = 0
SELECT count(*) AS diff_hourly FROM (
  SELECT bucket, tags_id, cnt FROM cv_b3 EXCEPT
  SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
  FROM b3_src GROUP BY 1, 2
) x;
SELECT count(*) AS diff_daily FROM (
  SELECT bucket, tags_id, cnt FROM cv_b3_daily EXCEPT
  SELECT time_bucket('1 day'::interval, time), tags_id, count(*)
  FROM b3_src GROUP BY 1, 2
) x;

DROP VIEW cv_b3_daily CASCADE;

-- ============================================================
-- INV-35~36: Sequential refresh vs batch refresh comparison
--
-- Insert data spanning 3 buckets. First refresh one-by-one,
-- then reset and refresh all at once. Results must be identical.
-- ============================================================
\echo '=== INV-35~36: sequential vs batch refresh ==='
TRUNCATE b3_src;
INSERT INTO b3_src VALUES
  ('2024-01-01 00:10+00', 1, 10.0),
  ('2024-01-01 01:10+00', 1, 20.0),
  ('2024-01-01 02:10+00', 1, 30.0);
CALL time_series.refresh_continuous_aggregate('cv_b3', NULL, NULL);

-- Backfill: modify all 3 buckets
INSERT INTO b3_src VALUES
  ('2024-01-01 00:20+00', 1, 15.0),
  ('2024-01-01 01:20+00', 1, 25.0),
  ('2024-01-01 02:20+00', 1, 35.0);

-- Sequential: refresh bucket by bucket
CALL time_series.refresh_continuous_aggregate('cv_b3', '2024-01-01 00:00+00', '2024-01-01 01:00+00');
CALL time_series.refresh_continuous_aggregate('cv_b3', '2024-01-01 01:00+00', '2024-01-01 02:00+00');
CALL time_series.refresh_continuous_aggregate('cv_b3', '2024-01-01 02:00+00', '2024-01-01 03:00+00');

-- Capture sequential result
CREATE TEMP TABLE seq_result AS SELECT * FROM cv_b3 ORDER BY bucket, tags_id;

-- Reset: full refresh to clean slate, then re-backfill
TRUNCATE b3_src;
INSERT INTO b3_src VALUES
  ('2024-01-01 00:10+00', 1, 10.0),
  ('2024-01-01 01:10+00', 1, 20.0),
  ('2024-01-01 02:10+00', 1, 30.0),
  ('2024-01-01 00:20+00', 1, 15.0),
  ('2024-01-01 01:20+00', 1, 25.0),
  ('2024-01-01 02:20+00', 1, 35.0);
CALL time_series.refresh_continuous_aggregate('cv_b3', NULL, NULL);

-- Batch: all at once
CREATE TEMP TABLE batch_result AS SELECT * FROM cv_b3 ORDER BY bucket, tags_id;

-- Compare: sequential == batch (refresh-path invariance)
SELECT count(*) AS seq_vs_batch_diff FROM (
  SELECT * FROM seq_result EXCEPT SELECT * FROM batch_result
) x;
-- Independent oracle: the invariance check above only proves the two
-- refresh paths agree with each other; anchor batch_result to a
-- hand-derived truth.  b3_src holds 6 rows, 2 per hour-bucket (tag=1):
-- (00:10,00:20) (01:10,01:20) (02:10,02:20) -> 3 buckets, cnt=2 each.
SELECT count(*) = 3       AS inv35_three_buckets,
       bool_and(cnt = 2)     AS inv35_all_cnt_2,
       bool_and(tags_id = 1) AS inv35_all_tag1
  FROM batch_result;

DROP TABLE seq_result;
DROP TABLE batch_result;

-- ============================================================
-- INV-44~45: NULL full refresh threshold behavior
-- ============================================================
\echo '=== INV-44~45: NULL refresh threshold ==='
CALL time_series.refresh_continuous_aggregate('cv_b3', NULL, NULL);

-- After full NULL refresh, L1 and L2 should both be empty
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log) AS l1_has_rows;
SELECT EXISTS(SELECT 1 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3')) AS l2_has_rows;

-- Watermark should have advanced
SELECT bool_and(watermark > '-infinity'::timestamptz) AS wm_advanced
FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b3');

-- ============================================================
-- INV-48 removed: tested "DELETE source data → watermark does NOT
-- decrease".  DELETE is rejected on hypertable sources, so the
-- precondition for this case never occurs.  Watermark monotonicity
-- in general (GREATEST semantics) is still exercised by other
-- partial-refresh tests above.
-- ============================================================

-- ============================================================
-- WM-01: No CAGG → INSERT should not write invalidation log
-- ============================================================
\echo '=== WM-01: no CAGG INSERT ==='
DROP TABLE b3_src CASCADE;

CREATE TABLE no_cagg_src (time TIMESTAMPTZ NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (time);
INSERT INTO no_cagg_src VALUES ('2024-01-01 00:00+00', 1.0);

-- No CAGG exists on this table → no trigger → L1 should have nothing for this table
SELECT EXISTS(SELECT 1 FROM time_series.cagg_invalidation_log
WHERE source_table_oid = 'no_cagg_src'::regclass) AS l1_has_rows;

DROP TABLE no_cagg_src;

-- ============================================================
-- WM-38~39: PREPARE + EXECUTE watermark query
-- ============================================================
\echo '=== WM-38~39: PREPARE watermark ==='

CREATE TABLE prep_src (time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO prep_src SELECT '2024-01-01'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 10) i;

CREATE MATERIALIZED VIEW cv_prep WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt
  FROM prep_src GROUP BY bucket, tags_id;
CALL time_series.refresh_continuous_aggregate('cv_prep', NULL, NULL);

-- PREPARE a query on the CAGG
PREPARE cagg_query AS SELECT count(*) FROM cv_prep;

-- Execute: should return data
EXECUTE cagg_query;

-- Insert more data and REFRESH
INSERT INTO prep_src SELECT '2024-01-02'::timestamptz + (i * interval '1 hour'), 1, i
FROM generate_series(1, 5) i;
CALL time_series.refresh_continuous_aggregate('cv_prep', NULL, NULL);

-- Execute again: should reflect new data (more rows)
EXECUTE cagg_query;

DEALLOCATE cagg_query;
DROP TABLE prep_src CASCADE;

-- ============================================================
-- upstream alignment batch: remaining 25 data variants
-- (cagg_invalidation.sql + cagg_watermark.sql)
-- ============================================================

-- Setup: fresh table + two CAGGs for multi-CAGG tests
CREATE TABLE tsdb_src (
    time TIMESTAMPTZ NOT NULL, tags_id INT NOT NULL, val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);
INSERT INTO tsdb_src
SELECT '2024-01-01 00:00+00'::timestamptz + (i * interval '10 min'),
       (i % 3) + 1, i * 1.5
FROM generate_series(1, 100) i;

CREATE MATERIALIZED VIEW cv_a
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(val) AS avg_val
  FROM tsdb_src GROUP BY bucket, tags_id;

CREATE MATERIALIZED VIEW cv_b
WITH (time_series.continuous) AS
  SELECT time_bucket('2 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM tsdb_src GROUP BY bucket;

-- ============================================================
-- V-01: Catalog grouping — 2 CAGGs on same source (#2)
-- ============================================================
\echo '=== V-01: catalog grouping ==='
SELECT count(*) AS caggs_on_src FROM time_series.continuous_agg
WHERE source_table_oid = 'tsdb_src'::regclass;

-- ============================================================
-- V-02: First PARTIAL refresh advances watermark (#4)
-- ============================================================
\echo '=== V-02: partial refresh advances watermark ==='
-- Before any REFRESH, watermark = -infinity
SELECT bool_and(watermark = '-infinity'::timestamptz) AS wm_neg_inf
FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');

CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 00:00+00', '2024-01-01 05:00+00');

-- Watermark should have advanced (no longer -inf)
SELECT bool_and(watermark > '-infinity'::timestamptz) AS wm_advanced
FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');

-- ============================================================
-- V-03: Re-REFRESH same partial window = no-op (#5)
-- ============================================================
\echo '=== V-03: partial refresh idempotent ==='
SELECT count(*) AS cnt_before FROM cv_a;
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 00:00+00', '2024-01-01 05:00+00');
SELECT count(*) AS cnt_after FROM cv_a;

-- ============================================================
-- V-04: Partial REFRESH below watermark → watermark unchanged (#6)
-- ============================================================
\echo '=== V-04: sub-window refresh no watermark move ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
-- Record watermark
CREATE TEMP TABLE saved_wm AS
  SELECT MAX(watermark) AS wm FROM time_series.cagg_watermark
  WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');
-- Refresh a sub-window well below watermark
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 00:00+00', '2024-01-01 02:00+00');
-- Watermark should not have decreased
SELECT bool_and(w.watermark >= s.wm) AS wm_not_decreased
FROM time_series.cagg_watermark w, saved_wm s
WHERE w.cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');
DROP TABLE saved_wm;

-- ============================================================
-- V-05: REFRESH different source → independent watermark (#7)
-- ============================================================
\echo '=== V-05: independent watermark by source ==='
CREATE TABLE tsdb_src2 (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO tsdb_src2 VALUES ('2024-06-01 00:30+00', 1, 10.0);
CREATE MATERIALIZED VIEW cv_other
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM tsdb_src2 GROUP BY bucket;

-- Refresh cv_other only
CALL time_series.refresh_continuous_aggregate('cv_other', NULL, NULL);
-- cv_a's watermark should be unchanged from before
SELECT bool_and(w.watermark > '2024-01-01'::timestamptz) AS cva_wm_unchanged
FROM time_series.cagg_watermark w
WHERE w.cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');

DROP TABLE tsdb_src2 CASCADE;

-- ============================================================
-- V-06: Two CAGGs on same source — REFRESH one, other's L2 independent (#8, #11)
-- ============================================================
\echo '=== V-06: same-source CAGG L2 independence ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_b', NULL, NULL);
-- Insert backfill
INSERT INTO tsdb_src VALUES ('2024-01-01 01:30+00', 1, 999.0);
-- Refresh only cv_a
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- cv_a should be correct
SELECT count(*) AS diff_a FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_a
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(val)::numeric, 10)
   FROM tsdb_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(val)::numeric, 10)
   FROM tsdb_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_a
   )
) x;
-- cv_b should still have L2 (stale for the backfill bucket)
SELECT count(*) AS cvb_l2 FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b');
-- Now refresh cv_b
CALL time_series.refresh_continuous_aggregate('cv_b', NULL, NULL);
SELECT count(*) AS diff_b FROM (
  (  SELECT bucket, cnt FROM cv_b
   EXCEPT
   SELECT time_bucket('2 hour'::interval, time), count(*)
   FROM tsdb_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('2 hour'::interval, time), count(*)
   FROM tsdb_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_b
   )
) x;

-- ============================================================
-- V-06b: DROP one CAGG → other CAGG's L2 preserved
--
-- ============================================================
\echo '=== V-06b: DROP one CAGG, other L2 preserved ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_b', NULL, NULL);
-- Insert backfill so both CAGGs get L2 entries
INSERT INTO tsdb_src VALUES ('2024-01-01 02:30+00', 1, 777.0);
-- Trigger L1→L2 migration for both (partial refresh cv_a only)
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 02:00+00', '2024-01-01 03:00+00');
-- cv_b should have L2 entries (from migration)
SELECT count(*) AS cvb_l2_before_drop FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_b');
-- Record cv_b's cagg_id
SELECT cagg_id AS cvb_id FROM time_series.continuous_agg
WHERE user_view_name = 'cv_b' \gset
-- DROP cv_a — should NOT affect cv_b's L2
DROP VIEW cv_a CASCADE;
-- cv_b's L2 should still be there
SELECT count(*) AS cvb_l2_after_drop FROM time_series.cagg_materialization_log
WHERE cagg_id = :cvb_id;
-- cv_b should still be queryable and refreshable
CALL time_series.refresh_continuous_aggregate('cv_b', NULL, NULL);
SELECT count(*) AS diff_b_after_drop FROM (
  (  SELECT bucket, cnt FROM cv_b
   EXCEPT
   SELECT time_bucket('2 hour'::interval, time), count(*)
   FROM tsdb_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('2 hour'::interval, time), count(*)
   FROM tsdb_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_b
   )
) x;
-- Re-create cv_a for remaining tests
CREATE MATERIALIZED VIEW cv_a
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(val) AS avg_val
  FROM tsdb_src GROUP BY bucket, tags_id;

-- ============================================================
-- V-07: L1→L2 migration + trimming exact L2 verification (#9, #10, #12)
-- ============================================================
\echo '=== V-07: L2 trimming exact verification ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
-- Insert in 3 distinct hour-buckets
INSERT INTO tsdb_src VALUES
  ('2024-01-01 01:30+00', 1, 10.0),
  ('2024-01-01 03:30+00', 2, 20.0),
  ('2024-01-01 05:30+00', 3, 30.0);
-- Narrow refresh: only bucket 03:00-04:00
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 03:00+00', '2024-01-01 04:00+00');
-- L2 should have residuals for hour-01 and hour-05 (bucket-03 consumed)
SELECT count(*) AS l2_residual FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');
-- Refreshed bucket should be correct
SELECT count(*) AS diff_mid FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_a
   WHERE bucket = '2024-01-01 03:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM tsdb_src WHERE time >= '2024-01-01 03:00+00' AND time < '2024-01-01 04:00+00'
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM tsdb_src WHERE time >= '2024-01-01 03:00+00' AND time < '2024-01-01 04:00+00'
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_a
   WHERE bucket = '2024-01-01 03:00+00'
   )
) x;
-- Full refresh to consume residuals
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS l2_clean FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');

-- ============================================================
-- V-08: Refresh end falls mid-invalidation range → residual (#13)
-- ============================================================
\echo '=== V-08: mid-invalidation residual ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
-- Two invalidation ranges: hour-01 and hour-03 to hour-06
INSERT INTO tsdb_src VALUES ('2024-01-01 01:15+00', 1, 1.0);
INSERT INTO tsdb_src VALUES ('2024-01-01 03:15+00', 2, 2.0);
INSERT INTO tsdb_src VALUES ('2024-01-01 04:15+00', 3, 3.0);
INSERT INTO tsdb_src VALUES ('2024-01-01 05:15+00', 1, 4.0);
-- Refresh [00:00, 04:00) — covers hour-01 fully, covers hour-03 but NOT hour-04,05
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 00:00+00', '2024-01-01 04:00+00');
-- L2 should have residual for hour-04 and hour-05
SELECT count(*) AS l2_mid_residual FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');
-- Refreshed range correct
SELECT count(*) AS diff_partial FROM (
  (  SELECT bucket, tags_id, cnt FROM cv_a WHERE bucket < '2024-01-01 04:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM tsdb_src WHERE time < '2024-01-01 04:00+00' GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*)
   FROM tsdb_src WHERE time < '2024-01-01 04:00+00' GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt FROM cv_a WHERE bucket < '2024-01-01 04:00+00'
   )
) x;
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);

-- ============================================================
-- V-09: Merge from pure L1→L2 migration without refresh overlap (#14)
-- ============================================================
\echo '=== V-09: merge from migration ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
-- Insert overlapping ranges that will merge in L2
INSERT INTO tsdb_src VALUES ('2024-01-01 01:10+00', 1, 1.0);
INSERT INTO tsdb_src VALUES ('2024-01-01 01:50+00', 2, 2.0);
-- Refresh a NON-overlapping window (far future) to trigger L1→L2 without trimming
CALL time_series.refresh_continuous_aggregate('cv_a',
  '2024-01-01 10:00+00', '2024-01-01 11:00+00');
-- L2 entries for hour-01 should exist (merged)
SELECT count(*) AS l2_merged FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);

-- ============================================================
-- V-10: NULL refresh empties both L1 and L2 (#15)
-- ============================================================
\echo '=== V-10: NULL refresh clears all ==='
INSERT INTO tsdb_src VALUES ('2024-01-01 02:30+00', 1, 50.0);
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS l1_empty FROM time_series.cagg_invalidation_log;
SELECT count(*) AS l2_empty FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_a');

-- ============================================================
-- V-11: TRUNCATE full round-trip (#16, #17)
-- ============================================================
\echo '=== V-11: TRUNCATE round-trip ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS before_trunc FROM cv_a;
-- TRUNCATE source → L1 = {-inf, +inf}
TRUNCATE tsdb_src;
SELECT count(*) > 0 AS has_l1_inf FROM time_series.cagg_invalidation_log;
-- CAGG still has old materialized data (stale)
ALTER VIEW cv_a SET (time_series.materialized_only = true);
SELECT count(*) AS stale_rows FROM cv_a;
ALTER VIEW cv_a SET (time_series.materialized_only = false);
-- REFRESH → CAGG empty
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS after_trunc_refresh FROM cv_a;
-- Re-populate + REFRESH → data restored
INSERT INTO tsdb_src
SELECT '2024-01-01 00:00+00'::timestamptz + (i * interval '10 min'),
       (i % 3) + 1, i * 1.5
FROM generate_series(1, 50) i;
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS diff_restored FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_a
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(val)::numeric, 10)
   FROM tsdb_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id, count(*), round(avg(val)::numeric, 10)
   FROM tsdb_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_val::numeric, 10) FROM cv_a
   )
) x;

-- ============================================================
-- V-12: 1-second bucket exhaustive window placement (#20)
-- ============================================================
\echo '=== V-12: 1-second bucket boundary ==='
CREATE TABLE sec_src (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO sec_src VALUES ('2024-01-01 00:00:05+00', 1, 100.0);
CREATE MATERIALIZED VIEW cv_sec WITH (time_series.continuous) AS
  SELECT time_bucket('1 second'::interval, time) AS bucket, avg(val) AS a
  FROM sec_src GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_sec', NULL, NULL);
-- Insert at exact second=5
INSERT INTO sec_src VALUES ('2024-01-01 00:00:05+00', 2, 200.0);
-- Refresh MISS: window [04, 05) doesn't include second=5
CALL time_series.refresh_continuous_aggregate('cv_sec',
  '2024-01-01 00:00:04+00', '2024-01-01 00:00:05+00');
SELECT a AS avg_miss FROM cv_sec WHERE bucket = '2024-01-01 00:00:05+00';
-- Refresh HIT: window [05, 06)
CALL time_series.refresh_continuous_aggregate('cv_sec',
  '2024-01-01 00:00:05+00', '2024-01-01 00:00:06+00');
SELECT a AS avg_hit FROM cv_sec WHERE bucket = '2024-01-01 00:00:05+00';
-- Refresh EXTENDED LEFT: [-inf, 06)
INSERT INTO sec_src VALUES ('2024-01-01 00:00:05+00', 3, 300.0);
CALL time_series.refresh_continuous_aggregate('cv_sec', NULL, '2024-01-01 00:00:06+00');
SELECT a AS avg_left_ext FROM cv_sec WHERE bucket = '2024-01-01 00:00:05+00';
-- Refresh EXTENDED RIGHT: [05, +inf)
INSERT INTO sec_src VALUES ('2024-01-01 00:00:05+00', 4, 400.0);
CALL time_series.refresh_continuous_aggregate('cv_sec', '2024-01-01 00:00:05+00', NULL);
SELECT a AS avg_right_ext FROM cv_sec WHERE bucket = '2024-01-01 00:00:05+00';

DROP TABLE sec_src CASCADE;

-- ============================================================
-- V-13: Sequential vs batch refresh for 2-bucket (#21)
-- ============================================================
\echo '=== V-13: sequential vs batch 2-bucket ==='
CREATE TABLE two_src (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO two_src VALUES
  ('2024-01-01 00:30+00', 1, 10.0),
  ('2024-01-01 01:30+00', 2, 20.0);
CREATE MATERIALIZED VIEW cv_two WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, avg(val) AS a
  FROM two_src GROUP BY bucket;
-- Batch: refresh whole range
CALL time_series.refresh_continuous_aggregate('cv_two', NULL, NULL);
CREATE TEMP TABLE batch_result AS SELECT bucket, a FROM cv_two ORDER BY bucket;
-- Reset
DROP VIEW cv_two CASCADE;
CREATE MATERIALIZED VIEW cv_two WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, avg(val) AS a
  FROM two_src GROUP BY bucket;
-- Sequential: one bucket at a time
CALL time_series.refresh_continuous_aggregate('cv_two',
  '2024-01-01 00:00+00', '2024-01-01 01:00+00');
CALL time_series.refresh_continuous_aggregate('cv_two',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- Compare (refresh-path invariance: sequential cv_two == batch_result)
SELECT count(*) AS seq_vs_batch_diff FROM (
  SELECT bucket, a FROM cv_two EXCEPT SELECT bucket, a FROM batch_result
) x;
-- Independent oracle: two_src has exactly 1 row per hour-bucket --
-- (00:30 val=10) -> bucket 00:00 avg=10; (01:30 val=20) -> 01:00 avg=20.
SELECT bucket, a,
       a = CASE bucket WHEN '2024-01-01 00:00+00'::timestamptz THEN 10.0
                       WHEN '2024-01-01 01:00+00'::timestamptz THEN 20.0
                       END AS v13_avg_ok
  FROM cv_two ORDER BY bucket;
DROP TABLE batch_result;
DROP TABLE two_src CASCADE;

-- ============================================================
-- V-14: L2 split by middle refresh (#22)
-- ============================================================
\echo '=== V-14: L2 split by middle refresh ==='
CREATE TABLE split_src (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO split_src VALUES
  ('2024-01-01 00:30+00', 1, 10.0),
  ('2024-01-01 01:30+00', 2, 20.0),
  ('2024-01-01 02:30+00', 3, 30.0);
CREATE MATERIALIZED VIEW cv_split WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, avg(val) AS a
  FROM split_src GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_split', NULL, NULL);
-- Backfill all 3 buckets
INSERT INTO split_src VALUES
  ('2024-01-01 00:45+00', 1, 100.0),
  ('2024-01-01 01:45+00', 2, 200.0),
  ('2024-01-01 02:45+00', 3, 300.0);
-- Refresh ONLY the middle bucket
CALL time_series.refresh_continuous_aggregate('cv_split',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
-- L2 should have residuals for hour-00 and hour-02
SELECT count(*) AS l2_split_count FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_split');
-- Middle bucket correct
SELECT count(*) AS diff_split_mid FROM (
  (  SELECT bucket, a FROM cv_split WHERE bucket = '2024-01-01 01:00+00'
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), avg(val)
   FROM split_src WHERE time >= '2024-01-01 01:00+00' AND time < '2024-01-01 02:00+00'
   GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), avg(val)
   FROM split_src WHERE time >= '2024-01-01 01:00+00' AND time < '2024-01-01 02:00+00'
   GROUP BY 1
   EXCEPT
   SELECT bucket, a FROM cv_split WHERE bucket = '2024-01-01 01:00+00'
   )
) x;
-- Full refresh cleans up
CALL time_series.refresh_continuous_aggregate('cv_split', NULL, NULL);
SELECT count(*) AS diff_split_full FROM (
  (  SELECT bucket, a FROM cv_split
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), avg(val) FROM split_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), avg(val) FROM split_src GROUP BY 1
   EXCEPT
   SELECT bucket, a FROM cv_split
   )
) x;
DROP TABLE split_src CASCADE;

-- ============================================================
-- V-15: Threshold capping step-by-step (#23)
-- ============================================================
\echo '=== V-15: threshold capping ==='
CREATE TABLE cap_src (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO cap_src VALUES
  ('2024-01-01 01:00+00', 1, 10.0),
  ('2024-01-01 02:00+00', 2, 20.0),
  ('2024-01-01 03:00+00', 3, 30.0);
CREATE MATERIALIZED VIEW cv_cap WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM cap_src GROUP BY bucket;
-- Step 1: Partial refresh [01:00, 02:00)
CALL time_series.refresh_continuous_aggregate('cv_cap',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
SELECT count(*) AS mat_step1 FROM cv_cap;
-- Step 2: Refresh far beyond data [10:00, 20:00) → no crash, no extra rows
CALL time_series.refresh_continuous_aggregate('cv_cap',
  '2024-01-01 10:00+00', '2024-01-01 20:00+00');
SELECT count(*) AS mat_step2 FROM cv_cap;
-- Step 3: Full refresh → all 3 rows
CALL time_series.refresh_continuous_aggregate('cv_cap', NULL, NULL);
SELECT count(*) AS mat_step3 FROM cv_cap;
DROP TABLE cap_src CASCADE;

-- ============================================================
-- V-16 removed: delete-reinsert lifecycle test relied on DELETE
-- support; hypertable sources are append-only.
-- ============================================================

-- ============================================================
-- V-17: Merge boundary with gap (#25, #26)
-- ============================================================
\echo '=== V-17: merge with gap ==='
CREATE TABLE merge_src (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
CREATE MATERIALIZED VIEW cv_merge WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM merge_src GROUP BY bucket;
-- Insert into 3 adjacent + 1 gap
INSERT INTO merge_src VALUES ('2024-01-01 01:30+00', 1, 1.0);
INSERT INTO merge_src VALUES ('2024-01-01 02:30+00', 2, 2.0);
INSERT INTO merge_src VALUES ('2024-01-01 03:30+00', 3, 3.0);
-- Gap: skip hour 04
INSERT INTO merge_src VALUES ('2024-01-01 05:30+00', 4, 4.0);
-- Full REFRESH: should process all, result correct
CALL time_series.refresh_continuous_aggregate('cv_merge', NULL, NULL);
SELECT count(*) AS diff_merge_gap FROM (
  (  SELECT bucket, cnt FROM cv_merge
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM merge_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM merge_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_merge
   )
) x;
-- Verify 4 buckets (01,02,03,05 — not 04)
SELECT count(*) AS merge_buckets FROM cv_merge;
DROP TABLE merge_src CASCADE;

-- ============================================================
-- V-18 removed: UPDATE crossing-threshold cases (A: val below
-- watermark, B/D: time crossing watermark up/down, C: val above
-- watermark) all require UPDATE support, which hypertable lacks.
-- ============================================================

-- ============================================================
-- V-19: TRUNCATE with active prepared statement (#30)
-- ============================================================
\echo '=== V-19: TRUNCATE + PREPARE ==='
CREATE TABLE trunc_prep (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO trunc_prep VALUES
  ('2024-01-01 00:30+00', 1, 10.0),
  ('2024-01-01 01:30+00', 2, 20.0);
CREATE MATERIALIZED VIEW cv_tp WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM trunc_prep GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_tp', NULL, NULL);

PREPARE tp_query AS SELECT count(*) FROM cv_tp;
EXECUTE tp_query;
-- TRUNCATE source
TRUNCATE trunc_prep;
-- Prepared statement still works (returns mat data before refresh)
EXECUTE tp_query;
-- REFRESH → empty
CALL time_series.refresh_continuous_aggregate('cv_tp', NULL, NULL);
EXECUTE tp_query;

DEALLOCATE tp_query;
DROP TABLE trunc_prep CASCADE;

-- ============================================================
-- V-20: ABORT (not ROLLBACK) does not write L1 (#29)
-- ============================================================
\echo '=== V-20: ABORT no L1 ==='
CALL time_series.refresh_continuous_aggregate('cv_a', NULL, NULL);
SELECT count(*) AS l1_before_abort FROM time_series.cagg_invalidation_log;
BEGIN;
INSERT INTO tsdb_src VALUES ('2024-01-01 01:45+00', 1, 999.0);
ABORT;
SELECT count(*) AS l1_after_abort FROM time_series.cagg_invalidation_log;

-- ============================================================
-- MPP-1: L1 segment distribution — explicit verification
--        INSERT different devices → L1 lands on different segments.
--        REFRESH gathers all L1 across segments correctly.
-- ============================================================
\echo '=== MPP-1: L1 segment distribution ==='
CREATE TABLE mpp_src (time TIMESTAMPTZ NOT NULL, device INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (device);
INSERT INTO mpp_src VALUES ('2024-01-01 00:30+00', 1, 10.0);

CREATE MATERIALIZED VIEW cv_mpp WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         device, avg(val) AS avg_val
  FROM mpp_src GROUP BY bucket, device;
CALL time_series.refresh_continuous_aggregate('cv_mpp', NULL, NULL);

-- INSERT multiple devices → trigger writes L1 to different segments
INSERT INTO mpp_src VALUES ('2024-01-01 00:45+00', 1, 11.0);
INSERT INTO mpp_src VALUES ('2024-01-01 00:45+00', 2, 22.0);
INSERT INTO mpp_src VALUES ('2024-01-01 00:45+00', 3, 33.0);
INSERT INTO mpp_src VALUES ('2024-01-01 00:45+00', 4, 44.0);
INSERT INTO mpp_src VALUES ('2024-01-01 00:45+00', 5, 55.0);

-- Verify L1 entries are distributed across segments (not all on one)
SELECT count(DISTINCT gp_segment_id) > 1 AS l1_multi_segment
FROM time_series.cagg_invalidation_log;

-- Total L1 count
SELECT count(*) AS l1_total FROM time_series.cagg_invalidation_log;

-- REFRESH gathers ALL L1 from ALL segments
CALL time_series.refresh_continuous_aggregate('cv_mpp', NULL, NULL);

-- L1 should be empty (all consumed)
SELECT count(*) AS l1_after_gather FROM time_series.cagg_invalidation_log;

-- All data correct (REFRESH didn't miss any segment's L1)
SELECT count(*) AS diff_mpp1 FROM (
  (  SELECT bucket, device, round(avg_val::numeric, 10) FROM cv_mpp
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, round(avg(val)::numeric, 10)
   FROM mpp_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, round(avg(val)::numeric, 10)
   FROM mpp_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, round(avg_val::numeric, 10) FROM cv_mpp
   )
) x;

-- Verify all 5 devices are materialized
SELECT count(DISTINCT device) AS devices_in_mat FROM cv_mpp;

DROP TABLE mpp_src CASCADE;

-- ============================================================
-- Threshold table tests (cagg_invalidation_threshold)
-- ============================================================

-- ============================================================
-- TH-01: CREATE CAGG → threshold rows initialized per segment
-- ============================================================
\echo '=== TH-01: threshold init ==='
CREATE TABLE th_src (time TIMESTAMPTZ NOT NULL, v INT, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO th_src VALUES ('2024-01-01 00:30+00', 1, 10.0);

CREATE MATERIALIZED VIEW cv_th1 WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM th_src GROUP BY bucket;

-- Should have per-segment rows (3 in demo cluster), all -infinity
SELECT count(*) AS th_rows FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;
SELECT bool_and(threshold = '-infinity'::timestamptz) AS th_all_neg_inf
FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- ============================================================
-- TH-02: REFRESH → threshold updated to MAX(watermark)
-- ============================================================
\echo '=== TH-02: threshold after REFRESH ==='
CALL time_series.refresh_continuous_aggregate('cv_th1', NULL, NULL);

-- Threshold should now be > -infinity (close to now())
SELECT bool_and(threshold > '2024-01-01'::timestamptz) AS th_advanced
FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- Threshold should equal MAX(watermark) for this source
SELECT bool_and(t.threshold = w.max_wm) AS th_equals_max_wm
FROM time_series.cagg_invalidation_threshold t,
     (SELECT MAX(watermark) AS max_wm FROM time_series.cagg_watermark
      WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                       WHERE user_view_name = 'cv_th1')) w
WHERE t.source_table_oid = 'th_src'::regclass;

-- ============================================================
-- TH-03: Second CAGG on same source → no duplicate threshold rows
-- ============================================================
\echo '=== TH-03: second CAGG no dup threshold ==='
CREATE MATERIALIZED VIEW cv_th2 WITH (time_series.continuous) AS
  SELECT time_bucket('2 hour'::interval, time) AS bucket, avg(val) AS a
  FROM th_src GROUP BY bucket;

-- Threshold row count should be SAME as before (no duplicates)
SELECT count(*) AS th_rows_after_2nd FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- ============================================================
-- TH-04: Multi-CAGG threshold = MAX of all watermarks
-- ============================================================
\echo '=== TH-04: multi-CAGG threshold = MAX ==='
-- Refresh both
CALL time_series.refresh_continuous_aggregate('cv_th1', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('cv_th2', NULL, NULL);

-- Threshold = MAX of both CAGGs' watermarks
SELECT bool_and(t.threshold = w.max_wm) AS th_is_max
FROM time_series.cagg_invalidation_threshold t,
     (SELECT MAX(w2.watermark) AS max_wm
      FROM time_series.cagg_watermark w2
      JOIN time_series.continuous_agg c ON w2.cagg_id = c.cagg_id
      WHERE c.source_table_oid = 'th_src'::regclass) w
WHERE t.source_table_oid = 'th_src'::regclass;

-- ============================================================
-- TH-05: DROP first CAGG → threshold preserved (second still exists)
-- ============================================================
\echo '=== TH-05: DROP first CAGG threshold preserved ==='
DROP VIEW cv_th1 CASCADE;

SELECT count(*) AS th_after_drop_1 FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- cv_th2 still works
CALL time_series.refresh_continuous_aggregate('cv_th2', NULL, NULL);
SELECT count(*) AS diff_th5 FROM (
  SELECT bucket, a FROM cv_th2 EXCEPT
  SELECT time_bucket('2 hour'::interval, time), avg(val)
  FROM th_src GROUP BY 1
) x;

-- ============================================================
-- TH-06: DROP last CAGG → threshold rows cleaned up
-- ============================================================
\echo '=== TH-06: DROP last CAGG threshold cleanup ==='
DROP VIEW cv_th2 CASCADE;

SELECT count(*) AS th_after_drop_all FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- ============================================================
-- TH-07: Trigger uses threshold (fast path) correctly
--        INSERT below threshold → L1; INSERT above → no L1
-- ============================================================
\echo '=== TH-07: trigger reads threshold ==='
-- Re-create for trigger test
CREATE MATERIALIZED VIEW cv_th_trig WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM th_src GROUP BY bucket;
CALL time_series.refresh_continuous_aggregate('cv_th_trig', NULL, NULL);

-- Verify threshold is high (> source data)
SELECT bool_and(threshold > '2024-12-31'::timestamptz) AS th_high
FROM time_series.cagg_invalidation_threshold
WHERE source_table_oid = 'th_src'::regclass;

-- INSERT below threshold → should write L1
INSERT INTO th_src VALUES ('2024-01-01 01:30+00', 2, 20.0);
SELECT count(*) > 0 AS l1_below_th FROM time_series.cagg_invalidation_log;

-- Clear L1
CALL time_series.refresh_continuous_aggregate('cv_th_trig', NULL, NULL);

-- INSERT above threshold (far future) → no L1
-- 2025 (was 2099): 4-hour chunks + 2020 origin can't reach 2099 within
-- UINT16_MAX chunk count.  Still well above the test watermark.
INSERT INTO th_src VALUES ('2025-01-01 00:00+00', 3, 30.0);
SELECT count(*) AS l1_above_th FROM time_series.cagg_invalidation_log;

DROP TABLE th_src CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE tsdb_src CASCADE;

-- ============================================================
-- I-17 ~ I-20: L1 Batch Accumulator + Subtransaction Rollback
--
-- PL/pgSQL EXCEPTION blocks create subtransactions. Previously,
-- GetCurrentTransactionId() returned a different xid inside the
-- subtransaction, causing cagg_l1_batch_add() to re-initialize
-- the batch and discard prior entries.
--
-- Fix: Use GetTopTransactionId() (always returns top-level xid).
--
-- Test construction principle:
--   Committed INSERTs MUST be in DIFFERENT buckets.  If they
--   share a bucket, one INSERT's L1 covers both, masking the
--   loss of the other's L1.
-- ============================================================

\echo '=== I-17: Subtransaction rollback — different buckets ==='

CREATE TABLE subxact_src (
    time   TIMESTAMPTZ NOT NULL,
    device INT,
    val    FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (device);

INSERT INTO subxact_src
SELECT '2026-01-01'::timestamptz + (i * interval '1 min'), 1, i::float8
FROM generate_series(1, 120) i;

CREATE MATERIALIZED VIEW subxact_hourly
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         sum(val) AS total
  FROM subxact_src GROUP BY 1;

CALL time_series.refresh_continuous_aggregate('subxact_hourly', NULL, NULL);

-- I-17: Basic subtransaction rollback, A/C in different buckets
DO $$
BEGIN
    INSERT INTO subxact_src VALUES ('2026-01-01 01:15:00+00', 1, 9001);
    BEGIN
        INSERT INTO subxact_src VALUES ('2026-01-01 01:45:00+00', 1, 9002);
        RAISE EXCEPTION 'force rollback';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    INSERT INTO subxact_src VALUES ('2026-01-01 00:30:00+00', 1, 9003);
END;
$$;

SELECT count(*) > 0 AS l1_exists FROM time_series.cagg_invalidation_log;

CALL time_series.refresh_continuous_aggregate('subxact_hourly', NULL, NULL);

-- Bucket 00:00: 1770 + 9003 = 10773
SELECT CASE WHEN total = 10773 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i17_bucket_00
FROM subxact_hourly WHERE bucket = '2026-01-01 00:00:00+00';

-- Bucket 01:00: 5370 + 9001 = 14371  (critical: before fix this was 5370)
SELECT CASE WHEN total = 14371 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i17_bucket_01
FROM subxact_hourly WHERE bucket = '2026-01-01 01:00:00+00';

\echo '=== I-18: Nested subtransactions (2 levels) ==='

DO $$
BEGIN
    INSERT INTO subxact_src VALUES ('2026-01-01 00:45:00+00', 1, 8001);
    BEGIN
        INSERT INTO subxact_src VALUES ('2026-01-01 01:10:00+00', 1, 8002);
        BEGIN
            INSERT INTO subxact_src VALUES ('2026-01-01 01:55:00+00', 1, 8003);
            RAISE EXCEPTION 'inner rollback';
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    INSERT INTO subxact_src VALUES ('2026-01-01 01:20:00+00', 1, 8004);
END;
$$;

CALL time_series.refresh_continuous_aggregate('subxact_hourly', NULL, NULL);

-- Bucket 00:00: 10773 + 8001 = 18774
SELECT CASE WHEN total = 18774 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i18_bucket_00
FROM subxact_hourly WHERE bucket = '2026-01-01 00:00:00+00';

-- Bucket 01:00: 14371 + 8002 + 8004 = 30377
SELECT CASE WHEN total = 30377 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i18_bucket_01
FROM subxact_hourly WHERE bucket = '2026-01-01 01:00:00+00';

\echo '=== I-19: All subtransaction INSERTs rolled back ==='

DO $$
BEGIN
    BEGIN
        INSERT INTO subxact_src VALUES ('2026-01-01 00:05:00+00', 1, 7777);
        RAISE EXCEPTION 'rollback all';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END;
$$;

SELECT count(*) AS should_be_zero FROM subxact_src WHERE val = 7777;

CALL time_series.refresh_continuous_aggregate('subxact_hourly', NULL, NULL);

SELECT CASE WHEN total = 18774 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i19_bucket_00
FROM subxact_hourly WHERE bucket = '2026-01-01 00:00:00+00';

\echo '=== I-20: INSERT above threshold + subtransaction ==='

DO $$
BEGIN
    INSERT INTO subxact_src VALUES ('2026-06-01 10:00:00+00', 1, 6001);
    BEGIN
        INSERT INTO subxact_src VALUES ('2026-06-01 11:00:00+00', 1, 6002);
        RAISE EXCEPTION 'rollback';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
    INSERT INTO subxact_src VALUES ('2026-01-01 00:50:00+00', 1, 6003);
END;
$$;

CALL time_series.refresh_continuous_aggregate('subxact_hourly', NULL, NULL);

-- Bucket 00:00: 18774 + 6003 = 24777
SELECT CASE WHEN total = 24777 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i20_bucket_00
FROM subxact_hourly WHERE bucket = '2026-01-01 00:00:00+00';

DROP TABLE subxact_src CASCADE;

-- ============================================================
-- I-21: DROP one CAGG must NOT wipe L1 for surviving CAGGs on same source.
--
-- Before fix: _cagg_cleanup_catalog unconditionally cleared
-- cagg_invalidation_log for the source on every CAGG drop, stripping
-- pending invalidations from any OTHER CAGGs still on that source.
-- Aligns with TSDB upstream's hyper_invalidation_log lifetime
-- (lives until source is dropped).
-- ============================================================
\echo '=== I-21: DROP one CAGG preserves L1 for siblings on same source ==='

CREATE TABLE i21_src (time TIMESTAMPTZ NOT NULL, tag INT, val INT)
USING time_series WITH (ts_partition_column='time', ts_chunk_interval='1 day', ts_chunk_origin='2020-01-01')
DISTRIBUTED BY (tag);

-- Two days of source data so threshold lands after day 1, leaving room
-- for a backfill INSERT into day 1 (time < threshold → L1 logged).
INSERT INTO i21_src VALUES
  ('2026-01-01 12:00+00', 1, 100),
  ('2026-01-02 12:00+00', 1, 100);

CREATE MATERIALIZED VIEW i21_cv1 WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tag, sum(val) AS total
  FROM i21_src GROUP BY bucket, tag;

CREATE MATERIALIZED VIEW i21_cv2 WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket, tag, sum(val) AS total
  FROM i21_src GROUP BY bucket, tag;

-- Initial refresh both → watermarks advance to '2026-01-02', L1 cleared.
CALL time_series.refresh_continuous_aggregate('i21_cv1', NULL, NULL);
CALL time_series.refresh_continuous_aggregate('i21_cv2', NULL, NULL);

-- Backfill INSERT at day 1 (time=12:30 < threshold='2026-01-02') → L1 logged.
INSERT INTO i21_src VALUES ('2026-01-01 12:30+00', 1, 200);

-- L1 should have at least 1 entry now (one per segment that received the row).
SELECT CASE WHEN count(*) >= 1 THEN 'OK'
            ELSE 'FAIL: L1 not written, count=' || count(*)::text END AS i21_l1_before_drop
FROM time_series.cagg_invalidation_log
WHERE source_table_oid = 'i21_src'::regclass::oid;

-- Drop ONE CAGG: the L1 must remain for the surviving sibling.
DROP VIEW i21_cv1 CASCADE;

SELECT CASE WHEN count(*) >= 1 THEN 'OK'
            ELSE 'FAIL: L1 wiped, count=' || count(*)::text END AS i21_l1_preserved
FROM time_series.cagg_invalidation_log
WHERE source_table_oid = 'i21_src'::regclass::oid;

-- Refresh the surviving CAGG → it MUST see the backfill via the preserved L1.
CALL time_series.refresh_continuous_aggregate('i21_cv2', NULL, NULL);
SELECT CASE WHEN total = 300 THEN 'OK' ELSE 'FAIL: ' || total::text END AS i21_sibling_sees_backfill
FROM i21_cv2 WHERE bucket = '2026-01-01 00:00:00+00';

-- Capture the OID before dropping the table.
SELECT 'i21_src'::regclass::oid AS i21_src_oid \gset

-- Drop the last CAGG → L1 should now be wiped (no consumers remain).
DROP VIEW i21_cv2 CASCADE;

SELECT CASE WHEN count(*) = 0 THEN 'OK'
            ELSE 'FAIL: L1 not wiped after last drop, count=' || count(*)::text END AS i21_l1_wiped_on_last_drop
FROM time_series.cagg_invalidation_log
WHERE source_table_oid = :'i21_src_oid'::oid;

DROP TABLE i21_src CASCADE;

\echo '=== INVALIDATION TESTS DONE ==='
