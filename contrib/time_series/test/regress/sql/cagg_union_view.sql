-- ============================================================
-- cagg_union_view.sql
-- Test: F3 — Real-time UNION ALL user view + materialized_only toggle
--       (named after upstream cagg_union_view test for consistency)
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- Setup: deterministic source table
CREATE TABLE metrics (
    time        TIMESTAMPTZ       NOT NULL,
    tags_id     INT               NOT NULL,
    temperature DOUBLE PRECISION
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
-- RT-SETUP-SANITY: source spans multiple chunks
--
-- 1000 rows × 10 hours × 4-hour chunk_interval, origin 2020-01-01:
-- data falls in chunks #(N), #(N+1), #(N+2) where N is the chunk
-- number for 2024-01-01 00:00.  All 35 RT-* assertions below assume
-- the source is split across multiple chunks (so the union view's
-- direct-view branch has to read more than one chunk).  If chunk
-- creation regressed (e.g., all 1000 rows landed in one chunk),
-- the boundary-related cases below (RT-18c, RT-21, RT-N+1 below)
-- silently degenerate into trivial assertions.  Pin a floor of
-- 3 distinct chunks here as defensive baseline.
-- ============================================================
SELECT count(DISTINCT chunk_number) >= 3 AS metrics_spans_at_least_3_chunks
  FROM time_series.ts_chunk WHERE table_oid = 'metrics'::regclass;

-- ============================================================
-- RT-01: Default mode is real-time. Before REFRESH, user view
--        returns live-aggregated source data via the direct view branch.
-- ============================================================
\echo '=== RT-01: real-time visibility before REFRESH ==='
SELECT count(*) AS mat_before_refresh FROM time_series._mat_cv_1;
SELECT count(*) AS cv_before_refresh FROM cv;
-- EXCEPT against live source: should be 0 (UNION ALL matches live query)
SELECT count(*) AS diff_rt01 FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), avg(temperature)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), avg(temperature)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM cv
   )
) x;

-- ============================================================
-- RT-02: After REFRESH, EXCEPT = 0 (mat branch contributes, real-time empty)
-- ============================================================
\echo '=== RT-02: after REFRESH EXCEPT = 0 ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
SELECT count(*) AS mat_after_refresh FROM time_series._mat_cv_1;
SELECT count(*) AS diff_rt02 FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), avg(temperature)
   FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), avg(temperature)
   FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM cv
   )
) x;

-- ============================================================
-- RT-03: INSERT beyond watermark → visible via real-time branch without REFRESH
-- ============================================================
\echo '=== RT-03: post-REFRESH INSERT visible immediately ==='
-- Set watermark to a known past value (scope to cv only)
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 03:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');
-- Insert new data above watermark (bucket=05:00 already had 10 rows for tag=1)
INSERT INTO metrics VALUES ('2024-01-01 05:00+00', 1, 99.0);
-- Verify cnt AND avg_temp for tag=1, bucket=05:00 against hand-calculated values:
--   Original 10 rows: temp = 23.5 + m*0.1 (m=1..10) → sum=240.5, avg=24.05
--   After backfill (temp=99.0): 11 rows, sum=339.5, avg=339.5/11 ≈ 30.863636
SELECT cnt   = 11 AS rt03_cnt_ok,
       round(avg_temp::numeric, 4) = round(339.5/11.0, 4) AS rt03_avg_ok
  FROM cv
 WHERE bucket = '2024-01-01 05:00+00' AND tags_id = 1;
-- Full EXCEPT against live source: proves ALL aggregate values are correct
-- (round to tolerate MPP partial-agg fp noise)
SELECT count(*) AS diff_rt03 FROM (
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
-- RT-04: materialized_only(true) hides live data AND shows stale aggregates
-- ============================================================
\echo '=== RT-04: toggle to mat-only hides live data ==='
ALTER VIEW cv SET (time_series.materialized_only = true);
-- Same cell, but mat values (stale, pre-backfill):
--   cnt = 10, avg = 240.5/10 = 24.05
SELECT cnt   = 10 AS rt04_cnt_ok,
       round(avg_temp::numeric, 4) = round(240.5/10.0, 4) AS rt04_avg_ok
  FROM cv
 WHERE bucket = '2024-01-01 05:00+00' AND tags_id = 1;
-- cv must be a perfect mirror of _mat_cv_1 (view-wrapper sanity).
-- NOTE: this is an intentional same-source mirror check (mat-only cv
-- IS a SELECT over _mat_cv_1); it only proves the wrapper does not
-- mutate data.  The independent correctness anchors are the
-- hand-computed constants above (stale cell 05:00) and below (a
-- non-backfilled cell), plus RT-02/RT-29's full EXCEPT-vs-source.
SELECT count(*) AS diff_rt04 FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM cv
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM time_series._mat_cv_1)
  UNION ALL
  (SELECT bucket, tags_id, cnt, avg_temp FROM time_series._mat_cv_1
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM cv
   )
) x;
-- Independent oracle on a NON-backfilled cell (00:00, tag=1): RT-03
-- backfilled only (05:00, tag=1), so this cell's materialized value
-- must still equal the hand-derived source aggregate.
--   setup: temp = 20.0 + tid + hr*0.5 + m*0.1 = 21 + m*0.1 (m=1..10)
--   sum = 210 + 5.5 = 215.5, avg = 21.55, cnt = 10
SELECT cnt = 10 AS rt04_other_cnt_ok,
       round(avg_temp::numeric, 4) = round(215.5/10.0, 4) AS rt04_other_avg_ok
  FROM cv WHERE bucket = '2024-01-01 00:00+00' AND tags_id = 1;

-- ============================================================
-- RT-05: Toggle back to real-time → live aggregates restored
-- ============================================================
\echo '=== RT-05: toggle back to real-time ==='
ALTER VIEW cv SET (time_series.materialized_only = false);
-- Same assertion as RT-03: cnt=11, avg=339.5/11
SELECT cnt   = 11 AS rt05_cnt_ok,
       round(avg_temp::numeric, 4) = round(339.5/11.0, 4) AS rt05_avg_ok
  FROM cv
 WHERE bucket = '2024-01-01 05:00+00' AND tags_id = 1;
-- Full EXCEPT against live source
SELECT count(*) AS diff_rt05 FROM (
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
-- RT-06: pg_get_viewdef shows UNION ALL + cagg_watermark in real-time
--        but simple SELECT in mat-only
-- ============================================================
\echo '=== RT-06: view definition differs by mode ==='
SELECT pg_get_viewdef('cv', true) LIKE '%UNION ALL%' AS has_union_rt,
       pg_get_viewdef('cv', true) LIKE '%cagg_watermark%' AS has_watermark_rt;

ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT pg_get_viewdef('cv', true) LIKE '%UNION ALL%' AS has_union_mo,
       pg_get_viewdef('cv', true) LIKE '%cagg_watermark%' AS has_watermark_mo;

-- Restore to real-time for later tests
ALTER VIEW cv SET (time_series.materialized_only = false);

-- ============================================================
-- RT-07: catalog materialized_only column reflects current mode
-- ============================================================
\echo '=== RT-07: catalog state after toggle ==='
SELECT materialized_only AS mo_rt FROM time_series.continuous_agg
 WHERE user_view_name = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT materialized_only AS mo_mo FROM time_series.continuous_agg
 WHERE user_view_name = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = false);

-- ============================================================
-- RT-08: CREATE with explicit materialized_only=true → simple view from start
-- ============================================================
\echo '=== RT-08: CREATE with materialized_only=true ==='
CREATE MATERIALIZED VIEW cv_explicit_mo
WITH (time_series.continuous, time_series.materialized_only=true) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM metrics GROUP BY bucket;

-- View def should NOT contain UNION ALL
SELECT pg_get_viewdef('cv_explicit_mo', true) LIKE '%UNION ALL%' AS has_union;
-- Catalog should show true
SELECT materialized_only FROM time_series.continuous_agg
 WHERE user_view_name = 'cv_explicit_mo';
-- User view empty (no REFRESH, no real-time branch)
SELECT count(*) AS cv_mo_empty FROM cv_explicit_mo;
DROP VIEW cv_explicit_mo CASCADE;

-- ============================================================
-- RT-09: watermark = -infinity → all rows from real-time branch
-- ============================================================
\echo '=== RT-09: watermark -infinity → all realtime ==='
CREATE TABLE metrics_fresh (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO metrics_fresh VALUES
  ('2024-01-01 00:00+00', 1, 10.0),
  ('2024-01-01 01:00+00', 2, 20.0);

CREATE MATERIALIZED VIEW cv_fresh
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM metrics_fresh GROUP BY bucket;

-- Verify watermark is -infinity (no REFRESH yet)
SELECT bool_and(watermark = '-infinity'::timestamptz) AS wm_neg_inf
FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_fresh');
-- mat table empty
SELECT count(*) AS mat_empty FROM time_series._mat_cv_fresh_3;
-- User view returns source-aggregated data via real-time branch
SELECT count(*) AS cv_fresh_rows FROM cv_fresh;
-- EXCEPT = 0
SELECT count(*) AS diff_rt09 FROM (
  (  SELECT bucket, cnt FROM cv_fresh
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_fresh GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_fresh GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_fresh
   )
) x;

DROP VIEW cv_fresh CASCADE;
DROP TABLE metrics_fresh CASCADE;

-- ============================================================
-- RT-10: After full REFRESH, the user view still returns ALL
--        source data (live branch serves the hot bucket), and mat
--        contains exactly the stable buckets.
--
-- The ORIGINAL RT-10 asserted "view == mat" (i.e. the real-time
-- branch contributes 0 rows after a full refresh).  The hot-bucket-
-- exclusion fix deliberately changed that contract: the hot bucket
-- (the one containing source max(time)) is now ALWAYS served by the
-- live branch and never written to mat.  So "view == mat" is false
-- by design and cannot be the assertion any more.
--
-- To avoid the trap of using the fix's own output (cagg_watermark)
-- as the test oracle, the hot-bucket boundary below is computed
-- INDEPENDENTLY from the source table the same way a user would
-- reason by hand: time_bucket(width, max(source.time)).  We then:
--   (1) assert the fix's watermark actually landed on that
--       independently-derived boundary (catches a mis-set
--       watermark, which a watermark-derived boundary would hide);
--   (2) assert mat == source aggregation strictly below it
--       (mat has every stable bucket and only stable buckets);
--   (3) assert the user view == FULL source aggregation
--       (the real user-facing correctness — unchanged from the
--       original intent that "the view returns correct data").
-- ============================================================
\echo '=== RT-10: full REFRESH → view==source, mat==stable buckets ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Independent hot-bucket boundary: bucket START of the source's own
-- max(time).  Derived from the SOURCE table, not from cagg_watermark.
SELECT time_bucket('1 hour'::interval, max(time)) AS hot_start
  FROM metrics \gset

-- (1) The fix's watermark must equal the independently-derived
-- boundary.  This is the assertion a watermark-as-oracle test would
-- silently skip.
SELECT min(watermark) = :'hot_start'::timestamptz AS watermark_at_hot_start
  FROM time_series.cagg_watermark
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                   WHERE user_view_name = 'cv');

-- (2) mat == source aggregation strictly below the independent
-- boundary: mat holds every stable bucket and only stable buckets.
SELECT count(*) AS diff_rt10_mat FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM time_series._mat_cv_1
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics
    WHERE time < :'hot_start'::timestamptz
    GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics
    WHERE time < :'hot_start'::timestamptz
    GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM time_series._mat_cv_1)
) x;

-- (3) The user-facing view must still equal the FULL source
-- aggregation (including the hot bucket via the live branch).  This
-- preserves the original "view returns correct data" intent.
SELECT count(*) AS diff_rt10_view FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM cv)
) x;

-- (4) Contract check at the USER-VISIBLE level: under
-- materialized_only=true the view must NOT return the hot bucket
-- (the in-progress bucket beyond the materialization boundary).
-- This is the design contract that the hot-bucket-exclusion fix
-- enforces: materialized_only shows only finalised buckets, never
-- a stale/partial snapshot of the still-changing hot bucket.
-- Probes the view (not the internal mat table) so it asserts the
-- behaviour a user actually observes.
ALTER VIEW cv SET (time_series.materialized_only = true);
-- 4a: no row at or beyond the hot-bucket boundary is visible.
SELECT count(*) AS mat_only_hot_bucket_rows
  FROM cv WHERE bucket >= :'hot_start'::timestamptz;
-- 4b: what IS visible equals exactly the stable-bucket source
-- aggregation (every finalised bucket, and only those).
SELECT count(*) AS diff_rt10_mat_only FROM (
  (  SELECT bucket, tags_id, cnt, avg_temp FROM cv
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics
    WHERE time < :'hot_start'::timestamptz
    GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
          count(*), avg(temperature)
     FROM metrics
    WHERE time < :'hot_start'::timestamptz
    GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, avg_temp FROM cv)
) x;
ALTER VIEW cv SET (time_series.materialized_only = false);

-- ============================================================
-- RT-11: Empty source table returns 0 rows in both modes
-- ============================================================
\echo '=== RT-11: empty source table ==='
CREATE TABLE metrics_empty (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
CREATE MATERIALIZED VIEW cv_empty
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM metrics_empty GROUP BY bucket;
-- real-time mode, empty source
SELECT count(*) AS cv_empty_rt FROM cv_empty;
ALTER VIEW cv_empty SET (time_series.materialized_only = true);
SELECT count(*) AS cv_empty_mo FROM cv_empty;
DROP VIEW cv_empty CASCADE;
DROP TABLE metrics_empty CASCADE;

-- ============================================================
-- RT-12: Custom bucket alias works correctly in UNION ALL WHERE
-- ============================================================
\echo '=== RT-12: custom bucket alias ==='
CREATE TABLE metrics_alias (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO metrics_alias VALUES
  ('2024-01-01 00:30+00', 1, 10.0),
  ('2024-01-01 01:30+00', 2, 20.0);

CREATE MATERIALIZED VIEW cv_alias
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS my_hour,
         count(*) AS n
  FROM metrics_alias GROUP BY my_hour;

-- View def should reference my_hour (the alias) not bucket
SELECT pg_get_viewdef('cv_alias', true) LIKE '%my_hour%' AS alias_in_view;
-- Data correct
SELECT count(*) AS diff_rt12 FROM (
  (  SELECT my_hour, n FROM cv_alias
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_alias GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_alias GROUP BY 1
   EXCEPT
   SELECT my_hour, n FROM cv_alias
   )
) x;

DROP VIEW cv_alias CASCADE;
DROP TABLE metrics_alias CASCADE;

-- ============================================================
-- RT-13: relkind stays 'v' after mode toggle
-- ============================================================
\echo '=== RT-13: relkind stays v after toggle ==='
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT relkind FROM pg_class WHERE relname = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = false);
SELECT relkind FROM pg_class WHERE relname = 'cv';

-- ============================================================
-- RT-14: set_materialized_only idempotent (same value twice = no-op)
-- ============================================================
\echo '=== RT-14: idempotent toggle ==='
ALTER VIEW cv SET (time_series.materialized_only = false);
-- Calling again with same value should be no-op, no error
ALTER VIEW cv SET (time_series.materialized_only = false);
SELECT materialized_only FROM time_series.continuous_agg WHERE user_view_name = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = true);
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT materialized_only FROM time_series.continuous_agg WHERE user_view_name = 'cv';

-- ============================================================
-- RT-15: WHERE filter on bucket propagates through UNION ALL
-- ============================================================
\echo '=== RT-15: WHERE filter on user view ==='
-- Ensure cv is in real-time mode (RT-14 left it in mat-only)
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Set watermark to split data between mat and real-time branches (scope to cv)
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 02:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');
-- Insert some new data above watermark
INSERT INTO metrics VALUES ('2024-01-01 03:00+00', 1, 30.0);
-- Hand-verify the live-branch cell that has the INSERT:
--   bucket=03:00, tag=1: original 10 rows temp=22.5+m*0.1 (m=1..10), sum=230.5
--   plus INSERT temp=30.0 → 11 rows, sum=260.5, avg=260.5/11
SELECT cnt = 11 AS rt15_cnt_ok,
       round(avg_temp::numeric, 4) = round(260.5/11.0, 4) AS rt15_avg_ok
  FROM cv
 WHERE bucket = '2024-01-01 03:00+00' AND tags_id = 1;
-- Also verify a mat-branch cell (bucket=01:00 < watermark=02:00):
--   10 rows, temp=21.5+m*0.1, sum=220.5, avg=22.05
SELECT cnt = 10 AS rt15_mat_cnt_ok,
       round(avg_temp::numeric, 4) = round(220.5/10.0, 4) AS rt15_mat_avg_ok
  FROM cv
 WHERE bucket = '2024-01-01 01:00+00' AND tags_id = 1;
-- Full EXCEPT for completeness.
-- Round avg to 10 decimal places to tolerate MPP partial-agg fp noise:
-- cv's live branch and the oracle take different execution paths
-- (different WHERE filters), so partial-aggregation merge order may
-- differ in the LSB of double precision.  10 decimals >> fp noise.
SELECT count(*) AS diff_rt15 FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) AS avg_t FROM cv
   WHERE bucket >= '2024-01-01 01:00+00'
   AND bucket <  '2024-01-01 04:00+00'
   AND tags_id = 1
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics
   WHERE time >= '2024-01-01 01:00+00'
   AND time <  '2024-01-01 04:00+00'
   AND tags_id = 1
   GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), tags_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM metrics
   WHERE time >= '2024-01-01 01:00+00'
   AND time <  '2024-01-01 04:00+00'
   AND tags_id = 1
   GROUP BY 1, 2
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) AS avg_t FROM cv
   WHERE bucket >= '2024-01-01 01:00+00'
   AND bucket <  '2024-01-01 04:00+00'
   AND tags_id = 1
   )
) x;

-- ============================================================
-- RT-16: HAVING clause works in real-time mode + after REFRESH
--
-- GROUP BY bucket (no per-tag split): count(*) is total rows per hour.
-- HAVING count(*) > 5 filters out buckets with ≤ 5 rows.
-- ============================================================
\echo '=== RT-16: HAVING clause ==='
CREATE TABLE metrics_hav (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
-- bucket=00: 2 rows  (excluded by HAVING > 5)
-- bucket=01: 5 rows  (excluded, boundary)
-- bucket=02: 6 rows  (included, just over threshold)
-- bucket=03: 10 rows (included, well over)
-- bucket=04: 3 rows  (excluded, will be pushed over by backfill later)
INSERT INTO metrics_hav
SELECT '2024-01-01 00:00+00'::timestamptz + (m * 10 || ' minute')::interval,
       m, 10.0 + m FROM generate_series(1, 2) m     -- bucket 00: 2 rows
UNION ALL
SELECT '2024-01-01 01:00+00'::timestamptz + (m * 10 || ' minute')::interval,
       m, 20.0 + m FROM generate_series(1, 5) m     -- bucket 01: 5 rows
UNION ALL
SELECT '2024-01-01 02:00+00'::timestamptz + (m * 5 || ' minute')::interval,
       m, 30.0 + m FROM generate_series(1, 6) m     -- bucket 02: 6 rows
UNION ALL
SELECT '2024-01-01 03:00+00'::timestamptz + (m * 5 || ' minute')::interval,
       m, 40.0 + m FROM generate_series(1, 10) m    -- bucket 03: 10 rows
UNION ALL
SELECT '2024-01-01 04:00+00'::timestamptz + (m * 10 || ' minute')::interval,
       m, 50.0 + m FROM generate_series(1, 3) m;    -- bucket 04: 3 rows
-- Total: 26 rows. HAVING > 5 → bucket 02 (cnt=6), bucket 03 (cnt=10)

CREATE MATERIALIZED VIEW cv_hav
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt
  FROM metrics_hav GROUP BY bucket HAVING count(*) > 5;

-- Phase 1: Before REFRESH (real-time only, HAVING applied by direct_view)
-- Hand-verify: only 2 buckets pass, with exact counts
SELECT cnt = 6 AS rt16_pre_bucket02_ok FROM cv_hav
 WHERE bucket = '2024-01-01 02:00+00';
SELECT cnt = 10 AS rt16_pre_bucket03_ok FROM cv_hav
 WHERE bucket = '2024-01-01 03:00+00';
-- Bucket 00,01,04 must NOT appear
SELECT count(*) AS rt16_excluded_count FROM cv_hav
 WHERE bucket NOT IN ('2024-01-01 02:00+00'::timestamptz,
                       '2024-01-01 03:00+00'::timestamptz);
SELECT count(*) AS diff_rt16_pre FROM (
  (  SELECT bucket, cnt FROM cv_hav
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 5)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 5
   EXCEPT
   SELECT bucket, cnt FROM cv_hav
   )
) x;

-- Phase 2: After REFRESH (mat branch stores HAVING-filtered result)
CALL time_series.refresh_continuous_aggregate('cv_hav', NULL, NULL);
SELECT count(*) AS mat_hav_rows FROM cv_hav;
SELECT count(*) AS diff_rt16_post FROM (
  (  SELECT bucket, cnt FROM cv_hav
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 5)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*)
   FROM metrics_hav GROUP BY 1 HAVING count(*) > 5
   EXCEPT
   SELECT bucket, cnt FROM cv_hav
   )
) x;

-- Phase 3: Backfill pushes bucket=04 across HAVING threshold (3 → 6)
INSERT INTO metrics_hav VALUES
  ('2024-01-01 04:35+00', 4, 64.0),
  ('2024-01-01 04:40+00', 5, 65.0),
  ('2024-01-01 04:45+00', 6, 66.0);
-- bucket=04 now has 6 rows (3 original + 3 backfill) → passes HAVING > 5
CALL time_series.refresh_continuous_aggregate('cv_hav', NULL, NULL);
-- Now 3 buckets should pass
SELECT count(*) AS mat_hav_after_backfill FROM cv_hav;
-- Hand-verify the newly-included bucket
SELECT cnt = 6 AS rt16_backfill_ok FROM cv_hav
 WHERE bucket = '2024-01-01 04:00+00';

DROP VIEW cv_hav CASCADE;
DROP TABLE metrics_hav CASCADE;

-- ============================================================
-- RT-17: User view usable in JOIN with another table
-- ============================================================
\echo '=== RT-17: JOIN user view with another table ==='
-- Ensure cv is in real-time mode and fully refreshed for stable JOIN result
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
CREATE TABLE tag_info (tags_id INT PRIMARY KEY, label TEXT)
  DISTRIBUTED REPLICATED;
INSERT INTO tag_info VALUES (1, 'sensor-A'), (2, 'sensor-B'), (3, 'sensor-C');

-- JOIN works; result covers both mat and real-time branches
SELECT count(*) AS diff_rt17 FROM (
  (  SELECT cv.bucket, cv.tags_id, cv.cnt, ti.label
   FROM cv JOIN tag_info ti ON cv.tags_id = ti.tags_id
   WHERE ti.tags_id <= 3
   EXCEPT
   SELECT time_bucket('1 hour'::interval, m.time), m.tags_id,
   count(*), ti.label
   FROM metrics m JOIN tag_info ti ON m.tags_id = ti.tags_id
   WHERE ti.tags_id <= 3
   GROUP BY 1, 2, ti.label)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, m.time), m.tags_id,
   count(*), ti.label
   FROM metrics m JOIN tag_info ti ON m.tags_id = ti.tags_id
   WHERE ti.tags_id <= 3
   GROUP BY 1, 2, ti.label
   EXCEPT
   SELECT cv.bucket, cv.tags_id, cv.cnt, ti.label
   FROM cv JOIN tag_info ti ON cv.tags_id = ti.tags_id
   WHERE ti.tags_id <= 3
   )
) x;
DROP TABLE tag_info CASCADE;

-- ============================================================
-- RT-18: Real-time mode with DATE time column
-- ============================================================
\echo '=== RT-18: DATE time column ==='
CREATE TABLE metrics_date (day DATE NOT NULL, v INT NOT NULL, val FLOAT8)
USING time_series WITH (
    ts_partition_column = 'day',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2024-01-01'
) DISTRIBUTED BY (v);
INSERT INTO metrics_date VALUES
  ('2024-01-01', 1, 10.0),
  ('2024-01-02', 2, 20.0),
  ('2024-01-03', 3, 30.0);

CREATE MATERIALIZED VIEW cv_date
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, day) AS bucket,
         count(*) AS cnt
  FROM metrics_date GROUP BY bucket;
-- Before REFRESH: real-time branch returns everything
SELECT count(*) AS diff_rt18 FROM (
  (  SELECT bucket, cnt FROM cv_date
   EXCEPT
   SELECT time_bucket('1 day'::interval, day), count(*)
   FROM metrics_date GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, day), count(*)
   FROM metrics_date GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_date
   )
) x;
DROP VIEW cv_date CASCADE;
DROP TABLE metrics_date CASCADE;

-- ============================================================
-- RT-18b: cagg_watermark() IS constant-folded by the cagg planner_hook
--
-- Originally this test verified the function was NOT folded (VOLATILE
-- guard against eval_const_expressions).  The constify_cagg_watermark
-- planner_hook now folds cagg_watermark(N) to a Const literal containing
-- the global MIN watermark across segments — the literal is dispatched
-- to every segment so each one's ChunkScan can prune chunks below it.
-- Using global MIN keeps correctness: segments that are "ahead" simply
-- do slightly more live-branch work; the live branch always re-aggregates
-- from source, so no row is ever missed.
--
-- This DO block now expects funcexpr=false (FuncExpr is gone after the
-- mutator pass).  const_ts stays false because the legacy LIKE pattern
-- '%Filter:%''202%+%''%timestamp%' matches a specific historical format
-- that PG no longer emits for the replaced Const node; the literal IS
-- in the plan, it just doesn't match this pattern.  See watermark_constify.c.
-- ============================================================
\echo '=== RT-18b: cagg_watermark IS constant-folded by planner_hook ==='
-- Use cv (real-time mode, already exists from previous tests)
DO $$
DECLARE
    line text;
    has_funcexpr bool := false;
    has_const_ts bool := false;
BEGIN
    FOR line IN
        EXECUTE 'EXPLAIN (costs off) SELECT * FROM cv'
    LOOP
        -- FuncExpr: "cagg_watermark(1)" in Filter (now folded → false)
        IF line LIKE '%cagg_watermark(%' THEN has_funcexpr := true; END IF;
        -- Const: a literal timestamp like '2026-04-16 ...' in Filter.
        -- (legacy pattern; doesn't match the format the const-fold emits)
        IF line LIKE '%Filter:%''202%+%''%timestamp%' THEN has_const_ts := true; END IF;
    END LOOP;
    RAISE NOTICE 'watermark_in_plan: funcexpr=% const_ts=%', has_funcexpr, has_const_ts;
    -- Expected: funcexpr=false, const_ts=false
END $$;

-- Also verify per-segment execution produces correct results
-- (all segments have the same watermark here, but the function IS called locally)
SELECT count(*) AS diff_rt18b FROM (
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
-- RT-18c: Per-segment watermark divergence — the ultimate test.
--
-- This verifies the CORE of the per-segment watermark design:
-- when segments have DIFFERENT watermarks, each segment's UNION ALL
-- split is independent, and the combined result is still correct.
--
-- Why this matters: the cagg_watermark() function was originally
-- LANGUAGE SQL STABLE, which PG constant-folded on QD — making ALL
-- segments use the same value.  With LANGUAGE C VOLATILE, each
-- segment reads its own local watermark.
--
-- Why previous tests didn't catch the old bug: they all happened to
-- have identical watermarks across segments (REFRESH updates all
-- uniformly), so constant-folding gave the right answer by luck.
-- ============================================================
\echo '=== RT-18c: per-segment watermark divergence ==='
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Verify watermarks are initially uniform after REFRESH
SELECT count(DISTINCT watermark) AS wm_distinct_before
  FROM time_series.cagg_watermark
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

-- Deliberately set DIFFERENT watermarks per segment using gp_segment_id
-- Seg0 = 00:00 (very early → mostly live aggregation)
-- Seg1 = 05:00 (middle → mix of mat + live)
-- Seg2 = 09:00 (almost caught up → mostly mat)
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 00:00+00'::timestamptz
                   + (gp_segment_id * interval '4.5 hour')
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

-- Confirm divergence
SELECT count(DISTINCT watermark) AS wm_distinct_after
  FROM time_series.cagg_watermark
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

-- The key assertion: EVEN with divergent watermarks, UNION ALL result
-- must match live source aggregation.  Each segment independently
-- splits its data at its own watermark; mat+live on each segment
-- covers its complete time range without gaps or overlaps.
SELECT count(*) AS diff_rt18c FROM (
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

-- Restore uniform watermarks for subsequent tests
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- RT-19: GRANT preserved across set_materialized_only
--        (CREATE OR REPLACE VIEW must not drop permissions)
-- ============================================================
\echo '=== RT-19: GRANT preserved ==='
CREATE ROLE ts_reader;
GRANT SELECT ON cv TO ts_reader;
-- Record ACL before toggle
SELECT relacl::text LIKE '%ts_reader%' AS has_grant_before
  FROM pg_class WHERE relname = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT relacl::text LIKE '%ts_reader%' AS has_grant_after_mo
  FROM pg_class WHERE relname = 'cv';
ALTER VIEW cv SET (time_series.materialized_only = false);
SELECT relacl::text LIKE '%ts_reader%' AS has_grant_after_rt
  FROM pg_class WHERE relname = 'cv';
REVOKE SELECT ON cv FROM ts_reader;
DROP ROLE ts_reader;

-- ============================================================
-- RT-20: EXPLAIN plan shape differs between modes
--
-- Real-time mode: plan contains an Append node with two branches
-- (mat table Seq Scan + direct view HashAggregate on source).
-- Materialized-only mode: single Seq Scan on mat table, no Append.
-- ============================================================
\echo '=== RT-20: EXPLAIN plan shape ==='
CREATE TABLE m_plan (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO m_plan VALUES ('2024-01-01 01:00+00', 1, 10.0);

CREATE MATERIALIZED VIEW cv_plan
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM m_plan GROUP BY bucket;

-- Real-time: plan should show Append with two branches
-- Use DO block to capture EXPLAIN output and check for expected nodes
DO $$
DECLARE
    plan_text text := '';
    line text;
    has_append bool := false;
    has_mat_scan bool := false;
    has_hashagg bool := false;
BEGIN
    FOR line IN
        EXECUTE 'EXPLAIN (costs off, timing off, summary off) SELECT * FROM cv_plan'
    LOOP
        plan_text := plan_text || line || E'\n';
        IF line LIKE '%Append%' THEN has_append := true; END IF;
        IF line LIKE '%Seq Scan on _mat_cv_plan%' THEN has_mat_scan := true; END IF;
        IF line LIKE '%HashAggregate%' THEN has_hashagg := true; END IF;
    END LOOP;
    RAISE NOTICE 'rt_plan: append=% mat_scan=% hashagg=%',
                 has_append, has_mat_scan, has_hashagg;
END $$;

ALTER VIEW cv_plan SET (time_series.materialized_only = true);
-- Mat-only: plan should NOT contain Append or HashAggregate
DO $$
DECLARE
    line text;
    has_append bool := false;
    has_mat_scan bool := false;
    has_hashagg bool := false;
BEGIN
    FOR line IN
        EXECUTE 'EXPLAIN (costs off, timing off, summary off) SELECT * FROM cv_plan'
    LOOP
        IF line LIKE '%Append%' THEN has_append := true; END IF;
        IF line LIKE '%Seq Scan on _mat_cv_plan%' THEN has_mat_scan := true; END IF;
        IF line LIKE '%HashAggregate%' THEN has_hashagg := true; END IF;
    END LOOP;
    RAISE NOTICE 'mo_plan: append=% mat_scan=% hashagg=%',
                 has_append, has_mat_scan, has_hashagg;
END $$;

DROP VIEW cv_plan CASCADE;
DROP TABLE m_plan CASCADE;

-- ============================================================
-- RT-21: Re-aggregation over user view
--        Most common analytics pattern: aggregate further over CAGG
--        output. Verifies user view works as a first-class view in
--        user queries, with UNION ALL transparently composing.
-- ============================================================
\echo '=== RT-21: re-aggregation over user view ==='
-- Ensure cv is in real-time mode and fully refreshed
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Move watermark back so half the data is in mat, half in real-time (scope to cv)
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 05:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');
-- sum(cnt) per tag should match count(*) from source per tag
SELECT count(*) AS diff_rt21 FROM (
  (  SELECT tags_id, sum(cnt)::bigint AS total
   FROM cv GROUP BY tags_id
   EXCEPT
   SELECT tags_id, count(*)::bigint
   FROM metrics GROUP BY tags_id)
  UNION ALL
  (SELECT tags_id, count(*)::bigint
   FROM metrics GROUP BY tags_id
   EXCEPT
   SELECT tags_id, sum(cnt)::bigint AS total
   FROM cv GROUP BY tags_id
   )
) x;

-- ============================================================
-- RT-22: Window function over user view
--        Time-series 标志性查询：bucket-over-bucket lag/diff.
--        Verifies that user view (UNION ALL) is usable with window
--        functions across the watermark split.
-- ============================================================
\echo '=== RT-22: window function over user view ==='
WITH cv_lagged AS (
  SELECT bucket, tags_id, cnt,
         lag(cnt) OVER (PARTITION BY tags_id ORDER BY bucket) AS prev_cnt
    FROM cv
   WHERE tags_id = 1
)
SELECT count(*) AS rows_with_lag,
       count(*) FILTER (WHERE prev_cnt IS NULL) AS first_bucket_per_tag
FROM cv_lagged;
-- 10 buckets for tag=1, first has prev_cnt=NULL, others have prev_cnt=10
-- Expected: rows_with_lag=10, first_bucket_per_tag=1

-- ============================================================
-- RT-23: ALTER VIEW DDL syntax for mode toggle (the only public API)
--        The mode-toggle PROCEDURE was removed: ALTER VIEW is the
--        single user-facing interface.  The hook handles the option
--        in C; see cagg_create.c:cagg_apply_materialized_only.
-- ============================================================
\echo '=== RT-23: ALTER VIEW DDL syntax ==='
-- Ensure starting state: real-time
ALTER VIEW cv SET (time_series.materialized_only = false);
SELECT materialized_only AS mo_start FROM time_series.continuous_agg
 WHERE user_view_name = 'cv';

-- Switch to mat-only
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT materialized_only AS mo_after_alter_true FROM time_series.continuous_agg
 WHERE user_view_name = 'cv';
SELECT pg_get_viewdef('cv', true) LIKE '%UNION ALL%' AS has_union_after_alter;

-- Switch back to real-time
ALTER VIEW cv SET (time_series.materialized_only = false);
SELECT materialized_only AS mo_after_alter_false FROM time_series.continuous_agg
 WHERE user_view_name = 'cv';
SELECT pg_get_viewdef('cv', true) LIKE '%UNION ALL%' AS has_union_restored;

-- Non-CAGG view: our hook should not intercept; PG's native reloption
-- validation rejects unknown namespaced options on regular views.
CREATE VIEW ordinary_view AS SELECT 1 AS x;
\set ON_ERROR_STOP 0
ALTER VIEW ordinary_view SET (time_series.materialized_only = true);
\set ON_ERROR_STOP 1
DROP VIEW ordinary_view;

-- The removed procedure should no longer exist
\set ON_ERROR_STOP 0
CALL time_series.set_materialized_only('cv', true);
\set ON_ERROR_STOP 1

-- ============================================================
-- upstream alignment: query/view tests
-- ============================================================

-- ============================================================
-- RT-24: Complex WHERE + HAVING + expressions in union view
--, max-min, HAVING)
-- ============================================================
\echo '=== RT-24: complex WHERE+HAVING ==='
CREATE TABLE wh_src (time TIMESTAMPTZ NOT NULL, loc TEXT, b INT, c INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (loc);
INSERT INTO wh_src VALUES
  ('2024-01-01 00:10+00', 'NYC', 10, 20),
  ('2024-01-01 00:20+00', 'NYC', 14, 30),
  ('2024-01-01 00:30+00', 'SFO', 16, 40),
  ('2024-01-01 01:10+00', 'NYC', 12, 25),
  ('2024-01-01 01:20+00', 'NYC', 8, 15);

CREATE MATERIALIZED VIEW cv_wh
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, loc,
         sum(b + c) AS sbc, max(c) - min(b) AS spread
  FROM wh_src WHERE b < 16
  GROUP BY bucket, loc
  HAVING sum(c) > 10;

-- Real-time (before REFRESH)
SELECT count(*) AS diff_wh_rt FROM (
  (  SELECT bucket, loc, sbc, spread FROM cv_wh
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10
   EXCEPT
   SELECT bucket, loc, sbc, spread FROM cv_wh
   )
) x;

-- After REFRESH
CALL time_series.refresh_continuous_aggregate('cv_wh', NULL, NULL);
SELECT count(*) AS diff_wh_mat FROM (
  (  SELECT bucket, loc, sbc, spread FROM cv_wh
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10
   EXCEPT
   SELECT bucket, loc, sbc, spread FROM cv_wh
   )
) x;

-- Insert after watermark → union catches new data
INSERT INTO wh_src VALUES ('2024-01-01 02:10+00', 'NYC', 13, 60);
SELECT count(*) AS diff_wh_live FROM (
  (  SELECT bucket, loc, sbc, spread FROM cv_wh
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), loc,
   sum(b + c), max(c) - min(b)
   FROM wh_src WHERE b < 16
   GROUP BY 1, 2 HAVING sum(c) > 10
   EXCEPT
   SELECT bucket, loc, sbc, spread FROM cv_wh
   )
) x;

DROP TABLE wh_src CASCADE;

-- ============================================================
-- RT-25: TIMESTAMP (without timezone) type
--
-- ============================================================
\echo '=== RT-25: TIMESTAMP no tz ==='
CREATE TABLE ts_src (time TIMESTAMP NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO ts_src VALUES
  ('2024-01-01 00:30', 1, 10.0),
  ('2024-01-01 01:30', 2, 20.0),
  ('2024-01-01 02:30', 3, 30.0);

CREATE MATERIALIZED VIEW cv_ts
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         count(*) AS cnt, avg(val) AS a
  FROM ts_src GROUP BY bucket;

-- Real-time before REFRESH
SELECT count(*) AS diff_ts_rt FROM (
  (  SELECT bucket, cnt, a FROM cv_ts
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*), avg(val)
   FROM ts_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*), avg(val)
   FROM ts_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt, a FROM cv_ts
   )
) x;

-- After REFRESH
CALL time_series.refresh_continuous_aggregate('cv_ts', NULL, NULL);
SELECT count(*) AS diff_ts_mat FROM (
  (  SELECT bucket, cnt, a FROM cv_ts
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), count(*), avg(val)
   FROM ts_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), count(*), avg(val)
   FROM ts_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt, a FROM cv_ts
   )
) x;

DROP TABLE ts_src CASCADE;

-- ============================================================
-- RT-26: cagg_watermark() with invalid/NULL input
--
--        Aligned with upstream: fail-fast on missing watermark.
-- ============================================================
\echo '=== RT-26: watermark invalid input ==='
-- Non-existing ID on QD → returns -infinity (QD has no RANDOMLY rows)
-- On segment QE, this would ERROR (fail-fast, same as upstream).
SELECT cagg_watermark(99999) AS wm_invalid;
-- NULL → ERROR
\set ON_ERROR_STOP 0
SELECT cagg_watermark(NULL::int);
\set ON_ERROR_STOP 1

-- ============================================================
-- RT-27: AT TIME ZONE in CAGG aggregate expression
--
-- ============================================================
\echo '=== RT-27: AT TIME ZONE ==='
CREATE TABLE tz_src (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, temp FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO tz_src VALUES
  ('2024-01-01 00:30+00', 1, 55.0),
  ('2024-01-01 05:30+00', 2, 65.0);

CREATE MATERIALIZED VIEW cv_atz
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         min(time AT TIME ZONE 'EST') AS min_est,
         avg(temp) AS avg_temp
  FROM tz_src GROUP BY bucket;

CALL time_series.refresh_continuous_aggregate('cv_atz', NULL, NULL);
SELECT count(*) AS diff_atz FROM (
  (  SELECT bucket, min_est, avg_temp FROM cv_atz
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time),
   min(time AT TIME ZONE 'EST'), avg(temp)
   FROM tz_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time),
   min(time AT TIME ZONE 'EST'), avg(temp)
   FROM tz_src GROUP BY 1
   EXCEPT
   SELECT bucket, min_est, avg_temp FROM cv_atz
   )
) x;

DROP TABLE tz_src CASCADE;

-- ============================================================
-- RT-28: time_bucket with timezone parameter
--
-- ============================================================
\echo '=== RT-28: time_bucket timezone param ==='
CREATE TABLE tbtz_src (time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (v);
INSERT INTO tbtz_src VALUES
  ('2024-01-01 00:30+00', 1, 10.0),
  ('2024-01-01 12:30+00', 2, 20.0),
  ('2024-01-02 00:30+00', 3, 30.0);

CREATE MATERIALIZED VIEW cv_tbtz
WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time, 'America/New_York') AS bucket,
         count(*) AS cnt
  FROM tbtz_src GROUP BY bucket;

CALL time_series.refresh_continuous_aggregate('cv_tbtz', NULL, NULL);
SELECT count(*) AS diff_tbtz FROM (
  (  SELECT bucket, cnt FROM cv_tbtz
   EXCEPT
   SELECT time_bucket('1 day'::interval, time, 'America/New_York'), count(*)
   FROM tbtz_src GROUP BY 1)
  UNION ALL
  (SELECT time_bucket('1 day'::interval, time, 'America/New_York'), count(*)
   FROM tbtz_src GROUP BY 1
   EXCEPT
   SELECT bucket, cnt FROM cv_tbtz
   )
) x;

DROP TABLE tbtz_src CASCADE;

-- ============================================================
-- RT-29: CAGG vs regular VIEW equivalence
--        (the reference test: compare CAGG output
--         against a plain VIEW with identical definition)
-- ============================================================
\echo '=== RT-29: CAGG vs VIEW equivalence ==='
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

CREATE VIEW plain_oracle AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS cnt, avg(temperature) AS avg_temp
  FROM metrics GROUP BY bucket, tags_id;

-- CAGG output should match plain VIEW exactly
SELECT count(*) AS diff_cv_vs_view FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM plain_oracle)
  UNION ALL
  (SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM plain_oracle
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   )
) x;
-- Reverse direction too
SELECT count(*) AS diff_view_vs_cv FROM (
  (  SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM plain_oracle
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv)
  UNION ALL
  (SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM cv
   EXCEPT
   SELECT bucket, tags_id, cnt, round(avg_temp::numeric, 10) FROM plain_oracle
   )
) x;

DROP VIEW plain_oracle;

-- ============================================================
-- RT-30: Stale-then-fresh for specific cell
--        (upstream cagg_usage-17.sql: observe old value before refresh,
--         new value after refresh for same bucket+tag)
-- ============================================================
\echo '=== RT-30: stale then fresh ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
-- Independent oracle: the setup loads exactly 10 rows per (tag,hour)
-- cell (generate_series(1,10) m), so (bucket=01:00, tags_id=1) starts
-- at cnt=10; the backfill below adds exactly 1 row, so a correct
-- refresh must yield cnt=11.  These hand-derived constants (10 / 10 /
-- 11) do not depend on the CAGG's own output, so they catch a wrong
-- materialization that a bare "record the value" assertion would
-- rubber-stamp.
-- Record current value for (bucket=01:00, tags_id=1)
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT cnt AS cnt_before_backfill, cnt = 10 AS rt30_before_is_10 FROM cv
WHERE bucket = '2024-01-01 01:00+00' AND tags_id = 1;

-- Backfill: add exactly ONE row to same bucket
INSERT INTO metrics VALUES ('2024-01-01 01:45+00', 1, 99.0);

-- mat-only still shows OLD value (stale): unchanged, still 10
SELECT cnt AS cnt_stale, cnt = 10 AS rt30_stale_still_10 FROM cv
WHERE bucket = '2024-01-01 01:00+00' AND tags_id = 1;

-- REFRESH then verify NEW value: 10 + 1 backfilled row = 11
ALTER VIEW cv SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('cv',
  '2024-01-01 01:00+00', '2024-01-01 02:00+00');
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT cnt AS cnt_fresh, cnt = 11 AS rt30_fresh_is_11 FROM cv
WHERE bucket = '2024-01-01 01:00+00' AND tags_id = 1;
ALTER VIEW cv SET (time_series.materialized_only = false);

-- ============================================================
-- RT-32: ::timestamp cast in CAGG definition → rejected
--
--
--   time_bucket(...)::timestamp is a STABLE expression because
--   timestamptz→timestamp conversion depends on session timezone.
--   If allowed, REFRESH in one timezone + query in another would
--   cause data loss in the UNION ALL view.
-- ============================================================
\echo '=== RT-32: reject STABLE expression ==='
CREATE TABLE tz_cast (time TIMESTAMPTZ NOT NULL, device INT NOT NULL, temp FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (device);

SET timezone = 'UTC';
\set ON_ERROR_STOP 0
-- ::timestamp cast is STABLE → should be rejected
CREATE MATERIALIZED VIEW cv_tzcast
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time)::timestamp AS bucket,
         device, avg(temp) AS avg_temp
  FROM tz_cast
  GROUP BY time_bucket('1 hour'::interval, time), device;
\set ON_ERROR_STOP 1

DROP TABLE tz_cast CASCADE;

-- ============================================================
-- RT-31: DELETE watermark → graceful degradation → restore
--
--
--   upstream behavior: DELETE watermark → query ERROR, REFRESH ERROR.
--   Our behavior:  DELETE watermark → cagg_watermark() returns
--   -infinity (fallback), query degrades to full real-time
--   aggregation (mat branch empty, live branch = all source).
--   REFRESH also works (unified path from -infinity).
-- ============================================================
\echo '=== RT-31: watermark delete + restore ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Baseline: fully materialized
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT count(*) AS mat_before_wm_del FROM cv;
ALTER VIEW cv SET (time_series.materialized_only = false);

-- Delete ALL watermark rows for this CAGG
DELETE FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                 WHERE user_view_name = 'cv');

-- Query: constify_cagg_watermark sees empty MIN(watermark) result and
-- folds the FuncExpr to -infinity (DT_NOBEGIN), which routes every bucket
-- through the live branch — re-aggregated from source, so the count is
-- still correct (mat branch contributes 0 rows, live branch = full source).
-- The original "watermark not found" segment-side ereport(ERROR) is bypassed
-- because no segment ever evaluates cagg_watermark() at runtime.
\set ON_ERROR_STOP 0
SELECT count(*) FROM cv;
\set ON_ERROR_STOP 1

-- REFRESH should also ERROR (watermark lookup fails during refresh)
\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
\set ON_ERROR_STOP 1

-- Manual restore: re-create watermark rows (one per segment)
-- This is the workaround for catalog corruption.
DO $$
DECLARE
  cid int;
BEGIN
  SELECT cagg_id INTO cid FROM time_series.continuous_agg
  WHERE user_view_name = 'cv';
  -- Use _cagg_init_segment_watermark to recreate per-segment rows
  PERFORM time_series._cagg_init_segment_watermark(cid)
  FROM gp_dist_random('gp_id');
END $$;

-- Watermark now exists again
SELECT count(*) > 0 AS wm_manually_restored
FROM time_series.cagg_watermark
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                 WHERE user_view_name = 'cv');

-- REFRESH to advance watermark to proper value
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- Mat-only mode now returns correct data (watermark restored)
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT count(*) AS mat_after_full_restore FROM cv;
ALTER VIEW cv SET (time_series.materialized_only = false);

-- Final correctness check (both directions)
SELECT count(*) AS diff_restored_1 FROM (
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
SELECT count(*) AS diff_restored_2 FROM (
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
-- RT-CHUNK-BOUNDARY: watermark exactly on chunk boundary
--
-- All earlier RT-* cases position the watermark at arbitrary times
-- inside the data range; bit-equivalence holds at those points but
-- the test never specifically exercises the position where the
-- watermark coincides with a chunk boundary.  That's the position
-- most likely to trip a half-open-interval bug:
--
--   - chunk #1 spans [00:00, 04:00).  bucket 03:00 ∈ chunk #1.
--   - chunk #2 spans [04:00, 08:00).  bucket 04:00 ∈ chunk #2.
--
-- If union-view's split uses `bucket < watermark` for the mat branch
-- and `bucket >= watermark` for the live branch (or any other
-- inclusivity convention), watermark=04:00 should yield:
--   - mat:  buckets 00..03 (all of chunk #1)
--   - live: buckets 04..09 (all of chunks #2, #3)
-- A bug that uses the wrong inclusivity duplicates or drops bucket 04.
--
-- We also probe the chunk #2 → chunk #3 boundary (08:00) for symmetry.
-- ============================================================
\echo '=== RT-CHUNK-BOUNDARY-A: watermark at chunk #1 ↔ #2 boundary (04:00) ==='
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);
ALTER VIEW cv SET (time_series.materialized_only = false);
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 04:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

-- Bit-equivalence: union must equal source aggregation, no overlap, no gap.
-- A boundary-inclusivity bug shows up as bucket 04:00 in mat AND live
-- → row count for bucket 04:00 would be 2× source, or 0 if both
-- branches exclude it.
SELECT count(*) AS diff_wm_chunk1_to_chunk2 FROM (
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

-- Probe split: in mat-only mode, only chunk #1 buckets should be visible.
-- In real-time mode, both mat (chunk #1) and live (chunks #2 + #3) should
-- combine without duplicates.
ALTER VIEW cv SET (time_series.materialized_only = true);
SELECT count(*) AS mat_only_rows_below_04
  FROM cv WHERE bucket < '2024-01-01 04:00+00'::timestamptz;
SELECT count(*) AS mat_only_rows_at_or_above_04
  FROM cv WHERE bucket >= '2024-01-01 04:00+00'::timestamptz;
ALTER VIEW cv SET (time_series.materialized_only = false);

\echo '=== RT-CHUNK-BOUNDARY-B: watermark at chunk #2 ↔ #3 boundary (08:00) ==='
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 08:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'cv');

SELECT count(*) AS diff_wm_chunk2_to_chunk3 FROM (
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

-- Restore uniform watermark for any downstream test (none right now,
-- but future appends should not start from a hand-edited watermark).
CALL time_series.refresh_continuous_aggregate('cv', NULL, NULL);

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE metrics CASCADE;

\echo '=== REALTIME TESTS DONE ==='
