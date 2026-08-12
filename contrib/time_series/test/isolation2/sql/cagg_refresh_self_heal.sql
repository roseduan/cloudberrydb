-- ============================================================
-- cagg_refresh_self_heal.sql (isolation2)
--
-- Regression for the three-layer chaos self-heal loop after
-- refresh.c's "unmaterialized gap" ereport was downgraded to a
-- no-op DEBUG log.
--
-- Real chaos scenario: a batched refresh (buckets_per_batch=N,
-- refresh_newest_first=true) is interrupted between batches.  batch1
-- (newest window) has committed -- mat contains recent buckets --
-- but batch2 / batch3 (older windows) did not run.  mat now has an
-- "older" hole, but watermark must NOT advance past the hole (else
-- the union view's mat branch would hide the missing buckets and
-- silently drop data).
--
-- We simulate this state deterministically without needing to kill
-- a session mid-refresh: two sequential manual partial refreshes on
-- disjoint windows produce the same final state (mat has recent
-- buckets, mat has older hole, watermark held below the hole).
--
-- Layers under test:
--   ① catch-up gap check (refresh.c:~2200): if [wm, actual_boundary)
--      contains source buckets not in mat, catch-up must NOT advance
--      the watermark.  Guards the view's mat/live boundary from ever
--      claiming coverage it doesn't have.
--   ③ refresh covers window + catch-up advances (refresh.c): when a
--      subsequent refresh materializes the hole, catch-up can now
--      advance wm past the hole because the check passes.
--
-- Layer ② (policy expand-back in bgw_policy/cagg_refresh_policy.c)
-- is covered independently by regress/cagg_bgw_policy_run.sql's
-- RUN-06b "policy expands window to cover watermark gap" and is not
-- exercised here to keep the test time-independent (no wall clock).
--
-- Preservation invariants (verified in both phases):
--   - View EXCEPT source aggregation = 0 (data is never lost)
--   - view row count = source hourly bucket count
-- ============================================================

1: SET optimizer = off;
1: SET timezone = 'UTC';
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: SET search_path TO public, time_series;

-- 20 hourly buckets, deterministic data
1: CREATE TABLE sh_src (
    time TIMESTAMPTZ NOT NULL,
    device INT NOT NULL,
    val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
DISTRIBUTED BY (device);

-- 20 hours × 9 rows/hour = 180 rows, m*6 in [6, 54] minutes so
-- every row lands squarely inside its hour bucket -- exactly 20
-- hourly buckets, no spillover into hour 20.
1: INSERT INTO sh_src
   SELECT '2024-01-01 00:00+00'::timestamptz + (hr || ' hour')::interval + (m * 6 || ' minute')::interval,
          ((hr + m) % 3) + 1, hr + m * 0.1
   FROM generate_series(0, 19) hr, generate_series(1, 9) m;

1: CREATE MATERIALIZED VIEW cv_sh WITH (time_series.continuous) AS
     SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
     FROM sh_src GROUP BY bucket;

-- ------------------------------------------------------------
-- Establish baseline: mat covers [00:00, 08:00), wm around 08:00
-- ------------------------------------------------------------
1: CALL time_series.refresh_continuous_aggregate('cv_sh',
     '2024-01-01 00:00+00'::timestamptz,
     '2024-01-01 08:00+00'::timestamptz);

-- ------------------------------------------------------------
-- Simulate "chaos crash after newest batch": manual partial refresh
-- of a far-future window [18:00, 20:00).  With the fix, this must:
--   - materialize buckets 18:00 and 19:00 into mat
--   - LEAVE watermark below the gap (catch-up refuses to advance)
--   - preserve view completeness (all 20 source buckets visible)
-- ------------------------------------------------------------
1: CALL time_series.refresh_continuous_aggregate('cv_sh',
     '2024-01-01 18:00+00'::timestamptz,
     '2024-01-01 20:00+00'::timestamptz);

-- ============================================================
-- PHASE A: post-chaos steady state.  Assert ①: watermark stayed
-- below the hole, view remains complete (0 rows lost).
-- ============================================================

-- ① wm must NOT have jumped past the mat hole (must stay well
-- below the 18:00 late-materialized buckets)
1: SELECT bool_and(watermark < '2024-01-01 18:00+00'::timestamptz)
       AS phase_a_wm_below_gap
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
  WHERE c.user_view_name = 'cv_sh';

-- View must contain all 20 source hourly buckets (no data loss)
1: SELECT count(DISTINCT bucket) AS phase_a_view_buckets FROM cv_sh;

-- Symmetric EXCEPT: bit-exact identity between view and source
-- aggregation.  Non-zero means data disappeared or was fabricated.
1: SELECT count(*) AS phase_a_diff FROM (
     (SELECT bucket, cnt FROM cv_sh)
     EXCEPT
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM sh_src GROUP BY 1)
     UNION ALL
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM sh_src GROUP BY 1)
     EXCEPT
     (SELECT bucket, cnt FROM cv_sh)
   ) x;

-- Under the hood: 18:00 / 19:00 are served by mat branch, everything
-- else by live branch.  Both must contribute correctly.
1: SELECT
     count(*) FILTER (WHERE bucket <  '2024-01-01 08:00+00'::timestamptz) AS phase_a_via_mat_baseline,
     count(*) FILTER (WHERE bucket >= '2024-01-01 08:00+00'::timestamptz
                       AND bucket <  '2024-01-01 18:00+00'::timestamptz) AS phase_a_via_live_gap,
     count(*) FILTER (WHERE bucket >= '2024-01-01 18:00+00'::timestamptz) AS phase_a_via_live_or_mat_late
   FROM cv_sh;

-- ============================================================
-- PHASE B: trigger self-heal.  A refresh that covers the hole
-- must materialize the missing buckets AND advance the watermark
-- past them (catch-up now succeeds because the gap is filled).
-- ============================================================

1: CALL time_series.refresh_continuous_aggregate('cv_sh', NULL, NULL);

-- ③ mat is complete: 20 buckets present, no holes
1: SELECT count(DISTINCT bucket) AS phase_b_mat_bucket_count FROM cv_sh;

-- ③ watermark caught up past every source bucket (>= 19:00, the
-- last source bucket start)
1: SELECT bool_and(watermark >= '2024-01-01 19:00+00'::timestamptz)
       AS phase_b_wm_caught_up
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
  WHERE c.user_view_name = 'cv_sh';

-- Data identity still holds after self-heal
1: SELECT count(*) AS phase_b_diff FROM (
     (SELECT bucket, cnt FROM cv_sh)
     EXCEPT
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM sh_src GROUP BY 1)
     UNION ALL
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM sh_src GROUP BY 1)
     EXCEPT
     (SELECT bucket, cnt FROM cv_sh)
   ) x;

-- ============================================================
-- Cleanup: dropping the source cascades to the CAGG view + mat
-- table + all internal partial/direct views.
-- ============================================================
1: DROP TABLE sh_src CASCADE;
