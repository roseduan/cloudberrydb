-- ============================================================
-- cagg_watermark_concurrent_update.sql (isolation2)
-- Reproduces the reference cagg concurrency isolation test.spec
--
-- Tests that watermark updates from REFRESH are visible to other
-- sessions:
--   1. After REFRESH commits, other sessions see the new watermark
--   2. Real-time UNION ALL view uses the updated watermark
--   3. Multiple REFRESH cycles advance watermark monotonically
--   7. Backend-local watermark cache (constify) stays coherent:
--      same-session, cross-session (refresh- and manual-DML-driven
--      sinval), and REPEATABLE READ snapshot consistency
-- ============================================================

1: SET optimizer = off;
1: SET timezone = 'UTC';
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: SET search_path TO public, time_series;

1: CREATE TABLE temperature (time TIMESTAMPTZ NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
   DISTRIBUTED BY (time);

-- Initial data: year 2000
1: INSERT INTO temperature
   SELECT '2000-01-01'::timestamptz + (i * interval '1 min'), (i % 100)::float8
   FROM generate_series(1, 1440) i;

-- CAGG with real-time mode (materialized_only = false)
1: CREATE MATERIALIZED VIEW cagg WITH (time_series.continuous) AS
   SELECT time_bucket('4 hour'::interval, time) AS bucket, avg(val) AS avg_val
   FROM temperature GROUP BY 1;

1: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

-- Add new data in 2020
1: INSERT INTO temperature
   SELECT '2020-01-01'::timestamptz + (i * interval '1 min'), (i % 100)::float8
   FROM generate_series(1, 1440) i;

2: SET optimizer = off;
2: SET search_path TO public, time_series;

-- ============================================================
-- SANITY: Verify the source hypertable is multi-chunk and the
-- year-2000 INSERT was rejected by the chunk-origin constraint.
--
-- ts_chunk_origin = '2020-01-01' means timestamps before 2020 are
-- rejected at INSERT time with HINT "timestamp is before the
-- chunk origin".  So the 1440 rows of year-2000 data above do NOT
-- land; only the 1440 rows of year-2020 data do.  At
-- ts_chunk_interval = 4 h, 1440 minutes = 24 hours = 6 chunks
-- (the last row at 24:00 may overflow into chunk 7).
--
-- Without these assertions a future regression that silently
-- rejects all year-2020 data too would still pass every
-- "wm_after >= 2020" check below (a NULL aggregate returns NULL,
-- and bool_and(NULL) = NULL, often filtered as "not false").
-- ============================================================
1: SELECT count(DISTINCT chunk_number) >= 6 AS source_spans_at_least_6_chunks
   FROM time_series.ts_chunk WHERE table_oid = 'temperature'::regclass;
1: SELECT count(*) = 1440 AS exactly_1440_rows_landed FROM temperature;

-- ============================================================
-- Test 1: Session 2 reads watermark BEFORE and AFTER session 1
--         REFRESHes. The watermark should advance.
--
-- Corresponds to upstream permutation:
--   "s1_prepare" "s2_prepare" "s3_lock_invalidation"
--   "s2_select" "s1_run_update" "s2_select"
--   "s3_release_invalidation" "s2_select" "s1_select"
-- ============================================================

-- Session 2: read watermark before REFRESH
2: SELECT bool_and(watermark < '2020-01-01'::timestamptz) AS wm_before
   FROM time_series.cagg_watermark;

-- Session 2: read from CAGG (real-time mode) — should see 2020 data
-- via direct_view branch even though mat table hasn't been refreshed
-- for 2020 range yet
2: SELECT count(*) AS rows_2020_before FROM cagg WHERE bucket >= '2020-01-01';

-- Session 1: REFRESH to materialize 2020 data
1: CALL time_series.refresh_continuous_aggregate('cagg', '2020-01-01', '2025-01-01');

-- Session 2: watermark should now have advanced
2: SELECT bool_and(watermark >= '2020-01-01'::timestamptz) AS wm_after
   FROM time_series.cagg_watermark;

-- Session 2: CAGG should still return correct data
2: SELECT count(*) AS rows_2020_after FROM cagg WHERE bucket >= '2020-01-01';

-- ============================================================
-- Test 2: Multiple REFRESH cycles — watermark advances
--         monotonically (never decreases).
-- ============================================================

-- Insert more data in 2021
1: INSERT INTO temperature VALUES ('2020-01-02 23:59:59+00', 42.0);

-- Save watermark before REFRESH, then verify it doesn't decrease after
1: CREATE TEMP TABLE wm_snap AS SELECT max(watermark) AS wm FROM time_series.cagg_watermark;

-- REFRESH
1: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

-- Watermark should be >= saved snapshot (monotonically increasing)
1: SELECT bool_and(w.watermark >= s.wm) AS wm_monotonic
   FROM time_series.cagg_watermark w, wm_snap s;

1: DROP TABLE wm_snap;

-- ============================================================
-- Test 3: Session 1 holds advisory lock (simulating slow REFRESH).
--         Session 2 reads CAGG view — should see consistent watermark
--         in both UNION ALL branches (no gap).
-- ============================================================

-- Session 1: hold advisory lock
1: BEGIN;
1: SELECT pg_advisory_xact_lock(
     (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cagg')
   );

-- Session 2: query CAGG — real-time view should work normally
-- (uses the COMMITTED watermark, not the in-progress one)
2: SELECT count(*) AS rows_during_lock FROM cagg;

-- Session 2: EXCEPT should be 0 even while lock is held
2: SELECT count(*) AS diff_during_lock FROM (
   (SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    EXCEPT
    SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1
    EXCEPT
    SELECT bucket, round(avg_val::numeric, 6) FROM cagg)
   ) x;

-- Release
1: COMMIT;

-- Final EXCEPT = 0
1: SELECT count(*) AS diff_final FROM (
   (SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    EXCEPT
    SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1
    EXCEPT
    SELECT bucket, round(avg_val::numeric, 6) FROM cagg)
   ) x;

-- ============================================================
-- Test 4: Fault injection — pause before watermark advance
--         Verify session 2 sees OLD watermark during pause,
--         then NEW watermark after resume.
--         (upstream watermark perm 1: intermediate-state verification)
-- ============================================================

1: INSERT INTO temperature
   SELECT '2024-01-01'::timestamptz + (i * interval '1 min'), i::float8
   FROM generate_series(1, 100) i;

-- Pause REFRESH before watermark update
1: SELECT gp_inject_fault('cagg_refresh_before_watermark_advance', 'suspend', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

1>: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

2: SELECT gp_wait_until_triggered_fault('cagg_refresh_before_watermark_advance', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- REFRESH paused: mat data written but watermark NOT advanced.
-- Session 2 should still see OLD watermark (pre-2024 data range).
2: SELECT bool_and(watermark < '2024-01-01'::timestamptz) AS wm_still_old
   FROM time_series.cagg_watermark;

-- Resume
2: SELECT gp_inject_fault('cagg_refresh_before_watermark_advance', 'resume', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1<:

2: SELECT gp_inject_fault('cagg_refresh_before_watermark_advance', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Now watermark should have advanced past 2024
2: SELECT bool_and(watermark >= '2024-01-01'::timestamptz) AS wm_now_new
   FROM time_series.cagg_watermark;

-- Final correctness
1: SELECT count(*) AS diff_wm_fault FROM (
   (SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    EXCEPT
    SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1
    EXCEPT
    SELECT bucket, round(avg_val::numeric, 6) FROM cagg)
   ) x;

-- ============================================================
-- Test 5: Two REFRESH cycles with INSERT between them
--         Watermark advances each cycle, reader sees update
--         (upstream watermark perm 2: two successive cycles)
-- ============================================================

-- Cycle 1: REFRESH materializes 2024 data
1: INSERT INTO temperature
   SELECT '2025-01-01'::timestamptz + (i * interval '1 min'), i::float8
   FROM generate_series(1, 100) i;

1: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

-- Record watermark after cycle 1
1: CREATE TEMP TABLE wm_c1 AS SELECT max(watermark) AS wm FROM time_series.cagg_watermark;

-- Session 2 reads — should see all data including 2025
2: SELECT count(*) AS rows_after_c1 FROM cagg WHERE bucket >= '2025-01-01';

-- Cycle 2: INSERT more data, REFRESH again
1: INSERT INTO temperature VALUES ('2025-06-01 12:00:00+00', 999.0);
1: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

-- Watermark should have advanced further
1: SELECT bool_and(w.watermark >= c.wm) AS wm_monotonic_c2
   FROM time_series.cagg_watermark w, wm_c1 c;

-- Session 2 sees the new data
2: SELECT count(*) AS rows_after_c2 FROM cagg WHERE bucket >= '2025-06-01';

1: DROP TABLE wm_c1;

-- Final correctness
1: SELECT count(*) AS diff_cycles FROM (
   (SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    EXCEPT
    SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature GROUP BY 1
    EXCEPT
    SELECT bucket, round(avg_val::numeric, 6) FROM cagg)
   ) x;

-- ============================================================
-- Test 6: Multi-chunk INSERT → watermark advances precisely
--
-- Tests 1-5 above check `bool_and(watermark >= X)` — a range
-- assertion that passes as long as the watermark advanced at
-- all.  It would silently accept a watermark that only advanced
-- ONE bucket when it should have advanced FIVE.  Test 6 pins the
-- exact final watermark to the bucket-boundary derived from the
-- newly-inserted MAX timestamp, catching "stuck" watermark
-- regressions where REFRESH advances by a single bucket per
-- chunk instead of by the full span.
--
-- Also asserts the watermark falls on a 4 h bucket boundary
-- (hypertable invariant: watermark = bucket_end for some bucket).
-- ============================================================

-- Insert 5 rows scattered across 5 distinct chunks.  Pick dates
-- that are *after* Test 5's last watermark (~2025-06-01) so REFRESH
-- has work to do, but also within ts_scan's "no upper bound"
-- max_chunk cap (= now + 1 interval + 10 chunks ≈ now + 44 h).
-- 2025-07-15/16 satisfies both: post-2025-06-01 (advances watermark)
-- and pre-now (within scan range).
1: INSERT INTO temperature VALUES
     ('2025-07-15 02:30:00+00', 700.0),
     ('2025-07-15 10:30:00+00', 701.0),
     ('2025-07-15 22:30:00+00', 702.0),
     ('2025-07-16 06:30:00+00', 703.0),
     ('2025-07-16 18:30:00+00', 704.0);

-- Self-check: the 5 timestamps fall in 5 distinct 4 h chunks.
1: WITH new_times(ts) AS (VALUES
       ('2025-07-15 02:30:00+00'::timestamptz),
       ('2025-07-15 10:30:00+00'::timestamptz),
       ('2025-07-15 22:30:00+00'::timestamptz),
       ('2025-07-16 06:30:00+00'::timestamptz),
       ('2025-07-16 18:30:00+00'::timestamptz))
   SELECT count(DISTINCT floor(extract(epoch from (ts - '2020-01-01'::timestamptz))/14400)::int)
            AS new_chunks_touched
   FROM new_times;

-- Capture expected watermark = bucket_start of MAX(new time)
-- where buckets are 4 h aligned on 2020-01-01.  The max new time
-- is 2025-07-16 18:30, whose bucket starts at 2025-07-16 16:00.
1: CREATE TEMP TABLE wm_t6_pre AS
   SELECT max(watermark) AS wm FROM time_series.cagg_watermark;

-- REFRESH advances watermark
1: CALL time_series.refresh_continuous_aggregate('cagg', NULL, NULL);

-- 1) Watermark equals the bucket-START containing MAX(time) — not
--    the bucket-END.  This is the documented time_series real-time
--    aggregation invariant: buckets *strictly less than* watermark
--    are fully materialized; the bucket [watermark, watermark+chunk)
--    is the "live boundary" — its rows are always re-aggregated
--    from source on read, so the mat table's copy of that bucket
--    can lag without breaking SELECT correctness.
--
--    Concretely: MAX = 2025-07-16 18:30 falls in bucket [16:00, 20:00)
--    so watermark = 16:00.  Pinning the exact equality (not just
--    range) catches a regression where REFRESH accidentally
--    advances watermark INTO or PAST the current bucket — which
--    would silently exclude the latest live-bucket rows from the
--    real-time view.
1: SELECT
     bool_and(w.watermark = time_bucket('4 hour'::interval, t.max_t))
       AS wm_at_bucket_start_of_max
   FROM time_series.cagg_watermark w,
        (SELECT max(time) AS max_t FROM temperature) t;

-- 2) Watermark must sit on a 4 h chunk-aligned boundary (origin
--    2020-01-01).  Hypertable invariant: watermark never falls
--    inside a chunk.
1: SELECT
     bool_and(
       extract(epoch from (w.watermark - '2020-01-01'::timestamptz))::bigint % 14400 = 0
     ) AS wm_chunk_aligned
   FROM time_series.cagg_watermark w;

-- 3) Watermark advanced by the FULL span of the new data, not
--    just one bucket.  pre-refresh wm was around 2025-06-01;
--    post-refresh must be at 2025-07-16.  Difference > 30 days.
1: SELECT
     bool_and(w.watermark - p.wm > interval '30 days') AS wm_advanced_by_full_span
   FROM time_series.cagg_watermark w, wm_t6_pre p;

-- 4) Mat data correctness across the 5 new chunks
1: SELECT count(*) AS diff_t6 FROM (
   (SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    WHERE bucket >= '2025-07-01'
    EXCEPT
    SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature WHERE time >= '2025-07-01' GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('4 hour'::interval, time), round(avg(val)::numeric, 6)
    FROM temperature WHERE time >= '2025-07-01' GROUP BY 1
    EXCEPT
    SELECT bucket, round(avg_val::numeric, 6) FROM cagg
    WHERE bucket >= '2025-07-01')
   ) x;

-- 5) Each of the 5 newly-touched chunks must have a mat row.
1: SELECT
     count(*) FILTER (WHERE bucket = '2025-07-15 00:00+00') AS mc_a,
     count(*) FILTER (WHERE bucket = '2025-07-15 08:00+00') AS mc_b,
     count(*) FILTER (WHERE bucket = '2025-07-15 20:00+00') AS mc_c,
     count(*) FILTER (WHERE bucket = '2025-07-16 04:00+00') AS mc_d,
     count(*) FILTER (WHERE bucket = '2025-07-16 16:00+00') AS mc_e
   FROM cagg;

1: DROP TABLE wm_t6_pre;

-- ============================================================
-- Test 7: Backend-local watermark cache coherence
--
-- constify (watermark_constify.c) caches (cagg_id -> global MIN
-- watermark) per backend to avoid a cross-segment SPI dispatch on
-- every planning.  Coherence is plain PG sinval: cagg_refresh (and
-- the planner-hook DML detector, for manual watermark UPDATEs)
-- broadcast a relcache invalidation on the cagg_watermark table;
-- the cache's relcache callback clears it.
--
-- The EXPLAIN output embeds the watermark as a timestamptz Const
-- in both UNION ALL branch filters — that Const IS the observable.
--
--   7a. Same-session: EXPLAIN shows the new watermark right after
--       a refresh in the same backend.
--   7b. Cross-session (refresh): a backend that cached the old
--       watermark sees the NEW value after another session's
--       refresh (stale Const here = missing invalidation).
--   7c. REPEATABLE READ: a transaction whose snapshot predates a
--       concurrent refresh keeps seeing the OLD watermark (cache
--       bypassed; SPI fallback reads under the transaction
--       snapshot).  After COMMIT it sees the new value.
--   7d. Cross-session (manual DML): an UPDATE of cagg_watermark
--       reaches no explicit inval call site; the planner hook
--       detects it and broadcasts from the writer's transaction.
-- ============================================================

-- Plan-shape pins for the EXPLAIN assertions
1: SET timezone = 'UTC';
1: SET enable_bitmapscan = off;
1: SET enable_indexscan = off;
2: SET timezone = 'UTC';
2: SET enable_bitmapscan = off;
2: SET enable_indexscan = off;

1: CREATE TABLE wmc_src (
    time TIMESTAMPTZ NOT NULL,
    dev  INT NOT NULL,
    val  DOUBLE PRECISION NOT NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2024-01-01 00:00+00'
)
DISTRIBUTED BY (dev);

1: INSERT INTO wmc_src VALUES
   ('2024-01-01 10:30+00', 1, 10.0),
   ('2024-01-01 11:30+00', 2, 11.0);

1: CREATE MATERIALIZED VIEW wmc_cv WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          count(*) AS cnt
   FROM wmc_src GROUP BY bucket;

-- Initial refresh: watermark -> 11:00
1: CALL time_series.refresh_continuous_aggregate('wmc_cv', NULL, NULL);

-- ---- 7a. Same-session coherence ----

-- First planning: cache miss -> SPI -> Const 11:00 (fills the cache)
1: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- Second planning: served from the backend cache -> identical Const
1: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- Advance the watermark in this same backend
1: INSERT INTO wmc_src VALUES ('2024-01-01 14:30+00', 3, 14.0);
1: CALL time_series.refresh_continuous_aggregate('wmc_cv', NULL, NULL);

-- The refresh's invalidation cleared this backend's cache: Const 14:00
1: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- ---- 7b. Cross-session propagation (refresh-driven) ----

-- Session 2 fills its own cache with 14:00
2: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- Session 1 advances the watermark to 18:00
1: INSERT INTO wmc_src VALUES ('2024-01-01 18:30+00', 4, 18.0);
1: CALL time_series.refresh_continuous_aggregate('wmc_cv', NULL, NULL);

-- Session 2 must see 18:00 (sinval propagated; stale cache would
-- still show 14:00 here)
2: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- ---- 7c. REPEATABLE READ snapshot consistency ----

2: BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
-- Pin the snapshot (sees 4 source rows, watermark 18:00)
2: SELECT count(*) AS pinned_rows FROM wmc_src;

-- Concurrent refresh advances the watermark to 22:00
1: INSERT INTO wmc_src VALUES ('2024-01-01 22:30+00', 5, 22.0);
1: CALL time_series.refresh_continuous_aggregate('wmc_cv', NULL, NULL);

-- RR planning must show the snapshot-consistent OLD value (18:00),
-- NOT the latest committed 22:00
2: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- And the query result matches the snapshot: buckets 10,11,14 from
-- mat, bucket 18 live; the 22:30 row is invisible to this snapshot
2: SELECT bucket, cnt FROM wmc_cv ORDER BY bucket;
2: COMMIT;

-- Back in READ COMMITTED: latest value (22:00)
2: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

-- ---- 7d. Manual watermark DML (planner-hook inval, cross-session) ----

-- The cagg_watermark table can be written by hand (tests and
-- operators do it).  Such DML reaches no explicit inval call site;
-- the planner hook detects "DML targets cagg_watermark" and
-- broadcasts the invalidation from the writer's own transaction.
-- Session 2's cache currently holds 22:00 — after session 1's
-- manual UPDATE it must observe 14:00, not the stale cached value.
1: UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 14:00:00+00'::timestamptz
   WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                    WHERE user_view_name = 'wmc_cv');

2: EXPLAIN (COSTS OFF) SELECT * FROM wmc_cv;

1: DROP VIEW wmc_cv CASCADE;
1: DROP TABLE wmc_src CASCADE;

-- Cleanup
1: DROP TABLE temperature CASCADE;
1: DROP EXTENSION time_series CASCADE;
1q:
2q:
