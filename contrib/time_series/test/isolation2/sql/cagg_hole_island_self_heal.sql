-- ============================================================
-- cagg_hole_island_self_heal.sql (isolation2)
--
-- Hole taxonomy classes 1b + 2, end-to-end through the REAL policy
-- path (see doc/bugs/cagg-refresh-stuck-across-gap.md §9.10):
--
--   1b. ISLAND FORMATION: a batch's materialization (TX2) is
--       chain-committed by the NEXT batch's SPI_commit_and_chain, so
--       an abort landing DURING batch 2 -- after that chain commit --
--       leaves batch 1 durable: a mat island with a hole below it.
--       Deterministic via the pre-existing
--       cagg_refresh_after_commit_and_chain injector, occurrence-
--       gated to the SECOND refresh call of the execution: batch 1
--       passes (occurrence 1), batch 2's chain first commits batch
--       1's work, then the fault errors out batch 2's empty TX2.
--
--   2.  WINDOW SLIDES PAST THE HOLE: instead of waiting an hour of
--       wall clock, the policy's start_offset is shrunk in place
--       (6h -> 3h), which moves the next window's inscribed start
--       above the hole -- the exact geometry of an abandoned region.
--       The next run must self-heal via expand-back: the hole's
--       ledger residues pull window_start back down, the hole is
--       materialized, and the watermark lifts off -infinity.
--
-- Together with cagg_orphan_backfill_below_window (class 3,
-- arrival-side) and cagg_hole_batch_boundary (boundary abort = zero
-- leakage), this completes deterministic coverage of every
-- refresh-reachable hole mechanism.
--
-- Invariant in every phase: the union view never loses rows -- the
-- watermark stays at -infinity until the window is whole, so all
-- phases serve from the live branch (slow but correct).
-- ============================================================

1: SET optimizer = off;
1: SET timezone = 'UTC';
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: DROP TABLE IF EXISTS hi_src CASCADE;
1: DROP TABLE IF EXISTS hi_anchor CASCADE;
1: CREATE EXTENSION time_series;
1: CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
1: SET search_path TO public, time_series;

-- Keep run_job fully deterministic: no scheduler ticks racing us.
1: SELECT time_series.stop_background_workers();

-- One anchor captured once; margins are >= 1 hour everywhere.
1: CREATE TABLE hi_anchor AS SELECT date_trunc('hour', now()) AS a DISTRIBUTED REPLICATED;

1: CREATE TABLE hi_src (
    time TIMESTAMPTZ NOT NULL,
    device INT NOT NULL,
    val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
DISTRIBUTED BY (device);

-- CAGG under test FIRST (cagg_id 1 in a fresh extension -> mat table
-- is time_series._mat_cv_hi_1, queried directly to observe the
-- island; the union view cannot show it while wm = -infinity).
1: CREATE MATERIALIZED VIEW cv_hi WITH (time_series.continuous) AS
     SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
     FROM hi_src GROUP BY bucket;

-- Helper CAGG advances the SHARED invalidation threshold so the bulk
-- insert below is L1-logged (batch-split precondition, and what makes
-- the hole's ledger residues exist for the class-2 heal).
1: CREATE MATERIALIZED VIEW cv_hi_helper WITH (time_series.continuous) AS
     SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
     FROM hi_src GROUP BY bucket;

-- Seed newest bucket (a-2h), refresh helper -> threshold ~a-2h;
-- cv_hi stays at watermark = -infinity.
1: INSERT INTO hi_src
   SELECT a - interval '2 hours' + (m * 6 || ' minute')::interval,
          (m % 3) + 1, m * 1.0
   FROM hi_anchor, generate_series(1, 9) m;

1: CALL time_series.refresh_continuous_aggregate('cv_hi_helper', NULL, NULL);

1: SELECT bool_and(watermark = '-infinity'::timestamptz) AS cold_start_wm_is_neg_inf
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
  WHERE c.user_view_name = 'cv_hi';

-- Bulk data for buckets a-5h, a-4h, a-3h: threshold-watched -> L1.
1: INSERT INTO hi_src
   SELECT a - (hr || ' hour')::interval + (m * 6 || ' minute')::interval,
          ((hr + m) % 3) + 1, hr + m * 0.1
   FROM hi_anchor, generate_series(3, 5) hr, generate_series(1, 9) m;

-- Policy: window ~[a-5h, a-1h) = 4 buckets, 2 per batch => exactly 2
-- batches, newest-first.  Only policy in a fresh extension -> 1001.
1: SELECT add_continuous_aggregate_policy('cv_hi',
       start_offset              => INTERVAL '6 hours',
       end_offset                => INTERVAL '1 hour',
       schedule_interval         => INTERVAL '4 hours',
       buckets_per_batch         => 2,
       max_batches_per_execution => 10,
       refresh_newest_first      => true) AS jid;

-- ------------------------------------------------------------
-- Arm the occurrence-gated fault: fire only on the SECOND pass
-- through cagg_refresh_after_commit_and_chain within this policy
-- execution.  Occurrence 1 = batch 1's chain (no fire); batch 2's
-- chain COMMITS batch 1's materialization, then occurrence 2 fires
-- and aborts batch 2's just-opened TX2.
-- ------------------------------------------------------------
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain',
       'error', '', '', '', 2, 2, 0, dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

1: CALL time_series.run_job(1001);

-- ============================================================
-- PHASE A: the island.  Batch 1 (newest, [a-3h, a-1h)) survived the
-- abort because batch 2's chain committed it; the hot bucket (a-2h)
-- is excluded by actual_boundary, so the island is exactly bucket
-- a-3h.  The hole (a-5h, a-4h) sits below it; the watermark holds.
-- ============================================================

1: SELECT last_run_success = false AS phase_a_job_failed
   FROM time_series.bgw_job_stat WHERE job_id = 1001;

-- The island: exactly one bucket, and it is the newest stable one.
1: SELECT count(DISTINCT bucket) AS phase_a_mat_buckets
   FROM time_series._mat_cv_hi_1;

1: SELECT bool_and(bucket = x.a - interval '3 hours') AS phase_a_island_is_newest
   FROM time_series._mat_cv_hi_1 m, hi_anchor x;

-- Watermark must NOT have advanced past the hole below the island.
1: SELECT bool_and(watermark = '-infinity'::timestamptz) AS phase_a_wm_held
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
  WHERE c.user_view_name = 'cv_hi';

-- View correctness invariant: all 4 buckets visible, bit-exact.
1: SELECT count(DISTINCT bucket) AS phase_a_view_buckets FROM cv_hi;
1: SELECT count(*) AS phase_a_diff FROM (
     (SELECT bucket, cnt FROM cv_hi)
     EXCEPT
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM hi_src GROUP BY 1)
     UNION ALL
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM hi_src GROUP BY 1)
     EXCEPT
     (SELECT bucket, cnt FROM cv_hi)
   ) x;

-- ============================================================
-- PHASE B: slide the window past the hole (class 2 geometry) and
-- self-heal.  Shrinking start_offset 6h -> 3h moves the next
-- window's inscribed start to ~a-2h, ABOVE the hole -- without the
-- ledger-driven expand-back the hole would now be abandoned forever.
-- The surviving L2 residues pull window_start back to the hole's
-- bucket floor; the hole materializes; the watermark lifts.
-- ============================================================

1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain',
       'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

1: UPDATE time_series.bgw_job
   SET config = jsonb_set(config, '{start_offset}', '"03:00:00"')
   WHERE id = 1001;

1: CALL time_series.run_job(1001);

1: SELECT last_run_success = true AS phase_b_job_succeeded
   FROM time_series.bgw_job_stat WHERE job_id = 1001;

-- Hole filled: the three stable buckets are in mat (hot bucket a-2h
-- excluded by design, served live).
1: SELECT count(DISTINCT bucket) AS phase_b_mat_buckets
   FROM time_series._mat_cv_hi_1;

1: SELECT bool_and(watermark > '-infinity'::timestamptz) AS phase_b_wm_is_finite
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
  WHERE c.user_view_name = 'cv_hi';

1: SELECT bool_and(w.watermark >= x.a - interval '3 hours') AS phase_b_wm_past_hole
   FROM time_series.cagg_watermark w
   JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
   CROSS JOIN hi_anchor x
  WHERE c.user_view_name = 'cv_hi';

-- View correctness invariant still holds.
1: SELECT count(*) AS phase_b_diff FROM (
     (SELECT bucket, cnt FROM cv_hi)
     EXCEPT
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM hi_src GROUP BY 1)
     UNION ALL
     (SELECT time_bucket('1 hour'::interval, time), count(*)
        FROM hi_src GROUP BY 1)
     EXCEPT
     (SELECT bucket, cnt FROM cv_hi)
   ) x;

-- ============================================================
-- Cleanup
-- ============================================================
1: DROP TABLE hi_src CASCADE;
1: DROP TABLE hi_anchor;
