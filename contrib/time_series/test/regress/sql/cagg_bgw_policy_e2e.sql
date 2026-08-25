-- ============================================================
-- cagg_bgw_policy_e2e.sql
--
-- Scope: SINGLE DATABASE end-to-end user journey for one CAGG
--        under the real BGW scheduler (no mock-time, no synchronous
--        run_job).  Walks a typical user's full lifecycle:
--
--          install extension
--             ↓
--          create source table + INSERT historical data
--             ↓
--          create CAGG (CREATE MATERIALIZED VIEW)
--             ↓
--          attach refresh policy (add_continuous_aggregate_policy)
--             ↓
--          E2E-01  first auto-refresh completes
--             ↓
--          E2E-HIST-OK  refresh audited in bgw_job_stat_history
--             ↓
--          E2E-02  INSERT new data → auto-pickup within schedule_interval
--                  (data verified bit-equivalent to source)
--             ↓
--          E2E-03  alter_job — change schedule_interval, pause, resume
--             ↓
--          E2E-04  remove_continuous_aggregate_policy
--                  (bgw_job + bgw_job_stat cleaned up)
--             ↓
--          DROP MV / DROP TABLE / DROP EXTENSION
--
-- For multi-DB topology / isolation (catalog & per-DB GUC), see
-- cagg_bgw_policy_multidb_e2e.sql — explicitly NOT duplicated here.
--
-- Error / recovery semantics live elsewhere (NOT duplicated here):
--   * run_job in an explicit txn → bgw_job.sql RUN-04
--   * failure recorded in bgw_job_stat → bgw_scheduler.sql DB-SCHED-06
--   * consecutive_failures reset after recovery →
--     cagg_bgw_policy_scheduler_run.sql BGW-FAILURE-RECOVERY
--   * DROP MV cascades bgw_job → cagg_bgw_policy_api.sql BGW-DDL-12
--   * max_runtime SIGTERM → bgw_scheduler.sql DB-SCHED-11
--   * max_retries auto-unschedule →
--     cagg_bgw_policy_scheduler_run.sql BGW-FULL-MAX-RETRIES
-- All faster + more deterministic under mock time, so exercising
-- them via real BGW here would just add wall-clock cost without
-- extra coverage.
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- ============================================================
-- Test infra: BGW polling helpers
-- ============================================================

-- One-shot reader: current total_runs for a given job_id.  Used by
-- "non-event" assertions (pause / lengthened-interval) where we
-- explicitly wait a fixed window and then check that no NEW ticks
-- happened.
CREATE OR REPLACE FUNCTION poll_runs(p_job_id int) RETURNS int
LANGUAGE sql AS $$
    SELECT total_runs::int FROM time_series.bgw_job_stat WHERE job_id = p_job_id;
$$;

-- Early-exit polling helper, modelled after upstream
-- test.wait_for_job_to_run (test/sql/utils/testsupport.sql).
--
-- Spins up to p_spins × 100 ms = 60 s by default, returning true as
-- soon as total_successes reaches p_target.  Returns false on early
-- failure (total_failures > 0) or timeout.
--
-- Why this works inside plpgsql despite the function being a single
-- "function call":  PG's default isolation is READ COMMITTED, in
-- which each top-level SELECT inside a plpgsql function takes a
-- fresh snapshot.  Worker commits between iterations are therefore
-- visible to subsequent SELECTs.  (REPEATABLE READ would freeze the
-- snapshot for the whole transaction; we don't use that here.)
-- p_spins default bumped from 600 (60 sec at 0.1s per spin) to 3000
-- (300 sec) so the helper survives heavy back-to-back regression
-- runs where BGW dispatch latency is observed to exceed 60 sec.  A
-- real hang still triggers a clean failure within 5 minutes.
CREATE OR REPLACE FUNCTION wait_for_job(p_job_id int,
                                         p_target_successes int,
                                         p_spins int DEFAULT 3000)
RETURNS bool LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
    FOR i IN 1..p_spins LOOP
        SELECT total_successes, total_failures
          FROM time_series.bgw_job_stat
         WHERE job_id = p_job_id INTO r;

        IF r.total_successes >= p_target_successes THEN
            RETURN true;
        END IF;
        IF r.total_failures > 0 THEN
            RAISE INFO 'wait_for_job: job % had a failure before reaching target', p_job_id;
            RETURN false;
        END IF;

        PERFORM pg_sleep(0.1);
    END LOOP;
    RAISE INFO 'wait_for_job: timeout after % spins (job %)', p_spins, p_job_id;
    RETURN false;
END $$;

-- ============================================================
-- Source data + CAGG
-- ============================================================
CREATE TABLE m_e2e (
    time TIMESTAMPTZ NOT NULL,
    tags_id INT NOT NULL,
    v INT
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);

-- 6000 rows across 100 hours × 5 tags (1-minute spacing).  Sized so that:
--   * every (bucket, tag) has ~12 rows (real aggregation, not 1-2)
--   * each segment gets ~750 rows (exercises distribution + motion)
--   * refresh has enough work that mark_end stats are meaningful
--   * full timeline (4.17 days) fits within the 7-day refresh window,
--     so all rows materialize via the BGW path (not via real-time
--     UNION ALL fallback).
INSERT INTO m_e2e
SELECT now() - (i * interval '1 minute'),
       (i % 5) + 1,
       i
FROM generate_series(1, 6000) i;

CREATE MATERIALIZED VIEW cv_e2e
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id, count(*) AS c
  FROM m_e2e GROUP BY bucket, tags_id;

-- ============================================================
-- E2E-01: Real scheduler picks up new policy and refreshes succeed
--
-- schedule_interval = 5 s (not 1 h) so the test can actually observe
-- multiple ticks within its walltime budget.  With 1 h the test only
-- ever sees total_runs = 1, which can't distinguish "policy ran exactly
-- once and stopped" from "schedule_interval honored".
--
-- Polling uses wait_for_job (early-exit) — typical wakeup is ~0.5 s,
-- worst case bounded at 60 s (BGW postmaster backoff + scheduler tick).
-- Compared to a fixed sleep×N pattern this both runs faster on the
-- happy path AND avoids the timing flake where a fixed sleep ends in
-- the middle of an in-flight tick.
-- ============================================================
\echo '=== E2E-01: real BGW scheduler picks up new policy promptly ==='
SELECT add_continuous_aggregate_policy('cv_e2e',
       '7 days'::interval, '0'::interval, '5 seconds'::interval) AS jid_e2e \gset

-- Force the worker to fire on first iteration: set next_start in the past.
UPDATE time_series.bgw_job_stat
   SET next_start = now() - interval '1 minute'
 WHERE job_id = :jid_e2e;

-- ALSO seed a stat row if mark_start hasn't run yet (race with first
-- scheduler tick) -- INSERT IF NOT EXISTS via UPSERT pattern.
-- The UPDATE above is no-op if the row doesn't exist; ensure it exists:
INSERT INTO time_series.bgw_job_stat (job_id, next_start)
SELECT :jid_e2e, now() - interval '1 minute'
WHERE NOT EXISTS (
    SELECT 1 FROM time_series.bgw_job_stat WHERE job_id = :jid_e2e
);

-- Wait for the first successful refresh.  Returns immediately when
-- total_successes >= 1; bounded at 60 s (600 spins × 100 ms).
SELECT wait_for_job(:jid_e2e, 1) AS worker_ran_within_60s;

-- Assert the worker actually wrote a successful run.
--
-- We deliberately do NOT assert last_run_success here.  Under continuous
-- 5 s dispatch, by the time we read bgw_job_stat there may be a NEW tick
-- in flight whose mark_start has run but mark_end hasn't, leaving
-- last_run_success transiently false.  total_successes >= 1 is the
-- monotonic, stable signal; wait_for_job already filters out failures
-- via its early-exit-on-failure branch.
SELECT total_runs      >= 1 AS at_least_one_run,
       total_successes >= 1 AS at_least_one_success
  FROM time_series.bgw_job_stat WHERE job_id = :jid_e2e;

-- Assert the cagg is materialized AND correct, row-by-row.
--
-- Row-count equivalence alone would let "right number of rows, wrong
-- (bucket, tags_id) pairings or wrong c values" slip through.  EXCEPT
-- compares each (bucket, tags_id, c) triple against a fresh aggregation
-- of the source.  diff_count = 0 ⇔ cagg is bit-equivalent to source.
WITH expected AS (
    SELECT time_bucket('1 hour'::interval, time) AS bucket,
           tags_id, count(*) AS c
      FROM m_e2e GROUP BY 1, 2
), actual AS (
    SELECT bucket, tags_id, c FROM cv_e2e
)
SELECT count(*) AS source_vs_cagg_diff
  FROM (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual   EXCEPT SELECT * FROM expected)
  ) d;

-- Also verify aggregation actually happened (not single-row buckets).
SELECT (SELECT max(c) FROM cv_e2e) > 1 AS bucket_aggregates_multiple_rows;

-- Assert schedule_interval = 5 s is honored: wait for a 2nd success.
-- If the scheduler is dispatching at ~5 s cadence, this returns within
-- ~5-6 s of the first success.  If the second tick fails to fire
-- entirely, wait_for_job returns false after 60 s.
SELECT wait_for_job(:jid_e2e, 2) AS schedule_interval_honored;

-- ============================================================
-- E2E-HIST-OK: bgw_job_stat_history records each successful run
--
-- Filter on execution_finish IS NOT NULL — a row with NULL finish is
-- an in-flight tick (mark_start ran, mark_end pending).  Such ghost
-- rows are real-world phenomena under continuous 5 s dispatch and
-- are explicitly handled by the retention policy's OR clause; we
-- don't fail the test on them here.  The substantive assertion is:
-- every COMPLETED row is succeeded=true with no error_data.
-- ============================================================
\echo '=== E2E-HIST-OK: stat_history captures successful CAGG runs ==='
SELECT count(*) >= 1                          AS history_has_finished_rows,
       bool_and(succeeded)                    AS all_finished_succeeded,
       bool_and(data->'error_data' IS NULL)   AS no_error_data_on_success
  FROM time_series.bgw_job_stat_history
 WHERE job_id = :jid_e2e
   AND execution_finish IS NOT NULL;

-- ============================================================
-- E2E-02: BGW auto-refresh picks up new INSERTs
--
-- The whole point of an auto-refresh policy is "I INSERT new data,
-- BGW notices and re-materializes within schedule_interval without
-- me lifting a finger."  E2E-01 only verifies the FIRST refresh
-- after policy registration; this case verifies the steady-state
-- "new data -> automatic refresh" loop, which is the user-facing
-- contract of the whole feature.
--
-- Strategy: insert one batch of new buckets offset into the past so
-- they fall within start_offset=7d but outside the existing range,
-- then poll for cv_e2e to include them without calling run_job.
-- ============================================================
\echo '=== E2E-02: BGW auto-refreshes on incremental INSERT ==='
SELECT count(*)        AS cv_rows_before    FROM cv_e2e \gset
SELECT total_successes AS successes_before
  FROM time_series.bgw_job_stat WHERE job_id = :jid_e2e \gset

-- Insert 300 new rows offset to ~6 days back (oldest still within
-- the 7d refresh window) — 1-minute spacing × 5 tags spans 5 hours
-- and writes 60 fresh invalidation_log entries.  This exercises
-- merged-refresh handling of a non-trivial invalidation batch,
-- not just a single-tick toy increment.
INSERT INTO m_e2e
SELECT now() - interval '6 days' - (i * interval '1 minute'),
       (i % 5) + 1,
       9000 + i
FROM generate_series(1, 300) i;

-- Wait for at least one new successful refresh after the INSERT.
-- Returns within ~5-10 s on the happy path (one tick + refresh).
SELECT wait_for_job(:jid_e2e, :successes_before + 1)
       AS post_insert_refresh_completed;

-- Assert: cv_e2e grew, AND it remains bit-equivalent to the source
-- (ie BGW absorbed exactly the new buckets — no duplicates, no
-- partial materialization, no stale (bucket, tags_id, c) triples).
SELECT (SELECT count(*) FROM cv_e2e) > :cv_rows_before
       AS cagg_grew_after_insert;

WITH expected AS (
    SELECT time_bucket('1 hour'::interval, time) AS bucket,
           tags_id, count(*) AS c
      FROM m_e2e GROUP BY 1, 2
), actual AS (
    SELECT bucket, tags_id, c FROM cv_e2e
)
SELECT count(*) AS source_vs_cagg_diff_after_insert
  FROM (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual   EXCEPT SELECT * FROM expected)
  ) d;

-- ============================================================
-- E2E-03 (alter_job pause / resume / interval change) was previously
-- here as a real-time test that waited 12 s of wall clock under
-- schedule_interval=5s to assert "paused → no new ticks", and another
-- 8 s window for the lengthen-interval case.  Removed because:
--
--   * pause:    cagg_bgw_policy_scheduler_run.sql BGW-FULL-06 verifies
--               the same property strictly stronger — it backdates
--               next_start and force-fires the (mock) scheduler;
--               total_runs MUST stay flat after a forced tick.  Our
--               "wait 12 s and assume two ticks would have happened"
--               is a weaker probabilistic surrogate.
--   * resume:   BGW-FULL-06 covers the resume half identically.
--   * interval: BGW-FULL-03 verifies alter_job(schedule_interval)
--               updates the catalog AND that next_start is recomputed.
--               The "wait 8 s and count ticks" assertion here added no
--               coverage beyond the catalog read while being the most
--               wall-clock-sensitive part of the file.
--
-- E2E-01/02/04 retain the real-fork end-to-end coverage.  The
-- mock-time scheduler runs the same bgw_scheduler_process code path
-- (RegisterDynamicBackgroundWorker + WaitForBackgroundWorkerStartup),
-- not a synchronous in-process call, so "alter_job → scheduler
-- honors catalog change" is genuinely exercised under mock.
-- ============================================================

-- ============================================================
-- E2E-04: cleanup the policy -- worker stops firing
-- ============================================================
\echo '=== E2E-04: remove policy -> no further runs ==='
SELECT total_runs AS runs_at_removal FROM time_series.bgw_job_stat
 WHERE job_id = :jid_e2e \gset

SELECT remove_continuous_aggregate_policy('cv_e2e');

-- After remove, the bgw_job row is gone and stat is cascade-deleted.
SELECT count(*) AS job_rows_after_remove
  FROM time_series.bgw_job WHERE id = :jid_e2e;
SELECT count(*) AS stat_rows_after_remove
  FROM time_series.bgw_job_stat WHERE job_id = :jid_e2e;

-- ============================================================
-- Cleanup
--
-- This file is the one E2E test that lets the REAL scheduler fork and
-- reap background workers for real (schedule_interval=5s, dozens of
-- ticks across E2E-01/E2E-02).  Stop it before DROP EXTENSION to avoid
-- a catalog-lock ABBA against check_for_stopped_and_timed_out_jobs()'s
-- BGWH_STOPPED path (unbounded lock wait; observed as "deadlock
-- detected"), same class of race already guarded against in every
-- iso2 test that ends in DROP EXTENSION -- see
-- compress_insert_no_deadlock.sql.
-- ============================================================
SELECT time_series.stop_background_workers();

DROP VIEW cv_e2e;
DROP TABLE m_e2e;
DROP FUNCTION poll_runs(int);
DROP FUNCTION wait_for_job(int, int, int);
DROP EXTENSION time_series CASCADE;
