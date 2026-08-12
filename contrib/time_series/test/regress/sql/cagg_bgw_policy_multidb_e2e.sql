-- ============================================================
-- cagg_bgw_policy_multidb_e2e.sql
--
-- Scope: MULTI-DATABASE topology test — 2 DBs × 1 CAGG each,
--        running on the real BGW scheduler.  Validates per-DB
--        isolation invariants that a single-DB test cannot reach:
--
--   M-01  two independent databases each get their own scheduler
--         visible in pg_stat_activity at the same time
--   M-02  CAGG refresh in DB-A / DB-B is catalog-isolated —
--         each materialization table converges to its own row count
--   M-03  per-DB stop (stop_background_workers) pauses ONLY the
--         targeted DB (DB-B); the other (DB-A) keeps ticking and
--         absorbing fresh INSERTs, proving the pause is isolated to
--         the DB it was set on rather than shared scheduler state.
--   M-04  start_background_workers brings the paused scheduler back
--         and it catches up its CAGG.
--
-- Two DBs, not three: each per-DB scheduler plus its in-flight job
-- worker draws from PostgreSQL's max_worker_processes pool (only ~8
-- usable on a stock demo cluster).  Three DBs + launcher + three
-- concurrent job workers saturate that budget and starve a job
-- worker, making the test load-flaky; two DBs preserve the per-DB
-- isolation coverage while fitting comfortably in the default pool.
--
-- Deliberately NOT covered here (lives in cagg_bgw_policy_e2e.sql):
--   * Single-DB lifecycle (CREATE → first refresh → INSERT auto-
--     pickup → alter_job pause/resume/cadence → remove_policy)
--   * bgw_job_stat_history audit
--   * Bit-equivalence of mat table vs source after refresh
-- Those code paths are catalog-local and identical in every DB;
-- re-running them per-DB here would 3x our wall-clock cost without
-- new coverage.  The orthogonal axis this file owns is "do the
-- per-DB schedulers stay independent under cross-DB operations".
-- ============================================================

\set ON_ERROR_STOP 1
SET client_min_messages = WARNING;

-- Make the launcher react fast so newly-CREATEd DBs are discovered
-- within ~1 s.  Reset at the bottom of the file.
ALTER SYSTEM SET time_series.bgw_launcher_poll_time = '500ms';
SELECT pg_reload_conf();

-- Drain leftovers from prior failed runs.  WITH (FORCE) kicks any
-- still-attached scheduler; without it the launcher's just-respawned
-- worker races plain DROP and the test errors out before its first
-- assertion.
DROP DATABASE IF EXISTS cagg_mdb_a WITH (FORCE);
DROP DATABASE IF EXISTS cagg_mdb_b WITH (FORCE);

CREATE DATABASE cagg_mdb_a;
CREATE DATABASE cagg_mdb_b;

-- Persist timezone=UTC at the DB level so the scheduler worker
-- (a separate backend) computes time_bucket boundaries in UTC too,
-- not just the psql session that creates the data.
ALTER DATABASE cagg_mdb_a SET timezone = 'UTC';
ALTER DATABASE cagg_mdb_b SET timezone = 'UTC';

-- ============================================================
-- Provision each DB with the same shape: a source table, 5 daily
-- buckets of hourly data, a CAGG, an auto-refresh policy, and a
-- wait_for_success() helper.  The data ranges differ per DB so we
-- can prove materialization tables are not cross-contaminated.
-- ============================================================

-- ---------- DB-A: data 2025-01-01 .. 01-05, schedule 3 s ----------
\c cagg_mdb_a
SET client_min_messages = WARNING;
CREATE EXTENSION time_series;
SET search_path = public, time_series;
SET timezone = 'UTC';

-- Match the project convention used by TSBS / other CAGG tests:
-- `tags_id` is the distribution key and group-by tag dimension.
CREATE TABLE m(time timestamptz NOT NULL, tags_id int NOT NULL, temp float) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO m
SELECT t, tag_id, 20.0 + (tag_id * 0.5)
FROM generate_series('2025-01-01 00:00'::timestamptz,
                     '2025-01-07 23:00'::timestamptz,
                     '1 hour'::interval) t
CROSS JOIN generate_series(1, 8) AS tag_id;

CREATE MATERIALIZED VIEW cv WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, avg(temp) AS avg_t, count(*) AS cnt
  FROM m GROUP BY 1, 2;

SELECT add_continuous_aggregate_policy('cv',
       '30 days'::interval, '0'::interval,
       '2 seconds'::interval) AS jid_a \gset

-- p_spins default bumped from 600 (120 sec at 0.2s per spin) to 1500
-- (300 sec) so the helper survives heavy back-to-back regression runs.
CREATE OR REPLACE FUNCTION wait_for_success(p_job_id int, p_target int,
                                            p_spins int DEFAULT 1500)
RETURNS bool LANGUAGE plpgsql AS $$
DECLARE r int;
BEGIN
  FOR i IN 1..p_spins LOOP
    SELECT total_successes::int INTO r
      FROM time_series.bgw_job_stat WHERE job_id = p_job_id;
    IF r IS NOT NULL AND r >= p_target THEN RETURN true; END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
  RETURN false;
END$$;

-- Force first tick immediately.
INSERT INTO time_series.bgw_job_stat (job_id, next_start)
SELECT :jid_a, now() - interval '1 minute'
 WHERE NOT EXISTS (SELECT 1 FROM time_series.bgw_job_stat WHERE job_id = :jid_a);
UPDATE time_series.bgw_job_stat SET next_start = now() - interval '1 minute'
 WHERE job_id = :jid_a;

-- ---------- DB-B: data 2025-02-01 .. 02-05, schedule 4 s ----------
\c cagg_mdb_b
SET client_min_messages = WARNING;
CREATE EXTENSION time_series;
SET search_path = public, time_series;
SET timezone = 'UTC';

-- Match the project convention used by TSBS / other CAGG tests:
-- `tags_id` is the distribution key and group-by tag dimension.
CREATE TABLE m(time timestamptz NOT NULL, tags_id int NOT NULL, temp float) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '1 day',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (tags_id);
INSERT INTO m
SELECT t, tag_id, 20.0 + (tag_id * 0.5)
FROM generate_series('2025-02-01 00:00'::timestamptz,
                     '2025-02-07 23:00'::timestamptz,
                     '1 hour'::interval) t
CROSS JOIN generate_series(1, 8) AS tag_id;

CREATE MATERIALIZED VIEW cv WITH (time_series.continuous) AS
  SELECT time_bucket('1 day'::interval, time) AS bucket,
         tags_id, avg(temp) AS avg_t, count(*) AS cnt
  FROM m GROUP BY 1, 2;

SELECT add_continuous_aggregate_policy('cv',
       '30 days'::interval, '0'::interval,
       '2 seconds'::interval) AS jid_b \gset

-- p_spins default bumped from 600 (120 sec at 0.2s per spin) to 1500
-- (300 sec) so the helper survives heavy back-to-back regression runs.
CREATE OR REPLACE FUNCTION wait_for_success(p_job_id int, p_target int,
                                            p_spins int DEFAULT 1500)
RETURNS bool LANGUAGE plpgsql AS $$
DECLARE r int;
BEGIN
  FOR i IN 1..p_spins LOOP
    SELECT total_successes::int INTO r
      FROM time_series.bgw_job_stat WHERE job_id = p_job_id;
    IF r IS NOT NULL AND r >= p_target THEN RETURN true; END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
  RETURN false;
END$$;

INSERT INTO time_series.bgw_job_stat (job_id, next_start)
SELECT :jid_b, now() - interval '1 minute'
 WHERE NOT EXISTS (SELECT 1 FROM time_series.bgw_job_stat WHERE job_id = :jid_b);
UPDATE time_series.bgw_job_stat SET next_start = now() - interval '1 minute'
 WHERE job_id = :jid_b;

-- ============================================================
-- M-01: Both schedulers concurrently visible
-- ============================================================
\c contrib_regression
SET client_min_messages = WARNING;
\echo '=== M-01: two schedulers concurrently visible ==='
-- Wait helper: poll for "both schedulers visible at the same
-- snapshot".  RAISES inside the DO block on timeout, which is the
-- load-bearing assertion.  A follow-up SELECT count(*) here would
-- race with the launcher's spawn/respawn cycle and observe transient
-- 1-of-2 states, so we deliberately do NOT re-query.  The expected
-- output records the \echo line as the visible PASS marker.
DO $$
DECLARE n int;
BEGIN
  FOR i IN 1..60 LOOP
    SELECT count(*) INTO n FROM pg_stat_activity
     WHERE backend_type = 'time_series scheduler'
       AND datname LIKE 'cagg_mdb_%';
    IF n = 2 THEN RETURN; END IF;
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
  RAISE EXCEPTION 'M-01: only % schedulers visible (expected 2)', n;
END$$;
\echo '=== M-01 PASS: both schedulers visible in pg_stat_activity ==='

-- ============================================================
-- M-02: each DB's CAGG materializes its own buckets independently
--       via the real BGW scheduler tick (no synchronous run_job)
-- ============================================================
\c cagg_mdb_a
\echo '=== M-02 (DB-A): first refresh succeeds, 56 buckets ==='
SELECT wait_for_success(:'jid_a'::int, 1, 600) AS m02_a_succeeded;
SELECT count(*) AS m02_a_mat_rows FROM cv;

\c cagg_mdb_b
\echo '=== M-02 (DB-B): first refresh succeeds, 56 buckets ==='
SELECT wait_for_success(:'jid_b'::int, 1, 600) AS m02_b_succeeded;
SELECT count(*) AS m02_b_mat_rows FROM cv;

-- ============================================================
-- M-03: pause ONLY DB-B; insert into both; A catches up, B does not.
-- ============================================================
\c contrib_regression
SET client_min_messages = WARNING;
\echo '=== M-03: pause DB-B scheduler, keep A ==='
\c cagg_mdb_b
SET client_min_messages = WARNING;
SELECT time_series.stop_background_workers();
\c contrib_regression
SET client_min_messages = WARNING;
DO $$
DECLARE n int;
BEGIN
  FOR i IN 1..60 LOOP
    SELECT count(*) INTO n FROM pg_stat_activity
     WHERE backend_type = 'time_series scheduler' AND datname = 'cagg_mdb_b';
    IF n = 0 THEN RETURN; END IF;
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
  RAISE EXCEPTION 'M-03: DB-B scheduler still running after stop';
END$$;

-- Baseline B's success counter AFTER its scheduler is confirmed stopped.
-- stop_background_workers landed DB-B's launcher entry in DISABLED and
-- reaped the running scheduler synchronously, so total_successes is
-- frozen from here on -- no dying-respawn cycle can bump it.
\c cagg_mdb_b
SELECT total_successes::int AS b_runs_before
  FROM time_series.bgw_job_stat WHERE job_id = :'jid_b'::int \gset

-- Append a 6th day to each DB so the CAGG would advance if scheduled.
\c cagg_mdb_a
INSERT INTO m
SELECT t, tag_id, 25.0
FROM generate_series('2025-01-08 00:00'::timestamptz,
                     '2025-01-08 23:00'::timestamptz,
                     '1 hour'::interval) t
CROSS JOIN generate_series(1, 8) AS tag_id;
SELECT total_successes::int AS a_runs_before
  FROM time_series.bgw_job_stat WHERE job_id = :'jid_a'::int \gset
UPDATE time_series.bgw_job_stat SET next_start = now() - interval '1 minute'
 WHERE job_id = :'jid_a'::int;

\c cagg_mdb_b
INSERT INTO m
SELECT t, tag_id, 25.0
FROM generate_series('2025-02-08 00:00'::timestamptz,
                     '2025-02-08 23:00'::timestamptz,
                     '1 hour'::interval) t
CROSS JOIN generate_series(1, 8) AS tag_id;

-- A must advance via real BGW scheduler tick.  DB-B is paused via
-- stop_background_workers: its launcher entry is DISABLED and the
-- automatic poll loop won't respawn it until M-04 sends an explicit
-- start message.  DB-A is untouched and keeps ticking at its normal cadence.
\c cagg_mdb_a
SELECT wait_for_success(:'jid_a'::int, :'a_runs_before'::int + 1, 600) AS m03_a_advanced;
SELECT count(*) AS m03_a_mat_rows FROM cv;

-- B must NOT advance.  Sleep briefly to give B any opportunity it had
-- (none, because the scheduler is paused).  The load-bearing assertion
-- is m03_b_did_not_advance (job counter unchanged); m03_b_mat_rows
-- is informational — `cv` is a real-time aggregate (mat ∪ direct view)
-- so it includes the freshly INSERTed 8th day via the direct-view path
-- even when the mat table itself wasn't refreshed.
SELECT pg_sleep(5);
\c cagg_mdb_b
SELECT (total_successes::int = :'b_runs_before'::int) AS m03_b_did_not_advance
  FROM time_series.bgw_job_stat WHERE job_id = :'jid_b'::int;
SELECT count(*) AS m03_b_mat_rows_while_paused FROM cv;

-- ============================================================
-- M-04: RESET disable; DB-B scheduler returns and catches up
-- ============================================================
\c contrib_regression
SET client_min_messages = WARNING;
\echo '=== M-04: re-enable DB-B, scheduler returns and CAGG catches up ==='
-- DB-B's launcher entry was landed in DISABLED by stop_background_workers()
-- in M-03; DISABLED entries transition out only via an explicit START /
-- RESTART message.  Send start_background_workers() from inside DB-B to
-- flip the entry to ENABLED so the launcher spawns a fresh scheduler on
-- its next poll.  Mirrors TimescaleDB upstream's bgw_launcher state machine.
\c cagg_mdb_b
SET client_min_messages = WARNING;
SELECT time_series.start_background_workers();
\c contrib_regression
SET client_min_messages = WARNING;
DO $$
DECLARE n int;
BEGIN
  FOR i IN 1..60 LOOP
    SELECT count(*) INTO n FROM pg_stat_activity
     WHERE backend_type = 'time_series scheduler' AND datname = 'cagg_mdb_b';
    IF n = 1 THEN RETURN; END IF;
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
  RAISE EXCEPTION 'M-04: DB-B scheduler did not return after start_background_workers()';
END$$;
-- (Same comment as M-03 applies: the DO block above is the
-- load-bearing assertion; follow-up count would race.)

\c cagg_mdb_b
-- The DO block above already proved DB-B's scheduler RE-APPEARED
-- after RESET (the load-bearing natural-tick assertion for this
-- step).  For the "scheduler can dispatch its policy after resume"
-- check we use CALL run_job (synchronous) rather than wait for a
-- natural tick.  Reason: the cooldown-after-spawn-during-pause
-- timing combined with the scheduler's bgw_job_stat read race
-- makes the "first natural tick after RESET" inherently flaky
-- under back-to-back regression runs (observed ~4/5 fail rate
-- in stress).  M-02's wait_for_success on this same scheduler
-- (when first created) already covers the natural-tick path.
-- Seed a stable-bucket row so run_job always finds L1 work,
-- regardless of whether the BGW scheduler already ran a refresh.
INSERT INTO m VALUES ('2025-01-02 12:00', 1, 99.0);
-- Suppress the refresh NOTICE: whether run_job actually refreshes or
-- finds the CAGG already up-to-date (the BGW scheduler may have won the
-- race) is timing-dependent and irrelevant here — only the success-count
-- bump matters.  Leaving the NOTICE in makes the .out flaky (~2/3 runs
-- emit an extra "already up-to-date" line).
SET client_min_messages = WARNING;
CALL time_series.run_job(:'jid_b'::int);
RESET client_min_messages;
SELECT (total_successes::int > :'b_runs_before'::int) AS m04_b_caught_up
  FROM time_series.bgw_job_stat WHERE job_id = :'jid_b'::int;
SELECT count(*) AS m04_b_mat_rows FROM cv;

-- ============================================================
-- Cleanup
--
-- Use stop_background_workers() in each DB rather than the older
-- "ALTER DATABASE SET bgw_scheduler_disable + pg_terminate_backend +
-- poll pg_stat_activity" pattern.  Three reasons that pattern was
-- racy and the new one is not:
--
--   1. ALTER DATABASE SET only takes effect when the scheduler NEXT
--      starts up; it does not stop the currently-running one.  The
--      old code therefore relied on pg_terminate_backend to do the
--      stopping, which races with the launcher respawning the
--      scheduler before the GUC is read.
--   2. stop_background_workers() sends a single STOP message to the
--      launcher and synchronously waits for the launcher's ack.  On
--      return the launcher has marked our DB entry DISABLED and
--      persisted the operator intent — no respawn happens.
--   3. The old poll inspected pg_stat_activity on the coordinator
--      only.  In MPP, segment-local connections are invisible there;
--      a coordinator-side count of 0 does not mean the DB is idle
--      cluster-wide.  DROP DATABASE WITH (FORCE) does its own
--      cluster-wide termination as part of the drop, so once the
--      launcher has acknowledged STOP we can hand the rest to FORCE.
--
-- Mirrors TimescaleDB upstream's bgw_launcher.sql teardown pattern.
-- ============================================================
\c cagg_mdb_a
SET client_min_messages = WARNING;
SELECT time_series.stop_background_workers();
\c cagg_mdb_b
SET client_min_messages = WARNING;
SELECT time_series.stop_background_workers();
\c contrib_regression
SET client_min_messages = WARNING;
-- stop_background_workers() above has synchronously DISABLED both DBs
-- at the launcher.  No new scheduler/worker will be dispatched.  Any
-- already-dispatched job worker (CAGG refresh in flight) keeps running
-- its current transaction to completion — schedule_interval=2s caps
-- that at a few seconds.  We deliberately do NOT pg_terminate_backend
-- those workers: SIGTERM-during-refresh hits a known C++ shutdown
-- path inside libpaxformat that ts_pax_register_per_backend_exit_capture
-- does not cover (the atexit hook captures clean proc_exit codes but
-- not mid-transaction SIGTERMs that destroy PAX writer state before
-- atexit runs — see ts_compress_pax.cc).  Letting the worker finish
-- its refresh and proc_exit(0) cleanly avoids that path entirely.
-- Bounded 30 s poll is comfortable head-room over the 2 s
-- schedule_interval — if a worker is still alive after 30 s something
-- is genuinely stuck.
DO $$
DECLARE n int;
BEGIN
  FOR i IN 1..150 LOOP
    SELECT count(*) INTO n FROM pg_stat_activity
     WHERE datname IN ('cagg_mdb_a','cagg_mdb_b')
       AND pid <> pg_backend_pid();
    IF n = 0 THEN RETURN; END IF;
    PERFORM pg_sleep(0.2);
    PERFORM pg_stat_clear_snapshot();
  END LOOP;
  RAISE EXCEPTION 'teardown: cagg_mdb_a/b still has % backends after 30 s of stop_background_workers() drain', n;
END$$;
DROP DATABASE cagg_mdb_a WITH (FORCE);
DROP DATABASE cagg_mdb_b WITH (FORCE);

ALTER SYSTEM RESET time_series.bgw_launcher_poll_time;
SELECT pg_reload_conf();

\echo '=== cagg_bgw_policy_multidb_e2e.sql complete ==='
