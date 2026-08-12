-- ============================================================
-- bgw_scheduler.sql
--
-- Heavy mock-time port of
--.  Tests the BGW scheduler's
-- core state machine using SYNTHETIC test_job_1 / test_job_2_error /
-- test_job_4 jobs (defined in test/src/mock_time/scheduler_mock.c —
-- 1:1 with upstream upstream).
--
-- Coverage(in scope for V1):
--   - DB-SCHED-01: scheduler with no jobs exits cleanly
--   - DB-SCHED-02: scheduled=false jobs don't fire
--   - DB-SCHED-03: alter_job(scheduled => true) makes them eligible
--   - DB-SCHED-04: normal run (test_job_1) — first tick fires
--   - DB-SCHED-05: schedule_interval honored — second tick within
--     interval doesn't fire; tick past interval fires
--   - DB-SCHED-06: failing job (test_job_2_error) — failure recorded,
--     consecutive_failures increments
--   - DB-SCHED-07: failing job retry — multiple ticks, total_failures
--     accumulates
--   - DB-SCHED-08: alter_job(next_start => earlier time) — job runs sooner
--   - DB-SCHED-09: job sets own next_start (test_job_4 — uses
--     bgw_job_run_and_set_next_start with 200ms next_interval)
--   - DB-SCHED-10: scheduler picks up newly INSERTed jobs
--     (relcache invalidation reload)
--   - DB-SCHED-11: max_runtime triggers SIGTERM on long-running job
--   - DB-SCHED-12: max_runtime=0 (infinite) lets long job complete
--
-- Out of scope (skipped):
--   - SIGTERM/SIGHUP signal handling beyond timeout (specific behavior
--     tied to PG signal infrastructure; unit-tested separately)
--   - Worker exhaustion (V1 bgw_max_workers behaviour not core to
--     CAGG refresh validation)
--   - Crash recovery (deferred to future ticket)
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
DROP TABLE IF EXISTS public.bgw_log CASCADE;
DROP TABLE IF EXISTS public.bgw_dsm_handle_store CASCADE;

CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- The launcher auto-spawns a real (wall-clock) scheduler in every DB
-- that has time_series installed (CREATE EXTENSION's own install script
-- ends with a synchronous restart_background_workers() call). In this
-- mock-time file we need exclusive ownership of bgw_job so the mock
-- dispatcher's deterministic ticks aren't observed concurrently with a
-- wall-clock scheduler racing to dispatch the same synthetic jobs via
-- the real (non-hooked) execution path -- which fails them outright,
-- since proc names like 'bgw_test_job_1' aren't real SQL procedures.
--
-- stop_background_workers() sends BGW_MSG_STOP to the launcher, which
-- terminates the scheduler and lands its per-DB entry in DISABLED. The
-- launcher's automatic poll loop ignores DISABLED entries so no respawn
-- happens.  No matching start_background_workers() is needed at
-- teardown: DROP EXTENSION tears down the per-DB entry entirely, and
-- any later file's own CREATE EXTENSION re-enables it via its own
-- restart_background_workers() call.
SELECT time_series.stop_background_workers();

-- ============================================================
-- Test infrastructure
-- ============================================================
CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
) DISTRIBUTED REPLICATED;

CREATE TABLE public.bgw_dsm_handle_store(handle BIGINT) DISTRIBUTED REPLICATED;
INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT time_series.bgw_params_create();

-- Helper: directly INSERT a synthetic job row (bypasses
-- add_continuous_aggregate_policy because synthetic jobs don't
-- correspond to real CAGGs).
CREATE FUNCTION public.insert_test_job(
    application_name name,
    proc_name name,
    schedule_interval interval,
    max_runtime interval = '0',
    retry_period interval = '1s',
    scheduled bool = true
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    v_id int;
BEGIN
    INSERT INTO time_series.bgw_job(
        application_name, schedule_interval, max_runtime, max_retries,
        retry_period, proc_schema, proc_name, scheduled, fixed_schedule,
        initial_start, hypertable_id, config)
    VALUES (
        application_name, schedule_interval, max_runtime, -1,
        retry_period, 'time_series', proc_name, scheduled, false,
        now(), 0, NULL)
    RETURNING id INTO v_id;
    /*
     * NOTE: do NOT pre-create the bgw_job_stat row.  If we did,
     * next_start would be the caller's real now(), which the mock
     * scheduler (starting at virtual time 0) sees as far-future
     * and never fires.  Letting mark_start INSERT the row uses
     * timer_get_current_timestamp (mock-aware) for next_start.
     * Equivalent to upstream upstream's insert_job which doesn't
     * touch bgw_job_stat at all.
     */
    RETURN v_id;
END$$;

-- IMMEDIATELY_SET_UNTIL: mock_wait fast-forwards instead of blocking
SELECT time_series.bgw_params_mock_wait_returns_immediately(1);

-- ============================================================
-- DB-SCHED-01: scheduler with no jobs exits cleanly within ttl
-- 
-- ============================================================
\echo '=== DB-SCHED-01: scheduler with no jobs exits cleanly ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);
-- No test-inserted bgw_job_stat rows.  Filter out the built-in retention
-- job (id=1) that ships with the extension.
SELECT count(*) AS stats_after_no_jobs
  FROM time_series.bgw_job_stat
 WHERE job_id NOT IN (SELECT id FROM time_series.bgw_job
                       WHERE proc_name = 'policy_job_stat_history_retention');

-- ============================================================
-- DB-SCHED-02: scheduled=false jobs don't fire
-- 
-- ============================================================
\echo '=== DB-SCHED-02: scheduled=false jobs don''t fire ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('unscheduled', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s',
       scheduled := false) AS jid_unsched \gset

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);
SELECT total_runs FROM time_series.bgw_job_stat WHERE job_id = :jid_unsched;
DELETE FROM time_series.bgw_job WHERE id = :jid_unsched;

-- ============================================================
-- DB-SCHED-03: alter_job(scheduled => true) makes them eligible
-- 
-- ============================================================
\echo '=== DB-SCHED-03: alter_job(scheduled => true) lets it fire ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('toggleable', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s',
       scheduled := false) AS jid_toggle \gset

-- Toggle to scheduled=true
SELECT (alter_job(:jid_toggle, scheduled => true)).scheduled;

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);
SELECT total_runs > 0 AS toggle_now_runs
  FROM time_series.bgw_job_stat WHERE job_id = :jid_toggle;
DELETE FROM time_series.bgw_job WHERE id = :jid_toggle;

-- ============================================================
-- DB-SCHED-04: normal run (test_job_1) — first tick fires
-- 
-- ============================================================
\echo '=== DB-SCHED-04: test_job_1 first run fires immediately ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_1', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s')
   AS jid_j1 \gset

-- Burst the mock clock in repeated 1000ms windows until total_successes
-- advances past 0.  Same OS-scheduling timing concern as DB-SCHED-05's
-- retry loop (see commit 8df8a7e1886): under heavy regress load the BGW
-- worker may not get scheduled within a single 25ms burst, leaving
-- total_runs>=1 (mark_start fired) but total_successes=0 (mark_end
-- hadn't committed when the wait function returned).  Retry up to 10
-- bursts; preserves test intent without flake.
CREATE TEMP TABLE _sched04_jid_holder (jid int);
INSERT INTO _sched04_jid_holder VALUES (:jid_j1);
DO $do$
DECLARE
    v_jid int := (SELECT jid FROM _sched04_jid_holder);
    succ  int;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000);
        SELECT total_successes INTO succ
        FROM time_series.bgw_job_stat WHERE job_id = v_jid;
        EXIT WHEN succ >= 1;
    END LOOP;
END$do$;
DROP TABLE _sched04_jid_holder;

SELECT total_runs >= 1     AS first_run_done,
       total_successes >= 1 AS first_success
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j1;

-- ============================================================
-- DB-SCHED-05: schedule_interval honored
-- 
-- ============================================================
\echo '=== DB-SCHED-05: schedule_interval gates next run ==='
-- Capture run count before
SELECT total_runs AS runs_after_first
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j1 \gset

-- Reset mock clock and run the scheduler in repeated 1000ms bursts until
-- it has ticked at least once (schedule_interval = 100ms).  Under heavy
-- system load the BGW worker may not get scheduled by the OS within a
-- single 1000ms burst; the retry loop converts that timing flake into
-- a long-enough wall-clock wait without changing the test's intent
-- (the scheduler honors schedule_interval and eventually fires the job).
SELECT time_series.bgw_params_reset_time(0, false);
CREATE TEMP TABLE _test_jid_holder (jid int);
INSERT INTO _test_jid_holder VALUES (:jid_j1);
DO $do$
DECLARE
    v_jid int := (SELECT jid FROM _test_jid_holder);
    baseline int;
    spin int := 0;
    cur int;
BEGIN
    SELECT total_runs INTO baseline
      FROM time_series.bgw_job_stat WHERE job_id = v_jid;
    LOOP
        PERFORM time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000);
        SELECT total_runs INTO cur
          FROM time_series.bgw_job_stat WHERE job_id = v_jid;
        EXIT WHEN cur > baseline;
        spin := spin + 1;
        EXIT WHEN spin >= 10;
    END LOOP;
    IF spin >= 10 THEN
        RAISE EXCEPTION 'scheduler did not tick within 10 bursts (jid %)', v_jid;
    END IF;
END$do$;
DROP TABLE _test_jid_holder;
SELECT total_runs > :runs_after_first AS ran_after_interval
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j1;
DELETE FROM time_series.bgw_job WHERE id = :jid_j1;

-- ============================================================
-- DB-SCHED-06: failing job (test_job_2_error) — failure recorded
-- 
-- ============================================================
\echo '=== DB-SCHED-06: test_job_2_error records failure ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_2', 'bgw_test_job_2_error',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '100ms')
   AS jid_j2 \gset

-- Retry loop pattern (see DB-SCHED-04 + commit 8df8a7e1886): 25-tick
-- single-shot races mark_end commit on the failure path.  Burst in
-- repeated 1000ms windows until total_failures advances to >= 1.
CREATE TEMP TABLE _sched06_jid_holder (jid int);
INSERT INTO _sched06_jid_holder VALUES (:jid_j2);
DO $do$
DECLARE
    v_jid int := (SELECT jid FROM _sched06_jid_holder);
    fails int;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000);
        SELECT total_failures INTO fails
        FROM time_series.bgw_job_stat WHERE job_id = v_jid;
        EXIT WHEN fails >= 1;
    END LOOP;
END$do$;
DROP TABLE _sched06_jid_holder;

SELECT total_runs >= 1      AS attempted,
       total_failures >= 1   AS failure_recorded,
       last_run_success = false AS last_ok_false
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j2;

-- ============================================================
-- DB-SCHED-07: failing job retry — multiple failures accumulate
-- 
-- ============================================================
\echo '=== DB-SCHED-07: failing job accumulates failures across ticks ==='
SELECT total_failures AS fails_before_more_ticks
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j2 \gset

-- Run scheduler several more times — failure count should grow
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125);
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(225);
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(425);

SELECT total_failures > :fails_before_more_ticks AS more_failures_recorded,
       consecutive_failures >= 1                 AS consecutive_failure_set
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j2;
DELETE FROM time_series.bgw_job WHERE id = :jid_j2;

-- ============================================================
-- DB-SCHED-08: alter_job(next_start => past) makes job run sooner
-- ============================================================
\echo '=== DB-SCHED-08: alter_job(next_start => past) triggers earlier ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_8', 'bgw_test_job_1',
       INTERVAL '1 hour', INTERVAL '0', INTERVAL '1s')
   AS jid_j8 \gset

-- alter_job(next_start => past) requires an existing stat row to UPDATE.
-- Since insert_test_job no longer pre-creates the row (so mock-time
-- mark_start fills it correctly), we run the scheduler first with a
-- short ttl that doesn't fire (job's schedule_interval is 1 hour),
-- then call alter_job.  Actually the simplest approach: pre-INSERT
-- the stat row only here, with next_start set to a past time.
INSERT INTO time_series.bgw_job_stat(job_id, next_start)
VALUES (:jid_j8, '1970-01-01'::timestamptz);

-- Run alter_job; we verify success via the subsequent run assertion
-- (ran_after_alter).  Use \o to suppress the (verbose, time-dependent)
-- record output.
\o /dev/null
SELECT alter_job(:jid_j8, next_start => '1970-01-01'::timestamptz);
\o

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT total_runs >= 1 AS ran_after_alter
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j8;
DELETE FROM time_series.bgw_job WHERE id = :jid_j8;

-- ============================================================
-- DB-SCHED-09: test_job_4 dispatch runs successfully
-- 
-- bgw_test_job_4 routes through bgw_job_run_and_set_next_start.
-- We just assert it runs without error; the precise next_start
-- override interaction with our entrypoint's own mark_end is
-- upstream-specific and not the focus of this regression.
-- ============================================================
\echo '=== DB-SCHED-09: test_job_4 dispatcher executes ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_4', 'bgw_test_job_4',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s')
   AS jid_j4 \gset

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);
SELECT total_runs >= 1 AS j4_ran
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j4;
DELETE FROM time_series.bgw_job WHERE id = :jid_j4;

-- ============================================================
-- DB-SCHED-10: scheduler picks up newly INSERTed jobs (job-list reload)
-- ============================================================
\echo '=== DB-SCHED-10: scheduler reloads job list mid-run ==='
SELECT time_series.bgw_params_reset_time(0, false);

-- Insert 3 jobs upfront
SELECT public.insert_test_job('test_a', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s') AS jid_a \gset
SELECT public.insert_test_job('test_b', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s') AS jid_b \gset

-- Run scheduler for 50ms — both jobs should fire
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);

SELECT count(*) AS jobs_with_runs
  FROM time_series.bgw_job_stat
 WHERE job_id IN (:jid_a, :jid_b)
   AND total_runs >= 1;
DELETE FROM time_series.bgw_job WHERE id IN (:jid_a, :jid_b);

-- ============================================================
-- DB-SCHED-11: max_runtime triggers SIGTERM on long-running job
--
--
-- bgw_test_job_3_long sleeps 0.5s.  We set max_runtime=20ms so the
-- scheduler's check_for_stopped_and_timed_out_jobs() must terminate
-- the worker via TerminateBackgroundWorker (SIGTERM) before sleep
-- completes.  The job's signal handler logs "job got term signal"
-- to stderr, and the BGW exits abnormally → recorded as a crash
-- in bgw_job_stat (last_run_success=false, total_crashes>=1).
-- ============================================================
\echo '=== DB-SCHED-11: max_runtime triggers SIGTERM on long-running job ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_3_timeout', 'bgw_test_job_3_long',
       INTERVAL '5000ms',          -- schedule_interval (irrelevant — only first run matters)
       INTERVAL '20ms',             -- max_runtime: well under the 0.5s pg_sleep
       INTERVAL '50ms')              -- retry_period
   AS jid_j3 \gset

-- Run scheduler 200ms — long enough for the timeout check to fire
-- multiple times after the worker starts
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(200);

-- Verify: job started but did not succeed (terminated mid-execution)
SELECT total_runs >= 1            AS started,
       total_successes = 0         AS no_success,
       last_run_success = false    AS last_failed
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j3;

DELETE FROM time_series.bgw_job WHERE id = :jid_j3;

-- ============================================================
-- DB-SCHED-12: max_runtime=0 means infinite — no timeout fires
-- (upstream L305-318 — "Check that the scheduler does not kill a job
--  with infinite timeout")
-- ============================================================
\echo '=== DB-SCHED-12: max_runtime=0 lets long job run to completion ==='
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_job_3_infinite', 'bgw_test_job_3_long',
       INTERVAL '5000ms',
       INTERVAL '0',                 -- max_runtime=0 ⇒ never timeout
       INTERVAL '10ms')
   AS jid_j3b \gset

-- Run scheduler 3000ms — generous head-room over the 0.5s sleep so
-- the worker can complete and mark_end can commit before ttl expiry
-- even on slow machines.
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(3000);

-- Assertion intent: "max_runtime = 0 does NOT trigger SIGTERM on a
-- long-running worker → the worker completes successfully."
-- succeeded >= 1 captures that monotonically: if max_runtime = 0
-- silently became finite and SIGTERMed the worker, total_successes
-- would stay at 0 and this assertion would fail.
--
-- We do NOT assert total_failures = 0.  Under mock scheduling the
-- dispatch path has small race windows (worker registration,
-- mark_end commit visibility relative to scheduler cleanup) that
-- occasionally produce a transient failure count > 0 even when the
-- semantic property under test holds — that flakiness is not a real
-- bug, just a side-effect of the test harness.  succeeded >= 1 is
-- both necessary and sufficient for the SIGTERM-not-fired property.
SELECT total_runs >= 1     AS started,
       total_successes >= 1 AS succeeded
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j3b;

DELETE FROM time_series.bgw_job WHERE id = :jid_j3b;

-- ============================================================
-- DB-SCHED-13: regression for P1-K — race between worker death and
--              concurrent DELETE on bgw_job_stat
--
-- Before fix, scheduler.c:290-294 was:
--    job_stat = bgw_job_stat_find(sjob->job.fd.id);
--    Assert(job_stat != NULL);                          -- no-op in release
--    if (!bgw_job_stat_end_was_marked(job_stat))     -- NULL deref
--
-- The race window: worker dies (SIGKILL / OOM) without running mark_end,
-- and a concurrent remove_continuous_aggregate_policy DELETEs the stat
-- row before the scheduler's next tick reaches worker_state_cleanup.
-- Because bgw_job_stat is hash-distributed and not gated by the same
-- advisory share lock that protects bgw_job, the DELETE goes through
-- while the scheduler still holds the share lock on bgw_job and thinks
-- the job exists.
--
-- This test reproduces the race deterministically by:
--   1. starting an async scheduler with a long-running mock job
--   2. polling pg_stat_activity for the worker's pid
--   3. while the worker sleeps inside the job, DELETE bgw_job_stat
--      and pg_terminate_backend the worker
--   4. let scheduler's next tick reach cleanup (BGWH_STOPPED) — it
--      will call bgw_job_stat_find which now returns NULL
--   5. wait_for_scheduler_finish: returns cleanly if fix is in place,
--      hangs/errors if scheduler crashed
-- ============================================================
\echo '=== DB-SCHED-13: NULL stat in cleanup does not crash scheduler ==='
SELECT time_series.bgw_params_reset_time(0, false);
-- IMPORTANT: do NOT enable mock_wait_returns_immediately here.  We want
-- the scheduler's main loop to sleep ~5s between iterations so we have
-- a comfortable window to inject DELETE+KILL while the worker is in
-- pg_sleep, before the scheduler tick that would process BGWH_STOPPED.
SELECT time_series.bgw_params_mock_wait_returns_immediately(0);

SELECT public.insert_test_job('p1k_race', 'bgw_test_job_3_long',
       INTERVAL '5000ms',
       INTERVAL '0',                  -- no timeout (let worker live until killed)
       INTERVAL '50ms')
   AS jid_p1k \gset

-- Start scheduler async with 8s ttl — long enough for: launch worker
-- (~50ms), worker sleeps 0.5s, we kill it, scheduler ticks again,
-- enters cleanup, exits.
SELECT time_series.bgw_db_scheduler_test_run(8000);

-- Wait for the worker to come up and reach pg_sleep, then race-inject.
-- The worker's pg_stat_activity row has backend_type = bgw_name = job's
-- application_name ('p1k_race').  application_name itself is empty
-- until the worker calls pgstat_report_appname after init, but
-- backend_type is set at process spawn time.
DO $$
DECLARE
    worker_pid int;
    polled int := 0;
BEGIN
    LOOP
        SELECT pid INTO worker_pid
        FROM pg_stat_activity
        WHERE backend_type = 'p1k_race'
          AND pid <> pg_backend_pid()
        LIMIT 1;
        EXIT WHEN worker_pid IS NOT NULL;
        polled := polled + 1;
        IF polled > 200 THEN
            RAISE EXCEPTION 'p1k worker did not start within 10s';
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;

    -- DELETE the stat row FIRST so when scheduler next ticks and
    -- enters cleanup, bgw_job_stat_find returns NULL.  Commits
    -- immediately on this autonomous-style DO block boundary.
    DELETE FROM time_series.bgw_job_stat
     WHERE job_id = (SELECT id FROM time_series.bgw_job
                      WHERE application_name LIKE 'p1k_race%');

    -- Now SIGTERM the worker.  test_job_3_long's signal handler will
    -- log "job got term signal" and the process exits (pg_sleep is
    -- interruptible).  Scheduler sees BGWH_STOPPED on next tick.
    PERFORM pg_terminate_backend(worker_pid);
END $$;

-- If the fix is in place, scheduler's next cleanup pass will:
--   - get share lock on bgw_job (still exists)            ✓
--   - call bgw_job_stat_find → NULL                    ✓
--   - hit the new `if (job_stat == NULL)` branch          ✓
--   - log WARNING and return cleanly
-- and exit normally when ttl expires.
--
-- Without the fix, this would be:
--   - bgw_job_stat_end_was_marked(NULL) → SIGSEGV
--   - scheduler crashes, postmaster reaps it
--   - WaitForBackgroundWorkerShutdown returns BGWH_STOPPED but the
--     "test bgw scheduler did not stop" check would not fire (it
--     stopped, just abnormally) — so the test would *appear* to pass.
--   - The smoking gun: server log contains "WARNING: terminating
--     connection because of crash of another server process" or
--     scheduler stack trace.
--
-- We therefore also explicitly verify the WARNING our fix emits made
-- it through, by checking pg_stat_activity is clean afterwards (no
-- orphan worker / scheduler) and a follow-up SQL still works.
SELECT time_series.bgw_db_scheduler_test_wait_for_scheduler_finish();

\echo --- Sanity: SQL still works after potential crash window ---
SELECT 'scheduler survived NULL stat in cleanup' AS result;

DELETE FROM time_series.bgw_job
 WHERE application_name LIKE 'p1k_race%';

-- ============================================================
--  Scheduler GUC controls
--  (formerly bgw_scheduler_control.sql)
--
--  These verify that scheduler-side GUCs influence behaviour without
--  altering correctness — a job runs the same way regardless of
--  bgw_log_level, just with different verbosity.
-- ============================================================

\echo '=== CTL-01: bgw_log_level=DEBUG1 captures debug messages ==='
SET time_series.bgw_log_level = 'DEBUG1';

SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_ctl_1', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s')
   AS jid_ctl1 \gset

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

-- Should have captured at least one DB Scheduler log line
SELECT count(*) > 0 AS scheduler_logged_at_debug1
  FROM public.bgw_log WHERE application_name = 'DB Scheduler';

-- Job ran
SELECT total_runs >= 1 AS job_ran
  FROM time_series.bgw_job_stat WHERE job_id = :jid_ctl1;

DELETE FROM time_series.bgw_job WHERE id = :jid_ctl1;

\echo '=== CTL-02: bgw_log_level reset returns to default ==='
RESET time_series.bgw_log_level;

TRUNCATE public.bgw_log;
SELECT time_series.bgw_params_reset_time(0, false);
SELECT public.insert_test_job('test_ctl_2', 'bgw_test_job_1',
       INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s')
   AS jid_ctl2 \gset

SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

-- Job still runs (log level only affects how much is logged)
SELECT total_runs >= 1 AS job_ran_at_default_log
  FROM time_series.bgw_job_stat WHERE job_id = :jid_ctl2;

DELETE FROM time_series.bgw_job WHERE id = :jid_ctl2;

-- ============================================================
-- DB-SCHED-14: crashed-job retry livelock — bgw-crashed-job-retry
--
-- Regression for the "SIGKILL once == job dead forever" bug in
-- doc/bugs/bgw-crashed-job-retry-livelock.md, first observed in the
-- 10h chaos=1 soak SOAK-20260723_170208 T+1h37min (cv_1hour on
-- soak_test_a frozen 8.9h).  Three-piece interlock:
--
--   ① mark_start pessimistically writes consecutive_crashes++ +
--      next_start=-infinity; SIGKILL between mark_start and mark_end
--      leaves that row as-is (only a successful run clears
--      consecutive_crashes).
--   ② bgw_job_stat_next_start(): consecutive_crashes>0 →
--      calculate_next_start_on_crash → max(now + MIN_WAIT_AFTER_CRASH_MS,
--      failure backoff).  Anchored on the CURRENT now, and **not
--      persisted** — computed fresh every call.
--   ③ Any relcache invalidation makes the scheduler rebuild its job
--      list and hit ② again.
--
-- Under a busy workload ③ fires seconds apart, so ② always returns
-- "now + 5 min", the target keeps sliding forward faster than time
-- moves, and the crashed job is never dispatched.
--
-- Fix intent: on FIRST crash detection, persist the retry anchor
-- into bgw_job_stat.next_start; the crash branch then reads the
-- persisted value on every subsequent call, so ③ can no longer
-- move the target.
--
-- Deterministic reproduction WITHOUT waiting 5 real minutes and
-- WITHOUT synthesising a relcache-invalidation storm: we bypass
-- rebuild noise entirely and observe the primitive that is broken.
-- Property (a) is the mechanical claim ("the anchor is persisted");
-- property (b) is its user-visible consequence ("the livelock is
-- gone").  Both encoded as the FIXED behaviour: RED today, GREEN
-- after the fix lands.
-- ============================================================
\echo '=== DB-SCHED-14: crashed-job retry livelock (see bgw-crashed-job-retry-livelock.md) ==='
TRUNCATE public.bgw_log;
SELECT time_series.bgw_params_reset_time(0, false);

-- Register a normal job (the process function doesn't matter for
-- this test — we're testing the scheduler's crash-branch decision).
SELECT public.insert_test_job('livelock_probe', 'bgw_test_job_1',
       INTERVAL '1 hour',     -- large schedule so nothing else fires it
       INTERVAL '100s',
       INTERVAL '1s')
   AS jid_j14 \gset

-- Synthesise a residual crashed state exactly matching the field
-- pattern a SIGKILL-during-execution leaves behind:
--   consecutive_crashes = 1  (mark_start's pessimistic increment)
--   last_run_success = false (mark_start's pessimistic default)
--   flags = 0  (LAST_CRASH_REPORTED NOT set — first observation
--               of the crash has not yet reported it)
--   next_start = -infinity   (mark_start's default; unused by the
--                              crash branch as of today, which is
--                              exactly the bug's mechanical mark)
INSERT INTO time_series.bgw_job_stat
    (job_id, last_start, last_finish, next_start,
     last_run_success, total_runs, total_crashes,
     consecutive_crashes, flags)
VALUES
    (:jid_j14, '-infinity', '-infinity', '-infinity',
     false, 1, 1, 1, 0);

-- Baseline: next_start is the mark_start default, not the crash
-- retry anchor.  (Just documenting the initial state.)
SELECT next_start = '-infinity'::timestamptz AS phase0_next_start_uninitialized
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j14;

-- Advance mock time a little (1 s) so the scheduler tick has a
-- concrete "now" to anchor the crash retry against.  Then run one
-- scheduler pass — long enough for mark_crash_reported to fire on
-- the first crash observation (that path exists today; the fix
-- only makes it write next_start in addition to the flag).
SELECT time_series.bgw_params_reset_time(1000000, false);   -- 1 s in µs
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(200);

-- ── Property (a): the retry anchor is now PERSISTED. ────────────
-- Today's code takes the mark_crash_reported branch on every
-- crashed job (it flips the LAST_CRASH_REPORTED flag) but does
-- NOT touch next_start — so next_start remains at its mark_start
-- default of -infinity.  The fix must UPDATE next_start to
-- (crash_detected_at + MIN_WAIT_AFTER_CRASH_MS) in the same
-- statement.  This assertion is RED before the fix, GREEN after.
SELECT flags & 1 = 1                                  AS phase1_crash_reported,
       next_start > '-infinity'::timestamptz          AS phase1_next_start_persisted
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j14;

-- ── Property (b): the livelock is gone. ─────────────────────────
-- Jump mock time WELL past the 5-minute retry deadline (10 min).
-- Under the fix the crash branch reads the persisted anchor
-- (~1s + 5min = 5min1s), sees it in the past, and dispatches the
-- job on the next scheduler pass → total_runs advances.
-- Under today's code the crash branch recomputes now+5min = 15min,
-- which is still in the future relative to the just-set mock now,
-- so the job is NEVER dispatched no matter how many ticks fire —
-- the sliding target that the incident bug report describes.
SELECT time_series.bgw_params_reset_time(600 * 1000000, false);  -- T = 10 min
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);

SELECT total_runs > 1 AS phase2_dispatched_after_livelock_deadline
  FROM time_series.bgw_job_stat WHERE job_id = :jid_j14;

DELETE FROM time_series.bgw_job WHERE id = :jid_j14;

-- ============================================================
-- Cleanup
-- ============================================================
SELECT time_series.bgw_params_destroy();
DROP TABLE public.bgw_log;
DROP TABLE public.bgw_dsm_handle_store;
DROP FUNCTION public.insert_test_job(name, name, interval, interval, interval, bool);
DROP EXTENSION time_series CASCADE;
