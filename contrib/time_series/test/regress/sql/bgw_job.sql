-- ============================================================
-- bgw_job.sql
--
-- Regression tests for the generic BGW job interface: add_job /
-- delete_job / run_job error paths, the built-in audit-log
-- retention task, and bgw_job_stat_history writes.  Lets users
-- register and remove arbitrary (int4, jsonb) procedures or
-- functions as scheduled BGW jobs.
--
-- Coverage focus is correctness of the validation and security
-- gates; this file does NOT actually wait for the BGW worker to
-- fire (mock-time scheduler tests cover that path).
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
DROP ROLE IF EXISTS bgw_job_other;

CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- Suppress this database's wall-clock scheduler for the duration of the
-- test.  HIST-01/02 below register scheduled jobs and assert exactly how
-- many times run_job() appends to bgw_job_stat_history; if the launcher's
-- per-database scheduler also fires the same due job, the history row
-- count doubles and the assertions flake.  stop_background_workers()
-- sends BGW_MSG_STOP to the launcher, which terminates the scheduler and
-- lands its per-DB entry in DISABLED so the automatic poll loop does not
-- respawn it.  Mirrors TimescaleDB upstream's mock-time test pattern.
SELECT time_series.stop_background_workers();

-- A trivial procedure with the required (int4, jsonb) signature.
CREATE PROCEDURE noop_job(jid int, cfg jsonb) LANGUAGE plpgsql AS $$
BEGIN END;
$$;

-- ============================================================
-- ADD-01: positive — add_job inserts bgw_job + bgw_job_stat rows
-- ============================================================
\echo '=== ADD-01: add_job inserts bgw_job + bgw_job_stat ==='
SELECT add_job('noop_job'::regproc, '1 hour'::interval,
               '{"hello":"world"}'::jsonb) AS jid \gset
SELECT proc_schema, proc_name, schedule_interval, scheduled, fixed_schedule,
       hypertable_id, owner, application_name
  FROM time_series.bgw_job WHERE id = :jid;
SELECT job_id, next_start IS NOT NULL AS has_next_start
  FROM time_series.bgw_job_stat WHERE job_id = :jid;

-- ============================================================
-- ADD-02..05: negative validation cases
-- ============================================================
\set ON_ERROR_STOP 0

\echo '=== ADD-02: schedule_interval = 0 rejected ==='
SELECT add_job('noop_job'::regproc, '0'::interval);

\echo '=== ADD-03: bad signature rejected (1 arg) ==='
CREATE FUNCTION bad_sig_one(t text) RETURNS void LANGUAGE plpgsql AS $$BEGIN END$$;
SELECT add_job('bad_sig_one'::regproc, '1 hour'::interval);

\echo '=== ADD-04: bad return type rejected (returns int) ==='
CREATE FUNCTION bad_ret(int, jsonb) RETURNS int LANGUAGE plpgsql AS $$BEGIN RETURN 1; END$$;
SELECT add_job('bad_ret'::regproc, '1 hour'::interval);

\echo '=== ADD-05: invalid timezone rejected ==='
SELECT add_job('noop_job'::regproc, '1 hour'::interval,
               timezone => 'Atlantis/Lost_City');

\set ON_ERROR_STOP 1

-- ============================================================
-- ADD-06: check_config positive — supplied validator runs and
--         is allowed to accept config
-- ============================================================
\echo '=== ADD-06: check_config validator runs and accepts ==='
CREATE FUNCTION my_check(cfg jsonb) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF cfg IS NULL OR NOT (cfg ? 'kind') THEN
        RAISE EXCEPTION 'config must contain "kind" key';
    END IF;
END$$;

SELECT add_job('noop_job'::regproc, '1 hour'::interval,
               '{"kind":"foo"}'::jsonb,
               check_config => 'my_check'::regproc) AS jid_ok \gset
SELECT check_schema, check_name FROM time_series.bgw_job WHERE id = :jid_ok;

-- ============================================================
-- ADD-07: check_config negative — validator rejects bad config
-- ============================================================
\echo '=== ADD-07: check_config rejects invalid config ==='
\set ON_ERROR_STOP 0
SELECT add_job('noop_job'::regproc, '1 hour'::interval,
               '{}'::jsonb,
               check_config => 'my_check'::regproc);
\set ON_ERROR_STOP 1

-- ============================================================
-- DEL-01: positive — delete_job removes bgw_job and bgw_job_stat
-- ============================================================
\echo '=== DEL-01: delete_job removes both rows ==='
SELECT count(*) AS job_before FROM time_series.bgw_job WHERE id = :jid;
SELECT count(*) AS stat_before FROM time_series.bgw_job_stat WHERE job_id = :jid;
SELECT delete_job(:jid);
SELECT count(*) AS job_after FROM time_series.bgw_job WHERE id = :jid;
SELECT count(*) AS stat_after FROM time_series.bgw_job_stat WHERE job_id = :jid;

-- ============================================================
-- DEL-02..03: negative cases
-- ============================================================
\set ON_ERROR_STOP 0

\echo '=== DEL-02: delete_job on already-gone job ==='
SELECT delete_job(:jid);

\echo '=== DEL-03: delete_job on nonexistent id ==='
SELECT delete_job(999999);

\set ON_ERROR_STOP 1

-- ============================================================
-- DEL-04: non-owner cannot delete another user's job
-- ============================================================
\echo '=== DEL-04: non-owner cannot delete; owner can ==='
SELECT add_job('noop_job'::regproc, '1 hour'::interval) AS jid2 \gset

CREATE ROLE bgw_job_other LOGIN;
GRANT USAGE ON SCHEMA time_series TO bgw_job_other;
GRANT EXECUTE ON FUNCTION time_series.delete_job(int) TO bgw_job_other;
GRANT SELECT ON time_series.bgw_job TO bgw_job_other;

\set ON_ERROR_STOP 0
SET ROLE bgw_job_other;
SELECT time_series.delete_job(:jid2);
RESET ROLE;
\set ON_ERROR_STOP 1

-- Owner (current session) can still delete.
SELECT delete_job(:jid2);
SELECT count(*) AS owner_cleanup FROM time_series.bgw_job WHERE id = :jid2;

-- ============================================================
-- RUN-01..02: run_job generic error paths
--   Both errors fire before any proc-specific dispatch in
--   bgw_run_job, so they don't depend on what (or whether) a job
--   is actually registered.
-- ============================================================
\set ON_ERROR_STOP 0

\echo '=== RUN-01: run_job for non-existent job → ERROR ==='
CALL time_series.run_job(99999);

\echo '=== RUN-02: run_job with NULL job_id → ERROR ==='
CALL time_series.run_job(NULL);

\echo '=== RUN-04: run_job inside explicit BEGIN/COMMIT raises clean error ==='
-- run_job manages transactions internally (AbortCurrentTransaction +
-- StartTransactionCommand so mark_end can record failure stat after
-- an exception).  PG forbids that in an atomic call context, so
-- run_job must reject up-front rather than crash with an assertion.
-- Documented contract: run_job is autocommit-only.
SELECT add_job('noop_job'::regproc, '1 hour'::interval) AS jid_txn \gset
\set VERBOSITY terse
BEGIN;
CALL time_series.run_job(:jid_txn);
ROLLBACK;
\set VERBOSITY default
-- Autocommit invocation still works (the supported path).
CALL time_series.run_job(:jid_txn);
SELECT total_runs > 0 AS run_attempted_autocommit
  FROM time_series.bgw_job_stat WHERE job_id = :jid_txn;
SELECT delete_job(:jid_txn);

\set ON_ERROR_STOP 1

-- ============================================================
-- RUN-03: mark_start INSERT path is idempotent
--
-- Race scenario: bgw_job_stat row pre-exists (or is missing) when
-- mark_start runs.  Without ON CONFLICT, the fallback INSERT would
-- PK-violate and crash the worker.  Two variants:
--   (a) stat row missing → mark_start UPDATE-first probe finds 0
--       rows, falls back to INSERT (the path under test).
--   (b) stat row pre-exists → mark_start UPDATE updates it; if a
--       race deleted the row between probe and INSERT, the
--       ON CONFLICT DO NOTHING fallback covers that too.
-- ============================================================
\echo '=== RUN-03: mark_start INSERT no longer PK-violates on race ==='
SELECT add_job('noop_job'::regproc, '1 hour'::interval) AS jid_race \gset

-- (a) Stat row missing → INSERT branch fires.
DELETE FROM time_series.bgw_job_stat WHERE job_id = :jid_race;
CALL time_series.run_job(:jid_race);
SELECT total_runs > 0 AS first_run_ok
  FROM time_series.bgw_job_stat WHERE job_id = :jid_race;

-- (b) Stat row exists with -infinity next_start (simulates a racing committer).
DELETE FROM time_series.bgw_job_stat WHERE job_id = :jid_race;
INSERT INTO time_series.bgw_job_stat (job_id, next_start)
VALUES (:jid_race, '-infinity');
CALL time_series.run_job(:jid_race);
SELECT total_runs > 0 AS second_run_ok
  FROM time_series.bgw_job_stat WHERE job_id = :jid_race;

SELECT delete_job(:jid_race);

-- ============================================================
-- RETENTION-A..G: built-in bgw_job_stat_history retention task
--   id=1, registered at extension install, no add_*/remove_* helper
--   — alter_job(1, ...) tunes it.  Mirrors upstream
--   sql/job_stat_history_log_retention.sql.
-- ============================================================
\echo '=== RETENTION-A: built-in retention job present at install ==='
SELECT id, proc_schema, proc_name, application_name,
       schedule_interval, max_retries, scheduled, fixed_schedule,
       config->>'drop_after' AS drop_after,
       check_schema, check_name
  FROM time_series.bgw_job WHERE id = 1;

\echo '=== RETENTION-B: nextval() does not collide with built-in id=1 ==='
SELECT nextval('time_series.bgw_job_id_seq') > 1 AS seq_advanced;

\echo '=== RETENTION-C: check function rejects NULL config ==='
\set ON_ERROR_STOP 0
SELECT time_series.policy_job_stat_history_retention_check(NULL);
\set ON_ERROR_STOP 1

\echo '=== RETENTION-D: check function rejects missing drop_after ==='
\set ON_ERROR_STOP 0
SELECT time_series.policy_job_stat_history_retention_check('{}'::jsonb);
\set ON_ERROR_STOP 1

\echo '=== RETENTION-E: check function accepts valid config ==='
SELECT time_series.policy_job_stat_history_retention_check(
       '{"drop_after": "30 days"}'::jsonb);

\echo '=== RETENTION-F: policy function actually purges old rows ==='
-- Insert two stat history rows tagged with pid=12345 to filter out any
-- rows the BGW scheduler may have written for job_id=1 in the meantime
-- (the built-in retention job is scheduled and may have fired once).
INSERT INTO time_series.bgw_job_stat_history
       (job_id, pid, execution_start, execution_finish, succeeded, data)
VALUES (1, 12345, now() - '60 days'::interval, now() - '60 days'::interval,
        true, '{}'::jsonb);
INSERT INTO time_series.bgw_job_stat_history
       (job_id, pid, execution_start, execution_finish, succeeded, data)
VALUES (1, 12345, now() - '1 day'::interval, now() - '1 day'::interval,
        true, '{}'::jsonb);

SELECT count(*) AS rows_before FROM time_series.bgw_job_stat_history
 WHERE pid = 12345;

SELECT time_series.policy_job_stat_history_retention(1,
       '{"drop_after": "30 days"}'::jsonb) >= 1 AS at_least_one_deleted;

SELECT count(*) AS rows_after FROM time_series.bgw_job_stat_history
 WHERE pid = 12345;

\echo '=== RETENTION-G: alter_job tunes drop_after ==='
SELECT (alter_job(1, config => '{"drop_after":"7 days"}'::jsonb)).config;
SELECT config->>'drop_after' AS drop_after FROM time_series.bgw_job WHERE id = 1;

\echo '=== RETENTION-G2: alter_job changes fixed_schedule / initial_start / timezone ==='
-- These three params mirror upstream alter_job: a policy can re-anchor
-- its schedule after creation, not only at add time.  Flip fixed_schedule,
-- set an explicit anchor + timezone, and confirm the row + the re-anchored
-- next_start.
-- Ensure job 1 has a bgw_job_stat row so the next_start re-anchor below
-- is observable (this fixture job was inserted without one).
INSERT INTO time_series.bgw_job_stat (job_id, next_start)
VALUES (1, now()) ON CONFLICT (job_id) DO NOTHING;
SELECT (alter_job(1,
        fixed_schedule => false,
        initial_start  => '2024-01-01 00:00:00+00'::timestamptz,
        timezone       => 'UTC')).id AS altered_jid;
SELECT fixed_schedule, initial_start, timezone
  FROM time_series.bgw_job WHERE id = 1;
-- initial_start (no explicit next_start) re-anchors next_start.
SELECT next_start = '2024-01-01 00:00:00+00'::timestamptz AS next_start_reanchored
  FROM time_series.bgw_job_stat WHERE job_id = 1;

\echo '=== RETENTION-G3: invalid timezone rejected ==='
\set ON_ERROR_STOP 0
SELECT alter_job(1, timezone => 'Not/AZone');
\set ON_ERROR_STOP 1

\echo '=== RETENTION-H: ghost rows (NULL execution_finish) get reclaimed ==='
-- A "ghost row" is a bgw_job_stat_history row where mark_start ran
-- (execution_start written) but mark_end never did (execution_finish
-- stays NULL forever).  Diagnostic of: worker SIGKILL'd, coordinator
-- restart mid-execution, OOM-killer fired.
--
-- Without explicit handling, retention WHERE execution_finish < cutoff
-- skips these rows because NULL < anything is NULL → never deleted.
-- Long-stability runs accumulate one ghost per crash forever.
--
-- Verify:
--   (a) job_history.is_crashed correctly flags NULL-execution_finish rows.
--   (b) job_errors view also surfaces them (succeeded IS NOT TRUE picks up NULL).
--   (c) policy_job_stat_history_retention DELETEs the old ghost; a fresh
--       ghost survives.
INSERT INTO time_series.bgw_job_stat_history
       (job_id, pid, execution_start, execution_finish, succeeded, data)
VALUES (1, 99001, now() - '2 minutes'::interval, NULL, NULL, NULL),
       (1, 99002, now() - '60 days'::interval,   NULL, NULL, NULL);

-- (a) Both rows show up as is_crashed in job_history.
SELECT pid, is_crashed, succeeded
  FROM time_series.job_history
 WHERE pid IN (99001, 99002)
 ORDER BY pid;

-- (b) Both rows surface in job_errors.
SELECT pid, is_crashed, sqlerrcode, err_message
  FROM time_series.job_errors
 WHERE pid IN (99001, 99002)
 ORDER BY pid;

-- (c) Retention with drop_after = 1 day deletes the 60-day-old ghost,
--     keeps the 2-minute-old one.
SELECT time_series.policy_job_stat_history_retention(1,
       '{"drop_after": "1 day"}'::jsonb) >= 1 AS at_least_one_deleted;

SELECT pid, (execution_finish IS NULL) AS still_ghost
  FROM time_series.bgw_job_stat_history
 WHERE pid IN (99001, 99002)
 ORDER BY pid;

DELETE FROM time_series.bgw_job_stat_history WHERE pid IN (99001, 99002);

-- ============================================================
-- HIST-01..03: bgw_job_stat_history audit log
--   Every successful run_job appends a row; failures are recorded
--   with succeeded=false.  Uses tiny noop / failing procedures
--   registered via add_job and fired by run_job; bgw_run_job
--   dispatches generically on bgw_job.proc_name.
-- ============================================================
CREATE PROCEDURE hist_noop(jid int, cfg jsonb) LANGUAGE plpgsql AS $$
BEGIN END;
$$;
CREATE PROCEDURE hist_fail(jid int, cfg jsonb) LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'intentional failure for HIST-03';
END$$;

\echo '=== HIST-01: every run_job appends a history row ==='
SELECT add_job('hist_noop'::regproc, '1 hour'::interval) AS hist_jid \gset

TRUNCATE time_series.bgw_job_stat_history;
SET client_min_messages TO warning;
CALL time_series.run_job(:hist_jid);
RESET client_min_messages;

SELECT count(*) AS history_rows,
       bool_and(succeeded) AS all_succeeded,
       bool_and(execution_finish > execution_start) AS finish_after_start,
       bool_and(data ? 'job') AS data_has_job_key
  FROM time_series.bgw_job_stat_history WHERE job_id = :hist_jid;

\echo '=== HIST-02: job_history view exposes structured columns ==='
SELECT count(*) AS rows_in_view,
       bool_and(proc_name = 'hist_noop') AS proc_matches,
       bool_and(duration > '0'::interval) AS has_duration
  FROM time_series.job_history WHERE job_id = :hist_jid;

\echo '=== HIST-03: failures are recorded with succeeded=false ==='
SELECT add_job('hist_fail'::regproc, '1 hour'::interval) AS fail_jid \gset

-- Disable BGW scheduling immediately.  add_job() defaults next_start
-- to now(), so the real scheduler will fork a worker for fail_jid on
-- its next ~5s tick.  That worker's mark_start INSERT commits in its
-- own txn (separate from our sync CALL below) and produces a history
-- row -- racing the synchronous-failure invariant this test asserts
-- (0 rows because the in-txn INSERT gets rolled back).  Pausing the
-- job here removes the race deterministically.
SELECT (alter_job(:fail_jid, scheduled => false)).id IS NOT NULL
       AS fail_jid_paused_for_sync_test;

TRUNCATE time_series.bgw_job_stat_history;
-- Suppress WARNINGs: run_job emits "BGW job N mark_end: invalid
-- last_start" on failure (transaction abort rolls back mark_start's
-- bgw_job_stat update, so last_start reverts to -infinity).  The
-- warning's now=<microseconds> field is non-deterministic.
SET client_min_messages TO error;
CALL time_series.run_job(:fail_jid);
RESET client_min_messages;

SELECT count(*) AS failure_rows,
       bool_and(NOT succeeded) AS all_failed
  FROM time_series.bgw_job_stat_history WHERE job_id = :fail_jid;

SELECT delete_job(:hist_jid);
SELECT delete_job(:fail_jid);
DROP PROCEDURE hist_noop(int, jsonb);
DROP PROCEDURE hist_fail(int, jsonb);

-- ============================================================
-- Cleanup
-- ============================================================
-- Quiet the retention scheduler before DROP EXTENSION.  Without this,
-- the final DROP EXTENSION can deadlock against an in-flight BGW
-- scheduler tick that reads bgw_job_stat_history while we hold the
-- exclusive lock to drop it.  Observed flake rate ~20% without the
-- pause; goes away once the scheduler stops firing on id=1.
SELECT (alter_job(1, scheduled => false)).id IS NOT NULL AS retention_paused;

SELECT delete_job(:jid_ok);

REVOKE ALL ON FUNCTION time_series.delete_job(int) FROM bgw_job_other;
REVOKE ALL ON time_series.bgw_job FROM bgw_job_other;
REVOKE ALL ON SCHEMA time_series FROM bgw_job_other;
DROP ROLE bgw_job_other;
DROP FUNCTION my_check(jsonb);
DROP FUNCTION bad_sig_one(text);
DROP FUNCTION bad_ret(int, jsonb);
DROP PROCEDURE noop_job(int, jsonb);
DROP EXTENSION time_series CASCADE;
