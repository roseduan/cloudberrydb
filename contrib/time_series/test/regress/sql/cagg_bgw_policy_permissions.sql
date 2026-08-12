-- ============================================================
-- cagg_bgw_policy_permissions.sql
--
-- Multi-user permission semantics for the CAGG policy API.
-- All cases create non-superuser roles, assign ownership of
-- a policy to one role, and verify that the other roles are
-- correctly accepted or rejected by:
--
--   * BGW-VIEW-03: time_series.job_history is owner-filtered;
--     non-owners see zero rows.
--   * PERM-01: refresh failure path records error in stat +
--     bgw_job_stat_history when the source object becomes
--     inaccessible (CHECK(false) injection).
--   * PERM-OWNER-01..07: alter_job, run_job, remove_*_policy,
--     and add_*_policy enforce pg_has_role(...,'MEMBER') on
--     bgw_job.owner; superuser bypasses.
--   * PERM-MOCK-01..03: mock-time test functions
--     (bgw_db_scheduler_test_run, bgw_test_job_sleep,
--     bgw_params_create) reject non-superuser callers.
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
-- Defensive: prior test runs may leave behind source tables and auto-named
-- materialization tables (which are NOT cleaned by DROP EXTENSION CASCADE
-- for views that were dropped via DROP MATERIALIZED VIEW).
DROP TABLE IF EXISTS metrics_tstz CASCADE;

CREATE EXTENSION time_series;
SET search_path TO public, time_series;
-- ============================================================
-- Setup: one hypertable + one CAGG.  The whole file tests permission
-- semantics (owner / non-owner / group / superuser) on catalog rows,
-- not data correctness — so a single source table is enough.  Data is
-- placed relative to now() so refresh windows (now()-Xd, now()] cover
-- it whenever PERM-01 / PERM-OWNER-04 actually run a refresh.
-- ============================================================
CREATE TABLE metrics_tstz (
    time        TIMESTAMPTZ       NOT NULL,
    tags_id     INT               NOT NULL,
    temperature DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (tags_id);

-- 48 hours of data ending ~2 hours before now(), so refresh windows that
-- exclude the most-recent few minutes still cover the last bucket.
INSERT INTO metrics_tstz
SELECT date_trunc('hour', now()) - ((48 - h) * interval '1 hour')
       + (m * interval '5 minutes'),
       (h % 5) + 1,
       20.0 + h * 0.1 + m * 0.01
FROM generate_series(1, 48) h,
     generate_series(0, 5)  m;

CREATE MATERIALIZED VIEW cv_tstz
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id,
         count(*) AS cnt, avg(temperature) AS avg_temp
  FROM metrics_tstz GROUP BY bucket, tags_id;
-- ============================================================
-- BGW-VIEW-03: job_history owner filter
-- ============================================================
-- ============================================================
-- BGW-VIEW-03: owner filter mirrors upstream split
--
-- upstream design: job_stats / jobs / continuous_aggregates views are
-- public (operational metadata), but job_history / job_errors filter
-- by job owner OR database owner.  V1 mirrors that:
--
--   cagg_policy_stats (≈ upstream job_stats) → no owner filter
--   job_history                          → filtered by job owner
--                                            OR database owner
--                                            + WITH (security_barrier)
--
-- Verifies:
--   - bob with SELECT grant sees policy metadata in cagg_policy_stats
--     (intentional, matches upstream)
--   - bob with SELECT grant sees ZERO rows in job_history
--   - alice (owner) sees her own job_history rows
--   - the database owner (gpadmin in this test environment) sees
--     all rows (covered implicitly: tests run as superuser/datdba
--     and add_continuous_aggregate_policy populates the bgw_job)
-- ============================================================
\echo '=== BGW-VIEW-03: job_history filtered by owner + db-owner ==='
DROP ROLE IF EXISTS view_alice;
DROP ROLE IF EXISTS view_bob;
CREATE ROLE view_alice;
CREATE ROLE view_bob;
GRANT USAGE ON SCHEMA time_series TO view_alice, view_bob;
GRANT SELECT ON time_series.cagg_policy_stats TO view_alice, view_bob;
GRANT SELECT ON time_series.bgw_job_stat       TO view_alice, view_bob;
GRANT SELECT ON time_series.bgw_job_stat_history TO view_alice, view_bob;
GRANT SELECT ON time_series.job_history        TO view_alice, view_bob;

SELECT add_continuous_aggregate_policy('cv_tstz',
       '2 days'::interval, '0 minutes'::interval, '10 seconds'::interval)
  AS jid_alice \gset

-- Mark this policy as alice-owned
UPDATE time_series.bgw_job SET owner = 'view_alice'::regrole WHERE id = :jid_alice;

-- Generate one history row by running the job synchronously.
-- run_job is invoked as the test owner (the database owner),
-- so the row's job_id refers to alice's job and the row is
-- visible to alice (owner) and to the db owner (test runner).
SET client_min_messages TO warning;
CALL time_series.run_job(:jid_alice);
RESET client_min_messages;

\echo --- cagg_policy_stats: NOT filtered, bob sees alice''s policy ---
\echo --- (intentional, matches upstream job_stats behavior) ---
SET ROLE view_bob;
SELECT count(*) AS bob_visible_policies FROM time_series.cagg_policy_stats;
RESET ROLE;

\echo --- job_history: filtered, bob sees ZERO rows ---
SET ROLE view_bob;
SELECT count(*) AS bob_visible_history FROM time_series.job_history;
RESET ROLE;

\echo --- job_history: alice (owner) sees her rows ---
SET ROLE view_alice;
SELECT count(*) >= 1 AS alice_sees_her_history FROM time_series.job_history;
RESET ROLE;

-- Cleanup
SELECT remove_continuous_aggregate_policy('cv_tstz');
REVOKE ALL ON SCHEMA time_series FROM view_alice, view_bob;
REVOKE ALL ON time_series.cagg_policy_stats FROM view_alice, view_bob;
REVOKE ALL ON time_series.bgw_job_stat FROM view_alice, view_bob;
REVOKE ALL ON time_series.bgw_job_stat_history FROM view_alice, view_bob;
REVOKE ALL ON time_series.job_history FROM view_alice, view_bob;
DROP ROLE view_alice;
-- ============================================================
-- Section 16: cross-user permission failure on BGW refresh
--
-- Borrowed from upstream  L292-315 ("create a view with a
-- function that it has no permission to execute") and L322-384 ("user
-- is the non-owner of the raw table").  V1 doesn't have the multi-user
-- ROLE infrastructure those tests use; we exercise the failure-recording
-- path with a missing source table, which routes through the same
-- PG_CATCH + mark_end branch as a real permission denied error.
-- ============================================================

\echo '=== PERM-01: refresh records failure when source table is missing ==='
-- V1 doesn't support running a policy as a non-superuser without granting
-- INSERT on every internal time_series catalog (continuous_agg,
-- cagg_invalidations, etc.).  The point of the original upstream test was to
-- verify the failure path of the policy worker — total_failures increments
-- when the underlying refresh raises.  We reproduce that by creating a
-- CAGG, attaching a policy, then renaming the source table out from under
-- it: the next run_job will fail and mark_end records the failure.
CREATE MATERIALIZED VIEW cv_perm WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*) AS c
    FROM metrics_tstz GROUP BY bucket, tags_id;
SELECT add_continuous_aggregate_policy('cv_perm',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval) AS jid_perm \gset

-- Inject a guaranteed failure: attach a CHECK(false) constraint to the
-- CAGG's materialization table so the refresh INSERT raises.
DO $$
DECLARE qual text;
BEGIN
    SELECT format('%I.%I', mat_table_schema, mat_table_name) INTO qual
      FROM time_series.continuous_agg WHERE user_view_name = 'cv_perm';
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT bgw_perm_check CHECK (false)', qual);
END$$;

\echo 'PERM-01 step 2: run_job with missing source records failure in stat'
\set VERBOSITY terse
\set ON_ERROR_STOP 0
CALL time_series.run_job(:jid_perm);
\set ON_ERROR_STOP 1
\set VERBOSITY default

-- mark_start ran (total_runs > 0); the refresh INSERT violated the CHECK
-- and aborted the inner subtransaction; PG_CATCH rolled the subtransaction
-- back, then mark_end recorded the failure in the now-healthy outer txn.
SELECT total_runs > 0           AS run_attempted,
       total_failures > 0       AS failure_recorded,
       last_run_success = false AS last_ok_should_be_false
  FROM time_series.bgw_job_stat WHERE job_id = :jid_perm;

-- Clean up.
SELECT remove_continuous_aggregate_policy('cv_perm');
DROP VIEW cv_perm;
-- ============================================================
-- PERM-OWNER-01..05: owner-based permission checks on alter_job /
-- run_job / remove_continuous_aggregate_policy.
--
-- bgw_job.owner is set to current_role at INSERT time.  Each of the
-- three management entry points must reject callers who are not a
-- member of that role.
-- pg_has_role(..., 'MEMBER') makes superuser and the owner role
-- itself pass; everyone else is rejected with INSUFFICIENT_PRIVILEGE.
-- ============================================================

-- Two unprivileged roles, no superuser bit.  Superuser creates the
-- CAGG + policy on behalf of ts_owner_a (CAGG creation requires
-- CREATE on time_series schema, which we don't want to grant to a
-- random user); we then UPDATE bgw_job.owner to make ts_owner_a the
-- legitimate owner.  ts_owner_b will try to alter / run / remove it.
SET client_min_messages TO warning;
DROP ROLE IF EXISTS ts_owner_a;
DROP ROLE IF EXISTS ts_owner_b;
RESET client_min_messages;
CREATE ROLE ts_owner_a LOGIN;
CREATE ROLE ts_owner_b LOGIN;

-- Both roles need to be able to read time_series catalog tables and
-- call the management functions.
GRANT USAGE ON SCHEMA time_series TO ts_owner_a, ts_owner_b;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA time_series TO ts_owner_a, ts_owner_b;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA time_series TO ts_owner_a, ts_owner_b;

CREATE MATERIALIZED VIEW cv_owner WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, tags_id, count(*) AS c
    FROM metrics_tstz GROUP BY bucket, tags_id;
SELECT add_continuous_aggregate_policy('cv_owner',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval)
   AS jid_owner \gset

-- Reassign the policy ownership to ts_owner_a so the perm checks
-- exercise a real cross-user attempt.
UPDATE time_series.bgw_job SET owner = 'ts_owner_a'::regrole
  WHERE id = :jid_owner;

\echo '=== PERM-OWNER-01: non-owner alter_job → INSUFFICIENT_PRIVILEGE ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT alter_job(:jid_owner, schedule_interval => '1 minute'::interval);
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '=== PERM-OWNER-02: non-owner run_job → INSUFFICIENT_PRIVILEGE ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
CALL time_series.run_job(:jid_owner);
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '=== PERM-OWNER-03: non-owner remove_continuous_aggregate_policy → INSUFFICIENT_PRIVILEGE ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT remove_continuous_aggregate_policy('cv_owner');
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '=== PERM-OWNER-04: owner can alter / remove their own policy ==='
SET ROLE ts_owner_a;
SELECT (alter_job(:jid_owner, schedule_interval => '30 seconds'::interval)).id
       = :jid_owner AS owner_alter_returns_same_jid;
SELECT schedule_interval = '30 seconds'::interval AS interval_actually_changed
  FROM time_series.bgw_job WHERE id = :jid_owner;
SELECT remove_continuous_aggregate_policy('cv_owner');
SELECT count(*) = 0 AS policy_actually_removed
  FROM time_series.bgw_job WHERE id = :jid_owner;
RESET ROLE;

\echo '=== PERM-OWNER-05: superuser can manage any policy ==='
-- Re-create the policy as superuser, owned by ts_owner_a.
SELECT add_continuous_aggregate_policy('cv_owner',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval)
   AS jid_owner_2 \gset
UPDATE time_series.bgw_job SET owner = 'ts_owner_a'::regrole
  WHERE id = :jid_owner_2;
SELECT (alter_job(:jid_owner_2, scheduled => false)).scheduled = false
   AS super_can_alter;
SELECT remove_continuous_aggregate_policy('cv_owner');
SELECT count(*) = 0 AS super_can_remove
  FROM time_series.bgw_job WHERE id = :jid_owner_2;

-- ============================================================
-- PERM-OWNER-06/07: add_continuous_aggregate_policy owner check
--
-- Without an owner check, in a multi-tenant configuration where the
-- DBA has granted INSERT/UPDATE/DELETE on bgw_job to PUBLIC (a typical
-- "let users manage their own policies" setup), any user could squat
-- on another user's CAGG by adding a policy first — taking ownership
-- of the policy and blocking the real owner from adding their own
-- (one-policy-per-CAGG).  add_continuous_aggregate_policy now checks
-- pg_has_role on the CAGG view's owner, mirroring alter_job /
-- remove_continuous_aggregate_policy / bgw_job_permission_check.
-- ============================================================
\echo '=== PERM-OWNER-06: non-owner add_policy → INSUFFICIENT_PRIVILEGE ==='
-- Reassign view ownership so ts_owner_b's add becomes a real cross-user
-- attempt against ts_owner_a's CAGG.  (PERM-OWNER-05 removed the policy,
-- so the CAGG is policy-less and ready to receive a fresh add.)
ALTER VIEW cv_owner OWNER TO ts_owner_a;

SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('cv_owner',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval);
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '--- verify no policy was created ---'
SELECT count(*) = 0 AS no_squat_policy
  FROM time_series.bgw_job
 WHERE proc_name = 'policy_refresh_cagg';

\echo '=== PERM-OWNER-07: owner CAN add policy on their own cagg ==='
SET ROLE ts_owner_a;
SELECT add_continuous_aggregate_policy('cv_owner',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval) IS NOT NULL
   AS owner_can_add;
SELECT remove_continuous_aggregate_policy('cv_owner');
RESET ROLE;

-- ============================================================
-- PERM-OWNER-08: role group membership grants the same privilege
--   as direct ownership.
--
-- V1's alter_job / run_job / remove_continuous_aggregate_policy
-- all gate on pg_has_role(current_user, bgw_job.owner, 'MEMBER').
-- pg_has_role returns TRUE when:
--   1. current_user = owner (direct)         — covered by PERM-OWNER-04
--   2. current_user is superuser             — covered by PERM-OWNER-05
--   3. current_user was GRANTed owner role   — UNCOVERED until now
--
-- Real-world scenario:  DBA creates a `data_admin` group role and
-- GRANTs it to several team members.  A policy is owned by
-- `data_admin`.  Every team member must be able to alter/run/remove
-- that policy by virtue of group membership (case 3 above).
--
-- If a regression replaced the pg_has_role check with
-- `current_user = owner` (strict equality), cases 1 and 2 would
-- still pass and our existing tests wouldn't catch the break — but
-- production multi-user clusters would immediately stop working.
-- ============================================================
\echo '=== PERM-OWNER-08: group-role membership grants alter privilege ==='
-- Group role (NOLOGIN by default — used only as ACL holder).
CREATE ROLE ts_group_owner;
-- ts_owner_b becomes a MEMBER of ts_group_owner via GRANT.
GRANT ts_group_owner TO ts_owner_b;

-- Re-add a policy on cv_owner and reassign its owner to the GROUP role.
SELECT add_continuous_aggregate_policy('cv_owner',
       '7 days'::interval, '1 hour'::interval, '10 seconds'::interval)
   AS jid_group \gset
UPDATE time_series.bgw_job SET owner = 'ts_group_owner'::regrole
 WHERE id = :jid_group;

-- ts_owner_b is a member of ts_group_owner → alter must succeed.
SET ROLE ts_owner_b;
SELECT (alter_job(:jid_group,
                  schedule_interval => '20 seconds'::interval)).id
       IS NOT NULL AS member_can_alter_owned_by_group;
RESET ROLE;

-- ts_owner_a is NOT a member of ts_group_owner → alter must fail.
SET ROLE ts_owner_a;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT alter_job(:jid_group,
                 schedule_interval => '30 seconds'::interval);
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

-- Cleanup this case's policy + group role before final teardown.
SELECT remove_continuous_aggregate_policy('cv_owner');
REVOKE ts_group_owner FROM ts_owner_b;
DROP ROLE ts_group_owner;

DROP VIEW cv_owner;
-- ============================================================
-- PERM-MOCK-01..03: mock-time test functions are not callable by
-- ordinary users.
--
-- Regression for P2-5: the 11 mock-time SQL functions (defined in
-- time_series--1.0.sql, used only by the regression test suite to
-- drive a synthetic BGW scheduler) are REVOKE-d from PUBLIC at
-- extension install time.  Verify a few representative ones reject
-- non-superuser callers cleanly.
-- ============================================================
\echo '=== PERM-MOCK-01: bgw_db_scheduler_test_run rejected for ordinary user ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT time_series.bgw_db_scheduler_test_run(100);
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '=== PERM-MOCK-02: bgw_test_job_sleep rejected for ordinary user ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT time_series.bgw_test_job_sleep();
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

\echo '=== PERM-MOCK-03: bgw_params_create rejected for ordinary user ==='
SET ROLE ts_owner_b;
\set VERBOSITY terse
\set ON_ERROR_STOP 0
SELECT time_series.bgw_params_create();
\set ON_ERROR_STOP 1
\set VERBOSITY default
RESET ROLE;

-- Cleanup roles.  REVOKE before DROP to avoid "role X cannot be
-- dropped because some objects depend on it" surprise.
REVOKE ALL ON ALL TABLES IN SCHEMA time_series FROM ts_owner_a, ts_owner_b;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA time_series FROM ts_owner_a, ts_owner_b;
REVOKE USAGE ON SCHEMA time_series FROM ts_owner_a, ts_owner_b;
DROP ROLE ts_owner_a;
DROP ROLE ts_owner_b;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VIEW IF EXISTS cv_tstz;
DROP TABLE IF EXISTS metrics_tstz;
DROP EXTENSION time_series CASCADE;
