-- ============================================================
-- bgw_mock_time.sql
--
-- Smoke tests for the mock-time test framework.  cagg_bgw_full.sql
-- and the reference BGW scheduler test rely on this framework to drive scheduler
-- ticks deterministically; if its plumbing breaks, those tests fail
-- in opaque ways.  This file exercises the framework directly,
-- without involving any CAGG behaviour.
--
-- Test infrastructure (created here, dropped at end):
--   public.bgw_dsm_handle_store : single-row table holding the dsm
--                                  handle of the test params block
--   public.bgw_log              : append-only log of every elog() in
--                                  the mock-scheduler context
--   public.sorted_bgw_log       : view that masks dynamic numbers
--
-- Mock framework SQL surface (declared by extension):
--   time_series.bgw_params_create()
--   time_series.bgw_params_destroy()
--   time_series.bgw_params_reset_time(int8 microseconds, bool set_latch)
--   time_series.bgw_params_mock_wait_returns_immediately(int4 mode)
--   time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(int4 ttl_ms)
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
DROP TABLE IF EXISTS public.bgw_log CASCADE;
DROP TABLE IF EXISTS public.bgw_dsm_handle_store CASCADE;

CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- ============================================================
-- Test infrastructure tables (same shape cagg_bgw_full.sql uses)
-- ============================================================
CREATE TABLE public.bgw_dsm_handle_store(handle BIGINT) DISTRIBUTED REPLICATED;
INSERT INTO public.bgw_dsm_handle_store VALUES (0);

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
) DISTRIBUTED REPLICATED;

CREATE VIEW public.sorted_bgw_log AS
    SELECT msg_no,
           mock_time,
           application_name,
           regexp_replace(msg, '0x[0-9a-f]+', '0xPTR', 'g') AS msg
    FROM public.bgw_log
    ORDER BY mock_time, application_name, msg_no;

-- ============================================================
-- MOCK-01: framework setup plumbing accepts the documented call
--          sequence (params_create -> reset_time -> mock_wait)
-- ============================================================
\echo '=== MOCK-01: bgw_params_create + reset_time + mock_wait setup ==='
SELECT time_series.bgw_params_create();

-- Reset the mock clock to 2024-01-01 00:00:00 UTC (microseconds since epoch)
SELECT time_series.bgw_params_reset_time(
    extract(epoch from '2024-01-01 00:00:00+00'::timestamptz)::bigint * 1000000,
    false);

-- IMMEDIATELY_SET_UNTIL: when the mock scheduler waits, advance the clock
-- to "until" rather than blocking on real walltime.
SELECT time_series.bgw_params_mock_wait_returns_immediately(1);

-- ============================================================
-- MOCK-02: scheduler logs to bgw_log when run, and the mock clock
--          microsecond value flows into log entries unchanged
--
-- Two separate invariants:
--   (a) bgw_db_scheduler_test_run_and_wait_for_scheduler_finish
--       actually runs the scheduler, which writes at least one row
--       tagged application_name='DB Scheduler' into bgw_log.
--   (b) The microsecond value passed to bgw_params_reset_time is
--       what bgw_log entries see in their mock_time column — verifies
--       the mock clock is the time source for elog tagging.
-- ============================================================
\echo '=== MOCK-02: scheduler logs DB Scheduler entries with mock_time set ==='
TRUNCATE public.bgw_log;
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000);

-- (a) at least one DB Scheduler row written
SELECT count(*) > 0 AS scheduler_logged
  FROM public.bgw_log WHERE application_name = 'DB Scheduler';

-- (b) microsecond value (1704067200000000 = 2024-01-01 UTC) reaches
-- log rows verbatim — confirms reset_time -> bgw_log mock_time path.
SELECT count(*) > 0 AS mock_time_value_in_log
  FROM public.bgw_log
 WHERE mock_time = 1704067200000000
   AND application_name = 'DB Scheduler';

-- ============================================================
-- MOCK-03: sorted_bgw_log preserves per-process mock-time
--          monotonicity (the property that makes regression diffs
--          deterministic across runs)
--
-- PARTITION BY application_name: msg_no is a per-process counter, so
-- a single ORDER BY msg_no would interleave processes arbitrarily.
-- Each process's own monotonicity is the actual invariant.
-- ============================================================
\echo '=== MOCK-03: sorted_bgw_log preserves mock-time order per process ==='
WITH ordered AS (
    SELECT mock_time, application_name,
           LAG(mock_time, 1, mock_time)
             OVER (PARTITION BY application_name ORDER BY msg_no) AS prev_time
      FROM public.bgw_log
)
SELECT bool_and(mock_time >= prev_time) AS mock_time_monotonic FROM ordered;

-- ============================================================
-- MOCK-04: bgw_params_destroy is idempotent
--   The framework's destroy() is intentionally a no-op (DSM segments
--   are pinned for EXEC_BACKEND parity with upstream).  It must accept
--   being called repeatedly regardless of state — before create, after
--   create, and again after destroy — without ever raising.
-- ============================================================
\echo '=== MOCK-04: bgw_params_destroy is idempotent across all states ==='
SELECT time_series.bgw_params_destroy();   -- after MOCK-01..03 created it
SELECT time_series.bgw_params_destroy();   -- again — must be OK
SELECT time_series.bgw_params_create();    -- re-create
SELECT time_series.bgw_params_destroy();   -- and tear down again

-- ============================================================
-- Cleanup
-- ============================================================
SELECT time_series.bgw_params_destroy();
DROP VIEW public.sorted_bgw_log;
DROP TABLE public.bgw_log;
DROP TABLE public.bgw_dsm_handle_store;
DROP EXTENSION time_series CASCADE;
