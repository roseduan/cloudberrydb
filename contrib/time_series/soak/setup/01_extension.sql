-- scripts/soak/setup/01_extension.sql
--
-- Idempotent: drop & recreate to start from a clean slate.  Tunes the
-- built-in retention job (id=1) for the long-stability environment —
-- 1 hour schedule + 7 days drop_after.
--
-- Invoked by soak.sh once per DB in $SOAK_DBS (default
-- soak_test_a + soak_test_b).  See doc/feature/cagg/v1/cagg_soak_test.md
-- §6.3 for the full setup flow.

SET optimizer = off;
SET timezone  = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

SET search_path TO public, time_series;

-- Aggressive retention so accumulated job-stat history doesn't pile up
-- across days.  Values come from conf/soak_params.sh
-- (SOAK_JOBLOG_SCHEDULE / SOAK_JOBLOG_DROP_AFTER) via psql -v;
-- the \if blocks supply the same defaults for direct psql -f use.
\if :{?joblog_schedule}
\else
  \set joblog_schedule '1 hour'
\endif
\if :{?joblog_drop_after}
\else
  \set joblog_drop_after '7 days'
\endif

SELECT time_series.alter_job(
    1,
    schedule_interval => (:'joblog_schedule')::interval,
    config            => jsonb_build_object('drop_after', :'joblog_drop_after'));
