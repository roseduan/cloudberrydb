#!/bin/bash
# monitor/bgw_scheduler_health.sh
# SOAK-LOOP: scope=per-db interval=60 interval_var=SOAK_BGW_SCHED_HEALTH_EVERY order=42
#
# Samples the BGW scheduler's dispatch health.  Complements
# refresh_perf / compress_perf, which record how long a job took
# ONCE it was dispatched — they say nothing about whether the
# scheduler is dispatching on time.
#
# WHY THIS EXISTS:
#   refresh_perf_scrape can report `p95=4s ✓ OK` while the scheduler
#   is stalling: the sampled jobs are on-time from their own start
#   moment, but next_start slipped 5+ minutes before the launcher
#   actually spawned them.  memory `bgw-launcher-test04-flake`
#   notes CBDB's launcher has known races.  Without this monitor,
#   scheduler dispatch delay is invisible.
#
# WHAT IT SAMPLES:
#   For each active policy job in time_series.bgw_job with
#   scheduled=true:
#     overdue_s = GREATEST(0, now() - next_start)
#   Aggregate per sample: overdue_jobs (count where overdue > warn_sec)
#   and worst_overdue_s.  One CSV row per (ts, db) — cheap.
#
# CSV columns:
#   ts, db, jobs_scheduled, jobs_overdue, worst_overdue_s, worst_job_name
#
# Usage (invoked by lib/loops.sh):
#   bgw_scheduler_health.sh <db> <results_dir>       one sample pass

set -uo pipefail

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: bgw_scheduler_health.sh <db> <results_dir>}"
RESULTS="${2:?usage: bgw_scheduler_health.sh <db> <results_dir>}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"
WARN_SEC="${SOAK_BGW_OVERDUE_WARN_SEC:-60}"

CSV="$RESULTS/data/bgw_scheduler_health.csv"
ERR="$RESULTS/errors/bgw_scheduler_health.err"

if [[ ! -f "$CSV" ]]; then
  echo "ts,db,jobs_scheduled,jobs_overdue,worst_overdue_s,worst_job_name" > "$CSV"
fi

TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# Pull scheduled=true jobs and compute per-job overdue = max(0, now -
# next_start).  Ignore jobs whose next_start is in the future (overdue=0).
#
# "Behind" is SCHEDULE-RELATIVE, not a flat threshold: a job counts as
# overdue only when it has slipped past its OWN schedule_interval (plus a
# small WARN_SEC grace for dispatch jitter) — i.e. it missed a full
# cycle.  A 1-hour housekeeping job (job_stat_history_retention) that is
# 214 s "late" is NOT behind; a 60 s cv_1min refresh that is 90 s late
# IS.  The old flat WARN_SEC cutoff wrongly flagged the slow-cadence
# jobs during a short/aggressive run (2026-07-08 chaos smoke).
PGOPTIONS='--client-min-messages=warning' \
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
     -At -F ',' -X -v ON_ERROR_STOP=1 \
     -c "
WITH j AS (
  -- next_start lives on bgw_job_stat, not bgw_job.  Skip rows with
  -- next_start = -infinity (never dispatched) so brand-new jobs do not
  -- appear as decades overdue.
  SELECT
    b.id,
    b.proc_name,
    COALESCE(b.config->>'cagg_name', b.config->>'table_name',
             b.config->>'table_oid', '?') AS target,
    GREATEST(0, EXTRACT(epoch FROM (now() - s.next_start)))::bigint AS overdue_s,
    EXTRACT(epoch FROM b.schedule_interval)::bigint AS sched_s
  FROM time_series.bgw_job b
  JOIN time_series.bgw_job_stat s ON s.job_id = b.id
  WHERE b.scheduled = true
    AND s.next_start > '-infinity'::timestamptz
),
agg AS (
  SELECT
    count(*) AS jobs_scheduled,
    -- overdue = slipped past its own schedule_interval + WARN_SEC grace
    count(*) FILTER (WHERE overdue_s > sched_s + ${WARN_SEC}) AS jobs_overdue,
    COALESCE(max(overdue_s) FILTER (WHERE overdue_s > sched_s + ${WARN_SEC}), 0) AS worst_overdue_s
  FROM j
),
worst AS (
  -- worst = the most-behind job that is ACTUALLY behind (past its own
  -- schedule + grace); '-' if none, so worst_job_name never fingers a
  -- slow-cadence job that is merely mid-cycle.
  SELECT proc_name || ':' || target AS worst_job_name
  FROM j
  WHERE overdue_s > sched_s + ${WARN_SEC}
  ORDER BY overdue_s DESC
  LIMIT 1
)
SELECT '$TS',
       '$DB',
       agg.jobs_scheduled,
       agg.jobs_overdue,
       agg.worst_overdue_s,
       COALESCE(worst.worst_job_name, '-')
  FROM agg LEFT JOIN worst ON true;
" 2>>"$ERR" >> "$CSV"
