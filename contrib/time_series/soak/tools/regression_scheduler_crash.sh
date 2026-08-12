#!/bin/bash
# tools/regression_scheduler_crash.sh
#
# Regression test for the scheduler-crash race fixed in:
#   - scheduler.c on_failure_to_start_job  (PG_TRY guard)
#   - job_stat.c bgw_job_stat_set_next_start (ERROR → WARNING)
#
# THE BUG (24h soak, 12 self-triggered events, deterministically repro'd in
# ~44 seconds with ddl_churn=60s):
#
#   1. ddl_churn does DROP MATVIEW cv_scratch CASCADE → cascades a DELETE
#      of cv_scratch's bgw_job and bgw_job_stat rows.
#   2. Concurrently the scheduler is dispatching CAGG refreshes; when the
#      cluster's max_worker_processes slot is exhausted, bgw_job_start
#      returns NULL and the scheduler enters on_failure_to_start_job.
#   3. on_failure_to_start_job takes a share lock on bgw_job (PASSES — the
#      race partner hasn't committed yet), then calls set_next_start which
#      UPDATEs bgw_job_stat.
#   4. The race partner commits between the two operations.  Under Read
#      Committed the UPDATE re-reads → 0 rows → set_next_start raised
#      ERROR.  on_failure_to_start_job ran OUTSIDE any PG_TRY, so the
#      ERROR exited the scheduler process (exit code 1).  Postmaster then
#      reaped every refresh worker the scheduler owned (FATAL ... "due to
#      administrator command").  CAGG refresh stalled ~20 minutes per
#      crash event while the launcher respawned the scheduler.
#
# THIS REGRESSION:
#   - Stages the same conditions (medium-load soak + ddl_churn=60s).
#   - Asserts ZERO of the four bug signatures in pg_log over the run.
#   - Asserts the scheduler PIDs do NOT change (no respawns).
#   - Asserts the launcher PID does NOT change (no full-cluster restart).
#
# Pre-fix verification:  removing either fix and re-running shows
# "unable to find job statistics for job ..." + "scheduler ... exited
# with exit code 1" within 1-2 minutes of soak start (compared to the
# 44-second repro on cbdb-soak).
#
# Designed to fit beside the other soak regressions (view_correctness,
# watermark_lag) rather than the isolation2 framework
# — the bug requires a real scheduler dispatching real jobs concurrent
# with DDL churn; isolation2's two-session model can't stage it.
#
# Usage (wraps soak.sh with regression-specific overrides):
#   bash tools/regression_scheduler_crash.sh                # default 10m
#   SOAK_REGRESSION_DURATION=5m bash tools/regression_scheduler_crash.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOAK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SOAK_DIR/conf/soak_params.sh"

DURATION="${SOAK_REGRESSION_DURATION:-10m}"

# Find the postmaster log directory by asking psql for data_directory.
# Fall back to the cbdb-demo standard path.
DATA_DIR=$(psql -h "${SOAK_HOST:-localhost}" -p "${SOAK_PORT:-7000}" \
                -U "${SOAK_USER:-gpadmin}" -d postgres -At -X \
                -c "SHOW data_directory;" 2>/dev/null \
            || echo "")
LOG_DIR="${DATA_DIR:-/home/gpadmin/hashdata-lightning/gpAux/gpdemo/datadirs/qddir/demoDataDir-1}/log"

if [[ ! -d "$LOG_DIR" ]]; then
  echo "FATAL: pg_log dir not found at $LOG_DIR" >&2
  exit 2
fi

# Snapshot launcher + scheduler PIDs at start.  ANY change during the
# run means the bug fired: launcher unchanged but scheduler PID change =
# scheduler crashed and was respawned; launcher PID change = postmaster
# event (not our bug, but invalidates the test premise).
LAUNCHER_PID_BEFORE=$(pgrep -f "time_series launcher" | head -1)
SCHED_PIDS_BEFORE=$(pgrep -f "time_series scheduler" | sort | tr '\n' ' ')
if [[ -z "$LAUNCHER_PID_BEFORE" ]]; then
  echo "FATAL: time_series launcher not running — start cbdb first" >&2
  exit 2
fi

echo "[regression] launcher pid before = $LAUNCHER_PID_BEFORE"
echo "[regression] scheduler pids before = $SCHED_PIDS_BEFORE"
echo "[regression] starting focused soak (duration=$DURATION, ddl_churn=60s)"

START_TS=$(date -u +%s)

# Aggressive ddl_churn (60s vs the default 900s) is what crystallises the
# race window to deterministic timing.  --skip-setup reuses soak_test_a /
# soak_test_b so the bgw_job_stat accumulated state stays warm — that
# matters because the 24h soak showed first-trigger times shrink as
# state accumulates.  --chaos=0 keeps the test free of injected faults
# (so any FATAL we see is the bug, not a chaos handler).
SOAK_DDL_CHURN_EVERY=60 \
SOAK_LATE_ARRIVAL_EVERY=300 \
SOAK_DBS="soak_test_a soak_test_b" \
bash "$SOAK_DIR/soak.sh" \
  --duration="$DURATION" --scale=200 --skip-setup --chaos=0

END_TS=$(date -u +%s)
DURATION_SEC=$(( END_TS - START_TS ))
echo "[regression] soak finished after ${DURATION_SEC}s"

# Re-snapshot PIDs.  Same set + same values = no scheduler crash.
LAUNCHER_PID_AFTER=$(pgrep -f "time_series launcher" | head -1)
SCHED_PIDS_AFTER=$(pgrep -f "time_series scheduler" | sort | tr '\n' ' ')
echo "[regression] launcher pid after  = $LAUNCHER_PID_AFTER"
echo "[regression] scheduler pids after  = $SCHED_PIDS_AFTER"

# Scan every pg_log csv touched during the run for the four bug
# signatures.  -newermt is precise to the start-time clock; +1 sec
# fudge avoids edge-case file-rotation drops.
SCAN_TS=$(date -u -d "@$((START_TS - 1))" '+%Y-%m-%d %H:%M:%S')

FATAL_ERROR=$(find "$LOG_DIR" -name 'gpdb-*.csv' -newermt "$SCAN_TS" \
                -exec grep -l "unable to find job statistics for job" {} + 2>/dev/null \
              | xargs -r grep -hE "unable to find job statistics for job" 2>/dev/null \
              | wc -l)
FATAL_EXIT=$(find "$LOG_DIR" -name 'gpdb-*.csv' -newermt "$SCAN_TS" \
               -exec grep -l 'time_series scheduler' {} + 2>/dev/null \
             | xargs -r grep -hE 'time_series scheduler.*exited with exit code 1' 2>/dev/null \
             | wc -l)
# Post-fix signatures: not failures by themselves, but their presence
# confirms the fix paths actually fired (i.e. the race DID happen and
# was handled).  Useful diagnostic, not an assertion.
POST_WARN=$(find "$LOG_DIR" -name 'gpdb-*.csv' -newermt "$SCAN_TS" \
              -exec grep -l 'bgw_job_stat row for job' {} + 2>/dev/null \
            | xargs -r grep -hE 'bgw_job_stat row for job .* is missing' 2>/dev/null \
            | wc -l)
POST_LOG=$(find "$LOG_DIR" -name 'gpdb-*.csv' -newermt "$SCAN_TS" \
             -exec grep -l 'could not clean up after failed launch' {} + 2>/dev/null \
           | xargs -r grep -hE 'could not clean up after failed launch of job' 2>/dev/null \
           | wc -l)

echo
echo "[regression] ── bug signatures (must be 0) ─────────────────────"
echo "[regression]   ERROR \"unable to find job statistics for job N\"  : $FATAL_ERROR"
echo "[regression]   LOG  \"scheduler ... exited with exit code 1\"   : $FATAL_EXIT"
echo "[regression] ── fix-path signatures (may be > 0, diagnostic) ────"
echo "[regression]   WARNING \"bgw_job_stat row for job N is missing\"   : $POST_WARN"
echo "[regression]   LOG   \"could not clean up after failed launch ...\" : $POST_LOG"
echo

FAIL=0
if (( FATAL_ERROR > 0 )); then
  echo "[regression] 🔴 FAIL: set_next_start ERROR fired $FATAL_ERROR time(s) — fix #2 regression"
  FAIL=1
fi
if (( FATAL_EXIT > 0 )); then
  echo "[regression] 🔴 FAIL: scheduler exited $FATAL_EXIT time(s) — fix #1 regression"
  FAIL=1
fi
if [[ "$LAUNCHER_PID_BEFORE" != "$LAUNCHER_PID_AFTER" ]]; then
  echo "[regression] 🔴 FAIL: launcher PID changed $LAUNCHER_PID_BEFORE → $LAUNCHER_PID_AFTER (cluster instability)"
  FAIL=1
fi
if [[ "$SCHED_PIDS_BEFORE" != "$SCHED_PIDS_AFTER" ]]; then
  echo "[regression] 🔴 FAIL: scheduler PID set changed (scheduler crashed mid-run)"
  echo "[regression]   before: $SCHED_PIDS_BEFORE"
  echo "[regression]   after : $SCHED_PIDS_AFTER"
  FAIL=1
fi

if (( FAIL == 0 )); then
  echo "[regression] ✅ PASS — no scheduler crash, no missing-stat ERROR over ${DURATION_SEC}s"
  # Diagnostic only: did the race actually happen?  If POST_WARN + POST_LOG
  # are both 0, the run never reached the fix path either — that means
  # the load profile didn't stage the race, NOT that the fix is wrong.
  if (( POST_WARN == 0 && POST_LOG == 0 )); then
    echo "[regression] (note: race did not fire — fix path not exercised in this run)"
  else
    echo "[regression] (fix path exercised: $POST_WARN WARNING / $POST_LOG LOG)"
  fi
  exit 0
else
  exit 1
fi
