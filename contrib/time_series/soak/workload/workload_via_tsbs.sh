#!/bin/bash
# workload/workload_via_tsbs.sh
# SOAK-LOOP: scope=per-db mode=self order=15
#
# Continuous workload — every WORKLOAD_INTERVAL seconds, fork a fresh
# tsbs_generate_data → tsbs_load_timescaledb pipeline that produces
# WORKLOAD_INTERVAL seconds of synthetic CPU data and inserts it into
# the existing cpu/tags tables created by setup/02_seed_via_tsbs.sh.
#
# Why fork-per-batch instead of one long pipeline:
#   tsbs_generate_data does NOT support streaming (it expects a
#   bounded [start, end] window).  Repeated invocation with rolling
#   timestamps is the simplest way to produce continuous load.  The
#   100-200ms fork overhead is absorbed inside the 1-second budget.
#
# Why --create-metrics-table=false + --do-create-db=false:
#   tsbs_load_timescaledb defaults BOTH to true, which means each
#   invocation would DROP and re-CREATE the cpu table — destroying
#   all prior data including the 8-day seed.  These flags pin it to
#   "INSERT into existing tables" mode.
#
# Tunables (env vars):
#   SOAK_TSBS_BIN              binary dir (default: /home/gpadmin/tsbs_bin)
#   SOAK_WORKLOAD_SCALE        hosts (default: 400 — gives 400 rows/s/DB at interval=1s;
#                              see soak doc §6.4.3 for why 400 not 1000)
#   SOAK_WORKLOAD_INTERVAL     batch window seconds (default: 1)
#   SOAK_WORKLOAD_LOG_INTERVAL TSBS log interval (default: 1s — sample per host per second)
#   Connection: SOAK_HOST/PORT/USER/DB (set by soak.sh)
#
# Usage (invoked by lib/loops.sh; mode=self → runs forever):
#   SOAK_DB=<db> workload_via_tsbs.sh <results_dir>
#   workload_via_tsbs.sh --report <results_dir>     print report section

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

# ── Report mode ──────────────────────────────────────────────────────
# The workload loop logs one WARN line per failed load batch; tally
# them per log file.  Single glob — workload_*.log matches both the
# supervisor naming (workload_via_tsbs_<db>.log) and the legacy driver
# naming (workload_<db>.log); listing two globs double-counted files.
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: workload_via_tsbs.sh --report <results_dir>}"
  echo "── workload (TSBS insert stream) ──"
  found=0
  for lg in "$RESULTS"/logs/workload_*.log; do
    [[ -f "$lg" ]] || continue
    found=1
    # grep -c prints the count even when it is 0 (exit status 1), so no
    # || fallback — that would emit a second "0" line.
    warns=$(grep -c 'WARN: load batch' "$lg" 2>/dev/null)
    printf '  %-40s batch_warns=%s\n' "$(basename "$lg")" "${warns:-0}"
  done
  [[ "$found" -eq 0 ]] && echo "  (no workload logs)"
  exit 0
fi

TSBS_BIN="${SOAK_TSBS_BIN:-/home/gpadmin/tsbs_bin}"
SCALE="${SOAK_WORKLOAD_SCALE:-400}"
INTERVAL="${SOAK_WORKLOAD_INTERVAL:-1}"
LOG_INTERVAL="${SOAK_WORKLOAD_LOG_INTERVAL:-1s}"

if [[ ! -x "$TSBS_BIN/tsbs_generate_data" ]] \
   || [[ ! -x "$TSBS_BIN/tsbs_load_timescaledb" ]]; then
  echo "FATAL: tsbs binaries missing under $TSBS_BIN" >&2
  exit 1
fi

soak_log "[workload] starting fork-per-batch loop"
soak_log "  scale          = $SCALE"
soak_log "  batch interval = ${INTERVAL}s"
soak_log "  log interval   = $LOG_INTERVAL (samples per host per batch)"

while true; do
  NOW=$(date -u +%s)
  END=$(( NOW + INTERVAL ))
  TS_START=$(date -u -d "@$NOW" '+%Y-%m-%dT%H:%M:%SZ')
  TS_END=$(date -u -d "@$END" '+%Y-%m-%dT%H:%M:%SZ')

  # NOTE on --seed: must be FIXED (matching the seed phase, default 123),
  # not $NOW.  TSBS generates hostnames deterministically from the seed,
  # and tags rows are inserted with ON CONFLICT DO NOTHING (tsbs/pkg/
  # targets/timescaledb/process.go).  A varying seed would create new
  # hostnames every batch → tags table grows unbounded.  Fixed seed
  # ⇒ identical SCALE hosts every batch ⇒ tags table stays at SCALE rows
  # (default SCALE=400 per soak doc §6.4.3).
  # Each batch's per-host random walk re-starts from the same initial
  # state (limitation of fork-per-batch model); this is acceptable
  # because we test BGW correctness, not value-time continuity.
  "$TSBS_BIN/tsbs_generate_data" \
      --use-case=cpu-only \
      --scale="$SCALE" \
      --seed="${SOAK_SEED_SEED:-123}" \
      --timestamp-start="$TS_START" \
      --timestamp-end="$TS_END" \
      --log-interval="$LOG_INTERVAL" \
      --format=timescaledb \
  | "$TSBS_BIN/tsbs_load_timescaledb" \
      --cloudberry-mpp=true \
      --host="${SOAK_HOST:-localhost}" \
      --port="${SOAK_PORT:?}" \
      --user="${SOAK_USER:-gpadmin}" \
      --db-name="${SOAK_DB:-soak_test}" \
      --do-create-db=false \
      --create-metrics-table=false \
      --workers="${SOAK_WORKLOAD_WORKERS:-2}" \
      --batch-size="${SOAK_WORKLOAD_BATCH_SIZE:-1000}" \
      > /dev/null 2>&1

  RC=${PIPESTATUS[1]}   # only the loader's exit matters; generator
                        # may exit nonzero on broken pipe at shutdown
  if [[ $RC -ne 0 ]]; then
    soak_log "[workload] WARN: load batch rc=$RC at $TS_START"
  fi

  # Pace: aim for one batch per INTERVAL seconds.  If a batch took
  # longer than INTERVAL, skip the sleep and start the next batch
  # immediately so we don't fall further behind.
  ELAPSED=$(( $(date -u +%s) - NOW ))
  if [[ $ELAPSED -lt $INTERVAL ]]; then
    sleep $(( INTERVAL - ELAPSED ))
  fi
done
