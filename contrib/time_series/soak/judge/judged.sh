#!/bin/bash
# judge/judged.sh — business verdict engine.
# SOAK-LOOP: scope=global mode=self order=95 interval_var=SOAK_JUDGE_INTERVAL
#
# Three-layer split (see README.zh.md §3.6):
#
#   monitor/*.sh   →  PURE COLLECTORS.  Sample DB state, append CSV.
#                     Their --report is a no-op (they no longer judge).
#                     (The emergency panic.disk_full early-stop lives in
#                      judge/verdicts/framework_health.sh — a framework
#                      survival check, not a business verdict.)
#
#   judge/         →  ALL business judgment.  Reads the monitor CSVs
#                     (and chaos_log.csv), applies verdicts/*.sh, raises
#                     verdicts/fail.*.<class>.<who> (SLO broken; run
#                     continues) or signals/panic.* (immediate abort).
#                     Can judge in TWO modes:
#                       - streaming: the loop below re-evaluates every
#                         SOAK_JUDGE_INTERVAL seconds, so a threshold
#                         breach is caught mid-run, not only at the end.
#                       - report:    --report prints the verdict summary
#                         (report.sh pulls it in like any other section).
#
#   report.sh      →  unchanged glue.  It already calls every SOAK-LOOP
#                     script's --report; this one contributes the verdict
#                     sections, the (now no-op) monitors contribute none.
#
# Verdict contract — each judge/verdicts/<name>.sh defines:
#   judge_run_<name>    <results_dir> <state_dir>   streaming tick
#   judge_report_<name> <results_dir>               print report section
# judged sources them all and dispatches by function-name prefix.
#
# MIGRATION STATUS: verdicts are being moved out of the monitors one at
# a time.  A dimension already migrated (its verdict lives in
# verdicts/) has its monitor --report reduced to a no-op; a dimension
# not yet migrated still self-judges in its own monitor --report.  When
# all are migrated the monitors are pure collectors.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_judge_source_verdicts() {
  shopt -s nullglob
  local v
  # Shared libs first (lib/chaos.sh provides chaos_windows_str used by
  # the chaos-aware perf verdicts), then the verdict rules.
  for v in "$SCRIPT_DIR"/lib/*.sh; do
    # shellcheck disable=SC1090
    source "$v"
  done
  for v in "$SCRIPT_DIR"/verdicts/*.sh; do
    # shellcheck disable=SC1090
    source "$v"
  done
  shopt -u nullglob
}

# ── Report mode ──────────────────────────────────────────────────────
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: judged.sh --report <results_dir>}"
  echo "── judge (business verdicts) ──"
  if [[ "${SOAK_JUDGE_ENABLED:-1}" -eq 0 ]]; then
    echo "  disabled (SOAK_JUDGE_ENABLED=0) — monitors self-judge in their own --report"
    exit 0
  fi
  _judge_source_verdicts
  fns=$(compgen -A function 'judge_report_' | sort || true)
  if [[ -z "$fns" ]]; then
    echo "  enabled, but no verdict rules present in verdicts/ yet"
    exit 0
  fi
  for fn in $fns; do
    echo
    "$fn" "$RESULTS" || echo "  (verdict '$fn' errored)"
  done
  exit 0
fi

# ── Loop mode (streaming) ────────────────────────────────────────────
RESULTS="${1:?usage: judged.sh <results_dir>}"

# Gate: off by default until enough verdicts are migrated.  Mirrors
# chaos_loop's SOAK_CHAOS_LEVEL=0 pattern — a disabled judge is a clean
# immediate exit, not a wedged loop.
if [[ "${SOAK_JUDGE_ENABLED:-1}" -eq 0 ]]; then
  echo "judged: SOAK_JUDGE_ENABLED=0, disabled — exiting"
  exit 0
fi

INTERVAL="${SOAK_JUDGE_INTERVAL:-30}"
STATE_DIR="$RESULTS/state/judge"
mkdir -p "$STATE_DIR" "$RESULTS/flags"

_judge_source_verdicts
RUN_FNS=$(compgen -A function 'judge_run_' || true)
echo "judged: started (interval=${INTERVAL}s, verdicts=$(echo "$RUN_FNS" | grep -c . || echo 0))"

while true; do
  for fn in $RUN_FNS; do
    "$fn" "$RESULTS" "$STATE_DIR" || true
  done
  sleep "$INTERVAL"
done
