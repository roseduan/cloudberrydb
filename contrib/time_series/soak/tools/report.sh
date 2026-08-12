#!/bin/bash
# tools/report.sh — assemble report.txt from per-loop --report sections.
#
# Pure glue: every SOAK-LOOP script owns its own report section (the
# `--report <results_dir>` mode); this script just discovers them, runs
# them in header `order=` sequence, and appends the flags verdict.
# Adding a new monitor therefore needs ZERO edits here.
#
# Stateless by design: takes the results dir as its only argument and
# reads everything (parameters included) from that dir, so it can be
# re-run against any historical results directory at any time:
#
#   bash tools/report.sh ./SOAK-20260609_072052
#
# Run parameters (duration, compress schedule, chaos level, ...) come
# from the dir's manifest.env, written by soak.sh at startup.
# Without a manifest (pre-refactor result dirs) sections fall back to
# their documented defaults.

set -uo pipefail

RESULTS="${1:?usage: report.sh <results_dir>}"
SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/loops.sh"

[[ -d "$RESULTS" ]] || { echo "ERROR: results dir not found: $RESULTS" >&2; exit 1; }

# Run parameters captured at launch — sourcing re-creates the exact
# SOAK_* environment of the run for the report sections.  `set -a`
# EXPORTS them so the child `<script> --report` processes inherit the
# run's actual parameters; without it a standalone report.sh re-run
# would silently fall back to each script's built-in defaults (e.g.
# judged.sh would read SOAK_JUDGE_ENABLED as unset).
if [[ -f "$RESULTS/manifest.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$RESULTS/manifest.env"
  set +a
fi

mkdir -p "$RESULTS/signals" "$RESULTS/verdicts"

# Move any streaming-written fail.* aside before re-running the judges
# so the audit section reflects the RUN-END state, not the first mid-run
# tick that happened to cross a threshold.  A verdict whose condition
# self-heals (plan_drift and view_slower_than_source do this routinely
# after a chaos storm) leaves a stale fail.* record from a streaming
# tick that never gets rewritten -- report.sh's `head -1` display then
# shows a mid-run breach as if it were current.  Preserved under
# verdicts/streaming/ so the audit trail is intact.
if compgen -G "$RESULTS/verdicts/fail.*" >/dev/null 2>&1; then
  mkdir -p "$RESULTS/verdicts/streaming"
  mv "$RESULTS"/verdicts/fail.* "$RESULTS/verdicts/streaming/" 2>/dev/null || true
fi

{
  echo "===== SOAK REPORT ====="
  echo "duration       : ${SOAK_RUN_DURATION:-unknown}"
  echo "preset         : ${SOAK_RUN_PRESET:-none}"
  echo "workload       : TSBS scale=${SOAK_WORKLOAD_SCALE:-400}, ${SOAK_WORKLOAD_INTERVAL:-1}s/batch"
  echo "started        : $(awk -F, 'NR==2 {print $1; found=1; exit} END {if (!found) print "unknown"}' "$RESULTS/data/system_metrics.csv" 2>/dev/null || echo unknown)"
  echo "ended          : $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  # One section per SOAK-LOOP script, ordered by the header's order=
  # field.  A section that errors must not sink the whole report.
  #
  # monitor/ is skipped: these are pure collectors — their report
  # sections are emitted by the corresponding judge/verdicts/*.sh
  # (which read the CSVs the collectors wrote).  Enforcing the skip
  # here keeps CSV as the single authoritative data source and lets
  # each monitor stay a minimal collection script with no --report stub.
  while IFS='|' read -r name script _scope _mode _interval _ivar _order; do
    [[ "$script" == */monitor/* ]] && continue
    echo
    bash "$script" --report "$RESULTS" 2>&1 \
      || echo "  (report section '$name' failed — see above)"
  done < <(discover_loops "$SOAK_DIR" | sort -t'|' -k7 -n)

  # ── Escalation bus ─────────────────────────────────────────────────
  # signals/  — runtime IPC:   chaos_active + panic.* (soak.sh consumed)
  # verdicts/ — end-of-run:    fail.* only              (SLO audit; see report + this section)
  echo
  echo "── signals (runtime IPC) ──"
  SIG_COUNT=0
  for f in "$RESULTS"/signals/*; do
    [[ -f "$f" ]] || continue
    base=$(basename "$f")
    [[ "$base" == "chaos_active" ]] && continue   # transient coordination, not a verdict
    SIG_COUNT=$(( SIG_COUNT + 1 ))
    echo "  $base: $(head -1 "$f" 2>/dev/null | cut -c1-120)"
  done
  if [[ "$SIG_COUNT" -eq 0 ]]; then
    echo "  none — no panic raised during the run"
  fi

  echo
  echo "── verdicts (audit) ──"
  # Show CURRENT (run-end) state: only fail.* written by the report-time
  # judges just above (streaming-mode files were moved to
  # verdicts/streaming/ at the top of this script).  `tail -1` picks the
  # most recent breach for each verdict (report writers usually only
  # write once, but if a judge appends we still surface the latest).
  VER_COUNT=0
  for f in "$RESULTS"/verdicts/fail.*; do
    [[ -f "$f" ]] || continue
    base=$(basename "$f")
    VER_COUNT=$(( VER_COUNT + 1 ))
    echo "  $base: $(tail -1 "$f" 2>/dev/null | cut -c1-120)"
  done
  if [[ "$VER_COUNT" -eq 0 ]]; then
    echo "  none — clean run"
  fi
  # Show a compact hint about streaming breaches that self-healed (a
  # verdict that fired mid-run but recovered by run end).  Not a fail,
  # just visibility -- useful for post-mortems of transient spikes.
  if [[ -d "$RESULTS/verdicts/streaming" ]]; then
    local_streaming_count=0
    for f in "$RESULTS"/verdicts/streaming/fail.*; do
      [[ -f "$f" ]] || continue
      name=$(basename "$f")
      [[ -f "$RESULTS/verdicts/$name" ]] && continue  # still active at run end
      local_streaming_count=$(( ${local_streaming_count:-0} + 1 ))
      if [[ "$local_streaming_count" -eq 1 ]]; then
        echo "  (transient during run, resolved by end:)"
      fi
      echo "    $name: $(head -1 "$f" 2>/dev/null | cut -c1-100)"
    done
  fi
} > "$RESULTS/report.txt"

cat "$RESULTS/report.txt"
