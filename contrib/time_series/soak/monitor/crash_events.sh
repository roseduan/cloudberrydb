#!/bin/bash
# monitor/crash_events.sh
# SOAK-LOOP: scope=global interval=60 interval_var=SOAK_CRASH_EVENTS_EVERY order=44
#
# Samples the postmaster logs for BACKEND DEATH events -- signal kills,
# PANICs, and postmaster-initiated instance resets -- and appends one row
# per event to crash_events.csv.
#
# WHY THIS EXISTS (SOAK-20260725_163359 retrospective):
#   A chaos kill landed mid-recompress and left a committed PAX chunk file
#   truncated.  Every scan of that chunk then SIGSEGV'd a segment, once a
#   minute, for 7.7 hours -- straight past the end of the run.  The
#   framework had NO signal for it.  What it had was an INDIRECT one:
#   scrape_liveness eventually noticed the monitors had gone stale, which
#   is how the run got stopped at all.  Every diagnosis step after that
#   was manual: `grep 'terminated by signal' pg_log/*.csv` at each
#   checkpoint, by hand, on four datadirs.
#
#   A crash is the single least ambiguous failure a soak can observe:
#   there is no threshold to calibrate and no chaos-vs-real judgement to
#   make about the event itself (only about its timing).  It should be a
#   first-class collected signal, not something an analyst greps for.
#
# WHAT COUNTS AS AN EVENT
#   - "was terminated by signal N"     backend/worker died on a signal
#   - "PANIC"                          elog(PANIC) anywhere
#   - "terminating connection because of crash of another server process"
#                                      postmaster reset the instance
#   Chaos-injected kills produce these too (that is the POINT of chaos),
#   so attribution to chaos windows is the JUDGE's job, not the
#   collector's -- this loop stays a pure collector like every other
#   monitor.
#
# CSV: ts, seg, pid, signal, kind, detail
#   seg     coordinator | 0 | 1 | 2 ...   (from the datadir it was found in)
#   signal  the signal number, or '-' for PANIC / reset lines
#   kind    signal | panic | instance_reset
#   detail  first 120 chars of the message, commas stripped
#
# Usage (invoked by lib/loops.sh):
#   crash_events.sh <results_dir>          one sample pass

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RESULTS="${1:?usage: crash_events.sh <results_dir>}"
CSV="$RESULTS/data/crash_events.csv"
STATE="$RESULTS/.crash_events.seen"

[[ -f "$CSV" ]] || echo "ts,seg,pid,signal,kind,detail" > "$CSV"
[[ -f "$STATE" ]] || : > "$STATE"

# Coordinator datadir comes from soak.sh's auto-detection; the segment
# datadirs are its siblings under gpdemo's layout.  Both are globbed
# rather than assumed so a non-demo cluster simply yields fewer dirs.
COORD_DIR="${SOAK_DATA_DIR:-/home/gpadmin/hashdata-lightning/gpAux/gpdemo/datadirs/qddir/demoDataDir-1}"
DATADIRS_ROOT="$(cd "$COORD_DIR/../.." 2>/dev/null && pwd)"

# Only scan logs touched recently -- a 24h run accumulates many files and
# re-reading all of them every minute is wasted IO.  The dedup state file
# makes the overlap harmless.
SCAN_MINUTES="${SOAK_CRASH_EVENTS_SCAN_MINUTES:-15}"

# Ingest floor.  find -mmin picks the FILE, but a pg_log csv can hold days
# of history, so grep over a recently-touched file still returns ancient
# lines -- a first pass once swallowed 2268 events going back weeks.
# Compare each line's timestamp against a floor instead.
#
# The floor is LOCAL time, not UTC, on purpose: these timestamps come from
# the postmaster's log_timezone, and the monitor runs in the same container
# as the server, so local time is the same clock.  The format sorts
# lexicographically, so a plain string compare is enough.
FLOOR=$(date -d "-${SCAN_MINUTES} minutes" '+%Y-%m-%d %H:%M' 2>/dev/null \
        || date -v-"${SCAN_MINUTES}"M '+%Y-%m-%d %H:%M' 2>/dev/null)

scan_dir() {
  local dir="$1" seg="$2"
  [[ -d "$dir/log" ]] || return 0
  local f
  while IFS= read -r f; do
    [[ -n "$f" ]] || continue
    # Match the three event classes.  grep -h so the filename does not
    # end up inside the CSV detail column.
    grep -hE 'was terminated by signal|PANIC|terminating connection because of crash of another server process' \
         "$f" 2>/dev/null | while IFS= read -r line; do
      local ts pid sig kind detail key
      # pg_log CSV starts with the timestamp; keep it as-is (it is the
      # server's log_timezone, which the report reader already lives with).
      ts=$(printf '%s' "$line" | cut -d, -f1 | tr -d '"')
      # Skip anything older than the floor (see FLOOR above).
      [[ -n "$FLOOR" && "${ts:0:16}" < "$FLOOR" ]] && continue
      pid=$(printf '%s' "$line" | grep -oE '\(PID [0-9]+\)|,p[0-9]+,' | head -1 | grep -oE '[0-9]+' | head -1)
      sig=$(printf '%s' "$line" | grep -oE 'terminated by signal [0-9]+' | grep -oE '[0-9]+$')
      if [[ -n "$sig" ]]; then
        kind=signal
      elif printf '%s' "$line" | grep -q 'crash of another server process'; then
        kind=instance_reset
        sig='-'
      else
        kind=panic
        sig='-'
      fi
      detail=$(printf '%s' "$line" | grep -oE '"(LOG|PANIC|FATAL|WARNING)","[^"]*","[^"]*"' | head -1 \
               | cut -c1-120 | tr -d ',"')
      [[ -n "$detail" ]] || detail=$(printf '%s' "$line" | cut -c1-120 | tr -d ',"')

      # Dedup across passes: (seg, ts, pid, kind) is stable for one event.
      key="${seg}|${ts}|${pid:-0}|${kind}"
      if ! grep -qxF "$key" "$STATE" 2>/dev/null; then
        printf '%s\n' "$key" >> "$STATE"
        printf '%s,%s,%s,%s,%s,%s\n' \
          "$ts" "$seg" "${pid:-0}" "$sig" "$kind" "$detail" >> "$CSV"
      fi
    done
  done < <(find "$dir/log" -name '*.csv' -mmin "-${SCAN_MINUTES}" 2>/dev/null)
}

scan_dir "$COORD_DIR" "coordinator"

if [[ -n "$DATADIRS_ROOT" && -d "$DATADIRS_ROOT" ]]; then
  for d in "$DATADIRS_ROOT"/dbfast*/demoDataDir*; do
    [[ -d "$d" ]] || continue
    # demoDataDirN -> seg N
    scan_dir "$d" "$(basename "$d" | grep -oE '[0-9]+$')"
  done
fi
