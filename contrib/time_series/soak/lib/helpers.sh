#!/bin/bash
# scripts/soak/lib/helpers.sh — shared helpers for the soak driver
#
# Sourced by soak.sh + monitor/system_metrics.sh.

# Guard against double-source
[[ -n "${_SOAK_LIB_LOADED:-}" ]] && return 0
_SOAK_LIB_LOADED=1

# ── Logging ──────────────────────────────────────────────────────────

soak_log() { printf '%s | %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }

# ── psql wrapper ─────────────────────────────────────────────────────
#
# Always sets optimizer=off + search_path because every CAGG query in
# the soak suite must run with PG planner + see time_series schema
# without qualification.

soak_psql() {
  PGOPTIONS='--client-min-messages=warning' \
  psql -h "${SOAK_HOST:-localhost}" \
       -p "${SOAK_PORT:?SOAK_PORT must be set}" \
       -U "${SOAK_USER:-gpadmin}" \
       -d "${SOAK_DB:-soak_test}" \
       -X -v ON_ERROR_STOP=1 \
       -c "SET optimizer = off;" \
       -c "SET search_path TO public, time_series;" \
       "$@"
}

# Variant: -f a script file. The session-level SETs above are not
# inherited across separate invocations, so each script must set its
# own SET clauses too.
soak_psql_file() {
  local f=$1
  shift
  PGOPTIONS='--client-min-messages=warning' \
  psql -h "${SOAK_HOST:-localhost}" \
       -p "${SOAK_PORT:?SOAK_PORT must be set}" \
       -U "${SOAK_USER:-gpadmin}" \
       -d "${SOAK_DB:-soak_test}" \
       -X -v ON_ERROR_STOP=1 \
       -f "$f" \
       "$@"
}

# ── BGW worker RSS sampling ──────────────────────────────────────────
#
# Returns one int (KB) per BGW worker (scheduler + active CAGG refresh
# workers).  Caller pipes through awk for sum/avg/max etc.
#
# Identifies workers by application_name pattern matching what
# scheduler.c sets via pgstat_report_appname.

soak_worker_rss_kb() {
  local pids
  pids=$(soak_psql -t -A -c "
    SELECT pid FROM pg_stat_activity
     WHERE application_name LIKE '%time_series%'
        OR application_name LIKE '%CAGG Policy%'
        OR application_name LIKE '%Refresh CAGG%'
        OR application_name LIKE '%Job History Log Retention%';" 2>/dev/null)

  for pid in $pids; do
    [[ -z "$pid" ]] && continue
    # /proc/<pid>/status VmRSS is reliable on Linux; Darwin would need
    # a different code path but soak runs on the Linux host (10.13.9.73).
    awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null || true
  done
}

# ── CSV output helpers ───────────────────────────────────────────────

soak_csv_append() {
  local f=$1
  shift
  if [[ ! -s "$f" ]]; then
    # Header line is the first arg, written once.
    printf '%s\n' "$1" >> "$f"
    shift
  fi
  printf '%s\n' "$*" >> "$f"
}
