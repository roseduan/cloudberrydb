#!/bin/bash
# tools/autopsy.sh — snapshot the crime scene when a run dies.
#
# Called by soak.sh the moment a panic.* flag trips (and runnable by hand
# against any results dir).  Writes everything under
# $RESULTS/autopsy/<timestamp>/.
#
# WHY THIS EXISTS
#   Diagnosing SOAK-20260725_163359's crash loop took a day, and roughly
#   half of that was spent recovering evidence that no longer existed by
#   the time anyone looked:
#     - no core files, because the postmaster had not been started with a
#       core limit; the fix was `prlimit --pid <postmaster> --core=...`
#       and then WAITING for the next crash;
#     - lock state, chunk catalog and PAX directory listings were read
#       7 hours after the fact, long after the interesting transient had
#       passed.
#   Everything collected here is cheap, and its value is highest at
#   exactly the moment the run gives up.
#
# Deliberately best-effort throughout: a dying cluster may refuse
# connections, and an autopsy that aborts halfway is worse than one that
# records "psql unavailable" and moves on.
#
# Usage:
#   autopsy.sh <results_dir> [reason]

set -uo pipefail

RESULTS="${1:?usage: autopsy.sh <results_dir> [reason]}"
REASON="${2:-manual}"
HOST="${SOAK_HOST:-localhost}"
PORT="${SOAK_PORT:-7000}"
USER="${SOAK_USER:-gpadmin}"

STAMP=$(date -u '+%Y%m%dT%H%M%SZ')
OUT="$RESULTS/autopsy/$STAMP"
mkdir -p "$OUT"

echo "reason: $REASON"          > "$OUT/00_reason.txt"
date -u '+collected: %FT%TZ'   >> "$OUT/00_reason.txt"

q() {  # q <file> <db> <sql>
  local out="$1" db="$2" sql="$3"
  timeout 30 psql -h "$HOST" -p "$PORT" -U "$USER" -d "$db" -X -A \
      -c "$sql" > "$OUT/$out" 2>&1 \
    || echo "(psql unavailable or query failed)" >> "$OUT/$out"
}

# ── Cluster shape + who is stuck where ───────────────────────────────
q 10_segment_config.txt template1 \
  "SELECT content, role, preferred_role, mode, status, port FROM gp_segment_configuration ORDER BY content, role;"
q 11_activity.txt template1 \
  "SELECT pid, datname, state, wait_event_type, wait_event, now()-query_start AS run_time, left(query,200) AS query
     FROM pg_stat_activity WHERE state <> 'idle' ORDER BY query_start;"
# Coordinator-visible locks only; segment-level locks need per-segment
# utility connections, which a wedged cluster often will not grant.  The
# ungranted ones are the interesting rows either way.
q 12_locks_ungranted.txt template1 \
  "SELECT l.pid, l.locktype, l.relation::regclass AS rel, l.mode, l.granted, left(a.query,120) AS query
     FROM pg_locks l LEFT JOIN pg_stat_activity a USING (pid)
    WHERE NOT l.granted ORDER BY l.pid;"

# ── Per-DB extension state ───────────────────────────────────────────
for db in ${SOAK_DBS:-soak_test_a soak_test_b}; do
  q "20_${db}_jobs.txt" "$db" \
    "SELECT j.id, j.proc_name, j.schedule_interval, s.last_run_success,
            s.consecutive_failures, s.consecutive_crashes, s.total_crashes,
            s.last_start, s.last_finish, s.next_start
       FROM time_series.bgw_job j
       LEFT JOIN time_series.bgw_job_stat s ON s.job_id = j.id ORDER BY j.id;"
  q "21_${db}_chunks.txt" "$db" \
    "SELECT * FROM time_series.ts_chunk ORDER BY table_oid, chunk_number;"
  q "22_${db}_logs.txt" "$db" \
    "SELECT 'L1' AS log, count(*) FROM time_series.cagg_invalidation_log
      UNION ALL SELECT 'L2', count(*) FROM time_series.cagg_materialization_log;"
  q "23_${db}_watermark.txt" "$db" \
    "SELECT ca.user_view_name, cw.cagg_id, cw.watermark
       FROM time_series.continuous_agg ca
       JOIN time_series.cagg_watermark cw ON cw.cagg_id = ca.cagg_id
      ORDER BY 1;"
done

# ── On-disk PAX state ────────────────────────────────────────────────
# File sizes and mtimes are what identified the redo-overwrote-live-file
# bug: a ".new" and its live sibling with IDENTICAL size and mtime.
COORD_DIR="${SOAK_DATA_DIR:-/home/gpadmin/hashdata-lightning/gpAux/gpdemo/datadirs/qddir/demoDataDir-1}"
DATADIRS_ROOT="$(cd "$COORD_DIR/../.." 2>/dev/null && pwd)"
{
  if [[ -n "$DATADIRS_ROOT" && -d "$DATADIRS_ROOT" ]]; then
    find "$DATADIRS_ROOT"/dbfast*/demoDataDir*/base/*/ts_compressed \
         -name 'chunk_*' -printf '%s\t%TY-%Tm-%Td %TH:%TM:%TS\t%p\n' 2>/dev/null | sort -k3
  else
    echo "(datadirs root not found: $DATADIRS_ROOT)"
  fi
} > "$OUT/30_pax_files.txt"

# ── Tail of every postmaster log ─────────────────────────────────────
{
  for d in "$COORD_DIR" "$DATADIRS_ROOT"/dbfast*/demoDataDir*; do
    [[ -d "$d/log" ]] || continue
    newest=$(find "$d/log" -name '*.csv' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
    [[ -n "$newest" ]] || continue
    echo "════════ $newest ════════"
    tail -80 "$newest" 2>/dev/null | cut -c1-400
    echo
  done
} > "$OUT/40_pg_log_tails.txt"

# ── Core files (if any) ──────────────────────────────────────────────
{
  if [[ -n "$DATADIRS_ROOT" && -d "$DATADIRS_ROOT" ]]; then
    find "$DATADIRS_ROOT" -maxdepth 3 -name 'core*' -printf '%s\t%TY-%Tm-%Td %TH:%TM\t%p\n' 2>/dev/null
  fi
  echo "--- core_pattern / limits ---"
  cat /proc/sys/kernel/core_pattern 2>/dev/null
  ulimit -c
} > "$OUT/50_cores.txt"

# ── System ───────────────────────────────────────────────────────────
{ df -h; echo; free -m 2>/dev/null; echo; uptime; } > "$OUT/60_system.txt" 2>&1

echo "$OUT"
