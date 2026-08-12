#!/bin/bash
# chaos/chaos_loop.sh — real-failure chaos injection loop for CAGG soak.
# SOAK-LOOP: scope=global mode=self order=90
#
# Injects ONLY real-world production failures (process death, cluster
# crash) — NOT code-point gp_inject_fault error/panic (those are
# deterministic regression tests, they belong in isolation2).
#
# Binary switch (SOAK_CHAOS_LEVEL):
#   0  off — exit immediately.
#   1  on — inject every fault in chaos/fault_catalog.csv (all with
#      level=1).  Faults include BGW worker kills (self-heal via
#      launcher respawn) and full cluster crash (recovered by
#      chaos_loop's own gpstart orchestration).  Cluster is DOWN
#      during a cluster_crash window — needs health-gate early-stop
#      suppression below.
#
# Phase model (Jepsen-style four-phase cycle):
#   inject ── active ── recover ── quiescence ── (next)
#     ↓         ↓          ↓            ↓
#  flag on    wait   flag off +    inter-fault
#                    gpstart if    gap
#                    cluster
#
# CHAOS_ACTIVE_FLAG exists on disk for the whole inject→recover window.
# Collectors that would otherwise early-stop the run on a dead cluster
# (view_check → panic.view_check_dead / panic.env_lost during
# cluster_crash) check this flag and hold their escalation while
# chaos owns the outage.
#
# Catalog (chaos/fault_catalog.csv) columns:
#   fault_name,level,target,mechanism,signal,active_s,recovery_budget_s,weight,description
#
# Exit: on SIGTERM/SIGINT the EXIT trap removes the active flag and, if a
# cluster crash was in flight, makes a best-effort gpstart so the run is
# not left with a dead cluster.

set -uo pipefail

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CATALOG="$CHAOS_DIR/fault_catalog.csv"

# ── Report mode ──────────────────────────────────────────────────────
# CSV: ts,action,fault_name,target,active_s,detail
# The verdict (recovery within budget, correctness during chaos) lives
# in judge/verdicts/chaos_recovery.sh — this section only summarizes the
# injection bookkeeping and flags a framework fault (inject/recover
# count mismatch = chaos_loop died mid-cycle → a crash may be un-healed).
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: chaos_loop.sh --report <results_dir>}"
  CSV="$RESULTS/data/chaos_log.csv"
  LEVEL="${SOAK_CHAOS_LEVEL:-0}"
  mkdir -p "$RESULTS/flags"
  echo "── chaos (real-failure injection) ──"
  if [[ "$LEVEL" -eq 0 ]]; then
    echo "  disabled (SOAK_CHAOS_LEVEL=0)"
    exit 0
  fi
  if [[ ! -s "$CSV" ]]; then
    echo "  level=$LEVEL but chaos_log.csv empty — chaos_loop.sh may have failed to start"
    exit 0
  fi
  INJ=$(awk -F, 'NR>1 && $2=="INJECT" {n++} END{print n+0}' "$CSV")
  HIT=$(awk -F, 'NR>1 && $2=="INJECT" && $6 ~ /killed|crashed|terminated/ {n++} END{print n+0}' "$CSV")
  # "kill pid=... failed" (poll caught a pid but it exited before SIGKILL
  # landed) is a MISS, not a phantom neither-hit-nor-miss: added
  # /kill.*failed/ so hit+miss==injects again.  Old totals mislabeled
  # roughly 6 of 14 SOAK-20260723 injects as "vanished".
  MISS=$(awk -F, 'NR>1 && $2=="INJECT" && $6 ~ /no in-flight|no scheduler|kill.*failed/ {n++} END{print n+0}' "$CSV")
  REC=$(awk -F, 'NR>1 && $2=="RECOVER" {n++} END{print n+0}' "$CSV")
  echo "  level            : $LEVEL"
  echo "  injects          : $INJ  (hit=$HIT, miss=$MISS)"
  echo "  recover markers  : $REC"
  echo "  by fault:"
  awk -F, 'NR>1 && $2=="INJECT" { n[$3]++; if ($6 ~ /killed|crashed|terminated/) h[$3]++ }
           END { for (k in n) printf "    %-24s injects=%-3d hits=%-3d\n", k, n[k], h[k]+0 }' "$CSV" | sort
  # by-db breakdown — db column added 2026-07-24 (7th column, "-" for
  # cluster-scope or legacy rows).  Older result dirs whose chaos_log.csv
  # was written before this migration will show db="-" for all rows;
  # that is expected, not a bug.
  echo "  by db:"
  awk -F, 'NR>1 && $2=="INJECT" {
             db=(NF>=7 ? $7 : "-"); if (db=="") db="-"; n[db]++;
             if ($6 ~ /killed|crashed|terminated/) h[db]++ }
           END { for (k in n) printf "    %-24s injects=%-3d hits=%-3d\n", k, n[k], h[k]+0 }' "$CSV" | sort
  if [[ "$INJ" -ne "$REC" ]]; then
    echo "  ⚠ inject/recover mismatch ($INJ vs $REC) — chaos_loop may have been killed mid-cycle"
    touch "$RESULTS/signals/panic.chaos"
  fi
  echo "  (self-heal + correctness verdicts: see 'judge' section)"
  exit 0
fi

# ── Loop mode ────────────────────────────────────────────────────────
RESULTS_ARG="${1:-}"
if [[ -n "$RESULTS_ARG" && -d "$RESULTS_ARG" ]]; then
  mkdir -p "$RESULTS_ARG/flags"
  CHAOS_ACTIVE_FLAG="${CHAOS_ACTIVE_FLAG:-$RESULTS_ARG/signals/chaos_active}"
  CHAOS_CSV="${CHAOS_CSV:-$RESULTS_ARG/chaos_log.csv}"
fi

CHAOS_LEVEL="${SOAK_CHAOS_LEVEL:-0}"
CHAOS_ACTIVE_FLAG="${CHAOS_ACTIVE_FLAG:-/tmp/cagg_soak_chaos_active.flag}"
CHAOS_CSV="${CHAOS_CSV:-/tmp/cagg_soak_chaos.csv}"
DBS="${SOAK_DBS:-soak_test_a soak_test_b}"
PORT="${PGPORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"
ADMIN_DB="${SOAK_ADMIN_DB:-${DBS%% *}}"
AVG_INTERVAL="${SOAK_CHAOS_AVG_INTERVAL:-900}"
JITTER_DIV="${SOAK_CHAOS_JITTER_DIV:-3}"

if [[ "$CHAOS_LEVEL" -eq 0 ]]; then
  echo "chaos_loop: SOAK_CHAOS_LEVEL=0, disabled — exiting"
  exit 0
fi
[[ -f "$CATALOG" ]] || { echo "chaos_loop: catalog not found: $CATALOG" >&2; exit 1; }

if [[ ! -f "$CHAOS_CSV" ]]; then
  # `db` (7th) added 2026-07-24 so per-fault post-mortems can pin down
  # which DB was hit — chaos picks a random DB per inject, and the
  # legacy 6-column row lost that info at write time.  The column lives
  # at the END so existing awk consumers keying on $1..$6 keep working;
  # judge/lib/chaos.sh only reads $1..$3.
  echo "ts,action,fault_name,target,active_s,detail,db" > "$CHAOS_CSV"
fi
chaos_log() {
  # args: action, fault_name, target, active_s, detail, db
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$1" "$2" "$3" "$4" "$5" "${6:--}" >> "$CHAOS_CSV"
}

# Like chaos_log but with an explicit timestamp (first arg).  Used for
# the RECOVERY_TIME row, whose ts is the COMPUTED exclusion-window end
# (INJECT + recovery budget), not the wall-clock moment it is written.
chaos_log_at() {
  local ts="$1"; shift
  # args after ts: action, fault_name, target, active_s, detail, db
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$ts" "$1" "$2" "$3" "$4" "$5" "${6:--}" >> "$CHAOS_CSV"
}

# ----- Catalog parsing --------------------------------------------------------
# All rows are enabled when SOAK_CHAOS_LEVEL=1 (SOAK_CHAOS_LEVEL=0 already
# exited above).  The `level` column is retained in the CSV for future
# tiering (e.g. env-specific faults that need extra infra) but is not
# consulted here.
declare -a FAULTS
total_weight=0
while IFS=, read -r fault_name level target mechanism signal active_s recovery_budget weight desc; do
  [[ "$fault_name" == "fault_name" ]] && continue
  [[ -z "${fault_name// }" ]] && continue
  # SOAK_CHAOS_ONLY=<name>[,<name>...] restricts the active set to the
  # named faults — used by focused smoke tests to guarantee a specific
  # rare fault (e.g. cluster_crash, weight 1/9) actually fires.  Unset in
  # normal runs, where the full weighted set is used.
  [[ -n "${SOAK_CHAOS_ONLY:-}" && ",${SOAK_CHAOS_ONLY}," != *",${fault_name},"* ]] && continue
  FAULTS+=("$fault_name|$target|$mechanism|$signal|$active_s|$recovery_budget|$weight")
  total_weight=$((total_weight + weight))
done < "$CATALOG"
if [[ ${#FAULTS[@]} -eq 0 ]] || [[ $total_weight -le 0 ]]; then
  echo "chaos_loop: no faults in $CATALOG (check SOAK_CHAOS_ONLY filter)" >&2
  exit 1
fi

pick_fault() {
  local pick=$((RANDOM % total_weight)) cum=0 entry w
  for entry in "${FAULTS[@]}"; do
    w=$(echo "$entry" | awk -F'|' '{print $7}')
    cum=$((cum + w))
    [[ $pick -lt $cum ]] && { printf '%s' "$entry"; return; }
  done
  printf '%s' "${FAULTS[0]}"
}
pick_db() { local a=($DBS); echo "${a[$((RANDOM % ${#a[@]}))]}"; }

psql_pid() {
  # Find one victim pid matching the SQL predicate; empty if none.
  local db="$1" pred="$2"
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$db" -At -v ON_ERROR_STOP=0 -c "
    SELECT pid FROM pg_stat_activity WHERE $pred ORDER BY random() LIMIT 1;" 2>/dev/null
}

# Poll up to active_s seconds for an IN-FLIGHT worker of the given kind,
# then kill it.  Polling (not one-shot) turns the old '8% lottery' into a
# reliable hit: refresh/compress runs are short, so a random instant
# rarely catches one, but waiting a few seconds almost always does.
kill_inflight() {
  local db="$1" pred="$2" signal="$3" active_s="$4" i pid
  for ((i=0; i<active_s; i++)); do
    pid=$(psql_pid "$db" "$pred")
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
      kill -"$signal" "$pid" 2>/dev/null && { echo "killed pid=$pid (sig $signal) after ${i}s"; return 0; }
      echo "kill pid=$pid failed"; return 1
    fi
    sleep 1
  done
  echo "no in-flight worker seen in ${active_s}s"
}

inject_fault() {
  local target="$1" mechanism="$2" signal="$3" active_s="$4" db="$5"
  case "$target" in
    refresh_worker)
      kill_inflight "$db" \
        "state='active' AND (query ILIKE '%refresh_continuous%' OR query ILIKE '%materializ%' OR application_name ILIKE '%efresh%')" \
        "${signal:-KILL}" "$active_s" ;;
    compress_worker)
      kill_inflight "$db" \
        "state='active' AND (query ILIKE '%compress%' OR application_name ILIKE '%ompress%')" \
        "${signal:-KILL}" "$active_s" ;;
    scheduler)
      # Scheduler is usually idle; terminate it directly (no poll).
      local pid; pid=$(psql_pid "$db" "backend_type = 'time_series scheduler'")
      if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
        psql -h "$HOST" -p "$PORT" -U "$USER" -d "$db" -At -c \
          "SELECT pg_terminate_backend($pid);" >/dev/null 2>&1 \
          && echo "terminated scheduler pid=$pid" || echo "terminate pid=$pid failed"
      else
        echo "no scheduler backend found in $db"
      fi ;;
    cluster)
      # CLASS-2 power-cord: kill -9 every postgres.  Recovery (gpstart)
      # happens in the recover phase.  Cluster is DOWN until then.
      pkill -"${signal:-KILL}" -u "$(id -un)" postgres 2>/dev/null
      echo "crashed cluster (pkill -${signal:-KILL} postgres)" ;;
    *)
      echo "unknown target=$target" ;;
  esac
}

# gpstart recovery for a cluster crash (CLASS-2 only).  Brings the
# cluster back after `pkill -KILL postgres`.  The datadirs root is two
# levels up from the coordinator's datadir (…/datadirs/qddir/
# demoDataDir-1 → …/datadirs).
#
# A hard kill (and, worse, a FAILED gpstart attempt) leaves behind state
# that a real crash+reboot would have cleared — and that blocks the next
# start.  Each of these bit a 2026-07-09 chaos=2 smoke:
#   • postmaster.pid files in every datadir.
#   • stale UNIX sockets + lock files → gpstart sees "/tmp/.s.PGSQL.7000
#     and a process running on port 7000" and refuses with "Coordinator
#     instance process running" (crash #1 failed in 12 s).
#   • a HALF-STARTED coordinator from a failed prior attempt still
#     attached to its SysV shm segment → the next gpstart's coordinator
#     dies with "pre-existing shared memory block (key …) is still in
#     use" (crash #2 failed after gpstart left an attached postmaster).
# So before EACH attempt we do what a reboot does: kill any surviving /
# half-started postgres, then clear pidfiles, sockets, and orphaned
# SysV shm+sem.  This assumes a DEDICATED cluster (the only postgres +
# IPC owned by this OS user on the host) — true for the soak env, and
# chaos=2 is destructive by definition.
#
# We do NOT wait for `pgrep postgres` to reach 0: a hard kill orphans
# the backends as ZOMBIES reparented to the container's PID 1, which
# never reaps them, so that count never falls — but zombies hold no
# ports/memory/sockets/IPC, so they don't block recovery.  (A long
# chaos=2 run accretes zombies; run the container with an init that
# reaps, e.g. `docker run --init`, to keep the process table clean.)
#
# gpstart can exit non-zero (or say "already running") even when the
# cluster is in fact up, so success is gated on an ACTUAL coordinator
# connection, never gpstart's exit code.  Output is teed to
# chaos_gpstart.log for post-mortem instead of discarded.
recover_cluster() {
  local root pidf tries i sockdir osu
  tries="${SOAK_CHAOS_GPSTART_TRIES:-3}"
  sockdir="${SOAK_SOCKET_DIR:-/tmp}"
  osu="$(id -un)"
  root="$(cd "$(dirname "$COORDINATOR_DATA_DIRECTORY")/.." 2>/dev/null && pwd)"
  for (( i=1; i<=tries; i++ )); do
    # Kill survivors / half-started coordinators, then clear every
    # artifact that blocks a fresh start (see header).
    pkill -9 postgres 2>/dev/null
    sleep 2
    if [[ -n "$root" ]]; then
      while IFS= read -r pidf; do
        rm -f "$pidf" 2>/dev/null
      done < <(find "$root" -name postmaster.pid 2>/dev/null)
    fi
    rm -f "$sockdir"/.s.PGSQL.* 2>/dev/null
    ipcs -m | awk -v u="$osu" '$3==u {print $2}' | xargs -r -n1 ipcrm -m 2>/dev/null
    ipcs -s | awk -v u="$osu" '$3==u {print $2}' | xargs -r -n1 ipcrm -s 2>/dev/null
    # Orphaned tuplestore/sort spill from a query the crash interrupted:
    # gpstart's crash-recovery startup does NOT always clear base/
    # pgsql_tmp, and it accretes fast (a 2026-07-09 chaos=2 smoke left
    # 77 GB of pgsql_tmpslice1_tuplestore* on the coordinator).  Safe to
    # remove — every backend that owned one is dead.
    if [[ -n "$root" ]]; then
      find "$root" -type d -name pgsql_tmp -prune \
        -exec sh -c 'rm -f "$1"/pgsql_tmp* 2>/dev/null' _ {} \; 2>/dev/null
    fi
    gpstart -a >>"${RESULTS_ARG:-/tmp}/logs/chaos_gpstart.log" 2>&1
    # Proof of life: can we run a trivial query on the coordinator?
    if psql -h "$HOST" -p "$PORT" -U "$USER" -d postgres \
            -At -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    [[ "$i" -lt "$tries" ]] && sleep 5
  done
  return 1
}

# measure_recovery — after the fault clears, how long until the target
# DB's managed CAGGs are materializing normally again?  Polls
# min-watermark-vs-source against 2×end_offset (same rule watermark_lag
# uses).  Echoes recovery seconds or 'timeout'.  Cluster-down (psql
# error) counts as not-yet-recovered.
measure_recovery() {
  local db="$1" budget="$2" t0 now elapsed bad
  local poll="${SOAK_CHAOS_RECOVERY_POLL:-3}"
  t0=$(date +%s)
  while true; do
    bad=$(psql -h "$HOST" -p "$PORT" -U "$USER" -d "$db" -At -v ON_ERROR_STOP=0 -c "
      WITH src AS (SELECT max(time) m FROM public.cpu
                    WHERE time >= (SELECT range_start FROM time_series.ts_chunk
                                    WHERE table_oid='public.cpu'::regclass
                                    ORDER BY chunk_number DESC LIMIT 1))
      SELECT count(*) FROM time_series.continuous_agg ca
        JOIN time_series.cagg_watermark cw ON cw.cagg_id=ca.cagg_id
        LEFT JOIN time_series.bgw_job j
          ON j.config->>'cagg_name'=ca.user_view_name AND j.proc_name='policy_refresh_cagg'
       CROSS JOIN src
       WHERE ca.user_view_name NOT IN ('cv_scratch','repro_cv')
         AND cw.watermark <> '-infinity'::timestamptz
         AND src.m IS NOT NULL
         AND (src.m - cw.watermark) > ((j.config->>'end_offset')::interval * 2)
      " 2>/dev/null)
    now=$(date +%s); elapsed=$(( now - t0 ))
    if [[ "$bad" == "0" ]]; then echo "$elapsed"; return 0; fi
    if [[ "$elapsed" -ge "$budget" ]]; then echo "timeout"; return 1; fi
    sleep "$poll"
  done
}

# ----- Cleanup on exit --------------------------------------------------------
IN_FLIGHT=""
cleanup() {
  rm -f "$CHAOS_ACTIVE_FLAG"
  if [[ -n "$IN_FLIGHT" ]]; then
    IFS='|' read -r fn tg db <<< "$IN_FLIGHT"
    chaos_log "RECOVER" "$fn" "$tg" "0" "shutdown_cleanup" "${db:--}"
    # If a cluster crash was mid-flight, best-effort gpstart so the run
    # is not left with a dead cluster.
    [[ "$tg" == "cluster" ]] && recover_cluster || true
    # Close the exclusion window too.  chaos_windows_str pairs INJECT
    # with RECOVERY_TIME by fault_name; an INJECT left unpaired at
    # shutdown becomes an OPEN window ([inject_ts, 9999-12-31]) that
    # blinds every chaos-aware verdict to all samples after this
    # moment.  SOAK-20260723_170208's last inject (19:13:05
    # compress_worker_kill, interrupted by run end) did exactly that
    # to compress_perf.  The run is over, so "window ends now" is the
    # honest stamp.
    chaos_log "RECOVERY_TIME" "$fn" "$tg" "0" "recovery_s=shutdown" "${db:--}"
    IN_FLIGHT=""
  fi
  chaos_log "EXIT" "-" "-" "0" "cleanup_done" "-"
}
trap 'exit 143' INT TERM
trap cleanup EXIT

chaos_log "START" "-" "-" "0" "chaos=on avg_interval=${AVG_INTERVAL}s faults=${#FAULTS[@]}" "-"

# ----- Main loop --------------------------------------------------------------
while true; do
  entry=$(pick_fault)
  IFS='|' read -r fn tg mech sig as rbudget _w <<< "$entry"
  db=$(pick_db)

  # 1. inject (+ active window is embedded in kill_inflight's poll)
  touch "$CHAOS_ACTIVE_FLAG"
  IN_FLIGHT="$fn|$tg|$db"
  detail=$(inject_fault "$tg" "$mech" "$sig" "$as" "$db" | tr '\n,' ';;' | cut -c1-160)
  chaos_log "INJECT" "$fn" "$tg" "$as" "${detail:-done}" "$db"
  inject_epoch=$(date +%s)

  # 2. recover: cluster crash needs gpstart; worker kills recover on their
  #    own -- but NOT as cheaply as the old "worker respawn (no action)"
  #    wording suggested.  SIGKILL on a background worker that holds shared
  #    memory makes postmaster treat it exactly like a crashed backend: it
  #    resets the WHOLE instance, dropping every client connection with
  #    "terminating connection because of crash of another server process"
  #    and refusing new ones with "the database system is in recovery mode"
  #    until recovery finishes.  Verified in SOAK-20260728_095744 (14:37:54
  #    refresh_worker_kill -> instance reset -> in-flight ddl_churn cycle
  #    and every query_load reader failed).  The blast radius is
  #    cluster-wide, so verdicts must whitelist these faults just like
  #    cluster_crash (see judge/lib/chaos.sh).
  if [[ "$tg" == "cluster" ]]; then
    if recover_cluster; then chaos_log "RECOVER" "$fn" "$tg" "0" "gpstart ok" "$db"
    else chaos_log "RECOVER" "$fn" "$tg" "0" "gpstart FAILED" "$db"; touch "$RESULTS_ARG/signals/panic.chaos"; fi
  else
    chaos_log "RECOVER" "$fn" "$tg" "0" "instance reset + worker respawn (automatic)" "$db"
  fi
  rm -f "$CHAOS_ACTIVE_FLAG"
  IN_FLIGHT=""

  # 3. measure self-heal (runs in the quiescence dead-time, free wall-clock)
  rec=$(measure_recovery "$db" "$rbudget")

  # The chaos EXCLUSION WINDOW is [INJECT.ts, RECOVERY_TIME.ts] (judge/
  # lib/chaos.sh).  Two problems make the naive "RECOVERY_TIME = now"
  # wrong:
  #   (a) measure_recovery polls the WATERMARK, which a worker/scheduler
  #       kill never regresses → returns 0 instantly → window collapses
  #       to an instant, so post-kill transient disruption counts as
  #       non-chaos (2026-07-09 chaos=1 smoke: false fail.view_incomplete
  #       + fail.perf).
  #   (b) the disruption is DELAYED: a scheduler kill freezes refresh
  #       next_start for minutes AFTER the instant of the kill.
  # So the window end is a COMPUTED timestamp = INJECT + the fault's full
  # RECOVERY BUDGET (or active_s + grace, whichever is larger).  Within
  # its declared recovery budget, degradation is contractually allowed
  # and must not count toward perf/incomplete fails.  We stamp the row
  # with that near-future timestamp instead of sleeping, so the loop is
  # not slowed.  chaos_recovery still judges the measured recovery_s
  # against the budget separately.
  grace="${SOAK_CHAOS_WINDOW_GRACE:-5}"
  window_s=$(( as + grace ))
  [[ "$rbudget" =~ ^[0-9]+$ && "$rbudget" -gt "$window_s" ]] && window_s="$rbudget"
  window_end_ts=$(date -u -d "@$(( inject_epoch + window_s ))" '+%Y-%m-%dT%H:%M:%SZ')
  chaos_log_at "$window_end_ts" "RECOVERY_TIME" "$fn" "$tg" "$rbudget" "recovery_s=${rec}" "$db"
  recovered_elapsed=0
  [[ "$rec" =~ ^[0-9]+$ ]] && recovered_elapsed="$rec"

  # 4. quiescence + jitter, targeting AVG_INTERVAL between faults
  jitter=$((AVG_INTERVAL / JITTER_DIV))
  rand=$((RANDOM % (2 * jitter + 1) - jitter))
  pause=$((AVG_INTERVAL - as - recovered_elapsed + rand))
  [[ $pause -lt 30 ]] && pause=30
  sleep "$pause"
done
