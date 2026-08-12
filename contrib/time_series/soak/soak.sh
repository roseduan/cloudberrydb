#!/bin/bash
# soak.sh — single entry point + driver for the time_series CAGG soak.
#
# Two presets:
#   smoke        5 min framework smoke test (scale=100, ~1 GB peak disk)
#                Verifies the soak framework itself works end-to-end:
#                each monitor / workload / compress / refresh path fires
#                at least once.  Use after any soak/* code change.
#
#   recommended  24 h production-ish (scale=500, fits in 100 GB disk)
#                Targets a 100 GB host disk budget.  This is the round
#                you cite when reporting "we ran soak and ...".
#                Aliases: default, prod, bp
#
# Common overrides — almost everything you'd want to change is here:
#   bash soak.sh                                 # recommended 24h scale=500 (cold-start by default)
#   bash soak.sh --preset=smoke                  # 5 min framework smoke (cold-start)
#   bash soak.sh --duration=4h                   # recommended but 4h
#   bash soak.sh --duration=24h --scale=1000     # recommended with bigger cardinality
#   bash soak.sh --chaos=1                       # recommended + chaos on (binary switch: 0=off, 1=on)
#   bash soak.sh --warm-start                    # pre-seed cpu table with N days of history
#                                                # before workload starts (preset's SOAK_SEED_DAYS).
#                                                # Default is cold-start: empty table at T=0,
#                                                # workload populates from scratch.
#
# All 86 fine-grained knobs (monitor cadences, alarm thresholds, etc.):
#     see conf/soak_params.sh.  Override via env: SOAK_X=y bash soak.sh
#
# Lifecycle:
#   parse args → apply preset → source conf/soak_params.sh →
#   setup DBs → write manifest → start loops → wait (health-gated) →
#   stop loops → report → exit
#
# Loop discovery is by header: any monitor/* workload/* chaos/* tools/*
# script with a `# SOAK-LOOP:` line is picked up automatically.
# The driver knows NO individual loop by name.
#
# Escalation bus (two-tier, no in-between advisory layer):
#   $RESULTS/signals/  — runtime IPC
#     chaos_active  — coordinator flag consumed by judge (chaos-aware windows)
#     panic.*       — health gate stops the run early on sight
#   $RESULTS/verdicts/ — SLO audit log (append-only, one row per trigger)
#     fail.<class>.<who>  — an SLO threshold was crossed; each trigger
#                            appends one row (ts + context)
#
# Exit code is intentionally a plain completion signal, NOT a verdict summary:
#   0 = run completed (natural 24h expiry OR panic-driven early stop)
#   1 = setup failure
#   3 = user interrupt (SIGINT / SIGTERM)
#
# Whether the run met its SLOs is determined by:
#   ls $RESULTS/signals/panic.*   — non-empty → environment failed mid-run
#   ls $RESULTS/verdicts/fail.*   — non-empty → SLO broken (run may still
#                                    have completed 24h — check both)
#   both empty                     → clean run

set -uo pipefail

# ── Paths + libs ─────────────────────────────────────────────────────
SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure PYTHONPATH / PATH / LD_LIBRARY_PATH for gpstart, gpstop, gpstate.
# chaos/chaos_loop.sh::recover_cluster shells out to `gpstart` after a
# cluster_crash; without greenplum_path.sh sourced, gpstart aborts with
# `ModuleNotFoundError: No module named 'gppylib'` and the run dies on
# panic.chaos.  Historically that worked only because the caller happened
# to have sourced greenplum_path.sh before launching soak.sh — a fragile
# implicit dependency.  Source it here explicitly, best-effort (a
# missing prefix on non-standard installs is not fatal).
for _gp_env in \
    /usr/local/cloudberry-db/greenplum_path.sh \
    /usr/local/gpdb/greenplum_path.sh \
    "${GPHOME:-}/greenplum_path.sh"; do
  if [[ -r "$_gp_env" ]]; then
    # shellcheck disable=SC1090
    source "$_gp_env"
    break
  fi
done
unset _gp_env
# Where each run's results go.  Default is SOAK_DIR — the directory
# that contains soak.sh — so results land in soak/SOAK-<timestamp>/
# regardless of where the user ran the script from.  Override with
# SOAK_RESULTS_BASE=<abs-or-rel-path> to pin an explicit location
# (per-user scratch, NFS share, /tmp for a smoke, etc.).
SOAK_RESULTS_BASE="${SOAK_RESULTS_BASE:-$SOAK_DIR}"

source "$SOAK_DIR/lib/helpers.sh"
source "$SOAK_DIR/lib/loops.sh"

# ── Argument parsing ─────────────────────────────────────────────────
# Priority (highest wins):
#   CLI flag  >  user env (SOAK_X=...)  >  preset defaults  >  conf/soak_params.sh
PRESET="recommended"
DURATION=""
SCALE_ARG=""
SKIP_SETUP=false
COLD_START=true       # ← default: empty table at T=0; pass --warm-start to opt out
CHAOS_ARG=""

for arg in "$@"; do
  case "$arg" in
    --preset=*)   PRESET="${arg#*=}" ;;
    --duration=*) DURATION="${arg#*=}" ;;
    --scale=*)    SCALE_ARG="${arg#*=}" ;;
    --chaos=*)    CHAOS_ARG="${arg#*=}" ;;
    --port=*)     SOAK_PORT="${arg#*=}" ;;
    --db=*)       SOAK_DBS="${arg#*=}" ;;
    --dbs=*)      SOAK_DBS="${arg#*=}" ;;
    --skip-setup) SKIP_SETUP=true ;;
    --cold-start) COLD_START=true ;;       # explicit opt-in (same as default)
    --warm-start) COLD_START=false ;;      # opt out of the cold-start default
    -h|--help)
      sed -n '2,44p' "$0" | sed 's/^# \?//'
      exit 0 ;;
    *) echo "ERROR: unknown arg: $arg" >&2; exit 1 ;;
  esac
done

# Cold-start (the default) forces seed_days=0, overriding any preset
# default or SOAK_SEED_DAYS env.  The soak then starts on an empty cpu
# table and the TSBS workload populates it from T=0 onwards.  Pass
# --warm-start to opt out and use the preset's SOAK_SEED_DAYS (which
# may further be overridden by env).
if [[ "$COLD_START" == true ]]; then
  SOAK_SEED_DAYS=0
fi

# ── Apply preset capacity defaults ───────────────────────────────────
# Each `: "${VAR:=...}"` is a "set-if-unset" — user env / CLI already
# pinned a value wins; otherwise the preset default applies; otherwise
# conf/soak_params.sh fills in below.
case "$PRESET" in
  smoke)
    : "${DURATION:=5m}"
    : "${SOAK_SEED_DAYS:=1}"
    : "${SOAK_SEED_SCALE:=100}"
    : "${SOAK_WORKLOAD_SCALE:=100}"
    : "${SOAK_COMPRESS_AFTER:=2 min}"
    : "${SOAK_COMPRESS_SCHEDULE:=1 min}"
    # Shrink monitor cadences so each loop fires at least once in 5 min.
    : "${SOAK_VIEW_CORRECTNESS_EVERY:=120}"
    : "${SOAK_PERF_PROBE_EVERY:=60}"
    : "${SOAK_LATE_ARRIVAL_EVERY:=120}"
    : "${SOAK_DDL_CHURN_EVERY:=120}"
    ;;
  recommended|default|prod|bp)
    : "${DURATION:=24h}"
    : "${SOAK_SEED_DAYS:=1}"           # ignored when cold-start (the default below)
    # scale=500 fits a 24h run comfortably inside a ~100 GB host disk
    # budget.  Empirically scale=4000 hit 71% disk_pct at just 8h (peak
    # ~89 GB on a 126 GB filesystem), so 24h at 4000 would blow past
    # the SOAK_DISK_KILL_PCT=90 panic gate.  500 keeps cardinality
    # production-meaningful while leaving room for PAX accumulation,
    # mat-table growth, WAL and the workload's working set.
    : "${SOAK_SEED_SCALE:=500}"
    : "${SOAK_WORKLOAD_SCALE:=500}"
    # Compress cadence: aggressive enough to fire ~144 events in 24h
    # and frequently race with refresh BGWs, but not so aggressive
    # that chunks live PAX-only.  Real customers run compress_after
    # at days/weeks; soak compresses time to surface compress-path
    # bugs in 24h that customers would only meet after a month.
    : "${SOAK_COMPRESS_AFTER:=30 min}"
    : "${SOAK_COMPRESS_SCHEDULE:=10 min}"
    # Monitor cadences = conf/soak_params.sh defaults (set for 24h runs,
    # comfortable at 12h too).
    PRESET=recommended    # normalize aliases for the manifest
    ;;
  *)
    echo "ERROR: unknown --preset='$PRESET' (valid: smoke, recommended)" >&2
    exit 1 ;;
esac
SOAK_RUN_PRESET="$PRESET"

# --scale=N is a convenience that pins both seed and workload scale to
# the same value (which is what 99% of users want).  Set AFTER the
# preset so it overrides the preset's defaults.
if [[ -n "$SCALE_ARG" ]]; then
  SOAK_SEED_SCALE="$SCALE_ARG"
  SOAK_WORKLOAD_SCALE="$SCALE_ARG"
fi

# Best-practice set fills every parameter the env / CLI / preset
# didn't pin (set-if-unset throughout).
source "$SOAK_DIR/conf/soak_params.sh"
if [[ -n "$CHAOS_ARG" ]]; then
  if [[ "$CHAOS_ARG" != "0" && "$CHAOS_ARG" != "1" ]]; then
    echo "ERROR: --chaos accepts 0 (off) or 1 (on); legacy values 2/3 were removed when chaos became a binary switch." >&2
    echo "  All faults now live at level=1 in chaos/fault_catalog.csv." >&2
    echo "  To restrict the fault set, use SOAK_CHAOS_ONLY=<name>[,<name>...]." >&2
    exit 1
  fi
  SOAK_CHAOS_LEVEL="$CHAOS_ARG"
fi
CHAOS_LEVEL="$SOAK_CHAOS_LEVEL"
export SOAK_CHAOS_LEVEL SOAK_HOST SOAK_PORT SOAK_USER SOAK_DBS SOAK_DATA_DIR
export PGPORT="$SOAK_PORT"

parse_duration() {
  local d=$1
  case "$d" in
    *d) echo $(( ${d%d} * 86400 )) ;;
    *h) echo $(( ${d%h} * 3600 ))  ;;
    *m) echo $(( ${d%m} * 60 ))    ;;
    *s) echo "${d%s}"              ;;
    *)  echo "$d" ;;
  esac
}
DURATION_SEC=$(parse_duration "$DURATION")

# ── Results dir + escalation bus ─────────────────────────────────────
# Directory layout:
#   $RESULTS/
#     manifest.env, report.txt           run metadata + final summary
#     data/                              structured CSV (monitor + judge + workload)
#     logs/                              stdout of loops (setup, judged, workload, ...)
#     errors/                            stderr of loops (psql errors, etc.)
#     signals/                           runtime IPC (chaos_active + panic.*)
#     verdicts/                          end-of-run SLO audit (fail.* only)
#     plan_snapshots/                    perf_probe baseline plans
#     state/                             internal runtime state (not user-facing)
#       pids/                            per-loop pidfile + heartbeat (.beat)
#       judge/                           judge daemon cross-tick state
#       cursors/                         scrape cursors, db_oid pins, cycle counters
RESULTS="${SOAK_RESULTS:-$SOAK_RESULTS_BASE/SOAK-$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$RESULTS"/{data,logs,errors,signals,verdicts,plan_snapshots,state/pids,state/judge,state/cursors}

# Chaos coordination files — under the run's signals dir so each run is
# isolated and view_check finds them by convention.
export CHAOS_ACTIVE_FLAG="$RESULTS/signals/chaos_active"
export CHAOS_CSV="$RESULTS/data/chaos_log.csv"

# ── Banner ───────────────────────────────────────────────────────────
soak_log "===== SOAK ====="
soak_log "  preset         = $PRESET"
soak_log "  duration       = $DURATION ($DURATION_SEC s)"
soak_log "  results        = $RESULTS"
soak_log "  port           = $SOAK_PORT"
soak_log "  databases      = $SOAK_DBS"
soak_log "  workload       = TSBS scale=${SOAK_WORKLOAD_SCALE}, ${SOAK_WORKLOAD_INTERVAL:-1}s/batch, per DB"
if [[ "$COLD_START" == true || "$SOAK_SEED_DAYS" -eq 0 ]]; then
  soak_log "  seed           = NONE (cold-start — empty cpu table at T=0)"
else
  soak_log "  seed           = ${SOAK_SEED_DAYS}d × scale ${SOAK_SEED_SCALE}"
fi
soak_log "  compress       = compress_after=${SOAK_COMPRESS_AFTER}, schedule=${SOAK_COMPRESS_SCHEDULE}"
soak_log "  disk kill      = at ${SOAK_DISK_KILL_PCT}% (0 = disabled)"
if [[ "$CHAOS_LEVEL" -eq 0 ]]; then
  soak_log "  chaos          = disabled (SOAK_CHAOS_LEVEL=0)"
else
  soak_log "  chaos          = ON, ~${SOAK_CHAOS_AVG_INTERVAL}s/fault ± 1/${SOAK_CHAOS_JITTER_DIV} jitter — gp_inject_fault + BGW kill"
fi

trap 'soak_log "shutdown requested"; stop_all_loops "$RESULTS"; exit 3' INT TERM

# ── 1. SETUP — per-DB ────────────────────────────────────────────────
# Sequential, not parallel: TSBS seed is disk-bound and concurrent
# seeds would thrash the page cache.
if [[ "$SKIP_SETUP" == true ]]; then
  soak_log "[setup] SKIPPED (--skip-setup); reusing existing DBs: $SOAK_DBS"
else
  for SOAK_DB in $SOAK_DBS; do
    export SOAK_DB
    soak_log "[setup] preparing $SOAK_DB"

    # WITH (FORCE) terminates lingering BGW workers from previous runs;
    # without it DROP fails and the subsequent CREATE collides.
    soak_psql -d postgres -c "DROP DATABASE IF EXISTS $SOAK_DB WITH (FORCE);" \
      >/dev/null 2>&1 || true
    psql -h "$SOAK_HOST" -p "$SOAK_PORT" -U "$SOAK_USER" -d postgres \
         -X -v ON_ERROR_STOP=1 \
         -c "CREATE DATABASE $SOAK_DB;" \
      || { soak_log "FATAL: CREATE DATABASE $SOAK_DB failed"; exit 1; }

    soak_log "[setup:$SOAK_DB] 01_extension.sql"
    soak_psql_file "$SOAK_DIR/setup/01_extension.sql" \
      -v joblog_schedule="${SOAK_JOBLOG_SCHEDULE:-1 hour}" \
      -v joblog_drop_after="${SOAK_JOBLOG_DROP_AFTER:-7 days}" \
      >> "$RESULTS/logs/setup.log" 2>&1 \
      || { soak_log "FATAL: 01_extension.sql failed for $SOAK_DB — see $RESULTS/logs/setup.log"; exit 1; }

    # 02 always runs: it creates the cpu / tags schema unconditionally,
    # and skips the TSBS data-streaming part when SOAK_SEED_DAYS=0
    # (cold-start).  03_caggs.sql below needs cpu to exist.
    if [[ "$SOAK_SEED_DAYS" -eq 0 ]]; then
      soak_log "[setup:$SOAK_DB] 02_seed_via_tsbs.sh (schema only, cold-start)"
    else
      soak_log "[setup:$SOAK_DB] 02_seed_via_tsbs.sh (TSBS generate + load seed)"
    fi
    bash "$SOAK_DIR/setup/02_seed_via_tsbs.sh" >> "$RESULTS/logs/setup.log" 2>&1 \
      || { soak_log "FATAL: 02_seed_via_tsbs.sh failed for $SOAK_DB — see $RESULTS/logs/setup.log"; exit 1; }

    soak_log "[setup:$SOAK_DB] 03_caggs.sql (compress_after=${SOAK_COMPRESS_AFTER}, schedule=${SOAK_COMPRESS_SCHEDULE})"
    soak_psql_file "$SOAK_DIR/setup/03_caggs.sql" \
      -v compress_after="${SOAK_COMPRESS_AFTER}" \
      -v compress_schedule="${SOAK_COMPRESS_SCHEDULE}" \
      >> "$RESULTS/logs/setup.log" 2>&1 \
      || { soak_log "FATAL: 03_caggs.sql failed for $SOAK_DB — see $RESULTS/logs/setup.log"; exit 1; }
  done
fi

# Record each DB's OID — view_check's environment-integrity guard
# compares against these to detect an external drop/recreate (the
# 2026-06-09 isolation2 incident burned 16 h of a 24 h run before the
# zombie cycles were noticed).
for SOAK_DB in $SOAK_DBS; do
  OID=$(PGOPTIONS='--client-min-messages=warning' \
    psql -h "$SOAK_HOST" -p "$SOAK_PORT" -U "$SOAK_USER" -d "$SOAK_DB" -X -At \
         -c "SELECT oid FROM pg_database WHERE datname = current_database();" 2>/dev/null) || OID=""
  [[ -n "$OID" ]] && echo "$OID" > "$RESULTS/state/cursors/db_oid_${SOAK_DB}"
done

# ── Preflight: max_worker_processes capacity check ──────────────────
# Even at scale=500, with 5 CAGG refresh policies + 1 compress policy per
# DB + ddl_churn creating extra cv_scratch CAGGs, every compress
# schedule tick fires ~25 jobs simultaneously.  PG's default
# max_worker_processes=14 (gpdemo) leaves ~9 BGWs for business jobs
# after system reservations, so the BGW pool runs dry and the
# scheduler logs "failed to launch job N: failed to start a
# background worker" (scheduler.c:522).  Compress fails most because
# refresh policies queue first.
#
# 32 is the recommended floor for soak workloads: covers the 25-job
# peak with ~25% headroom for autovacuum / logical_repl / launcher.
FIRST_DB_PRE="${SOAK_DBS%% *}"
MWP=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$SOAK_HOST" -p "$SOAK_PORT" -U "$SOAK_USER" \
       -d "$FIRST_DB_PRE" -X -At -c "SHOW max_worker_processes;" 2>/dev/null \
  || echo 0)
if [[ "$MWP" -lt 32 ]]; then
  soak_log "  ⚠ WARN max_worker_processes=$MWP < 32 — BGW pool will exhaust under recommended preset."
  soak_log "       Recommended: edit each postgresql.conf, set 'max_worker_processes = 32',"
  soak_log "       then gpstop -a -M fast && gpstart -a, then re-run soak."
  soak_log "       (smoke preset may still run; recommended preset will see ~60% compress failures)"
else
  soak_log "  max_worker_processes = $MWP (≥ 32 floor, ✓)"
fi

# Auto-detect data directory for system_metrics df sampling.
FIRST_DB="${SOAK_DBS%% *}"
DATA_DIR_FROM_PG=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$SOAK_HOST" -p "$SOAK_PORT" -U "$SOAK_USER" \
       -d "$FIRST_DB" -X -At -c "SHOW data_directory;" 2>/dev/null)
if [[ -n "$DATA_DIR_FROM_PG" && -d "$DATA_DIR_FROM_PG" ]]; then
  export SOAK_DATA_DIR="$DATA_DIR_FROM_PG"
  soak_log "  data_directory = $SOAK_DATA_DIR (auto-detected)"
else
  soak_log "  data_directory auto-detect failed; using SOAK_DATA_DIR=$SOAK_DATA_DIR"
fi
# gpstart (called by chaos/chaos_loop.sh::recover_cluster after
# cluster_crash) needs COORDINATOR_DATA_DIRECTORY in the environment.
# The coordinator's data_directory IS SOAK_DATA_DIR (psql above talks
# to coordinator port), so promote it here — otherwise the recover
# step fails with "Environment Variable COORDINATOR_DATA_DIRECTORY
# not set!" and the run aborts on panic.chaos.  User-provided value
# wins; only auto-detected paths are exported when unset.
export COORDINATOR_DATA_DIRECTORY="${COORDINATOR_DATA_DIRECTORY:-$SOAK_DATA_DIR}"

# Raise the core limit on the ALREADY-RUNNING postmasters.  A segment
# that dies on SIGSEGV is the most valuable artifact a soak can produce
# and the hardest to get after the fact: cores are only written if the
# limit was raised BEFORE the crash.  In SOAK-20260725_163359 that meant
# prlimit-ing three live postmasters by hand and then waiting for the
# next crash of a per-minute loop to catch one.
#
# prlimit(1) mutates a live process's limits, which children inherit at
# fork(), so this covers backends started from here on without a restart.
# Entirely best-effort: prlimit may be absent, or the postmasters may
# belong to another user.
if [[ "${SOAK_ENABLE_CORES:-1}" == "1" ]] && command -v prlimit >/dev/null 2>&1; then
  CORE_PIDS=$(pgrep -x postgres 2>/dev/null | head -32)
  CORE_OK=0
  for p in $CORE_PIDS; do
    prlimit --pid "$p" --core=unlimited:unlimited 2>/dev/null && CORE_OK=$((CORE_OK+1))
  done
  soak_log "  core limit raised on $CORE_OK postmaster/backend process(es) (SOAK_ENABLE_CORES=0 to skip)"
fi

# ── 2. MANIFEST — freeze the merged parameter set ────────────────────
{
  echo "# soak run manifest — written by soak.sh, sourced by tools/report.sh"
  echo "SOAK_RUN_DURATION='$DURATION ($DURATION_SEC s)'"
  echo "SOAK_RUN_PRESET='$SOAK_RUN_PRESET'"
  echo "SOAK_RUN_STARTED='$(date -u '+%Y-%m-%dT%H:%M:%SZ')'"
  # Pin the running code's identity in manifest.  Containers receive
  # source via `docker cp` without `.git`, so git rev-parse falls
  # through to a content fingerprint over the soak harness scripts +
  # SQL (sorted, sha256-truncated).  Format makes the fallback
  # self-identifying: 'git:<sha>' vs 'sha256:<prefix>' vs 'unknown'.
  echo "SOAK_RUN_GIT_SHA='$(
    # Subshell — disable pipefail locally so find/sha256sum sub-stage
    # exits (e.g. a future missing optional dir) do not kill the
    # fingerprint pipeline through inherited "set -o pipefail".
    set +o pipefail
    cd "$SOAK_DIR" 2>/dev/null || exit
    if sha="$(git rev-parse --short HEAD 2>/dev/null)" && [[ -n "$sha" ]]; then
      printf 'git:%s' "$sha"
    elif sha="$(find conf monitor setup tools *.sh -type f \( -name '*.sh' -o -name '*.sql' -o -name '*.py' \) 2>/dev/null \
                | sort | xargs sha256sum 2>/dev/null | sha256sum | cut -c1-12)" && [[ -n "$sha" ]]; then
      printf 'sha256:%s' "$sha"
    else
      printf 'unknown'
    fi
  )'"
  env | grep '^SOAK_' | sort | sed "s/'/'\\\\''/g; s/=/='/; s/\$/'/"
} > "$RESULTS/manifest.env"

# ── 3. RUN — start every discovered loop ─────────────────────────────
soak_log "[run] starting loops (SOAK-LOOP auto-discovery)"
start_loops "$SOAK_DIR" "$RESULTS"

# ── 4. WAIT — health-gated ───────────────────────────────────────────
# Wake every minute to poll the escalation bus.  A panic.* flag means
# the run can no longer produce valid signal (DBs gone, disk full,
# cluster unrecoverable) — stop now, keep the partial data, report.
soak_log "[run] waiting ${DURATION_SEC}s (health gate: signals/panic.*)"
END_EPOCH=$(( $(date +%s) + DURATION_SEC ))
EARLY_STOP=""
GATE_EVERY="${SOAK_HEALTH_GATE_EVERY:-60}"
while true; do
  NOW=$(date +%s)
  [[ "$NOW" -ge "$END_EPOCH" ]] && break
  REMAIN=$(( END_EPOCH - NOW ))
  sleep $(( REMAIN < GATE_EVERY ? REMAIN : GATE_EVERY ))
  # Framework self-monitoring: flag any silently-dead oneshot loop.
  check_loop_beats "$SOAK_DIR" "$RESULTS"
  for pf in "$RESULTS"/signals/panic.*; do
    [[ -f "$pf" ]] || continue
    EARLY_STOP="$(basename "$pf")"
    break 2
  done
done
if [[ -n "$EARLY_STOP" ]]; then
  soak_log "[run] EARLY STOP: $EARLY_STOP raised — $(head -1 "$RESULTS/signals/$EARLY_STOP" 2>/dev/null)"
  # Snapshot the crime scene NOW.  A panic means the run has already
  # failed; the transient state that explains it (ungranted locks, chunk
  # catalog, ".new" vs live PAX sizes, log tails) is worth far more at
  # this instant than after the loops have been torn down.  Diagnosing
  # SOAK-20260725_163359 cost a day largely because this evidence was
  # collected by hand, hours later.
  AUTOPSY_DIR=$(bash "$SOAK_DIR/tools/autopsy.sh" "$RESULTS" "$EARLY_STOP" 2>/dev/null | tail -1)
  [[ -n "$AUTOPSY_DIR" ]] && soak_log "[run] autopsy captured → $AUTOPSY_DIR"
fi

# ── 5. STOP ──────────────────────────────────────────────────────────
soak_log "[stop] stopping loops"
stop_all_loops "$RESULTS"

# ── 6. REPORT ────────────────────────────────────────────────────────
bash "$SOAK_DIR/tools/report.sh" "$RESULTS" > /dev/null \
  || soak_log "[report] WARNING: report generation had errors"
soak_log "[done] report → $RESULTS/report.txt"

# ── 7. EXIT ──────────────────────────────────────────────────────────
# Verdicts (fail.*) and signals (panic.*) are all captured on
# disk under $RESULTS/verdicts/ and $RESULTS/signals/.  The exit code
# only carries completion status — check those directories (or
# report.txt) for the actual SLO verdict.
soak_log "[exit] run complete — see \"$RESULTS/report.txt\" and \"$RESULTS/verdicts/\""
exit 0
