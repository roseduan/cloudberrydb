#!/bin/bash
# workload/ddl_churn.sh
# SOAK-LOOP: scope=per-db interval=900 interval_var=SOAK_DDL_CHURN_EVERY order=60
#
# Periodically DROP + CREATE a scratch CAGG to exercise CAGG construction
# DDL paths under concurrent workload + chaos.
#
# Why this exists:
#   - fault_catalog has a `cagg_create_before_trigger_install` entry,
#     but the soak's 3 long-lived CAGGs are created once during setup
#     and never re-created.  That fault point sat idle for 3 rounds.
#     This loop guarantees a steady stream of create-CAGG executions
#     while workload + chaos run, so the fault can actually fire.
#   - Recent commits (f35f4d8b1c9 catalog sync on ALTER SET SCHEMA;
#     558c8d96f21 CAGG refresh deadlocks + BGW test stability) touched
#     the CAGG lifecycle.  Re-creating CAGGs while INSERT trigger
#     installation is concurrent with workload exercises those paths.
#
# What it does (each cycle, per DB):
#   1. DROP MATERIALIZED VIEW IF EXISTS cv_scratch CASCADE
#   2. CREATE MATERIALIZED VIEW cv_scratch WITH (time_series.continuous) AS
#      SELECT time_bucket('5 min', time), tags_id, count(*) FROM cpu GROUP BY ...
#   3. Add a refresh policy (so the BGW path is exercised too)
#   4. Wait DDL_CHURN_EVERY seconds; repeat
#
# Scratch CAGG is sized small (5-min bucket, no compression policy)
# so it doesn't materially affect soak resource usage.  It shares the
# cpu source with the real CAGGs, which is what we want — concurrent
# trigger installation against an actively-written-to table is the
# exact code path commits 558c8d96f21 / f35f4d8b1c9 stabilised.
#
# Usage (invoked by lib/loops.sh):
#   ddl_churn.sh <db> <results_dir>          one drop+create+policy cycle
#   ddl_churn.sh --report <results_dir>      print report section

set -uo pipefail

# ── Report mode ──────────────────────────────────────────────────────
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: ddl_churn.sh --report <results_dir>}"
  CSV="$RESULTS/data/ddl_churn.csv"
  echo "── ddl churn (CAGG create/drop under load) ──"
  if [[ -s "$CSV" ]]; then
    # Chaos-aware fail attribution.  ddl_churn phases that ran ACROSS a
    # chaos event are excluded from the fail count -- they are chaos
    # noise, not a DDL correctness issue.
    #
    # ALL FOUR faults are whitelisted, not just cluster_crash: a SIGKILL
    # on any shmem-attached background worker makes postmaster reset the
    # whole instance, so a scheduler_kill or compress_worker_kill breaks
    # in-flight DDL exactly like a full crash does (see judge/lib/chaos.sh).
    #
    # PRE-GRACE is essential here: the CSV timestamp is captured once at
    # the START of a cycle and shared by all four phases, so a cycle that
    # began before the INJECT but was still running when the fault landed
    # would otherwise be attributed to "non-chaos".  That is exactly what
    # produced the 4 false failures of SOAK-20260728_095744 (cycle stamped
    # 14:37:29, cleanup ran 24.6 s, kill at 14:37:54).  The pre-grace must
    # cover the longest a cycle can take.
    JUDGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../judge" && pwd)"
    if [[ -f "$JUDGE_DIR/lib/chaos.sh" ]]; then
      # shellcheck disable=SC1090
      source "$JUDGE_DIR/lib/chaos.sh"
      CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                             "$CHAOS_ALL_FAULTS" \
                             "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" \
                             "${SOAK_DDL_CHURN_PRE_GRACE_SEC:-180}" 2>/dev/null)
    else
      CW=""
    fi

    awk -F, -v windows="$CW" '
      function in_chaos(ts,  i,a,n,se){ if(windows==""){return 0}
        n=split(windows,a," ");
        for(i=1;i<=n;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
      NR>1 {
        k=$2 "/" $3
        n[k]++
        if ($5 != 0) {
          if (in_chaos($1)) chaos_fail[k]++
          else              fail[k]++
        }
      }
      END {
        total_fail = 0; total_chaos_fail = 0
        for (k in n) {
          extra = (chaos_fail[k]+0 > 0) ? sprintf(" chaos_excl=%d", chaos_fail[k]) : ""
          printf "  ddl_churn[%-28s] cycles=%d fail=%d%s\n",
                 k, n[k], fail[k]+0, extra
          total_fail += fail[k]+0
          total_chaos_fail += chaos_fail[k]+0
        }
        # Print summary line as the very last line so the caller can grep -q.
        printf "__SUMMARY__ total_fail=%d total_chaos_fail=%d\n", total_fail, total_chaos_fail
      }
    ' "$CSV" | { output=$(cat); non_chaos=$(echo "$output" | awk -F= '/^__SUMMARY__/ {print $2+0; exit}'); chaos_excl=$(echo "$output" | awk -F= '/^__SUMMARY__/ {print $3+0; exit}'); echo "$output" | grep -v '^__SUMMARY__' | sort; if [[ "$non_chaos" -gt 0 ]]; then echo "  ⚠ WARN — DDL churn had $non_chaos non-chaos failed phase(s); inspect ddl_churn.err"; fi; if [[ "$chaos_excl" -gt 0 ]]; then echo "  ($chaos_excl phase failure(s) excluded as chaos-window collateral)"; fi; }
  else
    echo "  no samples"
  fi
  exit 0
fi

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: ddl_churn.sh <db> <results_dir>}"
RESULTS="${2:?usage: ddl_churn.sh <db> <results_dir>}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"

CSV="$RESULTS/data/ddl_churn.csv"
ERR="$RESULTS/errors/ddl_churn.err"

# Header (once)
if [[ ! -f "$CSV" ]]; then
  echo "ts,db,phase,duration_ms,rc,detail" > "$CSV"
fi

TS_NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

run_phase() {
  local phase=$1
  local sql=$2
  local t0 t1 dur out rc
  t0=$(date +%s%3N)
  out=$(PGOPTIONS='--client-min-messages=warning' \
    psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
         -At -X -v ON_ERROR_STOP=1 -c "$sql" 2>>"$ERR")
  rc=$?
  t1=$(date +%s%3N)
  dur=$(( t1 - t0 ))
  local detail
  detail=$(echo -n "$out" | head -c 80 | tr -d ',\n' )
  printf '%s,%s,%s,%s,%s,%s\n' \
    "$TS_NOW" "$DB" "$phase" "$dur" "$rc" "$detail" >> "$CSV"
}

# 0. Cleanup — remove policy + any leftover internal catalog entries from
#    a previous cycle.  time_series event triggers should handle this on
#    DROP VIEW, but if the DROP was interrupted (chaos kill, OOM, etc.)
#    the catalog can have stale entries that block re-CREATE.
run_phase "cleanup" "
  DO \$\$ DECLARE r RECORD;
  BEGIN
    -- Remove refresh policy if it still exists.  Guard with
    -- to_regclass() because remove_continuous_aggregate_policy takes
    -- regclass since commit e42750c, and 'cv_scratch'::regclass cast
    -- ERRORs at the parameter layer when the relation is gone --
    -- before if_exists can take effect.
    IF to_regclass('public.cv_scratch') IS NOT NULL THEN
      PERFORM time_series.remove_continuous_aggregate_policy(
        'public.cv_scratch'::regclass, if_exists => true);
    END IF;
    -- Chaos-safe policy sweep (SOAK-20260724_121637 finding): after
    -- a mid-cycle cluster_crash the view can be gone while the
    -- bgw_job row for its policy lingers -- the guard above misses
    -- it, and the next 'policy' phase raises 'refresh policy
    -- already exists'.  So also nuke any orphaned bgw_job rows that
    -- reference cv_scratch by name; this is a no-op on clean state.
    FOR r IN SELECT id FROM time_series.bgw_job
              WHERE proc_name = 'policy_refresh_cagg'
                AND (config->>'cagg_name' = 'cv_scratch'
                     OR config->>'cagg_name' LIKE '%.cv_scratch')
    LOOP
      PERFORM time_series.delete_job(r.id);
    END LOOP;
    -- Remove any stale internal mat/direct-view objects.
    FOR r IN SELECT mat_table_name, direct_view_name FROM time_series.continuous_agg
              WHERE user_view_name = 'cv_scratch'
    LOOP
      IF r.mat_table_name IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS time_series.%I CASCADE', r.mat_table_name);
      END IF;
      IF r.direct_view_name IS NOT NULL THEN
        EXECUTE format('DROP VIEW IF EXISTS time_series.%I CASCADE', r.direct_view_name);
      END IF;
    END LOOP;
    -- Remove stale catalog rows.
    DELETE FROM time_series.continuous_agg WHERE user_view_name = 'cv_scratch';
  END \$\$;
" 2>/dev/null || true

# 1. DROP first (idempotent — handles fresh run + retry).
#
# Continuous aggregates are created with `CREATE MATERIALIZED VIEW ... WITH
# (time_series.continuous)`, but the extension internally registers
# cv_scratch as a regular VIEW that UNIONs a backing mat table with a
# live branch (this is the cagg_union_view).  PG's pg_class then sees
# cv_scratch as relkind='v', so DROP MATERIALIZED VIEW errors out with
# "cv_scratch is not a materialized view".  Use DROP VIEW (or, equally
# valid, DROP TABLE on the underlying mat table) — the time_series
# event trigger turns either into a full cagg teardown.
run_phase "drop" "
  DROP VIEW IF EXISTS public.cv_scratch CASCADE;
"

# 2. CREATE with continuous-agg property — this hits
#    cagg_create_before_trigger_install (refresh.c).
#    Bucket / policy timings come from conf/soak_params.sh.
run_phase "create" "
  SET search_path TO public, time_series;
  CREATE MATERIALIZED VIEW public.cv_scratch
    WITH (time_series.continuous) AS
    SELECT time_bucket('${SOAK_DDL_SCRATCH_BUCKET:-5 min}'::interval, time) AS bucket,
           tags_id,
           count(*) AS cnt
      FROM public.cpu
     GROUP BY bucket, tags_id;
"

# 3. Add policy — exercises the BGW job registration path under
#    concurrent workload.
run_phase "policy" "
  SELECT time_series.add_continuous_aggregate_policy(
    'cv_scratch',
    start_offset      => INTERVAL '${SOAK_DDL_SCRATCH_START_OFFSET:-30 min}',
    end_offset        => INTERVAL '${SOAK_DDL_SCRATCH_END_OFFSET:-5 min}',
    schedule_interval => INTERVAL '${SOAK_DDL_SCRATCH_SCHEDULE:-5 min}'
  );
"
