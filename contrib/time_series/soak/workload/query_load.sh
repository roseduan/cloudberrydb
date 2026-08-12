#!/bin/bash
# workload/query_load.sh
# SOAK-LOOP: scope=per-db mode=self order=18
#
# Continuous CONCURRENT READ load against the CAGG views — the one
# workload class the framework was missing.  Until now soak only ever
# WROTE (forward insert, late-arrival, ddl churn); user-facing reads
# were sampled once every 5 min by perf_probe, which exerts no real
# concurrency.  Production is read-WHILE-write-WHILE-refresh: multiple
# dashboards hammering the views while BGW refresh/compress run.  That
# read↔refresh contention on shared catalogs (cagg_watermark,
# bgw_job_stat, L1/L2) is exactly the soil the 2026-06 watermark-freeze
# / bgw_job_stat-lock bug grew in — a soak with no read pressure can
# barely reproduce that class.  This loop adds it.
#
# Design:
#   - Forks SOAK_QUERY_LOAD_CONCURRENCY resident workers; each loops a
#     rotating set of realistic dashboard queries (count / aggregate /
#     fetch+ORDER+LIMIT / top-N).  Real SELECTs (not EXPLAIN) so rows
#     actually materialize and ship → genuine read pressure.
#   - This is WORKLOAD, not a verdict source: perf_probe still owns the
#     precise server-side latency / plan-drift signal.  Here we only
#     tally throughput + failure rate (a query erroring under load IS a
#     signal) and a coarse wall-clock latency.  We never raise fail.* —
#     a slow/failed read is reported, not run-aborting.
#   - To keep the CSV bounded under a 24h run, each worker flushes ONE
#     aggregate row per SOAK_QUERY_LOAD_FLUSH queries instead of one row
#     per query.
#
# Tunables (env):
#   SOAK_QUERY_LOAD_CONCURRENCY  resident reader workers per DB (default 4)
#   SOAK_QUERY_LOAD_PACE_MS      sleep between a worker's queries (default 100)
#   SOAK_QUERY_LOAD_FLUSH        queries per aggregate CSV row (default 25)
#   SOAK_QUERY_LOAD_SLOW_MS      wall-clock "slow query" threshold (default 2000)
#   Connection: SOAK_HOST/PORT/USER/DB (set by soak.sh)
#
# CSV: ts, db, worker, queries, fails, slow, sum_ms   (one row per flush)
#
# Usage (invoked by lib/loops.sh; mode=self → runs forever):
#   SOAK_DB=<db> query_load.sh <results_dir>
#   query_load.sh --report <results_dir>            print report section

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

# ── Report mode ──────────────────────────────────────────────────────
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: query_load.sh --report <results_dir>}"
  CSV="$RESULTS/data/query_load.csv"
  echo "── query load (concurrent CAGG reads) ──"
  if [[ ! -s "$CSV" ]]; then
    echo "  (no query_load.csv — SOAK_QUERY_LOAD_CONCURRENCY=0 or loop didn't run)"
    exit 0
  fi
  # Chaos-aware (fault × judge whitelist): reads legitimately fail while
  # the instance is down or recovering, so those windows are excluded from
  # the WARN decision.
  #
  # ALL FOUR faults are whitelisted.  The earlier comment here claimed
  # "BGW kills leave reads untouched" -- that is false: SIGKILL on a
  # shmem-attached worker makes postmaster reset the instance, which drops
  # every client connection.  SOAK-20260728_095744 proved it: all 1031
  # read errors were instance-reset signatures ("the database system is in
  # recovery mode" ×865, "terminating connection because of crash of
  # another server process" ×59, ...) with zero query-logic failures, yet
  # 0.10% were reported as non-chaos read regressions.
  source "$SOAK_DIR/judge/lib/chaos.sh" 2>/dev/null || true
  CW=""
  if type chaos_windows_str >/dev/null 2>&1; then
    # Pre-grace: a CSV row is an aggregate over SOAK_QUERY_LOAD_FLUSH
    # queries stamped at FLUSH time, so a failure early in the batch can
    # predate the row's timestamp by the batch's whole duration.
    CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                            "$CHAOS_ALL_FAULTS" \
                            "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" \
                            "${SOAK_QUERY_LOAD_PRE_GRACE_SEC:-120}" 2>/dev/null)
  fi
  awk -F, -v windows="$CW" '
    function in_chaos(ts,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    NR>1 {
      db=$2
      q[db]+=$4; f[db]+=$5; s[db]+=$6; ms[db]+=$7
      w[db ":" $3]=1
      if (!in_chaos($1)) { q_nc[db]+=$4; f_nc[db]+=$5 }
    }
    END {
      for (d in q) {
        nw=0; for (k in w) { split(k,a,":"); if (a[1]==d) nw++ }
        avg = (q[d]>0) ? ms[d]/q[d] : 0
        failpct = (q[d]>0) ? f[d]*100.0/q[d] : 0
        slowpct = (q[d]>0) ? s[d]*100.0/q[d] : 0
        printf "  %-14s workers=%d queries=%d fails=%d (%.2f%%) slow=%d (%.1f%%) avg=%.0fms\n",
               d, nw, q[d], f[d], failpct, s[d], slowpct, avg
        tot_q+=q[d]; tot_f+=f[d]
        tq_nc+=q_nc[d]; tf_nc+=f_nc[d]
      }
      nc_pct = (tq_nc>0) ? tf_nc*100.0/tq_nc : 0
      raw_pct = (tot_q>0) ? tot_f*100.0/tot_q : 0
      if (tq_nc>0 && nc_pct >= 1.0)
        printf "  ⚠ WARN — read failure rate %.2f%% OUTSIDE chaos windows (raw %.2f%%): reads erroring under load\n", nc_pct, raw_pct
      else
        printf "  ✓ OK — concurrent reads served (failure rate %.2f%% outside chaos windows; raw %.2f%%)\n", nc_pct, raw_pct
    }' "$CSV"
  exit 0
fi

# ── Collection / load mode ───────────────────────────────────────────
DB="${SOAK_DB:?query_load.sh needs SOAK_DB (per-db self loop)}"
RESULTS="${1:?usage: query_load.sh <results_dir>}"
HOST="${SOAK_HOST:-localhost}"; PORT="${SOAK_PORT:?}"; USER="${SOAK_USER:-gpadmin}"
CSV="$RESULTS/data/query_load.csv"
CONC="${SOAK_QUERY_LOAD_CONCURRENCY:-4}"
PACE_MS="${SOAK_QUERY_LOAD_PACE_MS:-100}"
FLUSH="${SOAK_QUERY_LOAD_FLUSH:-25}"
SLOW_MS="${SOAK_QUERY_LOAD_SLOW_MS:-2000}"

[[ "$CONC" -le 0 ]] && { soak_log "[query_load] disabled (CONCURRENCY=0)"; exit 0; }
[[ -f "$CSV" ]] || echo "ts,db,worker,queries,fails,slow,sum_ms" > "$CSV"

# Realistic dashboard read mix — count, aggregate, fetch+order+limit,
# top-N.  Real SELECTs so rows materialize and ship (true read load).
QUERIES=(
  "SELECT count(*) FROM public.cv_1min WHERE bucket >= now() - INTERVAL '1 hour';"
  "SELECT count(*), avg(avg_user) FROM public.cv_1hour WHERE bucket >= now() - INTERVAL '24 hours';"
  "SELECT bucket, tags_id, cnt, avg_user FROM public.cv_1hour WHERE bucket >= now() - INTERVAL '24 hours' ORDER BY bucket DESC, tags_id LIMIT 500;"
  "SELECT tags_id, max(max_system) FROM public.cv_5min WHERE bucket >= now() - INTERVAL '2 hours' GROUP BY tags_id ORDER BY 2 DESC LIMIT 20;"
)
NQ=${#QUERIES[@]}

run_worker() {
  local wid="$1" i=0 n=0 fails=0 slow=0 sum=0 q t0 t1 dt rc
  while true; do
    q="${QUERIES[$(( i % NQ ))]}"
    i=$(( i + 1 ))
    t0=$(date +%s%3N)
    PGOPTIONS='--client-min-messages=warning -c optimizer=off -c search_path=public,time_series' \
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -X -At -v ON_ERROR_STOP=1 \
           -c "$q" > /dev/null 2>>"$RESULTS/errors/query_load.err"
    rc=$?
    t1=$(date +%s%3N); dt=$(( t1 - t0 ))
    n=$(( n + 1 )); sum=$(( sum + dt ))
    [[ $rc -ne 0 ]] && { fails=$(( fails + 1 )); soak_log "[query_load:$DB:w$wid] WARN query rc=$rc"; }
    [[ $dt -ge $SLOW_MS ]] && slow=$(( slow + 1 ))
    if [[ $n -ge $FLUSH ]]; then
      printf '%s,%s,%s,%d,%d,%d,%d\n' \
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$DB" "$wid" "$n" "$fails" "$slow" "$sum" >> "$CSV"
      n=0; fails=0; slow=0; sum=0
    fi
    [[ $PACE_MS -gt 0 ]] && sleep "$(awk -v m="$PACE_MS" 'BEGIN{printf "%.3f", m/1000}')"
  done
}

soak_log "[query_load] starting $CONC concurrent readers on $DB (pace=${PACE_MS}ms, flush=${FLUSH})"
for w in $(seq 1 "$CONC"); do
  run_worker "$w" &
done
wait
