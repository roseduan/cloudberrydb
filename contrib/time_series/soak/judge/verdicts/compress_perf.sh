#!/bin/bash
# judge/verdicts/compress_perf.sh
# Verdict: compress BGW duration vs SOAK_COMPRESS_SCHEDULE.
# Migrated verbatim from monitor/compress_perf_scrape.sh --report (2026-07-08).
#
# CHAOS-AWARE (2026-07-10): durations sampled inside a chaos window are
# excluded from the percentile, and no fail/WARN is raised until there
# are SOAK_JUDGE_MIN_SAMPLES non-chaos durations (see refresh_perf.sh).

_judge_eval_compress_perf() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/compress_durations.csv"
  mkdir -p "$RESULTS/verdicts"
  local VERDICT_FILE="$RESULTS/verdicts/fail.perf.compress"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if [[ ! -s "$CSV" ]]; then
    echo "  (no compress_durations.csv — first scrape cycle hasn't fired or no compression policy active)"
    return 0
  fi
  local COMP_SCHED_MS
  COMP_SCHED_MS=$(echo "${SOAK_COMPRESS_SCHEDULE:-1 hour}" | awk '
    /sec/    { n=$1; print n*1000;  exit }
    /min/    { n=$1; print n*60000; exit }
    /hour/   { n=$1; print n*3600000; exit }
    /day/    { n=$1; print n*86400000; exit }
    { print 3600000; exit }   # fallback 1h
  ')
  # CARE: faults that can realistically slow / crash a compress worker.
  # refresh_worker_kill is UNRELATED — compress_worker keeps running.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  awk -F, -v sched="$COMP_SCHED_MS" -v verdict_file="$VERDICT_FILE" -v now="$NOW" \
      -v crit_mult="${SOAK_BGW_P95_CRIT_MULT:-2}" \
      -v windows="$CW" -v min_n="${SOAK_JUDGE_MIN_SAMPLES:-10}" \
      -v fail_pct="${SOAK_BGW_FAIL_PCT:-20}" '
    function in_chaos(ts,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    NR == 1 { next }
    {
      # cols: ts,db,id,job_id,table_name,execution_start,duration_ms,succeeded,is_crashed,chunks_compressed,compressed_bytes,error
      db = $2;
      if (in_chaos($1)) { cskip[db]++; next }   # chaos-window run: excluded from p95 + verdict
      n[db]++;
      d = $7 + 0;
      if ($9 == "true" || $9 == "t") {
        crashed[db]++;
      } else if ($8 == "false" || $8 == "f") {
        # Failed runs must not contaminate the latency percentiles (see
        # refresh_perf.sh — same lesson from SOAK-20260721_161133).
        ;
      } else if (d >= 0) {
        dur[db, n[db]] = d;
        if (d > max[db]) max[db] = d;
        chunks[db] += $10 + 0;
        bytes[db]  += $11 + 0;
      }
      if ($8 == "false" || $8 == "f") failed[db]++;
    }
    END {
      for (db in n) {
        delete a;
        j = 0;
        for (i = 1; i <= n[db]; i++) {
          if ((db, i) in dur) { j++; a[j] = dur[db, i]; }
        }
        for (i = 2; i <= j; i++) {
          v = a[i]; t = i;
          while (t > 1 && a[t-1] > v) { a[t] = a[t-1]; t--; }
          a[t] = v;
        }
        p50 = a[int(j*0.50)+0]; if (p50 == "") p50 = a[1];
        p95 = a[int(j*0.95)+0]; if (p95 == "") p95 = a[j];
        p99 = a[int(j*0.99)+0]; if (p99 == "") p99 = a[j];
        mx = max[db]+0;
        fl = failed[db]+0;
        cr = crashed[db]+0;
        chks = chunks[db]+0;
        bts  = bytes[db]+0;
        v = "✓ OK";
        if (j < min_n) {
          v = sprintf("✓ OK (only %d non-chaos samples; need %d to judge p95)", j, min_n);
        } else if (p95 > crit_mult * sched) {
          v = sprintf("🔴 CRITICAL (p95 %.0fms > %s× schedule %.0fms)", p95, crit_mult, sched);
          printf "%s db=%s p95=%.0fms schedule=%.0fms ratio=%.1fx\n",
                 now, db, p95, sched, p95/sched >> verdict_file;
        } else if (p95 > sched) {
          v = sprintf("⚠ WARN (p95 %.0fms > schedule %.0fms)", p95, sched);
        }
        fr = (n[db] > 0) ? fl * 100.0 / n[db] : 0;
        if (n[db] >= min_n && fr >= fail_pct) {
          fmsg = sprintf("🔴 CRITICAL (fail rate %.0f%% >= %d%% of %d non-chaos runs)", fr, fail_pct, n[db]);
          v = (v == "✓ OK") ? fmsg : v " ; " fmsg;
          printf "%s db=%s failed=%d of=%d rate=%.0f%%\n",
                 now, db, fl, n[db], fr >> verdict_file;
        }
        printf("  %-20s n=%-4d p50=%-7.1fms p95=%-7.1fms p99=%-7.1fms max=%-7.1fms fail=%d crash=%d chunks=%-6d reclaimed=%.1fMB chaos_excl=%d  %s\n",
               db, n[db], p50, p95, p99, mx, fl, cr, chks, bts/1048576, cskip[db]+0, v);
      }
    }
  ' "$CSV" | sort
}

judge_run_compress_perf()    { _judge_eval_compress_perf "$1" >/dev/null 2>&1; }
judge_report_compress_perf() {
  echo "── compress perf (from bgw_job_stat_history) ──"
  _judge_eval_compress_perf "$1"
}
