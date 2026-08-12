#!/bin/bash
# judge/verdicts/refresh_perf.sh
# Verdict: CAGG refresh BGW duration vs schedule_interval.
# Migrated verbatim from monitor/refresh_perf_scrape.sh --report (2026-07-08).
#   p95 < schedule                → ✓ OK
#   p95 in [schedule, 2×schedule] → ⚠ WARN (scheduler-kill territory)
#   p95 > 2×schedule              → 🔴 CRITICAL → verdicts/fail.perf
#
# CHAOS-AWARE (2026-07-10): durations sampled inside a chaos window are
# excluded from the percentile (a cold post-recovery run must not inflate
# p95), and no fail/WARN is raised until there are SOAK_JUDGE_MIN_SAMPLES
# non-chaos durations — p95 over a handful of samples is meaningless.

_judge_eval_refresh_perf() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/refresh_durations.csv"
  mkdir -p "$RESULTS/verdicts"
  local VERDICT_FILE="$RESULTS/verdicts/fail.perf.refresh"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if [[ ! -s "$CSV" ]]; then
    echo "  (no refresh_durations.csv — first scrape cycle hasn't fired yet)"
    return 0
  fi
  # CARE: faults that can realistically slow / crash a refresh worker.
  # compress_worker_kill is UNRELATED — refresh_worker keeps running.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  awk -F, -v verdict_file="$VERDICT_FILE" -v now="$NOW" \
      -v crit_mult="${SOAK_BGW_P95_CRIT_MULT:-2}" \
      -v windows="$CW" -v min_n="${SOAK_JUDGE_MIN_SAMPLES:-10}" \
      -v fail_pct="${SOAK_BGW_FAIL_PCT:-20}" '
    function in_chaos(ts,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    BEGIN {
      # Per-CAGG schedule_interval in ms — SYNC CONSTANT with
      # setup/03_caggs.sql policy definitions (test-subject layout,
      # deliberately not env-tunable; see soak_params.sh footer).
      sched["cv_1min"]  =   60 * 1000;
      sched["cv_5min"]  =  300 * 1000;
      sched["cv_1hour"] = 3600 * 1000;
    }
    NR == 1 { next }
    {
      # cols: ts,db,id,job_id,cagg_name,execution_start,duration_ms,succeeded,is_crashed,error
      key = $2 "/" $5;
      if (in_chaos($1)) { cskip[key]++; next }   # chaos-window run: excluded from p95 + verdict
      n[key]++;
      d = $7 + 0;
      if ($9 == "true" || $9 == "t") {
        crashed[key]++;
      } else if ($8 == "false" || $8 == "f") {
        # Failed runs (e.g. "failed to start job" retries at ~200ms) must
        # NOT contaminate the latency percentiles — 249 fast failures once
        # dragged a real p50 of ~40s down to 224ms (SOAK-20260721_161133).
        ;
      } else if (d >= 0) {
        dur[key, n[key]] = d;
        if (d > max[key]) max[key] = d;
        cn = $5; sub(/^[^.]*\./, "", cn);   # strip schema prefix (public.cv_1min → cv_1min)
        if (cn in sched && d > sched[cn]) slow_runs[key]++;
      }
      if ($8 == "false" || $8 == "f") failed[key]++;
    }
    END {
      for (k in n) {
        split(k, p, "/");
        db = p[1]; cn = p[2];
        scn = cn; sub(/^[^.]*\./, "", scn);   # schedule-lookup key: strip schema prefix
        delete a;
        j = 0;
        for (i = 1; i <= n[k]; i++) {
          if ((k, i) in dur) { j++; a[j] = dur[k, i]; }
        }
        for (i = 2; i <= j; i++) {
          v = a[i]; t = i;
          while (t > 1 && a[t-1] > v) { a[t] = a[t-1]; t--; }
          a[t] = v;
        }
        p50 = a[int(j*0.50)+0]; if (p50 == "") p50 = a[1];
        p95 = a[int(j*0.95)+0]; if (p95 == "") p95 = a[j];
        p99 = a[int(j*0.99)+0]; if (p99 == "") p99 = a[j];
        mx = max[k]+0;
        fl = failed[k]+0;
        cr = crashed[k]+0;
        kl = slow_runs[k]+0;
        v = "✓ OK";
        if (scn in sched) {
          s = sched[scn];
          if (j < min_n) {
            v = sprintf("✓ OK (only %d non-chaos samples; need %d to judge p95)", j, min_n);
          } else if (p95 > crit_mult * s) {
            v = sprintf("🔴 CRITICAL (p95 %.0fms > %s× schedule %.0fms)", p95, crit_mult, s);
            printf "%s db=%s cv=%s p95=%.0fms schedule=%.0fms ratio=%.1fx\n",
                   now, db, cn, p95, s, p95/s >> verdict_file;
          } else if (p95 > s) {
            v = sprintf("⚠ WARN (p95 %.0fms > schedule %.0fms, but ≤ %s×)", p95, s, crit_mult);
          }
        }
        # Fail-rate verdict — independent of schedule knowledge, so it
        # also covers jobs whose cagg name did not resolve ("?"): the
        # 881 "failed to start job" rows of SOAK-20260721_161133 sat
        # under a green verdict because only p95 was ever judged.
        fr = (n[k] > 0) ? failed[k] * 100.0 / n[k] : 0;
        if (n[k] >= min_n && fr >= fail_pct) {
          fmsg = sprintf("🔴 CRITICAL (fail rate %.0f%% >= %d%% of %d non-chaos runs)", fr, fail_pct, n[k]);
          v = (v == "✓ OK") ? fmsg : v " ; " fmsg;
          printf "%s db=%s cv=%s failed=%d of=%d rate=%.0f%%\n",
                 now, db, cn, failed[k]+0, n[k], fr >> verdict_file;
        }
        printf("  %-50s n=%-6d p50=%-7.1fms p95=%-7.1fms p99=%-7.1fms max=%-7.1fms fail=%d crash=%d slow_runs=%d chaos_excl=%d  %s\n",
               db "/" cn, n[k], p50, p95, p99, mx, fl, cr, kl, cskip[k]+0, v);
      }
    }
  ' "$CSV" | sort
}

judge_run_refresh_perf()    { _judge_eval_refresh_perf "$1" >/dev/null 2>&1; }
judge_report_refresh_perf() {
  echo "── refresh perf (from bgw_job_stat_history) ──"
  _judge_eval_refresh_perf "$1"
}
