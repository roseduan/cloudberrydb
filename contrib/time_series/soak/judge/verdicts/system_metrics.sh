#!/bin/bash
# judge/verdicts/system_metrics.sh
# Verdict: disk peak soft-alarm + BGW RSS leak trend.
# Migrated verbatim from monitor/system_metrics.sh --report (2026-07-08).
#
# NOTE: the HARD disk early-stop (panic.disk_full at SOAK_DISK_KILL_PCT)
# lives in judge/verdicts/framework_health.sh — its streaming judge_run
# tick raises it (NOT this verdict, NOT the collector).  This verdict
# only does the report-time peak WARN + RSS leak trend.

_judge_eval_system_metrics() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/system_metrics.csv"
  mkdir -p "$RESULTS/verdicts"
  if [[ ! -s "$CSV" ]]; then
    echo "  (no system_metrics.csv)"
    return 0
  fi
  # CSV: ts,disk_pct,disk_used_gb,free_gb,bgw_workers,rss_total_kb,rss_max_kb
  local VERDICT_FILE="$RESULTS/verdicts/fail.perf.rss_leak"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  awk -F, \
      -v warn_pct="${SOAK_RSS_GROWTH_WARN_PCT:-30}" \
      -v fail_pct="${SOAK_RSS_GROWTH_FAIL_PCT:-100}" \
      -v min_n="${SOAK_RSS_TREND_MIN_SAMPLES:-20}" \
      -v disk_warn_pct="${SOAK_DISK_WARN_PCT:-70}" \
      -v disk_kill_pct="${SOAK_DISK_KILL_PCT:-90}" \
      -v verdict_file="$VERDICT_FILE" -v now="$NOW" '
    NR>1 {
      n++
      disk[n]=$2+0; tot[n]=$6+0; mx[n]=$7+0
      if($2+0>maxd) maxd=$2
      if($6+0>maxr) maxr=$6
      if($7+0>mxr1) mxr1=$7
    }
    END {
      printf "  samples          : %d\n", n
      # Peak disk verdict inline with the peak line — soft alarm at
      # SOAK_DISK_WARN_PCT (default 70), hard cutoff already handled
      # via panic.disk_full at SOAK_DISK_KILL_PCT.  Bridges the gap
      # between "steady 38%" and "kill 90%": a transient spike (like
      # the 2026-07-02 cold-start bulk seed peak at 78%) shows up in
      # the report instead of hiding in system_metrics.csv.
      disk_marker = "  "
      if (maxd >= disk_kill_pct) disk_marker = "🔴"
      else if (maxd >= disk_warn_pct) disk_marker = "⚠ "
      printf "  peak disk_pct    : %d%%  %s\n", maxd, \
        (maxd >= disk_kill_pct ? "CRITICAL (>= " disk_kill_pct "% kill threshold)" \
         : maxd >= disk_warn_pct ? "WARN (>= " disk_warn_pct "% soft alarm; transient spike or steady climb — check ts curve)" \
         : "")
      printf "  peak rss_total   : %d MB\n", maxr/1024
      printf "  peak rss_max     : %d MB\n", mxr1/1024
      if (n < min_n) {
        printf "  rss trend        : (only %d samples; need %d to judge a leak)\n", n, min_n
        exit
      }
      q = int(n/4); if (q < 1) q = 1
      # first-quartile means
      f_tot=0; f_mx=0; for (i=1;   i<=q; i++) { f_tot+=tot[i]; f_mx+=mx[i] }
      l_tot=0; l_mx=0; for (i=n-q+1; i<=n; i++) { l_tot+=tot[i]; l_mx+=mx[i] }
      f_tot/=q; f_mx/=q; l_tot/=q; l_mx/=q
      g_tot = (f_tot>0) ? (l_tot-f_tot)*100.0/f_tot : 0
      g_mx  = (f_mx >0) ? (l_mx -f_mx )*100.0/f_mx  : 0
      printf "  rss_max  trend   : %d → %d MB  (%+.0f%% first-Q→last-Q)\n", f_mx/1024,  l_mx/1024,  g_mx
      printf "  rss_total trend  : %d → %d MB  (%+.0f%% first-Q→last-Q, advisory)\n", f_tot/1024, l_tot/1024, g_tot
      if (g_mx >= fail_pct) {
        printf "  🔴 LEAK — rss_max grew %.0f%% (>= %d%%): likely a BGW memory leak\n", g_mx, fail_pct
        printf "%s rss_max_growth=+%.0f%% first_q=%dMB last_q=%dMB threshold=+%d%%\n",
               now, g_mx, f_mx/1024, l_mx/1024, fail_pct >> verdict_file
      } else if (g_mx >= warn_pct) {
        printf "  ⚠ WARN — rss_max grew %.0f%% (>= %d%%): watch for a slow leak\n", g_mx, warn_pct
      } else {
        printf "  ✓ OK — rss_max stable across the run\n"
      }
    }' "$CSV"
}

judge_run_system_metrics()    { _judge_eval_system_metrics "$1" >/dev/null 2>&1; }
judge_report_system_metrics() {
  echo "── system metrics ──"
  _judge_eval_system_metrics "$1"
}
