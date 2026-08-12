#!/bin/bash
# judge/verdicts/bgw_scheduler_health.sh
#
# Verdict: BGW scheduler dispatch health.
# Migrated out of monitor/bgw_scheduler_health.sh --report on 2026-07-08
# as the first judge-layer verdict (the template for the rest).
#
# Reads bgw_scheduler_health.csv (produced by the monitor collector,
# which now only appends rows and no longer judges).  Computes per-db
# "overdue%" = share of samples where at least one scheduled job was
# overdue by more than SOAK_BGW_OVERDUE_WARN_SEC, and raises
# verdicts/fail.perf.bgw_overdue when any db crosses SOAK_BGW_OVERDUE_FAIL_PCT.
#
# Sourced by judged.sh, which calls judge_run_* every tick (streaming)
# and judge_report_* at report time.

# Shared evaluation.  Computes the per-db verdict, touches fail.perf on
# a CRITICAL breach, and prints the human-readable lines to stdout.
# Pure function of the CSV, so it is safe to call repeatedly (streaming)
# and again at report time; the flag touch is idempotent.
_judge_eval_bgw_scheduler_health() {
  local results="$1"
  local csv="$results/data/bgw_scheduler_health.csv"
  mkdir -p "$results/verdicts"
  local VERDICT_FILE="$results/verdicts/fail.perf.bgw_overdue"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if [[ ! -s "$csv" ]]; then
    echo "  (no bgw_scheduler_health.csv — collector has not fired yet)"
    return 0
  fi
  # Chaos-aware: killing the scheduler (a level-1 fault) makes jobs go
  # overdue BY DESIGN.  The fail decision uses only NON-chaos-window
  # samples (overdue%_q); the displayed overdue% is over all samples.
  # Minimum-evidence gate: with fewer than SOAK_JUDGE_MIN_SAMPLES
  # non-chaos samples the ratio is statistical noise (1 overdue of 2 =
  # 50%), so no fail/WARN is raised until the denominator is large enough.
  # CARE: only faults that stop policy dispatch.  worker_kills don't
  # touch the scheduler — overdue on THAT chaos would be a real bug.
  local CW; CW=$(chaos_windows_str "$results/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  awk -F, \
      -v warn_pct="${SOAK_BGW_OVERDUE_WARN_PCT:-10}" \
      -v fail_pct="${SOAK_BGW_OVERDUE_FAIL_PCT:-30}" \
      -v warn_worst_s="${SOAK_BGW_WORST_OVERDUE_WARN_SEC:-1800}" \
      -v fail_worst_s="${SOAK_BGW_WORST_OVERDUE_FAIL_SEC:-3600}" \
      -v min_n="${SOAK_JUDGE_MIN_SAMPLES:-10}" \
      -v windows="$CW" \
      -v verdict_file="$VERDICT_FILE" -v now="$NOW" '
    function in_chaos(ts,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    # Housekeeping/GC jobs that fire hourly-or-slower — their overdue
    # signal is dominated by scheduler-tick jitter, not by refresh
    # dispatch health.  When they claim the worst_job spot they also
    # bury the actually-important CAGG refresh delays.  Not filtered
    # from the overdue COUNT (they are still dispatched by the same
    # scheduler that would fail CAGG refresh), but suppressed from the
    # worst_job display and the absolute-worst thresholds so the
    # display finger-points where it matters.
    function is_housekeeping(job,   n) {
      n = index(job, "policy_job_stat_history_retention")
      return (n == 1)
    }
    NR == 1 { next }
    {
      db=$2; n[db]++
      overdue=$4+0
      worst=$5+0
      job=$6
      if (overdue > 0) with_overdue[db]++
      if (!in_chaos($1)) {
        nq[db]++
        if (overdue > 0) with_overdue_q[db]++
        # Track absolute worst_overdue across NON-chaos samples only;
        # a post-crash-recovery gpstart legitimately leaves next_start
        # far in the past for one sample.
        if (!is_housekeeping(job) && worst > max_worst_q[db]) {
          max_worst_q[db] = worst
          worst_job_q[db] = job
        }
      }
      if (worst > max_worst[db]) { max_worst[db] = worst; worst_job[db] = job }
      sum_worst[db] += worst
    }
    END {
      printf "  samples per db   : "
      first = 1
      for (db in n) { printf "%s%s=%d", (first?"":", "), db, n[db]; first=0 }
      print ""
      any_warn = 0; any_fail = 0
      for (db in n) {
        pct = (n[db] > 0) ? (with_overdue[db] * 100.0 / n[db]) : 0
        pctq = (nq[db] > 0) ? (with_overdue_q[db] * 100.0 / nq[db]) : 0
        avg = (n[db] > 0) ? (sum_worst[db] / n[db]) : 0
        # Use non-chaos worst for the threshold check; fall back to
        # overall max_worst[db] only for the display "worst_overdue"
        # column so the user still sees the extreme value.
        worst_q = max_worst_q[db]+0
        job_q = (worst_job_q[db] ? worst_job_q[db] : "-")
        verdict = "✓ OK"
        if (nq[db]+0 < min_n) { verdict = sprintf("✓ OK (only %d non-chaos samples; need %d to judge)", nq[db]+0, min_n) }
        else if (pctq >= fail_pct || (fail_worst_s+0 > 0 && worst_q >= fail_worst_s+0)) {
          verdict = sprintf("🔴 CRITICAL (%.1f%% overdue OR worst=%ds >= %ds)", pctq, worst_q, fail_worst_s+0); any_fail = 1
          printf "%s db=%s overdue_pct=%.1f%% threshold=%d%% worst=%ds threshold_worst=%ds worst_job=%s\n",
                 now, db, pctq, fail_pct, worst_q, fail_worst_s+0, job_q >> verdict_file
        }
        else if (pctq >= warn_pct || (warn_worst_s+0 > 0 && worst_q >= warn_worst_s+0)) {
          verdict = sprintf("⚠ WARN (%.1f%% overdue OR worst=%ds >= %ds)", pctq, worst_q, warn_worst_s+0)
          any_warn = 1
        }
        printf "  %-14s samples=%-4d overdue%%=%-5.1f%% (non-chaos %.1f%%) worst_overdue=%-4ds avg_worst=%.1fs worst_job=%s  %s\n",
               db, n[db], pct, pctq, max_worst[db]+0, avg, (worst_job[db] ? worst_job[db] : "-"), verdict
      }
      if (!any_warn && !any_fail) print "  ✓ OK — scheduler dispatched all jobs within tolerance"
    }
  ' "$csv"
}

# Streaming tick: evaluate + touch flags early, discard the display text.
judge_run_bgw_scheduler_health() {
  _judge_eval_bgw_scheduler_health "$1" >/dev/null 2>&1
}

# Report: print the section (also touches flags, so a report-only run
# with streaming disabled still produces the verdict).
judge_report_bgw_scheduler_health() {
  echo "── bgw scheduler dispatch health ──"
  _judge_eval_bgw_scheduler_health "$1"
}
