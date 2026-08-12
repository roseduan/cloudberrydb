#!/bin/bash
# judge/verdicts/watermark_lag.sh
# Verdict: managed-CAGG watermark lag vs policy tolerance.
# Migrated verbatim from monitor/watermark_lag.sh --report (2026-07-08).
# cv_scratch / repro_cv excluded (ddl_churn churns them → structurally high).

_judge_eval_watermark_lag() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/watermark_lag.csv"
  mkdir -p "$RESULTS/verdicts"
  local VERDICT_FILE="$RESULTS/verdicts/fail.perf.watermark"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if [[ ! -s "$CSV" ]]; then
    echo "  (no watermark_lag.csv — first monitor cycle has not fired)"
    return 0
  fi
  # Chaos-aware: a worker kill stalls the watermark for its active +
  # recovery window; those exceeded samples are EXPECTED, not a perf
  # regression.  Exclude samples inside a chaos window from the fail
  # decision (they're reported separately).  chaos_windows_str is a
  # no-op empty string when chaos is off → identical to the old behavior.
  # CARE: faults that stop or crash a refresh cycle → watermark stalls.
  # compress_worker_kill leaves refresh untouched → not excluded.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  local WM_EXCEEDED WM_NULL WM_FAIL_AFTER WM_BAD WM_EXCL
  WM_EXCEEDED=$(awk -F, 'NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ && $8=="t" {n++} END {print n+0}' "$CSV")
  WM_NULL=$(awk -F, 'NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ && $6=="" {n++} END {print n+0}' "$CSV")
  WM_EXCL=$(awk -F, -v windows="$CW" '
    function in_chaos(ts,  i,a,n,se){ n=split(windows,a," ");
      for(i=1;i<=n;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ && $8=="t" && in_chaos($1) {n++} END {print n+0}' "$CSV")
  echo "  exceeded rows : $WM_EXCEEDED  (of which $WM_EXCL inside a chaos window — excluded from fail)"
  echo "  undefined lag : $WM_NULL  (-infinity watermark or empty source)"
  # PINNED watermark (2026-07-23, lesson of SOAK-20260721_161133): a
  # watermark stuck at -infinity WHILE THE SOURCE HAS DATA is the
  # cold-start orphan failure mode — the CAGG is not materializing at
  # all, every query re-aggregates live, yet lag is "undefined" so the
  # exceeded rule above can never fire (cv_1hour sat broken for 12h
  # under a green verdict).  No chaos exclusion: chaos never resets a
  # watermark to -infinity, and the persistence threshold already
  # tolerates a slow cold start.
  local WM_PINNED_AFTER="${SOAK_WM_PINNED_FAIL_AFTER:-60}"
  local VERDICT_PINNED="$RESULTS/verdicts/fail.perf.watermark_pinned"
  local WM_PINNED_BAD
  # TRAILING streak, not cumulative count: a watermark that recovered
  # must stop counting -- the cumulative version re-flagged
  # SOAK-20260723_101159 at end-of-run from cold-start history rows even
  # though the wm had been finite (and advancing) for 3 hours.
  WM_PINNED_BAD=$(awk -F, -v limit="$WM_PINNED_AFTER" \
        -v verdict_file="$VERDICT_PINNED" -v now="$NOW" '
    NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ {
      k=$2 "/" $3
      if ($4=="-infinity" && $5!="") streak[k]++
      else                          streak[k]=0
    }
    END {
      bad=0
      for (k in streak) if (streak[k] >= limit) {
        split(k, p, "/")
        printf "%s db=%s cv=%s pinned_trailing=%d threshold=%d\n",
               now, p[1], p[2], streak[k], limit >> verdict_file
        bad++
      }
      print bad+0
    }' "$CSV")
  awk -F, '
    NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ {
      k=$2 "/" $3
      n[k]++
      if ($8=="t") ex[k]++
      if ($6 != "" && $6+0 > max[k]) max[k]=$6+0
      last[k]=$6
      off[k]=$7
    }
    END {
      for (k in n)
        printf "  %-28s samples=%-5d exceeded=%-5d max_lag=%-8s last_lag=%-8s end_offset=%s\n",
               k, n[k], ex[k]+0, max[k]+0, last[k], off[k]
    }
  ' "$CSV" | sort
  WM_FAIL_AFTER="${SOAK_WATERMARK_FAIL_AFTER:-5}"
  # Fail count EXCLUDES chaos-window samples (in_chaos): only sustained
  # lag OUTSIDE injected faults counts as a real perf regression.
  # Append one verdict row per offending (db, cv) with the current max_lag.
  WM_BAD=$(awk -F, -v limit="$WM_FAIL_AFTER" -v windows="$CW" \
                -v verdict_file="$VERDICT_FILE" -v now="$NOW" '
    function in_chaos(ts,  i,a,n,se){ n=split(windows,a," ");
      for(i=1;i<=n;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    NR>1 && $3 !~ /^(cv_scratch|repro_cv)$/ && $8=="t" && !in_chaos($1) {
      k=$2 "/" $3
      ex[k]++
      if ($6 != "" && $6+0 > mx[k]) mx[k]=$6+0
    }
    END {
      bad=0
      for (k in ex) if (ex[k] >= limit) {
        split(k, p, "/")
        printf "%s db=%s cv=%s exceeded=%d threshold=%d max_lag=%ds\n",
               now, p[1], p[2], ex[k], limit, mx[k]+0 >> verdict_file
        bad++
      }
      print bad+0
    }
  ' "$CSV")
  # SCHEDULE-RELATIVE FALLBACK (2026-07-24, judge blind spot of
  # SOAK-20260723_170208): if the monitor's end_offset column is empty
  # (policy-join regression — it silently was for every historical run
  # until the schema-qualified-name fix), the exceeded flag can never
  # fire and a frozen finite watermark rides under a green verdict.
  # This rule needs NOTHING from the policy join: a TRAILING streak of
  # samples whose lag exceeds MULT × schedule_interval (sync constants,
  # same table as refresh_perf) fails on its own.  Scoped to rows with
  # end_offset EMPTY so it never double-flags alongside the precise
  # exceeded rule.
  local WM_SCHED_MULT="${SOAK_WM_SCHED_LAG_MULT:-5}"
  local WM_SCHED_AFTER="${SOAK_WM_SCHED_LAG_FAIL_AFTER:-30}"
  local VERDICT_SCHED="$RESULTS/verdicts/fail.perf.watermark_schedule_lag"
  local WM_SCHED_BAD
  WM_SCHED_BAD=$(awk -F, -v mult="$WM_SCHED_MULT" -v limit="$WM_SCHED_AFTER" \
        -v verdict_file="$VERDICT_SCHED" -v now="$NOW" '
    BEGIN {
      # SYNC CONSTANT with setup/03_caggs.sql schedule_intervals (same
      # deliberate non-tunable table as judge/verdicts/refresh_perf.sh).
      sched["cv_1min"]  =   60;
      sched["cv_5min"]  =  300;
      sched["cv_1hour"] = 3600;
    }
    NR>1 && $3 in sched {
      k=$2 "/" $3
      if ($7=="" && $6!="" && $6+0 > mult * sched[$3]) { streak[k]++; lg[k]=$6+0 }
      else                                              streak[k]=0
    }
    END {
      bad=0
      for (k in streak) if (streak[k] >= limit) {
        split(k, p, "/")
        printf "%s db=%s cv=%s lag=%ds threshold=%dx_schedule trailing=%d\n",
               now, p[1], p[2], lg[k], mult, streak[k] >> verdict_file
        bad++
      }
      print bad+0
    }' "$CSV")
  if [[ "$WM_PINNED_BAD" -gt 0 ]]; then
    echo "  🔴 CRITICAL — watermark PINNED at -infinity while source has data (>= $WM_PINNED_AFTER samples): CAGG is not materializing at all"
  fi
  if [[ "$WM_SCHED_BAD" -gt 0 ]]; then
    echo "  🔴 CRITICAL — watermark lag > ${WM_SCHED_MULT}x schedule_interval for >= $WM_SCHED_AFTER trailing samples (end_offset unavailable — schedule-relative fallback)"
  fi
  if [[ "$WM_BAD" -gt 0 ]]; then
    echo "  🔴 CRITICAL — managed CAGG watermark exceeded threshold in at least $WM_FAIL_AFTER samples"
  elif [[ "$WM_EXCEEDED" -gt 0 ]]; then
    echo "  ⚠ WARN — transient watermark lag exceeded policy tolerance"
  elif [[ "$WM_SCHED_BAD" -eq 0 && "$WM_PINNED_BAD" -eq 0 ]]; then
    echo "  ✓ OK — managed CAGG watermarks stayed within tolerance"
  fi
}

judge_run_watermark_lag()    { _judge_eval_watermark_lag "$1" >/dev/null 2>&1; }
judge_report_watermark_lag() {
  echo "── watermark lag (materialization progress) ──"
  _judge_eval_watermark_lag "$1"
}
