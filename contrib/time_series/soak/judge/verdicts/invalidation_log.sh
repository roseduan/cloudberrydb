#!/bin/bash
# judge/verdicts/invalidation_log.sh
# Verdict: L1/L2 invalidation-log bloat + drain health.
# Migrated verbatim from monitor/invalidation_log.sh --report (2026-07-08).
# L1 stuck non-empty → fail.view_mismatch (invalidations not draining).

_judge_eval_invalidation_log() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/invalidation_log.csv"
  mkdir -p "$RESULTS/verdicts"
  if [[ ! -s "$CSV" ]]; then
    echo "  (no invalidation_log.csv — first sample has not fired)"
    return 0
  fi
  # Chaos-aware: a killed refresh worker stalls L1 drain during its
  # window.  The STUCK fail decision uses the non-chaos drain fraction.
  # CARE: faults that keep L1 from being cleaned (no refresh → L1 grows).
  # compress_worker_kill is unrelated.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  local VERDICT_FILE="$RESULTS/verdicts/fail.correctness.invalidation_stuck"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  awk -F, \
      -v warn_pct="${SOAK_INVAL_NONEMPTY_WARN_PCT:-60}" \
      -v fail_pct="${SOAK_INVAL_NONEMPTY_FAIL_PCT:-95}" \
      -v growth_warn="${SOAK_INVAL_GROWTH_WARN_PCT:-100}" \
      -v accel_floor="${SOAK_INVAL_L2_ACCEL_FLOOR:-0.05}" \
      -v min_n="${SOAK_INVAL_TREND_MIN_SAMPLES:-20}" \
      -v min_samples="${SOAK_JUDGE_MIN_SAMPLES:-10}" \
      -v windows="$CW" \
      -v verdict_file="$VERDICT_FILE" -v now="$NOW" '
    function in_chaos(ts,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
    NR>1 {
      n++
      l1[n]=$3+0; l2[n]=$4+0
      if($3+0>maxl1) maxl1=$3+0
      if($4+0>maxl2) maxl2=$4+0
      if($3+0>0) l1nonempty++
      if(!in_chaos($1)) { nq++; if($3+0>0) l1nonempty_q++ }
    }
    END {
      printf "  samples          : %d\n", n
      printf "  L1 rows  peak/last : %d / %d\n", maxl1, l1[n]
      printf "  L2 rows  peak/last : %d / %d\n", maxl2, l2[n]
      if (n == 0) exit
      frac = l1nonempty*100.0/n
      fracq = (nq>0) ? l1nonempty_q*100.0/nq : 0
      printf "  L1 non-empty     : %d/%d samples (%.0f%%, non-chaos %.0f%%)\n", l1nonempty, n, frac, fracq
      # Bloat — DE-NOISED, no absolute L1+L2 floor.
      #
      # L2 (materialization log) grows monotonically BY DESIGN — every
      # refresh appends a row — so a raw "+X% / >=N rows" test on L1+L2
      # fires on healthy linear accumulation (the 2026-06 run flagged
      # "+517% to 604 rows" right next to "✓ L1 drains" — self-
      # contradictory).  Ask the two questions that actually mean trouble:
      #   • L1 (invalidation backlog) must drain to ~0; a rising L1
      #     first-Q→last-Q trend is real (refresh falling behind writes).
      #   • L2 linear growth is fine; only ACCELERATING growth (the
      #     second-half per-sample slope steeper than the first) signals
      #     an unbounded leak rather than expected accumulation.
      if (n >= min_n) {
        q=int(n/4); if(q<1)q=1
        f1=0; for(i=1;i<=q;i++) f1+=l1[i]; f1/=q
        l1q=0; for(i=n-q+1;i<=n;i++) l1q+=l1[i]; l1q/=q
        printf "  L1 backlog trend : %.1f → %.1f rows (first-Q→last-Q)\n", f1, l1q
        printf "  L2 size trend    : %d → %d rows (monotonic by design)\n", l2[1], l2[n]
        if (l1q > f1 + 1 && l1q >= f1 * (1 + growth_warn/100.0))
          printf "  ⚠ WARN — L1 backlog trending UP %.1f→%.1f rows: invalidations outpacing refresh\n", f1, l1q
        h=int(n/2); if(h<2)h=2
        s1=(l2[h]-l2[1])/(h-1)
        s2=(l2[n]-l2[h])/(n-h)
        if (s1 >= 0 && s2 > accel_floor && s2 >= (s1>0?s1:accel_floor) * (1 + growth_warn/100.0))
          printf "  ⚠ WARN — L2 growth ACCELERATING (slope %.3f→%.3f rows/sample): possible unbounded bloat\n", s1, s2
        else
          printf "  ✓ L2 growth linear/decelerating (slope %.3f→%.3f rows/sample)\n", s1, s2
      }
      # Drain health.  STUCK fail uses the non-chaos fraction (fracq) so
      # a killed-refresh stall inside a chaos window is not mistaken for
      # a real drain failure.  Gated on a minimum non-chaos sample count
      # (SOAK_JUDGE_MIN_SAMPLES): a cold-start handful (e.g. 2/2 L1
      # non-empty = 100%) must not trip a STUCK fail on no real evidence.
      if (nq < min_samples) {
        printf "  ✓ OK — L1 drain not judged yet (%d non-chaos samples, need %d)\n", nq, min_samples
      } else if (fracq >= fail_pct) {
        printf "  🔴 STUCK — L1 non-empty in %.0f%% of non-chaos samples (>= %d%%): invalidations are NOT draining\n", fracq, fail_pct
        printf "     (this would otherwise masquerade as view_check coverage-thin WARN)\n"
        printf "%s L1_nonempty_pct=%.0f%% threshold=%d%% samples=%d\n",
               now, fracq, fail_pct, nq >> verdict_file
      } else if (frac >= warn_pct) {
        printf "  ⚠ WARN — L1 non-empty in %.0f%% of samples (>= %d%%): refresh may be lagging the write rate\n", frac, warn_pct
      } else {
        printf "  ✓ OK — L1 drains between refreshes; logs not bloating\n"
      }
    }' "$CSV"
}

judge_run_invalidation_log()    { _judge_eval_invalidation_log "$1" >/dev/null 2>&1; }
judge_report_invalidation_log() {
  echo "── invalidation log (L1/L2 bloat + drain health) ──"
  _judge_eval_invalidation_log "$1"
}
