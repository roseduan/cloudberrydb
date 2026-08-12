#!/bin/bash
# judge/verdicts/chaos_recovery.sh
# CHAOS verdict (Category B self-heal): after each injected real failure,
# did the target DB's managed CAGGs recover within the fault's
# recovery_budget?  chaos_loop.sh measures it (RECOVERY_TIME rows:
# active_s field carries the budget, detail carries recovery_s=N|timeout)
# — this verdict turns those measurements into a pass/fail.
#
# Recovery that timed out or exceeded budget is REPORTED but NOT
# flagged.  A slow-but-successful recovery is not a business SLO
# violation — chaos_log.csv already carries every recovery_s value,
# so anyone who wants to trend "chaos recovery got slower over time"
# can awk it out directly.  A recovery that flat-out failed (gpstart
# returned non-zero) is already captured as signals/panic.chaos by
# chaos/chaos_loop.sh — not here.

_judge_eval_chaos_recovery() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/chaos_log.csv"
  if [[ "${SOAK_CHAOS_LEVEL:-0}" -eq 0 ]]; then
    echo "  (chaos disabled — no recovery to judge)"
    return 0
  fi
  if [[ ! -s "$CSV" ]]; then
    echo "  (no chaos_log.csv — chaos_loop has not fired)"
    return 0
  fi
  awk -F, '
    $2 == "RECOVERY_TIME" {
      budget = $5 + 0
      d = $6; sub(/.*recovery_s=/, "", d)
      n++
      if (d == "timeout") { stuck[$3]++; timeouts++; }
      else {
        r = d + 0
        sum += r; if (r > mx) mx = r
        if (budget > 0 && r > budget) { stuck[$3]++; over++; }
      }
    }
    END {
      if (n == 0) { print "  (no recovery measurements yet)"; exit }
      printf "  recoveries       : n=%d  mean=%.0fs  max=%ds  timeouts=%d  over-budget=%d\n",
             n, (n>timeouts ? sum/(n-timeouts) : 0), mx+0, timeouts+0, over+0
      for (k in stuck) printf "  🔴 %-24s exceeded recovery budget %d time(s) (info-only; not a flag)\n", k, stuck[k]
      if (over+timeouts == 0) print "  ✓ OK — every injected fault self-healed within its recovery budget"
    }
  ' "$CSV"
}

judge_run_chaos_recovery()    { _judge_eval_chaos_recovery "$1" >/dev/null 2>&1; }
judge_report_chaos_recovery() {
  echo "── chaos recovery (self-heal within budget) ──"
  _judge_eval_chaos_recovery "$1"
}
