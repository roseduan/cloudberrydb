#!/bin/bash
# judge/verdicts/perf_probe.sh
# Verdict: view-query latency distribution + speedup floor + de-noised
# plan-drift detection.
# Migrated from monitor/perf_probe.sh --report (2026-07-08).
#
# NOTE (2026-07-24): previously DIAGNOSTIC ONLY — the section printed 🔴
# WARNING lines but wrote no fail.* flag, so a CAGG scan-path regression
# (SOAK-20260723_170208 had 🔴 view-vs-source SLOWER on both DBs and 🔴
# PLAN DRIFT + EXCESS SLOWDOWN on two probes) rated the run "clean" and
# nothing showed up in the verdicts/ audit.  Two conditions now escalate:
#
#   fail.perf.view_slower_than_source — a CAGG view is on average slower
#       than a naked source scan (ratio<1.0) over enough samples.  CAGG's
#       entire value proposition is inverted; treat as a perf regression.
#
#   fail.perf.plan_drift_slowdown — a probe's plan changed AND its
#       last-quartile mean exec exceeds a data-growth-normalized ratio.
#       This is the actionable subset of the drift detector — benign
#       replan noise is de-normed against source_24h's data-growth
#       baseline and does not fail.
#
# perf_probe.csv + plan_snapshots/ still carry the raw evidence.  If you
# find yourself staring at a 🔴 line, dig into the matching
# plan_snapshots/<db>_<probe>_drift_*.txt to see the new plan.

_judge_eval_perf_probe() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/perf_probe.csv"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  local VIEW_FAIL_FILE="$RESULTS/verdicts/fail.perf.view_slower_than_source"
  local DRIFT_FAIL_FILE="$RESULTS/verdicts/fail.perf.plan_drift_slowdown"
  mkdir -p "$RESULTS/verdicts"
  echo
  if [[ ! -s "$CSV" ]]; then
    echo "  (no perf_probe.csv — SOAK_PERF_PROBE_EVERY=0 or first cycle didn't fire)"
    return 0
  fi

  # Distribution per (db, probe); ERR rows counted separately.
  awk -F, '
    NR == 1 { next }
    $4 == "ERR" { k = $2 "/" $3; ERR[k]++; next }
    {
      k = $2 "/" $3
      N[k]++
      A[k, N[k]] = $4 + 0
    }
    END {
      for (k in N) {
        n = N[k]
        for (i = 1; i <= n; i++) a[i] = A[k, i]
        for (i = 1; i <= n; i++)
          for (j = i + 1; j <= n; j++)
            if (a[i] > a[j]) { t = a[i]; a[i] = a[j]; a[j] = t }
        p50_idx = int((n - 1) * 0.5) + 1
        p95_idx = int((n - 1) * 0.95) + 1
        p99_idx = int((n - 1) * 0.99) + 1

        err = (k in ERR) ? ERR[k] : 0
        fail_rate = (n + err > 0) ? (err * 100.0 / (n + err)) : 0
        err_str = (err > 0) ? sprintf(" err=%d (%.1f%%)", err, fail_rate) : ""

        printf "  %-32s n=%-4d  p50=%-7.1f p95=%-7.1f p99=%-7.1f max=%-7.1f ms%s\n",
          k, n, a[p50_idx], a[p95_idx], a[p99_idx], a[n], err_str
      }
    }
  ' "$CSV" | sort

  echo

  # Per-DB speedup floor check: cv_1hour_24h vs source_24h ratio.
  # ratio < 1.0 stays literal: it is the DEFINITION of "view slower
  # than source", not a tunable.  Requires at least min_n samples on
  # BOTH sides — a single-run comparison is meaningless.
  awk -F, -v note_ratio="${SOAK_PERF_SPEEDUP_NOTE_RATIO:-2.0}" \
          -v fail_ratio="${SOAK_PERF_VIEW_SLOWER_FAIL_RATIO:-0.9}" \
          -v min_n="${SOAK_JUDGE_MIN_SAMPLES:-10}" \
          -v fail_file="$VIEW_FAIL_FILE" -v now="$NOW" '
    NR > 1 && $4 != "ERR" && $3 == "cv_1hour_24h" { c[$2] += $4+0; cn[$2]++ }
    NR > 1 && $4 != "ERR" && $3 == "source_24h"   { s[$2] += $4+0; sn[$2]++ }
    END {
      for (db in cn) {
        if (cn[db] > 0 && sn[db] > 0) {
          ca = c[db] / cn[db]; sa = s[db] / sn[db]
          ratio = sa / ca
          printf "  view-vs-source[%s] : cv_1hour_24h avg %.1f ms, source_24h avg %.1f ms (cagg speedup %.1fx)\n",
            db, ca, sa, ratio
          if (ratio < 1.0) {
            printf "  🔴 WARNING[%s]    : view path is SLOWER than source — scan-path regression\n", db
            # Fail path uses a tighter ratio band (default 0.9) so
            # a run where the two paths are within measurement noise
            # (e.g. ratio 0.97) prints the WARN but does not write a
            # perf.fail flag.  A genuinely slower cagg tends to sit
            # well below 0.9 once it starts fanning source scans
            # (SOAK-20260723 soak_test_b landed at 0.75).
            if (cn[db] >= min_n && sn[db] >= min_n && ratio < fail_ratio+0) {
              printf "%s db=%s ratio=%.2f fail_below=%.2f view_avg_ms=%.1f source_avg_ms=%.1f n_view=%d n_source=%d\n",
                     now, db, ratio, fail_ratio+0, ca, sa, cn[db], sn[db] >> fail_file
            }
          }
          else if (ratio < note_ratio + 0)
            printf "  ⚠ NOTE[%s]       : speedup < %sx — CAGG barely helping (view near source-scan cost)\n", db, note_ratio
        }
      }
    }
  ' "$CSV"

  echo

  # Plan-shape drift — DE-NOISED.
  #
  # A plan_sig changing mid-run is, on a busy table, mostly BENIGN
  # evolution: ANALYZE updates stats and the planner legitimately
  # flips GroupAggregate↔HashAggregate etc.  The old code printed one
  # ⚠ line per (db,probe), so a healthy run produced a 10-line "drift
  # wall" that everyone learned to ignore — the worst outcome for an
  # alarm.  What's actually ACTIONABLE is drift that COINCIDES with a
  # slowdown: the plan changed AND the probe got slower.
  #
  # So: collapse benign drift to one summary line; escalate only a
  # probe whose last-quartile exec_ms exceeds its first-quartile by
  # SOAK_PERF_DEGRADED_RATIO (self-relative, needs no baseline).
  awk -F, -v min_cycles="${SOAK_PLAN_DRIFT_MIN_CYCLES:-4}" \
          -v deg="${SOAK_PERF_DEGRADED_RATIO:-1.5}" \
          -v floor_ms="${SOAK_PERF_SLOWDOWN_FLOOR_MS:-50}" \
          -v fail_file="$DRIFT_FAIL_FILE" -v now="$NOW" '
    # Self-relative slowdown ratio (last-quartile mean / first-quartile
    # mean) for one probe key; also stashes the two means for printing.
    function ratio(k,   n,q,f,l,i) {
      n=cyc[k]; q=int(n/4); if(q<1)q=1
      f=0; for(i=1;i<=q;i++) f+=ex[k,i]; f/=q
      l=0; for(i=n-q+1;i<=n;i++) l+=ex[k,i]; l/=q
      firstq[k]=f; lastq[k]=l
      return (f>0)? l/f : 0
    }
    NR > 1 && $6 != "ERR" && $6 != "" {
      db=$2; pr=$3; k=db "/" pr
      cyc[k]++
      ex[k, cyc[k]] = $4 + 0           # exec_ms by cycle index
      kdb[k]=db; kpr[k]=pr
      if (!(k in seen)) { first[k]=$6; seen[k]=1 }
      else if ($6 != first[k]) changes[k]++
    }
    END {
      # 1. Per-DB DATA-GROWTH baseline = source_24h self-slowdown.  The
      #    source aggregates straight off the growing source table, so
      #    its first→last quartile ratio IS the cost of 19h more data.
      #    Every cagg probe is normalized against it: only slowdown
      #    BEYOND data growth is a real cagg regression.  Without this,
      #    a healthy run flagged source_24h itself (5.1x) and every big
      #    scan as 🔴 — pure data-growth noise.
      for (k in seen)
        if (kpr[k]=="source_24h" && cyc[k]>=min_cycles) srcratio[kdb[k]]=ratio(k)
      drifted=0; total=0; actionable=0
      for (k in seen) {
        if (cyc[k] < min_cycles) continue
        total++
        r=ratio(k)
        if (k in changes) drifted++
        base=(kdb[k] in srcratio && srcratio[kdb[k]]>0)? srcratio[kdb[k]] : 1
        norm=(base>0)? r/base : r
        # Actionable: plan shape changed AND a cagg-specific slowdown
        # beyond this DB data-growth baseline (norm>=deg) AND meaningful
        # absolute time.  source_24h is the baseline itself → skip.
        if ((k in changes) && kpr[k] != "source_24h" && norm >= deg && lastq[k] >= floor_ms) {
          actionable++
          printf "  🔴 PLAN DRIFT + EXCESS SLOWDOWN[%s] : shape changed AND exec %.1f→%.1f ms (%.1fx, %.1fx beyond data-growth %.1fx) — see plan_snapshots/\n",
                 k, firstq[k], lastq[k], r, norm, base
          printf "%s probe=%s firstq_ms=%.1f lastq_ms=%.1f ratio=%.2f norm=%.2f baseline=%.2f\n",
                 now, k, firstq[k], lastq[k], r, norm, base >> fail_file
        }
      }
      if (drifted == 0)
        printf "  plan_sig stable across all %d probes ✓\n", total
      else
        printf "  plan drift: %d/%d probes changed shape mid-run (expected as data grows); %d show cagg-specific slowdown beyond the data-growth baseline%s\n",
               drifted, total, actionable, (actionable==0 ? " — none actionable" : "")
    }
  ' "$CSV"
}

judge_run_perf_probe()    { _judge_eval_perf_probe "$1" >/dev/null 2>&1; }
judge_report_perf_probe() {
  echo "── perf probe (view latency) ──"
  _judge_eval_perf_probe "$1"
}
