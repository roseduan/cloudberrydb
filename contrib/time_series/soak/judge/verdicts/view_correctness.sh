#!/bin/bash
# judge/verdicts/view_correctness.sh
# Verdict: THE correctness signal — view = source.
#
# SPLIT: the collector (monitor/view_correctness_scrape.sh) is PURE
# COLLECTION — it runs the comparison SQL, records verdicts to
# view_correctness.csv, records differing buckets to
# view_mismatch_buckets.csv + captures an evidence snapshot, and (after a
# settle delay) re-queries each differing bucket and records the outcome to
# view_mismatch_recheck.csv.  It does NOT decide fail.  THIS verdict owns it:
#
#   • fail.view_mismatch — TARGETED DELAYED RE-CHECK (2026-07-10).  A
#     single-cycle MISMATCH is a watermark-boundary eventual-consistency
#     transient (a late write's row is visible a few seconds before its
#     L1/L2 invalidation entry, so the decidable check briefly sees a
#     below-wm bucket as clean).  We used to require the SAME bucket to
#     recur across >= N check cycles — but the mat check window slides with
#     the watermark, so at a coarse check interval a PERSISTENT divergence
#     is observed in only ONE cycle (window slides past it) and would be
#     misfiled as transient (reproduced 2026-07-10).  Instead the collector
#     re-queries THAT exact bucket >= SOAK_VIEW_RECHECK_SETTLE_SEC after it
#     was first seen; a bucket that STILL diverges (and is decidable) is
#     confirmed real → fail.  Decoupled from window re-coverage, so a
#     persistent divergence is caught regardless of interval; a transient
#     resolves within the settle delay → not failed.
#   • fail.view_incomplete — too many NON-chaos incomplete cycles.

_judge_eval_view_correctness() {
  local RESULTS="$1"
  local VIEW_CSV="$RESULTS/data/view_correctness.csv"
  local INCOMPLETE_CSV="$RESULTS/data/view_incomplete.csv"
  local BUCKETS_CSV="$RESULTS/data/view_mismatch_buckets.csv"
  local RECHECK_CSV="$RESULTS/data/view_mismatch_recheck.csv"
  local SETTLE="${SOAK_VIEW_RECHECK_SETTLE_SEC:-180}"
  mkdir -p "$RESULTS/flags"

  if [[ ! -s "$VIEW_CSV" ]]; then
    echo "  (no view_correctness.csv — first cycle hasn't fired yet)"
    return 0
  fi
  # view_correctness.csv v2 (8 cols): ts,db,cagg,window,total_rows,mismatch_count,excluded_buckets,verdict
  # view_mismatch_buckets.csv (5 cols): ts,db,cagg,window,bucket
  # view_mismatch_recheck.csv (6 cols): ts,db,cagg,window,bucket,outcome  (outcome ∈ still_mismatch|resolved|pending)
  local TOTAL EXCLUDED ZERO_SIGNAL INCOMPLETE INCOMPLETE_Q DUMPS CONFIRMED RESOLVED PENDING
  TOTAL=$(awk -F, 'NR>1 && $3!="" {n++} END {print n+0}' "$VIEW_CSV")
  EXCLUDED=$(awk -F, 'NR>1 {n+=$7+0} END {print n+0}' "$VIEW_CSV")
  ZERO_SIGNAL=$(awk -F, 'NR>1 && $5==0 && $8=="match" {n++} END {print n+0}' "$VIEW_CSV")
  INCOMPLETE=$(awk -F, 'NR>1 {n++} END {print n+0}' "$INCOMPLETE_CSV" 2>/dev/null || echo 0)
  # Chaos-aware incomplete count: an incomplete cycle taken while a fault
  # was active (cluster crash → psql errors, worker kill → transient
  # disruption) is EXPECTED, not a dead check.  The fail decision uses
  # only NON-chaos-window incompletes; the displayed count is over all.
  # CARE: only cluster_crash makes the DB unqueryable / mat table
  # dramatically stale.  worker_kills → mat is briefly stale but recheck
  # settle absorbs it; scheduler_kill → dispatch pauses but existing
  # data stays correct.  Excluding those would only hide real bugs.
  local CW; CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
                 "$CHAOS_ALL_FAULTS" \
                 "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" 2>/dev/null)
  INCOMPLETE_Q=$(awk -F, -v windows="$CW" '
    function in_chaos(t,  i,a,m,se){ m=split(windows,a," ");
      for(i=1;i<=m;i++){split(a[i],se,"|"); if(t>=se[1]&&t<=se[2])return 1} return 0 }
    NR>1 && !in_chaos($1) && $7!="db_unavailable" {n++} END {print n+0}' "$INCOMPLETE_CSV" 2>/dev/null || echo 0)
  # db_unavailable = the DB was crash-recovering / restarting when the check
  # ran (e.g. a chaos SIGKILL of a BGW triggers a full cluster crash-reinit).
  # That is infrastructure disruption, NOT a wedged check, so it is EXCLUDED
  # from INCOMPLETE_Q (the fail.view_incomplete count).  A real crash-loop is
  # caught by the server log, not here.
  local DB_UNAVAIL; DB_UNAVAIL=$(awk -F, 'NR>1 && $7=="db_unavailable" {n++} END {print n+0}' "$INCOMPLETE_CSV" 2>/dev/null || echo 0)
  DUMPS=$(ls "$RESULTS"/view_mismatch_*.snapshot.txt 2>/dev/null | wc -l | tr -d ' ')

  # Targeted delayed re-check outcomes (collector re-queried each differing
  # bucket >= SETTLE after first sighting):
  #   CONFIRMED = keys with >= 1 still_mismatch     → real divergence.
  #   RESOLVED  = keys resolved and never still_mismatch → transient.
  #   PENDING   = seen in buckets.csv but no terminal outcome yet (settle
  #               not elapsed, or not decidable) → not judged this run.
  if [[ -s "$RECHECK_CSV" ]]; then
    CONFIRMED=$(awk -F, 'NR>1 && $6=="still_mismatch"{k[$2"|"$3"|"$4"|"$5]=1} END{n=0;for(x in k)n++;print n+0}' "$RECHECK_CSV")
    RESOLVED=$(awk  -F, 'NR>1{if($6=="still_mismatch")sm[$2"|"$3"|"$4"|"$5]=1; else if($6=="resolved")rs[$2"|"$3"|"$4"|"$5]=1}
                        END{n=0;for(x in rs)if(!(x in sm))n++;print n+0}' "$RECHECK_CSV")
  else
    CONFIRMED=0; RESOLVED=0
  fi
  PENDING=$(awk -F, '
    FNR==NR { if (FNR>1 && ($6=="still_mismatch"||$6=="resolved")) term[$2"|"$3"|"$4"|"$5]=1; next }
    FNR>1   { obs[$2"|"$3"|"$4"|"$5]=1 }
    END { n=0; for (k in obs) if (!(k in term)) n++; print n+0 }' \
    "$RECHECK_CSV" "$BUCKETS_CSV" 2>/dev/null || echo 0)

  echo "  checks total : $TOTAL"
  echo "  confirmed MISMATCH buckets: $CONFIRMED  (re-query >= ${SETTLE}s after first sighting STILL diverged = real)"
  echo "  resolved (transient) buckets: $RESOLVED  (re-query cleared within settle — watermark-boundary eventual consistency, NOT failures)"
  echo "  pending re-check buckets: $PENDING  (seen once; settle not elapsed or not yet decidable — not judged)"
  echo "  excluded buckets: $EXCLUDED  (pending L1/L2 below watermark — not decidable this cycle, re-covered next)"
  echo "  zero-signal rows: $ZERO_SIGNAL  (total_rows=0; no source/view rows were compared)"
  echo "  incomplete cycles: $INCOMPLETE  ($INCOMPLETE_Q non-chaos wedge counted; $DB_UNAVAIL db-unavailable/recovery excluded; psql error, timeout, or unexpected row count)"
  echo "  on-trigger dumps: $DUMPS  (raw SQL output + cagg_watermark + invalidation_log at the time of the bug)"

  if [[ "$CONFIRMED" -gt 0 ]]; then
    echo "  confirmed-mismatch breakdown (db/cagg/window bucket → re-checks):"
    awk -F, '
      $6=="still_mismatch" { kb=$2"/"$3"/"$4" bucket="$5; n[kb]++;
             if (!(kb in first)) first[kb]=$1; last[kb]=$1 }
      END { for (kb in n)
              printf "    %-52s still_mismatch x%-2d first=%s last=%s\n", kb, n[kb], first[kb], last[kb] }' \
      "$RECHECK_CSV" | sort
    local NOW_MM; NOW_MM=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    awk -F, -v now="$NOW_MM" -v out="$RESULTS/verdicts/fail.correctness.view_mismatch" '
      $6=="still_mismatch" {
        printf "%s db=%s cv=%s window=%s bucket=%s\n", now, $2, $3, $4, $5 >> out
      }' "$RECHECK_CSV"
  fi
  if [[ "$RESOLVED" -gt 0 && "$CONFIRMED" -eq 0 ]]; then
    echo "  resolved breakdown (re-query cleared, auto-cleared):"
    awk -F, '
      NR>1 && $6=="resolved" { kb=$2"/"$3"/"$4" bucket="$5; if(!(kb in sm)) ts[kb]=$1 }
      NR>1 && $6=="still_mismatch" { sm[$2"/"$3"/"$4" bucket="$5]=1 }
      END { c=0; for (kb in ts) if(!(kb in sm)) { if (++c<=10) printf "    %-52s cleared_at=%s\n", kb, ts[kb] }
            if (c>10) printf "    ... and %d more\n", c-10 }' \
      "$RECHECK_CSV" | sort
  fi

  if [[ "$EXCLUDED" -gt 0 ]]; then
    echo "  excluded-bucket breakdown (coverage, not correctness):"
    awk -F, 'NR>1 && $7+0 > 0 {
      k=$2 "/" $3
      n[k]++
      ex[k]+=$7+0
    } END {
      for (k in n) printf "    %-45s cycles_affected=%-4d buckets=%d\n", k, n[k], ex[k]
    }' "$VIEW_CSV" | sort
    # A CAGG whose checks were dominated by exclusions proved little about
    # its mat branch this run — surface as a coverage WARN rather than
    # letting "0 MISMATCH" imply full coverage.  Hourly/daily buckets get a
    # looser threshold (a short run only fits a handful, so a few pending
    # exclusions trips the default every time).
    awk -F, -v warn_pct="${SOAK_VIEW_COVERAGE_WARN_PCT:-50}" \
        -v hourly_pct="${SOAK_VIEW_COVERAGE_WARN_HOURLY_PCT:-85}" '
      NR>1 { cyc[$2 "/" $3]++; if ($7+0 > 0) hit[$2 "/" $3]++ }
      END {
        for (k in hit) {
          pct = warn_pct
          if (k ~ /(cv_1hour|cv_1day)/) pct = hourly_pct
          if (hit[k] * 100 > cyc[k] * pct)
            printf "  ⚠ WARN — %s had pending exclusions in %d/%d cycles (threshold %d%%): mat coverage thin this run\n",
                   k, hit[k], cyc[k], pct
        }
      }' "$VIEW_CSV"
  fi
  if [[ "$ZERO_SIGNAL" -gt 0 ]]; then
    echo "  zero-signal breakdown:"
    awk -F, 'NR>1 && $5==0 && $8=="match" {
      k=$2 "/" $3 "/" $4
      n[k]++
    } END {
      for (k in n) printf "    %-45s rows=%d\n", k, n[k]
    }' "$VIEW_CSV" | sort
  fi
  local VIEW_INCOMPLETE_FAILED=0
  if [[ "$INCOMPLETE" -gt 0 ]]; then
    echo "  incomplete breakdown:"
    awk -F, 'NR>1 {
      k=$2 "/rc=" $3 "/lines=" $4 "/" $7
      n[k]++
    } END {
      for (k in n) printf "    %-45s rows=%d\n", k, n[k]
    }' "$INCOMPLETE_CSV" | sort
    if [[ "$INCOMPLETE_Q" -ge "${SOAK_VIEW_INCOMPLETE_FAIL_AFTER:-3}" ]]; then
      echo "  FAILED — too many non-chaos incomplete correctness cycles ($INCOMPLETE_Q, threshold ${SOAK_VIEW_INCOMPLETE_FAIL_AFTER:-3})"
      local NOW_INC; NOW_INC=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
      echo "$NOW_INC non_chaos_incomplete=$INCOMPLETE_Q threshold=${SOAK_VIEW_INCOMPLETE_FAIL_AFTER:-3} db_unavail_excluded=$DB_UNAVAIL" \
          >> "$RESULTS/verdicts/fail.correctness.view_incomplete"
      VIEW_INCOMPLETE_FAILED=1
      # ESCALATE TO EARLY-STOP.  The old design let the run keep going
      # after this fail was written, on the theory that a fail is "SLO
      # audit only" and only panic.* triggers early-stop.  In practice
      # once the correctness proof has 3+ non-chaos gaps the run's
      # verdict is already decided — every additional hour just burns
      # wall-clock producing no new signal.  SOAK-20260723_170208
      # wrote this fail at 21:13 and burned 51 more minutes before the
      # separate panic.view_check_dead (consecutive-streak-4) fired.
      # Raise a panic so soak.sh's health gate exits the same tick.
      mkdir -p "$RESULTS/signals"
      if [[ ! -f "$RESULTS/signals/panic.view_incomplete_persistent" ]]; then
        echo "non_chaos_incomplete=$INCOMPLETE_Q threshold=${SOAK_VIEW_INCOMPLETE_FAIL_AFTER:-3} at $NOW_INC" \
            > "$RESULTS/signals/panic.view_incomplete_persistent"
      fi
    fi
  fi

  if [[ "$CONFIRMED" -gt 0 ]]; then
    echo "  FAILED — $CONFIRMED bucket(s) still diverged on targeted re-check (>= ${SETTLE}s after first seen); see view_mismatch_*.snapshot.txt"
  elif [[ "$VIEW_INCOMPLETE_FAILED" -eq 1 ]]; then
    echo "  FAILED — correctness proof has too many skipped cycles"
  else
    echo "  PASSED — view = source (no bucket confirmed by re-check; $RESOLVED resolved, $PENDING pending)"
  fi
}

judge_run_view_correctness()    { _judge_eval_view_correctness "$1" >/dev/null 2>&1; }
judge_report_view_correctness() {
  echo "── view correctness ──"
  _judge_eval_view_correctness "$1"
}
