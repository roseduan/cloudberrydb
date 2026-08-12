#!/bin/bash
# judge/verdicts/crash_events.sh
# Verdict: backend deaths (signals / PANIC / instance resets).
#
# Reads monitor/crash_events.sh's CSV and decides which deaths were
# EXPECTED (chaos injected them) and which are real:
#
#   non-chaos crash        -> fail.crash            (a crash nobody asked for)
#   crash loop             -> signals/panic.crash_loop (run-aborting)
#
# ── Why a dedicated crash-loop rule ─────────────────────────────────
# SOAK-20260725_163359: one chaos kill left a committed PAX chunk file
# truncated; every subsequent scan of it SIGSEGV'd a segment, once a
# minute, for 7.7 hours -- through the end of the run and beyond.  Two
# things made that expensive:
#
#   1. Nothing watched for crashes directly, so the only signal was
#      scrape_liveness noticing the monitors had gone stale.
#   2. The crashes were *downstream* of a chaos fault, so a naive
#      "was it inside a chaos window?" test would have excused them --
#      the fault was 20 minutes earlier and long since "recovered".
#
# So the loop test is deliberately independent of chaos windows: a
# healthy cluster does not crash repeatedly no matter what was injected.
# CRASH_LOOP_N deaths inside CRASH_LOOP_WINDOW_MIN minutes means the
# cluster is not recovering, and that is worth stopping the run for --
# every further hour produces the same evidence again.
#
# Chaos DOES excuse isolated crashes, with generous grace: a
# cluster_crash is literally implemented as pkill, and a worker kill
# resets the instance (see judge/lib/chaos.sh), so those windows are
# expected to contain deaths.

_judge_eval_crash_events() {
  local RESULTS="$1"
  local CSV="$RESULTS/data/crash_events.csv"
  mkdir -p "$RESULTS/verdicts" "$RESULTS/signals"
  local VERDICT_FILE="$RESULTS/verdicts/fail.crash"
  local PANIC_FILE="$RESULTS/signals/panic.crash_loop"
  local NOW; NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  if [[ ! -s "$CSV" ]]; then
    echo "  (no crash_events.csv — first monitor cycle has not fired)"
    return 0
  fi

  local TOTAL
  TOTAL=$(awk -F, 'NR>1 {n++} END {print n+0}' "$CSV")
  if [[ "$TOTAL" -eq 0 ]]; then
    echo "  ✓ OK — no backend deaths, PANICs or instance resets observed"
    return 0
  fi

  # Per-kind and per-seg breakdown, plus the crash-loop test.
  #
  # Timestamps in this CSV come from the postmaster log (server
  # log_timezone), while chaos_log.csv is UTC.  Rather than convert, the
  # chaos-window test is applied only to the count line as an FYI and the
  # authoritative rules below (non-chaos count, loop density) use a
  # window-free formulation:
  #   - the loop test is chaos-independent by design (see header);
  #   - the "unexplained" count treats any run with chaos enabled as
  #     having an expected baseline, and only flags deaths beyond the
  #     number of injected faults.
  local INJECTS=0
  if [[ -s "$RESULTS/data/chaos_log.csv" ]]; then
    INJECTS=$(awk -F, 'NR>1 && $2=="INJECT" {n++} END {print n+0}' \
                  "$RESULTS/data/chaos_log.csv")
  fi

  awk -F, '
    NR>1 {
      kind[$5]++
      seg[$2]++
      n++
    }
    END {
      printf "  events total : %d\n", n+0
      line=""
      for (k in kind) line = line sprintf("%s=%d ", k, kind[k])
      printf "  by kind      : %s\n", line
      line=""
      for (s in seg) line = line sprintf("%s=%d ", s, seg[s])
      printf "  by segment   : %s\n", line
    }
  ' "$CSV"

  # ── Crash-loop test ────────────────────────────────────────────────
  # Slide a real CRASH_LOOP_WINDOW_MIN-minute window over the events and
  # fail if any position holds CRASH_LOOP_N or more of them.
  #
  # Timestamps are converted to an absolute minute index with plain
  # integer arithmetic (Julian day number), NOT mktime: the CSV timestamps
  # come from the postmaster log and gawk is not guaranteed here -- the
  # same portability constraint judge/lib/chaos.sh documents.
  #
  # The input is sorted first: crash_events.csv is append-only but NOT
  # chronological, because each monitor pass walks the coordinator and
  # every segment datadir in turn, so a later segment's older events land
  # after an earlier one's newer events.
  local LOOP_N="${SOAK_CRASH_LOOP_N:-3}"
  local LOOP_WIN="${SOAK_CRASH_LOOP_WINDOW_MIN:-10}"
  local LOOP_BAD
  LOOP_BAD=$(tail -n +2 "$CSV" | sort -t, -k1,1 | awk -F, \
                 -v want="$LOOP_N" -v win="$LOOP_WIN" \
                 -v pf="$PANIC_FILE" -v now="$NOW" '
    # "2026-07-25 22:25:30.123 BST" -> minutes since an arbitrary epoch
    function minute_index(s,   d, t, Y, M, D, hh, mm, a, y, m, jdn) {
      split(substr(s, 1, 10), d, "-")
      split(substr(s, 12, 5), t, ":")
      Y = d[1] + 0; M = d[2] + 0; D = d[3] + 0
      hh = t[1] + 0; mm = t[2] + 0
      if (Y == 0 || M == 0 || D == 0) return -1
      a = int((14 - M) / 12); y = Y + 4800 - a; m = M + 12 * a - 3
      jdn = D + int((153 * m + 2) / 5) + 365 * y + int(y / 4) \
            - int(y / 100) + int(y / 400) - 32045
      return jdn * 1440 + hh * 60 + mm
    }
    { mi = minute_index($1); if (mi >= 0) { idx[++k] = mi; str[k] = substr($1, 1, 16) } }
    END {
      worst = 0; worst_from = ""; worst_to = ""
      # Two pointers: for each start i, extend j while within the window.
      j = 1
      for (i = 1; i <= k; i++) {
        if (j < i) j = i
        while (j < k && idx[j + 1] - idx[i] < win) j++
        n_in = j - i + 1
        if (n_in > worst) { worst = n_in; worst_from = str[i]; worst_to = str[j] }
      }
      if (worst >= want) {
        printf "%s crash_loop: %d deaths between %s and %s (threshold %d in %d min)\n",
               now, worst, worst_from, worst_to, want, win >> pf
        print 1
      } else print 0
    }
  ')

  # ── Unexplained deaths ─────────────────────────────────────────────
  # With chaos off, ANY death is unexplained.  With chaos on, the
  # injected faults are expected to kill things, so only flag when the
  # death count runs well ahead of the injection count -- a single
  # cluster_crash legitimately shows up as several deaths (the
  # coordinator plus each segment), hence the multiplier.
  local PER_INJECT="${SOAK_CRASH_PER_INJECT_ALLOWANCE:-8}"
  local ALLOWED=$(( INJECTS * PER_INJECT ))
  if [[ "$TOTAL" -gt "$ALLOWED" ]]; then
    printf '%s deaths=%d injects=%d allowance=%d\n' \
           "$NOW" "$TOTAL" "$INJECTS" "$ALLOWED" >> "$VERDICT_FILE"
    echo "  🔴 CRITICAL — $TOTAL deaths exceed the $ALLOWED expected from $INJECTS chaos injection(s)"
  fi

  if [[ "$LOOP_BAD" -gt 0 ]]; then
    echo "  🔴 PANIC — crash loop detected (>= $LOOP_N deaths within $LOOP_WIN min): cluster is not recovering"
  elif [[ "$TOTAL" -le "$ALLOWED" ]]; then
    if [[ "$INJECTS" -gt 0 ]]; then
      echo "  ✓ OK — all $TOTAL death(s) consistent with $INJECTS chaos injection(s), no crash loop"
    else
      echo "  ✓ OK — no crash loop"
    fi
  fi
}

judge_run_crash_events()    { _judge_eval_crash_events "$1" >/dev/null 2>&1; }
judge_report_crash_events() {
  echo "── crash events (backend deaths / PANIC / instance resets) ──"
  _judge_eval_crash_events "$1"
}
