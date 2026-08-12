#!/bin/bash
# judge/verdicts/framework_health.sh
#
# Verdict: is the SOAK FRAMEWORK ITSELF still able to run?  This is the
# one verdict that raises panic.* (early-stop) flags rather than fail.*
# (business) flags — soak.sh's health gate polls panic.* every cycle and
# aborts the run gracefully, so we don't burn the remaining hours after
# the ground has fallen out from under us.
#
# It owns the three early-stop DECISIONS that used to live inside the
# collectors (moved here 2026-07-09 so monitors are pure collection):
#
#   panic.view_check_dead — view_check produced INCOMPLETE output for
#       SOAK_VIEW_INCOMPLETE_ABORT cycles in a row (the check itself is
#       wedged, not the data).  Source: db_health.csv status=incomplete.
#   panic.env_lost        — a soak DB's OID changed: it was dropped and
#       recreated under us, so every subsequent comparison is garbage.
#       Source: db_health.csv status=oid_changed.
#   panic.disk_full       — disk crossed SOAK_DISK_KILL_PCT; PG refuses
#       WAL writes near 100% and everything stalls.  Source: latest
#       system_metrics.csv disk_pct.
#
# CHAOS-AWARE.  Under chaos (esp. chaos=2 cluster crash) the CAGGs go
# unqueryable for the recovery window BY DESIGN — view_check logs a burst
# of incompletes that must NOT trip view_check_dead.  We exclude samples
# inside a chaos window from the streak, which is exactly why this
# decision had to leave the collector: the collector has no idea a chaos
# window is open, but judge does (chaos_log.csv → chaos_windows_str).
# env_lost and disk_full are identity/physical invariants that a crash+
# gpstart never legitimately produces, so they fire regardless of chaos.
#
# Sourced by judged.sh; judge_run_* every tick (streaming, so a panic is
# raised within one judge interval of the triggering sample) and
# judge_report_* at report time.

_judge_eval_framework_health() {
  local results="$1"
  local dbh="$results/data/db_health.csv"
  local sm="$results/data/system_metrics.csv"
  # All three panic.* files go under signals/ (runtime IPC — read by
  # soak.sh's health gate every 60s).
  local signals="$results/signals"
  mkdir -p "$signals"
  local ts; ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  # ── 1+2. db_health streak (view_check_dead) + oid (env_lost) ────────
  if [[ ! -s "$dbh" ]]; then
    echo "  (no db_health.csv — view_correctness_scrape has not fired yet)"
  else
    # CARE: view_check_dead is about the DB being unqueryable for a
    # sustained stretch — only cluster_crash produces that.  BGW-related
    # faults leave the DB queryable → an incomplete streak during THOSE
    # chaos is a real wedge, not chaos noise.
    local CW; CW=$(chaos_windows_str "$results/data/chaos_log.csv" \
                   "$CHAOS_ALL_FAULTS" 2>/dev/null)
    awk -F, \
        -v abort="${SOAK_VIEW_INCOMPLETE_ABORT:-4}" \
        -v windows="$CW" \
        -v now="$ts" \
        -v dead_flag="$signals/panic.view_check_dead" \
        -v env_flag="$signals/panic.env_lost" '
      function in_chaos(t,  i,a,m,se){ m=split(windows,a," ");
        for(i=1;i<=m;i++){split(a[i],se,"|"); if(t>=se[1]&&t<=se[2])return 1} return 0 }
      NR == 1 { next }
      {
        db=$2; status=$3; seen[db]=1
        if (status == "oid_changed") { oid_lost[db]=$4; next }
        # Chaos-window incompletes are expected: they neither advance the
        # streak nor reset it (a mid-crash "ok" would be a fluke anyway).
        if (in_chaos($1)) { chaos_skipped[db]++; next }
        # db_unavailable = DB crash-recovering/restarting when the check ran
        # (e.g. a chaos SIGKILL of a BGW → full cluster crash-reinit).
        # Infrastructure down, not a wedged check → neutral for the
        # view_check_dead streak (a real crash-loop shows up in the
        # server log, not in this streak).
        if (status == "db_unavailable") { db_down[db]++; next }
        if (status == "incomplete") streak[db]++
        else                        streak[db]=0   # any "ok" clears it
        if (streak[db] > peak[db]) peak[db] = streak[db]
      }
      END {
        for (db in seen) {
          note = ""
          if (chaos_skipped[db] > 0) note = sprintf(" (%d chaos samples excluded)", chaos_skipped[db])
          if (db in oid_lost) {
            printf "  %-14s 🔴 PANIC env_lost: %s\n", db, oid_lost[db]
            printf "db=%s %s at %s\n", db, oid_lost[db], now > env_flag
          } else if (streak[db]+0 >= abort+0) {
            printf "  %-14s 🔴 PANIC view_check_dead: %d consecutive incomplete (>= %d)%s\n", db, streak[db], abort, note
            printf "db=%s consecutive_incomplete=%d at %s\n", db, streak[db], now > dead_flag
          } else {
            printf "  %-14s ✓ OK  trailing_incomplete=%d/%d peak=%d%s\n", db, streak[db]+0, abort, peak[db]+0, note
          }
        }
      }
    ' "$dbh"
  fi

  # ── 3. disk_full ────────────────────────────────────────────────────
  local kill_pct="${SOAK_DISK_KILL_PCT:-0}"
  local warn_pct="${SOAK_DISK_WARN_PCT:-0}"
  if [[ ! -s "$sm" ]]; then
    echo "  (no system_metrics.csv — disk not sampled yet)"
  else
    local last_pct
    last_pct=$(tail -1 "$sm" | cut -d, -f2)
    if [[ "$last_pct" =~ ^[0-9]+$ ]]; then
      if [[ "$kill_pct" -gt 0 && "$last_pct" -ge "$kill_pct" ]]; then
        echo "disk_pct=${last_pct}% threshold=${kill_pct}% at $ts" > "$signals/panic.disk_full"
        printf "  disk           🔴 PANIC disk_full: %s%% >= %s%%\n" "$last_pct" "$kill_pct"
      elif [[ "$warn_pct" -gt 0 && "$last_pct" -ge "$warn_pct" ]]; then
        printf "  disk           ⚠ WARN  %s%% (warn@%s%% kill@%s%%)\n" "$last_pct" "$warn_pct" "$kill_pct"
      else
        printf "  disk           ✓ OK   %s%% (warn@%s%% kill@%s%%)\n" "$last_pct" "${warn_pct:-off}" "${kill_pct:-off}"
      fi
    fi
  fi
}

judge_run_framework_health() {
  _judge_eval_framework_health "$1" >/dev/null 2>&1
}

judge_report_framework_health() {
  echo "── framework health (early-stop gate) ──"
  _judge_eval_framework_health "$1"
}
