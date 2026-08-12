#!/bin/bash
# judge/lib/chaos.sh — shared helper for chaos-aware verdicts.
#
# Emits the chaos-active time windows as a space-separated list of
# "start_ts|end_ts" pairs, where the timestamps are the ISO-8601 UTC
# strings straight out of chaos_log.csv.  Because ISO-8601 UTC sorts
# lexicographically == chronologically, a verdict awk can test window
# membership with plain string comparison — no epoch conversion (and no
# gawk mktime dependency) needed.
#
# A window is [INJECT.ts, RECOVERY_TIME.ts]: from the moment a fault is
# injected until measure_recovery declared the CAGGs healthy again (or
# timed out).
#
# ── Per-verdict fault whitelist (2026-07-21) ─────────────────────────
# The second argument is a comma-separated list of fault_names the
# CALLING verdict actually cares about.  A fault outside the list is
# NOT emitted — its window is invisible to this verdict.  This models
# "which chaos faults can realistically pollute this verdict's metric":
#
#   fault_name           │ pollutes ...
#   ─────────────────────┼─────────────────────────────────────────
#   refresh_worker_kill  │ everything (SIGKILL resets the instance)
#   compress_worker_kill │ everything (SIGKILL resets the instance)
#   scheduler_kill       │ everything (SIGKILL resets the instance)
#   cluster_crash        │ everything (db goes unqueryable)
#
# ── Worker kills are NOT lightweight (2026-07-29) ────────────────────
# The table above used to scope worker kills narrowly ("refresh_perf,
# watermark_lag" etc.).  That was wrong: these faults SIGKILL a
# background worker that holds shared memory, so postmaster treats it
# like any crashed backend and resets the WHOLE instance --
# "terminating connection because of crash of another server process"
# followed by "the database system is in recovery mode" for every
# session, verified in SOAK-20260728_095744 (14:37:54 refresh_worker_kill
# -> instance reset -> ddl_churn's in-flight cycle and 1031 query_load
# reads all failed).  Any verdict that touches the database at all can
# be polluted by any of these faults, so callers should whitelist all
# four rather than a narrow subset.
#
# Fault_name matching is EXACT (not substring).  Empty/absent second arg
# = accept all faults (backwards compat + emergency debug).
#
# INJECT ↔ RECOVERY_TIME are paired by matching fault_name, not by
# arrival order — that stays robust even when a fault is filtered out
# mid-run.
#
# Optional 3rd arg `grace_sec` extends each window's end by that many
# seconds (post-recovery grace).  The `recovery_budget` stamped on
# RECOVERY_TIME rows is TIGHT — scheduler_kill has a 60 s budget, but
# a killed CAGG refresh worker leaves cv_1min lag climbing for 5-10
# min while it catches up on missed schedules.  Verdicts that count
# "post-window still-elevated" samples as fails (watermark_lag,
# refresh_perf, query_load, view_correctness) all suffered from this
# in SOAK-20260724_121637 (270 exceeded rows on watermark_lag with
# only 21 chaos-window-excluded, yet ALL post-chaos-storm recovery).
# Pass e.g. 300 s to widen every closed window by 5 min; open (still-
# in-flight) windows stay at 9999-12-31 and are unaffected.  Default
# 0 s = backwards compat (no widening).
#
# Optional 4th arg `pre_grace_sec` widens each window's START backwards.
# Needed by any verdict whose sample timestamp marks the BEGINNING of a
# measurement that then ran for a while: the fault can land mid-flight,
# so the recorded timestamp sits BEFORE the window even though the fault
# is exactly what broke the sample.  ddl_churn hit this in
# SOAK-20260728_095744 -- its cycle stamped 14:37:29, its cleanup phase
# ran 24.6 s, and the refresh_worker_kill at 14:37:54 killed it, yet the
# stamp fell 25 s short of the window and the failure was reported as
# non-chaos.  Pass a value >= the longest sample duration the verdict can
# have (ddl_churn uses 120 s).
#
# Usage in a verdict:
#   source "$JUDGE_DIR/lib/chaos.sh"
#   CW=$(chaos_windows_str "$RESULTS/data/chaos_log.csv" \
#                          "cluster_crash,refresh_worker_kill,compress_worker_kill,scheduler_kill" \
#                          "${SOAK_CHAOS_POST_RECOVERY_GRACE_SEC:-300}" \
#                          "${SOAK_CHAOS_PRE_GRACE_SEC:-0}")
#   awk -v windows="$CW" '
#     function in_chaos(ts,  i,a,n,se){ n=split(windows,a," ");
#       for(i=1;i<=n;i++){split(a[i],se,"|"); if(ts>=se[1]&&ts<=se[2])return 1} return 0 }
#     ...  { if (!in_chaos($1)) { ...count toward fail... } }
#   '

# Every fault the framework injects has cluster-wide blast radius (see
# the note above), so this is the whitelist nearly every verdict wants.
# Sharing one constant stops the per-verdict lists from drifting apart
# again -- they had diverged into five different subsets by 2026-07-29,
# and the narrow ones were exactly where false "non-chaos" failures came
# from.  A verdict that genuinely needs a subset can still pass its own
# string.
CHAOS_ALL_FAULTS="cluster_crash,refresh_worker_kill,compress_worker_kill,scheduler_kill"

chaos_windows_str() {
  local chaos_csv="$1"
  local care_faults="${2:-}"
  local grace_sec="${3:-0}"
  local pre_grace_sec="${4:-0}"
  [[ -s "$chaos_csv" ]] || return 0
  local raw
  raw=$(awk -F, -v care_faults="$care_faults" '
    BEGIN {
      # Empty list = accept all faults (backwards compat).  Otherwise
      # parse into an associative set and enforce membership.
      n = split(care_faults, a, ",")
      for (i = 1; i <= n; i++) if (a[i] != "") care[a[i]] = 1
      filter = (length(care_faults) > 0)
    }
    $2 == "INJECT" && (!filter || ($3 in care)) {
      # Track this fault as pending; RECOVERY_TIME with the SAME
      # fault_name will close it.  Filtered-out faults never open a
      # window, so their RECOVERY_TIME just falls through.
      pend_ts = $1; pend_name = $3; pend = 1
    }
    $2 == "RECOVERY_TIME" && pend && $3 == pend_name {
      printf "%s|%s ", pend_ts, $1
      pend = 0; pend_name = ""
    }
    END {
      # An INJECT with no RECOVERY_TIME yet = a fault STILL IN FLIGHT
      # (the streaming judge_run evaluates mid-fault, before the window
      # closes).  Emit it as an OPEN window [inject_ts, far-future] so
      # samples taken during the active fault are excluded — otherwise
      # streaming would false-fail before the fault is even recovered.
      if (pend) printf "%s|9999-12-31T23:59:59Z ", pend_ts
    }
  ' "$chaos_csv")

  if [[ "${grace_sec:-0}" -le 0 && "${pre_grace_sec:-0}" -le 0 ]]; then
    printf '%s' "$raw"
    return
  fi

  # Widen each window: start backwards by pre_grace_sec (to catch samples
  # whose measurement began before the fault but was still running when it
  # landed), end forwards by grace_sec.  Open windows (9999-12-31...) keep
  # their far-future end; their post-recovery grace is applied on the next
  # call after RECOVERY_TIME lands, but their start still gets pre-grace.
  local win start end out=""
  for win in $raw; do
    start="${win%|*}"; end="${win#*|}"
    if [[ "${pre_grace_sec:-0}" -gt 0 ]]; then
      start=$(date -u -d "$start - ${pre_grace_sec} seconds" '+%Y-%m-%dT%H:%M:%SZ')
    fi
    if [[ "$end" == 9999-* || "${grace_sec:-0}" -le 0 ]]; then
      out+="${start}|${end} "
    else
      out+="${start}|$(date -u -d "$end + ${grace_sec} seconds" '+%Y-%m-%dT%H:%M:%SZ') "
    fi
  done
  printf '%s' "$out"
}
