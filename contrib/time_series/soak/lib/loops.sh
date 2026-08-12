#!/bin/bash
# lib/loops.sh — loop discovery + process-group lifecycle.
#
# Sourced by soak.sh (and tools/report.sh for discovery only).
# Never executed directly.
#
# ── The SOAK-LOOP contract ───────────────────────────────────────────
#
# Any script under monitor/ workload/ chaos/ tools/ that carries a
# header line of the form
#
#   # SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_FOO_EVERY order=30 mode=oneshot
#
# is auto-discovered and run as a background loop.  Adding a new
# monitor/workload = dropping in ONE self-describing file; no driver,
# registry, or report edits required (open-closed principle).
#
# Header fields (all optional except scope):
#   scope=per-db|global   per-db → invoked once per DB in $SOAK_DBS
#   mode=oneshot|self     oneshot (default): script does ONE collection
#                         pass and exits; the supervisor wraps it in a
#                         sleep-loop.  self: script runs forever and
#                         manages its own cadence (workload, chaos,
#                         watchdog); supervisor only starts/stops it.
#   interval=N            oneshot cadence in seconds (default 300)
#   interval_var=NAME     env var that overrides interval (default:
#                         SOAK_<NAME>_EVERY derived from the filename).
#                         Resolved value 0 disables the loop.
#   order=N               report section ordering (default 50); used by
#                         tools/report.sh, ignored at runtime.
#
# Invocation contract (what the supervisor calls):
#   oneshot per-db : bash script.sh <db> <results_dir>
#   oneshot global : bash script.sh <results_dir>
#   self    per-db : SOAK_DB=<db> bash script.sh <results_dir>
#   self    global : bash script.sh <results_dir>
#
# Report contract (used by tools/report.sh, NOT the supervisor):
#   bash script.sh --report <results_dir>   → prints its report section
#
# ── Why process groups ───────────────────────────────────────────────
# Every loop instance is launched via setsid, making it the leader of a
# fresh process group.  Shutdown is then `kill -- -PGID`: the loop, its
# psql children, and any tsbs pipeline all die together.  This replaces
# three generations of leak workarounds (kill_tree pkill -P walks, the
# belt-and-suspenders pkill -f pattern list) that each missed a case —
# most memorably pkill -f matching the driver's own docker-exec command
# line and killing the cleanup mid-sweep.

[[ -n "${_SOAK_SUPERVISOR_LOADED:-}" ]] && return 0
_SOAK_SUPERVISOR_LOADED=1

# discover_loops <soak_dir>
#   Emits one line per SOAK-LOOP script:
#     name|script_path|scope|mode|interval|interval_var|order
discover_loops() {
  local soak_dir="$1"
  local f hdr name scope mode interval interval_var order
  for f in "$soak_dir"/monitor/*.sh "$soak_dir"/workload/*.sh \
           "$soak_dir"/chaos/*.sh   "$soak_dir"/tools/*.sh \
           "$soak_dir"/judge/*.sh; do
    [[ -f "$f" ]] || continue
    hdr=$(head -20 "$f" | grep -m1 '^# SOAK-LOOP:') || continue
    name=$(basename "$f" .sh)
    scope=$(echo "$hdr"        | grep -oE 'scope=[a-z-]+'        | cut -d= -f2)
    mode=$(echo "$hdr"         | grep -oE 'mode=[a-z]+'          | cut -d= -f2)
    interval=$(echo "$hdr"     | grep -oE 'interval=[0-9]+'      | cut -d= -f2)
    interval_var=$(echo "$hdr" | grep -oE 'interval_var=[A-Z_]+' | cut -d= -f2)
    order=$(echo "$hdr"        | grep -oE 'order=[0-9]+'         | cut -d= -f2)
    [[ -z "$scope" ]] && { echo "WARN: $f has SOAK-LOOP header without scope= — skipped" >&2; continue; }
    mode="${mode:-oneshot}"
    interval="${interval:-300}"
    interval_var="${interval_var:-SOAK_$(echo "$name" | tr '[:lower:]' '[:upper:]')_EVERY}"
    order="${order:-50}"
    echo "${name}|${f}|${scope}|${mode}|${interval}|${interval_var}|${order}"
  done
}

# _launch_pgrp <pidfile> <logfile> <cmd...>
#   Start cmd in its own process group; record PGID for stop_all_loops.
_launch_pgrp() {
  local pidfile="$1" logfile="$2"
  shift 2
  if command -v setsid >/dev/null 2>&1; then
    setsid "$@" >> "$logfile" 2>&1 &
  else
    # Non-Linux fallback (dev laptops): plain background job.  PGID ==
    # the driver's group, so stop_all_loops falls back to direct kill.
    "$@" >> "$logfile" 2>&1 &
  fi
  echo $! > "$pidfile"
}

# start_loops <soak_dir> <results_dir>
#   Discover and start every enabled loop.  Reads $SOAK_DBS for per-db
#   fan-out and $SOAK_DISABLE_LOOPS (space-separated names) for opt-out.
start_loops() {
  local soak_dir="$1" results="$2"
  local pids_dir="$results/state/pids"
  mkdir -p "$pids_dir"
  local line name script scope mode interval interval_var order resolved db
  while IFS='|' read -r name script scope mode interval interval_var order; do
    if [[ " ${SOAK_DISABLE_LOOPS:-} " == *" $name "* ]]; then
      soak_log "  loop[$name] DISABLED via SOAK_DISABLE_LOOPS"
      continue
    fi
    resolved="${!interval_var:-$interval}"
    if [[ "$mode" == "oneshot" && "$resolved" -le 0 ]]; then
      soak_log "  loop[$name] disabled ($interval_var=0)"
      continue
    fi
    case "$mode/$scope" in
      self/per-db)
        for db in $SOAK_DBS; do
          SOAK_DB="$db" _launch_pgrp "$pids_dir/${name}_${db}" \
            "$results/logs/${name}_${db}.log" \
            env SOAK_DB="$db" bash "$script" "$results"
          soak_log "  loop[$name:$db] pgid=$(cat "$pids_dir/${name}_${db}") (self)"
        done
        ;;
      self/global)
        _launch_pgrp "$pids_dir/$name" "$results/logs/${name}.log" \
          bash "$script" "$results"
        soak_log "  loop[$name] pgid=$(cat "$pids_dir/$name") (self)"
        ;;
      oneshot/per-db)
        # $4 = beat file (liveness), $5 = stagger cap (de-phasing).
        # touch-before-first-sleep so the beat exists from second one;
        # touch-per-cycle proves the wrapper is still alive.
        _launch_pgrp "$pids_dir/$name" "$results/logs/${name}.log" \
          bash -c 'touch "$4"
                   cap=$(( $5 < $1 ? $5 : $1 ))
                   [[ "$cap" -gt 0 ]] && sleep $(( RANDOM % (cap + 1) ))
                   while true; do
                     sleep "$1"
                     touch "$4"
                     for db in $SOAK_DBS; do
                       # timeout 5x interval: a single hung collector (e.g. a
                       # psql stuck behind post-crash-reinit state) must not
                       # wedge the wrapper past the 10x permanently-dead
                       # threshold -- SOAK-20260723_101159 lost its last 2h to
                       # exactly that (watermark_lag hung once, never returned,
                       # panic gate early-stopped the run).  -k 10 escalates to
                       # SIGKILL if the collector ignores TERM.
                       timeout -k 10 $(( $1 * 5 )) bash "$2" "$db" "$3" || true
                     done
                   done' _ "$resolved" "$script" "$results" \
                        "$pids_dir/${name}.beat" "${SOAK_LOOP_STAGGER_MAX:-60}"
        soak_log "  loop[$name] pgid=$(cat "$pids_dir/$name") every ${resolved}s per-db"
        ;;
      oneshot/global)
        _launch_pgrp "$pids_dir/$name" "$results/logs/${name}.log" \
          bash -c 'touch "$4"
                   cap=$(( $5 < $1 ? $5 : $1 ))
                   [[ "$cap" -gt 0 ]] && sleep $(( RANDOM % (cap + 1) ))
                   while true; do
                     sleep "$1"
                     touch "$4"
                     # Same 5x-interval hang cap as the per-db branch.
                     timeout -k 10 $(( $1 * 5 )) bash "$2" "$3" || true
                   done' _ "$resolved" "$script" "$results" \
                        "$pids_dir/${name}.beat" "${SOAK_LOOP_STAGGER_MAX:-60}"
        soak_log "  loop[$name] pgid=$(cat "$pids_dir/$name") every ${resolved}s"
        ;;
      *)
        soak_log "  loop[$name] SKIPPED: unknown mode/scope '$mode/$scope'"
        ;;
    esac
  done < <(discover_loops "$soak_dir")
}

# check_loop_beats <soak_dir> <results_dir>
#   Liveness check for oneshot loops.  A wrapper touches its .beat file
#   every cycle.  Two thresholds, no in-between "warn" state:
#     age > STALE_MULT × interval          — soak_log a note only
#                                            (short blip, no flag)
#     age > PERMANENTLY_DEAD_MULT × interval — raise
#                                            signals/panic.loop_permanently_dead.<name>
#                                            (health gate stops the run
#                                            — a monitor loop that stayed
#                                            dead for that long left an
#                                            observation gap so wide the
#                                            rest of the run is spinning
#                                            without visibility).
#   Self-mode loops are excluded (they own their cadence and have their
#   own liveness signals, if any).
check_loop_beats() {
  local soak_dir="$1" results="$2"
  local stale_mult="${SOAK_LOOP_BEAT_STALE_MULT:-3}"
  local dead_mult="${SOAK_LOOP_PERMANENTLY_DEAD_MULT:-10}"
  local now line name script scope mode interval interval_var order
  local resolved bf mt age flag
  now=$(date +%s)
  while IFS='|' read -r name script scope mode interval interval_var order; do
    [[ "$mode" == "oneshot" ]] || continue
    [[ " ${SOAK_DISABLE_LOOPS:-} " == *" $name "* ]] && continue
    resolved="${!interval_var:-$interval}"
    [[ "$resolved" -le 0 ]] && continue
    bf="$results/state/pids/${name}.beat"
    [[ -f "$bf" ]] || continue   # loop never started (or already flagged + cleaned)
    mt=$(stat -c %Y "$bf" 2>/dev/null || stat -f %m "$bf" 2>/dev/null || echo "$now")
    age=$(( now - mt ))
    if [[ "$age" -gt $(( resolved * dead_mult )) ]]; then
      flag="$results/signals/panic.loop_permanently_dead.${name}"
      if [[ ! -f "$flag" ]]; then
        soak_log "LOOP PERMANENTLY DEAD: $name beat is ${age}s old (> ${dead_mult}×${resolved}s) — raising signals/panic.loop_permanently_dead.$name"
        echo "name=$name beat_age=${age}s interval=${resolved}s permanently_dead_mult=${dead_mult} at $(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$flag"
      fi
    elif [[ "$age" -gt $(( resolved * stale_mult )) ]]; then
      soak_log "[watch] loop $name beat aged out (${age}s > ${stale_mult}×${resolved}s) — awaiting recovery"
    fi
  done < <(discover_loops "$soak_dir")
}

# stop_all_loops <results_dir>
#   TERM each recorded process group, grace period, then KILL.
stop_all_loops() {
  local results="$1"
  local pids_dir="$results/state/pids"
  local pf pgid
  [[ -d "$pids_dir" ]] || return 0
  for pf in "$pids_dir"/*; do
    [[ -f "$pf" ]] || continue
    pgid=$(cat "$pf")
    kill -TERM -- "-$pgid" 2>/dev/null || kill -TERM "$pgid" 2>/dev/null || true
  done
  # TERM → KILL grace: cleanup traps must fit their CHEAP work (log
  # lines, one psql) inside this window — see chaos_loop cleanup.
  sleep "${SOAK_STOP_GRACE_SEC:-2}"
  for pf in "$pids_dir"/*; do
    [[ -f "$pf" ]] || continue
    pgid=$(cat "$pf")
    kill -KILL -- "-$pgid" 2>/dev/null || kill -KILL "$pgid" 2>/dev/null || true
    rm -f "$pf"
  done
}
