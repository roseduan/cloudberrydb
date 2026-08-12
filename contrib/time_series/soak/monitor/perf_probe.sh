#!/bin/bash
# monitor/perf_probe.sh
# SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_PERF_PROBE_EVERY order=20
#
# CAGG view latency + plan probe — samples user-facing read paths so soak
# detects performance regressions (view falls back to
# scanning the source table when the planner can't constify the
# watermark; or plan shape silently degrades — latency moves a bit but
# no one notices).
#
# DESIGN — EXPLAIN ANALYZE INSTEAD OF BASH WALL-CLOCK
#
# Previous version (v1) used `t0=$(date +%s%3N); psql -c "$sql"; t1=...`
# to measure latency, then subtracted.  Two problems with that:
#
#   (a) psql cold-start overhead is ~50-100ms.  For a 2-5ms count(*)
#       query, the bash-measured number is 90%+ psql startup — a real
#       2× slowdown in the query (2ms → 5ms) barely moves the number.
#       Signal-to-noise ratio terrible for fast probes.
#
#   (b) No visibility into plan shape.  Scan-path regression manifests as plan
#       regression first (live branch chooses Seq Scan over chunk-aware
#       scan), latency degradation second.  Catching plan shape changes
#       directly is way more sensitive than waiting for latency to creep.
#
# This version uses `EXPLAIN (ANALYZE, TIMING ON, COSTS OFF, BUFFERS,
# FORMAT TEXT) <query>` for every probe.  We parse:
#
#   * `Execution Time: X ms`  → server-side query execution time
#     (excludes psql startup, network, fetch — what the query engine
#     actually spent).  This is what regression detection cares about.
#   * `Planning Time: X ms`   → planner overhead (independent signal).
#   * Plan text minus numbers → SHA-style signature, fingerprints
#     plan shape across runs.  Any shape change = real plan regression.
#
# What we lose: the previous "rows" column.  EXPLAIN ANALYZE executes
# the query but DOES NOT ship results to the client, so we can't count
# returned rows.  For our probes (4 × count(*), 1 × LIMIT 1000) the row
# count was either always 1 or always ≤ 1000 — not useful as a signal.
# Dropped.
#
# CSV format (one row per probe per cycle, per DB):
#
#   ts, db, probe, exec_ms, plan_ms, plan_sig
#
# - exec_ms: PG's reported Execution Time (server-side)
# - plan_ms: PG's reported Planning Time
# - plan_sig: 12-hex-char prefix of md5sum(plan_text minus cost/rows/timing)
#             → stable across executions with same plan, changes on plan shape diff
#
# Usage (invoked by lib/loops.sh):
#   perf_probe.sh <db> <results_dir>           one collection pass

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

# ── Collection mode ──────────────────────────────────────────────────
DB=${1:?usage: perf_probe.sh <db> <results_dir>}
RESULTS=${2:?usage: perf_probe.sh <db> <results_dir>}
CSV="$RESULTS/data/perf_probe.csv"
export SOAK_DB="$DB"

# Header (only if file is empty)
if [[ ! -s "$CSV" ]]; then
  echo "ts,db,probe,exec_ms,plan_ms,plan_sig" > "$CSV"
fi

# Directory for first-cycle plan snapshots (full EXPLAIN output per
# (db, probe), captured once at run start so the driver report can
# reference the "baseline plan" if a later cycle's plan_sig changes).
PLAN_DIR="$RESULTS/plan_snapshots"
mkdir -p "$PLAN_DIR"

# Compute a stable plan signature: md5 of the EXPLAIN output with all
# cost / rows / timing / buffer values stripped out (these legitimately
# vary across runs).  Same plan shape → same sig.
#
# 2026-06 fix: rounds 1-3 saw plan_sig change on virtually every cycle
# (476 distinct sigs in 476 cycles), producing constant false-positive
# "PLAN DRIFT" alarms.  Root cause: probe queries embed `now() -
# INTERVAL '1 hour'`, which PG constant-folds at plan time into a fresh
# `'<NOW>'::timestamp with time zone` literal inside the EXPLAIN's
# Filter clause every execution.  Normalize those to a constant marker
# so genuine plan-shape changes are detectable again.  Also normalize a
# few non-deterministic memory/parallelism counters that legitimately
# fluctuate without implying plan-shape change.
plan_signature() {
  echo "$1" \
    | sed '/^Planning:/,$d' \
    | grep -v -E "^(Execution|Planning) Time:" \
    | sed -E 's/cost=[0-9.]+\.\.[0-9.]+//g' \
    | sed -E 's/rows=[0-9]+//g' \
    | sed -E 's/width=[0-9]+//g' \
    | sed -E 's/actual time=[0-9.]+\.\.[0-9.]+//g' \
    | sed -E 's/loops=[0-9]+//g' \
    | sed -E 's/Rows Removed by Filter: [0-9]+/Rows Removed by Filter: N/g' \
    | sed -E 's/Buffers:.*//g' \
    | sed -E 's/Heap Blocks:.*//g' \
    | sed -E 's/Heap Fetches:.*//g' \
    | sed -E 's/[0-9]+ ms//g' \
    | sed -E "s/'[0-9]{4}-[0-9]{2}-[0-9]{2}[^']*'::timestamp( with time zone)?/TS/g" \
    | sed -E 's/Memory Usage: [0-9]+kB/Memory Usage: NkB/g' \
    | sed -E 's/Memory: [0-9]+kB/Memory: NkB/g' \
    | sed -E 's/[Ww]orkers (Planned|Launched): [0-9]+/Workers/g' \
    | sed -E 's/peak memory: [0-9]+ kB/peak memory: NkB/g' \
    | sed -E 's/[0-9]+K bytes/NK bytes/g' \
    | grep -v '^ *Extra Text:' \
    | awk 'NF' \
    | md5sum \
    | head -c 12
}
#
# `awk 'NF'` (collapse blank lines).  MPP EXPLAIN emits one `Extra
# Text:` block per segment that actually reported execution stats, each
# followed by a blank line.  The grep above strips the text rows but
# leaves the trailing blanks behind, so a query that reported on 1 seg
# vs 2 segs hashed differently despite identical plan shape -- producing
# spurious "PLAN DRIFT" warnings every time MPP load balancing
# reassigned the reporting segment.  Collapse blank lines so the
# fingerprint reflects plan shape only, not which segment ran.

# Run one probe via EXPLAIN ANALYZE.
# Captures: server-side exec time, planning time, plan signature.
# On first cycle for each (db, probe), also dumps full plan to PLAN_DIR.
probe() {
  local label=$1
  local sql=$2
  local output exec_ms plan_ms plan_sig
  local snapshot_file="$PLAN_DIR/${DB}_${label}_init.txt"
  local err_file
  err_file="$RESULTS/errors/perf_probe.err"

  # EXPLAIN (ANALYZE, TIMING ON, ...) executes the query server-side
  # and reports Execution Time + Planning Time + a full plan tree.
  # FORMAT TEXT is the default but explicit-is-better.
  output=$(PGOPTIONS='--client-min-messages=warning -c optimizer=off -c search_path=public,time_series' \
           psql -h "${SOAK_HOST:-localhost}" \
                -p "${SOAK_PORT:-7000}" \
                -U "${SOAK_USER:-gpadmin}" \
                -d "${SOAK_DB:-soak_test}" \
                -X -v ON_ERROR_STOP=1 -At \
                -c "EXPLAIN (ANALYZE, TIMING ON, COSTS OFF, BUFFERS, FORMAT TEXT) $sql" \
                2>>"$err_file") || {
    # Probe failed entirely — record an ERR row so the driver can
    # tally cycle-failure rate without leaving a gap in the CSV.
    printf '%s,%s,%s,%s,%s,%s\n' \
      "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$DB" "$label" "ERR" "ERR" "ERR" >> "$CSV"
    return
  }

  exec_ms=$(echo "$output" | grep -E "^Execution Time:" | grep -oE "[0-9.]+" | head -1)
  plan_ms=$(echo "$output" | grep -E "^Planning Time:"  | grep -oE "[0-9.]+" | head -1)
  plan_sig=$(plan_signature "$output")

  # Default to ERR if parsing failed (shouldn't happen on healthy PG,
  # but be defensive against unexpected EXPLAIN output formats).
  [[ -z "$exec_ms" ]] && exec_ms="ERR"
  [[ -z "$plan_ms" ]] && plan_ms="ERR"
  [[ -z "$plan_sig" ]] && plan_sig="ERR"

  # Capture full plan snapshot on first observation of this (db, probe)
  # AND on every subsequent plan_sig change.  Without the per-change
  # capture, the driver report can flag "PLAN DRIFT" with only the
  # baseline plan on disk -- no way to see what the *new* (typically
  # slower) plan looks like.  Per-(db,probe) cap below prevents a
  # pathological flapping probe from filling the snapshot dir.
  local sig_state="$PLAN_DIR/.${DB}_${label}.last_sig"
  local prev_sig=""
  [[ -f "$sig_state" ]] && prev_sig="$(cat "$sig_state" 2>/dev/null)"
  if [[ ! -f "$snapshot_file" ]]; then
    {
      echo "# Baseline plan for $DB / $label (captured $(date -u '+%Y-%m-%dT%H:%M:%SZ'))"
      echo "# plan_sig at capture: $plan_sig"
      echo "# Query:"
      echo "$sql"
      echo "# ── EXPLAIN output ──"
      echo "$output"
    } > "$snapshot_file"
    printf '%s' "$plan_sig" > "$sig_state"
  elif [[ -n "$plan_sig" && "$plan_sig" != "ERR" && "$plan_sig" != "$prev_sig" ]]; then
    # Plan shape changed since the last cycle.  Save the new plan as
    # ${label}_drift_NNN_<sig>.txt (NNN = count of drift snapshots so
    # far).  Capped at SOAK_PERF_DRIFT_MAX_SNAPSHOTS (default 10) per
    # (db, probe) to bound disk usage when a probe flaps.
    local cap="${SOAK_PERF_DRIFT_MAX_SNAPSHOTS:-10}"
    local existing
    existing=$(find "$PLAN_DIR" -maxdepth 1 -name "${DB}_${label}_drift_*.txt" 2>/dev/null | wc -l | tr -d ' ')
    if (( existing < cap )); then
      local seq
      printf -v seq '%03d' "$((existing + 1))"
      local drift_file="$PLAN_DIR/${DB}_${label}_drift_${seq}_${plan_sig}.txt"
      {
        echo "# Drifted plan for $DB / $label (captured $(date -u '+%Y-%m-%dT%H:%M:%SZ'))"
        echo "# plan_sig: $plan_sig   (prev: $prev_sig)   drift_seq: $seq/$cap"
        echo "# Query:"
        echo "$sql"
        echo "# ── EXPLAIN output ──"
        echo "$output"
      } > "$drift_file"
    fi
    printf '%s' "$plan_sig" > "$sig_state"
  fi

  printf '%s,%s,%s,%s,%s,%s\n' \
    "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$DB" "$label" "$exec_ms" "$plan_ms" "$plan_sig" >> "$CSV"
}

# 1. Small window through cv_1min (BI point query)
probe "cv_1min_1h" \
  "SELECT count(*) FROM public.cv_1min WHERE bucket >= now() - INTERVAL '1 hour';"

# 2. Medium window through cv_1hour (operational report)
probe "cv_1hour_24h" \
  "SELECT count(*) FROM public.cv_1hour WHERE bucket >= now() - INTERVAL '24 hours';"

# 3. Control: same 24h window aggregated directly from source.
#    Establishes the cv_1hour-vs-cpu speedup floor each cycle.  If
#    cv_1hour_24h exec_ms >= source_24h exec_ms, the view degraded
#    to a source scan (scan-path regression signature).
probe "source_24h" \
  "SELECT count(*) FROM (
     SELECT time_series.time_bucket('1 hour'::interval, \"time\"), tags_id, count(*)
       FROM public.cpu
      WHERE \"time\" >= now() - INTERVAL '24 hours'
      GROUP BY 1, 2
   ) q;"

# 5. User-shape fetch probe — what a BI dashboard actually feels.
#    The 4 count(*) probes above prove the *scan* path is healthy.
#    This one adds: full row materialization + ORDER BY + LIMIT +
#    projection of 6 columns.  Server-side it exercises sort, limit,
#    and projection paths.  Catches plan-shape regressions on the
#    fetch side (e.g., sort spill to disk, motion fan-out blowup,
#    incorrect projection re-evaluation).
#
#    EXPLAIN ANALYZE executes the LIMIT 1000 fully but doesn't ship
#    rows to client.  So we see server-side fetch cost, just not the
#    wire-level serialization.  For scan-path-regression detection that's fine —
#    the regression manifests in plan/exec, not wire serialization.
probe "cv_1hour_24h_fetch" \
  "SELECT bucket, tags_id, cnt, avg_user, max_system, min_idle
     FROM public.cv_1hour
    WHERE bucket >= now() - INTERVAL '24 hours'
    ORDER BY bucket DESC, tags_id
    LIMIT 1000;"
