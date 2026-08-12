#!/bin/bash
# workload/late_arrival.sh
# SOAK-LOOP: scope=per-db interval=3600 interval_var=SOAK_LATE_ARRIVAL_EVERY order=55
#
# Inserts rows into ALREADY-MATERIALIZED history → exercises CAGG's
# hardest path: invalidation-log entry → BGW re-materialize → mat row
# REPLACED → view returns the corrected aggregate.  Pure-append workload
# never touches this path (invalidation log stays empty); this loop is
# the only thing that does.
#
# ── Multi-mode (the late-arrival spectrum) ──────────────────────────
# Real late data is not one fixed shape.  Each cycle rotates one of
# three modes so over a run we cover the whole spectrum:
#
#   near  — small jitter straddling the watermark (now-300s..now-30s).
#           The MOST sensitive case: part lands above the watermark
#           (live, no invalidation) and part below (mat → invalidation),
#           directly stressing watermark advance + the threshold clamp —
#           the exact mechanism behind the 2026-06 freeze.
#   mid   — the original now-1h batch (a settled mat bucket).  Kept so
#           the classic single-bucket re-materialize stays covered.
#   bulk  — large backfill scattered across HOURS (now-5h..now-3h),
#           thousands of rows over many buckets: a device reconnecting
#           and dumping hours of buffered data → a wide invalidation
#           range the refresh must chew through.
#
# Rows scatter via random() inside the window (genuine out-of-order),
# spanning many buckets — unlike the old single-1min-bucket insert.
#
# Sizing keeps late rows a negligible fraction of forward workload; bulk
# is the heaviest but still << the ~1000 rows/s insert stream.
#
# Usage (invoked by lib/loops.sh):
#   late_arrival.sh <db> <results_dir>          one insert batch
#   late_arrival.sh --report <results_dir>      print report section

set -uo pipefail

# ── Report mode ──────────────────────────────────────────────────────
# CSV schema v2: ts,db,mode,inserted,target_window,window_start,window_end,duration_ms,rc
if [[ "${1:-}" == "--report" ]]; then
  RESULTS="${2:?usage: late_arrival.sh --report <results_dir>}"
  CSV="$RESULTS/data/late_arrival.csv"
  echo "── late arrival (invalidation → re-materialize coverage) ──"
  if [[ -s "$CSV" ]]; then
    awk -F, '
      NR>1 {
        n[$2]++; rows[$2]+=$4+0
        if ($9 != 0) fail[$2]++
        mk=$2 ";" $3; mc[mk]++; mr[mk]+=$4+0
      }
      END {
        for (db in n)
          printf "  late_arrival[%s] cycles=%d inserted=%d fail=%d\n",
                 db, n[db], rows[db], fail[db]+0
        for (k in mc) { split(k,a,";");
          printf "    %-14s %-5s cycles=%d inserted=%d\n", a[1], a[2], mc[k], mr[k] }
      }
    ' "$CSV" | sort
  else
    echo "  no samples (interval longer than run, or loop disabled)"
  fi
  exit 0
fi

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: late_arrival.sh <db> <results_dir>}"
RESULTS="${2:?usage: late_arrival.sh <db> <results_dir>}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"
SCALE="${SOAK_WORKLOAD_SCALE:-400}"

CSV="$RESULTS/data/late_arrival.csv"
ERR="$RESULTS/errors/late_arrival.err"
CYCLE_FILE="$RESULTS/state/cursors/late_cycle_${DB}"

# Header (once) — v2 schema with the mode column.
[[ -f "$CSV" ]] || echo "ts,db,mode,inserted,target_window,window_start,window_end,duration_ms,rc" > "$CSV"

# Rotate mode deterministically per DB so coverage stays balanced.
CYCLE=$(( $(cat "$CYCLE_FILE" 2>/dev/null || echo 0) + 1 ))
echo "$CYCLE" > "$CYCLE_FILE"
case $(( CYCLE % 3 )) in
  1) MODE=near;  OFFSET="${SOAK_LATE_NEAR_OFFSET:-30}";    SPAN="${SOAK_LATE_NEAR_SPAN:-270}";   ROWS="${SOAK_LATE_NEAR_ROWS:-40}" ;;
  2) MODE=mid;   OFFSET="${SOAK_LATE_OFFSET_SEC:-3600}";   SPAN="${SOAK_LATE_MID_SPAN:-60}";     ROWS="${SOAK_LATE_BATCH_ROWS:-60}" ;;
  0) MODE=bulk;  OFFSET="${SOAK_LATE_BULK_OFFSET:-10800}"; SPAN="${SOAK_LATE_BULK_SPAN:-7200}";  ROWS="${SOAK_LATE_BULK_ROWS:-2000}" ;;
esac

TS_NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# Scatter ROWS rows uniformly across [now-OFFSET-SPAN, now-OFFSET) via
# random() — genuine out-of-order arrival spanning multiple buckets.
# Integer tunables are inlined (psql :var is a pre-parse text replace
# that can't be multiplied by INTERVAL); all are values we control.
T0=$(date +%s%3N)
OUT=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
       -At -F '|' -X -v ON_ERROR_STOP=1 \
       -c "
WITH win AS (
  SELECT now() - INTERVAL '${OFFSET} seconds'              AS w_end,
         now() - INTERVAL '$(( OFFSET + SPAN )) seconds'    AS w_start
),
ins AS (
INSERT INTO public.cpu (
  time, tags_id,
  usage_user, usage_system, usage_idle, usage_nice, usage_iowait,
  usage_irq,  usage_softirq, usage_steal, usage_guest, usage_guest_nice
)
SELECT
  (SELECT w_end FROM win) - (random() * INTERVAL '${SPAN} seconds') AS time,
  (floor(random() * ${SCALE})::int + 1) AS tags_id,
  random()*100, random()*100, random()*100, random()*100, random()*100,
  random()*100, random()*100, random()*100, random()*100, random()*100
FROM generate_series(1, ${ROWS}) s
RETURNING 1
)
SELECT count(*),
       to_char((SELECT w_start FROM win) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
       to_char((SELECT w_end   FROM win) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
  FROM ins;
" 2>>"$ERR") || RC=$?
RC=${RC:-0}
T1=$(date +%s%3N)
DUR_MS=$(( T1 - T0 ))

# OUT: inserted|window_start|window_end
IFS='|' read -r INSERTED WIN_START WIN_END <<< "$OUT"
INSERTED="${INSERTED:-0}"
WIN_START="${WIN_START:-}"
WIN_END="${WIN_END:-}"
# ';' inner separator — outer CSV is comma-separated.
WIN_LABEL="now-${OFFSET}s;span${SPAN}s"

printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "$TS_NOW" "$DB" "$MODE" "$INSERTED" "$WIN_LABEL" "$WIN_START" "$WIN_END" "$DUR_MS" "$RC" >> "$CSV"
