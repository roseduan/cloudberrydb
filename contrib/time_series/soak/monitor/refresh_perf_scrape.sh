#!/bin/bash
# monitor/refresh_perf_scrape.sh
# SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_PERF_PROBE_EVERY order=30
#
# Incremental scrape of time_series.job_history for refresh runs.
# Reads new rows (id > $cursor) WHERE proc_name='policy_refresh_cagg' and
# appends one CSV line per row to refresh_durations.csv. Cursor file
# stores the max id seen so far per DB to avoid double-counting.
#
# This is a PASSIVE probe: no new query is issued against the source
# table; we only read the job_history view that BGW workers
# already populate as a side effect of running.  Cost per cycle:
# < 10 ms (one bounded scan + one max() query).
#
# Typical row volume per 5-min cycle (recommended preset, 2 DB, 3 CAGGs):
#   cv_1min refresh fires 5×/5min × 3 CAGGs × 2 DBs ≈ 30 rows
#   plus cv_5min/cv_1hour at lower frequency ≈ 3 rows
#   total ~33 rows/cycle/DB → easily handled by a single SELECT.
#
# Usage (invoked by lib/loops.sh):
#   refresh_perf_scrape.sh <db> <results_dir>        one scrape pass

set -uo pipefail

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?db required}"
RESULTS="${2:?results dir required}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"

CSV="$RESULTS/data/refresh_durations.csv"
CURSOR_FILE="$RESULTS/state/cursors/refresh_cursor_${DB}"
ERR="$RESULTS/errors/refresh_perf_scrape.err"

# Initialize CSV header on first run (idempotent).
if [[ ! -f "$CSV" ]]; then
  echo "ts,db,id,job_id,cagg_name,execution_start,duration_ms,succeeded,is_crashed,error" > "$CSV"
fi

# Load cursor (0 = scrape everything on first run).
CURSOR="$(cat "$CURSOR_FILE" 2>/dev/null || echo 0)"
TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# Step 1: extract new rows, pipe to CSV (one row per refresh run).
# We use AT for unaligned, F ',' for field separator, t for tuples-only.
# is_crashed rows (execution_finish=NULL) have duration_ms = -1 marker.
PGOPTIONS='--client-min-messages=warning' \
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
     -At -F ',' -X -v ON_ERROR_STOP=1 \
     -c "
SELECT '$TS',
       '$DB',
       id,
       job_id,
       COALESCE(config->>'cagg_name',
                config->>'mat_table_name',
                '?'),
       execution_start::text,
       CASE WHEN execution_finish IS NULL
            THEN -1
            ELSE (EXTRACT(epoch FROM duration) * 1000)::bigint
       END,
       COALESCE(succeeded, false),
       is_crashed,
       regexp_replace(COALESCE(error_data->>'message', ''), ',', ';', 'g')
  FROM time_series.job_history
 WHERE proc_name = 'policy_refresh_cagg'
   AND id > $CURSOR
 ORDER BY id;
" 2>>"$ERR" >> "$CSV"

# Step 2: update cursor to the highest id seen so far.
NEW_CURSOR=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
       -At -X -v ON_ERROR_STOP=1 \
       -c "SELECT COALESCE(MAX(id), $CURSOR)::text
             FROM time_series.job_history
            WHERE proc_name = 'policy_refresh_cagg';" \
       2>>"$ERR")
if [[ -n "$NEW_CURSOR" ]] && [[ "$NEW_CURSOR" =~ ^[0-9]+$ ]]; then
  echo "$NEW_CURSOR" > "$CURSOR_FILE"
fi
