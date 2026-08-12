#!/bin/bash
# monitor/compress_perf_scrape.sh
# SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_PERF_PROBE_EVERY order=35
#
# Incremental scrape of time_series.job_history for compression-policy
# runs.  Same pattern as refresh_perf_scrape.sh but filtered to
# proc_name='policy_compression', and includes two extra columns
# (chunks_compressed, compressed_bytes) joined from ts_compressed_chunk.
#
# Compression runs are infrequent compared to refresh: at large
# profile (schedule=6h) we see ~28 rows in 7 days.  Even with a
# 12h schedule that's still a sparse stream — but value-per-row is
# high (each compress is multi-second and reclaims tens of MB).
#
# Usage (invoked by lib/loops.sh):
#   compress_perf_scrape.sh <db> <results_dir>       one scrape pass

set -uo pipefail

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?db required}"
RESULTS="${2:?results dir required}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"

CSV="$RESULTS/data/compress_durations.csv"
CURSOR_FILE="$RESULTS/state/cursors/compress_cursor_${DB}"
ERR="$RESULTS/errors/compress_perf_scrape.err"

# Initialize CSV header on first run.
#
# 2026-06: renamed bytes_reclaimed → compressed_bytes.  ts_compressed_chunk
# only populates compressed_size; uncompressed_size is always NULL today,
# so (uncompressed - compressed) cannot be computed from catalog alone.
# compressed_bytes is what actually landed in PAX — a real, observable
# signal (and a useful sanity bound: compress flat-lining would show as
# this column going to zero across consecutive runs).
if [[ ! -f "$CSV" ]]; then
  echo "ts,db,id,job_id,table_name,execution_start,duration_ms,succeeded,is_crashed,chunks_compressed,compressed_bytes,error" > "$CSV"
fi

CURSOR="$(cat "$CURSOR_FILE" 2>/dev/null || echo 0)"
TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# Per-job chunks_compressed / compressed_bytes come from a correlated
# subquery against time_series.ts_compressed_chunk, scoped to rows whose
# compressed_at falls inside this job's [execution_start, execution_finish]
# window.  compressed_bytes = sum(compressed_size) (= the PAX file bytes
# that landed during this job; see CSV-header comment above for why we
# don't try to compute heap-bytes-reclaimed).
#
# 2026-06 fix: previous version read these from data->'result'->'chunks_*',
# but policy_compression never populates those keys, so 42 consecutive
# rows in round 3 showed chunks=0 / bytes=0 despite 42 successful runs.
# The ts_compressed_chunk table is the source of truth — it always gets a
# new row per compressed chunk, with the post-rename compressed_at stamp.
#
# Query bgw_job_stat_history directly (not the job_history view) — the
# view enforces an owner filter that's irrelevant here (soak always runs
# as the BGW jobs' owner) and hides the JSONB we still need for proc_name.
PGOPTIONS='--client-min-messages=warning' \
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
     -At -F ',' -X -v ON_ERROR_STOP=1 \
     -c "
-- Only emit rows where the compression policy actually compressed at
-- least one chunk (chunks_compressed>0) OR crashed/failed.  BGW fires
-- the compression policy on its schedule every N minutes regardless
-- of whether any chunk is past compress_after; without this filter
-- ~47% of the rows in the 12h run had chunks=0/bytes=0 and just
-- polluted the histogram (real signal per row was diluted).  Crashed
-- and failed rows are kept so is_crashed/succeeded=false is still
-- observable.  The cursor still advances via the follow-up MAX(id)
-- query, so skipped 0-chunk rows will never be re-emitted.
WITH j AS (
  SELECT j.id, j.job_id, j.data, j.execution_start, j.execution_finish,
         j.succeeded,
         (j.execution_finish IS NULL AND j.execution_start IS NOT NULL)
           AS is_crashed,
         COALESCE((
           SELECT count(*)::int FROM time_series.ts_compressed_chunk cc
            WHERE cc.compressed_at >= j.execution_start
              AND cc.compressed_at <= COALESCE(j.execution_finish, NOW())
         ), 0) AS chunks_compressed,
         COALESCE((
           SELECT sum(cc.compressed_size)::bigint
             FROM time_series.ts_compressed_chunk cc
            WHERE cc.compressed_at >= j.execution_start
              AND cc.compressed_at <= COALESCE(j.execution_finish, NOW())
         ), 0) AS compressed_bytes
    FROM time_series.bgw_job_stat_history j
   WHERE (j.data->'job'->>'proc_name')::text = 'policy_compression'
     AND j.id > $CURSOR
)
SELECT '$TS',
       '$DB',
       j.id,
       j.job_id,
       COALESCE(j.data->'job'->'config'->>'table_name',
                j.data->'job'->'config'->>'table_oid',
                '?'),
       j.execution_start::text,
       CASE WHEN j.execution_finish IS NULL
            THEN -1
            ELSE (EXTRACT(epoch FROM (j.execution_finish - j.execution_start)) * 1000)::bigint
       END,
       COALESCE(j.succeeded, false),
       j.is_crashed,
       j.chunks_compressed,
       j.compressed_bytes,
       regexp_replace(COALESCE(j.data->'error_data'->>'message', ''), ',', ';', 'g')
  FROM j
 WHERE j.chunks_compressed > 0
    OR j.is_crashed
    OR j.succeeded = false
 ORDER BY j.id;
" 2>>"$ERR" >> "$CSV"

NEW_CURSOR=$(PGOPTIONS='--client-min-messages=warning' \
  psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
       -At -X -v ON_ERROR_STOP=1 \
       -c "SELECT COALESCE(MAX(id), $CURSOR)::text
             FROM time_series.bgw_job_stat_history
            WHERE (data->'job'->>'proc_name')::text = 'policy_compression';" \
       2>>"$ERR")
if [[ -n "$NEW_CURSOR" ]] && [[ "$NEW_CURSOR" =~ ^[0-9]+$ ]]; then
  echo "$NEW_CURSOR" > "$CURSOR_FILE"
fi
