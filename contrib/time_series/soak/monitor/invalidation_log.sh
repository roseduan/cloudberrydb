#!/bin/bash
# monitor/invalidation_log.sh
# SOAK-LOOP: scope=per-db interval=60 interval_var=SOAK_INVALIDATION_LOG_EVERY order=42
#
# Watches the CAGG invalidation logs (L1 = cagg_invalidation_log,
# per source; L2 = cagg_materialization_log, per cagg).  Two signals
# that nothing else in the framework covers:
#
#   1. BLOAT — README §0's first-class soak rationale is "L1/L2
#      invalidation log bloat (row growth after thousands of
#      refreshes)".  Nobody was sampling log SIZE.  We track row
#      counts over the run and flag a sustained climb (first-Q vs
#      last-Q mean), same trend test as the RSS leak check.
#
#   2. DRAIN HEALTH — a healthy system drains L1 to ~0 shortly after
#      each refresh.  If L1 stays non-empty across many consecutive
#      samples, invalidations are NOT being processed.  That is a real
#      bug class, and it is dangerous precisely because view_check's
#      decidable-region gate would EXCLUDE the perpetually-dirty
#      buckets and report only "coverage thin" — the wrong diagnosis.
#      This monitor names it directly: a high non-empty fraction →
#      WARN, persistently stuck → fail.view_mismatch.
#
# The logs carry data-time ranges (lowest/greatest_modified), not a
# wall-clock insertion stamp, so "age of a pending entry" isn't
# readable from the table.  We approximate staleness by the fraction
# of consecutive samples L1 stayed non-empty — measured across our own
# sampling, which is exactly the drain-health question.
#
# CSV: ts, db, l1_rows, l2_rows, l1_newest_modified, l2_newest_modified
#
# Usage (invoked by lib/loops.sh):
#   invalidation_log.sh <db> <results_dir>        one sample

set -uo pipefail

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: invalidation_log.sh <db> <results_dir>}"
RESULTS="${2:?usage: invalidation_log.sh <db> <results_dir>}"
HOST="${SOAK_HOST:-localhost}"; PORT="${SOAK_PORT:-7000}"; USER="${SOAK_USER:-gpadmin}"
CSV="$RESULTS/data/invalidation_log.csv"
ERR="$RESULTS/errors/invalidation_log.err"
[[ -f "$CSV" ]] || echo "ts,db,l1_rows,l2_rows,l1_newest_modified,l2_newest_modified" > "$CSV"

TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
PGOPTIONS='-c optimizer=off --client-min-messages=warning' \
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -At -F',' -X -v ON_ERROR_STOP=1 -c "
SELECT '$TS', '$DB',
       (SELECT count(*) FROM time_series.cagg_invalidation_log),
       (SELECT count(*) FROM time_series.cagg_materialization_log),
       COALESCE((SELECT max(greatest_modified)::text FROM time_series.cagg_invalidation_log), ''),
       COALESCE((SELECT max(greatest_modified)::text FROM time_series.cagg_materialization_log), '');
" 2>>"$ERR" >> "$CSV"
