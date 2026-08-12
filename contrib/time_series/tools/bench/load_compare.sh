#!/bin/bash
# Compare pure INSERT throughput: time_series vs plain heap, same schema.
# Uses TSBS's tsbs_load_cb tool with --use-hypertable={true,false}.
#
# Inputs (override via env):
#   TSBS_LOAD_BIN   path to compiled tsbs_load_cb binary
#   TSBS_DATA_GZ    path to gzipped TSBS data file (cb_<scale>.gz)
#   PGPORT          target cluster port (default 7000)
#   PGUSER          target user        (default gpadmin)
#   WORKERS         parallel workers   (default 4)
#   BATCH           per-batch row count (default 1000)
#   CHUNK_TIME      time_series chunk interval (default 8h)
#
# Output: two passes (heap, time_series), each reports elapsed,
# rows loaded, throughput, table size.

set -e

TSBS_LOAD_BIN="${TSBS_LOAD_BIN:-/tmp/tsbs_load_cb}"
TSBS_DATA_GZ="${TSBS_DATA_GZ:-/tmp/cb_2000.gz}"
PGPORT="${PGPORT:-7000}"
PGUSER="${PGUSER:-gpadmin}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-1000}"
CHUNK_TIME="${CHUNK_TIME:-8h}"

[ -x "$TSBS_LOAD_BIN" ] || { echo "ERROR: TSBS_LOAD_BIN not executable: $TSBS_LOAD_BIN" >&2; exit 1; }
[ -f "$TSBS_DATA_GZ" ]  || { echo "ERROR: TSBS_DATA_GZ not found: $TSBS_DATA_GZ" >&2; exit 1; }

run_load() {
    local label="$1"
    local use_ht="$2"
    local db="tsbs_${label}"

    PGPORT="$PGPORT" psql -d postgres -c "DROP DATABASE IF EXISTS $db;" >/dev/null 2>&1
    PGPORT="$PGPORT" psql -d postgres -c "CREATE DATABASE $db;" >/dev/null
    PGPORT="$PGPORT" psql -d "$db" -c "CREATE EXTENSION time_series;" >/dev/null

    echo "============================================================"
    echo "  Loading dataset into ${label} (use-hypertable=${use_ht})"
    echo "============================================================"
    local start=$(date +%s)
    cat "$TSBS_DATA_GZ" | gunzip | "$TSBS_LOAD_BIN" load cloudberry_ts \
        --postgres='sslmode=disable' --user="$PGUSER" --pass='' \
        --admin-db-name="$db" --db-name="$db" \
        --host=localhost --port="$PGPORT" \
        --field-index='' --time-index=false \
        --time-partition-index=false --partition-index=false \
        --workers="$WORKERS" --batch-size="$BATCH" --chunk-time="$CHUNK_TIME" \
        --use-hypertable="$use_ht" --do-create-db=false --hash-workers \
        2>&1 | tail -4
    local end=$(date +%s)
    local elapsed=$((end - start))

    local rows=$(PGPORT="$PGPORT" psql -d "$db" -At -c "SELECT count(*) FROM cpu" 2>/dev/null)
    local rate=$(( rows / elapsed ))
    local cpu_size=$(PGPORT="$PGPORT" psql -d "$db" -At -c "SELECT pg_size_pretty(pg_total_relation_size('cpu'))" 2>/dev/null)

    printf "  Elapsed: %d s\n" "$elapsed"
    printf "  Rows:    %s\n"   "$rows"
    printf "  Rate:    %s rows/s\n" "$rate"
    printf "  Size:    %s\n"   "$cpu_size"
    echo
}

run_load heap         false
run_load timeseries   true
