#!/bin/bash
# stress.sh — Large-data INSERT / COPY stress test for time_series.
#
# Designed to be re-run after every feature / refactor to make sure
# concurrent writes still settle correctly (row counts match, no
# silently dropped rows, no orphan chunks, no error logs).
#
# Usage:
#   ./stress.sh --workers N {--rows-per-worker R | --data-size SIZE}
#               --mode {insert|copy} [opts]
#
# Required:
#   --workers N            Parallel writer count.
#   --mode MODE            insert  → INSERT...SELECT generate_series
#                          copy    → COPY FROM stdin (server-generated CSV)
#
# Volume — exactly one of:
#   --rows-per-worker R    Rows each worker writes.
#   --data-size SIZE       Target total on-disk size; rows derived from
#                          an estimated row width.  Supports K/M/G/T
#                          suffixes (1024-based).  Example: 10G, 100G, 1T.
#
# Schema:
#   --cols N               Extra `metric_N double precision` columns
#                          beyond the 3 base metrics (default 0).
#                          Use this to scale row width linearly.
#   --toast-bytes N        Length (bytes) of the per-row `big_text_val`
#                          column.  Default 4096; PG TOAST threshold is
#                          ~2032 B, so values above that go to the TOAST
#                          table (exercises the toaster path).  Pass 0
#                          to drop the column entirely.
#   --tables N             Number of identical tables to write to in
#                          parallel (default 1).  Each table is named
#                          <table>_<i> when N > 1, and gets its own set
#                          of --workers writers.  Total parallel writers
#                          = workers * tables; total volume scales the
#                          same way (each table holds --data-size bytes).
#
# Optional:
#   --port PORT            PG port (default $PGPORT or 7000)
#   --db DBNAME            Target database (default ts_stress)
#   --table TBL            Table name (default ts_stress)
#   --chunk-interval INT   ts_chunk_interval reloption (default '1 hour')
#   --drop                 Drop the target database after the run.
#                          (Default: keep the database so you can
#                          inspect it / re-run queries against it.)
#
# Examples:
#   # 1.6 M rows split across 8 workers, INSERT...SELECT path
#   ./stress.sh --workers 8 --rows-per-worker 200000 --mode insert
#
#   # ~10 GB on-disk, 16 workers, COPY path
#   ./stress.sh --workers 16 --data-size 10G --mode copy
#
#   # ~100 GB with 20 extra metric columns (wide row → TOAST stress)
#   ./stress.sh --workers 32 --data-size 100G --cols 20 --mode insert \
#               --chunk-interval '6 hour' --keep
#
# Exit status: 0 if total row count matches expected and zero errors,
# non-zero otherwise.  Use in CI loops for regression detection.

set -u

WORKERS=""
ROWS=""
DATA_SIZE=""
MODE=""
COLS=0
TOAST_BYTES=4096
TABLES=1
PORT="${PGPORT:-7000}"
DB="ts_stress"
TBL="ts_stress"
CHUNK_INTERVAL="1 hour"
DROP=false

usage() { sed -n '/^# Usage/,/^# Exit status/p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

while [ $# -gt 0 ]; do
    case "$1" in
        --workers)         WORKERS="$2"; shift 2 ;;
        --rows-per-worker) ROWS="$2";    shift 2 ;;
        --data-size)       DATA_SIZE="$2"; shift 2 ;;
        --cols)            COLS="$2";    shift 2 ;;
        --toast-bytes)     TOAST_BYTES="$2"; shift 2 ;;
        --tables)          TABLES="$2";  shift 2 ;;
        --mode)            MODE="$2";    shift 2 ;;
        --port)            PORT="$2";    shift 2 ;;
        --db)              DB="$2";      shift 2 ;;
        --table)           TBL="$2";     shift 2 ;;
        --chunk-interval)  CHUNK_INTERVAL="$2"; shift 2 ;;
        --drop)            DROP=true;    shift   ;;
        --keep)            DROP=false;   shift   ;;	# kept for back-compat; default is keep
        -h|--help)         usage ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

[ -z "$WORKERS" ] || [ -z "$MODE" ] && usage
case "$MODE" in insert|copy) ;; *) echo "--mode must be insert or copy" >&2; usage ;; esac
[ "$TABLES" -ge 1 ] 2>/dev/null || { echo "--tables must be a positive integer" >&2; usage; }

# Exactly one of --rows-per-worker / --data-size must be set.
if [ -n "$ROWS" ] && [ -n "$DATA_SIZE" ]; then
    echo "--rows-per-worker and --data-size are mutually exclusive" >&2
    usage
fi
if [ -z "$ROWS" ] && [ -z "$DATA_SIZE" ]; then
    echo "either --rows-per-worker or --data-size required" >&2
    usage
fi

# Parse a size string like "10G", "100M", "1T" into raw bytes (1024-based).
parse_size() {
    local s="$1"
    awk -v s="$s" '
    BEGIN {
        if (match(s, /^[0-9]+(\.[0-9]+)?$/)) { print s + 0; exit }
        if (match(s, /^([0-9]+(\.[0-9]+)?)([KMGTkmgt])$/, m)) {
            n = m[1] + 0
            switch (m[3]) {
                case /[Kk]/: printf "%.0f\n", n * 1024;                       break
                case /[Mm]/: printf "%.0f\n", n * 1024 * 1024;                break
                case /[Gg]/: printf "%.0f\n", n * 1024 * 1024 * 1024;         break
                case /[Tt]/: printf "%.0f\n", n * 1024 * 1024 * 1024 * 1024;  break
            }
            exit
        }
        print "bad-size"
    }'
}

# Rough on-disk bytes per row for the diverse-types base schema below.
# Fixed part summary:
#   header + line ptr ≈ 28
#   ts/worker/seq                            ≈ 24
#   smallint+real+3 doubles+numeric          ≈ 44
#   bool                                     ≈   2
#   char(16) + varchar(64=md5) + text(md5)   ≈  82
#   bytea(16-byte sha)                       ≈  20
#   jsonb (small)                            ≈  60
#   inet (16)                                ≈  16
#   uuid (16)                                ≈  16
#   date (4) + interval (16)                 ≈  20
#   int_arr[4]                               ≈  28
# Total ≈ 340 b/row physical for the inline part.
# Variable parts:
#   8 b  per extra double-precision metric_<N> column
#   TOAST_BYTES per row from big_text_val (lives in the TOAST table,
#     but counts toward the table's total relation size; TOAST is
#     attempted-compress first, so this is an upper bound — md5 hex
#     barely compresses so the bound is tight).
EST_ROW_BYTES=$((340 + 8 * COLS + TOAST_BYTES))

if [ -n "$DATA_SIZE" ]; then
    BYTES=$(parse_size "$DATA_SIZE")
    if [ "$BYTES" = "bad-size" ]; then
        echo "bad --data-size: $DATA_SIZE (use 10K/100M/1G/1T)" >&2
        exit 2
    fi
    # Each worker gets equal share.
    TOTAL_ROWS=$(awk -v b="$BYTES" -v r="$EST_ROW_BYTES" \
                     'BEGIN { printf "%d", b / r }')
    ROWS=$(awk -v t="$TOTAL_ROWS" -v w="$WORKERS" \
               'BEGIN { printf "%d", (t + w - 1) / w }')
fi

EXPECTED_PER_TABLE=$((WORKERS * ROWS))
EXPECTED=$((EXPECTED_PER_TABLE * TABLES))
HUMAN_SIZE_PER_TABLE=$(awk -v b="$((EXPECTED_PER_TABLE * EST_ROW_BYTES))" 'BEGIN {
    units[0]="B"; units[1]="KB"; units[2]="MB"; units[3]="GB"; units[4]="TB"
    i = 0; while (b >= 1024 && i < 4) { b /= 1024; i++ }
    printf "%.2f %s", b, units[i]
}')
HUMAN_SIZE=$(awk -v b="$((EXPECTED * EST_ROW_BYTES))" 'BEGIN {
    units[0]="B"; units[1]="KB"; units[2]="MB"; units[3]="GB"; units[4]="TB"
    i = 0; while (b >= 1024 && i < 4) { b /= 1024; i++ }
    printf "%.2f %s", b, units[i]
}')

# Resolve the i-th table name (1-based).  Single-table runs keep the
# bare $TBL name so backwards compatibility / existing data is preserved.
table_name_for() {
    if [ "$TABLES" -eq 1 ]; then
        printf '%s' "$TBL"
    else
        printf '%s_%d' "$TBL" "$1"
    fi
}

PSQL() { PGPORT="$PORT" psql -X -v ON_ERROR_STOP=1 "$@"; }
PSQL_NOSTOP() { PGPORT="$PORT" psql -X "$@"; }

# Reference timestamp for all generators.  Workers offset from this
# by (i * 0.7 s) so each row gets a unique ts within its worker.
START_TS="2025-01-01 00:00:00"

# ---------------------------------------------------------------------
# Build the dynamic SQL pieces (column list, generator expressions)
# ---------------------------------------------------------------------
# Base table covers a representative cross-section of PG built-in types
# so the write path is exercised against every storage / serialisation
# code path (fixed-width, varlena, varlena-with-toast, network, array,
# composite-formatted UUID).  Extra `--cols N` columns are simple
# double-precision metrics that scale row width linearly.
BASE_COLS="ts, worker, seq, small_val, real_val, metric_a, metric_b, metric_c, \
numeric_val, bool_val, char_val, varchar_val, text_val, bytea_val, \
json_val, inet_val, uuid_val, date_val, interval_val, int_arr"

BASE_TYPES="    ts           timestamptz NOT NULL,
    worker       int,
    seq          bigint,
    small_val    smallint,
    real_val     real,
    metric_a     double precision,
    metric_b     double precision,
    metric_c     double precision,
    numeric_val  numeric(12, 4),
    bool_val     boolean,
    char_val     char(16),
    varchar_val  varchar(64),
    text_val     text,
    bytea_val    bytea,
    json_val     jsonb,
    inet_val     inet,
    uuid_val     uuid,
    date_val     date,
    interval_val interval,
    int_arr      int[]"

# Add big_text_val (TOAST-stress column) when TOAST_BYTES > 0.
# Each row's value is repeat(md5(<I>), N), so payload size = 32 * N bytes.
# md5 hex barely compresses, so almost all of it lands in the TOAST table.
if [ "$TOAST_BYTES" -gt 0 ]; then
    TOAST_REPEATS=$(( (TOAST_BYTES + 31) / 32 ))   # ceil(N / 32)
    BASE_COLS="${BASE_COLS}, big_text_val"
    BASE_TYPES="${BASE_TYPES},
    big_text_val text"
fi

# Generator expressions for the BASE columns.  Two placeholders:
#   <I>  → the row-index column reference (e.g. 'i' or 's')
#   <W>  → the worker number (literal int)
# Substituted per-mode below.
BASE_GEN_EXPRS="'$START_TS'::timestamptz + (<I> * interval '0.7 seconds'),
       <W>,
       <I>,
       (<I> % 32000)::smallint,
       random()::real,
       random() * 100,
       random() * 50,
       random(),
       round((random() * 1000)::numeric, 4),
       (<I> % 2 = 0),
       substr(md5(<I>::text), 1, 16)::char(16),
       md5(<I>::text || ':v')::varchar(64),
       md5(<I>::text || ':' || <W>),
       decode(md5(<I>::text), 'hex'),
       jsonb_build_object('id', <I>, 'w', <W>, 'v', random()),
       ('10.' || ((<I> / 65536) % 256) || '.' || ((<I> / 256) % 256) || '.' || (<I> % 256))::inet,
       ('00000000-0000-0000-0000-' || lpad(to_hex(<I>), 12, '0'))::uuid,
       ('2025-01-01'::date + (<I> % 365)),
       make_interval(secs => (<I> % 3600)::int),
       ARRAY[<I>, <I>+1, <I>+2, (<I>*7) % 1000]::int[]"

# Append the TOAST-stress expression when the column is enabled.
if [ "$TOAST_BYTES" -gt 0 ]; then
    BASE_GEN_EXPRS="${BASE_GEN_EXPRS},
       repeat(md5(<I>::text), ${TOAST_REPEATS})"
fi

EXTRA_COL_LIST=""
EXTRA_COL_DEFS=""
EXTRA_GEN_EXPRS=""
for ((j = 1; j <= COLS; j++)); do
    EXTRA_COL_LIST="${EXTRA_COL_LIST}, metric_$j"
    EXTRA_COL_DEFS="${EXTRA_COL_DEFS},
    metric_$j     double precision"
    EXTRA_GEN_EXPRS="${EXTRA_GEN_EXPRS},
       random()"
done

COL_LIST_FULL="${BASE_COLS}${EXTRA_COL_LIST}"

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------
echo "=========================================================="
echo " time_series stress"
echo "   mode=$MODE workers=$WORKERS  tables=$TABLES (parallel writers total=$((WORKERS * TABLES)))"
if [ -n "$DATA_SIZE" ]; then
    echo "   target=$DATA_SIZE/table  (est. row width ${EST_ROW_BYTES} B; per-table ≈ ${HUMAN_SIZE_PER_TABLE}; grand total ≈ ${HUMAN_SIZE})"
fi
echo "   rows/worker=$ROWS  per-table=$EXPECTED_PER_TABLE  total expected=$EXPECTED  cols=base+${COLS} extra"
echo "   toast-bytes=$TOAST_BYTES  chunk_interval=$CHUNK_INTERVAL"
echo "=========================================================="

PSQL -d postgres -c "DROP DATABASE IF EXISTS $DB;" >/dev/null 2>&1
PSQL -d postgres -c "CREATE DATABASE $DB;" >/dev/null
PSQL -d "$DB"    -c "CREATE EXTENSION time_series;" >/dev/null
for t in $(seq 1 "$TABLES"); do
    this_tbl=$(table_name_for "$t")
    PSQL -d "$DB" -c "
CREATE TABLE $this_tbl (
${BASE_TYPES}${EXTRA_COL_DEFS}
) USING time_series
  WITH (ts_partition_column='ts',
        ts_chunk_interval='$CHUNK_INTERVAL',
        ts_chunk_origin='2025-01-01')
  DISTRIBUTED BY (worker);
" >/dev/null
done

# ---------------------------------------------------------------------
# Worker bodies
# ---------------------------------------------------------------------

# Substitute placeholders in BASE_GEN_EXPRS.
#   $1 = row-index column ('i' for INSERT, 's' for COPY)
#   $2 = worker number (literal int)
expand_gen_exprs() {
    local idx="$1"
    local w="$2"
    local g="${BASE_GEN_EXPRS//<I>/$idx}"
    g="${g//<W>/$w}"
    printf '%s' "$g"
}

run_insert_worker() {
    local w="$1"
    local tbl="$2"
    local gen
    gen=$(expand_gen_exprs "i" "$w")
    PSQL -d "$DB" -c "
INSERT INTO $tbl (${COL_LIST_FULL})
SELECT ${gen}${EXTRA_GEN_EXPRS}
FROM generate_series(1, $ROWS) i;
" 2>&1 | tail -1
}

run_copy_worker() {
    local w="$1"
    local tbl="$2"
    local gen
    gen=$(expand_gen_exprs "s" "$w")
    # Pipeline: one psql session generates CSV (server-side
    # generate_series → COPY TO STDOUT, with PG-native ISO timestamps);
    # a second psql session COPY FROM STDIN into the time_series table.
    # Both ends use the COPY wire protocol, so this exercises the
    # multi_insert path through the CopyMultiInsertBuffer code path.
    PSQL -d "$DB" -c "
        COPY (
            SELECT ${gen}${EXTRA_GEN_EXPRS}
            FROM generate_series(1, $ROWS) s
        ) TO STDOUT WITH (FORMAT csv);
    " 2>/dev/null \
    | PSQL -d "$DB" -c "
        COPY $tbl (${COL_LIST_FULL})
        FROM STDIN WITH (FORMAT csv);
    " 2>&1 | tail -1
}

# ---------------------------------------------------------------------
# Run all workers in parallel
# ---------------------------------------------------------------------
start_ts=$(date +%s.%N)
pids=()
for t in $(seq 1 "$TABLES"); do
    this_tbl=$(table_name_for "$t")
    for w in $(seq 1 "$WORKERS"); do
        if [ "$MODE" = insert ]; then
            run_insert_worker "$w" "$this_tbl" &
        else
            run_copy_worker "$w" "$this_tbl" &
        fi
        pids+=($!)
    done
done

failed=0
for pid in "${pids[@]}"; do
    wait "$pid" || failed=$((failed + 1))
done
end_ts=$(date +%s.%N)
elapsed=$(awk -v a="$start_ts" -v b="$end_ts" 'BEGIN { printf "%.3f", b - a }')

# ---------------------------------------------------------------------
# Verify (per-table row count, then aggregate)
# ---------------------------------------------------------------------
total_actual=0
total_chunks=0
ok=true
[ "$failed" -eq 0 ] || ok=false

echo
echo "----------------------------------------------------------"
for t in $(seq 1 "$TABLES"); do
    this_tbl=$(table_name_for "$t")
    actual=$(PSQL_NOSTOP -d "$DB" -At -c "SELECT count(*) FROM $this_tbl;" 2>/dev/null)
    size=$(PSQL_NOSTOP -d "$DB" -At -c "SELECT pg_size_pretty(pg_total_relation_size('$this_tbl'));" 2>/dev/null)
    chunks=$(PSQL_NOSTOP -d "$DB" -At -c "SELECT count(*) FROM time_series.ts_chunk WHERE table_oid='$this_tbl'::regclass;" 2>/dev/null)
    [ "$actual" = "$EXPECTED_PER_TABLE" ] || ok=false
    total_actual=$((total_actual + actual))
    total_chunks=$((total_chunks + chunks))
    if [ "$TABLES" -gt 1 ]; then
        printf " %-20s rows=%s size=%s chunks=%s\n" "$this_tbl" "$actual" "$size" "$chunks"
    fi
done

rate=$(awk -v r="$total_actual" -v t="$elapsed" 'BEGIN {
    if (t+0 <= 0) { print 0 } else { printf "%.0f", r / t }
}')

printf " elapsed:        %.2f s\n" "$elapsed"
printf " expected rows:  %s  (per-table=%s × %d tables)\n" "$EXPECTED" "$EXPECTED_PER_TABLE" "$TABLES"
printf " actual rows:    %s\n"     "$total_actual"
printf " worker errors:  %d\n"     "$failed"
printf " throughput:     %s rows/s\n" "$rate"
if [ "$TABLES" -eq 1 ]; then
    printf " table size:     %s\n" "$size"
fi
printf " chunk rows (per-segment aggregate, all tables): %s\n" "$total_chunks"
printf " result:         %s\n" "$($ok && echo PASS || echo FAIL)"
echo "----------------------------------------------------------"

if [ "$DROP" = true ]; then
    PSQL_NOSTOP -d postgres -c "DROP DATABASE $DB;" >/dev/null 2>&1
else
    echo " data kept in database '$DB' (port $PORT); pass --drop to clean up"
fi

$ok && exit 0 || exit 1
