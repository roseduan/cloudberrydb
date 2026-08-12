#!/usr/bin/env bash
# Compare compression ratio and speed:
#   Cloudberry time_series (PAX)  vs  TimescaleDB hypertable (Gorilla / delta).
#
# Generates the same N rows on both clusters via generate_series (no TSBS
# data file dependency), with TSBS-cpu-like schema and slow-drift cosine
# values + small jitter — closer to real metrics than random doubles, so
# columnar codecs have something to chew on.
#
# Variables (override via env):
#   CB_PORT     Cloudberry port    (default 7000)
#   TSDB_PORT   TimescaleDB port   (default 15432)
#   CB_DB       CB database name   (default bench_compress_cb)
#   TSDB_DB     TSDB database name (default bench_compress_tsdb)
#   PGUSER                          (default gpadmin)
#   N_ROWS      number of rows     (default 1_000_000)
#   CHUNK_TIME  chunk interval     (default 8 hour)
#   ROW_DT_SEC  seconds between rows (default 2.88; smaller → denser data)
#   TSDB_TABLESPACE  optional tablespace to place TSDB cpu hypertable on
#                    (default empty = TSDB cluster's default location).  Use
#                    when TSDB's PGDATA is on a small disk: pre-create a
#                    tablespace on a larger mount and pass its name here.
#
# Output: a markdown table with rows, chunk count, ratio, compress wall-time.

set -e

CB_PORT="${CB_PORT:-7000}"
TSDB_PORT="${TSDB_PORT:-15432}"
CB_DB="${CB_DB:-bench_compress_cb}"
TSDB_DB="${TSDB_DB:-bench_compress_tsdb}"
PGUSER="${PGUSER:-gpadmin}"
N_ROWS="${N_ROWS:-1000000}"
CHUNK_TIME="${CHUNK_TIME:-8 hour}"
ROW_DT_SEC="${ROW_DT_SEC:-2.88}"
TSDB_TABLESPACE="${TSDB_TABLESPACE:-}"
# yes (default) → schema includes additional_tags jsonb column;
# no            → numeric-only schema (12 cols), like PAX README.encoding.md
INCLUDE_JSONB="${INCLUDE_JSONB:-yes}"

run_cb()   { PGPORT="$CB_PORT"   PGUSER="$PGUSER" psql -d "$1" -At "${@:2}" ; }
run_tsdb() { PGPORT="$TSDB_PORT" PGUSER="$PGUSER" psql -d "$1" -At "${@:2}" ; }

echo "=============================================================="
echo "  Compress comparison: CB time_series  vs  TimescaleDB"
echo "  N_ROWS=$N_ROWS  CHUNK_TIME=$CHUNK_TIME"
echo "=============================================================="

# ---------- setup ----------
echo "[1/6] Recreating databases ..."

# CB time_series leaves a BGW scheduler attached to every DB with the extension
# installed — DROP DATABASE blocks on it until the worker exits.  Mirror the
# Makefile's _ts_drop_stale_dbs pattern: stop_background_workers (no-op when
# the DB or extension is absent), terminate any leftover backends, then drop.
cb_drop_db() {
    local db="$1"
    PGPORT="$CB_PORT" psql -X -d "$db" -tAc "SELECT time_series.stop_background_workers()" \
        >/dev/null 2>&1 || true
    for _ in 1 2 3 4 5; do
        PGPORT="$CB_PORT" psql -X -d postgres -tAc \
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$db'" \
            >/dev/null 2>&1 || true
        out=$(PGPORT="$CB_PORT" psql -X -d postgres -c "DROP DATABASE IF EXISTS $db" 2>&1)
        echo "$out" | grep -qE 'DROP DATABASE|does not exist' && return 0
        sleep 1
    done
    echo "ERROR: could not drop CB database $db" >&2
    return 1
}

cb_drop_db "$CB_DB"
PGPORT="$CB_PORT"   psql -d postgres -c "CREATE DATABASE $CB_DB;"           >/dev/null
PGPORT="$CB_PORT"   psql -d "$CB_DB"  -c "CREATE EXTENSION time_series;"    >/dev/null

PGPORT="$TSDB_PORT" psql -d postgres -c "DROP DATABASE IF EXISTS $TSDB_DB;" >/dev/null 2>&1
PGPORT="$TSDB_PORT" psql -d postgres -c "CREATE DATABASE $TSDB_DB;"         >/dev/null
PGPORT="$TSDB_PORT" psql -d "$TSDB_DB" -c "CREATE EXTENSION timescaledb;"   >/dev/null 2>&1

# ---------- schema ----------
SCHEMA_COLS='
    "time"           timestamptz NOT NULL,
    tags_id          integer,
    usage_user       double precision,
    usage_system     double precision,
    usage_idle       double precision,
    usage_nice       double precision,
    usage_iowait     double precision,
    usage_irq        double precision,
    usage_softirq    double precision,
    usage_steal      double precision,
    usage_guest      double precision,
    usage_guest_nice double precision'

if [ "$INCLUDE_JSONB" = "yes" ]; then
    SCHEMA_COLS="$SCHEMA_COLS,
    additional_tags  jsonb"
    echo "  (schema includes additional_tags jsonb column)"
else
    echo "  (numeric-only schema, NO jsonb column)"
fi

echo "[2/6] Creating cpu table on CB (time_series) ..."
PGPORT="$CB_PORT" psql -d "$CB_DB" -v ON_ERROR_STOP=1 <<EOF >/dev/null
CREATE TABLE cpu ($SCHEMA_COLS)
USING time_series
WITH (ts_partition_column='time', ts_chunk_interval='$CHUNK_TIME', ts_chunk_origin='2025-01-01')
DISTRIBUTED BY (tags_id);
EOF

echo "[2/6] Creating cpu hypertable on TSDB ..."
# attach_tablespace tells TSDB to place new chunks on this tablespace.  The
# parent table itself stays in TSDB's default location (it's empty anyway —
# all data lives in chunks).  Don't put TABLESPACE on the parent CREATE
# TABLE: that triggers TSDB's "already attached" error since create_hypertable
# auto-attaches the parent's tablespace when one is set.
TSDB_ATTACH_TS=""
if [ -n "$TSDB_TABLESPACE" ]; then
    TSDB_ATTACH_TS="SELECT attach_tablespace('$TSDB_TABLESPACE', 'cpu');"
    echo "  (chunks routed to TSDB tablespace $TSDB_TABLESPACE)"
fi
PGPORT="$TSDB_PORT" psql -d "$TSDB_DB" -v ON_ERROR_STOP=1 <<EOF >/dev/null
CREATE TABLE cpu ($SCHEMA_COLS);
SELECT create_hypertable('cpu', 'time', chunk_time_interval => INTERVAL '$CHUNK_TIME');
$TSDB_ATTACH_TS
EOF

# ---------- data generation ----------
# TSBS-style cpu-only metrics:
#   - 100 hosts (tags_id 0..99)
#   - timestamp monotonically increasing ROW_DT_SEC per row
#   - usage_* values: **integer 0..100 cast to float8** so the mantissa
#     is clean (47.0 / 48.0 / ...) instead of full-entropy 64-bit doubles.
#     Matches what tsbs_generate_data actually emits: tsbs uses a clamped
#     random walk + ToPointAllInt64 (cmd/tsbs_generate_data/cpu.go), so
#     real-world TSBS data goes to storage as integers — and GORILLA /
#     DELTA_DELTA / ZSTD all hit their sweet spot on that.
#
#   - additional_tags: jsonb with limited key set (compresses well via dict)
#
#   Prior version of this script used `50 + 30*sin(i/5000) + 2*(random()-0.5)`
#   which produces full-entropy float8 mantissa on every row — the worst
#   case for every columnar codec.  See insert-bench-zh.md §9.x for the
#   bench numbers under that old data.
NUMERIC_COLS="
       greatest(0, least(100, floor(50 + 30 * sin(i::float8 / 5000.0) + (random() - 0.5))))::float8,
       greatest(0, least(100, floor(30 + 20 * cos(i::float8 / 5000.0) + (random() - 0.5))))::float8,
       greatest(0, least(100, floor(100 - (50 + 30 * sin(i::float8 / 5000.0)))))::float8,
       greatest(0, least(100, floor( 3 + 1 * (random() - 0.5))))::float8,
       greatest(0, least(100, floor( 5 + 1 * (random() - 0.5))))::float8,
       greatest(0, least(100, floor( 1 + 0.5 * random())))::float8,
       greatest(0, least(100, floor( 2 + 0.5 * random())))::float8,
       greatest(0, least(100, floor( 0 + 0.3 * random())))::float8,
       greatest(0, least(100, floor( 0 + 0.2 * random())))::float8,
       greatest(0, least(100, floor( 0 + 0.2 * random())))::float8"

JSONB_COL=""
if [ "$INCLUDE_JSONB" = "yes" ]; then
    JSONB_COL=",
       jsonb_build_object('host', 'host_' || (i % 100), 'rack', 'rack_' || ((i % 100) / 10))"
fi

GEN_SQL="
INSERT INTO cpu
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '$ROW_DT_SEC seconds') AS time,
       (i % 100) AS tags_id,
$NUMERIC_COLS$JSONB_COL
FROM generate_series(1, $N_ROWS) i;
"

echo "[3/6] Loading $N_ROWS rows into CB ..."
t1=$(date +%s.%N)
PGPORT="$CB_PORT" psql -d "$CB_DB" -v ON_ERROR_STOP=1 -c "$GEN_SQL" >/dev/null
t2=$(date +%s.%N)
cb_load_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.2f", b-a}')

echo "[3/6] Loading $N_ROWS rows into TSDB ..."
t1=$(date +%s.%N)
PGPORT="$TSDB_PORT" psql -d "$TSDB_DB" -v ON_ERROR_STOP=1 -c "$GEN_SQL" >/dev/null
t2=$(date +%s.%N)
tsdb_load_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.2f", b-a}')

# ---------- pre-compress sizes ----------
echo "[4/6] Measuring pre-compress sizes ..."

# Apples-to-apples: total relation size on both sides (includes TOAST + indexes).
# Using pg_total_relation_size('cpu') means we capture the same envelope: heap +
# toast + indexes + per-fork book-keeping.  For CB this rolls up all chunk forks
# under the relation's relfilenode.
cb_pre_bytes=$(run_cb "$CB_DB" -c "SELECT pg_total_relation_size('cpu');")

cb_chunk_count=$(run_cb "$CB_DB" -c "
SELECT count(DISTINCT chunk_number) FROM time_series.ts_chunk
WHERE table_oid='cpu'::regclass;")

# TSDB: hypertable_size aggregates all child chunks + indexes + toast
tsdb_pre_bytes=$(run_tsdb "$TSDB_DB" -c "SELECT hypertable_size('cpu');")
tsdb_chunk_count=$(run_tsdb "$TSDB_DB" -c "SELECT count(*) FROM show_chunks('cpu');")

# ---------- compress ----------
echo "[5/6] Configuring compression ..."
PGPORT="$CB_PORT" psql -d "$CB_DB" -v ON_ERROR_STOP=1 \
    -c "SELECT time_series.set_compress_config('cpu'::regclass, 'tags_id', '\"time\"');" >/dev/null

PGPORT="$TSDB_PORT" psql -d "$TSDB_DB" -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE cpu SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'tags_id',
            timescaledb.compress_orderby = 'time DESC');" >/dev/null

echo "[5/6] Running compress on CB ..."
t1=$(date +%s.%N)
PGPORT="$CB_PORT" psql -d "$CB_DB" -v ON_ERROR_STOP=1 \
    -c "SELECT time_series.compress_chunks('cpu'::regclass);" >/dev/null
t2=$(date +%s.%N)
cb_compress_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.2f", b-a}')

echo "[5/6] Running compress on TSDB ..."
t1=$(date +%s.%N)
PGPORT="$TSDB_PORT" psql -d "$TSDB_DB" -v ON_ERROR_STOP=1 \
    -c "SELECT compress_chunk(c) FROM show_chunks('cpu') c;" >/dev/null
t2=$(date +%s.%N)
tsdb_compress_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.2f", b-a}')

# ---------- post-compress sizes ----------
echo "[6/6] Measuring post-compress sizes ..."

# CB stores PAX files OUTSIDE the relation's relfilenode
# (base/<db>/ts_compressed/<relid>/chunk_*.pax.seg*), so
# pg_total_relation_size doesn't see them.  Real post-compress storage is:
#     heap-fork residual (pg_total_relation_size)  +  PAX bytes
#
# Capture both "no reclaim" (heap + PAX still side-by-side) and "after reclaim"
# (heap forks truncated, PAX only) for honest comparison with TSDB which has no
# separate reclaim step.
cb_pax_bytes=$(run_cb "$CB_DB" -c "
SELECT coalesce(sum(compressed_size), 0)
FROM time_series.ts_compressed_chunk WHERE table_oid='cpu'::regclass;")
cb_heap_residual_pre_reclaim=$(run_cb "$CB_DB" -c "SELECT pg_total_relation_size('cpu');")
cb_post_compress_bytes=$(( cb_heap_residual_pre_reclaim + cb_pax_bytes ))

PGPORT="$CB_PORT" psql -d "$CB_DB" -v ON_ERROR_STOP=1 \
    -c "SELECT time_series.reclaim_chunk_heaps('cpu'::regclass);" >/dev/null

cb_heap_residual_post_reclaim=$(run_cb "$CB_DB" -c "SELECT pg_total_relation_size('cpu');")
cb_post_bytes=$(( cb_heap_residual_post_reclaim + cb_pax_bytes ))

# TSDB compress replaces the chunk in place — no separate reclaim step.
tsdb_post_bytes=$(run_tsdb "$TSDB_DB" -c "SELECT hypertable_size('cpu');")

# ---------- report ----------
hr_bytes() { awk -v b="$1" 'BEGIN{
    units[1]="B"; units[2]="KB"; units[3]="MB"; units[4]="GB";
    i=1; while (b>=1024 && i<4) { b/=1024; i++ }
    printf "%.1f %s", b, units[i]
}'; }

cb_ratio=$(awk -v a="$cb_post_bytes" -v b="$cb_pre_bytes" 'BEGIN{
    if (b==0) print "n/a"; else printf "%.2f", a/b
}')
tsdb_ratio=$(awk -v a="$tsdb_post_bytes" -v b="$tsdb_pre_bytes" 'BEGIN{
    if (b==0) print "n/a"; else printf "%.2f", a/b
}')

cb_chunks_per_s=$(awk -v c="$cb_chunk_count" -v t="$cb_compress_s" 'BEGIN{
    if (t==0) print "n/a"; else printf "%.1f", c/t
}')
tsdb_chunks_per_s=$(awk -v c="$tsdb_chunk_count" -v t="$tsdb_compress_s" 'BEGIN{
    if (t==0) print "n/a"; else printf "%.1f", c/t
}')

cat <<EOF

==============================================================
  Result
==============================================================

| Metric                       | CB time_series       | TimescaleDB           |
|------------------------------|----------------------|-----------------------|
| Rows                         | $N_ROWS              | $N_ROWS               |
| Chunks                       | $cb_chunk_count      | $tsdb_chunk_count     |
| Load time                    | ${cb_load_s} s       | ${tsdb_load_s} s      |
| Pre-compress size            | $(hr_bytes "$cb_pre_bytes")    | $(hr_bytes "$tsdb_pre_bytes")    |
| Post-compress size (no reclaim) | $(hr_bytes "$cb_post_compress_bytes") | — (TSDB compresses in place) |
| Post-compress size (after reclaim) | $(hr_bytes "$cb_post_bytes")    | $(hr_bytes "$tsdb_post_bytes")    |
| **Compression ratio**        | **$cb_ratio**            | **$tsdb_ratio**             |
| Compress wall-time           | ${cb_compress_s} s       | ${tsdb_compress_s} s      |
| **Compress throughput**      | **${cb_chunks_per_s} chunks/s** | **${tsdb_chunks_per_s} chunks/s** |

Lower ratio = better compression.  Higher chunks/s = faster compress.
EOF
