#!/usr/bin/env bash
#
# bench.sh — one-shot TSBS bench driver for Cloudberry (time_series) and
# TimescaleDB.  Wraps load + run + compare so the common flows are a
# single command.  Lower-level building blocks (load_compare.sh,
# run_tsbs_compare.sh) remain for ad-hoc use.
#
# Subcommands
#   load     <target>                    Drop + recreate DB, load data
#   run      <target>                    Execute query suite, save results
#   all      <target>                    load + run + print summary
#   compare  <a.tsv> <b.tsv>             Print side-by-side table + ratio
#   diff                                 Run BOTH CB and TSDB, then compare
#
# `<target>` is `cb` or `tsdb`.
#
# GUC handling
#   Pass `--guc name=value` zero or more times; the script applies them
#   via `ALTER DATABASE <db> SET name = value` before running queries
#   (libpq's startup options= escaping is fragile through pgx).  The
#   GUC list is also persisted with the result file for reproducibility.
#
# Layout (defaults rooted at the directory holding this script)
#   data/data_scale_<N>.gz   — gzipped TSBS data file (cpu-only format
#                              accepted by both cloudberry_ts and
#                              timescaledb loaders).  Decompressed
#                              on demand to a sibling .dat.
#   sql/                     — per-run scratch: generated `.bin` query
#                              files, loader logs, per-query stdout,
#                              and result TSVs.  Wiped at the start of
#                              every `run` / `all` / `diff`.
#
# Defaults
#   --scale 500
#   --data-cb  <bench>/data/data_scale_500
#   --data-tsdb <bench>/data/data_scale_500
#   --sql-dir  <bench>/sql
#   --queries 10
#   --workers 4
#   --batch 10000
#   --chunk-time 12h
#   Timestamps from `--ts-start` / `--ts-end` (default 2025-01-01 .. 2025-01-04;
#     these MUST match the data generation window or queries scan empty
#     ranges — common pitfall).
#
# Example: full CB-vs-TSDB run with ChunkAppend + parallel
#   ./bench.sh diff \
#       --guc enable_parallel=on \
#       --guc max_parallel_workers_per_gather=4 \
#       --guc time_series.enable_chunk_append=on
#
# Example: load only, then run twice with different GUCs
#   ./bench.sh load cb
#   ./bench.sh run cb --guc enable_parallel=off --out=/tmp/cb_noparallel.tsv
#   ./bench.sh run cb --guc enable_parallel=on  --out=/tmp/cb_parallel.tsv
#   ./bench.sh compare /tmp/cb_noparallel.tsv /tmp/cb_parallel.tsv

set -u
shopt -s extglob

# ─── Configurable defaults (override via flags or env) ────────────────
# Bench root = the directory holding this script.  Data and the
# generated `.bin` query files live underneath it so the tree is
# self-contained and reproducible.
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$BENCH_DIR/data"
SQL_DIR="$BENCH_DIR/sql"

SCALE="${SCALE:-500}"
# The data file lives under data/ and is gzipped to keep the tree
# small.  TSBS's cloudberry_ts and timescaledb formats happen to share
# the same on-disk shape for cpu-only, so one file feeds both loaders.
DATA_GZ="${DATA_GZ:-$DATA_DIR/data_scale_${SCALE}.gz}"
DATA_CB="${DATA_CB:-$DATA_DIR/data_scale_${SCALE}}"
DATA_TSDB="${DATA_TSDB:-$DATA_DIR/data_scale_${SCALE}}"
PER_TYPE_N="${PER_TYPE_N:-10}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-10000}"
CHUNK_TIME="${CHUNK_TIME:-12h}"
TS_START="${TS_START:-2025-01-01T00:00:00Z}"
TS_END="${TS_END:-2025-01-04T00:00:00Z}"
PER_QUERY_TIMEOUT="${PER_QUERY_TIMEOUT:-180}"

CB_PORT="${CB_PORT:-7000}"
CB_USER="${CB_USER:-gpadmin}"
CB_DB="${CB_DB:-tsbs_cb_500}"
TSDB_PORT="${TSDB_PORT:-15432}"
TSDB_USER="${TSDB_USER:-gpadmin}"
TSDB_DB="${TSDB_DB:-tsbs_cb_500}"

PG16_PSQL="${PG16_PSQL:-/workspace/pg16-install/bin/psql}"
TSBS_DIR="${TSBS_DIR:-/workspace/hashdata-lightning/tsbs}"
LOAD_CB_BIN="${LOAD_CB_BIN:-/tmp/tsbs_load_cb}"
LOAD_TSDB_BIN="${LOAD_TSDB_BIN:-/tmp/tsbs_load_tsdb}"
RUN_CB_BIN="${RUN_CB_BIN:-/tmp/tsbs_run_cb}"
RUN_TSDB_BIN="${RUN_TSDB_BIN:-/tmp/tsbs_run_tsdb}"

# Standard TSBS CPU query suite.  `lastpoint` excluded by default — the
# LATERAL pattern hangs on CB without specific planner work; tracked in
# project_ts_lateral_lastpoint_limit memory.  Add it back via QSET env.
DEFAULT_QSET="single-groupby-1-1-1 single-groupby-1-1-12 single-groupby-1-8-1 \
single-groupby-5-1-1 single-groupby-5-1-12 single-groupby-5-8-1 \
cpu-max-all-1 cpu-max-all-8 \
double-groupby-1 double-groupby-5 double-groupby-all \
high-cpu-1 high-cpu-all groupby-orderby-limit"
QSET="${QSET:-$DEFAULT_QSET}"

# ─── Helpers ──────────────────────────────────────────────────────────
die() { echo "bench.sh: $*" >&2; exit 1; }

source_greenplum() {
    [ -f /workspace/lightning-install/greenplum_path.sh ] && \
        source /workspace/lightning-install/greenplum_path.sh
}

usage() {
    cat <<EOF
Usage: $0 <subcommand> [options]

Subcommands:
  load     <cb|tsdb>                          drop + recreate + load
  run      <cb|tsdb> [--out FILE]             execute query suite
  all      <cb|tsdb> [--out FILE]             load + run
  compare  <a.tsv> <b.tsv>                    side-by-side + ratio
  diff     [--out-cb F] [--out-tsdb F]        run both + compare

Common options:
  --guc name=value          apply GUC via ALTER DATABASE (repeatable)
  --scale N                 (default $SCALE)
  --data-cb PATH            (default $DATA_CB)
  --data-tsdb PATH          (default $DATA_TSDB)
  --sql-dir DIR             (default $SQL_DIR)
  --queries N               (default $PER_TYPE_N)
  --workers N               (default $WORKERS)
  --batch N                 (default $BATCH)
  --chunk-time DUR          (default $CHUNK_TIME)
  --ts-start ISO            (default $TS_START)
  --ts-end   ISO            (default $TS_END)

Env overrides: CB_PORT, CB_USER, CB_DB, TSDB_PORT, TSDB_USER, TSDB_DB,
  PG16_PSQL, TSBS_DIR, LOAD_CB_BIN, LOAD_TSDB_BIN, RUN_CB_BIN, RUN_TSDB_BIN,
  QSET, PER_QUERY_TIMEOUT
EOF
}

# Parse --flag=value / --flag value style; consumes argv and sets globals
# plus the GUCS array.  Returns the remaining positional args to caller.
GUCS=()
OUT_FILE=""
OUT_CB=""
OUT_TSDB=""
parse_args() {
    local positional=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --guc)         GUCS+=("$2"); shift 2 ;;
            --guc=*)       GUCS+=("${1#*=}"); shift ;;
            --scale)       SCALE="$2"; shift 2 ;;
            --scale=*)     SCALE="${1#*=}"; shift ;;
            --data-cb)     DATA_CB="$2"; shift 2 ;;
            --data-cb=*)   DATA_CB="${1#*=}"; shift ;;
            --data-tsdb)   DATA_TSDB="$2"; shift 2 ;;
            --data-tsdb=*) DATA_TSDB="${1#*=}"; shift ;;
            --sql-dir)     SQL_DIR="$2"; shift 2 ;;
            --sql-dir=*)   SQL_DIR="${1#*=}"; shift ;;
            --queries)     PER_TYPE_N="$2"; shift 2 ;;
            --queries=*)   PER_TYPE_N="${1#*=}"; shift ;;
            --workers)     WORKERS="$2"; shift 2 ;;
            --workers=*)   WORKERS="${1#*=}"; shift ;;
            --batch)       BATCH="$2"; shift 2 ;;
            --batch=*)     BATCH="${1#*=}"; shift ;;
            --chunk-time)  CHUNK_TIME="$2"; shift 2 ;;
            --chunk-time=*) CHUNK_TIME="${1#*=}"; shift ;;
            --ts-start)    TS_START="$2"; shift 2 ;;
            --ts-start=*)  TS_START="${1#*=}"; shift ;;
            --ts-end)      TS_END="$2"; shift 2 ;;
            --ts-end=*)    TS_END="${1#*=}"; shift ;;
            --out)         OUT_FILE="$2"; shift 2 ;;
            --out=*)       OUT_FILE="${1#*=}"; shift ;;
            --out-cb)      OUT_CB="$2"; shift 2 ;;
            --out-cb=*)    OUT_CB="${1#*=}"; shift ;;
            --out-tsdb)    OUT_TSDB="$2"; shift 2 ;;
            --out-tsdb=*)  OUT_TSDB="${1#*=}"; shift ;;
            -h|--help)     usage; exit 0 ;;
            --)            shift; positional+=("$@"); break ;;
            -*)            die "unknown option: $1" ;;
            *)             positional+=("$1"); shift ;;
        esac
    done
    POSITIONAL=("${positional[@]:-}")
}

# Resolve target → (psql_cmd, port, user, db, data_file, load_bin, run_bin)
resolve_target() {
    local target="$1"
    case "$target" in
        cb)
            TGT_PSQL="psql"
            TGT_PORT="$CB_PORT"
            TGT_USER="$CB_USER"
            TGT_DB="$CB_DB"
            TGT_DATA="$DATA_CB"
            TGT_LOAD_BIN="$LOAD_CB_BIN"
            TGT_RUN_BIN="$RUN_CB_BIN"
            TGT_FORMAT="cloudberry_ts"
            TGT_EXT="time_series"
            ;;
        tsdb)
            TGT_PSQL="$PG16_PSQL -h localhost"
            TGT_PORT="$TSDB_PORT"
            TGT_USER="$TSDB_USER"
            TGT_DB="$TSDB_DB"
            TGT_DATA="$DATA_TSDB"
            TGT_LOAD_BIN="$LOAD_TSDB_BIN"
            TGT_RUN_BIN="$RUN_TSDB_BIN"
            TGT_FORMAT="timescaledb"
            TGT_EXT="timescaledb"
            ;;
        *) die "unknown target: $target (expected cb or tsdb)" ;;
    esac
}

psql_exec() {
    PGPORT="$TGT_PORT" $TGT_PSQL -U "$TGT_USER" -d "$1" -c "$2" "${@:3}"
}

# Wipe and recreate sql/.  Called at the start of every run/diff/all so
# each bench iteration starts from a clean scratch dir (the user asked
# for stale .bin / per-query logs to never silently feed the next run).
# `load` does not call this — sql/ only holds query-side artefacts.
reset_sql_dir() {
    rm -rf "$SQL_DIR"
    mkdir -p "$SQL_DIR"
    echo "  sql: wiped + recreated $SQL_DIR"
}

# Auto-decompress data/data_scale_N.gz to a sibling data_scale_N file
# if the uncompressed copy is missing or stale (older than the .gz).
# Both loaders read the uncompressed form via `cat | tsbs_load`.
ensure_data_decompressed() {
    local plain="$1"
    local gz="${plain}.gz"
    if [ -f "$plain" ] && [ -s "$plain" ]; then
        if [ ! -f "$gz" ] || [ "$plain" -nt "$gz" ]; then
            return 0
        fi
    fi
    [ -f "$gz" ] || die "no data file: neither $plain nor $gz exists"
    echo "  data: decompressing $gz → $plain (one-time)"
    gunzip -k -c "$gz" > "$plain" || die "gunzip failed for $gz"
}

apply_gucs() {
    # Apply each GUC via ALTER DATABASE.  CB-specific GUCs (e.g.
    # `enable_parallel`, `time_series.*`) will error on TSDB and vice
    # versa; treat such failures as "this side doesn't know that knob"
    # and warn rather than abort, so a single `--guc enable_parallel=on`
    # works for `diff` mode without per-target plumbing.
    local g
    for g in "${GUCS[@]+"${GUCS[@]}"}"; do
        local k="${g%%=*}"
        local v="${g#*=}"
        if psql_exec postgres \
            "ALTER DATABASE \"$TGT_DB\" SET \"$k\" = '$v';" >/dev/null 2>&1; then
            echo "  guc: $k = $v"
        else
            echo "  guc: $k skipped (not recognized by this target)" >&2
        fi
    done
}

reset_db() {
    # Force-terminate any sessions on the target DB (time_series BGW workers
    # in particular hang on idle connections during shutdown).
    psql_exec postgres \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$TGT_DB';" \
        >/dev/null 2>&1 || true

    if [ "$TGT_EXT" = "time_series" ]; then
        psql_exec "$TGT_DB" "SELECT time_series.stop_background_workers();" >/dev/null 2>&1 || true
    fi

    local i
    for i in 1 2 3; do
        psql_exec postgres "DROP DATABASE IF EXISTS \"$TGT_DB\" WITH (FORCE);" \
            >/dev/null 2>&1 && break
        sleep 1
    done
    psql_exec postgres "CREATE DATABASE \"$TGT_DB\";" >/dev/null
    psql_exec "$TGT_DB" "CREATE EXTENSION IF NOT EXISTS $TGT_EXT;" >/dev/null
}

# ─── Subcommands ──────────────────────────────────────────────────────
do_load() {
    local target="$1"
    resolve_target "$target"
    ensure_data_decompressed "$TGT_DATA"
    [ -x "$TGT_LOAD_BIN" ] || die "loader missing: $TGT_LOAD_BIN"

    echo "[load:$target] db=$TGT_DB data=$TGT_DATA"
    reset_db
    apply_gucs

    local t0=$(date +%s)
    cat "$TGT_DATA" | "$TGT_LOAD_BIN" load "$TGT_FORMAT" \
        --postgres='sslmode=disable' --user="$TGT_USER" --pass='' \
        --admin-db-name="$TGT_DB" --db-name="$TGT_DB" \
        --host=localhost --port="$TGT_PORT" \
        --field-index='' --time-index=false \
        --time-partition-index=false --partition-index=false \
        --workers="$WORKERS" --batch-size="$BATCH" --chunk-time="$CHUNK_TIME" \
        --use-hypertable=true --do-create-db=false \
        > "$SQL_DIR/load_${target}.log" 2>&1
    local elapsed=$(( $(date +%s) - t0 ))
    echo "  loaded in ${elapsed}s"
    tail -3 "$SQL_DIR/load_${target}.log"

    # For fair scan-vs-scan comparison: TSDB loader auto-creates btree
    # indexes on cpu_tags; drop them so we measure scan-vs-scan, not
    # indexed-lookup vs scan.
    if [ "$target" = "tsdb" ]; then
        psql_exec "$TGT_DB" \
            "DO \$\$ DECLARE r record; BEGIN
                FOR r IN SELECT indexname FROM pg_indexes
                         WHERE schemaname='public' AND indexname LIKE 'cpu_tags%'
                LOOP EXECUTE format('DROP INDEX %I CASCADE', r.indexname); END LOOP;
             END\$\$;" >/dev/null
        echo "  TSDB indexes dropped (fair-scan mode)"
    fi
}

generate_queries() {
    local target="$1"
    resolve_target "$target"
    mkdir -p "$SQL_DIR"
    local q
    for q in $QSET; do
        local f="$SQL_DIR/q_${target}_${q}.bin"
        [ -f "$f" ] && continue
        "$TSBS_DIR/tsbs_generate_queries" \
            --use-case=cpu-only --format="$TGT_FORMAT" \
            --queries="$PER_TYPE_N" --scale="$SCALE" --seed=1 \
            --timestamp-start="$TS_START" --timestamp-end="$TS_END" \
            --query-type="$q" --file="$f" > /dev/null 2>&1
    done
}

extract_mean_ms() {
    awk '/^all queries/{found=1; next} found && /mean:/{
        for (i=1; i<=NF; i++) if ($i=="mean:") { gsub(/ms,?/, "", $(i+1)); print $(i+1); exit }
    }' "$1"
}

do_run() {
    local target="$1"
    resolve_target "$target"
    [ -x "$TGT_RUN_BIN" ] || die "runner missing: $TGT_RUN_BIN"

    # Wipe sql/ exactly once per invocation, before generating queries.
    # `diff` sequences run-cb then run-tsdb back-to-back, so a guard
    # avoids the second call clobbering the first call's TSV.
    if [ "${SQL_DIR_RESET:-0}" != "1" ]; then
        reset_sql_dir
        SQL_DIR_RESET=1
    fi

    apply_gucs
    generate_queries "$target"

    # Default --out lives in the bench root, NOT under sql/ — sql/ gets
    # wiped on every run, so a TSV stored there would be lost before
    # `compare` could reach it.
    local out="${OUT_FILE:-$BENCH_DIR/results_${target}.tsv}"
    local results_dir="$SQL_DIR/results_${target}"
    mkdir -p "$results_dir"

    # TSV header includes the active GUC list as a comment so we can
    # reconstruct the run later.
    {
        echo "# bench=$target db=$TGT_DB scale=$SCALE queries=$PER_TYPE_N ts=$TS_START..$TS_END"
        if [ "${#GUCS[@]}" -gt 0 ]; then
            echo "# gucs: ${GUCS[*]}"
        else
            echo "# gucs: <db-defaults>"
        fi
        printf "query\tmean_ms\n"
    } > "$out"

    echo "[run:$target] writing $out"
    local q
    for q in $QSET; do
        local r="$results_dir/${q}.txt"
        local rc
        # --debug=1 makes the TSBS runner print each query's SQL to
        # stdout before executing it.  Timing output (mean / min / max
        # / wall) is unaffected, so we capture both with one run and
        # then split the SQL out into a sibling .sql file for humans.
        timeout "$PER_QUERY_TIMEOUT" "$TGT_RUN_BIN" \
            --file="$SQL_DIR/q_${target}_${q}.bin" \
            --hosts=localhost --port="$TGT_PORT" \
            --user="$TGT_USER" --db-name="$TGT_DB" \
            --postgres='sslmode=disable' \
            --workers=1 --max-queries="$PER_TYPE_N" \
            --debug=1 \
            > "$r" 2>&1
        rc=$?

        local m
        if [ "$rc" = "124" ]; then
            m="TIMEOUT"
        else
            m=$(extract_mean_ms "$r")
            [ -z "$m" ] && m="ERROR"
        fi
        printf "%s\t%s\n" "$q" "$m" >> "$out"
        printf "  %-32s %s\n" "$q" "$m"

        # Extract the human-readable SQL from the debug log into
        # sql/<target>_<query>.sql.  Each `--debug=1` query block is a
        # block of lines starting with SELECT/WITH/INSERT/UPDATE/
        # DELETE; the block ends at the next such line or at TSBS's
        # `Run complete` summary.  Stop before timing / "all queries"
        # / "wall clock time" output.
        # TSBS query formats differ between query types: single-groupby
        # has its top-level SELECT at column 0, double-groupby's WITH /
        # SELECT have leading whitespace, etc.  Inner subqueries appear
        # at deeper indent.  So we identify a "new query block" as a
        # SELECT/WITH/INSERT/UPDATE/DELETE at the SAME indent as the
        # first such keyword in this output (the base indent).  Blank
        # lines are dropped to keep the .sql clean.
        awk '
            BEGIN { buf = ""; base_indent = -1 }
            /^Run complete/ {
                if (buf != "") { print buf ";"; buf = "" }
                exit
            }
            NF == 0 { next }
            {
                match($0, /^[ \t]*/)
                indent = RLENGTH
                rest = substr($0, indent + 1)
                is_kw = (rest ~ /^(SELECT|WITH|INSERT|UPDATE|DELETE)[ \t]/)

                if (is_kw) {
                    if (base_indent == -1) {
                        base_indent = indent
                    } else if (indent == base_indent && buf != "") {
                        # If the current buffer started with `WITH` and
                        # we are now seeing `SELECT` at the same indent,
                        # this is the SELECT-after-CTE that belongs to
                        # the same statement, not a new query.
                        first = buf
                        nl = index(buf, "\n")
                        if (nl > 0) first = substr(buf, 1, nl - 1)
                        sub(/^[ \t]+/, "", first)
                        if (first ~ /^WITH/ && rest ~ /^SELECT/) {
                            # continuation; do not flush
                        } else {
                            print buf ";\n"
                            buf = ""
                        }
                    }
                }
                if (buf == "") buf = $0
                else buf = buf "\n" $0
            }
            END { if (buf != "") print buf ";" }
        ' "$r" > "$SQL_DIR/${target}_${q}.sql"
    done
    echo "[run:$target] done — $out"
}

do_all() {
    local target="$1"
    # Wipe sql/ up front so the load_<target>.log written by do_load is
    # not nuked when do_run hits its own (now guarded) reset.
    reset_sql_dir
    SQL_DIR_RESET=1
    do_load "$target"
    do_run "$target"
}

do_compare() {
    local a="$1" b="$2"
    [ -f "$a" ] || die "missing $a"
    [ -f "$b" ] || die "missing $b"

    local label_a label_b
    label_a=$(basename "$a" .tsv)
    label_b=$(basename "$b" .tsv)

    echo "# $a"
    grep '^#' "$a" | sed 's/^# */    /'
    echo "# $b"
    grep '^#' "$b" | sed 's/^# */    /'

    printf "%-32s | %12s | %12s | %s\n" "query" "$label_a" "$label_b" "ratio (a/b)"
    printf -- "---------------------------------+--------------+--------------+--------\n"

    # Build assoc maps from each tsv (skip comments and header) and
    # record the order queries first appear in `a` so the table prints
    # in run-order rather than hash order.
    declare -A va vb seen
    local order=()
    while IFS=$'\t' read -r q v; do
        [[ "$q" =~ ^# || "$q" == "query" ]] && continue
        va[$q]="$v"
        if [ -z "${seen[$q]:-}" ]; then seen[$q]=1; order+=("$q"); fi
    done < "$a"
    while IFS=$'\t' read -r q v; do
        [[ "$q" =~ ^# || "$q" == "query" ]] && continue
        vb[$q]="$v"
        if [ -z "${seen[$q]:-}" ]; then seen[$q]=1; order+=("$q"); fi
    done < "$b"

    local q
    for q in "${order[@]+"${order[@]}"}"; do
        local x="${va[$q]:-MISSING}"
        local y="${vb[$q]:-MISSING}"
        local r="n/a"
        if [[ "$x" =~ ^[0-9.]+$ && "$y" =~ ^[0-9.]+$ ]]; then
            r=$(awk -v a="$x" -v b="$y" 'BEGIN{ if (b > 0) printf "%.2fx", a/b; else print "n/a" }')
        fi
        printf "%-32s | %12s | %12s | %s\n" "$q" "$x" "$y" "$r"
    done
}

do_diff() {
    # Run both CB and TSDB end-to-end (no load), then print the compare
    # table.  Useful as the "single command that gives you the side-by-
    # side answer."  sql/ is wiped once at the start of do_run cb; the
    # SQL_DIR_RESET guard prevents the second do_run from wiping again
    # and losing the CB output that already lives there.
    reset_sql_dir
    SQL_DIR_RESET=1
    # Same rationale as do_run: results TSVs default to bench root so
    # they survive sql/ wipes between runs.
    local out_cb="${OUT_CB:-$BENCH_DIR/results_cb.tsv}"
    local out_tsdb="${OUT_TSDB:-$BENCH_DIR/results_tsdb.tsv}"
    OUT_FILE="$out_cb"
    do_run cb
    OUT_FILE="$out_tsdb"
    do_run tsdb
    echo
    do_compare "$out_cb" "$out_tsdb"
}

# ─── Entrypoint ───────────────────────────────────────────────────────
source_greenplum
mkdir -p "$DATA_DIR"
# sql/ is created by reset_sql_dir at the top of each run/diff/all so
# stale .bin / per-query stdout from a previous run never leak in.
# `load` writes its log via $SQL_DIR/load_*.log, so we ensure it
# exists here too (load by itself never resets).
mkdir -p "$SQL_DIR"

CMD="${1:-}"
[ -z "$CMD" ] && { usage; exit 2; }
shift

case "$CMD" in
    load|run|all)
        [ $# -lt 1 ] && die "missing target (cb|tsdb)"
        target="$1"; shift
        parse_args "$@"
        case "$CMD" in
            load) do_load "$target" ;;
            run)  do_run  "$target" ;;
            all)  do_all  "$target" ;;
        esac
        ;;
    compare)
        parse_args "$@"
        [ "${#POSITIONAL[@]}" -lt 2 ] && die "compare needs two TSV files"
        do_compare "${POSITIONAL[0]}" "${POSITIONAL[1]}"
        ;;
    diff)
        parse_args "$@"
        do_diff
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "unknown subcommand: $CMD" >&2
        usage
        exit 2
        ;;
esac
