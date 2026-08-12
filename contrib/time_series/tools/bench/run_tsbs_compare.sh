#!/bin/bash
# TSBS comparison runner with two modes:
#
#   perf    — per-query median latency.  Invoke twice (cb / tsdb), then
#             diff outputs externally.
#   verify  — both clusters fetched in one invocation; query results
#             diffed after normalisation.  Reports OK / DIFF per query.
#
# Usage (perf — backward-compatible single-cluster runner):
#   ./run_tsbs_compare.sh perf  <label> <runner> <prefix> <port> <user> <db>
#
# Usage (verify — both clusters in one go):
#   ./run_tsbs_compare.sh verify \
#       <cb_runner>   <cb_prefix>   <cb_port>   <cb_user>   <cb_db> \
#       <tsdb_runner> <tsdb_prefix> <tsdb_port> <tsdb_user> <tsdb_db>
#
# Common env overrides:
#   QUERIES_DIR   directory holding generated .bin files (default /tmp)
#   QUERIES       space-separated query names (default = full DevOps set)
#   HEAVY         subset run with --max-queries=20 instead of 100 (perf mode)
#   VERIFY_N      rows per query in verify mode (default 5)
#   FLOAT_DIGITS  float precision kept by normaliser (default 4)

QUERIES_DIR="${QUERIES_DIR:-/tmp}"
# POSTGRES_OPTS gets folded into the --postgres connection string so that
# startup-time GUCs (e.g. `-c time_series.enable_chunk_append=on`) actually
# reach the backend.  PGOPTIONS env is NOT propagated by tsbs's Go lib/pq
# client — it must travel in the connection string itself.
POSTGRES_OPTS="${POSTGRES_OPTS:-}"

DEFAULT_QUERIES="single-groupby-1-1-1 single-groupby-1-1-12 single-groupby-1-8-1 \
single-groupby-5-1-1 single-groupby-5-1-12 single-groupby-5-8-1 \
cpu-max-all-1 cpu-max-all-8 cpu-max-all-32-24 \
double-groupby-1 double-groupby-5 double-groupby-all \
groupby-orderby-limit high-cpu-all high-cpu-1 lastpoint"

DEFAULT_HEAVY="cpu-max-all-8 cpu-max-all-32-24 double-groupby-1 double-groupby-5 \
double-groupby-all groupby-orderby-limit high-cpu-all lastpoint"

QUERIES="${QUERIES:-$DEFAULT_QUERIES}"
HEAVY="${HEAVY:-$DEFAULT_HEAVY}"
VERIFY_N="${VERIFY_N:-5}"
FLOAT_DIGITS="${FLOAT_DIGITS:-4}"

# Strip everything that's expected to differ between clusters (banners,
# per-query timing lines, etc.), round floats to FLOAT_DIGITS decimals,
# and sort lines — TSBS queries don't all have ORDER BY, so row order
# is non-deterministic.  What's left should be the actual row payload.
normalize() {
    # Drop everything that's expected to differ between clusters:
    #   - timing banners (`wall clock time: …`, `Took: …`, etc.)
    #   - the per-query descriptor TSBS prints, which starts with the
    #     cluster name (`Cloudberry …` / `TimescaleDB …`)
    #   - empty lines
    # Mask synthetic tag IDs ("id": N inside additional_tags JSON):
    # Cloudberry's MPP tags table uses per-segment bigserial so the same
    # logical host gets a different surrogate id than TSDB's serial.
    # Hostnames still match, which is what we care about.
    # Then round floats and sort (TSBS queries lack stable ORDER BY).
    grep -vE '^(time|TsbsBenchmark|run [0-9]|all queries|Took:|min:|max:|avg:|med:|stddev:|response time|elapsed|wall clock|Spent[ ]|target|workers|Run\s|Cloudberry |TimescaleDB |PostgreSQL )' "$1" \
    | grep -vE '^[[:space:]]*$' \
    | sed -E "s/(\"(id|tags_id)\":[[:space:]]*)[0-9]+/\\1<masked>/g" \
    | sed -E "s/([0-9]+\\.[0-9]{${FLOAT_DIGITS}})[0-9]+/\\1/g" \
    | LC_ALL=C sort
}

run_perf() {
    local label="$1" runner="$2" prefix="$3" port="$4" user="$5" db="$6"
    for q in $QUERIES; do
        local n=100
        for h in $HEAVY; do [[ "$q" == "$h" ]] && n=20; done
        local line
        line=$(timeout 1800 "$runner" --file="$QUERIES_DIR/${prefix}_q_${q}.bin" \
            --hosts=localhost --port="$port" --user="$user" --db-name="$db" \
            --postgres="sslmode=disable${POSTGRES_OPTS:+ options='${POSTGRES_OPTS}'}" \
            --workers=4 --max-queries=$n 2>&1 | grep -A1 "^all queries" | tail -1)
        local med
        med=$(echo "$line" | sed -n 's/.*med: *\([0-9.]*\)ms.*/\1/p')
        [ -z "$med" ] && med="TIMEOUT"
        printf "%-30s | %12s\n" "$q" "$med"
    done
}

run_verify() {
    local cb_runner="$1"   cb_prefix="$2"   cb_port="$3"   cb_user="$4"   cb_db="$5"
    local tsdb_runner="$6" tsdb_prefix="$7" tsdb_port="$8" tsdb_user="$9" tsdb_db="${10}"

    local outdir
    outdir=$(mktemp -d)
    local ok=0 diff_count=0 missing=0

    printf "%-30s | %-8s | %s\n" "query" "result" "detail"
    printf -- "-------------------------------+----------+------------------------\n"

    for q in $QUERIES; do
        local cb_in="$QUERIES_DIR/${cb_prefix}_q_${q}.bin"
        local tsdb_in="$QUERIES_DIR/${tsdb_prefix}_q_${q}.bin"

        if [ ! -f "$cb_in" ] || [ ! -f "$tsdb_in" ]; then
            printf "%-30s | %-8s | %s\n" "$q" "MISSING" "input bin not found"
            missing=$((missing + 1))
            continue
        fi

        local cb_out="$outdir/cb_${q}.raw"
        local tsdb_out="$outdir/tsdb_${q}.raw"

        timeout 600 "$cb_runner" --file="$cb_in" \
            --hosts=localhost --port="$cb_port" --user="$cb_user" --db-name="$cb_db" \
            --postgres="sslmode=disable${POSTGRES_OPTS:+ options='${POSTGRES_OPTS}'}" \
            --workers=1 --max-queries="$VERIFY_N" --print-responses=true \
            > "$cb_out" 2>/dev/null

        timeout 600 "$tsdb_runner" --file="$tsdb_in" \
            --hosts=localhost --port="$tsdb_port" --user="$tsdb_user" --db-name="$tsdb_db" \
            --postgres="sslmode=disable${POSTGRES_OPTS:+ options='${POSTGRES_OPTS}'}" \
            --workers=1 --max-queries="$VERIFY_N" --print-responses=true \
            > "$tsdb_out" 2>/dev/null

        normalize "$cb_out"   > "$outdir/cb_${q}.norm"
        normalize "$tsdb_out" > "$outdir/tsdb_${q}.norm"

        if diff -q "$outdir/cb_${q}.norm" "$outdir/tsdb_${q}.norm" >/dev/null; then
            local nrows
            nrows=$(wc -l < "$outdir/cb_${q}.norm")
            printf "%-30s | %-8s | %s\n" "$q" "OK" "${nrows} rows match"
            ok=$((ok + 1))
        else
            local cb_lines tsdb_lines
            cb_lines=$(wc -l < "$outdir/cb_${q}.norm")
            tsdb_lines=$(wc -l < "$outdir/tsdb_${q}.norm")
            printf "%-30s | %-8s | %s\n" "$q" "DIFF" \
                "cb=${cb_lines}rows tsdb=${tsdb_lines}rows — see $outdir/${q}.diff"
            diff "$outdir/cb_${q}.norm" "$outdir/tsdb_${q}.norm" \
                > "$outdir/${q}.diff" 2>&1
            diff_count=$((diff_count + 1))
        fi
    done

    printf -- "-------------------------------+----------+------------------------\n"
    printf "Summary: %d OK, %d DIFF, %d MISSING (artefacts in %s)\n" \
        "$ok" "$diff_count" "$missing" "$outdir"

    [ "$diff_count" -eq 0 ]
}

MODE="${1:-}"
shift || true

case "$MODE" in
    perf)
        if [ $# -ne 6 ]; then
            echo "Usage: $0 perf <label> <runner> <prefix> <port> <user> <db>" >&2
            exit 2
        fi
        run_perf "$@"
        ;;
    verify)
        if [ $# -ne 10 ]; then
            echo "Usage: $0 verify <cb_runner> <cb_prefix> <cb_port> <cb_user> <cb_db> \\" >&2
            echo "                  <tsdb_runner> <tsdb_prefix> <tsdb_port> <tsdb_user> <tsdb_db>" >&2
            exit 2
        fi
        run_verify "$@"
        ;;
    *)
        echo "Usage: $0 {perf|verify} ..." >&2
        echo "       $0 perf <label> <runner> <prefix> <port> <user> <db>" >&2
        echo "       $0 verify <cb_runner> <cb_prefix> <cb_port> <cb_user> <cb_db> \\" >&2
        echo "                 <tsdb_runner> <tsdb_prefix> <tsdb_port> <tsdb_user> <tsdb_db>" >&2
        exit 2
        ;;
esac
