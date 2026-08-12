# time_series Benchmarks

Standalone benchmarks for the `contrib/time_series` extension.  These
scripts are intentionally *not* part of the extension's regression suite;
they require external tooling and a running cluster, and they wipe / load
significant volumes of data.

| Script | Compares | Inputs |
|---|---|---|
| **`bench.sh`** *(top-level driver)* | One-shot TSBS load + run + compare for CB / TSDB | TSBS data files + binaries (see Prerequisites) |
| `insert_compare.sql` | INSERT path on `USING heap` vs `USING time_series`, three batch sizes | None (uses `generate_series`) |
| `compress_compare.sql` | `compress_chunks` / `reclaim_chunk_heaps` throughput + PAX/heap size ratio | None |
| `post_compress_perf.sql` | Range scan, column projection, and INSERT on ACTIVE vs COMPRESSED vs PARTIAL chunks | None |
| `compress_vs_tsdb.sh` | Compression ratio + speed: CB time_series (PAX) vs TimescaleDB hypertable | Running TSDB cluster |
| `load_compare.sh` | Bulk-load throughput via TSBS `tsbs_load_cb`, `--use-hypertable=true` vs `false` | TSBS data file, `tsbs_load_cb` binary |
| `run_tsbs_compare.sh perf` | TSBS query latency runner (call twice, once per target DB) | TSBS query files, `tsbs_run_queries_*` binary |
| `run_tsbs_compare.sh verify` | TSBS query *result* checker — runs both clusters, diffs normalised output | Same as above |

Most day-to-day comparison work goes through **`bench.sh`**.  The other
scripts remain for ad-hoc / lower-level tasks.

## Prerequisites

1. **Running Cloudberry cluster** with the `time_series` extension installed.
   The default scripts target `PGPORT=7000`, `PGUSER=gpadmin`.

2. **TSBS toolchain** built once.  Sources live at `tsbs/` at the repo
   root.  Recommended location for the resulting binaries:

   ```bash
   cd tsbs && go build -o /tmp/tsbs_generate_data   ./cmd/tsbs_generate_data
   cd tsbs && go build -o /tmp/tsbs_generate_queries ./cmd/tsbs_generate_queries
   cd tsbs && go build -o /tmp/tsbs_load_cb         ./cmd/tsbs_load_cloudberry_ts
   cd tsbs && go build -o /tmp/tsbs_run_cb          ./cmd/tsbs_run_queries_cloudberry_ts
   cd tsbs && go build -o /tmp/tsbs_load_tsdb       ./cmd/tsbs_load_timescaledb
   cd tsbs && go build -o /tmp/tsbs_run_tsdb        ./cmd/tsbs_run_queries_timescaledb
   ```

3. **TSBS data file** generated once and dropped under `data/` (the
   directory is gitignored — fresh clones come up empty):

   ```bash
   mkdir -p contrib/time_series/tools/bench/data
   /tmp/tsbs_generate_data --use-case=cpu-only --seed=123 --scale=500 \
     --timestamp-start=2025-01-01T00:00:00Z \
     --timestamp-end=2025-01-04T00:00:00Z \
     --log-interval=10s --format=cloudberry_ts \
     | gzip > contrib/time_series/tools/bench/data/data_scale_500.gz
   ```

   Scale 500 / 3 days / 10s interval yields ~13 M rows, ~330 MB gz.
   `bench.sh` decompresses to `data/data_scale_500` on first load.  The
   `cloudberry_ts` and `timescaledb` formats happen to share the same
   on-disk shape for cpu-only, so one file feeds both targets.

4. **TSBS query files** — `bench.sh` generates them on demand under
   `sql/` (gitignored, wiped each run).  `run_tsbs_compare.sh` (the
   lower-level script) consumes them from any directory you pass via
   `QUERIES_DIR`.

## Quick start: `bench.sh` (one-shot driver)

`bench.sh` is the top-level driver for TSBS comparisons.  It wraps load
+ run + compare into single commands and applies session-level GUCs via
`ALTER DATABASE ... SET` (avoiding libpq's brittle `options=-c k=v`
escaping through the pgx client).

### Layout

```
tools/bench/
  data/                      ← tracked .gz; one file feeds both targets
    data_scale_500.gz        ← cpu-only TSBS data (scale=500, 3 days, 10s)
    data_scale_500           ← auto-decompressed on first load (gitignored)
  sql/                       ← per-run scratch (gitignored, wiped each run)
    q_cb_<query>.bin           generated TSBS query bin (input to runner)
    q_tsdb_<query>.bin
    cb_<query>.sql             human-readable SQL extracted from the
    tsdb_<query>.sql           runner's `--debug=1` output (one block per
                               query, separated by `;`)
    load_<target>.log          loader stdout/stderr
    results_<target>/<q>.txt   raw per-query TSBS run output (debug + timing)
  results_cb.tsv             ← mean-ms summary (default --out location,
  results_tsdb.tsv             bench root so it survives sql/ wipes)
```

The `.bin` files are required by the TSBS runner.  The `.sql` files
exist so you can read what was actually executed without writing a
gob decoder — they're produced as a side effect of running with
`--debug=1`.

`data/*.gz` is the only artefact that travels with the repo.  Everything
under `sql/` is per-run scratch and gets removed + recreated at the
start of every `run`, `all`, and `diff`.  This guarantees a stale `.bin`
from a previous run can't silently feed the next one.

### Commands

```bash
cd contrib/time_series/tools/bench

# 1) Load CB once, then run TSDB-side from the existing data file
./bench.sh load cb            # drops + recreates tsbs_cb_500 on CB
./bench.sh load tsdb          # same on TSDB (auto-drops cpu_tags index)

# 2) Run both sides + print the side-by-side table in one shot
./bench.sh diff \
    --guc enable_parallel=on \
    --guc max_parallel_workers_per_gather=4 \
    --guc time_series.enable_chunk_append=on

# 3) A/B compare the same target under two GUC sets
./bench.sh run cb --out=/tmp/cb_off.tsv --guc enable_parallel=off
./bench.sh run cb --out=/tmp/cb_on.tsv  --guc enable_parallel=on
./bench.sh compare /tmp/cb_off.tsv /tmp/cb_on.tsv
```

GUC handling notes:

- `--guc` is repeatable.  Each value is applied with
  `ALTER DATABASE <db> SET <k> = '<v>'`, so it persists for the duration
  of the bench DB and the GUC list is recorded as a comment line in the
  output TSV.
- GUCs the target doesn't recognise (e.g. `enable_parallel` on TSDB,
  `timescaledb.*` on CB) are skipped with a warning — so a single
  `--guc enable_parallel=on` works for `diff` mode without per-target
  plumbing.
- A `gpconfig + gpstart -ra` is **not** needed: session-level / DB-level
  GUCs reach the QEs because every new TSBS connection inherits the
  database's `ALTER DATABASE SET` list.

Subcommands and defaults are documented in `./bench.sh --help`.  The
common overrides — `--scale`, `--data-cb`, `--data-tsdb`, `--queries`,
`--workers`, `--chunk-time`, `--ts-start`, `--ts-end` — match the
existing IoT bench script.

> **Timestamp pitfall.**  The query generator's `--timestamp-start` /
> `--timestamp-end` must overlap the loaded data's actual time range,
> or queries scan empty ranges and you get implausibly fast / uniform
> latencies dominated by MPP dispatch overhead.  Defaults are
> 2025-01-01..2025-01-04 to match the cached `/tmp/cb_500` /
> `/tmp/tsdb_500` files.

## Usage

### INSERT performance (heap vs time_series, in-process)

```bash
PGPORT=7000 psql -d insert_bench -f insert_compare.sql
```

Creates `insert_bench` if missing.  Three benchmarks: 10K single-row
INSERT loop, 100×100-row VALUES INSERT, and 1M-row INSERT...SELECT.
Each prints elapsed wall time per pass.

### Bulk-load throughput (TSBS, heap vs time_series)

```bash
./load_compare.sh
```

Runs `tsbs_load_cb load cloudberry_ts` twice into fresh databases
(`tsbs_heap`, `tsbs_timeseries`).  Override via env:

```bash
TSBS_LOAD_BIN=/path/to/tsbs_load_cb \
TSBS_DATA_GZ=/path/to/cb_2000.gz \
WORKERS=8 BATCH=2000 CHUNK_TIME=1day \
./load_compare.sh
```

### TSBS query suite — latency (run per-cluster, diff externally)

```bash
./run_tsbs_compare.sh perf cb   /tmp/tsbs_run_cb   cb   7000  gpadmin tsbs_cb   > cb.txt
./run_tsbs_compare.sh perf tsdb /tmp/tsbs_run_tsdb tsdb 15432 gpadmin tsbs_tsdb > tsdb.txt
paste cb.txt tsdb.txt
```

### TSBS query suite — result-equality check (Cloudberry vs TimescaleDB)

Runs each query on both clusters with `--print-responses=true`, strips
banners/timing lines, rounds floats to `FLOAT_DIGITS` decimals (default 4),
sorts rows (TSBS queries lack stable `ORDER BY`), then diffs.  Reports
`OK` / `DIFF` / `MISSING` per query and exits non-zero if any DIFF.

```bash
./run_tsbs_compare.sh verify \
    /tmp/tsbs_run_cb   cb   7000  gpadmin tsbs_cb \
    /tmp/tsbs_run_tsdb tsdb 15432 gpadmin tsbs_tsdb
```

Env overrides:

- `VERIFY_N` — rows per query to fetch (default 5).
- `FLOAT_DIGITS` — float precision the normaliser keeps (default 4).
  Lower it if MPP vs single-segment float-accumulation order makes the
  4th decimal drift.
- `QUERIES` — subset of the DevOps query list.

Per-query `.diff` artefacts are written to a temp dir announced in
the summary line.

The DBs `tsbs_cb` / `tsbs_tsdb` are expected to have been loaded ahead
of time (see `load_compare.sh` and standard TSDB load scripts).

## Notes

- These scripts have no `set -u` / `set -o pipefail` shielding by design;
  they are interactive benchmarks, not CI.
- Output is human-readable text, not JSON.  For CI-style automation,
  parse the wall-time / median-latency lines.
- The benchmarks tolerate disk usage in the tens of GB during runs.
  `df -h` before kicking off `load_compare.sh` at scale ≥ 4000.
