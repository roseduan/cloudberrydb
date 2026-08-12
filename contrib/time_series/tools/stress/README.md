# time_series stress harness

Large-data regression for `time_series`.  Spawns N parallel writers
(INSERT...SELECT or COPY) across one or more identical tables and
checks the final row count + per-worker exit status.  Designed to be
re-run after any feature / refactor that touches the write path so
silent breakages (dropped rows, orphan chunks, smgr races) are caught
early.

The flags below are knobs, not fixed defaults — tune `--workers`,
`--rows-per-worker` / `--data-size`, `--chunk-interval`, and
`--tables` to your machine.  The script itself is portable; the
arguments are what need to be re-thought per environment.

## Usage

```bash
# INSERT...SELECT, 8 workers, 200K rows each
./stress.sh --workers 8 --rows-per-worker 200000 --mode insert

# COPY (CSV via stdin), 16 workers, 500K rows each
./stress.sh --workers 16 --rows-per-worker 500000 --mode copy

# Heavier: 32 workers × 1M rows, 6-hour chunks, keep DB after
./stress.sh --workers 32 --rows-per-worker 1000000 \
            --mode insert --chunk-interval '6 hour' --keep
```

Exit 0 on PASS, non-zero on FAIL.  Plug into a shell loop for fuzz:

```bash
for run in $(seq 1 20); do
    ./stress.sh --workers 8 --rows-per-worker 100000 --mode insert \
        || { echo "FAIL at run $run"; break; }
done
```

## What it checks

| Check | Failure mode it catches |
|---|---|
| Worker exit status | psql ERROR (e.g. "could not read block") |
| Total row count == workers × rows | Silently dropped rows / lost commits |
| No PG ERROR in psql output | Race conditions, smgr failures |
| Chunk catalog populated | INSERT path orphaning chunks |

Throughput / table size / chunk count are printed for trend tracking
but not asserted (run the same args twice, eyeball drift).

## Configuration knobs

| Flag | Default | Notes |
|---|---|---|
| `--workers N` | required | Parallel psql sessions **per table** |
| `--rows-per-worker R` | required | Rows each session writes |
| `--data-size SIZE` | (alt. to rows) | Target on-disk bytes **per table**; rows derived from row-width estimate.  K/M/G/T (1024-based). |
| `--mode insert|copy` | required | INSERT...SELECT vs COPY FROM stdin |
| `--tables N` | 1 | Identical tables written to in parallel.  Each gets `--workers` writers, total parallelism = workers × tables, total volume = data-size × tables.  Named `<table>_<i>` when N>1, bare `<table>` when N=1. |
| `--cols N` | 0 | Extra `metric_N double precision` columns (scales row width). |
| `--toast-bytes N` | 4096 | Length of the per-row `big_text_val` column.  >2032 B exercises TOAST; 0 drops the column. |
| `--port` | `$PGPORT` or 7000 | Cluster port |
| `--db` | ts_stress | Database name (dropped + recreated each run) |
| `--table` | ts_stress | Base table name (suffix added when --tables > 1) |
| `--chunk-interval` | '1 hour' | `ts_chunk_interval` reloption |
| `--drop` | (off) | DROP DATABASE after the run.  Default is **keep** so you can inspect the data / re-run queries. |

## Suggested matrix

To exercise different write paths, rotate through:

```
mode=insert, workers=1,2,4,8,16   rows=100K..1M
mode=copy,   workers=1,2,4,8,16   rows=100K..1M
chunk_interval=10min/1hour/8hour   (affects chunk count)
```

The COPY path stresses `multi_insert` page packing + COPY protocol;
the INSERT...SELECT path stresses the `multi_insert` path + a single
big query.
