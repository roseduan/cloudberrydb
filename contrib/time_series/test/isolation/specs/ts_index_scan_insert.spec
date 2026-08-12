# ts_index_scan_insert
#
# Tests index scan correctness while concurrent inserts are happening.
# Verifies that ts_btree index scans return consistent results
# with respect to snapshot isolation during concurrent writes.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_isi (
        ts  timestamptz NOT NULL,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    CREATE INDEX ON ts_isi USING ts_btree (ts ts_btree_timestamptz_ops);

    /* Pre-load 3 days of data, 100 rows per chunk */
    INSERT INTO ts_isi
    SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
    FROM generate_series(1, 100) i;

    INSERT INTO ts_isi
    SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 200 + i
    FROM generate_series(1, 100) i;

    INSERT INTO ts_isi
    SELECT '2025-01-03'::timestamptz + (i * interval '1 minute'), 300 + i
    FROM generate_series(1, 100) i;
}

teardown
{
    DROP TABLE IF EXISTS ts_isi;
}

session writer
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w_begin	{ BEGIN; }
step w_insert_chunk2
{
    INSERT INTO ts_isi
    SELECT '2025-01-02 12:00:00+00'::timestamptz + (i * interval '1 minute'), 500 + i
    FROM generate_series(1, 100) i;
}
step w_insert_chunk4
{
    INSERT INTO ts_isi
    SELECT '2025-01-04'::timestamptz + (i * interval '1 minute'), 600 + i
    FROM generate_series(1, 100) i;
}
step w_commit	{ COMMIT; }

session reader
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r_count_all	{ SELECT count(*) FROM ts_isi; }
step r_count_day2	{ SELECT count(*) FROM ts_isi WHERE ts >= '2025-01-02' AND ts < '2025-01-03'; }
step r_count_day4	{ SELECT count(*) FROM ts_isi WHERE ts >= '2025-01-04'; }
step r_min_max	{ SELECT min(val), max(val) FROM ts_isi; }

# Index scan on chunk2 while writer inserts into chunk2 (uncommitted)
permutation r_count_day2 w_begin w_insert_chunk2 r_count_day2 w_commit r_count_day2

# Full scan while writer adds a new chunk4 (uncommitted)
permutation r_count_all w_begin w_insert_chunk4 r_count_all r_count_day4 w_commit r_count_all r_count_day4

# Aggregate via index while concurrent inserts
permutation r_min_max w_begin w_insert_chunk2 w_insert_chunk4 r_min_max w_commit r_min_max
