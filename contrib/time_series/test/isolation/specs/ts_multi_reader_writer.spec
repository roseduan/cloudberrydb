# ts_multi_reader_writer
#
# Multiple readers query the table while a writer adds data.
# Verifies that concurrent readers each see a consistent snapshot
# and that reads do not block writes or vice versa.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_mrw (
        ts  timestamptz NOT NULL,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_mrw USING ts_btree (ts ts_btree_timestamptz_ops);

    INSERT INTO ts_mrw
    SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
    FROM generate_series(1, 100) i;
}

teardown
{
    DROP TABLE IF EXISTS ts_mrw;
}

session writer
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w_begin	{ BEGIN; }
step w_insert_chunk1
{
    INSERT INTO ts_mrw
    SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'), 500 + i
    FROM generate_series(1, 100) i;
}
step w_insert_chunk2
{
    INSERT INTO ts_mrw
    SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 1000 + i
    FROM generate_series(1, 100) i;
}
step w_commit	{ COMMIT; }

session reader1
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r1_begin_rr	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step r1_count		{ SELECT count(*) FROM ts_mrw; }
step r1_sum		{ SELECT sum(val) FROM ts_mrw; }
step r1_commit		{ COMMIT; }

session reader2
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r2_count		{ SELECT count(*) FROM ts_mrw; }
step r2_max		{ SELECT max(val) FROM ts_mrw; }

# Two readers see initial 100 rows. Writer inserts 100 more (uncommitted).
# reader1 (RR) still sees 100, reader2 (auto-commit) also sees 100.
# After commit, reader1 (still in RR) sees 100, reader2 sees 200.
permutation r1_begin_rr r1_count r2_count w_begin w_insert_chunk1 r1_count r2_count w_commit r1_count r2_count r1_commit r1_count

# Writer adds to new chunk. Both readers do aggregations.
permutation r1_begin_rr r1_sum w_begin w_insert_chunk2 r2_max r1_sum w_commit r2_max r1_sum r1_commit r1_sum

# Interleaved reads during multi-chunk inserts
permutation w_begin w_insert_chunk1 r1_count r2_count w_insert_chunk2 r1_count r2_count w_commit r1_count r2_count
