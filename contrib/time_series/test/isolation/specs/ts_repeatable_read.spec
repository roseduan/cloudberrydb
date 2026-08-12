# ts_repeatable_read
#
# Tests REPEATABLE READ isolation level with time_series.
# A reader in a repeatable-read transaction must see a consistent
# snapshot throughout, even as concurrent writers commit new data.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_rr (
        ts  timestamptz NOT NULL,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_rr USING ts_btree (ts ts_btree_timestamptz_ops);

    INSERT INTO ts_rr
    SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
    FROM generate_series(1, 50) i;
}

teardown
{
    DROP TABLE IF EXISTS ts_rr;
}

session writer1
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w1_insert
{
    INSERT INTO ts_rr
    SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'), 100 + i
    FROM generate_series(1, 50) i;
}

session writer2
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w2_insert
{
    INSERT INTO ts_rr
    SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 200 + i
    FROM generate_series(1, 50) i;
}

session reader
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r_begin_rr	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step r_count	{ SELECT count(*) FROM ts_rr; }
step r_count_day2	{ SELECT count(*) FROM ts_rr WHERE ts >= '2025-01-02'; }
step r_commit	{ COMMIT; }

# Reader starts RR txn seeing 50 rows. Writer1 commits 50 more into
# same chunk — reader still sees 50. Writer2 commits 50 into new
# chunk — reader still sees 50. After reader commits and re-reads,
# it sees all 150.
permutation r_begin_rr r_count w1_insert r_count w2_insert r_count r_count_day2 r_commit r_count r_count_day2
