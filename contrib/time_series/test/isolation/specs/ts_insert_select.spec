# ts_insert_select
#
# One session inserts rows while another reads from the same time_series table.
# Verifies snapshot isolation: uncommitted inserts are not visible to readers.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_is (
        ts  timestamptz NOT NULL,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_is USING ts_btree (ts ts_btree_timestamptz_ops);

    INSERT INTO ts_is
    SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
    FROM generate_series(1, 50) i;
}

teardown
{
    DROP TABLE IF EXISTS ts_is;
}

session writer
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w_begin	{ BEGIN; }
step w_insert
{
    INSERT INTO ts_is
    SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 1000 + i
    FROM generate_series(1, 50) i;
}
step w_commit	{ COMMIT; }

session reader
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r_count_all		{ SELECT count(*) FROM ts_is; }
step r_count_chunk2		{ SELECT count(*) FROM ts_is WHERE ts >= '2025-01-02'; }

# Reader sees 50 initial rows, writer inserts in transaction,
# reader still sees only 50 until commit, then sees 100.
permutation r_count_all w_begin w_insert r_count_all r_count_chunk2 w_commit r_count_all r_count_chunk2
