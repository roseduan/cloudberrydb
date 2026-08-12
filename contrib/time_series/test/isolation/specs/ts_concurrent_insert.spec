# ts_concurrent_insert
#
# Two sessions insert rows into the same time_series table concurrently.
# Tests both same-chunk and cross-chunk concurrent inserts.
# Verifies no data loss after concurrent operations.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_conc (
        ts  timestamptz NOT NULL,
        sid int,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_conc USING ts_btree (ts ts_btree_timestamptz_ops);
}

teardown
{
    DROP TABLE IF EXISTS ts_conc;
}

session s1
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s1_insert_chunk1
{
    INSERT INTO ts_conc
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 100) i;
}

step s1_insert_chunk2
{
    INSERT INTO ts_conc
    SELECT '2025-01-02 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 100) i;
}

step s1_count	{ SELECT count(*) FROM ts_conc WHERE sid = 1; }

session s2
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s2_insert_chunk1
{
    INSERT INTO ts_conc
    SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'),
           2, i
    FROM generate_series(1, 100) i;
}

step s2_insert_chunk2
{
    INSERT INTO ts_conc
    SELECT '2025-01-02 12:00:00+00'::timestamptz + (i * interval '1 minute'),
           2, i
    FROM generate_series(1, 100) i;
}

step s2_count	{ SELECT count(*) FROM ts_conc WHERE sid = 2; }

# Both sessions insert into the SAME chunk concurrently
permutation s1_insert_chunk1 s2_insert_chunk1 s1_count s2_count

# Both sessions insert into DIFFERENT chunks concurrently
permutation s1_insert_chunk1 s2_insert_chunk2 s1_count s2_count

# Interleaved inserts across multiple chunks
permutation s1_insert_chunk1 s2_insert_chunk1 s1_insert_chunk2 s2_insert_chunk2 s1_count s2_count
