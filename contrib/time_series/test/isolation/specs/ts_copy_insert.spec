# ts_copy_insert
#
# Tests concurrent COPY and INSERT operations on time_series.
# Verifies that bulk COPY and regular INSERT can coexist
# without data loss or visibility issues.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_ci (
        ts  timestamptz NOT NULL,
        sid int,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_ci USING ts_btree (ts ts_btree_timestamptz_ops);
}

teardown
{
    DROP TABLE IF EXISTS ts_ci;
}

session inserter
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step i_insert_chunk1
{
    INSERT INTO ts_ci
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 200) i;
}
step i_insert_chunk2
{
    INSERT INTO ts_ci
    SELECT '2025-01-02 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 200) i;
}
step i_count	{ SELECT count(*) FROM ts_ci WHERE sid = 1; }

session copier
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step c_copy_chunk1
{
    COPY (
        SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'),
               2, i
        FROM generate_series(1, 200) i
    ) TO '/tmp/ts_ci_copy.csv' WITH (FORMAT csv);
    COPY ts_ci FROM '/tmp/ts_ci_copy.csv' WITH (FORMAT csv);
}
step c_copy_chunk2
{
    COPY (
        SELECT '2025-01-02 12:00:00+00'::timestamptz + (i * interval '1 minute'),
               2, i
        FROM generate_series(1, 200) i
    ) TO '/tmp/ts_ci_copy2.csv' WITH (FORMAT csv);
    COPY ts_ci FROM '/tmp/ts_ci_copy2.csv' WITH (FORMAT csv);
}
step c_count	{ SELECT count(*) FROM ts_ci WHERE sid = 2; }

# COPY and INSERT into same chunk concurrently
permutation i_insert_chunk1 c_copy_chunk1 i_count c_count

# COPY and INSERT into different chunks concurrently
permutation i_insert_chunk1 c_copy_chunk2 i_count c_count

# Both operations across multiple chunks
permutation i_insert_chunk1 c_copy_chunk1 i_insert_chunk2 c_copy_chunk2 i_count c_count
