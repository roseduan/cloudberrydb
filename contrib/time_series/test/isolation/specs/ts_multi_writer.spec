# ts_multi_writer
#
# Three concurrent writers inserting into time_series table simultaneously.
# Tests contention under higher concurrency with overlapping and
# non-overlapping chunk targets.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_mw (
        ts  timestamptz NOT NULL,
        sid int,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_mw USING ts_btree (ts ts_btree_timestamptz_ops);
}

teardown
{
    DROP TABLE IF EXISTS ts_mw;
}

session s1
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s1_insert
{
    INSERT INTO ts_mw
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           1, i
    FROM generate_series(1, 200) i;
}
step s1_count	{ SELECT count(*) FROM ts_mw WHERE sid = 1; }

session s2
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s2_insert_same
{
    INSERT INTO ts_mw
    SELECT '2025-01-01 06:00:00+00'::timestamptz + (i * interval '1 minute'),
           2, i
    FROM generate_series(1, 200) i;
}
step s2_insert_diff
{
    INSERT INTO ts_mw
    SELECT '2025-01-02 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           2, i
    FROM generate_series(1, 200) i;
}
step s2_count	{ SELECT count(*) FROM ts_mw WHERE sid = 2; }

session s3
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s3_insert_same
{
    INSERT INTO ts_mw
    SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'),
           3, i
    FROM generate_series(1, 200) i;
}
step s3_insert_diff
{
    INSERT INTO ts_mw
    SELECT '2025-01-03 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           3, i
    FROM generate_series(1, 200) i;
}
step s3_count	{ SELECT count(*) FROM ts_mw WHERE sid = 3; }

step s3_total	{ SELECT count(*) FROM ts_mw; }

# All three writers target the same chunk
permutation s1_insert s2_insert_same s3_insert_same s1_count s2_count s3_count s3_total

# Each writer targets a different chunk
permutation s1_insert s2_insert_diff s3_insert_diff s1_count s2_count s3_count s3_total

# Mixed: s1 and s2 same chunk, s3 different chunk
permutation s1_insert s2_insert_same s3_insert_diff s1_count s2_count s3_count s3_total
