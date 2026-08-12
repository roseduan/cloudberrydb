# ts_insert_rollback
#
# Verify that rolled-back inserts are never visible to other sessions.
# Tests both same-chunk and cross-chunk rollback scenarios.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_rb (
        ts  timestamptz NOT NULL,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
    -- Index AM (ts_btree) disabled; re-add when re-enabled:
    -- CREATE INDEX ON ts_rb USING ts_btree (ts ts_btree_timestamptz_ops);

    INSERT INTO ts_rb
    SELECT '2025-01-01'::timestamptz + (i * interval '1 minute'), i
    FROM generate_series(1, 50) i;
}

teardown
{
    DROP TABLE IF EXISTS ts_rb;
}

session writer
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step w_begin	{ BEGIN; }
step w_insert_chunk1
{
    INSERT INTO ts_rb
    SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'), 1000 + i
    FROM generate_series(1, 50) i;
}
step w_insert_chunk2
{
    INSERT INTO ts_rb
    SELECT '2025-01-02'::timestamptz + (i * interval '1 minute'), 2000 + i
    FROM generate_series(1, 50) i;
}
step w_insert_chunk1b
{
    INSERT INTO ts_rb VALUES ('2025-01-01 06:00:00+00', 100);
}
step w_insert_chunk2b
{
    INSERT INTO ts_rb VALUES ('2025-01-02 06:00:00+00', 200);
}
step w_insert_chunk3
{
    INSERT INTO ts_rb VALUES ('2025-01-03 06:00:00+00', 300);
}
step w_rollback	{ ROLLBACK; }
step w_commit	{ COMMIT; }

session reader
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step r_count	{ SELECT count(*) FROM ts_rb; }
step r_count_chunk2	{ SELECT count(*) FROM ts_rb WHERE ts >= '2025-01-02'; }

# Insert into existing chunk, rollback — count stays at 50
permutation w_begin w_insert_chunk1 r_count w_rollback r_count

# Insert into new chunk, rollback — no chunk2 rows visible
permutation w_begin w_insert_chunk2 r_count r_count_chunk2 w_rollback r_count r_count_chunk2

# Insert into both chunks, rollback — all reverted
permutation w_begin w_insert_chunk1 w_insert_chunk2 r_count w_rollback r_count

# Insert chunk1 commit, then insert chunk2 rollback — only chunk1 visible
permutation w_begin w_insert_chunk1 w_commit r_count w_begin w_insert_chunk2 r_count w_rollback r_count r_count_chunk2

# Two rollbacks to same chunk, then insert — all rows visible (cache invalidation regression)
permutation w_begin w_insert_chunk1 w_insert_chunk2 w_rollback w_begin w_insert_chunk1 w_rollback w_insert_chunk1b w_insert_chunk2b w_insert_chunk3 r_count
