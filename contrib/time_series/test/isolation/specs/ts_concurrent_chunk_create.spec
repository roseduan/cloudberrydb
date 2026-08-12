# ts_concurrent_chunk_create
#
# Regression for the "concurrent INSERT to brand-new chunk" scenario
# under the duplicate-tolerant chunk-creation model.
#
# Background.  Two concurrent INSERTs targeting the FIRST row of a
# brand-new chunk both run the slow path.  In the previous design we
# used ShareUpdateExclusiveLock on the user relation to serialise them
# and guarantee at most one ts_chunk row per (table, chunk).  Under
# Cloudberry MPP this SUEL was xact-scoped and held across the long
# COPY xact, so multiple master sessions formed cross-segment lock
# cycles the local deadlock detector couldn't see — permanent stall.
#
# Current design (commit-this-change): the slow path holds no
# xact-scoped lock on the user relation.  Two concurrent INSERTs are
# free to both insert their own ts_chunk row for the same
# (table_oid, chunk_number); the row update / lock paths are written
# to tolerate duplicate rows (update iterates all matching rows;
# lock_if_compressed picks the min-ctid canonical row).  Brief locks
# protect the smgrcreate / P_NEW extension races.
#
# Properties verified by this permutation:
#   1. s2_insert does NOT block on s1's open xact (no SUEL).  Output
#      shows the step executing without a <waiting ...> marker.
#   2. After both commit, both data rows are visible (data preserved).
#   3. The chunk is logically unique: distinct (gp_segment_id,
#      chunk_number) pairs == 1.
#   4. ts_chunk catalog may contain duplicate rows (chunk_rows = 2 in
#      this permutation, because both backends pass their SnapshotSelf
#      check and INSERT their own row).  The expected output asserts
#      this exact count so a future regression that re-introduces SUEL
#      (or any other serialization that would make chunk_rows = 1)
#      shows up clearly in the diff.

setup
{
    CREATE EXTENSION IF NOT EXISTS time_series;
    SET search_path = public, time_series;
    SET timezone = 'UTC';
    CREATE TABLE ts_race (
        ts  timestamptz NOT NULL,
        sid int,
        val int
    ) USING time_series WITH (
        ts_partition_column = 'ts', ts_chunk_interval = '1 day',
        ts_chunk_origin = '2025-01-01'
    ) DISTRIBUTED BY (ts);
}

teardown
{
    DROP TABLE IF EXISTS ts_race;
}

session s1
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s1_begin	{ BEGIN; }
step s1_insert	{ INSERT INTO ts_race VALUES ('2025-06-15 12:00:00+00', 1, 1); }
step s1_commit	{ COMMIT; }

session s2
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step s2_begin	{ BEGIN; }
step s2_insert	{ INSERT INTO ts_race VALUES ('2025-06-15 12:00:00+00', 2, 2); }
step s2_commit	{ COMMIT; }

session checker
setup	{ SET search_path = public, time_series; SET timezone = 'UTC'; }

step c_chunk_rows
{
    SELECT count(*) AS chunk_rows
    FROM gp_dist_random('time_series.ts_chunk')
    WHERE table_oid = 'ts_race'::regclass;
}

step c_distinct_chunks
{
    SELECT count(*) AS distinct_chunks
    FROM (
        SELECT DISTINCT gp_segment_id, chunk_number
        FROM gp_dist_random('time_series.ts_chunk')
        WHERE table_oid = 'ts_race'::regclass
    ) t;
}

step c_data_rows	{ SELECT count(*) AS data_rows FROM ts_race; }

# Both INSERTs target the same brand-new chunk (DISTRIBUTED BY (ts)
# with identical ts hashes them to the same segment).  s2 must NOT
# wait on s1; both proceed concurrently; both insert their own
# ts_chunk row → chunk_rows = 2, distinct_chunks = 1, data_rows = 2.
permutation s1_begin s1_insert s2_begin s2_insert s1_commit s2_commit
            c_chunk_rows c_distinct_chunks c_data_rows
