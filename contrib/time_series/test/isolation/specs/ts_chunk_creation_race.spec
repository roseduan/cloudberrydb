# ts_chunk_creation_race
#
# Regression for the duplicate-row race in ts_chunk_catalog_insert
# (contrib/time_series/src/access/ts_catalog.c).
#
# Two backends both INSERT the FIRST row of a brand-new chunk at the
# same wall-clock partition.  Without the ShareUpdateExclusiveLock the
# slow-path takes on the user relation, both sessions' "is there a
# row already?" probes use GetTransactionSnapshot, neither sees the
# other's uncommitted INSERT, both call CatalogTupleInsert, and
# ts_chunk ends up with TWO rows for the same (table_oid,
# chunk_number) on the same segment.  Cloudberry forbids UNIQUE on
# DISTRIBUTED RANDOMLY tables, so the catalog itself can't backstop
# this — the slow-path takes a SUEL on the user relation
# (ShareUpdateExclusiveLock self-conflicts) to serialise creators.
#
# Permutation: s1 holds its INSERT in an open xact (slow path takes
# the SUEL); s2 INSERT then waits on SUEL; s1 commits; s2 unblocks,
# rechecks via SnapshotSelf, sees s1's now-committed ts_chunk row,
# and skips the catalog insert.  Final count must be exactly one row
# per segment.

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

# Cluster-wide row counts on each catalog table.
# With the SUEL fix, exactly one ts_chunk row exists per segment that
# took data; aggregated count is one-per-segment-with-data.
# A regression of the fix would surface as count > NSEGS_WITH_DATA.
step c_chunk_rows
{
    SELECT count(*) AS chunk_rows
    FROM gp_dist_random('time_series.ts_chunk')
    WHERE table_oid = 'ts_race'::regclass;
}

# Distinct (table, chunk) pairs across the cluster — must equal
# chunk_rows above.  If they diverge, some segment carries duplicates.
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

# Race scenario: s1 holds the slow-path SUEL across s2's INSERT,
# forcing s2 to wait.  After s1 commits, s2 unblocks and must skip
# the catalog insert (row already created by s1).
permutation s1_begin s1_insert s2_begin s2_insert s1_commit s2_commit
            c_chunk_rows c_distinct_chunks c_data_rows
