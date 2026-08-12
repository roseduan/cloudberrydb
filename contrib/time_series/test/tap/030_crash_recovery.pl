
# Copyright (c) 2025, HashData Technology Limited
#
# Crash recovery / WAL replay correctness for the time_series storage AM.
#
# Exercises `pg_ctl stop -m immediate` at multiple points in the chunk
# lifecycle:
#   1. INSERT only           — WAL replay must restore fork files + rows
#   2. compress_chunks       — PAX file + ts_compressed_chunk row must persist
#   3. reclaim_chunk_heaps   — heap fork must stay truncated post-crash
#   4. INSERT into reclaimed → PARTIAL — fork extension after truncate must replay
#
# Runs on a single-node PG cluster spun up by PostgresNode (no MPP);
# tests the storage AM's WAL records, not distributed dispatch.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

plan tests => 22;

# ----------------------------------------------------------------------
# Cluster setup
# ----------------------------------------------------------------------
my $node = get_new_node('ts_crash');
$node->init();
$node->append_conf('postgresql.conf', <<EOF
shared_preload_libraries = 'time_series'
max_wal_size = '1GB'
log_statement = 'none'
EOF
);
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION time_series');
$node->safe_psql('postgres', "SET timezone = 'UTC'");

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
sub row_count {
    my ($t) = @_;
    return $node->safe_psql('postgres', "SELECT count(*) FROM $t");
}

sub chunk_count_with_status {
    my ($t, $status) = @_;
    return $node->safe_psql('postgres', qq{
        SELECT count(DISTINCT chunk_number)
        FROM time_series.ts_chunk
        WHERE table_oid='$t'::regclass AND status = $status
    });
}

sub crash_and_restart {
    my ($label) = @_;
    diag("crash+restart: $label");
    $node->stop('immediate');
    $node->start;
}

# ----------------------------------------------------------------------
# Scenario 1: INSERT only, then crash
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    CREATE TABLE ts_c1 (ts timestamptz, k int) USING time_series
    WITH (ts_partition_column='ts',
          ts_chunk_interval='1 day',
          ts_chunk_origin='2025-01-01');
});

$node->safe_psql('postgres', q{
    INSERT INTO ts_c1
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' hours')::interval, i
    FROM generate_series(0, 71) i;
});

is(row_count('ts_c1'), '72', 'S1: 72 rows visible before crash');

my $chunks_before = $node->safe_psql('postgres', qq{
    SELECT count(DISTINCT chunk_number)
    FROM time_series.ts_chunk WHERE table_oid='ts_c1'::regclass
});
is($chunks_before, '3', 'S1: 3 chunks created before crash');

crash_and_restart('after INSERT');

is(row_count('ts_c1'), '72', 'S1: 72 rows survive crash');
is($node->safe_psql('postgres', qq{
    SELECT count(DISTINCT chunk_number)
    FROM time_series.ts_chunk WHERE table_oid='ts_c1'::regclass
}), '3', 'S1: 3 chunks survive crash');

# ----------------------------------------------------------------------
# Scenario 2: compress_chunks, then crash
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    SELECT time_series.set_compress_config('ts_c1'::regclass, NULL, 'ts');
    SELECT time_series.compress_chunks('ts_c1'::regclass);
});

is(chunk_count_with_status('ts_c1', 1), '3',
    'S2: 3 chunks COMPRESSED before crash');

crash_and_restart('after compress');

is(row_count('ts_c1'), '72', 'S2: 72 rows readable after compress+crash');
is(chunk_count_with_status('ts_c1', 1), '3',
    'S2: 3 chunks still COMPRESSED after crash');

# ----------------------------------------------------------------------
# Scenario 3: reclaim_chunk_heaps, then crash
# ----------------------------------------------------------------------
$node->safe_psql('postgres',
    "SELECT time_series.reclaim_chunk_heaps('ts_c1'::regclass)");

crash_and_restart('after reclaim');

is(row_count('ts_c1'), '72', 'S3: 72 rows readable after reclaim+crash');

# Spot-check rows scattered across chunks — confirms PAX files intact.
my $spot = $node->safe_psql('postgres',
    'SELECT string_agg(k::text, \',\' ORDER BY k) FROM ts_c1 WHERE k IN (0, 23, 24, 47, 48, 71)');
is($spot, '0,23,24,47,48,71',
    'S3: spot-checked rows across chunks intact after reclaim+crash');

# ----------------------------------------------------------------------
# Scenario 4: INSERT into reclaimed (truncated) chunk → PARTIAL → crash
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    INSERT INTO ts_c1 VALUES
        ('2025-01-01 18:00+00', 1001),
        ('2025-01-02 18:00+00', 1002),
        ('2025-01-03 18:00+00', 1003);
});

is(row_count('ts_c1'), '75', 'S4: 75 rows after late-INSERT into reclaimed chunks');
is(chunk_count_with_status('ts_c1', 2), '3',
    'S4: 3 chunks flipped to PARTIAL before crash');

crash_and_restart('after PARTIAL insert');

is(row_count('ts_c1'), '75', 'S4: 75 rows survive PARTIAL crash');
is(chunk_count_with_status('ts_c1', 2), '3',
    'S4: 3 chunks still PARTIAL after crash');

my $late = $node->safe_psql('postgres',
    'SELECT count(*) FROM ts_c1 WHERE k >= 1001');
is($late, '3', 'S4: late-INSERT rows visible after crash');

# ----------------------------------------------------------------------
# Scenario 5: large multi-chunk INSERT, then crash → big WAL replay
#
# The existing scenarios 1-4 use 3 chunks.  Replay correctness can
# regress in subtle ways once the WAL stream contains many fork-create
# records back-to-back.  This scenario creates 50 chunks in one INSERT
# and verifies every chunk's heap fork is restored.
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    CREATE TABLE ts_big_recovery (ts timestamptz, k int) USING time_series
    WITH (ts_partition_column='ts',
          ts_chunk_interval='1 hour',
          ts_chunk_origin='2025-01-01');

    INSERT INTO ts_big_recovery
    SELECT '2025-01-01'::timestamptz + (h || ' hours')::interval, h * 100 + i
    FROM generate_series(0, 49) h, generate_series(0, 99) i;
});

is(row_count('ts_big_recovery'), '5000',
    'S5: 5000 rows before big-WAL crash');
my $big_chunks_before = $node->safe_psql('postgres', qq{
    SELECT count(DISTINCT chunk_number) FROM time_series.ts_chunk
    WHERE table_oid='ts_big_recovery'::regclass
});
is($big_chunks_before, '50', 'S5: 50 chunks before crash');

crash_and_restart('after 50-chunk INSERT');

is(row_count('ts_big_recovery'), '5000',
    'S5: 5000 rows survive big-WAL replay');
my $big_chunks_after = $node->safe_psql('postgres', qq{
    SELECT count(DISTINCT chunk_number) FROM time_series.ts_chunk
    WHERE table_oid='ts_big_recovery'::regclass
});
is($big_chunks_after, '50', 'S5: 50 chunks survive big-WAL replay');

# ----------------------------------------------------------------------
# Scenario 6: crash between compress and reclaim
#
# compress_chunks() uses PreventInTransactionBlock, so it commits as
# its own atomic xact.  But the compress_and_reclaim PROCEDURE has a
# COMMIT between compress and reclaim; a crash at that mid-point leaves
# the table in "COMPRESSED, heap forks not yet truncated" state.  After
# restart, reads must still work and a fresh reclaim_chunk_heaps must
# complete cleanly.
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    SELECT time_series.set_compress_config('ts_big_recovery', NULL, 'ts');
    SELECT time_series.compress_chunks('ts_big_recovery');
});

is(chunk_count_with_status('ts_big_recovery', 1), '50',
    'S6: 50 chunks COMPRESSED post-compress, pre-reclaim');

crash_and_restart('between compress and reclaim');

is(chunk_count_with_status('ts_big_recovery', 1), '50',
    'S6: 50 chunks still COMPRESSED after intra-pipeline crash');
is(row_count('ts_big_recovery'), '5000',
    'S6: 5000 rows readable from PAX after crash');

$node->safe_psql('postgres',
    "SELECT time_series.reclaim_chunk_heaps('ts_big_recovery')");
is(row_count('ts_big_recovery'), '5000',
    'S6: 5000 rows still readable after post-recovery reclaim');

# ----------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
    DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_c1'::regclass;
    DELETE FROM time_series.ts_compress_config WHERE table_oid='ts_c1'::regclass;
    DROP TABLE ts_c1;

    DELETE FROM time_series.ts_compressed_chunk
        WHERE table_oid='ts_big_recovery'::regclass;
    DELETE FROM time_series.ts_compress_config
        WHERE table_oid='ts_big_recovery'::regclass;
    DROP TABLE ts_big_recovery;
});

$node->stop;
done_testing();
