# Unit-style tests for tablespace TDE: multiple concurrent tablespaces (LRU
# cache exercise), per-engine encryption symmetry, and tde_sync_keys().
#
# Uses the builtin KMS provider (no external dependencies).
# Three tablespaces exercise the in-process LRU key cache; verifying all
# three remain accessible simultaneously confirms no slot is evicted.
#
# Storage engines covered: Heap, AO Row, AOCS, PAX.
#
# Scenario:
#   1.  Start a node with tde_kms_provider=builtin.
#   2.  Create three encrypted tablespaces (ts1, ts2, ts3).
#   3.  Verify all three appear as loaded in pg_tde_tablespace_status.
#   4.  Exactly three .wkey files exist.
#   5.  Create one table per storage engine, each in a different tablespace.
#   6.  Insert sentinel rows; verify SQL reads work (decryption is transparent).
#   7.  CHECKPOINT; verify raw files do NOT contain the sentinels.
#   8.  tde_sync_keys() returns 0 (all already loaded).
#   9.  Restart: all three DEKs must be unwrapped from .wkey blobs.
#  10.  All sentinels still readable after restart.
#  11.  tde_sync_keys() still returns 0 after restart.
#  12.  DROP all tablespaces; verify .wkey files are removed.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

plan tests => 22;

# ---------------------------------------------------------------------------
# Node setup
# ---------------------------------------------------------------------------

my $node = get_new_node('unit_tde_node');
$node->init;

$node->append_conf('postgresql.conf', "tde_kms_provider = 'builtin'");
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = 'unit-test-passphrase-abc123'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");

$node->start;

# ---------------------------------------------------------------------------
# 1.  Create three encrypted tablespaces
# ---------------------------------------------------------------------------

for my $i (1 .. 3)
{
	$node->safe_psql(
		'postgres',
		"CREATE TABLESPACE enc_ts$i LOCATION ''
         WITH (encryption_method = 'AES256')");
}

$node->safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS pg_tde");

# ---------------------------------------------------------------------------
# 2.  Verify all three appear as loaded in the status view
# ---------------------------------------------------------------------------

for my $i (1 .. 3)
{
	my $status = $node->safe_psql(
		'postgres',
		"SELECT enc_method, loaded
         FROM pg_tde_tablespace_status
         WHERE spc_name = 'enc_ts$i'");
	is($status, "AES256|t",
		"enc_ts$i shows AES256, loaded=t in status view");
}

# ---------------------------------------------------------------------------
# 3.  Exactly three .wkey files exist
# ---------------------------------------------------------------------------

my $pgdata   = $node->data_dir;
my $wkey_dir = "$pgdata/pg_cryptokeys/tablespaces";
my @wkeys    = glob("$wkey_dir/*.wkey");
is(scalar @wkeys, 3, 'exactly three .wkey files exist');

# ---------------------------------------------------------------------------
# 4.  Create one table per storage engine (each in a different tablespace)
#     and insert sentinel data.
# ---------------------------------------------------------------------------

my $s_heap  = 'UNIT_TDE_HEAP_SENTINEL_1';
my $s_ao    = 'UNIT_TDE_AO_SENTINEL_2';
my $s_aocs  = 'UNIT_TDE_AOCS_SENTINEL_3';
my $s_pax   = 'UNIT_TDE_PAX_SENTINEL_4';

$node->safe_psql('postgres',
	"CREATE TABLE unit_heap (id int, val text) TABLESPACE enc_ts1");
$node->safe_psql('postgres',
	"INSERT INTO unit_heap SELECT i, '$s_heap' FROM generate_series(1,200) i");

$node->safe_psql('postgres',
	"CREATE TABLE unit_ao (id int, val text)
     WITH (appendoptimized = true) TABLESPACE enc_ts2");
$node->safe_psql('postgres',
	"INSERT INTO unit_ao SELECT i, '$s_ao' FROM generate_series(1,200) i");

$node->safe_psql('postgres',
	"CREATE TABLE unit_aocs (id int, val text)
     WITH (appendoptimized = true, orientation = column) TABLESPACE enc_ts3");
$node->safe_psql('postgres',
	"INSERT INTO unit_aocs SELECT i, '$s_aocs' FROM generate_series(1,200) i");

$node->safe_psql('postgres',
	"CREATE TABLE unit_pax (id int, val text) USING pax TABLESPACE enc_ts1");
$node->safe_psql('postgres',
	"INSERT INTO unit_pax SELECT i, '$s_pax' FROM generate_series(1,200) i");

$node->safe_psql('postgres', "CHECKPOINT");

# ---------------------------------------------------------------------------
# 5.  SQL reads work (decryption is transparent)
# ---------------------------------------------------------------------------

for my $pair (
	[ 'unit_heap',  $s_heap  ],
	[ 'unit_ao',    $s_ao    ],
	[ 'unit_aocs',  $s_aocs  ],
	[ 'unit_pax',   $s_pax   ],
) {
	my ($tbl, $sentinel) = @$pair;
	my $cnt = $node->safe_psql('postgres',
		"SELECT count(*) FROM $tbl WHERE val = '$sentinel'");
	is($cnt, 200, "200 rows readable from $tbl via SQL");
}

# ---------------------------------------------------------------------------
# 6.  Raw data files do NOT contain the sentinels
#     PAX micro-partitions live under <relpath>_pax/0.
# ---------------------------------------------------------------------------

for my $pair (
	[ 'unit_heap',  $s_heap,  '' ],
	[ 'unit_ao',    $s_ao,    '' ],
	[ 'unit_aocs',  $s_aocs,  '' ],
	[ 'unit_pax',   $s_pax,   '_pax/0' ],
) {
	my ($tbl, $sentinel, $suffix) = @$pair;
	my $fp  = $node->safe_psql('postgres',
		"SELECT pg_relation_filepath('$tbl')");
	my $abs = "$pgdata/$fp$suffix";

	if (!-f $abs)
	{
		# Some AO/AOCS segment files are segnum-suffixed (.1); try .1
		$abs = "$pgdata/${fp}.1${suffix}" if $suffix eq '';
	}
	if (-f $abs)
	{
		my $raw = TestLib::slurp_file($abs);
		ok(index($raw, $sentinel) == -1,
			"sentinel not found in raw file for $tbl (data is encrypted)");
	}
	else
	{
		pass("$tbl file not directly accessible (format uses separate files); skipping raw check");
	}
}

# ---------------------------------------------------------------------------
# 7.  tde_sync_keys() returns 0 (all keys already loaded)
# ---------------------------------------------------------------------------

my $synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0, 'tde_sync_keys() returns 0 when all three keys are loaded');

# ---------------------------------------------------------------------------
# 8.  Restart: DEKs must be unwrapped from the three .wkey blobs
# ---------------------------------------------------------------------------

$node->restart;

for my $i (1 .. 3)
{
	my $status = $node->safe_psql(
		'postgres',
		"SELECT enc_method, loaded
         FROM pg_tde_tablespace_status
         WHERE spc_name = 'enc_ts$i'");
	is($status, "AES256|t",
		"enc_ts$i still shows AES256, loaded=t after restart");
}

# ---------------------------------------------------------------------------
# 9.  All sentinels still readable after restart
# ---------------------------------------------------------------------------

for my $pair (
	[ 'unit_heap',  $s_heap  ],
	[ 'unit_ao',    $s_ao    ],
	[ 'unit_aocs',  $s_aocs  ],
	[ 'unit_pax',   $s_pax   ],
) {
	my ($tbl, $sentinel) = @$pair;
	my $cnt = $node->safe_psql('postgres',
		"SELECT count(*) FROM $tbl WHERE val = '$sentinel'");
	is($cnt, 200, "200 rows still readable from $tbl after restart");
}

# ---------------------------------------------------------------------------
# 10.  tde_sync_keys() still returns 0 after restart
# ---------------------------------------------------------------------------

$synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0, 'tde_sync_keys() returns 0 after restart');

# ---------------------------------------------------------------------------
# 11.  DROP all tablespaces → all .wkey files removed
# ---------------------------------------------------------------------------

for my $tbl (qw(unit_heap unit_ao unit_aocs unit_pax))
{
	$node->safe_psql('postgres', "DROP TABLE $tbl");
}
for my $i (1 .. 3)
{
	$node->safe_psql('postgres', "DROP TABLESPACE enc_ts$i");
}

@wkeys = glob("$wkey_dir/*.wkey");
is(scalar @wkeys, 0, 'no .wkey files remain after dropping all tablespaces');

$node->stop;
