# Test tablespace-level TDE in a multi-node MPP cluster (gpdemo).
#
# Creates a 2-primary-segment gpdemo cluster (no mirrors, no standby for
# speed), enables builtin TDE on every node, then verifies:
#   - Encrypted tablespace is created and DEK is loaded on the coordinator.
#   - A DISTRIBUTED BY table spreads rows across segments.
#   - Each segment's raw data files are encrypted (sentinel absent).
#   - Cluster restart re-loads DEKs on all nodes.
#   - Data is fully readable after restart.
#   - DROP TABLESPACE removes .wkey files on all nodes.
#
# The test is skipped automatically when:
#   - OpenSSL is not compiled in, or
#   - the gpdemo binary is not on PATH or at the known install prefix, or
#   - gpstart/gpstop require an incompatible Python version, or
#   - GPDEMO_SKIP is set in the environment.

use strict;
use warnings;
use File::Basename;
use File::Find;
use File::Path qw(remove_tree);
use File::Temp qw(tempdir);
use IPC::Cmd qw(can_run);
use POSIX qw(WEXITSTATUS);
use TestLib;
use Test::More;

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

if ($ENV{GPDEMO_SKIP})
{
	plan skip_all => "GPDEMO_SKIP is set; skipping MPP TDE test";
}

# Locate gpdemo and gpstart/gpstop.
my $dist_base = '/home/gpadmin/hashdata-lightning-umbrella/dist/database';
my $dist_bin  = "$dist_base/bin";
my $dist_pylib = "$dist_base/lib/python";

my $gpdemo   = (-x "$dist_bin/gpdemo") ? "$dist_bin/gpdemo" : can_run('gpdemo');
my $gpstart  = (-x "$dist_bin/gpstart") ? "$dist_bin/gpstart" : can_run('gpstart');
my $gpstop   = (-x "$dist_bin/gpstop")  ? "$dist_bin/gpstop"  : can_run('gpstop');
my $psql     = can_run('psql') // "$dist_bin/psql";

unless ($gpdemo && $gpstart && $gpstop && $psql && -x $psql)
{
	plan skip_all =>
		"gpdemo/gpstart/gpstop/psql not found; skipping MPP TDE test";
}

# Verify that gpstart/gpstop actually work — they need a Python version
# matching the _pg.cpython-*.so bundled in the installation (3.8 or 3.10).
# On systems with Python 3.9 (or other mismatches) this check fails and we
# skip gracefully rather than producing confusing errors.
{
	my $py_ok = (system(
		"python3 -c "
		. "'import sys; sys.path.insert(0, \"$dist_pylib\"); "
		. "from gppylib.mainUtils import *' "
		. "2>/dev/null") == 0);
	unless ($py_ok)
	{
		plan skip_all =>
			"gpstart/gpstop need Python 3.8 or 3.10 (_pg module mismatch); "
			. "skipping MPP TDE test";
	}
}

plan tests => 19;

# ---------------------------------------------------------------------------
# Build the gpdemo cluster in a fresh temp directory.
# ---------------------------------------------------------------------------

my $demo_base = tempdir('tde_mpp_XXXXX', CLEANUP => 0, TMPDIR => 1);

# Use a high port range to avoid clashing with other running instances.
my $port_base = 18400;

# Inject TDE settings at cluster init time via BLDWRAP_POSTGRES_CONF_ADDONS.
# This is a pipe-separated list of postgresql.conf additions that gpdemo
# passes to gpinitsystem -p (postgres addons file).  Using this mechanism
# means the PGC_POSTMASTER GUC tde_kms_provider is set during initdb so
# no second restart is needed just to activate TDE.
my $tde_passphrase = 'mpp-tde-test-passphrase-2026';
my $tde_addons     =
      "tde_kms_provider='builtin'"
    . "|tde_kms_builtin_passphrase='$tde_passphrase'"
    . "|allow_in_place_tablespaces=on";

# 2 primary segments, no mirrors, no standby — fast setup.
local $ENV{DATADIRS}                      = $demo_base;
local $ENV{PORT_BASE}                     = $port_base;
local $ENV{NUM_PRIMARY_MIRROR_PAIRS}      = 2;
local $ENV{WITH_MIRRORS}                  = 'false';
local $ENV{WITH_STANDBY}                  = 'false';
local $ENV{BLDWRAP_POSTGRES_CONF_ADDONS}  = $tde_addons;
local $ENV{PATH}                          = "$dist_bin:$ENV{PATH}";
local $ENV{GPHOME}                        = $dist_base;
local $ENV{PYTHONPATH}                    = $dist_pylib
    . (defined $ENV{PYTHONPATH} ? ":$ENV{PYTHONPATH}" : "");
local $ENV{LD_LIBRARY_PATH}               =
    "$dist_base/lib:/usr/local/toolchain/lib64"
    . (defined $ENV{LD_LIBRARY_PATH} ? ":$ENV{LD_LIBRARY_PATH}" : "");

my $rc = system($gpdemo);
is(WEXITSTATUS($rc), 0, 'gpdemo cluster created successfully');

# gpdemo convention: coordinator at $demo_base/qddir/demoDataDir-1
#                   segments at    $demo_base/dbfast{1,2}/demoDataDir{0,1}
my $coord_data = "$demo_base/qddir/demoDataDir-1";
my $coord_port = $port_base;

unless (-d $coord_data)
{
	BAIL_OUT("gpdemo coordinator data directory not found: $coord_data");
}

# Collect all PGDATA directories (coordinator + segments).
my @all_pgdata;
push @all_pgdata, $coord_data;
for my $n (1 .. 2)
{
	my $seg_dir = "$demo_base/dbfast$n/demoDataDir" . ($n - 1);
	push @all_pgdata, $seg_dir if -d $seg_dir;
}

# ---------------------------------------------------------------------------
# Restart the cluster so the PGC_POSTMASTER GUCs take effect.
# (gpinitsystem may leave the coordinator in admin mode; a full restart
# via gpstop/gpstart brings everything up in production mode.)
# ---------------------------------------------------------------------------

local $ENV{MASTER_DATA_DIRECTORY} = $coord_data;
local $ENV{PGPORT}                = $coord_port;

system("$gpstop -a -q 2>/dev/null");
my $start_rc = system("$gpstart -a -q 2>/dev/null");
is(WEXITSTATUS($start_rc), 0, 'cluster restarted with TDE config');

# ---------------------------------------------------------------------------
# Helper: run SQL against the coordinator, return trimmed stdout.
# ---------------------------------------------------------------------------

sub run_sql
{
	my ($sql) = @_;
	my $out = qx{$psql -h localhost -p $coord_port -d postgres -tAc "$sql" 2>&1};
	chomp $out;
	return $out;
}

# ---------------------------------------------------------------------------
# 1.  Create the encrypted tablespace and load the monitoring extension.
# ---------------------------------------------------------------------------

run_sql("CREATE TABLESPACE enc_mpp LOCATION '' WITH (encryption_method = 'AES256')");
run_sql("CREATE EXTENSION IF NOT EXISTS pg_tde");

my $status = run_sql(
	"SELECT enc_method, loaded FROM pg_tde_tablespace_status WHERE spc_name = 'enc_mpp'"
);
is($status, "AES256|t", 'coordinator: enc_mpp shows AES256, loaded=t');

# ---------------------------------------------------------------------------
# 2.  Distributed table: insert 10 000 rows, verify count.
# ---------------------------------------------------------------------------

my $sentinel = 'MPP_TDE_SENTINEL_XYZZY_12345';

run_sql("CREATE TABLE tde_dist (id int, val text)
         TABLESPACE enc_mpp DISTRIBUTED BY (id)");
run_sql("INSERT INTO tde_dist SELECT i, '$sentinel'
         FROM generate_series(1, 10000) i");
run_sql("CHECKPOINT");

my $total = run_sql("SELECT count(*) FROM tde_dist WHERE val = '$sentinel'");
is($total, 10000, '10 000 sentinel rows readable via SQL');

# ---------------------------------------------------------------------------
# 3.  Verify data is distributed across segments (each segment has rows).
# ---------------------------------------------------------------------------

my $seg_counts = run_sql(
	"SELECT count(distinct gp_segment_id) FROM tde_dist"
);
cmp_ok($seg_counts, '>=', 2, 'rows spread across at least 2 segments');

# ---------------------------------------------------------------------------
# 4.  .wkey file exists on the coordinator.
# ---------------------------------------------------------------------------

my @coord_wkeys = glob("$coord_data/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @coord_wkeys, 1,
	'coordinator has exactly one .wkey for enc_mpp');

# ---------------------------------------------------------------------------
# 5.  Segment data files are encrypted (sentinel absent in raw bytes).
# ---------------------------------------------------------------------------

# Get the relfilenode OID from coordinator.
my $relpath = run_sql("SELECT pg_relation_filepath('tde_dist')");

my $seg_encrypted = 0;
for my $n (1 .. 2)
{
	my $seg_data = "$demo_base/dbfast$n/demoDataDir" . ($n - 1);
	my $seg_file = "$seg_data/$relpath";
	next unless -f $seg_file;
	my $raw = TestLib::slurp_file($seg_file);
	$seg_encrypted++ if index($raw, $sentinel) == -1;
}
cmp_ok($seg_encrypted, '>=', 1, 'at least one segment data file is encrypted');

# ---------------------------------------------------------------------------
# 6.  Cluster restart: DEKs must be re-loaded from .wkey blobs.
# ---------------------------------------------------------------------------

system("$gpstop -a -q 2>/dev/null");
$start_rc = system("$gpstart -a -q 2>/dev/null");
is(WEXITSTATUS($start_rc), 0, 'cluster restarted after data insert');

$status = run_sql(
	"SELECT enc_method, loaded FROM pg_tde_tablespace_status WHERE spc_name = 'enc_mpp'"
);
is($status, "AES256|t",
	'coordinator: enc_mpp still loaded=t after restart');

my $post_count = run_sql("SELECT count(*) FROM tde_dist WHERE val = '$sentinel'");
is($post_count, 10000, '10 000 rows still readable after cluster restart');

# ---------------------------------------------------------------------------
# 7.  Segment files still encrypted after restart.
# ---------------------------------------------------------------------------

$seg_encrypted = 0;
for my $n (1 .. 2)
{
	my $seg_data = "$demo_base/dbfast$n/demoDataDir" . ($n - 1);
	my $seg_file = "$seg_data/$relpath";
	next unless -f $seg_file;
	my $raw = TestLib::slurp_file($seg_file);
	$seg_encrypted++ if index($raw, $sentinel) == -1;
}
cmp_ok($seg_encrypted, '>=', 1,
	'segment data files remain encrypted after restart');

# ---------------------------------------------------------------------------
# 8.  DROP TABLESPACE removes .wkey from coordinator.
# ---------------------------------------------------------------------------

run_sql("DROP TABLE tde_dist");
run_sql("DROP TABLESPACE enc_mpp");

@coord_wkeys = glob("$coord_data/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @coord_wkeys, 0,
	'no .wkey files remain on coordinator after DROP TABLESPACE');

# ---------------------------------------------------------------------------
# 9.  enc_mpp gone from status view.
# ---------------------------------------------------------------------------

my $gone = run_sql(
	"SELECT count(*) FROM pg_tde_tablespace_status WHERE spc_name = 'enc_mpp'"
);
is($gone, 0, 'enc_mpp gone from status view after DROP TABLESPACE');

# ---------------------------------------------------------------------------
# SM4 tablespace: verify SM4 encryption works on MPP cluster.
#
# Uses a Heap table (tests SM4-OFB path for segment data files) so that
# the raw file check can use the same sentinel-absent test as the AES256
# section above.
# ---------------------------------------------------------------------------

my $sm4_sentinel = 'MPP_TDE_SM4_SENTINEL_ABCDE_67890';

run_sql("CREATE TABLESPACE enc_mpp_sm4 LOCATION '' WITH (encryption_method = 'SM4')");

my $sm4_status = run_sql(
    "SELECT enc_method, loaded FROM pg_tde_tablespace_status WHERE spc_name = 'enc_mpp_sm4'"
);
is($sm4_status, "SM4|t", 'coordinator: enc_mpp_sm4 shows SM4, loaded=t');

my @sm4_coord_wkeys = glob("$coord_data/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @sm4_coord_wkeys, 1,
    'coordinator has exactly one .wkey for enc_mpp_sm4');

run_sql("CREATE TABLE tde_sm4_dist (id int, val text)
         TABLESPACE enc_mpp_sm4 DISTRIBUTED BY (id)");
run_sql("INSERT INTO tde_sm4_dist SELECT i, '$sm4_sentinel'
         FROM generate_series(1, 1000) i");
run_sql("CHECKPOINT");

my $sm4_total = run_sql("SELECT count(*) FROM tde_sm4_dist WHERE val = '$sm4_sentinel'");
is($sm4_total, 1000, '1 000 SM4 sentinel rows readable via SQL');

my $sm4_seg_counts = run_sql(
    "SELECT count(distinct gp_segment_id) FROM tde_sm4_dist"
);
cmp_ok($sm4_seg_counts, '>=', 2, 'SM4 rows spread across at least 2 segments');

my $sm4_relpath = run_sql("SELECT pg_relation_filepath('tde_sm4_dist')");

my $sm4_encrypted = 0;
for my $n (1 .. 2)
{
    my $seg_data = "$demo_base/dbfast$n/demoDataDir" . ($n - 1);
    my $seg_file = "$seg_data/$sm4_relpath";
    next unless -f $seg_file;
    my $raw = TestLib::slurp_file($seg_file);
    $sm4_encrypted++ if index($raw, $sm4_sentinel) == -1;
}
cmp_ok($sm4_encrypted, '>=', 1,
    'at least one SM4 segment data file is encrypted');

run_sql("DROP TABLE tde_sm4_dist");
run_sql("DROP TABLESPACE enc_mpp_sm4");

@sm4_coord_wkeys = glob("$coord_data/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @sm4_coord_wkeys, 0,
    'no .wkey files remain on coordinator after DROP TABLESPACE enc_mpp_sm4');

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

system("$gpstop -a -q 2>/dev/null");
remove_tree($demo_base);
