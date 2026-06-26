# Test tablespace-level TDE with the local_cmd KMS provider.
#
# The local_cmd provider delegates KEK retrieval to a shell command
# (tde_kms_command).  The command must write exactly 64 lowercase hex
# characters (a 256-bit KEK) to stdout and exit 0.  The DEK is then
# wrapped / unwrapped locally with AES-256-KWP, just like the builtin
# provider.
#
# Scenario:
#   1.  Write a shell script that outputs a fixed KEK.
#   2.  Start a node with tde_kms_provider='local_cmd' pointing at the script.
#   3.  CREATE TABLESPACE enc_cmd (AES256); verify status + raw-file encryption.
#   4.  Restart: DEK must be reloaded by re-invoking the script.
#   5.  Replace the script with one that returns a wrong KEK; restart.
#       The server must start up but the tablespace must be absent from
#       pg_tde_tablespace_status (unwrap silently failed).
#   6.  tde_sync_keys() with the wrong script still returns 0.
#   7.  Restore the correct script; tde_sync_keys() returns 1 (key loaded
#       without a full restart).
#   8.  Data is readable again after the sync-only restore.
#   9.  DROP TABLESPACE removes the .wkey file.

use strict;
use warnings;
use Cwd qw(abs_path);
use PostgresNode;
use TestLib;
use Test::More;

unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

plan tests => 14;

# ---------------------------------------------------------------------------
# Prepare KEK scripts inside the test's own temp area.
# We write them into the node's data directory after init so the path is
# known and contains no spaces.
# ---------------------------------------------------------------------------

# Fixed 256-bit KEK expressed as 64 lowercase hex chars.
my $kek_correct = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
my $kek_wrong   = 'fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210';

# ---------------------------------------------------------------------------
# Node setup
# ---------------------------------------------------------------------------

my $node = get_new_node('tde_local_cmd');
$node->init;

# Use an absolute path: PostgreSQL runs tde_kms_command via popen() from its
# own working directory, so a relative path stored in postgresql.conf would
# not resolve correctly.
my $pgdata     = abs_path($node->data_dir);
my $script_dir = "$pgdata/kek_scripts";
mkdir $script_dir or die "mkdir $script_dir: $!";
my $kek_script = "$script_dir/get_kek.sh";

# Write the initial (correct) script.
_write_kek_script($kek_script, $kek_correct);

$node->append_conf('postgresql.conf', "tde_kms_provider   = 'local_cmd'");
$node->append_conf('postgresql.conf', "tde_kms_command    = '$kek_script'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");

$node->start;

# ---------------------------------------------------------------------------
# 1.  Create encrypted tablespace; check status view.
# ---------------------------------------------------------------------------

$node->safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS pg_tde");
$node->safe_psql(
	'postgres',
	"CREATE TABLESPACE enc_cmd LOCATION ''
	 WITH (encryption_method = 'AES256')");

my $status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
	 FROM pg_tde_tablespace_status
	 WHERE spc_name = 'enc_cmd'");
is($status, "AES256|t",
	'enc_cmd shows AES256, loaded=t after local_cmd CREATE TABLESPACE');

# ---------------------------------------------------------------------------
# 2.  Verify .wkey file was created.
# ---------------------------------------------------------------------------

my @wkeys = glob("$pgdata/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @wkeys, 1, 'exactly one .wkey file created for enc_cmd');

# ---------------------------------------------------------------------------
# 3.  Insert sentinel data; verify SQL readable and file encrypted.
# ---------------------------------------------------------------------------

my $sentinel = 'LOCAL_CMD_TDE_SENTINEL_XYZZY_77777';

$node->safe_psql('postgres',
	"CREATE TABLE tde_cmd_heap (id int, val text) TABLESPACE enc_cmd");
$node->safe_psql('postgres',
	"INSERT INTO tde_cmd_heap
	 SELECT i, '$sentinel' FROM generate_series(1, 200) i");
$node->safe_psql('postgres', "CHECKPOINT");

my $row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_cmd_heap WHERE val = '$sentinel'");
is($row_count, 200, '200 sentinel rows readable via SQL (local_cmd provider)');

my $relpath  = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_cmd_heap')");
my $abs_path = "$pgdata/$relpath";
my $raw      = slurp_file($abs_path);
ok(index($raw, $sentinel) == -1,
	'sentinel absent from raw encrypted heap file (local_cmd provider)');

# ---------------------------------------------------------------------------
# 4.  Restart: DEK must be reloaded by re-invoking the script.
# ---------------------------------------------------------------------------

$node->restart;

$status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
	 FROM pg_tde_tablespace_status
	 WHERE spc_name = 'enc_cmd'");
is($status, "AES256|t", 'enc_cmd still AES256, loaded=t after restart');

$row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_cmd_heap WHERE val = '$sentinel'");
is($row_count, 200, '200 rows readable after restart (local_cmd DEK reload)');

$raw = slurp_file($abs_path);
ok(index($raw, $sentinel) == -1,
	'heap file still encrypted on disk after restart');

my $synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0, 'tde_sync_keys() = 0 after restart (all keys already loaded)');

# ---------------------------------------------------------------------------
# 5.  Wrong KEK: replace script, restart.
#     The server must come up but enc_cmd must be absent from status.
# ---------------------------------------------------------------------------

$node->stop;

_write_kek_script($kek_script, $kek_wrong);

$node->start;

my $absent = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status
	 WHERE spc_name = 'enc_cmd'");
is($absent, 0,
	'enc_cmd absent from status after restart with wrong KEK script');

# ---------------------------------------------------------------------------
# 6.  tde_sync_keys() with wrong script still returns 0 (unwrap fails).
# ---------------------------------------------------------------------------

$synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0,
	'tde_sync_keys() = 0 with wrong KEK (unwrap fails, no key loaded)');

# ---------------------------------------------------------------------------
# 7.  Restore correct script; tde_sync_keys() loads the key without restart.
# ---------------------------------------------------------------------------

_write_kek_script($kek_script, $kek_correct);

$synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 1,
	'tde_sync_keys() = 1 after restoring correct KEK script');

# ---------------------------------------------------------------------------
# 8.  Data readable again after sync-only restore (no full restart needed).
# ---------------------------------------------------------------------------

$row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_cmd_heap WHERE val = '$sentinel'");
is($row_count, 200,
	'200 rows readable after tde_sync_keys() with restored KEK script');

# ---------------------------------------------------------------------------
# 9.  DROP TABLESPACE removes the .wkey file.
# ---------------------------------------------------------------------------

$node->safe_psql('postgres', "DROP TABLE tde_cmd_heap");
$node->safe_psql('postgres', "DROP TABLESPACE enc_cmd");

@wkeys = glob("$pgdata/pg_cryptokeys/tablespaces/*.wkey");
is(scalar @wkeys, 0,
	'no .wkey files remain after DROP TABLESPACE enc_cmd');

my $gone = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status
	 WHERE spc_name = 'enc_cmd'");
is($gone, 0, 'enc_cmd gone from status view after DROP TABLESPACE');

$node->stop;

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

sub _write_kek_script
{
	my ($path, $hex_kek) = @_;
	open(my $fh, '>', $path) or die "Cannot write $path: $!";
	print $fh "#!/bin/sh\n";
	print $fh "printf '%s\\n' '$hex_kek'\n";
	close $fh;
	chmod 0755, $path or die "chmod $path: $!";
}
