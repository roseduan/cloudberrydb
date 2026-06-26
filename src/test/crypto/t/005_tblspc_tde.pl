# Test tablespace-level TDE (per-tablespace DEK, builtin KMS provider)
#
# Scenario:
#   1. Start a node with tde_kms_provider=builtin and a passphrase.
#   2. CREATE TABLESPACE with encryption_method='AES256'.
#   3. Insert a distinctive sentinel string into a heap table.
#   4. Verify the sentinel is readable via SQL (decryption works).
#   5. Verify the raw data file does NOT contain the sentinel (file is encrypted).
#   6. Check pg_tde_tablespace_status view shows the tablespace as loaded.
#   7. Restart: DEK must be reloaded from .wkey – SQL reads still work.
#   8. tde_sync_keys() returns 0 (all keys already loaded after restart).
#   9. tde_open_tablespace() on an already-loaded key is idempotent.
#  10. DROP TABLESPACE cleans up the .wkey file.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Tablespace TDE uses AES-CTR which requires OpenSSL.
if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 24;
}
else
{
	plan skip_all => "tests cannot run without OpenSSL";
}

#
# Node setup
#
my $node = get_new_node('tde_node');
$node->init;

# Configure builtin KMS with a test passphrase.
# tde_kms_provider is PGC_POSTMASTER so must be in postgresql.conf.
$node->append_conf('postgresql.conf', "tde_kms_provider = 'builtin'");
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = 'tde_test_pass_007'");
# allow_in_place_tablespaces lets us create a tablespace inside $PGDATA,
# which avoids needing a separate directory on the test host.
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");

$node->start;

#
# 1.  Create the encrypted tablespace
#
$node->safe_psql(
	'postgres',
	"CREATE TABLESPACE enc_spc LOCATION ''
     WITH (encryption_method = 'AES256')");

#
# 2.  Verify pg_tde_tablespace_status (via pg_tde extension)
#
$node->safe_psql('postgres', "CREATE EXTENSION pg_tde");

my $status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
     FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_spc'");
ok($status eq "AES256|t", 'tablespace enc_spc shows AES256, loaded=t in status view');

#
# 3.  Create a heap table in the encrypted tablespace and insert sentinel data
#
my $sentinel = 'TDE_SENTINEL_XYZZY_12345';

$node->safe_psql('postgres',
	"CREATE TABLE tde_heap (id int, val text) TABLESPACE enc_spc");
$node->safe_psql('postgres',
	"INSERT INTO tde_heap SELECT i, '$sentinel' FROM generate_series(1,200) i");

# Force the data to disk
$node->safe_psql('postgres', "CHECKPOINT");

#
# 4.  SQL read works (decryption is transparent)
#
my $row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_heap WHERE val = '$sentinel'");
is($row_count, 200, 'all 200 sentinel rows readable via SQL');

#
# 5.  Raw file does NOT contain the sentinel (data is encrypted on disk)
#
my $pgdata   = $node->data_dir;
my $filepath = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_heap')");

# filepath is relative to $PGDATA (e.g. pg_tblspc/NNN/GP.../OID/relfile)
my $abs_filepath = "$pgdata/$filepath";
ok(-f $abs_filepath, "data file exists at $abs_filepath");

my $raw = slurp_file($abs_filepath);
ok(index($raw, $sentinel) == -1,
	'sentinel string not found in raw encrypted data file');

#
# 6.  tde_sync_keys() with everything already loaded returns 0
#
my $synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0, 'tde_sync_keys() returns 0 when all keys already loaded');

#
# 7.  tde_open_tablespace() on an already-loaded tablespace is idempotent
#
my $opened = $node->safe_psql('postgres',
	"SELECT tde_open_tablespace('enc_spc')");
is($opened, 't', 'tde_open_tablespace() succeeds on already-loaded key');

#
# 8.  Error on non-existent tablespace
#
my ($rc, $stdout, $stderr) = $node->psql('postgres',
	"SELECT tde_open_tablespace('no_such_tablespace')");
ok($rc != 0, 'tde_open_tablespace() errors on missing tablespace');

#
# 9.  Non-superuser is denied
#
$node->safe_psql('postgres', "CREATE ROLE tde_unpriv LOGIN");
$node->safe_psql('postgres', "GRANT CONNECT ON DATABASE postgres TO tde_unpriv");

($rc, $stdout, $stderr) = $node->psql('postgres',
	"SELECT * FROM pg_tde_status()", extra_params => ['-U', 'tde_unpriv']);
ok($rc != 0 && $stderr =~ /permission denied/, 'non-superuser denied pg_tde_status');

($rc, $stdout, $stderr) = $node->psql('postgres',
	"SELECT tde_sync_keys()", extra_params => ['-U', 'tde_unpriv']);
ok($rc != 0 && $stderr =~ /permission denied/, 'non-superuser denied tde_sync_keys');

($rc, $stdout, $stderr) = $node->psql('postgres',
	"SELECT tde_open_tablespace('enc_spc')", extra_params => ['-U', 'tde_unpriv']);
ok($rc != 0 && $stderr =~ /permission denied/, 'non-superuser denied tde_open_tablespace');

$node->safe_psql('postgres', "REVOKE CONNECT ON DATABASE postgres FROM tde_unpriv");
$node->safe_psql('postgres', "DROP ROLE tde_unpriv");

#
# PAX-1.  Create a PAX table in the encrypted tablespace and insert sentinel
#
# pax is registered in pg_am at initdb time (via cdb_init.d/pax-cdbinit).
# No shared_preload_libraries entry is required; the library is loaded on
# demand.
#
$node->safe_psql('postgres',
	"CREATE TABLE tde_pax (id int, val text) USING pax TABLESPACE enc_spc");
$node->safe_psql('postgres',
	"INSERT INTO tde_pax SELECT i, '$sentinel' FROM generate_series(1,200) i");
$node->safe_psql('postgres', "CHECKPOINT");

my $pax_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_pax WHERE val = '$sentinel'");
is($pax_count, 200,
	'all 200 sentinel rows readable from encrypted PAX table via SQL');

#
# PAX-2.  PAX micro-partition file (relfilenode_pax/0) exists on disk
#
# pg_relation_filepath returns e.g. pg_tblspc/OID/.../relfilenode.
# PAX stores micro-partitions under <relfilenode>_pax/0.
#
my $pax_relpath = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_pax')");
my $pax_file = "$pgdata/${pax_relpath}_pax/0";
ok(-f $pax_file, "PAX micro-partition file exists at $pax_file");

#
# PAX-3.  Raw PAX file does NOT contain the sentinel (data is encrypted)
#
my $pax_raw = slurp_file($pax_file);
ok(index($pax_raw, $sentinel) == -1,
	'sentinel not found in raw encrypted PAX micro-partition file');

#
# PAX-4.  Contrast: PAX table in the default (unencrypted) tablespace
#          has the sentinel visible in the raw file
#
$node->safe_psql('postgres',
	"CREATE TABLE tde_pax_plain (id int, val text) USING pax");
$node->safe_psql('postgres',
	"INSERT INTO tde_pax_plain SELECT i, '$sentinel'"
	. " FROM generate_series(1,50) i");
$node->safe_psql('postgres', "CHECKPOINT");

my $plain_pax_relpath = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_pax_plain')");
my $plain_pax_file = "$pgdata/${plain_pax_relpath}_pax/0";
my $plain_pax_raw  = slurp_file($plain_pax_file);
ok(index($plain_pax_raw, $sentinel) != -1,
	'sentinel IS visible in raw unencrypted PAX micro-partition file (contrast)');

$node->safe_psql('postgres', "DROP TABLE tde_pax_plain");

#
# 10. Restart: DEK must be reloaded from .wkey automatically
#
$node->restart;

# Status should still show the key as loaded
$status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
     FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_spc'");
ok($status eq "AES256|t", 'DEK reloaded from .wkey after restart');

# Data still readable after restart
$row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_heap WHERE val = '$sentinel'");
is($row_count, 200, 'all rows readable after restart');

#
# 11. File is still encrypted after restart
#
$raw = slurp_file($abs_filepath);
ok(index($raw, $sentinel) == -1,
	'data remains encrypted on disk after restart');

#
# 12. tde_sync_keys() still returns 0 – all keys loaded at startup
#
$synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0, 'tde_sync_keys() returns 0 after restart');

#
# PAX-5.  Encrypted PAX table is still readable after restart
#
$pax_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_pax WHERE val = '$sentinel'");
is($pax_count, 200,
	'encrypted PAX table fully readable after cluster restart');

#
# PAX-6.  PAX file is still encrypted on disk after restart
#
$pax_raw = slurp_file($pax_file);
ok(index($pax_raw, $sentinel) == -1,
	'PAX micro-partition file remains encrypted after cluster restart');

#
# PAX-7.  UPDATE round-trip on encrypted PAX table
#
my $sentinel2 = 'TDE_SENTINEL_UPDATE_ROUNDTRIP';
$node->safe_psql('postgres',
	"UPDATE tde_pax SET val = '$sentinel2' WHERE id <= 10");
$node->safe_psql('postgres', "CHECKPOINT");

my $upd_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_pax WHERE val = '$sentinel2'");
is($upd_count, 10, 'updated rows readable from encrypted PAX table');

# The new micro-partition written for the updated rows must also be encrypted.
# Walk all segment files (0, 1, 2 …) to confirm.
my $pax_relpath3 = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_pax')");
my $pax_dir = "$pgdata/${pax_relpath3}_pax";
my @seg_files = grep { -f $_ } glob("$pax_dir/[0-9]*");
my $found_in_seg = 0;
for my $sf (@seg_files) {
	my $content = slurp_file($sf);
	$found_in_seg++ if index($content, $sentinel2) != -1;
}
is($found_in_seg, 0,
	'updated sentinel not visible in any raw encrypted PAX segment file');

#
# 13. DROP TABLESPACE removes the .wkey file
#
$node->safe_psql('postgres', "DROP TABLE tde_pax");
$node->safe_psql('postgres', "DROP TABLE tde_heap");
$node->safe_psql('postgres', "DROP TABLESPACE enc_spc");

# Retrieve the tablespace OID that was just dropped (it's gone from catalog,
# but we captured it from pg_tde_status before the drop).
my $wkey_dir = "$pgdata/pg_cryptokeys/tablespaces";
my @wkeys    = glob("$wkey_dir/*.wkey");
is(scalar @wkeys, 0, 'no .wkey files remain after DROP TABLESPACE');

#
# 14. pg_tde_tablespace_status no longer contains enc_spc
#
my $after_drop = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_spc'");
is($after_drop, 0, 'enc_spc gone from status view after DROP TABLESPACE');

$node->stop;
