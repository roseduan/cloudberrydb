# Test tablespace-level TDE with the native KMIP TCP provider (kms_kmip.c).
#
# This test connects to a real KMIP server via TTLV binary protocol over mTLS
# (TCP port 5696).  It is skipped automatically when:
#   - OpenSSL is not compiled in, or
#   - the required mTLS certificate files are not present, or
#   - the KMIP server is not reachable at the configured host:port.
#
# Override coordinates via environment variables:
#   KMIP_HOST        (default: 172.19.0.3)
#   KMIP_PORT        (default: 5696)
#   KMIP_KEY_ID      (default: 536eb7d9-6b0c-4a61-85d0-fa223a27accc)
#   KMIP_CA_CERT     (default: /tmp/kmip-certs/ca.crt)
#   KMIP_CLIENT_CERT (default: /tmp/kmip-certs/client.crt)
#   KMIP_CLIENT_KEY  (default: /tmp/kmip-certs/client.key)
#
# Scenario:
#   1.  Start a node with tde_kms_provider=kmip pointing at the server.
#   2.  CREATE TABLESPACE with encryption_method='AES256'.
#   3.  Insert sentinel string into a heap table.
#   4.  Verify SQL read returns all rows (decryption is transparent).
#   5.  Verify raw data file does NOT contain the sentinel (file is encrypted).
#   6.  Verify pg_tde_tablespace_status shows AES256, loaded=t.
#   7.  Restart: DEK is unwrapped by KMIP server from the stored .wkey blob.
#   8.  SQL reads still work after restart.
#   9.  File is still encrypted after restart.
#  10.  UPDATE round-trip: modified rows are encrypted on disk.
#  11.  DROP TABLESPACE removes the .wkey file.
#  12.  enc_kmip gone from status view after DROP.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Require OpenSSL (kms_kmip.c is compiled only when USE_OPENSSL).
unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

# Read server coordinates from environment.
my $kmip_host        = $ENV{KMIP_HOST}        // '172.19.0.3';
my $kmip_port        = $ENV{KMIP_PORT}        // '5696';
my $kmip_key_id      = $ENV{KMIP_KEY_ID}      // '536eb7d9-6b0c-4a61-85d0-fa223a27accc';
my $kmip_ca_cert     = $ENV{KMIP_CA_CERT}     // '/tmp/kmip-certs/ca.crt';
my $kmip_client_cert = $ENV{KMIP_CLIENT_CERT} // '/tmp/kmip-certs/client.crt';
my $kmip_client_key  = $ENV{KMIP_CLIENT_KEY}  // '/tmp/kmip-certs/client.key';

# Check that certificate files exist.
unless (-f $kmip_ca_cert && -f $kmip_client_cert && -f $kmip_client_key)
{
	plan skip_all =>
		"KMIP certificate files not found "
		. "(set KMIP_CA_CERT / KMIP_CLIENT_CERT / KMIP_CLIENT_KEY to override)";
}

# Probe: can we reach the KMIP server via TCP?
sub kmip_reachable
{
	my $reachable = 0;
	eval {
		require IO::Socket::INET;
		my $sock = IO::Socket::INET->new(
			PeerAddr => $kmip_host,
			PeerPort => $kmip_port,
			Proto    => 'tcp',
			Timeout  => 3,
		);
		$reachable = 1 if $sock;
		$sock->close if $sock;
	};
	return $reachable;
}

unless (kmip_reachable())
{
	plan skip_all =>
		"KMIP server not reachable at $kmip_host:$kmip_port "
		. "(set KMIP_HOST / KMIP_PORT to override)";
}

plan tests => 13;

#
# Node setup
#
my $node = get_new_node('kmip_tde_node');
$node->init;

$node->append_conf('postgresql.conf', "tde_kms_provider = 'kmip'");
$node->append_conf('postgresql.conf', "tde_kms_host = '$kmip_host'");
$node->append_conf('postgresql.conf', "tde_kms_port = $kmip_port");
$node->append_conf('postgresql.conf', "tde_kms_ca_cert = '$kmip_ca_cert'");
$node->append_conf('postgresql.conf',
	"tde_kms_client_cert = '$kmip_client_cert'");
$node->append_conf('postgresql.conf', "tde_kms_client_key = '$kmip_client_key'");
$node->append_conf('postgresql.conf',
	"tde_kms_default_key_id = '$kmip_key_id'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");

$node->start;

#
# 1.  Create the encrypted tablespace
#
$node->safe_psql(
	'postgres',
	"CREATE TABLESPACE enc_kmip LOCATION ''
     WITH (encryption_method = 'AES256')");

#
# 2.  Verify pg_tde_tablespace_status
#
$node->safe_psql('postgres', "CREATE EXTENSION pg_tde");

my $status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
     FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_kmip'");
ok($status eq "AES256|t",
	'tablespace enc_kmip shows AES256, loaded=t in status view');

#
# 3.  Create heap table, insert sentinel
#
my $sentinel = 'KMIP_TDE_SENTINEL_XYZZY_98765';

$node->safe_psql('postgres',
	"CREATE TABLE tde_kmip_heap (id int, val text) TABLESPACE enc_kmip");
$node->safe_psql('postgres',
	"INSERT INTO tde_kmip_heap SELECT i, '$sentinel'"
		. " FROM generate_series(1,200) i");
$node->safe_psql('postgres', "CHECKPOINT");

#
# 4.  SQL read works (decryption is transparent)
#
my $row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_kmip_heap WHERE val = '$sentinel'");
is($row_count, 200, 'all 200 sentinel rows readable via SQL (KMIP wrap)');

#
# 5.  Raw file does NOT contain the sentinel (data is encrypted on disk)
#
my $pgdata   = $node->data_dir;
my $filepath = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('tde_kmip_heap')");

my $abs_filepath = "$pgdata/$filepath";
ok(-f $abs_filepath, "data file exists at $abs_filepath");

my $raw = slurp_file($abs_filepath);
ok(index($raw, $sentinel) == -1,
	'sentinel string not found in raw encrypted data file');

#
# 6.  .wkey file exists and has the expected size.
#     .wkey layout: [header 16B][key_id NB][wrapped_dek MB]
#     For KMIP AES-256: wrapped_dek = [IV 12B][AuthTag 16B][Ciphertext 32B] = 60B
#     key_id = tde_kms_default_key_id string (36 bytes for a UUID)
#     Total: 16 + 36 + 60 = 112 bytes
#
my $wkey_dir = "$pgdata/pg_cryptokeys/tablespaces";
my @wkeys    = glob("$wkey_dir/*.wkey");
is(scalar @wkeys, 1, 'exactly one .wkey file exists for enc_kmip');

if (@wkeys)
{
	my $key_id_len    = length($kmip_key_id);
	my $expected_size = 16 + $key_id_len + 60;    # hdr + key_id + IV+AuthTag+CT
	my $wkey_size     = -s $wkeys[0];
	is($wkey_size, $expected_size,
		".wkey file is $expected_size bytes "
			. "(16B hdr + ${key_id_len}B key_id + 60B wrapped DEK)");
}
else
{
	fail('.wkey file missing — skipping size check');
}

#
# 7.  Restart: DEK must be unwrapped by KMIP from the stored blob
#
$node->restart;

# Status view should show loaded=t again
$status = $node->safe_psql(
	'postgres',
	"SELECT enc_method, loaded
     FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_kmip'");
ok($status eq "AES256|t",
	'DEK re-unwrapped by KMIP after restart, loaded=t');

#
# 8.  Data still readable after restart
#
$row_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_kmip_heap WHERE val = '$sentinel'");
is($row_count, 200, 'all rows readable after restart (KMIP unwrap)');

#
# 9.  File still encrypted after restart
#
$raw = slurp_file($abs_filepath);
ok(index($raw, $sentinel) == -1,
	'data remains encrypted on disk after restart');

#
# 10.  UPDATE round-trip on KMIP-wrapped encrypted heap table
#
my $sentinel2 = 'KMIP_TDE_UPDATE_ROUNDTRIP';
$node->safe_psql('postgres',
	"UPDATE tde_kmip_heap SET val = '$sentinel2' WHERE id <= 5");
$node->safe_psql('postgres', "CHECKPOINT");

my $upd_count = $node->safe_psql('postgres',
	"SELECT count(*) FROM tde_kmip_heap WHERE val = '$sentinel2'");
is($upd_count, 5,
	'5 updated rows readable from KMIP-wrapped encrypted table');

my $raw2 = slurp_file($abs_filepath);
ok(index($raw2, $sentinel2) == -1,
	'updated sentinel not visible in raw encrypted file');

#
# 11.  DROP TABLESPACE removes the .wkey file
#
$node->safe_psql('postgres', "DROP TABLE tde_kmip_heap");
$node->safe_psql('postgres', "DROP TABLESPACE enc_kmip");

@wkeys = glob("$wkey_dir/*.wkey");
is(scalar @wkeys, 0, 'no .wkey files remain after DROP TABLESPACE');

#
# 12.  enc_kmip gone from status view
#
my $after_drop = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status
     WHERE spc_name = 'enc_kmip'");
is($after_drop, 0, 'enc_kmip gone from status view after DROP TABLESPACE');

$node->stop;
