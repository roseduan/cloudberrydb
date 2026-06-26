# Failure and resilience tests for tablespace TDE.
#
# Tests three failure scenarios and verifies the database handles each
# gracefully (the server stays up; the inaccessible tablespace is absent
# from pg_tde_tablespace_status until the problem is resolved).
#
# Scenario A — Missing .wkey at startup:
#   1.  Create cluster with builtin TDE, create encrypted tablespace + table.
#   2.  Stop the cluster, move the .wkey file away.
#   3.  Restart: server comes up; tablespace is absent from status view
#       (count(*) = 0), because TblspcKmgrStartup never loaded a shmem entry.
#   4.  Accessing the encrypted table returns an error mentioning "invalid page"
#       (the buffer manager reads ciphertext as plaintext and rejects it).
#   5.  Restore the .wkey; tde_sync_keys() returns 1 (one new key loaded).
#   6.  Data is readable again after sync.
#
# Scenario B — Corrupted .wkey (CRC mismatch):
#   7.  Stop cluster, flip a byte in the CRC field of the .wkey file.
#   8.  Restart: server comes up (non-fatal).
#   9.  Log contains a warning about CRC/corrupt (log_min_messages = warning
#       is required so that ereport(WARNING) messages reach the log).
#  10.  Tablespace is absent from status view (count(*) = 0).
#
# Scenario C — Wrong passphrase (DEK unwrap fails):
#  11.  Stop cluster, append wrong passphrase to postgresql.conf.
#  12.  Restart: server comes up (non-fatal).
#  13.  Tablespace is absent from status view (count(*) = 0).
#
# Recovery after Scenario C:
#  14.  Restore correct passphrase, restart. (already counted above as test 13)
#  15.  Tablespace reappears loaded=t; data readable; tde_sync_keys() = 0.

use strict;
use warnings;
use File::Copy qw(copy);
use PostgresNode;
use TestLib;
use Test::More;

unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

plan tests => 13;

# ---------------------------------------------------------------------------
# Node setup
# ---------------------------------------------------------------------------

my $passphrase = 'failure-test-passphrase-2026';

my $node = get_new_node('failure_tde_node');
$node->init;

$node->append_conf('postgresql.conf', "tde_kms_provider = 'builtin'");
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = '$passphrase'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");

# Use log_min_messages = warning so that ereport(WARNING) messages (e.g. CRC
# mismatch, DEK unwrap failure) appear in the server log.  The default is
# already WARNING, but we set it explicitly for clarity.
# NOTE: Do NOT use log_min_messages = log here — in PostgreSQL's special
# ordering LOG sits *above* ERROR/WARNING for log_min_messages purposes, so
# setting it to LOG would silently suppress WARNING messages in the log.
$node->append_conf('postgresql.conf', "log_min_messages = warning");

$node->start;

$node->safe_psql(
	'postgres',
	"CREATE TABLESPACE enc_fail LOCATION ''
     WITH (encryption_method = 'AES256')");

$node->safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS pg_tde");

my $sentinel = 'FAILURE_TDE_SENTINEL_XYZ_9999';

$node->safe_psql('postgres',
	"CREATE TABLE fail_heap (id int, val text) TABLESPACE enc_fail");
$node->safe_psql('postgres',
	"INSERT INTO fail_heap SELECT i, '$sentinel' FROM generate_series(1,100) i");
$node->safe_psql('postgres', "CHECKPOINT");

my $pgdata    = $node->data_dir;
my $wkey_dir  = "$pgdata/pg_cryptokeys/tablespaces";
my @wkeys     = glob("$wkey_dir/*.wkey");
my $wkey_file = $wkeys[0];

# Keep a clean backup for Scenario B restoration.
my $wkey_backup = "$wkey_dir/good.wkey.bak";
copy($wkey_file, $wkey_backup)
	or BAIL_OUT("cannot copy .wkey to backup: $!");

# ---------------------------------------------------------------------------
# Scenario A: Missing .wkey at startup
# ---------------------------------------------------------------------------

$node->stop;

rename($wkey_file, "$wkey_file.missing")
	or BAIL_OUT("cannot rename .wkey for missing-file test: $!");

$node->start;

# A.1 — Tablespace must be absent from the status view.
# When .wkey is missing TblspcKmgrStartup skips the tablespace (wkey_read
# returns false silently for ENOENT), so no shmem entry is created and the
# view returns zero rows.
my $cnt_spc = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status WHERE spc_name = 'enc_fail'");
is($cnt_spc, 0,
	'Scenario A: tablespace absent from status view when .wkey is missing');

# A.2 — Accessing the locked tablespace must return an error.
# Without a shmem entry, TblspcEncryptionEnabled() returns false for that
# tablespace, so the buffer manager reads ciphertext as plaintext and rejects
# the page with an "invalid page" error.
my ($exit, $stdout, $stderr) = $node->psql('postgres',
	"SELECT count(*) FROM fail_heap");
isnt($exit, 0, 'Scenario A: accessing locked tablespace returns non-zero exit');
like($stderr, qr/invalid page/i,
	'Scenario A: error message mentions "invalid page" (ciphertext read as plaintext)');

# A.3 — Restore .wkey; tde_sync_keys() should load 1 new key.
rename("$wkey_file.missing", $wkey_file)
	or BAIL_OUT("cannot restore .wkey: $!");

my $synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 1, 'Scenario A: tde_sync_keys() returns 1 after restoring .wkey');

# A.4 — Data must be readable after restore + sync.
my $count = $node->safe_psql('postgres',
	"SELECT count(*) FROM fail_heap WHERE val = '$sentinel'");
is($count, 100,
	'Scenario A: 100 rows readable after .wkey restored and synced');

# ---------------------------------------------------------------------------
# Scenario B: Corrupted .wkey (CRC mismatch)
# ---------------------------------------------------------------------------

$node->stop;

# Flip one byte in the CRC field (bytes 12–15 in the .wkey header).
open my $fh, '+<:raw', $wkey_file
	or BAIL_OUT("cannot open .wkey for corruption test: $!");
seek $fh, 12, 0;
my $byte;
read $fh, $byte, 1;
seek $fh, 12, 0;
print $fh chr(ord($byte) ^ 0xFF);
close $fh;

my $log_offset = -s $node->logfile;
$node->start;

# B.1 — Server must come up (CRC failure at startup is non-fatal).
pass('Scenario B: server starts despite corrupted .wkey');

# B.2 — Log should contain a CRC / corrupt warning.
# This relies on log_min_messages = warning so ereport(WARNING) reaches the log.
my $log = TestLib::slurp_file($node->logfile, $log_offset);
like($log, qr/CRC|corrupt/i,
	'Scenario B: startup log contains CRC/corrupt warning');

# B.3 — Tablespace is absent from status view (no shmem entry created).
$cnt_spc = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status WHERE spc_name = 'enc_fail'");
is($cnt_spc, 0,
	'Scenario B: tablespace absent from status view after CRC corruption');

# Restore the clean .wkey from backup so Scenario C starts from a valid state.
copy($wkey_backup, $wkey_file)
	or BAIL_OUT("cannot restore .wkey from backup: $!");

# ---------------------------------------------------------------------------
# Scenario C: Wrong passphrase (DEK unwrap fails)
# ---------------------------------------------------------------------------

$node->stop;

# Append a wrong passphrase (last value in the file wins for PGC_SIGHUP GUCs).
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = 'THIS-IS-THE-WRONG-PASSPHRASE'");

$log_offset = -s $node->logfile;
$node->start;

# C.1 — Server must come up.
pass('Scenario C: server starts despite wrong passphrase');

# C.2 — Tablespace is absent from status view (DEK unwrap failed → no shmem entry).
$cnt_spc = $node->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_tde_tablespace_status WHERE spc_name = 'enc_fail'");
is($cnt_spc, 0,
	'Scenario C: tablespace absent from status view with wrong passphrase');

# ---------------------------------------------------------------------------
# Recovery: restore the correct passphrase and restart.
# ---------------------------------------------------------------------------

$node->stop;

# Append the correct passphrase (overrides the wrong one appended above).
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = '$passphrase'");

$node->start;

# Recovery.1 — Tablespace should be loaded=t.
my $status = $node->safe_psql(
	'postgres',
	"SELECT loaded FROM pg_tde_tablespace_status WHERE spc_name = 'enc_fail'");
is($status, 't',
	'Recovery: tablespace loaded=t after restoring correct passphrase');

# Recovery.2 — tde_sync_keys() returns 0 (key was loaded at startup).
$synced = $node->safe_psql('postgres', "SELECT tde_sync_keys()");
is($synced, 0,
	'Recovery: tde_sync_keys() returns 0 after correct-passphrase restart');

# Recovery.3 — Data is readable again.
$count = $node->safe_psql('postgres',
	"SELECT count(*) FROM fail_heap WHERE val = '$sentinel'");
is($count, 100,
	'Recovery: all 100 rows readable after restoring correct passphrase');

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

$node->safe_psql('postgres', "DROP TABLE fail_heap");
$node->safe_psql('postgres', "DROP TABLESPACE enc_fail");

$node->stop;
