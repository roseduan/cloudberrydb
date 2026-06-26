# Verify the tde_tablespace SQL regression test produces the expected output.
#
# Cloudberry's pg_regress --temp-instance does not work here because the
# Cloudberry postgres binary requires --gp_dbid / --gp_contentid flags that
# pg_regress does not pass.  This TAP wrapper starts the node via
# PostgresNode.pm (which handles Cloudberry startup flags) and then runs the
# SQL file with psql -f, comparing the output to the expected file.

use strict;
use warnings;
use File::Spec;
use FindBin;
use IPC::Cmd qw(can_run);
use PostgresNode;
use TestLib;
use Test::More;

unless ($ENV{with_ssl} eq 'openssl')
{
	plan skip_all => "tests cannot run without OpenSSL";
}

plan tests => 3;

# ---------------------------------------------------------------------------
# Locate the SQL and expected files (relative to the crypto test directory).
# ---------------------------------------------------------------------------

my $sql_file = File::Spec->catfile(
	$FindBin::Bin, '..', '..', 'regress', 'sql', 'tde_tablespace.sql');
my $exp_file = File::Spec->catfile(
	$FindBin::Bin, '..', '..', 'regress', 'expected', 'tde_tablespace.out');

BAIL_OUT("tde_tablespace.sql not found at: $sql_file")
	unless -f $sql_file;
BAIL_OUT("tde_tablespace.out not found at: $exp_file")
	unless -f $exp_file;

pass('SQL and expected files found');

# ---------------------------------------------------------------------------
# Start a fresh node with TDE config.
# ---------------------------------------------------------------------------

my $passphrase = 'regression-test-key-do-not-use-in-production';

my $node = get_new_node('tde_sql_regress');
$node->init;
$node->append_conf('postgresql.conf', "tde_kms_provider = 'builtin'");
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = '$passphrase'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");
$node->start;

# ---------------------------------------------------------------------------
# Run the SQL file via psql -f and capture output.
# ---------------------------------------------------------------------------

my $psql = can_run('psql') // 'psql';

my $host = $node->host;
my $port = $node->port;

my $actual = qx{$psql -h "$host" -p $port -d postgres -f "$sql_file" 2>&1};
$actual =~ s/\r\n/\n/g;
my $expected = TestLib::slurp_file($exp_file);
$expected =~ s/\r\n/\n/g;

# Strip trailing whitespace per line (psql pads column headers with spaces).
$actual   =~ s/ +$//mg;
$expected =~ s/ +$//mg;

# Strip trailing blank lines.
$actual   =~ s/\s+$//;
$expected =~ s/\s+$//;

is($actual, $expected, 'tde_tablespace SQL output matches expected');

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

$node->stop;
pass('node stopped cleanly');
