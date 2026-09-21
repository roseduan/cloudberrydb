# Copyright (c) 2024-2024, MDB, Mother Russia

# Test the mdb_admin exception in pg_signal_backend(): a member of the
# mdb_admin role may terminate a superuser-owned backend, but only when that
# backend is an autovacuum worker or has application_name = 'MDB'.  A role that
# merely holds pg_signal_backend (and is not an mdb_admin member) may not.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init();
$node->start;

$node->safe_psql(
	'postgres', "
    CREATE DATABASE regress;
    CREATE ROLE mdb_admin;
    CREATE ROLE mdb_reg_lh_1;
    CREATE ROLE mdb_reg_lh_2;
    GRANT pg_signal_backend TO mdb_admin;
    GRANT pg_signal_backend TO mdb_reg_lh_1;
    GRANT mdb_admin TO mdb_reg_lh_2;
");

# A long-lived, superuser-owned backend tagged as an MDB service connection.
my $bg = $node->background_psql('regress');
$bg->query_safe("SET application_name = 'MDB'");

my $target_pid = $node->safe_psql('regress',
	"SELECT pid FROM pg_stat_activity WHERE application_name = 'MDB'");
like($target_pid, qr/^\d+$/, "found the MDB superuser backend (pid $target_pid)");

# 1. A pg_signal_backend member that is NOT an mdb_admin member cannot
#    terminate a superuser-owned backend.
my ($rc1, $stdout1, $stderr1) = $node->psql('regress',
	"SET ROLE mdb_reg_lh_1; SELECT pg_terminate_backend($target_pid);");
isnt($rc1, 0, "non-mdb_admin pg_signal_backend member is refused");
like(
	$stderr1,
	qr/permission denied to terminate process/,
	"refusal is the superuser-process error");

# The target must still be alive.
is( $node->safe_psql(
		'regress', "SELECT count(*) FROM pg_stat_activity WHERE pid = $target_pid"),
	'1',
	"target backend survived the refused termination");

# 2. An mdb_admin member CAN terminate the MDB-tagged superuser backend.
my ($rc2, $stdout2, $stderr2) = $node->psql('regress',
	"SET ROLE mdb_reg_lh_2; SELECT pg_terminate_backend($target_pid);");
is($rc2, 0, "mdb_admin member is allowed");
is($stdout2, 't', "pg_terminate_backend reports success");

$node->poll_query_until('regress',
	"SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $target_pid)")
  or die "target backend did not terminate";
pass("target backend was terminated by the mdb_admin member");

eval { $bg->quit; };
$node->stop;

done_testing();
