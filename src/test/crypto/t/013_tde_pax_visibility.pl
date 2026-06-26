# Test that visibility is correct on an encrypted PAX tablespace after a DELETE.
#
# This guards the visibility-bitmap read paths (sequential scan in pax.cc and
# index_fetch_tuple in pax_scanner.cc) for a PAX table that lives in an
# encrypted tablespace.  A DELETE produces a .visimap file; the scanners must
# return exactly the surviving rows.
#
# Note on TDE: the .visimap is written and read via WriteN/ReadN, which map to
# plain sequential write()/read() and apply no encryption (only the positional
# PWriteN/PReadN used for column data encrypt).  So the bitmap is stored in
# plaintext and the visibility result is independent of whether the Open call
# is given the tablespace's TDE options.  This test pins that correct behavior
# so a future change to the bitmap I/O cannot silently corrupt visibility.
#
# Scenario:
#   1. builtin KMS + AES256 encrypted tablespace.
#   2. PAX table with 4000 rows; DELETE the first 2000.
#   3. Sequential scan must see exactly the 2000 surviving rows.
#   4. After restart (bitmap re-read from disk) a Bitmap Heap Scan that fetches
#      a non-indexed column (forcing pax_scanner.cc index_fetch_tuple) must also
#      see exactly the surviving rows.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;

# Tablespace TDE uses AES-CTR which requires OpenSSL.
if ($ENV{with_ssl} eq 'openssl')
{
	plan tests => 7;
}
else
{
	plan skip_all => "tests cannot run without OpenSSL";
}

#
# Node setup (builtin KMS, in-place tablespace)
#
my $node = get_new_node('tde_pax_vis_node');
$node->init;
$node->append_conf('postgresql.conf', "tde_kms_provider = 'builtin'");
$node->append_conf('postgresql.conf',
	"tde_kms_builtin_passphrase = 'tde_test_pass_013'");
$node->append_conf('postgresql.conf', "allow_in_place_tablespaces = on");
# Use the Postgres planner so the index-fetch plan shape is deterministic.
$node->append_conf('postgresql.conf', "optimizer = off");
$node->start;

$node->safe_psql('postgres',
	"CREATE TABLESPACE enc_vis LOCATION '' WITH (encryption_method = 'AES256')");

#
# PAX table in the encrypted tablespace: 4000 rows, delete the first 2000.
#
$node->safe_psql('postgres',
	"CREATE TABLE pax_vis (id int, val text) USING pax TABLESPACE enc_vis");
$node->safe_psql('postgres',
	"INSERT INTO pax_vis SELECT i, 'row_' || i FROM generate_series(1,4000) i");
$node->safe_psql('postgres', "DELETE FROM pax_vis WHERE id <= 2000");
$node->safe_psql('postgres', "CHECKPOINT");

#
# A visibility bitmap file must now exist for the micro-partition.  If it does
# not, the rest of the test would pass vacuously, so assert it up front.
#
my $pgdata      = $node->data_dir;
my $pax_relpath = $node->safe_psql('postgres',
	"SELECT pg_relation_filepath('pax_vis')");
my $pax_dir = "$pgdata/${pax_relpath}_pax";
my @visimaps = glob("$pax_dir/*.visimap");
ok(scalar @visimaps >= 1, 'a .visimap file was written after DELETE');

#
# 1. Sequential scan: exactly the 2000 surviving rows, none of the deleted ones.
#
my $seq_total = $node->safe_psql('postgres',
	"SET enable_seqscan = on; SELECT count(*) FROM pax_vis");
is($seq_total, 2000, 'seqscan sees exactly the 2000 surviving rows');

my $seq_deleted = $node->safe_psql('postgres',
	"SET enable_seqscan = on; SELECT count(*) FROM pax_vis WHERE id <= 2000");
is($seq_deleted, 0, 'seqscan sees none of the deleted rows');

my $seq_minmax = $node->safe_psql('postgres',
	"SET enable_seqscan = on; SELECT min(id) || '/' || max(id) FROM pax_vis");
is($seq_minmax, '2001/4000', 'seqscan surviving id range is 2001..4000');

#
# 2. Restart so the bitmap is re-read from disk, then force a Bitmap Heap Scan
#    that fetches a non-indexed column.  This drives the index_fetch_tuple path
#    in pax_scanner.cc (PaxIndexScanDesc::OpenMicroPartition) the reviewer
#    flagged, rather than re-reading via the sequential scanner.
#
$node->restart;

$node->safe_psql('postgres', "CREATE INDEX pax_vis_idx ON pax_vis (id)");
$node->safe_psql('postgres', "ANALYZE pax_vis");

# Sanity: confirm the planner drives a table fetch through the index (Bitmap
# Heap Scan / Index Scan), not a sequential scan.
my $plan = $node->safe_psql('postgres',
	"SET enable_seqscan = off;"
	. " EXPLAIN (COSTS off) SELECT val FROM pax_vis WHERE id BETWEEN 1900 AND 2100");
ok($plan =~ /Bitmap Heap Scan|Index Scan/,
	'planner fetches table rows through the index for the range predicate')
	or diag($plan);

# Range 1900..2100 spans the delete boundary: ids 1900..2000 are deleted,
# 2001..2100 survive => exactly 100 rows, all fetched through the index.
my $idx_count = $node->safe_psql('postgres',
	"SET enable_seqscan = off;"
	. " SELECT count(*) FROM pax_vis WHERE id BETWEEN 1900 AND 2100 AND val IS NOT NULL");
is($idx_count, 100,
	'index fetch returns only the 100 surviving rows (2001..2100) in the range');

my $idx_minmax = $node->safe_psql('postgres',
	"SET enable_seqscan = off;"
	. " SELECT min(id) || '/' || max(id) FROM pax_vis WHERE id BETWEEN 1900 AND 2100 AND val IS NOT NULL");
is($idx_minmax, '2001/2100',
	'index fetch surviving id range is 2001..2100, none of the deleted rows');

$node->safe_psql('postgres', "DROP TABLE pax_vis");
$node->safe_psql('postgres', "DROP TABLESPACE enc_vis");
$node->stop;
