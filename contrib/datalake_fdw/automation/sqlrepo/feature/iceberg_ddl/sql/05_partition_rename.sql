-- 05_partition_rename.sql
-- Test ALTER TABLE ... RENAME COLUMN on an Iceberg partition column (issue #444)
--
-- The PARTITION BY declaration is stored in pg_lake_table.ltoptions as
-- "iceberg_partition_by=col1,col2" -- by column *name*.  Two consumers resolve
-- those names against the live schema: createPartitionSpecFromRequest() on the
-- QD and icebergWrite::computePartition() on the QE.  Before #444 a RENAME
-- COLUMN left that declaration stale, so the DDL reported success and every
-- subsequent read and write of the table failed with
--   ERROR: partition column "<old name>" not found in iceberg table schema
--   ERROR: iceberg partition column "<old name>" not found in table "<table>"
--
-- 04_partition_by.sql covers only how the declaration is *created*.  That is
-- how #444 slipped through: the spec was written correctly and then silently
-- rotted on the next rename.  These tests pin the declaration across renames
-- and, more importantly, assert the end-to-end read/write path afterwards.

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: RENAME COLUMN on an Iceberg partition column');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER dt_pr_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dt_pr_catalog_server;
CREATE FOREIGN CATALOG dt_pr_catalog SERVER dt_pr_catalog_server;
SET iceberg_default_catalog = 'dt_pr_catalog';

CREATE SERVER dt_pr_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER dt_pr_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dt_pr_volume SERVER dt_pr_volume_server
OPTIONS (base_path '/dt_pr_volume/');
SET iceberg_default_volume = 'dt_pr_volume';

-- Reads the partition declaration the way both consumers do.
CREATE OR REPLACE FUNCTION dt_pr_spec(regclass)
RETURNS text AS $$
    SELECT o FROM (SELECT unnest(ltoptions) AS o FROM pg_lake_table
                   WHERE ltrelid = $1) s
     WHERE o LIKE 'iceberg_partition_by%';
$$ LANGUAGE SQL STABLE;

-- ============================================================
-- Test 1: renaming the partition column keeps the table readable and writable
--
-- This is the #444 regression itself.  Asserting only ltoptions would pass
-- even if the value were updated to something the consumers cannot use, so
-- every step below also exercises the data path.
-- ============================================================
SELECT test_log('Test 1: rename the partition column');

CREATE ICEBERG TABLE dt_pr_single (id bigint, region text) PARTITION BY (region);
INSERT INTO dt_pr_single VALUES (1,'east'),(2,'west'),(3,'east');

SELECT dt_pr_spec('dt_pr_single'::regclass) AS spec_before;

ALTER TABLE dt_pr_single RENAME COLUMN region TO region2;

SELECT dt_pr_spec('dt_pr_single'::regclass) AS spec_after;

-- pg_lake_table is not touched by the standard ALTER dispatch, so the new
-- value has to be dispatched explicitly.  A segment left on the stale list
-- fails its next INSERT; DISTINCT collapses to one row only if all agree.
SELECT DISTINCT o AS spec_on_segments
FROM (SELECT unnest(ltoptions) AS o FROM gp_dist_random('pg_lake_table')
      WHERE ltrelid = 'dt_pr_single'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- Data written before the rename must still read back.
SELECT id, region2 FROM dt_pr_single ORDER BY id;
SELECT count(*) AS total FROM dt_pr_single;

-- Writes after the rename, into an existing partition and into a new one.
INSERT INTO dt_pr_single VALUES (4,'south'),(5,'east');
SELECT region2, count(*) FROM dt_pr_single GROUP BY region2 ORDER BY region2;

-- Partition pruning must still return the right rows under the new name.
SELECT id FROM dt_pr_single WHERE region2 = 'east' ORDER BY id;

-- ============================================================
-- Test 2: renames chain
--
-- The second rename has to match the name the first one wrote, not the
-- original one, so this catches a fix that only ever rewrites from the
-- CREATE-time name.
-- ============================================================
SELECT test_log('Test 2: chained renames');

ALTER TABLE dt_pr_single RENAME COLUMN region2 TO region3;
SELECT dt_pr_spec('dt_pr_single'::regclass) AS spec_after_second_rename;
INSERT INTO dt_pr_single VALUES (6,'west');
SELECT region3, count(*) FROM dt_pr_single GROUP BY region3 ORDER BY region3;

-- ============================================================
-- Test 3: multi-column spec -- only the renamed token changes, order kept
--
-- Order is part of the Iceberg partition spec, so a rewrite that rebuilds the
-- list from an unordered source would corrupt the spec of every table with
-- more than one partition column.
-- ============================================================
SELECT test_log('Test 3: multi-column partition spec');

CREATE ICEBERG TABLE dt_pr_multi (id bigint, region text, dt text)
PARTITION BY (region, dt);
INSERT INTO dt_pr_multi VALUES (1,'east','d1'),(2,'west','d2');

ALTER TABLE dt_pr_multi RENAME COLUMN dt TO dt2;
SELECT dt_pr_spec('dt_pr_multi'::regclass) AS spec_after;

INSERT INTO dt_pr_multi VALUES (3,'east','d3');
SELECT id, region, dt2 FROM dt_pr_multi ORDER BY id;

-- ============================================================
-- Test 4: renaming a non-partition column leaves the spec alone
--
-- Guards the opposite failure: a rewrite keyed on the wrong thing could
-- clobber the declaration when an unrelated column is renamed.
-- ============================================================
SELECT test_log('Test 4: renaming a non-partition column');

CREATE ICEBERG TABLE dt_pr_other (id bigint, region text, payload text)
PARTITION BY (region);
INSERT INTO dt_pr_other VALUES (1,'east','p1');

ALTER TABLE dt_pr_other RENAME COLUMN payload TO payload2;
SELECT dt_pr_spec('dt_pr_other'::regclass) AS spec_unchanged;

INSERT INTO dt_pr_other VALUES (2,'west','p2');
SELECT id, region, payload2 FROM dt_pr_other ORDER BY id;

-- ============================================================
-- Test 5: a comma in a partition column name is rejected
--
-- ltoptions encodes the list comma-separated with no quoting, so a new name
-- containing a comma would turn one partition column into two.  CREATE TABLE
-- already rejects such a name; this is the same gate on the RENAME path.
-- The error is raised on a segment, and the dispatcher appends that segment's
-- address and pid to the message ("  (seg0 10.0.0.1:7002 pid=123)"), which
-- differs on every run and every machine.  Keep only the text before it.
-- ============================================================
SELECT test_log('Test 5: comma in a partition column name is rejected');

CREATE ICEBERG TABLE dt_pr_comma (id bigint, region text) PARTITION BY (region);
INSERT INTO dt_pr_comma VALUES (1,'east');

DO $$
BEGIN
    ALTER TABLE dt_pr_comma RENAME COLUMN region TO "we,ird";
    RAISE NOTICE 'UNEXPECTED: the rename was accepted';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'rejected: %', split_part(SQLERRM, '  (seg', 1);
END $$;

-- The rejection must have rolled the rename back: the column keeps its name,
-- the declaration is untouched, and the table still reads and writes.
SELECT attname FROM pg_attribute
 WHERE attrelid = 'dt_pr_comma'::regclass AND attnum > 0 AND NOT attisdropped
 ORDER BY attnum;
SELECT dt_pr_spec('dt_pr_comma'::regclass) AS spec_unchanged;
INSERT INTO dt_pr_comma VALUES (2,'west');
SELECT id, region FROM dt_pr_comma ORDER BY id;

-- A comma is only a problem for a partition column.
ALTER TABLE dt_pr_comma RENAME COLUMN id TO "i,d";
SELECT "i,d", region FROM dt_pr_comma ORDER BY 1;

-- ============================================================
-- Test 6: unpartitioned tables are unaffected
--
-- They carry no iceberg_partition_by option at all; this pins that the fix
-- did not add a requirement for one.
-- ============================================================
SELECT test_log('Test 6: unpartitioned table rename');

CREATE ICEBERG TABLE dt_pr_nopart (id bigint, region text);
INSERT INTO dt_pr_nopart VALUES (1,'east'),(2,'west');

ALTER TABLE dt_pr_nopart RENAME COLUMN region TO region2;
SELECT count(*) AS spec_rows FROM (SELECT dt_pr_spec('dt_pr_nopart'::regclass) s) t
 WHERE s IS NOT NULL;

INSERT INTO dt_pr_nopart VALUES (3,'north');
SELECT id, region2 FROM dt_pr_nopart ORDER BY id;

-- ============================================================
-- Test 7: a non-superuser table owner can rename a partition column
--
-- pg_iceberg_upsert_partition_by_option_local() is dispatched to every segment
-- in the invoking user's own session, so the ordinary case is a table owner who
-- is not a superuser.  Its gate is an ownership check in the C body rather than
-- REVOKE EXECUTE FROM PUBLIC, which would turn exactly this case into a
-- privilege error on the QEs.  Both halves are pinned here: the owner succeeds
-- end to end, and a non-owner calling the function directly is rejected.
-- ============================================================
SELECT test_log('Test 7: non-superuser owner');

CREATE ROLE dt_pr_owner;
CREATE ROLE dt_pr_stranger;
GRANT USAGE ON FOREIGN SERVER dt_pr_catalog_server TO dt_pr_owner;
GRANT USAGE ON FOREIGN SERVER dt_pr_volume_server TO dt_pr_owner;
CREATE USER MAPPING FOR dt_pr_owner SERVER dt_pr_catalog_server;
CREATE USER MAPPING FOR dt_pr_owner SERVER dt_pr_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
GRANT CREATE ON SCHEMA public TO dt_pr_owner;

SET ROLE dt_pr_owner;
SET iceberg_default_catalog = 'dt_pr_catalog';
SET iceberg_default_volume = 'dt_pr_volume';

CREATE ICEBERG TABLE dt_pr_acl (id bigint, region text) PARTITION BY (region);
INSERT INTO dt_pr_acl VALUES (1,'east');

ALTER TABLE dt_pr_acl RENAME COLUMN region TO area;
SELECT dt_pr_spec('dt_pr_acl'::regclass);

-- The write path is what the stale declaration used to break on the QEs.
INSERT INTO dt_pr_acl VALUES (2,'west');
SELECT id, area FROM dt_pr_acl ORDER BY id;

RESET ROLE;

-- A non-owner calling the segment-local half directly is still rejected.
SET ROLE dt_pr_stranger;
SELECT pg_catalog.pg_iceberg_upsert_partition_by_option_local(
           'dt_pr_acl'::regclass::oid, 'id');
RESET ROLE;

SELECT dt_pr_spec('dt_pr_acl'::regclass);

DROP TABLE dt_pr_acl;
DROP USER MAPPING FOR dt_pr_owner SERVER dt_pr_volume_server;
DROP USER MAPPING FOR dt_pr_owner SERVER dt_pr_catalog_server;
REVOKE CREATE ON SCHEMA public FROM dt_pr_owner;
REVOKE USAGE ON FOREIGN SERVER dt_pr_catalog_server FROM dt_pr_owner;
REVOKE USAGE ON FOREIGN SERVER dt_pr_volume_server FROM dt_pr_owner;
DROP ROLE dt_pr_owner;
DROP ROLE dt_pr_stranger;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE dt_pr_single;
DROP TABLE dt_pr_multi;
DROP TABLE dt_pr_other;
DROP TABLE dt_pr_comma;
DROP TABLE dt_pr_nopart;
DROP FUNCTION dt_pr_spec(regclass);
DROP VOLUME dt_pr_volume;
DROP USER MAPPING FOR current_user SERVER dt_pr_volume_server;
DROP SERVER dt_pr_volume_server;
DROP CATALOG dt_pr_catalog;
DROP USER MAPPING FOR current_user SERVER dt_pr_catalog_server;
DROP SERVER dt_pr_catalog_server;
