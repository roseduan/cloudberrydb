-- 04_partition_by.sql
-- Test Iceberg CREATE TABLE ... PARTITION BY (col, ...)
--
-- PARTITION BY on a lake table is NOT PostgreSQL native partitioning: no
-- parent/child tables are built (relkind stays 'r'), the declaration is
-- recorded in pg_lake_table.ltoptions as "iceberg_partition_by=col1,col2"
-- for later translation into an Iceberg partition spec.

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg CREATE TABLE PARTITION BY');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER dt_pb_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dt_pb_catalog_server;
CREATE FOREIGN CATALOG dt_pb_catalog SERVER dt_pb_catalog_server;
SET iceberg_default_catalog = 'dt_pb_catalog';

CREATE SERVER dt_pb_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER dt_pb_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dt_pb_volume SERVER dt_pb_volume_server
OPTIONS (base_path '/dt_pb_volume/');
SET iceberg_default_volume = 'dt_pb_volume';

-- ============================================================
-- Test 1: Single identity partition column
-- ============================================================
SELECT test_log('Test 1: Single partition column');

CREATE ICEBERG TABLE dt_pb_single (id bigint, region text) PARTITION BY (region);

-- Must NOT be PG native partitioning: plain relation, not partitioned parent
SELECT relkind, relispartition FROM pg_class WHERE relname = 'dt_pb_single';

-- Declaration recorded in ltoptions
SELECT o AS ltopt
FROM (SELECT unnest(ltoptions) AS o
      FROM pg_lake_table
      WHERE ltrelid = 'dt_pb_single'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- ============================================================
-- Test 2: Multiple partition columns keep declaration order
-- ============================================================
SELECT test_log('Test 2: Multiple partition columns');

CREATE ICEBERG TABLE dt_pb_multi (id bigint, region text, dt date, val int)
PARTITION BY (region, dt);

SELECT o AS ltopt
FROM (SELECT unnest(ltoptions) AS o
      FROM pg_lake_table
      WHERE ltrelid = 'dt_pb_multi'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- ============================================================
-- Test 3: INSERT routes each row into its Iceberg partition and reads
-- back (identity, incl. a NULL partition value); partition-aware
-- UPDATE/DELETE now work via merge-on-read position deletes.
-- ============================================================
SELECT test_log('Test 3: partitioned INSERT/SELECT + partition-aware UPDATE/DELETE');

INSERT INTO dt_pb_single VALUES (1, 'east'), (2, 'west'), (3, 'east'), (4, NULL), (5, 'west');

-- Read back: per-partition counts (NULL partition included) and total
SELECT region, count(*) FROM dt_pb_single GROUP BY region ORDER BY region;
SELECT count(*) AS total FROM dt_pb_single;

-- Partition predicate returns exactly the matching rows
SELECT id FROM dt_pb_single WHERE region = 'east' ORDER BY id;

-- DELETE within a single partition
DELETE FROM dt_pb_single WHERE id = 2;                 -- removes one 'west' row
SELECT region, count(*) FROM dt_pb_single GROUP BY region ORDER BY region;

-- UPDATE of a non-partition column value stays in the same partition
UPDATE dt_pb_single SET id = 30 WHERE id = 3;          -- still 'east'
SELECT id, region FROM dt_pb_single WHERE region = 'east' ORDER BY id;

-- UPDATE that changes the partition key moves the row across partitions
UPDATE dt_pb_single SET region = 'north' WHERE id = 1; -- 'east' -> 'north'
SELECT region, count(*) FROM dt_pb_single GROUP BY region ORDER BY region;

-- DELETE of the NULL-partition row
DELETE FROM dt_pb_single WHERE id = 4;                 -- removes the NULL-partition row
SELECT region, count(*) FROM dt_pb_single GROUP BY region ORDER BY region;
SELECT count(*) AS total FROM dt_pb_single;

-- ============================================================
-- Test 4: IF NOT EXISTS variant accepts PARTITION BY
-- ============================================================
SELECT test_log('Test 4: CREATE ICEBERG TABLE IF NOT EXISTS ... PARTITION BY');

CREATE ICEBERG TABLE IF NOT EXISTS dt_pb_ine (id bigint, region text)
PARTITION BY (region);

SELECT o AS ltopt
FROM (SELECT unnest(ltoptions) AS o
      FROM pg_lake_table
      WHERE ltrelid = 'dt_pb_ine'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- Second create with IF NOT EXISTS is a no-op NOTICE, not an error
CREATE ICEBERG TABLE IF NOT EXISTS dt_pb_ine (id bigint, region text)
PARTITION BY (region);

-- ============================================================
-- Test 5: Table without PARTITION BY records nothing (no regression)
-- ============================================================
SELECT test_log('Test 5: No PARTITION BY leaves ltoptions untouched');

CREATE ICEBERG TABLE dt_pb_none (id bigint, region text);

-- unpartitioned tables keep full write support
INSERT INTO dt_pb_none VALUES (1, 'east'), (2, 'west');
SELECT count(*) FROM dt_pb_none;

SELECT relkind FROM pg_class WHERE relname = 'dt_pb_none';
SELECT count(*) AS partition_by_options
FROM (SELECT unnest(ltoptions) AS o
      FROM pg_lake_table
      WHERE ltrelid = 'dt_pb_none'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- ============================================================
-- Test 6: Error on nonexistent partition column
-- ============================================================
SELECT test_log('Test 6: Nonexistent partition column errors out');

CREATE ICEBERG TABLE dt_pb_bad (id bigint, region text) PARTITION BY (nope);

-- The failed create must not leave anything behind
SELECT count(*) AS leftover FROM pg_class WHERE relname = 'dt_pb_bad';

-- ============================================================
-- Test 7: Drop and recreate with a different partition column
-- ============================================================
SELECT test_log('Test 7: Drop and recreate with different partition column');

DROP TABLE dt_pb_single;
CREATE ICEBERG TABLE dt_pb_single (id bigint, region text, dt date)
PARTITION BY (dt);

SELECT o AS ltopt
FROM (SELECT unnest(ltoptions) AS o
      FROM pg_lake_table
      WHERE ltrelid = 'dt_pb_single'::regclass) s
WHERE o LIKE 'iceberg_partition_by%';

-- ============================================================
-- Test 8: A column may appear only once in PARTITION BY
-- ============================================================
SELECT test_log('Test 8: duplicate partition column errors out');

CREATE ICEBERG TABLE dt_pb_dup (id bigint, region text) PARTITION BY (region, region);

SELECT count(*) AS leftover FROM pg_class WHERE relname = 'dt_pb_dup';

-- ============================================================
-- Test 9: PARTITION BY is only for Iceberg lake tables (not HUDI)
-- ============================================================
SELECT test_log('Test 9: PARTITION BY rejected for non-Iceberg lake table');

CREATE HUDI TABLE dt_pb_hudi (id bigint, region text) PARTITION BY (region);

SELECT count(*) AS leftover FROM pg_class WHERE relname = 'dt_pb_hudi';

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE dt_pb_single;
DROP TABLE dt_pb_multi;
DROP TABLE dt_pb_ine;
DROP TABLE dt_pb_none;

DROP VOLUME dt_pb_volume;
DROP USER MAPPING FOR current_user SERVER dt_pb_volume_server;
DROP SERVER dt_pb_volume_server;
DROP CATALOG dt_pb_catalog;
DROP USER MAPPING FOR current_user SERVER dt_pb_catalog_server;
DROP SERVER dt_pb_catalog_server;
