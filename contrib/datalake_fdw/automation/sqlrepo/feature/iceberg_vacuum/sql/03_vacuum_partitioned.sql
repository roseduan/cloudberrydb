-- 03_vacuum_partitioned.sql
-- Test Iceberg VACUUM (compaction) on a PARTITION BY table: rewrite must be
-- partition-aware -- each compacted file keeps its partition, and position
-- deletes are materialized per partition during compaction.

\i ../../../lib/sql/common_setup.sql

-- See 01_vacuum_basic.sql: suppress the non-deterministic ORCA missing-stats
-- NOTICE for Iceberg AM tables.
SET optimizer_print_missing_stats = off;

SELECT test_log('Feature Test: Iceberg VACUUM on partitioned table');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER vp_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vp_catalog_server;
CREATE FOREIGN CATALOG vp_catalog SERVER vp_catalog_server;
SET iceberg_default_catalog = 'vp_catalog';

CREATE SERVER vp_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER vp_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME vp_volume SERVER vp_volume_server OPTIONS(base_path '/vp_volume/');
SET iceberg_default_volume = 'vp_volume';

-- ============================================================
-- Test 1: Multiple separate INSERTs create multiple small files per partition
-- ============================================================
SELECT test_log('Test 1: Multiple INSERTs into a partitioned table');

CREATE ICEBERG TABLE vp_compact (id bigint, region text) PARTITION BY (region);
INSERT INTO vp_compact VALUES (1, 'east'), (2, 'west');
INSERT INTO vp_compact VALUES (3, 'east'), (4, 'west');
INSERT INTO vp_compact VALUES (5, 'east'), (6, 'west');

-- ============================================================
-- Test 2: Verify data before VACUUM
-- ============================================================
SELECT test_log('Test 2: Verify data before VACUUM');

SELECT region, count(*) FROM vp_compact GROUP BY region ORDER BY region;
SELECT count(*) AS total FROM vp_compact;

-- ============================================================
-- Test 3: VACUUM compacts per partition (must succeed, data unchanged)
-- ============================================================
SELECT test_log('Test 3: VACUUM partitioned table');

SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM vp_compact;

SELECT region, count(*) FROM vp_compact GROUP BY region ORDER BY region;
SELECT count(*) AS total FROM vp_compact;
SELECT id, region FROM vp_compact ORDER BY id;

-- ============================================================
-- Test 4: VACUUM after DELETE materializes position deletes per partition
-- ============================================================
SELECT test_log('Test 4: DELETE then VACUUM (MOR + compaction)');

DELETE FROM vp_compact WHERE id = 3;              -- east
INSERT INTO vp_compact VALUES (7, 'east');
INSERT INTO vp_compact VALUES (8, 'east');
VACUUM vp_compact;

-- id = 3 stays deleted (not resurrected by compaction); no duplicates
SELECT region, count(*) FROM vp_compact GROUP BY region ORDER BY region;
SELECT id, region FROM vp_compact ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE vp_compact;

DROP VOLUME vp_volume;
DROP USER MAPPING FOR current_user SERVER vp_volume_server;
DROP SERVER vp_volume_server;
DROP CATALOG vp_catalog;
DROP USER MAPPING FOR current_user SERVER vp_catalog_server;
DROP SERVER vp_catalog_server;
