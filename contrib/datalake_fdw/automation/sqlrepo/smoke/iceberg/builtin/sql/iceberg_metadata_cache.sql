-- Iceberg Metadata Cache Test
-- Purpose: Exercise fragment cache lookup/store/eviction paths
-- Target: iceberg_fragment_cache.c, datalake.enable_iceberg_fragment_cache GUC

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup
CREATE SERVER cache_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cache_catalog_server;
CREATE FOREIGN CATALOG cache_catalog SERVER cache_catalog_server;
SET iceberg_default_catalog = 'cache_catalog';

CREATE SERVER cache_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER cache_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cache_volume SERVER cache_volume_server OPTIONS(base_path '/cache_volume/');
SET iceberg_default_volume = 'cache_volume';

-- ============================================================
-- Test 1: Cache enabled (default) - repeated reads
-- ============================================================
SHOW datalake.enable_iceberg_fragment_cache;

CREATE ICEBERG TABLE cache_test_1 (id bigint, name text);
INSERT INTO cache_test_1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');

-- First read (populates cache)
SELECT COUNT(*) FROM cache_test_1;

-- Second read (should hit cache - same snapshot)
SELECT COUNT(*) FROM cache_test_1;

-- Third read with filter (same table, cache should be used)
SELECT * FROM cache_test_1 WHERE id = 2;

-- ============================================================
-- Test 2: Cache disabled - bypass cache
-- ============================================================
SET datalake.enable_iceberg_fragment_cache = off;
SHOW datalake.enable_iceberg_fragment_cache;

-- Read should bypass cache
SELECT COUNT(*) FROM cache_test_1;
SELECT * FROM cache_test_1 WHERE id = 1;

-- Re-enable
SET datalake.enable_iceberg_fragment_cache = on;

-- ============================================================
-- Test 3: Cache invalidation after INSERT (snapshot changes)
-- ============================================================

-- Read populates cache
SELECT COUNT(*) FROM cache_test_1;

-- INSERT changes the snapshot
INSERT INTO cache_test_1 VALUES (4, 'Diana');

-- Next read should see new data (cache invalidated by new snapshot)
SELECT COUNT(*) FROM cache_test_1;
SELECT * FROM cache_test_1 ORDER BY id;

-- ============================================================
-- Test 4: Cache invalidation after UPDATE
-- ============================================================
SELECT * FROM cache_test_1 WHERE id = 1;

UPDATE cache_test_1 SET name = 'Alice Updated' WHERE id = 1;

-- Should see updated data
SELECT * FROM cache_test_1 WHERE id = 1;

-- ============================================================
-- Test 5: Cache invalidation after DELETE
-- ============================================================
SELECT COUNT(*) FROM cache_test_1;

DELETE FROM cache_test_1 WHERE id = 4;

-- Should reflect deletion
SELECT COUNT(*) FROM cache_test_1;

-- ============================================================
-- Test 6: Multiple tables - cache stores per-table entries
-- ============================================================
CREATE ICEBERG TABLE cache_test_2 (id bigint, val int);
INSERT INTO cache_test_2 VALUES (10, 100), (20, 200);

CREATE ICEBERG TABLE cache_test_3 (id bigint, tag text);
INSERT INTO cache_test_3 VALUES (100, 'x'), (200, 'y'), (300, 'z');

-- Read all three tables (populates cache for each)
SELECT COUNT(*) AS t1_count FROM cache_test_1;
SELECT COUNT(*) AS t2_count FROM cache_test_2;
SELECT COUNT(*) AS t3_count FROM cache_test_3;

-- Read again (should hit cache for each)
SELECT COUNT(*) AS t1_count FROM cache_test_1;
SELECT COUNT(*) AS t2_count FROM cache_test_2;
SELECT COUNT(*) AS t3_count FROM cache_test_3;

-- Modify one table, others should still use cache
INSERT INTO cache_test_2 VALUES (30, 300);
SELECT COUNT(*) AS t2_count FROM cache_test_2;

-- Other tables unaffected
SELECT COUNT(*) AS t1_count FROM cache_test_1;
SELECT COUNT(*) AS t3_count FROM cache_test_3;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE cache_test_3;
DROP TABLE cache_test_2;
DROP TABLE cache_test_1;
DROP VOLUME cache_volume;
DROP USER MAPPING FOR current_user SERVER cache_volume_server;
DROP SERVER cache_volume_server;
DROP CATALOG cache_catalog;
DROP USER MAPPING FOR current_user SERVER cache_catalog_server;
DROP SERVER cache_catalog_server;
