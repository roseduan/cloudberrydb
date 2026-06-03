-- 01_vacuum_basic.sql
-- Test Iceberg VACUUM basic operations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg VACUUM Basic');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER vb_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vb_catalog_server;
CREATE FOREIGN CATALOG vb_catalog SERVER vb_catalog_server;
SET iceberg_default_catalog = 'vb_catalog';

CREATE SERVER vb_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER vb_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME vb_volume SERVER vb_volume_server OPTIONS(base_path '/vb_volume/');
SET iceberg_default_volume = 'vb_volume';

-- ============================================================
-- Test 1: Create table with multiple separate INSERTs
-- ============================================================
SELECT test_log('Test 1: Multiple separate INSERTs to create multiple files');

CREATE ICEBERG TABLE vb_compact (id bigint, val int);
INSERT INTO vb_compact VALUES (1, 100);
INSERT INTO vb_compact VALUES (2, 200);
INSERT INTO vb_compact VALUES (3, 300);
INSERT INTO vb_compact VALUES (4, 400);
INSERT INTO vb_compact VALUES (5, 500);

-- ============================================================
-- Test 2: Verify data before VACUUM
-- ============================================================
SELECT test_log('Test 2: Verify data before VACUUM');

SELECT COUNT(*) AS row_count FROM vb_compact;
SELECT SUM(val) AS total_val FROM vb_compact;

-- ============================================================
-- Test 3: VACUUM the table
-- ============================================================
SELECT test_log('Test 3: VACUUM table');

SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM vb_compact;

-- ============================================================
-- Test 4: Verify data integrity after VACUUM
-- ============================================================
SELECT test_log('Test 4: Verify data integrity after VACUUM');

SELECT COUNT(*) AS row_count FROM vb_compact;
SELECT SUM(val) AS total_val FROM vb_compact;
SELECT * FROM vb_compact ORDER BY id;

-- ============================================================
-- Test 5: ANALYZE table
-- ============================================================
SELECT test_log('Test 5: ANALYZE table');

ANALYZE vb_compact;

-- ============================================================
-- Test 6: Continue INSERT + SELECT after VACUUM
-- ============================================================
SELECT test_log('Test 6: INSERT + SELECT after VACUUM');

INSERT INTO vb_compact VALUES (6, 600), (7, 700);
SELECT COUNT(*) AS final_count FROM vb_compact;
SELECT SUM(val) AS final_sum FROM vb_compact;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE vb_compact;
DROP VOLUME vb_volume;
DROP USER MAPPING FOR current_user SERVER vb_volume_server;
DROP SERVER vb_volume_server;
DROP CATALOG vb_catalog;
DROP USER MAPPING FOR current_user SERVER vb_catalog_server;
DROP SERVER vb_catalog_server;
