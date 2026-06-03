-- 02_catalog_multi.sql
-- Test multiple Iceberg catalogs coexisting

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Multiple Iceberg Catalogs');

-- ============================================================
-- Setup: Create two independent catalogs with separate volumes
-- ============================================================
SELECT test_log('Setup: Create two catalogs');

-- Catalog A
CREATE SERVER cm_cat_a_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cm_cat_a_server;
CREATE FOREIGN CATALOG cm_catalog_a SERVER cm_cat_a_server;

CREATE SERVER cm_vol_a_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER cm_vol_a_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cm_volume_a SERVER cm_vol_a_server OPTIONS(base_path '/cm_vol_a/');

-- Catalog B
CREATE SERVER cm_cat_b_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cm_cat_b_server;
CREATE FOREIGN CATALOG cm_catalog_b SERVER cm_cat_b_server;

CREATE SERVER cm_vol_b_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER cm_vol_b_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cm_volume_b SERVER cm_vol_b_server OPTIONS(base_path '/cm_vol_b/');

-- ============================================================
-- Test 1: Create tables in each catalog
-- ============================================================
SELECT test_log('Test 1: Create tables in separate catalogs');

SET iceberg_default_catalog = 'cm_catalog_a';
SET iceberg_default_volume = 'cm_volume_a';
CREATE ICEBERG TABLE cm_data_a (id bigint, source text);
INSERT INTO cm_data_a VALUES (1, 'catalog_a'), (2, 'catalog_a');

SET iceberg_default_catalog = 'cm_catalog_b';
SET iceberg_default_volume = 'cm_volume_b';
CREATE ICEBERG TABLE cm_data_b (id bigint, source text);
INSERT INTO cm_data_b VALUES (10, 'catalog_b'), (20, 'catalog_b'), (30, 'catalog_b');

-- ============================================================
-- Test 2: Verify data isolation between catalogs
-- ============================================================
SELECT test_log('Test 2: Verify data isolation');

SELECT COUNT(*) AS catalog_a_rows FROM cm_data_a;
SELECT COUNT(*) AS catalog_b_rows FROM cm_data_b;

SELECT * FROM cm_data_a ORDER BY id;
SELECT * FROM cm_data_b ORDER BY id;

-- ============================================================
-- Test 3: Drop one catalog, verify other is unaffected
-- ============================================================
SELECT test_log('Test 3: Drop catalog A, verify B unaffected');

DROP TABLE cm_data_a;
DROP VOLUME cm_volume_a;
DROP USER MAPPING FOR current_user SERVER cm_vol_a_server;
DROP SERVER cm_vol_a_server;
DROP CATALOG cm_catalog_a;
DROP USER MAPPING FOR current_user SERVER cm_cat_a_server;
DROP SERVER cm_cat_a_server;

-- Catalog B should still work
SELECT COUNT(*) AS catalog_b_still_works FROM cm_data_b;
SELECT * FROM cm_data_b ORDER BY id;

-- ============================================================
-- Cleanup: Drop catalog B
-- ============================================================
DROP TABLE cm_data_b;
DROP VOLUME cm_volume_b;
DROP USER MAPPING FOR current_user SERVER cm_vol_b_server;
DROP SERVER cm_vol_b_server;
DROP CATALOG cm_catalog_b;
DROP USER MAPPING FOR current_user SERVER cm_cat_b_server;
DROP SERVER cm_cat_b_server;
