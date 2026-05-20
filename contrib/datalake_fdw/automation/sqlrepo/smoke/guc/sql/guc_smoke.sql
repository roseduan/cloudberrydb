-- GUC Parameters Smoke Test
-- Purpose: Verify datalake_fdw GUC parameters work correctly
-- Tests each GUC can be SET and affects behavior

-- Setup extensions
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: Iceberg builtin catalog + S3 volume
-- ============================================================
CREATE SERVER guc_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER guc_catalog_server;
CREATE FOREIGN CATALOG guc_catalog SERVER guc_catalog_server;
SET iceberg_default_catalog = 'guc_catalog';

CREATE SERVER guc_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER guc_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME guc_volume SERVER guc_volume_server OPTIONS(base_path '/guc_volume/');
SET iceberg_default_volume = 'guc_volume';

-- Create test data
CREATE ICEBERG TABLE guc_test (
    id bigint,
    name text,
    val int);

INSERT INTO guc_test VALUES
    (1, 'Alice', 100),
    (2, 'Bob', 200),
    (3, 'Charlie', 300);

-- ============================================================
-- Test 1: datalake.disable_filter_pushdown
-- ============================================================
-- Show default value
SHOW datalake.disable_filter_pushdown;

-- Results with pushdown (default)
SELECT id, name FROM guc_test WHERE id = 2;

-- Disable pushdown - same results expected
SET datalake.disable_filter_pushdown = on;
SHOW datalake.disable_filter_pushdown;
SELECT id, name FROM guc_test WHERE id = 2;

-- Reset
SET datalake.disable_filter_pushdown = off;

-- ============================================================
-- Test 2: datalake.external_table_debug
-- ============================================================
SHOW datalake.external_table_debug;

-- Enable debug mode (should produce debug output in log)
SET datalake.external_table_debug = on;
SELECT COUNT(*) FROM guc_test;

-- Reset
SET datalake.external_table_debug = off;

-- ============================================================
-- Test 3: datalake.disable_cache_file
-- ============================================================
SHOW datalake.disable_cache_file;

SET datalake.disable_cache_file = on;
SELECT COUNT(*) FROM guc_test;

SET datalake.disable_cache_file = off;
SELECT COUNT(*) FROM guc_test;

-- ============================================================
-- Test 4: datalake.iceberg_postion_deletes_threshold
-- ============================================================
SHOW datalake.iceberg_postion_deletes_threshold;

-- Set to a different valid value (valid range: 100000..10000000)
SET datalake.iceberg_postion_deletes_threshold = 200000;
SHOW datalake.iceberg_postion_deletes_threshold;

-- DELETE should still work (tests threshold path)
DELETE FROM guc_test WHERE id = 3;
SELECT * FROM guc_test ORDER BY id;

-- Reset to default
SET datalake.iceberg_postion_deletes_threshold = 100000;

-- Re-insert for further tests
INSERT INTO guc_test VALUES (3, 'Charlie', 300);

-- ============================================================
-- Test 5: datalake.external_table_ignore_hidden_file
-- ============================================================
SHOW datalake.external_table_ignore_hidden_file;

SET datalake.external_table_ignore_hidden_file = on;
SELECT COUNT(*) FROM guc_test;

SET datalake.external_table_ignore_hidden_file = off;
SELECT COUNT(*) FROM guc_test;

-- ============================================================
-- Test 6: datalake.enable_list_in_master
-- ============================================================
SHOW datalake.enable_list_in_master;

SET datalake.enable_list_in_master = on;
SELECT COUNT(*) FROM guc_test;

SET datalake.enable_list_in_master = off;

-- ============================================================
-- Test 7: datalake.external_table_limit_segment_num
-- ============================================================
SHOW datalake.external_table_limit_segment_num;

-- Limit to 1 segment
SET datalake.external_table_limit_segment_num = 1;
SELECT COUNT(*) FROM guc_test;

-- Reset to default (0 = no limit)
SET datalake.external_table_limit_segment_num = 0;

-- ============================================================
-- Test 8: datalake.hudi_log_merger_threshold
-- ============================================================
SHOW datalake.hudi_log_merger_threshold;

SET datalake.hudi_log_merger_threshold = 256;
SHOW datalake.hudi_log_merger_threshold;

-- Reset
SET datalake.hudi_log_merger_threshold = 512;

-- ============================================================
-- Test 9: datalake.hudi_log_scale_factor
-- ============================================================
SHOW datalake.hudi_log_scale_factor;

SET datalake.hudi_log_scale_factor = 2.0;
SHOW datalake.hudi_log_scale_factor;

-- Reset
SET datalake.hudi_log_scale_factor = 1.3;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE guc_test;
DROP VOLUME guc_volume;
DROP USER MAPPING FOR current_user SERVER guc_volume_server;
DROP SERVER guc_volume_server;
DROP CATALOG guc_catalog;
DROP USER MAPPING FOR current_user SERVER guc_catalog_server;
DROP SERVER guc_catalog_server;
