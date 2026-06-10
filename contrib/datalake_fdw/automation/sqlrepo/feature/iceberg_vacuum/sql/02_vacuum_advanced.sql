-- 02_vacuum_advanced.sql
-- Test Iceberg VACUUM advanced operations with toolkit

\i ../../../lib/sql/common_setup.sql

-- See 01_vacuum_basic.sql: the ORCA "missing statistics" NOTICE for an Iceberg
-- AM table is non-deterministic after VACUUM/ANALYZE/INSERT.  Suppress it for
-- stable regression output.
SET optimizer_print_missing_stats = off;

SELECT test_log('Feature Test: Iceberg VACUUM Advanced');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER va_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER va_catalog_server;
CREATE FOREIGN CATALOG va_catalog SERVER va_catalog_server;
SET iceberg_default_catalog = 'va_catalog';

CREATE SERVER va_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER va_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME va_volume SERVER va_volume_server OPTIONS(base_path '/va_volume/');
SET iceberg_default_volume = 'va_volume';

CREATE ICEBERG TABLE va_toolkit (id bigint, val int, label text);
INSERT INTO va_toolkit VALUES (1, 100, 'a');
INSERT INTO va_toolkit VALUES (2, 200, 'b');
INSERT INTO va_toolkit VALUES (3, 300, 'c');
INSERT INTO va_toolkit VALUES (4, 400, 'd');
INSERT INTO va_toolkit VALUES (5, 500, 'e');

-- ============================================================
-- Test 1: plan_file_groups via toolkit
-- ============================================================
SELECT test_log('Test 1: plan_file_groups via iceberg_toolkit');

SELECT
    result::json->>'status' AS status
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'va_toolkit',
        'va_catalog_server', 'va_catalog',
        'va_volume_server', 'va_volume', '') AS result
) t;

-- ============================================================
-- Test 2: plan_file_groups with custom params
-- ============================================================
SELECT test_log('Test 2: plan_file_groups with custom params');

SELECT
    result::json->>'status' AS status
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'va_toolkit',
        'va_catalog_server', 'va_catalog',
        'va_volume_server', 'va_volume',
        '{"minInputFiles": 2, "targetFileSizeMb": 128}') AS result
) t;

-- ============================================================
-- Test 3: get_statistics
-- ============================================================
SELECT test_log('Test 3: get_statistics');

SELECT
    result::json->'fragments'->>'total-records' AS total_records,
    (result::json->'fragments'->>'total-files-size')::bigint > 0 AS has_size
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'va_toolkit',
        'va_catalog_server', 'va_catalog',
        'va_volume_server', 'va_volume', '') AS result
) t;

-- ============================================================
-- Test 4: VACUUM + DML interleave
-- ============================================================
SELECT test_log('Test 4: VACUUM + DML interleave');

-- First VACUUM
SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM va_toolkit;

-- Verify data intact
SELECT COUNT(*) AS after_vacuum FROM va_toolkit;
SELECT SUM(val) AS sum_after_vacuum FROM va_toolkit;

-- More DML after VACUUM
INSERT INTO va_toolkit VALUES (6, 600, 'f'), (7, 700, 'g');
UPDATE va_toolkit SET val = val + 1 WHERE id = 1;
DELETE FROM va_toolkit WHERE id = 7;

-- Verify final state
SELECT COUNT(*) AS final_count FROM va_toolkit;
SELECT SUM(val) AS final_sum FROM va_toolkit;
SELECT * FROM va_toolkit ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE va_toolkit;
DROP VOLUME va_volume;
DROP USER MAPPING FOR current_user SERVER va_volume_server;
DROP SERVER va_volume_server;
DROP CATALOG va_catalog;
DROP USER MAPPING FOR current_user SERVER va_catalog_server;
DROP SERVER va_catalog_server;
