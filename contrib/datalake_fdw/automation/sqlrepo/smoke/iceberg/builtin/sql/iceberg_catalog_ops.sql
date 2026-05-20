-- Iceberg Catalog FDW Operations Test
-- Purpose: Exercise iceberg_toolkit_catalog_fdw.c operations
-- Target: iceberg_toolkit_catalog_fdw.c (+133), iceberg_catalog_fdw.c (+84),
--         iceberg_toolkit_common.c (+119), agent_cli_wrapper.c (+115)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER co_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER co_catalog_server;
CREATE FOREIGN CATALOG co_catalog SERVER co_catalog_server;
SET iceberg_default_catalog = 'co_catalog';

CREATE SERVER co_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER co_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME co_volume SERVER co_volume_server OPTIONS(base_path '/co_volume/');
SET iceberg_default_volume = 'co_volume';

-- ============================================================
-- Test 1: get_statistics operation
-- ============================================================
CREATE ICEBERG TABLE co_stats (id bigint, name text, val int);
INSERT INTO co_stats VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);
INSERT INTO co_stats VALUES (4, 'd', 40), (5, 'e', 50);

SELECT
    result::json->'fragments'->>'total-records' AS total_records,
    (result::json->'fragments'->>'total-files-size')::bigint > 0 AS has_size
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'co_stats',
        'co_catalog_server', 'co_catalog',
        'co_volume_server', 'co_volume', '') AS result
) t;

-- ============================================================
-- Test 2: load_table operation
-- ============================================================
DO $$
DECLARE
    result text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'load_table', 'public', 'co_stats',
        'co_catalog_server', 'co_catalog',
        'co_volume_server', 'co_volume', '')
    INTO result;
    IF result IS NOT NULL THEN
        RAISE NOTICE 'load_table: OK';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'load_table error: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 3: plan_file_groups operation (compaction planning)
-- ============================================================
-- Create multiple data files to have something to plan
INSERT INTO co_stats VALUES (6, 'f', 60);
INSERT INTO co_stats VALUES (7, 'g', 70);
INSERT INTO co_stats VALUES (8, 'h', 80);

SELECT
    result::json->>'status' AS status
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'co_stats',
        'co_catalog_server', 'co_catalog',
        'co_volume_server', 'co_volume', '') AS result
) t;

-- Plan with custom parameters
SELECT
    result::json->>'status' AS status
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'co_stats',
        'co_catalog_server', 'co_catalog',
        'co_volume_server', 'co_volume',
        '{"minInputFiles": 2, "targetFileSizeMb": 128}') AS result
) t;

-- ============================================================
-- Test 4: VACUUM exercises commit_file_groups path
-- ============================================================
SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM co_stats;
RESET datalake.iceberg_vacuum_compact_min_input_files;

SELECT COUNT(*) FROM co_stats;

-- ============================================================
-- Test 5: ANALYZE exercises get_statistics + catalog metadata
-- ============================================================
ANALYZE co_stats;

-- ============================================================
-- Test 6: Multiple tables in same namespace (catalog list paths)
-- ============================================================
CREATE ICEBERG TABLE co_table2 (id bigint, data text);
INSERT INTO co_table2 VALUES (1, 'table2_a'), (2, 'table2_b');

CREATE ICEBERG TABLE co_table3 (id bigint);
INSERT INTO co_table3 VALUES (1), (2), (3);

SELECT COUNT(*) FROM co_stats;
SELECT COUNT(*) FROM co_table2;
SELECT COUNT(*) FROM co_table3;

-- ============================================================
-- Test 7: DML exercises commit_append, commit_update, commit_delete
-- ============================================================
-- commit_append
INSERT INTO co_stats SELECT i + 100, 'bulk_' || i, i FROM generate_series(1, 20) i;
SELECT COUNT(*) FROM co_stats;

-- commit_update (via UPDATE)
UPDATE co_stats SET name = 'modified' WHERE id > 105;
SELECT COUNT(*) FROM co_stats WHERE name = 'modified';

-- commit_delete (via DELETE)
DELETE FROM co_stats WHERE id > 110;
SELECT COUNT(*) FROM co_stats;

-- ============================================================
-- Test 8: Error handling (non-existent table)
-- ============================================================
DO $$
BEGIN
    PERFORM iceberg_toolkit.catalog_fdw(
        'load_table', 'public', 'co_nonexistent_xyz',
        'co_catalog_server', 'co_catalog',
        'co_volume_server', 'co_volume', '');
    RAISE NOTICE 'Unexpected success';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 9: Create and drop tables (exercises create_table, drop_table paths)
-- ============================================================
CREATE ICEBERG TABLE co_temp1 (id bigint);
INSERT INTO co_temp1 VALUES (1);
DROP TABLE co_temp1;

CREATE ICEBERG TABLE co_temp2 (id bigint, name text, val decimal(10,2));
INSERT INTO co_temp2 VALUES (1, 'test', 99.99);
SELECT * FROM co_temp2;
DROP TABLE co_temp2;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE co_stats;
DROP TABLE co_table2;
DROP TABLE co_table3;
DROP VOLUME co_volume;
DROP USER MAPPING FOR current_user SERVER co_volume_server;
DROP SERVER co_volume_server;
DROP CATALOG co_catalog;
DROP USER MAPPING FOR current_user SERVER co_catalog_server;
DROP SERVER co_catalog_server;
