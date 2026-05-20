-- Iceberg Toolkit Error Handling Test
-- Purpose: Exercise error response formatting and edge cases
-- Target: iceberg_toolkit_common.c error paths (+119),
--         agent_cli_wrapper.c error handling (+115),
--         iceberg_catalog_fdw.c error paths

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE SERVER te_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER te_catalog_server;
CREATE FOREIGN CATALOG te_catalog SERVER te_catalog_server;
SET iceberg_default_catalog = 'te_catalog';

CREATE SERVER te_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER te_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME te_volume SERVER te_volume_server OPTIONS(base_path '/te_volume/');
SET iceberg_default_volume = 'te_volume';

-- ============================================================
-- Test 1: Load non-existent table (error response formatting)
-- ============================================================
DO $$
BEGIN
    PERFORM iceberg_toolkit.catalog_fdw(
        'load_table', 'public', 'te_nonexistent_table_xyz_abc',
        'te_catalog_server', 'te_catalog',
        'te_volume_server', 'te_volume', '');
    RAISE NOTICE 'Unexpected: no error';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error caught: %', substring(SQLERRM from 1 for 100);
END;
$$;

-- ============================================================
-- Test 2: Get statistics on non-existent table
-- ============================================================
DO $$
BEGIN
    PERFORM iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'te_missing_stats_table',
        'te_catalog_server', 'te_catalog',
        'te_volume_server', 'te_volume', '');
    RAISE NOTICE 'Unexpected: no error';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Stats error: %', substring(SQLERRM from 1 for 100);
END;
$$;

-- ============================================================
-- Test 3: Plan file groups on non-existent table
-- ============================================================
DO $$
BEGIN
    PERFORM iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'te_missing_plan_table',
        'te_catalog_server', 'te_catalog',
        'te_volume_server', 'te_volume', '');
    RAISE NOTICE 'Unexpected: no error';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Plan error: %', substring(SQLERRM from 1 for 100);
END;
$$;

-- ============================================================
-- Test 4: Invalid namespace
-- ============================================================
DO $$
BEGIN
    PERFORM iceberg_toolkit.catalog_fdw(
        'load_table', 'nonexistent_namespace_xyz', 'some_table',
        'te_catalog_server', 'te_catalog',
        'te_volume_server', 'te_volume', '');
    RAISE NOTICE 'Unexpected: no error';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Namespace error: %', substring(SQLERRM from 1 for 100);
END;
$$;

-- ============================================================
-- Test 5: Create table, then try to create again (duplicate error)
-- ============================================================
CREATE ICEBERG TABLE te_dup_test (id bigint, val text);
INSERT INTO te_dup_test VALUES (1, 'test');

-- This should handle the duplicate gracefully
DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE te_dup_test (id bigint, val text)';
    RAISE NOTICE 'Unexpected: no error on duplicate';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Duplicate error: %', substring(SQLERRM from 1 for 100);
END;
$$;

-- IF NOT EXISTS should succeed silently
CREATE ICEBERG TABLE IF NOT EXISTS te_dup_test (id bigint, val text);

SELECT COUNT(*) FROM te_dup_test;
DROP TABLE te_dup_test;

-- ============================================================
-- Test 6: Operations on empty table (edge case)
-- ============================================================
CREATE ICEBERG TABLE te_empty (id bigint, val text);

-- Stats on empty table
SELECT
    result::json->'fragments'->>'total-records' AS total_records
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'te_empty',
        'te_catalog_server', 'te_catalog',
        'te_volume_server', 'te_volume', '') AS result
) t;

-- ANALYZE on empty table
ANALYZE te_empty;

-- VACUUM on empty table
VACUUM te_empty;

-- Select from empty table
SELECT COUNT(*) FROM te_empty;
SELECT * FROM te_empty;

-- Delete from empty table
DELETE FROM te_empty;

-- Update empty table
UPDATE te_empty SET val = 'x';

DROP TABLE te_empty;

-- ============================================================
-- Test 7: Table with only NULLs
-- ============================================================
CREATE ICEBERG TABLE te_nulls (id bigint, val text, num decimal(10,2));

INSERT INTO te_nulls VALUES (NULL, NULL, NULL);
INSERT INTO te_nulls VALUES (NULL, NULL, NULL);
INSERT INTO te_nulls VALUES (NULL, NULL, NULL);

SELECT COUNT(*) FROM te_nulls;
SELECT COUNT(id), COUNT(val), COUNT(num) FROM te_nulls;

DROP TABLE te_nulls;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME te_volume;
DROP USER MAPPING FOR current_user SERVER te_volume_server;
DROP SERVER te_volume_server;
DROP CATALOG te_catalog;
DROP USER MAPPING FOR current_user SERVER te_catalog_server;
DROP SERVER te_catalog_server;
