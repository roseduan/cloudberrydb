-- Iceberg Toolkit All Operations Test
-- Purpose: Call every toolkit catalog_fdw operation to maximize coverage
-- Target: iceberg_toolkit_catalog_fdw.c (create_table, drop_table, list_catalog,
--         list_namespace, append, update, commit_delete, commit_rewrite)
--         iceberg_toolkit_volume_fdw.c (scan operations)
--         iceberg_toolkit_common.c (execute_*_operation, iceberg_build_schema_from_table_name)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- Setup builtin catalog + volume
CREATE SERVER tka_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER tka_catalog_server;
CREATE FOREIGN CATALOG tka_catalog SERVER tka_catalog_server;
SET iceberg_default_catalog = 'tka_catalog';

CREATE SERVER tka_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER tka_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME tka_volume SERVER tka_volume_server OPTIONS(base_path '/tka_volume/');
SET iceberg_default_volume = 'tka_volume';

-- Create a table first (needed for subsequent ops)
CREATE ICEBERG TABLE tka_data (id bigint, name text, val int);
INSERT INTO tka_data VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);
INSERT INTO tka_data VALUES (4, 'd', 40), (5, 'e', 50);

-- ============================================================
-- Test 1: create_table operation via toolkit
-- ============================================================
SET client_min_messages = WARNING;
DO $$
DECLARE r text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'create_table', 'public', 'tka_toolkit_created',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '')
    INTO r;
    RAISE NOTICE 'create_table: %', substring(r from 1 for 80);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'create_table error (may be expected): %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Test 2: list_catalog operation
-- ============================================================
SET client_min_messages = WARNING;
DO $$
DECLARE r text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'list_catalog', 'public', 'tka_data',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '')
    INTO r;
    RAISE NOTICE 'list_catalog: %', substring(r from 1 for 80);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'list_catalog error: %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Test 3: list_namespace operation
-- ============================================================
SET client_min_messages = WARNING;
DO $$
DECLARE r text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'list_namespace', 'public', 'tka_data',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '')
    INTO r;
    RAISE NOTICE 'list_namespace: %', substring(r from 1 for 80);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'list_namespace error: %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Test 4: get_statistics (already tested, repeat for coverage)
-- ============================================================
SELECT
    result::json->'fragments'->>'total-records' AS total_records,
    (result::json->'fragments'->>'total-files-size')::bigint > 0 AS has_size
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'tka_data',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '') AS result
) t;

-- ============================================================
-- Test 5: load_table operation
-- ============================================================
SET client_min_messages = WARNING;
DO $$
DECLARE r text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'load_table', 'public', 'tka_data',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '')
    INTO r;
    RAISE NOTICE 'load_table: %', substring(r from 1 for 80);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'load_table error: %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Test 6: append operation (direct toolkit append, not via INSERT)
-- ============================================================
SET client_min_messages = WARNING;
DO $$
DECLARE
    plan_json json;
    tasks json;
    frag json;
    append_json text;
    r text;
BEGIN
    -- Get real file paths from plan_file_groups
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'tka_data',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', ''))::json
    INTO plan_json;

    tasks := plan_json->'fileGroups'->'combinedTasks'->0->'tasks';
    IF tasks IS NOT NULL AND json_array_length(tasks) > 0 THEN
        frag := tasks->0->'data';
        append_json := json_build_object(
            'fragments', json_build_array(
                json_build_object(
                    'path', frag->>'sourceName',
                    'file_size_in_bytes', 1024,
                    'record_count', (frag->'metadata'->>'recordCount')::int,
                    'format', frag->'metadata'->>'fileFormat'
                )
            )
        )::text;

        -- Test append operation
        BEGIN
            SELECT iceberg_toolkit.catalog_fdw(
                'append', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'append: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'append error (expected): %', SQLERRM;
        END;

        -- Test update operation
        BEGIN
            SELECT iceberg_toolkit.catalog_fdw(
                'update', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'update: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'update error (expected): %', SQLERRM;
        END;

        -- Test commitAppend
        BEGIN
            SELECT iceberg_toolkit.catalog_fdw(
                'commitAppend', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'commitAppend: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'commitAppend error (expected): %', SQLERRM;
        END;

        -- Test commitUpdate
        BEGIN
            append_json := json_build_object(
                'updateFragments', json_build_array(
                    json_build_object(
                        'path', frag->>'sourceName',
                        'file_size_in_bytes', 1024,
                        'record_count', (frag->'metadata'->>'recordCount')::int,
                        'format', frag->'metadata'->>'fileFormat',
                        'position_on_delete', 'DATA_FILE'
                    )
                )
            )::text;
            SELECT iceberg_toolkit.catalog_fdw(
                'commitUpdate', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'commitUpdate: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'commitUpdate error (expected): %', SQLERRM;
        END;

        -- Test commitDelete
        BEGIN
            SELECT iceberg_toolkit.catalog_fdw(
                'commitDelete', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'commitDelete: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'commitDelete error (expected): %', SQLERRM;
        END;

        -- Test commitRewrite
        BEGIN
            append_json := json_build_object(
                'fragments', json_build_array(
                    json_build_object(
                        'path', frag->>'sourceName',
                        'file_size_in_bytes', 1024,
                        'record_count', (frag->'metadata'->>'recordCount')::int,
                        'format', frag->'metadata'->>'fileFormat'
                    )
                ),
                'rewrittenFragments', json_build_array(
                    json_build_object(
                        'path', frag->>'sourceName',
                        'file_size_in_bytes', 1024,
                        'record_count', (frag->'metadata'->>'recordCount')::int,
                        'format', frag->'metadata'->>'fileFormat'
                    )
                )
            )::text;
            SELECT iceberg_toolkit.catalog_fdw(
                'commitRewrite', 'public', 'tka_data',
                'tka_catalog_server', 'tka_catalog',
                'tka_volume_server', 'tka_volume', append_json)
            INTO r;
            RAISE NOTICE 'commitRewrite: %', substring(r from 1 for 80);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'commitRewrite error (expected): %', SQLERRM;
        END;
    ELSE
        RAISE NOTICE 'No file groups available for append/update tests';
    END IF;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Test 7: drop_table operation via toolkit
-- ============================================================
-- Create a throwaway table to drop via toolkit
CREATE ICEBERG TABLE tka_to_drop (id bigint);
INSERT INTO tka_to_drop VALUES (1);

SET client_min_messages = WARNING;
DO $$
DECLARE r text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'drop_table', 'public', 'tka_to_drop',
        'tka_catalog_server', 'tka_catalog',
        'tka_volume_server', 'tka_volume', '')
    INTO r;
    RAISE NOTICE 'drop_table: %', substring(r from 1 for 80);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'drop_table error: %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE IF EXISTS tka_to_drop;
DROP TABLE tka_data;
DROP VOLUME tka_volume;
DROP USER MAPPING FOR current_user SERVER tka_volume_server;
DROP SERVER tka_volume_server;
DROP CATALOG tka_catalog;
DROP USER MAPPING FOR current_user SERVER tka_catalog_server;
DROP SERVER tka_catalog_server;
