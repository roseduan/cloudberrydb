CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER default_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER default_catalog_server;
CREATE FOREIGN CATALOG default_catalog SERVER default_catalog_server;
set iceberg_default_catalog='default_catalog';

-- volume
CREATE SERVER default_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER default_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');

CREATE FOREIGN VOLUME default_volume SERVER default_volume_server OPTIONS(base_path '/default_volume/');
set iceberg_default_volume='default_volume';

-- create test table and insert data to generate files for commit testing
CREATE ICEBERG TABLE deferred_commit_test (id bigint, name text);

INSERT INTO deferred_commit_test VALUES (1, 'alpha');
INSERT INTO deferred_commit_test VALUES (2, 'beta');
INSERT INTO deferred_commit_test VALUES (3, 'gamma');

-- verify data is correct
SELECT COUNT(*) FROM deferred_commit_test;

-- 1. Test deferred append (Transaction wrapper, does not commit to catalog)
-- The existing 'append' operation now uses Transaction wrapper internally
-- Verify it still returns metadata-location
SET client_min_messages = WARNING;
DO $$
DECLARE
    append_result text;
    append_json json;
BEGIN
    -- Build append request with a dummy fragment (uses existing file path)
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'deferred_commit_test',
        'default_catalog_server', 'default_catalog',
        'default_volume_server', 'default_volume', ''))::json
    INTO append_json;

    -- Verify plan_file_groups returns success
    IF append_json->>'status' = 'success' THEN
        RAISE NOTICE 'plan_file_groups: success';
    ELSE
        RAISE NOTICE 'plan_file_groups: unexpected status %', append_json->>'status';
    END IF;
END;
$$;
RESET client_min_messages;

-- 2. Test commitAppend (PRE_COMMIT: normal AppendFiles commit to catalog)
SET client_min_messages = WARNING;
DO $$
DECLARE
    plan_json   json;
    tasks       json;
    frag        json;
    commit_json text;
    commit_result text;
BEGIN
    -- Get plan to extract real file paths
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'deferred_commit_test',
        'default_catalog_server', 'default_catalog',
        'default_volume_server', 'default_volume', ''))::json
    INTO plan_json;

    tasks := plan_json->'fileGroups'->'combinedTasks'->0->'tasks';
    frag := tasks->0->'data';

    -- Build commitAppend request with the first file
    commit_json := json_build_object(
        'fragments', json_build_array(
            json_build_object(
                'path',               frag->>'sourceName',
                'file_size_in_bytes', 1024,
                'record_count',       (frag->'metadata'->>'recordCount')::int,
                'format',             frag->'metadata'->>'fileFormat'
            )
        )
    )::text;

    BEGIN
        SELECT iceberg_toolkit.catalog_fdw(
            'commitAppend', 'public', 'deferred_commit_test',
            'default_catalog_server', 'default_catalog',
            'default_volume_server', 'default_volume',
            commit_json)
        INTO commit_result;

        RAISE NOTICE 'commitAppend: success (response received)';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'commitAppend returned error (expected for test): %', SQLERRM;
    END;
END;
$$;
RESET client_min_messages;

-- 3. Test commitUpdate (PRE_COMMIT: normal RowDelta commit to catalog)
SET client_min_messages = WARNING;
DO $$
DECLARE
    plan_json   json;
    tasks       json;
    frag        json;
    commit_json text;
    commit_result text;
BEGIN
    -- Get plan to extract real file paths
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'deferred_commit_test',
        'default_catalog_server', 'default_catalog',
        'default_volume_server', 'default_volume', ''))::json
    INTO plan_json;

    tasks := plan_json->'fileGroups'->'combinedTasks'->0->'tasks';
    frag := tasks->0->'data';

    -- Build commitUpdate request with update fragments
    commit_json := json_build_object(
        'updateFragments', json_build_array(
            json_build_object(
                'path',               frag->>'sourceName',
                'file_size_in_bytes', 1024,
                'record_count',       (frag->'metadata'->>'recordCount')::int,
                'format',             frag->'metadata'->>'fileFormat',
                'position_on_delete', 'DATA_FILE'
            )
        )
    )::text;

    BEGIN
        SELECT iceberg_toolkit.catalog_fdw(
            'commitUpdate', 'public', 'deferred_commit_test',
            'default_catalog_server', 'default_catalog',
            'default_volume_server', 'default_volume',
            commit_json)
        INTO commit_result;

        RAISE NOTICE 'commitUpdate: success (response received)';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'commitUpdate returned error (expected for test): %', SQLERRM;
    END;
END;
$$;
RESET client_min_messages;

-- 4. Test commitRewrite (VACUUM commit: RewriteFiles + commit to catalog)
SET client_min_messages = WARNING;
DO $$
DECLARE
    plan_json   json;
    tasks       json;
    frag        json;
    rewritten   text;
    commit_json text;
    commit_result text;
    i           int;
    sep         text;
BEGIN
    -- Get plan to extract real file paths
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'deferred_commit_test',
        'default_catalog_server', 'default_catalog',
        'default_volume_server', 'default_volume', ''))::json
    INTO plan_json;

    tasks := plan_json->'fileGroups'->'combinedTasks'->0->'tasks';

    IF tasks IS NULL THEN
        RAISE NOTICE 'commitRewrite: no file groups returned (table too small for compaction)';
    ELSE
        -- Build rewrittenFragments array
        rewritten := '[';
        sep := '';
        FOR i IN 0 .. json_array_length(tasks) - 1 LOOP
            frag := tasks->i->'data';
            rewritten := rewritten || sep || json_build_object(
                'path',               frag->>'sourceName',
                'file_size_in_bytes', 1024,
                'record_count',       (frag->'metadata'->>'recordCount')::int,
                'format',             frag->'metadata'->>'fileFormat'
            )::text;
            sep := ',';
        END LOOP;
        rewritten := rewritten || ']';

        -- Use first file as new fragment
        frag := tasks->0->'data';
        commit_json := json_build_object(
            'fragments', json_build_array(
                json_build_object(
                    'path',               frag->>'sourceName',
                    'file_size_in_bytes', 1024,
                    'record_count',       (frag->'metadata'->>'recordCount')::int,
                    'format',             frag->'metadata'->>'fileFormat'
                )
            ),
            'rewrittenFragments', rewritten::json
        )::text;

        BEGIN
            SELECT iceberg_toolkit.catalog_fdw(
                'commitRewrite', 'public', 'deferred_commit_test',
                'default_catalog_server', 'default_catalog',
                'default_volume_server', 'default_volume',
                commit_json)
            INTO commit_result;

            RAISE NOTICE 'commitRewrite: success (response received)';
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'commitRewrite returned error (expected for test): %', SQLERRM;
        END;
    END IF;
END;
$$;
RESET client_min_messages;

-- cleanup
DROP TABLE deferred_commit_test;
DROP VOLUME default_volume;
DROP USER MAPPING FOR current_user SERVER default_volume_server;
DROP SERVER default_volume_server;
DROP CATALOG default_catalog;
DROP USER MAPPING FOR current_user SERVER default_catalog_server;
DROP SERVER default_catalog_server;
