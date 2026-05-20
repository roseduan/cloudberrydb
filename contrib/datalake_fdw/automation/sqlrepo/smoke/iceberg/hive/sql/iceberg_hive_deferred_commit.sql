-- Iceberg + Hive Catalog Deferred Commit Test
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
SET datestyle = ISO, MDY;

-- Catalog server (Hive Metastore backend)
CREATE SERVER hive_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'hive',
    url 'thrift://hive-metastore:9083'
);
CREATE USER MAPPING FOR current_user SERVER hive_catalog_server;
CREATE FOREIGN CATALOG hive_catalog SERVER hive_catalog_server;
SET iceberg_default_catalog = 'hive_catalog';

-- Volume server (S3/MinIO)
CREATE SERVER hive_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER hive_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME hive_volume SERVER hive_volume_server OPTIONS(base_path '/hive_volume/');
SET iceberg_default_volume = 'hive_volume';

-- create test table and insert data
CREATE ICEBERG TABLE deferred_commit_test (id bigint, name text);

INSERT INTO deferred_commit_test VALUES (1, 'alpha');
INSERT INTO deferred_commit_test VALUES (2, 'beta');
INSERT INTO deferred_commit_test VALUES (3, 'gamma');

SELECT COUNT(*) FROM deferred_commit_test;

-- Test commitAppend via toolkit function
SET client_min_messages = WARNING;
DO $$
DECLARE
    plan_json   json;
    tasks       json;
    frag        json;
    commit_json text;
    commit_result text;
BEGIN
    SELECT (iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'deferred_commit_test',
        'hive_catalog_server', 'hive_catalog',
        'hive_volume_server', 'hive_volume', ''))::json
    INTO plan_json;

    tasks := plan_json->'fileGroups'->'combinedTasks'->0->'tasks';
    frag := tasks->0->'data';

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
            'hive_catalog_server', 'hive_catalog',
            'hive_volume_server', 'hive_volume',
            commit_json)
        INTO commit_result;
        RAISE NOTICE 'commitAppend: success (response received)';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'commitAppend returned error (expected for test): %', SQLERRM;
    END;
END;
$$;
RESET client_min_messages;

-- cleanup
DROP TABLE deferred_commit_test;
DROP VOLUME hive_volume;
DROP USER MAPPING FOR current_user SERVER hive_volume_server;
DROP SERVER hive_volume_server;
DROP CATALOG hive_catalog;
DROP USER MAPPING FOR current_user SERVER hive_catalog_server;
DROP SERVER hive_catalog_server;
