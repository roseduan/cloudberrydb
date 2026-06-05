-- 02_iceberg_errors.sql
-- Test Iceberg error paths and negative scenarios

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg Error Paths');

-- ============================================================
-- Setup: Iceberg catalog and volume
-- ============================================================
CREATE SERVER ne_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ne_catalog_server;
CREATE FOREIGN CATALOG ne_catalog SERVER ne_catalog_server;
SET iceberg_default_catalog = 'ne_catalog';

CREATE SERVER ne_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ne_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ne_volume SERVER ne_volume_server OPTIONS(base_path '/ne_volume/');
SET iceberg_default_volume = 'ne_volume';

-- ============================================================
-- Test 1: DROP TABLE IF EXISTS on non-existent (no error)
-- ============================================================
SELECT test_log('Test 1: DROP TABLE IF EXISTS non-existent');

DROP TABLE IF EXISTS ne_nonexistent_xyz;

-- ============================================================
-- Test 2: INSERT type mismatch (text into int)
-- ============================================================
SELECT test_log('Test 2: INSERT type mismatch');

CREATE ICEBERG TABLE ne_types (id bigint, val int);

DO $$
BEGIN
    INSERT INTO ne_types VALUES ('not_a_number', 'also_not');
EXCEPTION WHEN OTHERS THEN
    -- Truncate to primary message; dlagent stack traces contain
    -- sun.reflect.GeneratedMethodAccessor<N> numbers that differ per JVM run.
    RAISE NOTICE 'Expected error: %', split_part(SQLERRM, E'\n', 1);
END $$;

-- ============================================================
-- Test 3: Valid INSERT after error (recovery)
-- ============================================================
SELECT test_log('Test 3: Valid INSERT after error');

INSERT INTO ne_types VALUES (1, 100);
SELECT * FROM ne_types ORDER BY id;
DROP TABLE ne_types;

-- ============================================================
-- Test 4: Toolkit operation on non-existent table
-- ============================================================
SELECT test_log('Test 4: Toolkit on non-existent table');

DO $$
DECLARE
    result text;
BEGIN
    SELECT iceberg_toolkit.catalog_fdw(
        'load_table', 'public', 'ne_table_does_not_exist',
        'ne_catalog_server', 'ne_catalog',
        'ne_volume_server', 'ne_volume', '')
    INTO result;
    RAISE NOTICE 'Unexpected success';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', split_part(SQLERRM, E'\n', 1);
END $$;

-- ============================================================
-- Test 5: Invalid volume options
-- ============================================================
SELECT test_log('Test 5: Invalid volume options');

-- Bad endpoint: use a resolvable host with a closed port (connection refused
-- fails fast on every arch).  An unresolvable hostname stalled ~40min on the
-- aarch64 runner's DNS until the fdw-side curl gave up with CURL 28
-- (pipeline #139686), while x86 failed fast with an agent HTTP 500 -- the
-- expected NOTICE text then depends on who times out first.  The transport
-- details are normalized out of the NOTICE for the same reason.
DO $$
BEGIN
    DROP SERVER IF EXISTS ne_bad_vol_server CASCADE;
    CREATE SERVER ne_bad_vol_server FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:1', region 'us-east-1',
             bucket_name 'nonexistent_bucket', path_style_access 'true');
    CREATE USER MAPPING FOR current_user SERVER ne_bad_vol_server
    OPTIONS (access_key_id 'bad_key', secret_access_key 'bad_secret');
    CREATE FOREIGN VOLUME ne_bad_volume SERVER ne_bad_vol_server OPTIONS(base_path '/bad_path/');
    SET iceberg_default_volume = 'ne_bad_volume';

    CREATE ICEBERG TABLE ne_bad_vol_table (id bigint);
    INSERT INTO ne_bad_vol_table VALUES (1);
    DROP TABLE IF EXISTS ne_bad_vol_table;
    DROP VOLUME IF EXISTS ne_bad_volume;
    DROP USER MAPPING IF EXISTS FOR current_user SERVER ne_bad_vol_server;
    DROP SERVER IF EXISTS ne_bad_vol_server;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %',
        regexp_replace(split_part(SQLERRM, E'\n', 1),
                       'HTTP Status: .*$', 'HTTP error from datalake agent');
    DROP TABLE IF EXISTS ne_bad_vol_table;
    DROP VOLUME IF EXISTS ne_bad_volume;
    DROP USER MAPPING IF EXISTS FOR current_user SERVER ne_bad_vol_server;
    DROP SERVER IF EXISTS ne_bad_vol_server;
END $$;

-- Restore default volume for cleanup
SET iceberg_default_volume = 'ne_volume';

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME ne_volume;
DROP USER MAPPING FOR current_user SERVER ne_volume_server;
DROP SERVER ne_volume_server;
DROP CATALOG ne_catalog;
DROP USER MAPPING FOR current_user SERVER ne_catalog_server;
DROP SERVER ne_catalog_server;
