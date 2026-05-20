-- Error Handling / Negative Tests Smoke
-- Purpose: Verify datalake_fdw produces clear error messages for invalid operations
-- Tests: bad paths, format mismatch, schema mismatch, invalid options, permission errors

-- Clean up previous run leftovers
DROP SERVER IF EXISTS s3_neg_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup (inline to avoid \i path issues with pg_regress)
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: S3 server for negative tests
-- ============================================================
CREATE SERVER s3_neg_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER s3_neg_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Non-existent file path
-- Should return empty result or clear error, not crash
-- ============================================================
DROP FOREIGN TABLE IF EXISTS neg_nonexistent;
CREATE FOREIGN TABLE neg_nonexistent (id int, name text)
SERVER s3_neg_server
OPTIONS (filePath '/warehouse/this/path/does/not/exist/', format 'orc');

DO $$
BEGIN
    PERFORM COUNT(*) FROM neg_nonexistent;
    RAISE NOTICE 'Non-existent path returned rows (unexpected or empty)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on non-existent path: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_nonexistent;

-- ============================================================
-- Test 2: Format mismatch - reading ORC data as Parquet
-- Should produce a clear error message
-- ============================================================
DROP FOREIGN TABLE IF EXISTS neg_format_mismatch;
CREATE FOREIGN TABLE neg_format_mismatch (id int, name text)
SERVER s3_neg_server
OPTIONS (filePath '/warehouse/test-data/orc/basic/', format 'parquet');

-- This should fail or return incorrect data - wrapped in DO block to catch error
DO $$
BEGIN
    PERFORM COUNT(*) FROM neg_format_mismatch;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on format mismatch: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_format_mismatch;

-- ============================================================
-- Test 3: Schema mismatch - more columns than data
-- ============================================================
DROP FOREIGN TABLE IF EXISTS neg_schema_extra_cols;
CREATE FOREIGN TABLE neg_schema_extra_cols (
    id int,
    name text,
    extra_col1 int,
    extra_col2 text,
    extra_col3 float
)
SERVER s3_neg_server
OPTIONS (filePath '/warehouse/test-data/orc/basic/', format 'orc');

-- Query with extra columns - should handle gracefully (NULLs or error)
DO $$
BEGIN
    PERFORM COUNT(*) FROM neg_schema_extra_cols;
    RAISE NOTICE 'Extra columns query succeeded (may return NULLs for missing cols)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on extra columns: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_schema_extra_cols;

-- ============================================================
-- Test 4: Invalid server options
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE SERVER neg_bad_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (host ''nonexistent-host-12345'', protocol ''s3'', isvirtual ''false'', ishttps ''false'')';
    EXECUTE 'CREATE USER MAPPING FOR gpadmin
        SERVER neg_bad_server
        OPTIONS (user ''gpadmin'', accesskey ''bad'', secretkey ''bad'')';
    EXECUTE 'CREATE FOREIGN TABLE neg_bad_conn (id int)
        SERVER neg_bad_server
        OPTIONS (filePath ''/warehouse/test/'', format ''orc'')';

    PERFORM COUNT(*) FROM neg_bad_conn;
    RAISE NOTICE 'Unexpected success with bad server';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on bad server connection: %', SQLERRM;
END;
$$;

-- Cleanup bad server if it was created
DROP FOREIGN TABLE IF EXISTS neg_bad_conn;
DROP USER MAPPING IF EXISTS FOR gpadmin SERVER neg_bad_server;
DROP SERVER IF EXISTS neg_bad_server;

-- ============================================================
-- Test 5: Invalid format name
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE neg_bad_format (id int)
        SERVER s3_neg_server
        OPTIONS (filePath ''/warehouse/test/'', format ''invalid_format'')';
    RAISE NOTICE 'Unexpected success with invalid format';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on invalid format: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_bad_format;

-- ============================================================
-- Test 6: Invalid compression option
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE neg_bad_compress (id int)
        SERVER s3_neg_server
        OPTIONS (filePath ''/warehouse/test/'', format ''parquet'', compression ''invalid_codec'')';
    RAISE NOTICE 'Unexpected success with invalid compression';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on invalid compression: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_bad_compress;

-- ============================================================
-- Test 7: Empty table scan (no files, zero rows)
-- Should return 0 rows cleanly, not error
-- ============================================================
DROP FOREIGN TABLE IF EXISTS neg_empty_scan;
CREATE FOREIGN TABLE neg_empty_scan (id int)
SERVER s3_neg_server
OPTIONS (filePath '/warehouse/test-data/empty/', format 'orc');

DO $$
DECLARE
    cnt int;
BEGIN
    SELECT COUNT(*) INTO cnt FROM neg_empty_scan;
    RAISE NOTICE 'Empty scan returned % rows', cnt;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on empty scan: %', SQLERRM;
END;
$$;

DROP FOREIGN TABLE IF EXISTS neg_empty_scan;

-- ============================================================
-- Test 8: Iceberg - DROP non-existent table
-- ============================================================
CREATE SERVER neg_iceberg_catalog
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER neg_iceberg_catalog;
CREATE FOREIGN CATALOG neg_catalog SERVER neg_iceberg_catalog;
SET iceberg_default_catalog = 'neg_catalog';

CREATE SERVER neg_iceberg_volume
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER neg_iceberg_volume
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME neg_volume SERVER neg_iceberg_volume OPTIONS(base_path '/neg_volume/');
SET iceberg_default_volume = 'neg_volume';

-- DROP IF EXISTS should not error
DROP TABLE IF EXISTS this_table_does_not_exist;

-- ============================================================
-- Test 9: INSERT with type mismatch
-- ============================================================
CREATE ICEBERG TABLE neg_type_mismatch (
    id bigint,
    val int);

-- This should fail: text into int column
DO $$
BEGIN
    EXECUTE 'INSERT INTO neg_type_mismatch VALUES (1, ''not_a_number'')';
    RAISE NOTICE 'Unexpected success with type mismatch';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on type mismatch: %', SQLERRM;
END;
$$;

-- Valid insert should work after the error
INSERT INTO neg_type_mismatch VALUES (1, 42);
SELECT * FROM neg_type_mismatch ORDER BY id;

DROP TABLE IF EXISTS neg_type_mismatch;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME IF EXISTS neg_volume;
DROP USER MAPPING IF EXISTS FOR current_user SERVER neg_iceberg_volume;
DROP SERVER IF EXISTS neg_iceberg_volume;
DROP CATALOG IF EXISTS neg_catalog;
DROP USER MAPPING IF EXISTS FOR current_user SERVER neg_iceberg_catalog;
DROP SERVER IF EXISTS neg_iceberg_catalog;

DROP USER MAPPING IF EXISTS FOR gpadmin SERVER s3_neg_server;
DROP SERVER IF EXISTS s3_neg_server;
