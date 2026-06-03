-- 01_fdw_errors.sql
-- Test FDW error paths and negative scenarios

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Error Paths');

-- ============================================================
-- Setup: FDW server
-- ============================================================
DROP SERVER IF EXISTS ne_server CASCADE;
CREATE SERVER ne_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER ne_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Write valid ORC data for format mismatch test
CREATE FOREIGN TABLE ne_orc_w (id int, name text)
SERVER ne_server
OPTIONS (filePath '/warehouse/fdw-test/feature/negative/orc_data/', format 'orc');
INSERT INTO ne_orc_w VALUES (1, 'orc_test');
DROP FOREIGN TABLE ne_orc_w;

-- ============================================================
-- Test 1: Non-existent file path
-- ============================================================
SELECT test_log('Test 1: Non-existent file path');

DO $$
BEGIN
    CREATE FOREIGN TABLE ne_nofile (id int, name text)
    SERVER ne_server
    OPTIONS (filePath '/warehouse/fdw-test/feature/negative/does_not_exist/', format 'parquet');
    PERFORM COUNT(*) FROM ne_nofile;
    DROP FOREIGN TABLE IF EXISTS ne_nofile;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_nofile;
END $$;

-- ============================================================
-- Test 2: Format mismatch (ORC file read as Parquet)
-- ============================================================
SELECT test_log('Test 2: Format mismatch (ORC as Parquet)');

DO $$
BEGIN
    CREATE FOREIGN TABLE ne_mismatch (id int, name text)
    SERVER ne_server
    OPTIONS (filePath '/warehouse/fdw-test/feature/negative/orc_data/', format 'parquet');
    PERFORM COUNT(*) FROM ne_mismatch;
    DROP FOREIGN TABLE IF EXISTS ne_mismatch;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_mismatch;
END $$;

-- ============================================================
-- Test 3: Schema mismatch (extra columns)
-- ============================================================
SELECT test_log('Test 3: Schema mismatch (extra columns)');

DO $$
BEGIN
    CREATE FOREIGN TABLE ne_schema (id int, name text, extra_col int, another_col text)
    SERVER ne_server
    OPTIONS (filePath '/warehouse/fdw-test/feature/negative/orc_data/', format 'orc');
    PERFORM COUNT(*) FROM ne_schema;
    DROP FOREIGN TABLE IF EXISTS ne_schema;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_schema;
END $$;

-- ============================================================
-- Test 4: Invalid server options (bad host)
-- ============================================================
SELECT test_log('Test 4: Invalid server options');

DO $$
BEGIN
    DROP SERVER IF EXISTS ne_bad_server CASCADE;
    CREATE SERVER ne_bad_server FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'invalid_host:9999', protocol 's3', isvirtual 'false', ishttps 'false');
    CREATE USER MAPPING FOR gpadmin SERVER ne_bad_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');
    CREATE FOREIGN TABLE ne_badhost (id int)
    SERVER ne_bad_server
    OPTIONS (filePath '/warehouse/test/', format 'parquet');
    PERFORM COUNT(*) FROM ne_badhost;
    DROP FOREIGN TABLE IF EXISTS ne_badhost;
    DROP USER MAPPING IF EXISTS FOR gpadmin SERVER ne_bad_server;
    DROP SERVER IF EXISTS ne_bad_server;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_badhost;
    DROP USER MAPPING IF EXISTS FOR gpadmin SERVER ne_bad_server;
    DROP SERVER IF EXISTS ne_bad_server;
END $$;

-- ============================================================
-- Test 5: Invalid format name
-- ============================================================
SELECT test_log('Test 5: Invalid format name');

DO $$
BEGIN
    CREATE FOREIGN TABLE ne_badfmt (id int, name text)
    SERVER ne_server
    OPTIONS (filePath '/warehouse/fdw-test/feature/negative/orc_data/', format 'invalid_format');
    PERFORM COUNT(*) FROM ne_badfmt;
    DROP FOREIGN TABLE IF EXISTS ne_badfmt;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_badfmt;
END $$;

-- ============================================================
-- Test 6: Empty directory scan
-- ============================================================
SELECT test_log('Test 6: Empty directory scan');

DO $$
BEGIN
    CREATE FOREIGN TABLE ne_empty_dir (id int, name text)
    SERVER ne_server
    OPTIONS (filePath '/warehouse/fdw-test/feature/negative/empty_dir/', format 'parquet');
    PERFORM COUNT(*) FROM ne_empty_dir;
    DROP FOREIGN TABLE IF EXISTS ne_empty_dir;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
    DROP FOREIGN TABLE IF EXISTS ne_empty_dir;
END $$;

-- ============================================================
-- Cleanup
-- ============================================================
DROP USER MAPPING FOR gpadmin SERVER ne_server;
DROP SERVER ne_server;
