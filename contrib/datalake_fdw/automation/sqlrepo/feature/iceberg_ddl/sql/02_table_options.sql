-- 02_table_options.sql
-- Test Iceberg CREATE TABLE with various OPTIONS

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg Table Options');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER do_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER do_catalog_server;
CREATE FOREIGN CATALOG do_catalog SERVER do_catalog_server;
SET iceberg_default_catalog = 'do_catalog';

CREATE SERVER do_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER do_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME do_volume SERVER do_volume_server OPTIONS(base_path '/do_volume/');
SET iceberg_default_volume = 'do_volume';

-- ============================================================
-- Test 1: namespace and table_name options
-- ============================================================
SELECT test_log('Test 1: OPTIONS namespace and table_name');

CREATE ICEBERG TABLE do_named (id bigint, val text)
OPTIONS (namespace 'test_ddl_ns', table_name 'ext_name');
INSERT INTO do_named VALUES (1, 'named_test');
SELECT * FROM do_named;
DROP TABLE do_named;

-- ============================================================
-- Test 2: base_location option
-- ============================================================
SELECT test_log('Test 2: OPTIONS base_location');

CREATE ICEBERG TABLE do_located (id bigint, val int)
OPTIONS (base_location '/custom/loc/');
INSERT INTO do_located VALUES (1, 100);
SELECT * FROM do_located;
DROP TABLE do_located;

-- ============================================================
-- Test 3: CREATE ICEBERG TABLE IF NOT EXISTS
-- ============================================================
SELECT test_log('Test 3: CREATE TABLE IF NOT EXISTS');

CREATE ICEBERG TABLE do_ifne (id bigint, name text);
INSERT INTO do_ifne VALUES (1, 'original');

-- Try to create again with different schema -- should keep original
CREATE ICEBERG TABLE IF NOT EXISTS do_ifne (id bigint, name text, extra int);
SELECT * FROM do_ifne ORDER BY id;
DROP TABLE do_ifne;

-- ============================================================
-- Test 4: DROP TABLE IF EXISTS on non-existent table
-- ============================================================
SELECT test_log('Test 4: DROP TABLE IF EXISTS on non-existent');

DROP TABLE IF EXISTS do_never_created;

-- ============================================================
-- Test 5: Drop and immediately recreate
-- ============================================================
SELECT test_log('Test 5: Drop and immediately recreate');

CREATE ICEBERG TABLE do_quick (id bigint, val int);
INSERT INTO do_quick VALUES (1, 10);
SELECT COUNT(*) AS before_drop FROM do_quick;
DROP TABLE do_quick;

CREATE ICEBERG TABLE do_quick (id bigint, val int);
INSERT INTO do_quick VALUES (2, 20), (3, 30);
SELECT * FROM do_quick ORDER BY id;
DROP TABLE do_quick;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME do_volume;
DROP USER MAPPING FOR current_user SERVER do_volume_server;
DROP SERVER do_volume_server;
DROP CATALOG do_catalog;
DROP USER MAPPING FOR current_user SERVER do_catalog_server;
DROP SERVER do_catalog_server;
