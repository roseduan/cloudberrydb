-- 01_catalog_lifecycle.sql
-- Test Iceberg catalog creation, deletion, and recreation

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg Catalog Lifecycle');

-- ============================================================
-- Test 1: Basic catalog + volume creation, verify usable
-- ============================================================
SELECT test_log('Test 1: Basic catalog and volume creation');

CREATE SERVER cl_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cl_catalog_server;
CREATE FOREIGN CATALOG cl_catalog SERVER cl_catalog_server;
SET iceberg_default_catalog = 'cl_catalog';

CREATE SERVER cl_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER cl_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cl_volume SERVER cl_volume_server OPTIONS(base_path '/cl_volume/');
SET iceberg_default_volume = 'cl_volume';

-- Verify catalog is usable: create table, insert, select
CREATE ICEBERG TABLE cl_verify (id bigint, name text);
INSERT INTO cl_verify VALUES (1, 'catalog_test');
SELECT * FROM cl_verify ORDER BY id;
DROP TABLE cl_verify;

-- ============================================================
-- Test 2: Drop catalog and recreate with same name
-- ============================================================
SELECT test_log('Test 2: Drop and recreate catalog');

DROP VOLUME cl_volume;
DROP USER MAPPING FOR current_user SERVER cl_volume_server;
DROP SERVER cl_volume_server;
DROP CATALOG cl_catalog;
DROP USER MAPPING FOR current_user SERVER cl_catalog_server;
DROP SERVER cl_catalog_server;

-- Recreate with same names
CREATE SERVER cl_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cl_catalog_server;
CREATE FOREIGN CATALOG cl_catalog SERVER cl_catalog_server;
SET iceberg_default_catalog = 'cl_catalog';

CREATE SERVER cl_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER cl_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cl_volume SERVER cl_volume_server OPTIONS(base_path '/cl_volume_2/');
SET iceberg_default_volume = 'cl_volume';

CREATE ICEBERG TABLE cl_recreated (id bigint, val int);
INSERT INTO cl_recreated VALUES (1, 100), (2, 200);
SELECT COUNT(*) AS row_count FROM cl_recreated;
DROP TABLE cl_recreated;

-- ============================================================
-- Test 3: DROP TABLE IF EXISTS on non-existent table
-- ============================================================
SELECT test_log('Test 3: DROP TABLE IF EXISTS on non-existent table');
DROP TABLE IF EXISTS cl_nonexistent_table_xyz;

-- ============================================================
-- Test 4: Multiple tables in same catalog
-- ============================================================
SELECT test_log('Test 4: Multiple tables in same catalog');

CREATE ICEBERG TABLE cl_table_a (id bigint, name text);
CREATE ICEBERG TABLE cl_table_b (id bigint, val int);
CREATE ICEBERG TABLE cl_table_c (id bigint);

INSERT INTO cl_table_a VALUES (1, 'a'), (2, 'b');
INSERT INTO cl_table_b VALUES (1, 10), (2, 20);
INSERT INTO cl_table_c VALUES (1), (2), (3);

SELECT COUNT(*) AS table_a_count FROM cl_table_a;
SELECT COUNT(*) AS table_b_count FROM cl_table_b;
SELECT COUNT(*) AS table_c_count FROM cl_table_c;

DROP TABLE cl_table_a;
DROP TABLE cl_table_b;
DROP TABLE cl_table_c;

-- ============================================================
-- Test 5: Table with namespace option
-- ============================================================
SELECT test_log('Test 5: Table with namespace option');

CREATE ICEBERG TABLE cl_ns_table (id bigint, data text)
OPTIONS (namespace 'test_ns');
INSERT INTO cl_ns_table VALUES (1, 'ns_test');
SELECT * FROM cl_ns_table;
DROP TABLE cl_ns_table;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME cl_volume;
DROP USER MAPPING FOR current_user SERVER cl_volume_server;
DROP SERVER cl_volume_server;
DROP CATALOG cl_catalog;
DROP USER MAPPING FOR current_user SERVER cl_catalog_server;
DROP SERVER cl_catalog_server;
