-- 03_table_name_validation.sql
-- Issue #369: Iceberg table / namespace names are interpolated verbatim into
-- the warehouse storage path and the catalog REST URL.  CREATE ICEBERG TABLE
-- must therefore reject names outside the safe cross-catalog whitelist
-- (ASCII letters, digits and underscore, and not all digits).

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg table name validation (issue #369)');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER dt_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dt_catalog_server;
CREATE FOREIGN CATALOG dt_catalog SERVER dt_catalog_server;
SET iceberg_default_catalog = 'dt_catalog';

CREATE SERVER dt_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER dt_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dt_volume SERVER dt_volume_server OPTIONS(base_path '/dt_volume/');
SET iceberg_default_volume = 'dt_volume';

CREATE SCHEMA ib;

-- ============================================================
-- Test 1: Names with URL-unsafe / path-unsafe characters are rejected
-- at CREATE time (no table is left behind).
-- ============================================================
SELECT test_log('Test 1: invalid names are rejected');

-- '#' (fragment) plus non-ASCII
CREATE ICEBERG TABLE ib."应付账款#6月对公" (id int, amt numeric(12,2));
-- '/' (path separator)
CREATE ICEBERG TABLE ib."T_ACC_2024/12/31" (id int, amt numeric(12,2));
-- space
CREATE ICEBERG TABLE ib."ap account" (id int);
-- '%'
CREATE ICEBERG TABLE ib."ap%2f" (id int);
-- backslash
CREATE ICEBERG TABLE ib."ap\back" (id int);
-- '?'
CREATE ICEBERG TABLE ib."ap?q" (id int);
-- double quote
CREATE ICEBERG TABLE ib."ap""q" (id int);
-- '.' (namespace separator in iceberg identifiers)
CREATE ICEBERG TABLE ib."ap.acc" (id int);
-- pure non-ASCII (Hive metastore does not support it either)
CREATE ICEBERG TABLE ib."应付账款" (id int);
-- all digits
CREATE ICEBERG TABLE ib."123" (id int);

-- The rejected tables must not exist in PG either.
SELECT count(*) AS leftover_tables
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'ib';

-- ============================================================
-- Test 2: A valid ASCII name still works end-to-end.
-- ============================================================
SELECT test_log('Test 2: valid name works end-to-end');

CREATE ICEBERG TABLE ib.dt_name_ok (id bigint, amt numeric(12,2));
INSERT INTO ib.dt_name_ok VALUES (1, 100.00), (2, 200.00);
SELECT count(*) AS n, sum(amt) AS s FROM ib.dt_name_ok;
DROP TABLE ib.dt_name_ok;

-- ============================================================
-- Cleanup
-- ============================================================
DROP SCHEMA ib;
DROP VOLUME dt_volume;
DROP USER MAPPING FOR current_user SERVER dt_volume_server;
DROP SERVER dt_volume_server;
DROP CATALOG dt_catalog;
DROP USER MAPPING FOR current_user SERVER dt_catalog_server;
DROP SERVER dt_catalog_server;
