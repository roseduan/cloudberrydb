-- Iceberg Namespace Test
-- Purpose: Exercise namespace resolution and location generation
-- Target: pg_iceberg_catalog.c (namespace resolution, pg_iceberg_generate_builtin_location)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup
CREATE SERVER ns_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ns_catalog_server;
CREATE FOREIGN CATALOG ns_catalog SERVER ns_catalog_server;
SET iceberg_default_catalog = 'ns_catalog';

CREATE SERVER ns_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER ns_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ns_volume SERVER ns_volume_server OPTIONS(base_path '/ns_volume/');
SET iceberg_default_volume = 'ns_volume';

-- ============================================================
-- Test 1: Default namespace (public schema, no explicit namespace)
-- ============================================================
CREATE ICEBERG TABLE ns_default_test (id bigint, val text);
INSERT INTO ns_default_test VALUES (1, 'default_ns');
SELECT * FROM ns_default_test;
DROP TABLE ns_default_test;

-- ============================================================
-- Test 2: Explicit namespace via OPTIONS
-- ============================================================
CREATE ICEBERG TABLE ns_explicit_test (id bigint, val text)
OPTIONS (namespace 'custom_ns');

INSERT INTO ns_explicit_test VALUES (1, 'custom_ns');
SELECT * FROM ns_explicit_test;
DROP TABLE ns_explicit_test;

-- ============================================================
-- Test 3: Explicit namespace and table_name via OPTIONS
-- ============================================================
CREATE ICEBERG TABLE ns_both_opts (id bigint, val text)
OPTIONS (namespace 'my_ns', table_name 'external_table_name');

INSERT INTO ns_both_opts VALUES (1, 'both_opts');
SELECT * FROM ns_both_opts;
DROP TABLE ns_both_opts;

-- ============================================================
-- Test 4: Tables in different PostgreSQL schemas
-- ============================================================
CREATE SCHEMA ns_schema_a;
CREATE SCHEMA ns_schema_b;

CREATE ICEBERG TABLE ns_schema_a.ns_schema_test (id bigint, val text);
INSERT INTO ns_schema_a.ns_schema_test VALUES (1, 'schema_a');

CREATE ICEBERG TABLE ns_schema_b.ns_schema_test (id bigint, val text);
INSERT INTO ns_schema_b.ns_schema_test VALUES (2, 'schema_b');

-- Verify data isolation between schemas
SELECT * FROM ns_schema_a.ns_schema_test;
SELECT * FROM ns_schema_b.ns_schema_test;

DROP TABLE ns_schema_a.ns_schema_test;
DROP TABLE ns_schema_b.ns_schema_test;
DROP SCHEMA ns_schema_a;
DROP SCHEMA ns_schema_b;

-- ============================================================
-- Test 5: Multiple tables in same namespace
-- ============================================================
CREATE ICEBERG TABLE ns_multi_1 (id bigint)
OPTIONS (namespace 'shared_ns');
CREATE ICEBERG TABLE ns_multi_2 (id bigint)
OPTIONS (namespace 'shared_ns');

INSERT INTO ns_multi_1 VALUES (1), (2);
INSERT INTO ns_multi_2 VALUES (10), (20);

SELECT COUNT(*) AS t1_count FROM ns_multi_1;
SELECT COUNT(*) AS t2_count FROM ns_multi_2;

DROP TABLE ns_multi_1;
DROP TABLE ns_multi_2;

-- ============================================================
-- Test 6: Catalog with default_namespace option
-- ============================================================
CREATE SERVER ns_cat_with_default_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ns_cat_with_default_server;
CREATE FOREIGN CATALOG ns_cat_with_default SERVER ns_cat_with_default_server
OPTIONS (default_namespace 'my_default_ns');

SET iceberg_default_catalog = 'ns_cat_with_default';

-- Table should use 'my_default_ns' as namespace
CREATE ICEBERG TABLE ns_use_default (id bigint, val text);
INSERT INTO ns_use_default VALUES (1, 'using_default_ns');
SELECT * FROM ns_use_default;
DROP TABLE ns_use_default;

-- Explicit namespace should override default
CREATE ICEBERG TABLE ns_override_default (id bigint)
OPTIONS (namespace 'override_ns');
INSERT INTO ns_override_default VALUES (1);
SELECT COUNT(*) FROM ns_override_default;
DROP TABLE ns_override_default;

-- ============================================================
-- Cleanup
-- ============================================================
DROP CATALOG ns_cat_with_default;
DROP USER MAPPING FOR current_user SERVER ns_cat_with_default_server;
DROP SERVER ns_cat_with_default_server;

SET iceberg_default_catalog = 'ns_catalog';
DROP VOLUME ns_volume;
DROP USER MAPPING FOR current_user SERVER ns_volume_server;
DROP SERVER ns_volume_server;
DROP CATALOG ns_catalog;
DROP USER MAPPING FOR current_user SERVER ns_catalog_server;
DROP SERVER ns_catalog_server;
