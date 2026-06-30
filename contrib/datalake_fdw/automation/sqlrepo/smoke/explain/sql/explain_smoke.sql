-- EXPLAIN / Cost Estimation Smoke Test
-- Purpose: Verify EXPLAIN output includes Foreign Scan node and basic cost info
-- Uses Iceberg (self-contained) and S3 foreign tables

-- Setup extensions
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: Iceberg builtin catalog + S3 volume
-- ============================================================
CREATE SERVER explain_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER explain_catalog_server;
CREATE FOREIGN CATALOG explain_catalog SERVER explain_catalog_server;
SET iceberg_default_catalog = 'explain_catalog';

CREATE SERVER explain_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER explain_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME explain_volume SERVER explain_volume_server OPTIONS(base_path '/explain_volume/');
SET iceberg_default_volume = 'explain_volume';

-- Create test data
CREATE ICEBERG TABLE explain_test (
    id bigint,
    name text,
    category text,
    price decimal(10,2));

INSERT INTO explain_test VALUES
    (1, 'Alpha', 'A', 100.00),
    (2, 'Beta', 'B', 200.00),
    (3, 'Gamma', 'A', 300.00),
    (4, 'Delta', 'B', 400.00),
    (5, 'Epsilon', 'C', 500.00);

-- ============================================================
-- Test 1: Basic EXPLAIN shows Foreign Scan
-- ============================================================
EXPLAIN (COSTS OFF) SELECT * FROM explain_test;

-- ============================================================
-- Test 2: EXPLAIN with WHERE shows filter info
-- ============================================================
EXPLAIN (COSTS OFF) SELECT * FROM explain_test WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM explain_test WHERE category = 'A';
EXPLAIN (COSTS OFF) SELECT * FROM explain_test WHERE price > 200.00;

-- ============================================================
-- Test 3: EXPLAIN with aggregation
-- ============================================================
EXPLAIN (COSTS OFF) SELECT category, COUNT(*) FROM explain_test GROUP BY category;

-- ============================================================
-- Test 4: EXPLAIN with JOIN (Iceberg + local)
-- ============================================================
CREATE TABLE explain_local (cat text, label text);
INSERT INTO explain_local VALUES ('A', 'Alpha Cat'), ('B', 'Beta Cat');

EXPLAIN (COSTS OFF)
SELECT e.name, l.label
FROM explain_test e JOIN explain_local l ON e.category = l.cat;

DROP TABLE explain_local;

-- ============================================================
-- Test 5: EXPLAIN ANALYZE (actually execute)
-- ============================================================
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM explain_test WHERE id > 0;

-- ============================================================
-- Test 6: EXPLAIN with VERBOSE (shows column details)
-- ============================================================
EXPLAIN (VERBOSE, COSTS OFF) SELECT id, name FROM explain_test WHERE id = 1;

-- ============================================================
-- Test 7: EXPLAIN on S3 foreign table
-- ============================================================
DROP SERVER IF EXISTS s3_explain_server CASCADE;
CREATE SERVER s3_explain_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'hadoop', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER s3_explain_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

DROP FOREIGN TABLE IF EXISTS s3_explain_test;
CREATE FOREIGN TABLE s3_explain_test (id int, name text)
SERVER s3_explain_server
OPTIONS (filePath '/test-data/orc/basic/', format 'orc');

EXPLAIN (COSTS OFF) SELECT * FROM s3_explain_test;
EXPLAIN (COSTS OFF) SELECT * FROM s3_explain_test WHERE id > 1;

-- Cleanup S3
DROP FOREIGN TABLE s3_explain_test;
DROP USER MAPPING FOR gpadmin SERVER s3_explain_server;
DROP SERVER s3_explain_server;

-- ============================================================
-- Cleanup Iceberg
-- ============================================================
DROP TABLE explain_test;
DROP VOLUME explain_volume;
DROP USER MAPPING FOR current_user SERVER explain_volume_server;
DROP SERVER explain_volume_server;
DROP CATALOG explain_catalog;
DROP USER MAPPING FOR current_user SERVER explain_catalog_server;
DROP SERVER explain_catalog_server;
