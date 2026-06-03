-- 02_read_features.sql
-- Test FDW read features: projection, NULLs, large data, aggregates, filters

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Read Features');

-- ============================================================
-- Setup: FDW server and write test data
-- ============================================================
DROP SERVER IF EXISTS rf_server CASCADE;
CREATE SERVER rf_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER rf_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Write base data for projection and filter tests
CREATE FOREIGN TABLE rf_base_w (id int, name text, amount decimal(10,2), category int, flag boolean)
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/base/', format 'parquet');
INSERT INTO rf_base_w SELECT g, 'item_' || g, (g * 10.5)::decimal(10,2), g % 5, (g % 2 = 0)
FROM generate_series(1, 100) g;
DROP FOREIGN TABLE rf_base_w;

CREATE FOREIGN TABLE rf_base_r (id int, name text, amount decimal(10,2), category int, flag boolean)
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/base/', format 'parquet');

-- ============================================================
-- Test 1: Column projection
-- ============================================================
SELECT test_log('Test 1: Column projection');

SELECT id FROM rf_base_r ORDER BY id LIMIT 5;

-- ============================================================
-- Test 2: NULL value reads
-- ============================================================
SELECT test_log('Test 2: NULL value reads');

CREATE FOREIGN TABLE rf_null_w (id int, name text, val decimal(10,2))
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/nulls/', format 'parquet');
INSERT INTO rf_null_w VALUES (1, NULL, NULL), (2, 'present', 99.99), (3, NULL, 50.00);
DROP FOREIGN TABLE rf_null_w;

CREATE FOREIGN TABLE rf_null_r (id int, name text, val decimal(10,2))
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/nulls/', format 'parquet');
SELECT * FROM rf_null_r ORDER BY id;
SELECT COUNT(*) AS total, COUNT(name) AS non_null_name, COUNT(val) AS non_null_val FROM rf_null_r;
DROP FOREIGN TABLE rf_null_r;

-- ============================================================
-- Test 3: Large data reads
-- ============================================================
SELECT test_log('Test 3: Large data reads (5000 rows)');

CREATE FOREIGN TABLE rf_large_w (id int, val int, label text)
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/large/', format 'parquet');
INSERT INTO rf_large_w SELECT g, g * 7, 'label_' || g FROM generate_series(1, 5000) g;
DROP FOREIGN TABLE rf_large_w;

CREATE FOREIGN TABLE rf_large_r (id int, val int, label text)
SERVER rf_server
OPTIONS (filePath '/warehouse/fdw-test/feature/readfeat/large/', format 'parquet');
SELECT COUNT(*) AS large_count FROM rf_large_r;
DROP FOREIGN TABLE rf_large_r;

-- ============================================================
-- Test 4: Aggregates
-- ============================================================
SELECT test_log('Test 4: Aggregates');

SELECT COUNT(*) AS cnt,
       SUM(amount) AS total,
       AVG(amount) AS avg_amt,
       MIN(amount) AS min_amt,
       MAX(amount) AS max_amt
FROM rf_base_r;

-- ============================================================
-- Test 5: WHERE filter on reads
-- ============================================================
SELECT test_log('Test 5: WHERE filter on reads');

SELECT id, name, amount FROM rf_base_r WHERE id <= 5 ORDER BY id;
SELECT COUNT(*) AS flagged FROM rf_base_r WHERE flag = true;
SELECT COUNT(*) AS cat_zero FROM rf_base_r WHERE category = 0;

-- ============================================================
-- Test 6: ORDER BY + LIMIT
-- ============================================================
SELECT test_log('Test 6: ORDER BY + LIMIT');

SELECT id, amount FROM rf_base_r ORDER BY amount DESC LIMIT 5;
SELECT id, amount FROM rf_base_r ORDER BY amount ASC LIMIT 3;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE rf_base_r;
DROP USER MAPPING FOR gpadmin SERVER rf_server;
DROP SERVER rf_server;
