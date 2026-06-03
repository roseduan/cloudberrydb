-- 01_insert_select.sql
-- Test Iceberg INSERT and SELECT operations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg INSERT and SELECT');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER is_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER is_catalog_server;
CREATE FOREIGN CATALOG is_catalog SERVER is_catalog_server;
SET iceberg_default_catalog = 'is_catalog';

CREATE SERVER is_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER is_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME is_volume SERVER is_volume_server OPTIONS(base_path '/is_volume/');
SET iceberg_default_volume = 'is_volume';

CREATE ICEBERG TABLE is_data (id bigint, name text, amount decimal(10,2), flag boolean);

-- ============================================================
-- Test 1: Single-row INSERT + SELECT
-- ============================================================
SELECT test_log('Test 1: Single-row INSERT + SELECT');

INSERT INTO is_data VALUES (1, 'alice', 100.50, true);
SELECT * FROM is_data ORDER BY id;

-- ============================================================
-- Test 2: Multi-row INSERT (VALUES list)
-- ============================================================
SELECT test_log('Test 2: Multi-row INSERT');

INSERT INTO is_data VALUES
    (2, 'bob', 200.75, false),
    (3, 'carol', 300.00, true),
    (4, 'dave', 150.25, false),
    (5, 'eve', 500.99, true),
    (6, 'frank', 75.00, true);
SELECT COUNT(*) AS total_rows FROM is_data;

-- ============================================================
-- Test 3: Bulk INSERT with generate_series
-- ============================================================
SELECT test_log('Test 3: Bulk INSERT (1000 rows)');

CREATE ICEBERG TABLE is_bulk (id bigint, val int);
INSERT INTO is_bulk SELECT g, g * 10 FROM generate_series(1, 1000) g;
SELECT COUNT(*) AS bulk_count FROM is_bulk;

-- ============================================================
-- Test 4: INSERT with NULL values
-- ============================================================
SELECT test_log('Test 4: INSERT with NULL values');

INSERT INTO is_data VALUES (7, NULL, NULL, NULL);
INSERT INTO is_data VALUES (8, 'grace', NULL, true);
SELECT id, name, amount, flag FROM is_data WHERE id >= 7 ORDER BY id;

-- ============================================================
-- Test 5: SELECT WHERE conditions
-- ============================================================
SELECT test_log('Test 5: SELECT WHERE conditions');

SELECT id, name, amount FROM is_data WHERE flag = true ORDER BY id;
SELECT id, name FROM is_data WHERE amount > 200.00 ORDER BY id;
SELECT id, name FROM is_data WHERE name IS NOT NULL AND amount IS NOT NULL ORDER BY id;

-- ============================================================
-- Test 6: SELECT ORDER BY + LIMIT
-- ============================================================
SELECT test_log('Test 6: SELECT ORDER BY + LIMIT');

SELECT id, name, amount FROM is_data ORDER BY amount DESC NULLS LAST LIMIT 3;
SELECT id, val FROM is_bulk ORDER BY id LIMIT 5;
SELECT id, val FROM is_bulk ORDER BY id DESC LIMIT 5;

-- ============================================================
-- Test 7: Aggregates
-- ============================================================
SELECT test_log('Test 7: Aggregate functions');

SELECT COUNT(*) AS cnt,
       SUM(amount) AS total,
       AVG(amount) AS avg_amt,
       MIN(amount) AS min_amt,
       MAX(amount) AS max_amt
FROM is_data WHERE amount IS NOT NULL;

SELECT SUM(val) AS bulk_sum, MIN(val) AS bulk_min, MAX(val) AS bulk_max FROM is_bulk;

-- ============================================================
-- Test 8: GROUP BY
-- ============================================================
SELECT test_log('Test 8: GROUP BY query');

SELECT flag, COUNT(*) AS cnt, SUM(amount) AS total
FROM is_data
WHERE flag IS NOT NULL
GROUP BY flag
ORDER BY flag;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE is_data;
DROP TABLE is_bulk;
DROP VOLUME is_volume;
DROP USER MAPPING FOR current_user SERVER is_volume_server;
DROP SERVER is_volume_server;
DROP CATALOG is_catalog;
DROP USER MAPPING FOR current_user SERVER is_catalog_server;
DROP SERVER is_catalog_server;
