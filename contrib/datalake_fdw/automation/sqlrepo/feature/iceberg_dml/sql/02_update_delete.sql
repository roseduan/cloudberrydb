-- 02_update_delete.sql
-- Test Iceberg UPDATE and DELETE operations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg UPDATE and DELETE');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER ud_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ud_catalog_server;
CREATE FOREIGN CATALOG ud_catalog SERVER ud_catalog_server;
SET iceberg_default_catalog = 'ud_catalog';

CREATE SERVER ud_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ud_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ud_volume SERVER ud_volume_server OPTIONS(base_path '/ud_volume/');
SET iceberg_default_volume = 'ud_volume';

CREATE ICEBERG TABLE ud_data (id bigint, name text, value decimal(10,2));
INSERT INTO ud_data VALUES
    (1, 'alpha', 100.00),
    (2, 'beta', 200.00),
    (3, 'gamma', 300.00),
    (4, 'delta', 400.00),
    (5, 'epsilon', 500.00);

-- ============================================================
-- Test 1: Basic UPDATE SET ... WHERE
-- ============================================================
SELECT test_log('Test 1: Basic UPDATE');

UPDATE ud_data SET value = 150.00 WHERE id = 1;
SELECT id, name, value FROM ud_data WHERE id = 1;

-- ============================================================
-- Test 2: UPDATE with expression
-- ============================================================
SELECT test_log('Test 2: UPDATE with expression');

UPDATE ud_data SET value = value * 1.1 WHERE id = 2;
SELECT id, name, value FROM ud_data WHERE id = 2;

-- ============================================================
-- Test 3: UPDATE multiple columns
-- ============================================================
SELECT test_log('Test 3: UPDATE multiple columns');

UPDATE ud_data SET name = 'GAMMA', value = 333.33 WHERE id = 3;
SELECT id, name, value FROM ud_data WHERE id = 3;

-- ============================================================
-- Test 4: Basic DELETE WHERE
-- ============================================================
SELECT test_log('Test 4: Basic DELETE');

DELETE FROM ud_data WHERE id = 4;
SELECT COUNT(*) AS after_delete FROM ud_data;
SELECT id FROM ud_data ORDER BY id;

-- ============================================================
-- Test 5: INSERT after DELETE (reuse rows)
-- ============================================================
SELECT test_log('Test 5: INSERT after DELETE');

INSERT INTO ud_data VALUES (6, 'zeta', 600.00), (7, 'eta', 700.00);
SELECT COUNT(*) AS after_insert FROM ud_data;
SELECT * FROM ud_data ORDER BY id;

-- ============================================================
-- Test 6: UPDATE + DELETE combo, verify with aggregates
-- ============================================================
SELECT test_log('Test 6: UPDATE + DELETE combo');

UPDATE ud_data SET value = 0.00 WHERE id = 5;
DELETE FROM ud_data WHERE id = 7;
SELECT COUNT(*) AS final_count, SUM(value) AS final_sum FROM ud_data;

-- ============================================================
-- Test 7: DELETE all rows
-- ============================================================
SELECT test_log('Test 7: DELETE all rows');

CREATE ICEBERG TABLE ud_empty_test (id bigint, val int);
INSERT INTO ud_empty_test VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ud_empty_test WHERE id > 0;
SELECT COUNT(*) AS should_be_zero FROM ud_empty_test;

-- ============================================================
-- Test 8: UPDATE/DELETE on empty table (no error)
-- ============================================================
SELECT test_log('Test 8: UPDATE/DELETE on empty table');

UPDATE ud_empty_test SET val = 999 WHERE id = 1;
DELETE FROM ud_empty_test WHERE id = 1;
SELECT COUNT(*) AS still_zero FROM ud_empty_test;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE ud_data;
DROP TABLE ud_empty_test;
DROP VOLUME ud_volume;
DROP USER MAPPING FOR current_user SERVER ud_volume_server;
DROP SERVER ud_volume_server;
DROP CATALOG ud_catalog;
DROP USER MAPPING FOR current_user SERVER ud_catalog_server;
DROP SERVER ud_catalog_server;
