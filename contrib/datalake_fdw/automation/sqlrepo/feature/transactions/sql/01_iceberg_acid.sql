-- 01_iceberg_acid.sql
-- Test Iceberg ACID operations (INSERT/UPDATE/DELETE)

\i ../../../lib/sql/common_setup.sql

-- Create builtin catalog and volume
CREATE SERVER txn_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER txn_catalog_server;
CREATE FOREIGN CATALOG txn_catalog SERVER txn_catalog_server;
SET iceberg_default_catalog='txn_catalog';

CREATE SERVER txn_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER txn_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME txn_volume SERVER txn_volume_server OPTIONS(base_path '/txn_volume/');
SET iceberg_default_volume='txn_volume';

SELECT test_log('Feature Test: Iceberg ACID Operations');

-- Test 1: Create table and initial INSERT
SELECT test_log('Test 1: Initial INSERT');
CREATE ICEBERG TABLE acid_test (
    id bigint,
    name text,
    value numeric(15,2),
    status text
);

INSERT INTO acid_test VALUES
    (1, 'Alice', 100, 'active'),
    (2, 'Bob', 200, 'active'),
    (3, 'Charlie', 300, 'active');

SELECT * FROM acid_test ORDER BY id;

-- Test 2: UPDATE operation
SELECT test_log('Test 2: UPDATE operation');
UPDATE acid_test SET value = value * 1.1 WHERE id = 1;
UPDATE acid_test SET status = 'inactive' WHERE id = 2;

SELECT * FROM acid_test ORDER BY id;

-- Test 3: DELETE operation
SELECT test_log('Test 3: DELETE operation');
DELETE FROM acid_test WHERE id = 3;

SELECT * FROM acid_test ORDER BY id;

-- Test 4: INSERT after DELETE (reuse deleted ID)
SELECT test_log('Test 4: INSERT after DELETE');
INSERT INTO acid_test VALUES (3, 'David', 400, 'active');

SELECT * FROM acid_test ORDER BY id;

-- Test 5: Complex UPDATE with subquery
SELECT test_log('Test 5: Complex UPDATE');
UPDATE acid_test SET value = value + 50 WHERE status = 'active';

SELECT * FROM acid_test ORDER BY id;

-- Test 6: Verify final state
SELECT test_log('Test 6: Verify final state');
SELECT COUNT(*) as total_rows, SUM(value) as total_value FROM acid_test;

-- Cleanup
DROP TABLE acid_test;
DROP VOLUME txn_volume;
DROP USER MAPPING FOR current_user SERVER txn_volume_server;
DROP SERVER txn_volume_server;
DROP CATALOG txn_catalog;
DROP USER MAPPING FOR current_user SERVER txn_catalog_server;
DROP SERVER txn_catalog_server;
