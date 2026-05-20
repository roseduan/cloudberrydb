-- Iceberg DML Edge Cases Test
-- Purpose: Exercise all INSERT/UPDATE/DELETE code paths with edge cases
-- Target: pg_iceberg_am_handler.c (tuple_insert, tuple_update, tuple_delete,
--         multi_insert), pg_iceberg_metadata_tracker.c

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup
CREATE SERVER dml_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dml_catalog_server;
CREATE FOREIGN CATALOG dml_catalog SERVER dml_catalog_server;
SET iceberg_default_catalog = 'dml_catalog';

CREATE SERVER dml_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER dml_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dml_volume SERVER dml_volume_server OPTIONS(base_path '/dml_volume/');
SET iceberg_default_volume = 'dml_volume';

-- ============================================================
-- Test 1: INSERT variations
-- ============================================================
CREATE ICEBERG TABLE dml_insert_test (id bigint, name text, val int);

-- Single row
INSERT INTO dml_insert_test VALUES (1, 'Alice', 100);

-- Multi-row batch
INSERT INTO dml_insert_test VALUES
    (2, 'Bob', 200),
    (3, 'Charlie', 300),
    (4, 'Diana', 400);

-- INSERT with NULL values
INSERT INTO dml_insert_test VALUES (5, NULL, NULL);

-- INSERT with empty string
INSERT INTO dml_insert_test VALUES (6, '', 0);

-- INSERT ... SELECT from same iceberg table
INSERT INTO dml_insert_test SELECT id + 100, name, val FROM dml_insert_test WHERE id <= 2;

-- INSERT ... SELECT from a generated series (heap)
CREATE TEMP TABLE dml_heap_source AS SELECT generate_series(200, 203)::bigint AS id, 'heap'::text AS name, 999 AS val;
INSERT INTO dml_insert_test SELECT * FROM dml_heap_source;
DROP TABLE dml_heap_source;

SELECT COUNT(*) FROM dml_insert_test;
SELECT * FROM dml_insert_test ORDER BY id;

DROP TABLE dml_insert_test;

-- ============================================================
-- Test 2: UPDATE variations
-- ============================================================
CREATE ICEBERG TABLE dml_update_test (id bigint, name text, val int);

INSERT INTO dml_update_test VALUES
    (1, 'Alice', 100),
    (2, 'Bob', 200),
    (3, 'Charlie', 300),
    (4, 'Diana', 400),
    (5, 'Eve', 500);

-- Update single column
UPDATE dml_update_test SET name = 'Alice Updated' WHERE id = 1;

-- Update multiple columns
UPDATE dml_update_test SET name = 'Bob Updated', val = 250 WHERE id = 2;

-- Update with arithmetic expression
UPDATE dml_update_test SET val = val + 50 WHERE id = 3;

-- Update to NULL
UPDATE dml_update_test SET name = NULL WHERE id = 4;

-- Update with complex WHERE
UPDATE dml_update_test SET val = 999 WHERE id IN (1, 3, 5);

-- Update with AND/OR
UPDATE dml_update_test SET name = 'filtered' WHERE (id > 3 AND val < 600) OR name IS NULL;

-- Update with BETWEEN
UPDATE dml_update_test SET val = 0 WHERE id BETWEEN 2 AND 4;

SELECT * FROM dml_update_test ORDER BY id;

DROP TABLE dml_update_test;

-- ============================================================
-- Test 3: DELETE variations
-- ============================================================
CREATE ICEBERG TABLE dml_delete_test (id bigint, name text, val int);

INSERT INTO dml_delete_test VALUES
    (1, 'Alice', 100),
    (2, 'Bob', 200),
    (3, 'Charlie', 300),
    (4, 'Diana', 400),
    (5, 'Eve', 500),
    (6, 'Frank', 600),
    (7, 'Grace', 700),
    (8, 'Henry', 800);

-- Delete single row by exact match
DELETE FROM dml_delete_test WHERE id = 1;
SELECT COUNT(*) FROM dml_delete_test;

-- Delete multiple rows by range
DELETE FROM dml_delete_test WHERE id BETWEEN 6 AND 8;
SELECT COUNT(*) FROM dml_delete_test;

-- Delete with IN
DELETE FROM dml_delete_test WHERE id IN (2, 4);
SELECT COUNT(*) FROM dml_delete_test;

-- Verify remaining rows
SELECT * FROM dml_delete_test ORDER BY id;

-- Delete all remaining rows
DELETE FROM dml_delete_test;
SELECT COUNT(*) FROM dml_delete_test;

DROP TABLE dml_delete_test;

-- ============================================================
-- Test 4: DELETE on empty table (no-op)
-- ============================================================
CREATE ICEBERG TABLE dml_empty_del (id bigint);
DELETE FROM dml_empty_del WHERE id = 1;
SELECT COUNT(*) FROM dml_empty_del;
DROP TABLE dml_empty_del;

-- ============================================================
-- Test 5: Mixed DML in transaction
-- ============================================================
CREATE ICEBERG TABLE dml_txn_test (id bigint, name text);

BEGIN;
INSERT INTO dml_txn_test VALUES (1, 'txn_insert');
INSERT INTO dml_txn_test VALUES (2, 'txn_insert2');
UPDATE dml_txn_test SET name = 'txn_updated' WHERE id = 1;
DELETE FROM dml_txn_test WHERE id = 2;
COMMIT;

SELECT * FROM dml_txn_test ORDER BY id;

DROP TABLE dml_txn_test;

-- ============================================================
-- Test 6: Transaction abort - data should not change
-- ============================================================
CREATE ICEBERG TABLE dml_abort_test (id bigint, val int);
INSERT INTO dml_abort_test VALUES (1, 100);

-- This transaction will abort due to division by zero
DO $$
BEGIN
    INSERT INTO dml_abort_test VALUES (2, 200);
    -- Force an error
    PERFORM 1/0;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Transaction aborted as expected: %', SQLERRM;
END;
$$;

-- Only the original row should exist
SELECT * FROM dml_abort_test ORDER BY id;

DROP TABLE dml_abort_test;

-- ============================================================
-- Test 7: Multiple INSERTs in same transaction (file accumulation)
-- ============================================================
CREATE ICEBERG TABLE dml_multi_insert (id bigint, val text);

BEGIN;
INSERT INTO dml_multi_insert VALUES (1, 'batch1');
INSERT INTO dml_multi_insert VALUES (2, 'batch2');
INSERT INTO dml_multi_insert VALUES (3, 'batch3');
INSERT INTO dml_multi_insert VALUES (4, 'batch4');
INSERT INTO dml_multi_insert VALUES (5, 'batch5');
COMMIT;

SELECT COUNT(*) FROM dml_multi_insert;
SELECT * FROM dml_multi_insert ORDER BY id;

DROP TABLE dml_multi_insert;

-- ============================================================
-- Test 8: Aggregation queries
-- ============================================================
CREATE ICEBERG TABLE dml_agg_test (id bigint, category text, amount decimal(10,2));

INSERT INTO dml_agg_test VALUES
    (1, 'A', 100.50),
    (2, 'B', 200.75),
    (3, 'A', 300.25),
    (4, 'B', 400.00),
    (5, 'A', 50.50),
    (6, 'C', 600.00);

-- Aggregation queries
SELECT COUNT(*) FROM dml_agg_test;
SELECT category, COUNT(*), SUM(amount) FROM dml_agg_test GROUP BY category ORDER BY category;
SELECT MIN(amount), MAX(amount), AVG(amount) FROM dml_agg_test;
SELECT DISTINCT category FROM dml_agg_test ORDER BY category;
SELECT category FROM dml_agg_test GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;

DROP TABLE dml_agg_test;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME dml_volume;
DROP USER MAPPING FOR current_user SERVER dml_volume_server;
DROP SERVER dml_volume_server;
DROP CATALOG dml_catalog;
DROP USER MAPPING FOR current_user SERVER dml_catalog_server;
DROP SERVER dml_catalog_server;
