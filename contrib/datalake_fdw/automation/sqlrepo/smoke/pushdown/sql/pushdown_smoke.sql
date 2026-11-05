-- Predicate Pushdown Smoke Test
-- Purpose: Verify filter pushdown works correctly for all supported operators
-- Uses Iceberg tables (self-contained, no external data needed)
-- Tests: =, >, <, >=, <=, !=, IS NULL, IS NOT NULL, LIKE, IN
-- Also tests: EXPLAIN output and GUC toggle

-- Clean up previous run leftovers
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup (inline to avoid \i path issues with pg_regress)
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: Iceberg with builtin catalog + S3 volume
-- ============================================================
CREATE SERVER pushdown_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER pushdown_catalog_server;
CREATE FOREIGN CATALOG pushdown_catalog SERVER pushdown_catalog_server;
SET iceberg_default_catalog = 'pushdown_catalog';

CREATE SERVER pushdown_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER pushdown_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME pushdown_volume SERVER pushdown_volume_server OPTIONS(base_path '/pushdown_volume/');
SET iceberg_default_volume = 'pushdown_volume';

-- ============================================================
-- Create test table with diverse data
-- ============================================================
CREATE ICEBERG TABLE pushdown_test (
    id bigint,
    name text,
    category text,
    price decimal(10,2),
    quantity int,
    created_date date,
    is_active boolean
);

INSERT INTO pushdown_test VALUES
    (1, 'Alpha Product', 'electronics', 299.99, 10, '2024-01-15', true),
    (2, 'Beta Widget', 'office', 19.99, 100, '2024-02-20', true),
    (3, 'Gamma Device', 'electronics', 599.99, 5, '2024-03-10', false),
    (4, 'Delta Supply', 'office', 9.99, 500, '2024-04-01', true),
    (5, 'Epsilon Tool', 'hardware', 149.99, 25, '2024-05-15', false),
    (6, NULL, 'hardware', NULL, NULL, NULL, NULL),
    (7, 'Eta Service', NULL, 0.00, 0, '2024-07-01', true),
    (8, 'Theta Pack', 'office', 49.99, 200, '2024-08-20', true);

-- Verify base data
SELECT COUNT(*) FROM pushdown_test;

-- ============================================================
-- Test 1: Equality operator (=)
-- ============================================================
SELECT id, name FROM pushdown_test WHERE id = 3;
SELECT id, name FROM pushdown_test WHERE category = 'office' ORDER BY id;

-- ============================================================
-- Test 2: Comparison operators (>, <, >=, <=)
-- ============================================================
SELECT id, price FROM pushdown_test WHERE price > 100.00 ORDER BY id;
SELECT id, price FROM pushdown_test WHERE price < 50.00 ORDER BY id;
SELECT id, quantity FROM pushdown_test WHERE quantity >= 100 ORDER BY id;
SELECT id, quantity FROM pushdown_test WHERE quantity <= 10 ORDER BY id;

-- ============================================================
-- Test 3: Not-equal operator (!=)
-- ============================================================
SELECT id, category FROM pushdown_test WHERE category != 'office' ORDER BY id;

-- ============================================================
-- Test 4: IS NULL / IS NOT NULL
-- ============================================================
SELECT id FROM pushdown_test WHERE name IS NULL;
SELECT id FROM pushdown_test WHERE price IS NOT NULL ORDER BY id;
SELECT id FROM pushdown_test WHERE category IS NULL;

-- ============================================================
-- Test 5: LIKE pattern matching
-- ============================================================
SELECT id, name FROM pushdown_test WHERE name LIKE 'Alpha%';
SELECT id, name FROM pushdown_test WHERE name LIKE '%Widget%';
SELECT id, name FROM pushdown_test WHERE name LIKE '%Tool';

-- ============================================================
-- Test 6: IN list
-- ============================================================
SELECT id, category FROM pushdown_test WHERE category IN ('electronics', 'hardware') ORDER BY id;
SELECT id, name FROM pushdown_test WHERE id IN (1, 3, 5, 7) ORDER BY id;

-- ============================================================
-- Test 7: Combined filters
-- ============================================================
SELECT id, name, price FROM pushdown_test
WHERE category = 'office' AND price > 10.00
ORDER BY id;

SELECT id, name FROM pushdown_test
WHERE (category = 'electronics' OR category = 'hardware') AND is_active = false
ORDER BY id;

SELECT id, name, price FROM pushdown_test
WHERE price BETWEEN 10.00 AND 200.00 AND quantity > 0
ORDER BY id;

-- ============================================================
-- Test 8: Date filters
-- ============================================================
SELECT id, created_date FROM pushdown_test
WHERE created_date > '2024-04-01' ORDER BY id;
SELECT id, created_date FROM pushdown_test
WHERE created_date BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id;

-- ============================================================
-- Test 9: Boolean filters
-- ============================================================
SELECT id, name FROM pushdown_test WHERE is_active = true ORDER BY id;
SELECT id, name FROM pushdown_test WHERE is_active = false ORDER BY id;

-- ============================================================
-- Test 10: EXPLAIN shows Foreign Scan (pushdown active)
-- ============================================================
EXPLAIN (COSTS OFF) SELECT * FROM pushdown_test WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM pushdown_test WHERE category = 'office' AND price > 10.00;

-- ============================================================
-- Test 11: GUC toggle - disable pushdown and verify results match
-- ============================================================

-- Results with pushdown enabled (default)
SELECT id, name FROM pushdown_test WHERE category = 'electronics' ORDER BY id;

-- Disable pushdown
SET datalake.disable_filter_pushdown = on;
SELECT id, name FROM pushdown_test WHERE category = 'electronics' ORDER BY id;

-- Re-enable pushdown
SET datalake.disable_filter_pushdown = off;

-- ============================================================
-- Test 12: an OR or NOT with a branch the filter cannot express
-- ============================================================
-- The agent skips data files the pushed filter rules out, so the filter
-- must never be stricter than the WHERE clause.  An unsupported branch
-- (IS DISTINCT FROM, IS TRUE) used to be dropped from inside its OR/NOT,
-- leaving "id = 1" alone (NOT is rewritten to OR first); the file holding
-- the other row was then skipped.  Two INSERTs give each row its own
-- data file.
CREATE ICEBERG TABLE pushdown_or_test (id int, b int);
INSERT INTO pushdown_or_test VALUES (1, 0);
INSERT INTO pushdown_or_test VALUES (100, 5);

SELECT * FROM pushdown_or_test WHERE id = 1 OR b IS DISTINCT FROM 0 ORDER BY id;
SELECT * FROM pushdown_or_test WHERE id = 1 OR (b = 5) IS TRUE ORDER BY id;
SELECT * FROM pushdown_or_test WHERE NOT (id <> 1 AND b IS DISTINCT FROM 5) ORDER BY id;
-- Under AND the supported part is still pushed: same rows either way.
SELECT * FROM pushdown_or_test WHERE (id = 1 AND b IS DISTINCT FROM 7) OR b = 5 ORDER BY id;

DROP TABLE pushdown_or_test;

-- ============================================================
-- Test 13: comparisons file bounds cannot decide the way PG does
-- ============================================================
-- Each of these skipped a file holding a matching row, or raised an error:
--   1    char(n) ignores trailing spaces, file bounds compare bytes
--   2-3  text bounds are in byte order, a non-C collation is not
--        ('a' < 'B' under en_US)
--   4-5  the agent rounds a float8 constant to the float4 column
--   6    bool ordering raised "unsupported data type"
--   7    an IN list holding a NULL raised an error
--   8    extra_float_digits = 0 sent 0.30000000000000004 as 0.3
--   9-10 bounds leave NaN out, but NaN satisfies "> 1" in PG
-- Rows differ by collation, so compare against pushdown off -- both the
-- agent's file pruning and the reader's row group pruning -- instead of
-- listing them.  Rows 1 and 2 get a data file each.
CREATE ICEBERG TABLE pushdown_type_test (id int, c3 char(3), tx text, f4 float4,
                                         bo boolean, f8 float8);
INSERT INTO pushdown_type_test VALUES (1, 'a', 'B', 0.1, true, 0.3);
INSERT INTO pushdown_type_test VALUES (2, 'b', 'a', 2.5, false, 5);
-- Lake tables are distributed randomly, so put 30 of each in: every
-- segment's data file then holds both 0.5 and NaN.
INSERT INTO pushdown_type_test
    SELECT 3, 'c', 'c', 3, false, CASE WHEN g % 2 = 0 THEN 0.5 ELSE 'NaN'::float8 END
    FROM generate_series(1, 60) g;

SET extra_float_digits = 0;
CREATE TEMP VIEW pushdown_type_q AS
    SELECT 1 AS q, array_agg(id ORDER BY id) AS ids FROM pushdown_type_test WHERE c3 = 'a  '
    UNION ALL SELECT 2, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE tx > 'a'
    UNION ALL SELECT 3, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE tx < 'B'
    UNION ALL SELECT 4, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE f4 > 0.1::float8
    UNION ALL SELECT 5, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE f4 < 2.5000001::float8
    UNION ALL SELECT 6, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE bo < true
    UNION ALL SELECT 7, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE id IN (1, NULL)
    UNION ALL SELECT 8, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE f8 < 0.30000000000000004
    UNION ALL SELECT 9, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE f8 > 1
    UNION ALL SELECT 10, array_agg(id ORDER BY id) FROM pushdown_type_test WHERE 1 < f8;

SET datalake.iceberg_enable_predicate_pushdown = off;
SET gp_external_enable_filter_pushdown = off;
CREATE TEMP TABLE pushdown_type_expected AS SELECT * FROM pushdown_type_q
    DISTRIBUTED RANDOMLY;
RESET datalake.iceberg_enable_predicate_pushdown;
RESET gp_external_enable_filter_pushdown;

SELECT e.q, a.ids IS NOT DISTINCT FROM e.ids AS same_as_without_pushdown
FROM pushdown_type_expected e JOIN pushdown_type_q a USING (q)
ORDER BY e.q;
RESET extra_float_digits;

DROP TABLE pushdown_type_expected;
DROP VIEW pushdown_type_q;
DROP TABLE pushdown_type_test;

-- 'a' = ANY(arr) tests whether the array column holds 'a', not an IN list
-- over a column; it used to raise "unsupported data type (25)" while the
-- filter was built, so an empty table is enough.
CREATE ICEBERG TABLE pushdown_any_test (id int, arr text[]);
SELECT count(*) FROM pushdown_any_test WHERE 'a' = ANY(arr);
DROP TABLE pushdown_any_test;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE IF EXISTS pushdown_test;
DROP VOLUME IF EXISTS pushdown_volume;
DROP USER MAPPING IF EXISTS FOR current_user SERVER pushdown_volume_server;
DROP SERVER IF EXISTS pushdown_volume_server;
DROP CATALOG IF EXISTS pushdown_catalog;
DROP USER MAPPING IF EXISTS FOR current_user SERVER pushdown_catalog_server;
DROP SERVER IF EXISTS pushdown_catalog_server;
