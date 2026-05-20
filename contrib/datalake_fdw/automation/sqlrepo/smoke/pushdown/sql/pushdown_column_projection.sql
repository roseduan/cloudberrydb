-- Column Projection and Extended Pushdown Test
-- Purpose: Exercise deparse.c target list generation and cross-type filter paths
-- Target: deparse.c (datalakeDeparseTargetList), filters.c (cross-type operators)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- Setup: Iceberg with builtin catalog + S3 volume
CREATE SERVER proj_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER proj_catalog_server;
CREATE FOREIGN CATALOG proj_catalog SERVER proj_catalog_server;
SET iceberg_default_catalog = 'proj_catalog';

CREATE SERVER proj_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER proj_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME proj_volume SERVER proj_volume_server OPTIONS(base_path '/proj_volume/');
SET iceberg_default_volume = 'proj_volume';

-- Create test table with diverse column types
CREATE ICEBERG TABLE proj_test (
    id bigint,
    small_val smallint,
    int_val int,
    name text,
    price decimal(10,2),
    quantity int,
    is_active boolean,
    created_date date
);

INSERT INTO proj_test VALUES
    (1, 10, 100, 'Alpha', 299.99, 10, true, '2024-01-15'),
    (2, 20, 200, 'Beta', 19.99, 100, true, '2024-02-20'),
    (3, 30, 300, 'Gamma', 599.99, 5, false, '2024-03-10'),
    (4, 40, 400, 'Delta', 9.99, 500, true, '2024-04-01'),
    (5, 50, 500, 'Epsilon', 149.99, 25, false, '2024-05-15'),
    (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- ============================================================
-- Test 1: Single column projection
-- ============================================================
SELECT id FROM proj_test ORDER BY id;
SELECT name FROM proj_test WHERE name IS NOT NULL ORDER BY name;

-- ============================================================
-- Test 2: Two-column subset
-- ============================================================
SELECT id, price FROM proj_test WHERE price IS NOT NULL ORDER BY id;

-- ============================================================
-- Test 3: Full row (SELECT *)
-- ============================================================
SELECT * FROM proj_test ORDER BY id;

-- ============================================================
-- Test 4: Column subset with filter on non-selected column
-- ============================================================
SELECT id, name FROM proj_test WHERE quantity > 50 ORDER BY id;
SELECT price, quantity FROM proj_test WHERE is_active = true ORDER BY price;

-- ============================================================
-- Test 5: EXPLAIN shows column projection
-- ============================================================
EXPLAIN (COSTS OFF) SELECT id FROM proj_test WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT id, name FROM proj_test WHERE price > 100;
EXPLAIN (COSTS OFF) SELECT * FROM proj_test;

-- ============================================================
-- Test 6: Cross-type integer comparisons
-- ============================================================
-- smallint compared with int literal
SELECT id, small_val FROM proj_test WHERE small_val > 25 ORDER BY id;

-- int compared with bigint column
SELECT id, int_val FROM proj_test WHERE int_val = id * 100 ORDER BY id;

-- ============================================================
-- Test 7: ScalarArrayOpExpr (IN lists)
-- ============================================================
SELECT id, name FROM proj_test WHERE id IN (1, 3, 5) ORDER BY id;
SELECT id, name FROM proj_test WHERE name IN ('Alpha', 'Gamma', 'Epsilon') ORDER BY id;

-- ============================================================
-- Test 8: Complex boolean filter combinations
-- ============================================================
-- (A AND B) OR (C AND D)
SELECT id, name FROM proj_test
WHERE (is_active = true AND price < 100) OR (is_active = false AND quantity < 10)
ORDER BY id;

-- NOT
SELECT id, name FROM proj_test
WHERE NOT (is_active = true)
ORDER BY id;

-- Nested
SELECT id FROM proj_test
WHERE (id > 2 AND id < 5) OR (id = 1)
ORDER BY id;

-- ============================================================
-- Test 9: BETWEEN (desugars to >= AND <=)
-- ============================================================
SELECT id, price FROM proj_test WHERE price BETWEEN 10.00 AND 200.00 ORDER BY id;

-- ============================================================
-- Test 10: COUNT with filter (no data columns projected)
-- ============================================================
SELECT COUNT(*) FROM proj_test WHERE is_active = true;
SELECT COUNT(*) FROM proj_test WHERE price > 100;

-- ============================================================
-- Test 11: DISTINCT on projected column
-- ============================================================
SELECT DISTINCT is_active FROM proj_test WHERE is_active IS NOT NULL ORDER BY is_active;

-- ============================================================
-- Test 12: ORDER BY on non-selected column
-- ============================================================
SELECT name FROM proj_test WHERE name IS NOT NULL ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE proj_test;
DROP VOLUME proj_volume;
DROP USER MAPPING FOR current_user SERVER proj_volume_server;
DROP SERVER proj_volume_server;
DROP CATALOG proj_catalog;
DROP USER MAPPING FOR current_user SERVER proj_catalog_server;
DROP SERVER proj_catalog_server;
