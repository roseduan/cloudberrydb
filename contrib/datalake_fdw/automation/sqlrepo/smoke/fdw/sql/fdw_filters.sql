-- FDW Filter Pushdown Test
-- Purpose: Exercise filters.c (486 lines, 0%) and deparse.c (0%) via FDW path
-- Target: filters.c (all operator types), deparse.c, datalake_option.c (filter pushdown)

-- Setup FDW
DROP SERVER IF EXISTS fdw_flt_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_flt_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_flt_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Write test data
CREATE FOREIGN TABLE fdw_flt_w (
    id int, name text, category text,
    price decimal(10,2), quantity int,
    is_active boolean, created_date date
)
SERVER fdw_flt_server
OPTIONS (filePath '/warehouse/fdw-test/filters/', format 'parquet');

INSERT INTO fdw_flt_w VALUES
    (1, 'Alpha', 'electronics', 299.99, 10, true, '2024-01-15'),
    (2, 'Beta', 'office', 19.99, 100, true, '2024-02-20'),
    (3, 'Gamma', 'electronics', 599.99, 5, false, '2024-03-10'),
    (4, 'Delta', 'office', 9.99, 500, true, '2024-04-01'),
    (5, 'Epsilon', 'hardware', 149.99, 25, false, '2024-05-15'),
    (6, 'Zeta', 'hardware', 49.99, 200, true, '2024-06-01'),
    (7, 'Eta', 'electronics', 899.99, 3, true, '2024-07-20'),
    (8, 'Theta', 'office', 29.99, 150, false, '2024-08-10');
DROP FOREIGN TABLE fdw_flt_w;

-- Create readable table
CREATE FOREIGN TABLE fdw_flt_r (
    id int, name text, category text,
    price decimal(10,2), quantity int,
    is_active boolean, created_date date
)
SERVER fdw_flt_server
OPTIONS (filePath '/warehouse/fdw-test/filters/', format 'parquet');

-- Verify baseline
SELECT COUNT(*) FROM fdw_flt_r;

-- ============================================================
-- Test 1: Equality (=)
-- ============================================================
SELECT id, name FROM fdw_flt_r WHERE id = 3;
SELECT id, name FROM fdw_flt_r WHERE category = 'office' ORDER BY id;

-- ============================================================
-- Test 2: Comparison operators (>, <, >=, <=)
-- ============================================================
SELECT id, price FROM fdw_flt_r WHERE price > 100.00 ORDER BY id;
SELECT id, price FROM fdw_flt_r WHERE price < 50.00 ORDER BY id;
SELECT id, quantity FROM fdw_flt_r WHERE quantity >= 100 ORDER BY id;
SELECT id, quantity FROM fdw_flt_r WHERE quantity <= 10 ORDER BY id;

-- ============================================================
-- Test 3: Not-equal (!=)
-- ============================================================
SELECT id, category FROM fdw_flt_r WHERE category != 'office' ORDER BY id;

-- ============================================================
-- Test 4: IS NULL / IS NOT NULL
-- ============================================================
SELECT id FROM fdw_flt_r WHERE name IS NOT NULL ORDER BY id;

-- ============================================================
-- Test 5: Boolean filter
-- ============================================================
SELECT id, name FROM fdw_flt_r WHERE is_active = true ORDER BY id;
SELECT id, name FROM fdw_flt_r WHERE is_active = false ORDER BY id;

-- ============================================================
-- Test 6: Date filter
-- ============================================================
SELECT id, created_date FROM fdw_flt_r WHERE created_date > '2024-04-01' ORDER BY id;

-- ============================================================
-- Test 7: Combined filters (AND)
-- ============================================================
SELECT id, name, price FROM fdw_flt_r
WHERE category = 'electronics' AND price > 500.00
ORDER BY id;

-- ============================================================
-- Test 8: Combined filters (OR)
-- ============================================================
SELECT id, name FROM fdw_flt_r
WHERE category = 'electronics' OR category = 'hardware'
ORDER BY id;

-- ============================================================
-- Test 9: BETWEEN (desugars to >= AND <=)
-- ============================================================
SELECT id, price FROM fdw_flt_r WHERE price BETWEEN 20.00 AND 200.00 ORDER BY id;

-- ============================================================
-- Test 10: IN list
-- ============================================================
SELECT id, category FROM fdw_flt_r WHERE id IN (1, 3, 5, 7) ORDER BY id;

-- ============================================================
-- Test 11: Column projection (SELECT subset)
-- ============================================================
SELECT id FROM fdw_flt_r ORDER BY id;
SELECT id, name FROM fdw_flt_r ORDER BY id;
SELECT id, price, quantity FROM fdw_flt_r WHERE id <= 3 ORDER BY id;

-- ============================================================
-- Test 12: EXPLAIN shows Foreign Scan
-- ============================================================
EXPLAIN (COSTS OFF) SELECT * FROM fdw_flt_r WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT id, name FROM fdw_flt_r WHERE price > 100;

-- ============================================================
-- Test 13: Disable filter pushdown GUC
-- ============================================================
SET datalake.disable_filter_pushdown = on;
SELECT id, name FROM fdw_flt_r WHERE category = 'electronics' ORDER BY id;
SET datalake.disable_filter_pushdown = off;

-- Cleanup
DROP FOREIGN TABLE fdw_flt_r;
DROP USER MAPPING FOR gpadmin SERVER fdw_flt_server;
DROP SERVER fdw_flt_server;
