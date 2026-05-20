-- FDW Filter Pushdown Test
-- Purpose: Exercise filter serialization paths in dlproxy/filters.c
-- Target: filters.c (+384), headers.c (+77), protocol.c (+214),
--         libchurl.c (read paths), datalake_option.c, datalake_fdw.c

-- Setup FDW
DROP SERVER IF EXISTS fdw_fp_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_fp_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_fp_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Create test data with diverse types for filter testing
-- ============================================================
CREATE FOREIGN TABLE fdw_fp_data_w (
    id int,
    name text,
    amount decimal(10,2),
    flag boolean,
    category int,
    ts timestamp,
    col_date date,
    col_bigint bigint,
    col_real real
)
SERVER fdw_fp_server
OPTIONS (filePath '/warehouse/fdw-test/filter-pushdown/', format 'parquet');

INSERT INTO fdw_fp_data_w
SELECT
    i,
    'item_' || i,
    (i * 5.5)::decimal(10,2),
    (i % 2 = 0),
    i % 10,
    '2024-01-01'::timestamp + (i || ' hours')::interval,
    '2024-01-01'::date + (i % 365),
    i::bigint * 1000,
    (i * 0.1)::real
FROM generate_series(1, 1000) i;
DROP FOREIGN TABLE fdw_fp_data_w;

CREATE FOREIGN TABLE fdw_fp_data (
    id int,
    name text,
    amount decimal(10,2),
    flag boolean,
    category int,
    ts timestamp,
    col_date date,
    col_bigint bigint,
    col_real real
)
SERVER fdw_fp_server
OPTIONS (filePath '/warehouse/fdw-test/filter-pushdown/', format 'parquet');

-- ============================================================
-- Test 1: Equality operator (=)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE id = 500;
SELECT COUNT(*) FROM fdw_fp_data WHERE name = 'item_100';
SELECT COUNT(*) FROM fdw_fp_data WHERE flag = true;
SELECT COUNT(*) FROM fdw_fp_data WHERE category = 5;

-- ============================================================
-- Test 2: Inequality operators (!=, <>, >, <, >=, <=)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE id != 500;
SELECT COUNT(*) FROM fdw_fp_data WHERE id > 990;
SELECT COUNT(*) FROM fdw_fp_data WHERE id < 10;
SELECT COUNT(*) FROM fdw_fp_data WHERE id >= 995;
SELECT COUNT(*) FROM fdw_fp_data WHERE id <= 5;

-- ============================================================
-- Test 3: NULL checks (IS NULL, IS NOT NULL)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE name IS NULL;
SELECT COUNT(*) FROM fdw_fp_data WHERE name IS NOT NULL;

-- ============================================================
-- Test 4: IN operator (ScalarArrayOpExpr)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE id IN (1, 5, 10, 50, 100, 500, 1000);
SELECT COUNT(*) FROM fdw_fp_data WHERE category IN (1, 3, 5, 7, 9);
SELECT COUNT(*) FROM fdw_fp_data WHERE name IN ('item_1', 'item_2', 'item_3');

-- ============================================================
-- Test 5: BETWEEN (translates to >= AND <=)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE id BETWEEN 100 AND 200;
SELECT COUNT(*) FROM fdw_fp_data WHERE amount BETWEEN 50.00 AND 100.00;

-- ============================================================
-- Test 6: Compound filters (AND, OR)
-- ============================================================
SELECT COUNT(*) FROM fdw_fp_data WHERE id > 500 AND flag = true;
SELECT COUNT(*) FROM fdw_fp_data WHERE category = 1 AND id < 100;
SELECT COUNT(*) FROM fdw_fp_data WHERE id < 10 OR id > 990;

-- ============================================================
-- Test 7: Type-specific filters
-- ============================================================
-- Decimal filter
SELECT COUNT(*) FROM fdw_fp_data WHERE amount > 5000.00;
SELECT COUNT(*) FROM fdw_fp_data WHERE amount = 55.00;

-- Bigint filter
SELECT COUNT(*) FROM fdw_fp_data WHERE col_bigint > 500000;
SELECT COUNT(*) FROM fdw_fp_data WHERE col_bigint = 100000;

-- Real/float filter
SELECT COUNT(*) FROM fdw_fp_data WHERE col_real > 50.0;

-- Timestamp filter
SELECT COUNT(*) FROM fdw_fp_data WHERE ts > '2024-02-01'::timestamp;
SELECT COUNT(*) FROM fdw_fp_data WHERE ts < '2024-01-02'::timestamp;

-- Date filter
SELECT COUNT(*) FROM fdw_fp_data WHERE col_date > '2024-06-01'::date;
SELECT COUNT(*) FROM fdw_fp_data WHERE col_date = '2024-03-15'::date;

-- Boolean filter
SELECT COUNT(*) FROM fdw_fp_data WHERE flag = false;

-- ============================================================
-- Test 8: Column projection (only select subset of columns)
-- ============================================================
SELECT id FROM fdw_fp_data WHERE id <= 5 ORDER BY id;
SELECT name, amount FROM fdw_fp_data WHERE id <= 3 ORDER BY name;
SELECT flag, category FROM fdw_fp_data WHERE id = 1;

-- ============================================================
-- Test 9: Aggregates with filters
-- ============================================================
SELECT COUNT(*), SUM(amount), AVG(amount) FROM fdw_fp_data WHERE category = 0;
SELECT MIN(id), MAX(id) FROM fdw_fp_data WHERE flag = true;
SELECT category, COUNT(*) FROM fdw_fp_data GROUP BY category ORDER BY category;

-- ============================================================
-- Test 10: LIMIT with filters
-- ============================================================
SELECT * FROM fdw_fp_data WHERE category = 1 ORDER BY id LIMIT 5;

-- Cleanup
DROP FOREIGN TABLE fdw_fp_data;
DROP USER MAPPING FOR gpadmin SERVER fdw_fp_server;
DROP SERVER fdw_fp_server;
