-- 01_filter_pushdown.sql
-- Test FDW filter pushdown operations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Filter Pushdown');

-- ============================================================
-- Setup: FDW server and write test data
-- ============================================================
DROP SERVER IF EXISTS fp_server CASCADE;
CREATE SERVER fp_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER fp_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Write 1000 rows of test data
CREATE FOREIGN TABLE fp_data_w (
    id int, name text, amount decimal(10,2), flag boolean, category int
) SERVER fp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/basic/', format 'parquet');

INSERT INTO fp_data_w
SELECT g,
       'item_' || g,
       (g * 1.5)::decimal(10,2),
       (g % 2 = 0),
       g % 10
FROM generate_series(1, 1000) g;
DROP FOREIGN TABLE fp_data_w;

CREATE FOREIGN TABLE fp_data_r (
    id int, name text, amount decimal(10,2), flag boolean, category int
) SERVER fp_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/basic/', format 'parquet');

-- ============================================================
-- Test 1: Equality on int, text, boolean
-- ============================================================
SELECT test_log('Test 1: Equality pushdown');

SELECT id, name FROM fp_data_r WHERE id = 500;
SELECT id, name FROM fp_data_r WHERE name = 'item_1';
SELECT COUNT(*) AS true_count FROM fp_data_r WHERE flag = true;

-- ============================================================
-- Test 2: Inequality operators
-- ============================================================
SELECT test_log('Test 2: Inequality pushdown');

SELECT COUNT(*) AS ne_count FROM fp_data_r WHERE id != 500;
SELECT COUNT(*) AS gt_count FROM fp_data_r WHERE id > 990;
SELECT COUNT(*) AS lt_count FROM fp_data_r WHERE id < 11;
SELECT COUNT(*) AS gte_count FROM fp_data_r WHERE id >= 999;
SELECT COUNT(*) AS lte_count FROM fp_data_r WHERE id <= 2;

-- ============================================================
-- Test 3: IS NULL / IS NOT NULL
-- ============================================================
SELECT test_log('Test 3: IS NULL / IS NOT NULL');

SELECT COUNT(*) AS not_null_names FROM fp_data_r WHERE name IS NOT NULL;
SELECT COUNT(*) AS null_names FROM fp_data_r WHERE name IS NULL;

-- ============================================================
-- Test 4: IN operator
-- ============================================================
SELECT test_log('Test 4: IN operator');

SELECT id, name FROM fp_data_r WHERE id IN (1, 50, 100, 500, 1000) ORDER BY id;
SELECT COUNT(*) AS in_cats FROM fp_data_r WHERE category IN (0, 5);

-- ============================================================
-- Test 5: BETWEEN
-- ============================================================
SELECT test_log('Test 5: BETWEEN');

SELECT COUNT(*) AS between_count FROM fp_data_r WHERE id BETWEEN 100 AND 200;
SELECT COUNT(*) AS between_amt FROM fp_data_r WHERE amount BETWEEN 100.00 AND 200.00;

-- ============================================================
-- Test 6: AND / OR compounds
-- ============================================================
SELECT test_log('Test 6: AND / OR compound filters');

SELECT COUNT(*) AS and_count FROM fp_data_r WHERE flag = true AND category = 0;
SELECT COUNT(*) AS or_count FROM fp_data_r WHERE id < 10 OR id > 990;
SELECT COUNT(*) AS complex FROM fp_data_r
WHERE (flag = true AND amount > 500.00) OR (category = 0 AND id < 100);

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE fp_data_r;
DROP USER MAPPING FOR gpadmin SERVER fp_server;
DROP SERVER fp_server;
