-- 02_advanced_pushdown.sql
-- Test FDW advanced pushdown: decimal, bigint, temporal, projection, EXPLAIN

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Advanced Pushdown');

-- ============================================================
-- Setup: FDW server and write multi-type data
-- ============================================================
DROP SERVER IF EXISTS ap_server CASCADE;
CREATE SERVER ap_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER ap_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

CREATE FOREIGN TABLE ap_data_w (
    id int, big_id bigint, amount decimal(10,2), val_r real,
    label text, flag boolean, d date, ts timestamp
) SERVER ap_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/adv/', format 'parquet');

INSERT INTO ap_data_w
SELECT g,
       g::bigint * 1000000,
       (g * 2.5)::decimal(10,2),
       (g * 0.1)::real,
       'adv_' || g,
       (g % 3 = 0),
       ('2024-01-01'::date + (g % 365)),
       ('2024-01-01 00:00:00'::timestamp + (g || ' hours')::interval)
FROM generate_series(1, 500) g;
DROP FOREIGN TABLE ap_data_w;

CREATE FOREIGN TABLE ap_data_r (
    id int, big_id bigint, amount decimal(10,2), val_r real,
    label text, flag boolean, d date, ts timestamp
) SERVER ap_server
OPTIONS (filePath '/warehouse/fdw-test/feature/pushdown/adv/', format 'parquet');

-- ============================================================
-- Test 1: Decimal filter
-- ============================================================
SELECT test_log('Test 1: Decimal filter');

SELECT COUNT(*) AS dec_gt FROM ap_data_r WHERE amount > 500.00;
SELECT COUNT(*) AS dec_range FROM ap_data_r WHERE amount BETWEEN 100.00 AND 200.00;

-- ============================================================
-- Test 2: Bigint filter
-- ============================================================
SELECT test_log('Test 2: Bigint filter');

SELECT COUNT(*) AS big_gt FROM ap_data_r WHERE big_id > 250000000;
SELECT id, big_id FROM ap_data_r WHERE big_id = 1000000;

-- ============================================================
-- Test 3: Timestamp/Date filter
-- ============================================================
SELECT test_log('Test 3: Timestamp/Date filter');

SELECT COUNT(*) AS date_filter FROM ap_data_r WHERE d > '2024-06-01';
SELECT COUNT(*) AS ts_filter FROM ap_data_r WHERE ts < '2024-01-15 00:00:00';

-- ============================================================
-- Test 4: Column projection
-- ============================================================
SELECT test_log('Test 4: Column projection');

SELECT id, label FROM ap_data_r WHERE id <= 5 ORDER BY id;
SELECT id, amount, flag FROM ap_data_r WHERE id <= 3 ORDER BY id;

-- ============================================================
-- Test 5: Aggregates + WHERE
-- ============================================================
SELECT test_log('Test 5: Aggregates + WHERE');

SELECT COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt
FROM ap_data_r WHERE flag = true;
SELECT MIN(big_id) AS min_big, MAX(big_id) AS max_big
FROM ap_data_r WHERE id BETWEEN 100 AND 200;

-- ============================================================
-- Test 6: LIMIT + WHERE
-- ============================================================
SELECT test_log('Test 6: LIMIT + WHERE');

SELECT id, amount FROM ap_data_r WHERE amount > 100.00 ORDER BY id LIMIT 5;
SELECT id, label FROM ap_data_r WHERE flag = true ORDER BY id DESC LIMIT 3;

-- ============================================================
-- Test 7: EXPLAIN to verify pushdown
-- ============================================================
SELECT test_log('Test 7: EXPLAIN pushdown verification');

EXPLAIN (COSTS OFF) SELECT * FROM ap_data_r WHERE id = 100;
EXPLAIN (COSTS OFF) SELECT * FROM ap_data_r WHERE flag = true AND amount > 500.00;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE ap_data_r;
DROP USER MAPPING FOR gpadmin SERVER ap_server;
DROP SERVER ap_server;
