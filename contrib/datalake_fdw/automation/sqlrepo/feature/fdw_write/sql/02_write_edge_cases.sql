-- 02_write_edge_cases.sql
-- Test FDW write edge cases: NULLs, boundaries, bulk, empty strings

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Write Edge Cases');

-- ============================================================
-- Setup: FDW server
-- ============================================================
DROP SERVER IF EXISTS we_server CASCADE;
CREATE SERVER we_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER we_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: NULL values in Parquet write + readback
-- ============================================================
SELECT test_log('Test 1: NULL values in Parquet');

CREATE FOREIGN TABLE we_null_w (id int, name text, val decimal(10,2), flag boolean)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/nulls/', format 'parquet');
INSERT INTO we_null_w VALUES
    (1, NULL, NULL, NULL),
    (2, 'present', 99.99, true),
    (3, NULL, 50.00, false),
    (4, 'also_present', NULL, NULL);
DROP FOREIGN TABLE we_null_w;

CREATE FOREIGN TABLE we_null_r (id int, name text, val decimal(10,2), flag boolean)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/nulls/', format 'parquet');
SELECT * FROM we_null_r ORDER BY id;
SELECT COUNT(*) AS total, COUNT(name) AS with_name, COUNT(val) AS with_val FROM we_null_r;
DROP FOREIGN TABLE we_null_r;

-- ============================================================
-- Test 2: Boundary values (type min/max)
-- ============================================================
SELECT test_log('Test 2: Boundary values');

CREATE FOREIGN TABLE we_bounds_w (
    id int, val_small smallint, val_int int, val_big bigint
) SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bounds/', format 'parquet');
INSERT INTO we_bounds_w VALUES
    (1, -32768, -2147483648, -9223372036854775808),
    (2, 0, 0, 0),
    (3, 32767, 2147483647, 9223372036854775807);
DROP FOREIGN TABLE we_bounds_w;

CREATE FOREIGN TABLE we_bounds_r (
    id int, val_small smallint, val_int int, val_big bigint
) SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bounds/', format 'parquet');
SELECT * FROM we_bounds_r ORDER BY id;
DROP FOREIGN TABLE we_bounds_r;

-- ============================================================
-- Test 3: Bulk Parquet write (5000 rows)
-- ============================================================
SELECT test_log('Test 3: Bulk Parquet write (5000 rows)');

CREATE FOREIGN TABLE we_bulk_pq_w (id int, val int, label text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bulk_pq/', format 'parquet');
INSERT INTO we_bulk_pq_w SELECT g, g * 3, 'row_' || g FROM generate_series(1, 5000) g;
DROP FOREIGN TABLE we_bulk_pq_w;

CREATE FOREIGN TABLE we_bulk_pq_r (id int, val int, label text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bulk_pq/', format 'parquet');
SELECT COUNT(*) AS bulk_pq_count FROM we_bulk_pq_r;
SELECT MIN(id) AS min_id, MAX(id) AS max_id, SUM(val) AS total_val FROM we_bulk_pq_r;
DROP FOREIGN TABLE we_bulk_pq_r;

-- ============================================================
-- Test 4: Bulk ORC write (2000 rows)
-- ============================================================
SELECT test_log('Test 4: Bulk ORC write (2000 rows)');

CREATE FOREIGN TABLE we_bulk_orc_w (id int, val int, label text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bulk_orc/', format 'orc');
INSERT INTO we_bulk_orc_w SELECT g, g * 5, 'orc_' || g FROM generate_series(1, 2000) g;
DROP FOREIGN TABLE we_bulk_orc_w;

CREATE FOREIGN TABLE we_bulk_orc_r (id int, val int, label text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/bulk_orc/', format 'orc');
SELECT COUNT(*) AS bulk_orc_count FROM we_bulk_orc_r;
SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM we_bulk_orc_r;
DROP FOREIGN TABLE we_bulk_orc_r;

-- ============================================================
-- Test 5: Empty string vs NULL distinction
-- ============================================================
SELECT test_log('Test 5: Empty string vs NULL distinction');

CREATE FOREIGN TABLE we_empty_w (id int, val text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/emptystr/', format 'parquet');
INSERT INTO we_empty_w VALUES (1, ''), (2, NULL), (3, 'notempty');
DROP FOREIGN TABLE we_empty_w;

CREATE FOREIGN TABLE we_empty_r (id int, val text)
SERVER we_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write_edge/emptystr/', format 'parquet');
SELECT id, val, val IS NULL AS is_null, val = '' AS is_empty FROM we_empty_r ORDER BY id;
DROP FOREIGN TABLE we_empty_r;

-- ============================================================
-- Cleanup
-- ============================================================
DROP USER MAPPING FOR gpadmin SERVER we_server;
DROP SERVER we_server;
