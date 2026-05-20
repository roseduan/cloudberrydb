-- FDW Parquet Read/Write Test
-- Purpose: Exercise the FDW path (not AM path) for Parquet format
-- Target: datalake_fdw.c (BeginForeignScan/Modify), fdwFunction.c,
--         parquetWrite.cpp, parquetRead.cpp, protocol.c, headers.c

-- Setup FDW
DROP SERVER IF EXISTS fdw_pq_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_pq_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_pq_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Basic Parquet write + read round-trip
-- ============================================================
CREATE FOREIGN TABLE fdw_pq_w1 (id int, name text, amount decimal(10,2))
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/basic/', format 'parquet');

INSERT INTO fdw_pq_w1 SELECT i, 'item_' || i, i * 10.50 FROM generate_series(1, 10) i;
DROP FOREIGN TABLE fdw_pq_w1;

CREATE FOREIGN TABLE fdw_pq_r1 (id int, name text, amount decimal(10,2))
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/basic/', format 'parquet');

SELECT COUNT(*) FROM fdw_pq_r1;
SELECT * FROM fdw_pq_r1 ORDER BY id;
DROP FOREIGN TABLE fdw_pq_r1;

-- ============================================================
-- Test 2: Multi-column with diverse types
-- ============================================================
CREATE FOREIGN TABLE fdw_pq_w2 (
    col_int int, col_bigint bigint, col_text text,
    col_decimal decimal(15,2), col_bool boolean, col_date date
)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/types/', format 'parquet');

INSERT INTO fdw_pq_w2 VALUES
    (1, 9223372036854775807, 'hello', 99999.99, true, '2024-01-15'),
    (2, -9223372036854775808, 'world', -99999.99, false, '1970-01-01'),
    (3, 0, '', 0.00, true, '2024-12-31');
DROP FOREIGN TABLE fdw_pq_w2;

CREATE FOREIGN TABLE fdw_pq_r2 (
    col_int int, col_bigint bigint, col_text text,
    col_decimal decimal(15,2), col_bool boolean, col_date date
)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/types/', format 'parquet');

SELECT * FROM fdw_pq_r2 ORDER BY col_int;
DROP FOREIGN TABLE fdw_pq_r2;

-- ============================================================
-- Test 3: NULL values
-- ============================================================
CREATE FOREIGN TABLE fdw_pq_w3 (id int, name text, val int)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/nulls/', format 'parquet');

INSERT INTO fdw_pq_w3 VALUES (1, 'has_all', 100), (2, NULL, 200), (3, 'no_val', NULL), (4, NULL, NULL);
DROP FOREIGN TABLE fdw_pq_w3;

CREATE FOREIGN TABLE fdw_pq_r3 (id int, name text, val int)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/nulls/', format 'parquet');

SELECT * FROM fdw_pq_r3 ORDER BY id;
SELECT COUNT(*) AS total, COUNT(name) AS non_null_names, COUNT(val) AS non_null_vals FROM fdw_pq_r3;
DROP FOREIGN TABLE fdw_pq_r3;

-- ============================================================
-- Test 4: Bulk insert (1000 rows)
-- ============================================================
CREATE FOREIGN TABLE fdw_pq_w4 (id int, val text)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/bulk/', format 'parquet');

INSERT INTO fdw_pq_w4 SELECT i, 'row_' || i FROM generate_series(1, 1000) i;
DROP FOREIGN TABLE fdw_pq_w4;

CREATE FOREIGN TABLE fdw_pq_r4 (id int, val text)
SERVER fdw_pq_server
OPTIONS (filePath '/warehouse/fdw-test/parquet/bulk/', format 'parquet');

SELECT COUNT(*) FROM fdw_pq_r4;
SELECT MIN(id), MAX(id) FROM fdw_pq_r4;
DROP FOREIGN TABLE fdw_pq_r4;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_pq_server;
DROP SERVER fdw_pq_server;
