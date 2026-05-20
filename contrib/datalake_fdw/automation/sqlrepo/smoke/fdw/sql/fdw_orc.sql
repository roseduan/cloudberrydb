-- FDW ORC Read/Write Test
-- Purpose: Exercise orcWriter.cpp (0% coverage) and ORC read via FDW path
-- Target: orcWriter.cpp (486 lines), orcRead.cpp, orcFileReader.cpp, orcInputStream.cpp

-- Setup FDW
DROP SERVER IF EXISTS fdw_orc_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_orc_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_orc_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Basic ORC write + read
-- ============================================================
CREATE FOREIGN TABLE fdw_orc_w1 (id int, name text, amount decimal(10,2))
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/basic/', format 'orc');

INSERT INTO fdw_orc_w1 SELECT i, 'orc_' || i, i * 5.25 FROM generate_series(1, 10) i;
DROP FOREIGN TABLE fdw_orc_w1;

CREATE FOREIGN TABLE fdw_orc_r1 (id int, name text, amount decimal(10,2))
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/basic/', format 'orc');

SELECT COUNT(*) FROM fdw_orc_r1;
SELECT * FROM fdw_orc_r1 ORDER BY id;
DROP FOREIGN TABLE fdw_orc_r1;

-- ============================================================
-- Test 2: Multi-type columns
-- ============================================================
CREATE FOREIGN TABLE fdw_orc_w2 (
    col_int int, col_bigint bigint, col_text text,
    col_bool boolean, col_decimal decimal(15,4)
)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/types/', format 'orc');

INSERT INTO fdw_orc_w2 VALUES
    (1, 100000000000, 'hello', true, 12345.6789),
    (2, -100000000000, 'world', false, -12345.6789),
    (3, 0, '', true, 0.0000);
DROP FOREIGN TABLE fdw_orc_w2;

CREATE FOREIGN TABLE fdw_orc_r2 (
    col_int int, col_bigint bigint, col_text text,
    col_bool boolean, col_decimal decimal(15,4)
)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/types/', format 'orc');

SELECT * FROM fdw_orc_r2 ORDER BY col_int;
DROP FOREIGN TABLE fdw_orc_r2;

-- ============================================================
-- Test 3: NULL handling
-- ============================================================
CREATE FOREIGN TABLE fdw_orc_w3 (id int, name text, val int)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/nulls/', format 'orc');

INSERT INTO fdw_orc_w3 VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', NULL), (4, NULL, NULL);
DROP FOREIGN TABLE fdw_orc_w3;

CREATE FOREIGN TABLE fdw_orc_r3 (id int, name text, val int)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/nulls/', format 'orc');

SELECT * FROM fdw_orc_r3 ORDER BY id;
SELECT COUNT(*) AS total, COUNT(name) AS names, COUNT(val) AS vals FROM fdw_orc_r3;
DROP FOREIGN TABLE fdw_orc_r3;

-- ============================================================
-- Test 4: Bulk insert (1000 rows)
-- ============================================================
CREATE FOREIGN TABLE fdw_orc_w4 (id int, val text)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/bulk/', format 'orc');

INSERT INTO fdw_orc_w4 SELECT i, 'row_' || i FROM generate_series(1, 1000) i;
DROP FOREIGN TABLE fdw_orc_w4;

CREATE FOREIGN TABLE fdw_orc_r4 (id int, val text)
SERVER fdw_orc_server
OPTIONS (filePath '/warehouse/fdw-test/orc/bulk/', format 'orc');

SELECT COUNT(*) FROM fdw_orc_r4;
DROP FOREIGN TABLE fdw_orc_r4;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_orc_server;
DROP SERVER fdw_orc_server;
