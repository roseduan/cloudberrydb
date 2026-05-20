-- FDW Avro Read/Write Test
-- Purpose: Exercise avroWriter.cpp/avroWrite.cpp/avroRead.cpp (all 0% coverage)
-- Target: avroWriter.cpp (169 lines), avroWrite.cpp, avroRead.cpp, avroBlockReader.cpp

-- Setup FDW
DROP SERVER IF EXISTS fdw_avro_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_avro_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_avro_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Basic Avro write + read
-- ============================================================
CREATE FOREIGN TABLE fdw_avro_w1 (id int, name text)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/basic/', format 'avro');

INSERT INTO fdw_avro_w1 SELECT i, 'avro_' || i FROM generate_series(1, 10) i;
DROP FOREIGN TABLE fdw_avro_w1;

CREATE FOREIGN TABLE fdw_avro_r1 (id int, name text)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/basic/', format 'avro');

SELECT COUNT(*) FROM fdw_avro_r1;
SELECT * FROM fdw_avro_r1 ORDER BY id;
DROP FOREIGN TABLE fdw_avro_r1;

-- ============================================================
-- Test 2: Multiple types
-- ============================================================
CREATE FOREIGN TABLE fdw_avro_w2 (col_int int, col_bigint bigint, col_text text, col_bool boolean)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/types/', format 'avro');

INSERT INTO fdw_avro_w2 VALUES
    (1, 1000000, 'hello', true),
    (2, -1000000, 'world', false),
    (3, 0, '', true);
DROP FOREIGN TABLE fdw_avro_w2;

CREATE FOREIGN TABLE fdw_avro_r2 (col_int int, col_bigint bigint, col_text text, col_bool boolean)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/types/', format 'avro');

SELECT * FROM fdw_avro_r2 ORDER BY col_int;
DROP FOREIGN TABLE fdw_avro_r2;

-- ============================================================
-- Test 3: NULL handling
-- ============================================================
CREATE FOREIGN TABLE fdw_avro_w3 (id int, name text, val int)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/nulls/', format 'avro');

INSERT INTO fdw_avro_w3 VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', NULL), (4, NULL, NULL);
DROP FOREIGN TABLE fdw_avro_w3;

CREATE FOREIGN TABLE fdw_avro_r3 (id int, name text, val int)
SERVER fdw_avro_server
OPTIONS (filePath '/warehouse/fdw-test/avro/nulls/', format 'avro');

SELECT * FROM fdw_avro_r3 ORDER BY id;
DROP FOREIGN TABLE fdw_avro_r3;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_avro_server;
DROP SERVER fdw_avro_server;
