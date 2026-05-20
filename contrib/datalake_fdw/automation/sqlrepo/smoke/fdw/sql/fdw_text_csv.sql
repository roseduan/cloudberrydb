-- FDW Text/CSV Read/Write Test
-- Purpose: Exercise archiveWrite/archiveRead/textFileRead (all 0% coverage)
-- Target: archiveWrite.cpp, archiveRead.cpp, textFileRead.cpp, lineRecordReader.cpp, datalakeBuffer.cpp

-- Setup FDW
DROP SERVER IF EXISTS fdw_txt_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_txt_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_txt_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Text write + read
-- ============================================================
CREATE FOREIGN TABLE fdw_txt_w1 (id int, name text, amount decimal(10,2))
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/basic/', format 'text');

INSERT INTO fdw_txt_w1 SELECT i, 'txt_' || i, i * 5.25 FROM generate_series(1, 10) i;
DROP FOREIGN TABLE fdw_txt_w1;

CREATE FOREIGN TABLE fdw_txt_r1 (id int, name text, amount decimal(10,2))
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/basic/', format 'text');

SELECT COUNT(*) FROM fdw_txt_r1;
SELECT * FROM fdw_txt_r1 ORDER BY id;
DROP FOREIGN TABLE fdw_txt_r1;

-- ============================================================
-- Test 2: CSV write + read
-- ============================================================
CREATE FOREIGN TABLE fdw_csv_w1 (id int, name text, amount decimal(10,2))
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/csv/basic/', format 'csv');

INSERT INTO fdw_csv_w1 SELECT i, 'csv_' || i, i * 3.14 FROM generate_series(1, 10) i;
DROP FOREIGN TABLE fdw_csv_w1;

CREATE FOREIGN TABLE fdw_csv_r1 (id int, name text, amount decimal(10,2))
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/csv/basic/', format 'csv');

SELECT COUNT(*) FROM fdw_csv_r1;
SELECT * FROM fdw_csv_r1 ORDER BY id;
DROP FOREIGN TABLE fdw_csv_r1;

-- ============================================================
-- Test 3: Text with NULL values
-- ============================================================
CREATE FOREIGN TABLE fdw_txt_w3 (id int, name text, val int)
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/nulls/', format 'text');

INSERT INTO fdw_txt_w3 VALUES (1, 'a', 10), (2, NULL, 20), (3, 'c', NULL);
DROP FOREIGN TABLE fdw_txt_w3;

CREATE FOREIGN TABLE fdw_txt_r3 (id int, name text, val int)
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/nulls/', format 'text');

SELECT * FROM fdw_txt_r3 ORDER BY id;
DROP FOREIGN TABLE fdw_txt_r3;

-- ============================================================
-- Test 4: Bulk text write (500 rows)
-- ============================================================
CREATE FOREIGN TABLE fdw_txt_w4 (id int, val text)
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/bulk/', format 'text');

INSERT INTO fdw_txt_w4 SELECT i, 'bulk_row_' || i FROM generate_series(1, 500) i;
DROP FOREIGN TABLE fdw_txt_w4;

CREATE FOREIGN TABLE fdw_txt_r4 (id int, val text)
SERVER fdw_txt_server
OPTIONS (filePath '/warehouse/fdw-test/text/bulk/', format 'text');

SELECT COUNT(*) FROM fdw_txt_r4;
DROP FOREIGN TABLE fdw_txt_r4;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_txt_server;
DROP SERVER fdw_txt_server;
