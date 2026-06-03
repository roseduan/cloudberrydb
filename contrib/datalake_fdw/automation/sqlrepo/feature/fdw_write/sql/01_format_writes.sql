-- 01_format_writes.sql
-- Test FDW write operations across formats with multi-type readback

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: FDW Format Writes');

-- ============================================================
-- Setup: FDW server
-- ============================================================
DROP SERVER IF EXISTS fw_server CASCADE;
CREATE SERVER fw_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin SERVER fw_server
OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Parquet multi-type write + readback
-- ============================================================
SELECT test_log('Test 1: Parquet multi-type write + readback');

CREATE FOREIGN TABLE fw_parquet_w (
    id int, big_id bigint, label text, amount decimal(10,2),
    flag boolean, d date, ts timestamp
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/parquet/', format 'parquet');
INSERT INTO fw_parquet_w VALUES
    (1, 1000000, 'parquet_row1', 99.99, true, '2024-01-15', '2024-01-15 10:30:00'),
    (2, 2000000, 'parquet_row2', 199.50, false, '2024-06-30', '2024-06-30 14:00:00'),
    (3, 3000000, 'parquet_row3', 0.01, true, '2024-12-31', '2024-12-31 23:59:59');
DROP FOREIGN TABLE fw_parquet_w;

CREATE FOREIGN TABLE fw_parquet_r (
    id int, big_id bigint, label text, amount decimal(10,2),
    flag boolean, d date, ts timestamp
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/parquet/', format 'parquet');
SELECT * FROM fw_parquet_r ORDER BY id;
SELECT COUNT(*) AS parquet_rows FROM fw_parquet_r;
DROP FOREIGN TABLE fw_parquet_r;

-- ============================================================
-- Test 2: ORC multi-type write + readback
-- ============================================================
SELECT test_log('Test 2: ORC multi-type write + readback');

CREATE FOREIGN TABLE fw_orc_w (
    id int, big_id bigint, label text, amount decimal(10,2),
    flag boolean, d date, ts timestamp
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/orc/', format 'orc');
INSERT INTO fw_orc_w VALUES
    (1, 100, 'orc_row1', 11.11, true, '2024-02-01', '2024-02-01 08:00:00'),
    (2, 200, 'orc_row2', 22.22, false, '2024-07-15', '2024-07-15 16:30:00'),
    (3, 300, 'orc_row3', 33.33, true, '2024-11-20', '2024-11-20 12:00:00');
DROP FOREIGN TABLE fw_orc_w;

CREATE FOREIGN TABLE fw_orc_r (
    id int, big_id bigint, label text, amount decimal(10,2),
    flag boolean, d date, ts timestamp
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/orc/', format 'orc');
SELECT * FROM fw_orc_r ORDER BY id;
SELECT COUNT(*) AS orc_rows FROM fw_orc_r;
DROP FOREIGN TABLE fw_orc_r;

-- ============================================================
-- Test 3: Avro write + readback
-- ============================================================
SELECT test_log('Test 3: Avro write + readback');

CREATE FOREIGN TABLE fw_avro_w (
    id int, big_id bigint, val_r real, val_d double precision,
    label text, flag boolean
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/avro/', format 'avro');
INSERT INTO fw_avro_w VALUES
    (1, 100, 3.14, 2.718281828, 'avro_1', true),
    (2, 200, -1.5, 99.999, 'avro_2', false),
    (3, 300, 0.0, 0.0, 'avro_3', true);
DROP FOREIGN TABLE fw_avro_w;

CREATE FOREIGN TABLE fw_avro_r (
    id int, big_id bigint, val_r real, val_d double precision,
    label text, flag boolean
) SERVER fw_server
OPTIONS (filePath '/warehouse/fdw-test/feature/write/avro/', format 'avro');
SELECT * FROM fw_avro_r ORDER BY id;
SELECT COUNT(*) AS avro_rows FROM fw_avro_r;
DROP FOREIGN TABLE fw_avro_r;

-- ============================================================
-- Test 4: COUNT consistency across formats
-- ============================================================
SELECT test_log('Test 4: COUNT consistency across formats');

-- Re-read all three formats to verify counts
CREATE FOREIGN TABLE fw_cnt_p (id int, big_id bigint, label text, amount decimal(10,2), flag boolean, d date, ts timestamp)
SERVER fw_server OPTIONS (filePath '/warehouse/fdw-test/feature/write/parquet/', format 'parquet');
CREATE FOREIGN TABLE fw_cnt_o (id int, big_id bigint, label text, amount decimal(10,2), flag boolean, d date, ts timestamp)
SERVER fw_server OPTIONS (filePath '/warehouse/fdw-test/feature/write/orc/', format 'orc');
CREATE FOREIGN TABLE fw_cnt_a (id int, big_id bigint, val_r real, val_d double precision, label text, flag boolean)
SERVER fw_server OPTIONS (filePath '/warehouse/fdw-test/feature/write/avro/', format 'avro');

SELECT 'parquet' AS fmt, COUNT(*) FROM fw_cnt_p
UNION ALL
SELECT 'orc', COUNT(*) FROM fw_cnt_o
UNION ALL
SELECT 'avro', COUNT(*) FROM fw_cnt_a
ORDER BY fmt;

DROP FOREIGN TABLE fw_cnt_p;
DROP FOREIGN TABLE fw_cnt_o;
DROP FOREIGN TABLE fw_cnt_a;

-- ============================================================
-- Cleanup
-- ============================================================
DROP USER MAPPING FOR gpadmin SERVER fw_server;
DROP SERVER fw_server;
