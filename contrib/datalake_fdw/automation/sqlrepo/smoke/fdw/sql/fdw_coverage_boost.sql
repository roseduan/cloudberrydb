-- FDW Coverage Boost Test
-- Purpose: Push ORC/Avro/Parquet reader/writer files over 80%
-- Target: orcWriter.cpp (+40), orcFileReader.cpp (+38), parquetFileWriter.cpp (+35),
--         parquetFileReader.cpp (+56), avroRead.cpp (+43),
--         fdwFunction.c (+34), readPolicy.cpp (+30), datalake_option.c (+95)

-- Setup FDW
DROP SERVER IF EXISTS fdw_boost_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_boost_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_boost_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: ORC write large strings (fillStringValues buffer resize)
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_orc_bigstr_w (id int, big_text text, big_bytea bytea)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/orc-bigstr/', format 'orc');

INSERT INTO fdw_boost_orc_bigstr_w VALUES (1, repeat('a', 1000), decode(repeat('FF', 500), 'hex'));
INSERT INTO fdw_boost_orc_bigstr_w VALUES (2, repeat('b', 5000), decode(repeat('AA', 100), 'hex'));
INSERT INTO fdw_boost_orc_bigstr_w VALUES (3, '', '\x');
DROP FOREIGN TABLE fdw_boost_orc_bigstr_w;

CREATE FOREIGN TABLE fdw_boost_orc_bigstr_r (id int, big_text text, big_bytea bytea)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/orc-bigstr/', format 'orc');

SELECT id, length(big_text), length(big_bytea) FROM fdw_boost_orc_bigstr_r ORDER BY id;
DROP FOREIGN TABLE fdw_boost_orc_bigstr_r;

-- ============================================================
-- Test 3: ORC decimal precision edge cases
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_orc_dec_w (
    col_dec3 decimal(3,0),
    col_dec9 decimal(9,3),
    col_dec18 decimal(18,6),
    col_dec28 decimal(28,10),
    col_dec38 decimal(38,18)
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/orc-dec/', format 'orc');

INSERT INTO fdw_boost_orc_dec_w VALUES (999, 999999.999, 999999999999.999999, 999999999999999999.9999999999, 99999999999999999999.999999999999999999);
INSERT INTO fdw_boost_orc_dec_w VALUES (0, 0.000, 0.000000, 0.0000000000, 0.000000000000000000);
INSERT INTO fdw_boost_orc_dec_w VALUES (-1, -1.001, -1.000001, -1.0000000001, -1.000000000000000001);
INSERT INTO fdw_boost_orc_dec_w VALUES (NULL, NULL, NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_boost_orc_dec_w;

CREATE FOREIGN TABLE fdw_boost_orc_dec_r (
    col_dec3 decimal(3,0),
    col_dec9 decimal(9,3),
    col_dec18 decimal(18,6),
    col_dec28 decimal(28,10),
    col_dec38 decimal(38,18)
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/orc-dec/', format 'orc');

SELECT * FROM fdw_boost_orc_dec_r ORDER BY col_dec3 NULLS LAST;
DROP FOREIGN TABLE fdw_boost_orc_dec_r;

-- ============================================================
-- Test 4: Parquet with all compression types (writeProperties branches)
-- ============================================================

-- gzip compression
CREATE FOREIGN TABLE fdw_boost_pq_gz_w (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-gz/', format 'parquet', compresstype 'gzip');
INSERT INTO fdw_boost_pq_gz_w SELECT i, 'gz_' || i FROM generate_series(1, 50) i;
DROP FOREIGN TABLE fdw_boost_pq_gz_w;

-- zstd compression
CREATE FOREIGN TABLE fdw_boost_pq_zstd_w (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-zstd/', format 'parquet', compresstype 'zstd');
INSERT INTO fdw_boost_pq_zstd_w SELECT i, 'zstd_' || i FROM generate_series(1, 50) i;
DROP FOREIGN TABLE fdw_boost_pq_zstd_w;

-- lz4 compression
CREATE FOREIGN TABLE fdw_boost_pq_lz4_w (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-lz4/', format 'parquet', compresstype 'lz4');
INSERT INTO fdw_boost_pq_lz4_w SELECT i, 'lz4_' || i FROM generate_series(1, 50) i;
DROP FOREIGN TABLE fdw_boost_pq_lz4_w;

-- Read back all compressed formats
CREATE FOREIGN TABLE fdw_boost_pq_gz_r (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-gz/', format 'parquet');
SELECT COUNT(*) FROM fdw_boost_pq_gz_r;
DROP FOREIGN TABLE fdw_boost_pq_gz_r;

CREATE FOREIGN TABLE fdw_boost_pq_zstd_r (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-zstd/', format 'parquet');
SELECT COUNT(*) FROM fdw_boost_pq_zstd_r;
DROP FOREIGN TABLE fdw_boost_pq_zstd_r;

CREATE FOREIGN TABLE fdw_boost_pq_lz4_r (id int, val text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-lz4/', format 'parquet');
SELECT COUNT(*) FROM fdw_boost_pq_lz4_r;
DROP FOREIGN TABLE fdw_boost_pq_lz4_r;

-- ============================================================
-- Test 5: Parquet with wide schema (parquetFileWriter many columns)
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_pq_wide_w (
    c1 int, c2 bigint, c3 text, c4 boolean, c5 decimal(10,2),
    c6 date, c7 timestamp, c8 real, c9 double precision, c10 smallint,
    c11 bytea, c12 varchar(50)
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-wide/', format 'parquet');

INSERT INTO fdw_boost_pq_wide_w VALUES (
    1, 1000000, 'wide_test', true, 99.99,
    '2024-06-15', '2024-06-15 12:00:00', 1.5, 2.5, 10,
    '\xABCD', 'short'
);
INSERT INTO fdw_boost_pq_wide_w VALUES (
    2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);
DROP FOREIGN TABLE fdw_boost_pq_wide_w;

CREATE FOREIGN TABLE fdw_boost_pq_wide_r (
    c1 int, c2 bigint, c3 text, c4 boolean, c5 decimal(10,2),
    c6 date, c7 timestamp, c8 real, c9 double precision, c10 smallint,
    c11 bytea, c12 varchar(50)
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/pq-wide/', format 'parquet');

SELECT c1, c2, c3, c4, c5 FROM fdw_boost_pq_wide_r ORDER BY c1;
SELECT c6, c7, c8, c9, c10 FROM fdw_boost_pq_wide_r ORDER BY c1;
DROP FOREIGN TABLE fdw_boost_pq_wide_r;

-- ============================================================
-- Test 6: Avro with wide type coverage
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_avro_w (
    id int, val_text text, val_bool boolean, val_real real,
    val_double double precision, val_date date, val_ts timestamp,
    val_dec decimal(10,2), val_bigint bigint, val_small smallint
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/avro-wide/', format 'avro');

INSERT INTO fdw_boost_avro_w
SELECT i, 'avro_' || i, (i%2=0), (i*0.1)::real, i*0.01,
       '2024-01-01'::date + i, '2024-01-01'::timestamp + (i || ' hours')::interval,
       (i*1.11)::decimal(10,2), i::bigint * 10000, (i%100)::smallint
FROM generate_series(1, 200) i;
DROP FOREIGN TABLE fdw_boost_avro_w;

CREATE FOREIGN TABLE fdw_boost_avro_r (
    id int, val_text text, val_bool boolean, val_real real,
    val_double double precision, val_date date, val_ts timestamp,
    val_dec decimal(10,2), val_bigint bigint, val_small smallint
)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/avro-wide/', format 'avro');

SELECT COUNT(*) FROM fdw_boost_avro_r;
SELECT * FROM fdw_boost_avro_r WHERE id <= 5 ORDER BY id;
SELECT COUNT(*) FROM fdw_boost_avro_r WHERE val_bool = true;
SELECT MIN(val_date), MAX(val_date) FROM fdw_boost_avro_r;
DROP FOREIGN TABLE fdw_boost_avro_r;

-- ============================================================
-- Test 7: Text/CSV format write and read (fdwFunction CSV/TEXT paths)
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_text_w (id int, name text, val decimal(10,2))
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/text/', format 'text');

INSERT INTO fdw_boost_text_w VALUES (1, 'hello', 10.50);
INSERT INTO fdw_boost_text_w VALUES (2, 'world', 20.75);
INSERT INTO fdw_boost_text_w VALUES (3, NULL, NULL);
DROP FOREIGN TABLE fdw_boost_text_w;

CREATE FOREIGN TABLE fdw_boost_text_r (id int, name text, val decimal(10,2))
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/text/', format 'text');

SELECT * FROM fdw_boost_text_r ORDER BY id;
DROP FOREIGN TABLE fdw_boost_text_r;

CREATE FOREIGN TABLE fdw_boost_csv_w (id int, name text, val decimal(10,2))
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/csv/', format 'csv');

INSERT INTO fdw_boost_csv_w VALUES (1, 'csv_hello', 10.50);
INSERT INTO fdw_boost_csv_w VALUES (2, 'csv_world', 20.75);
DROP FOREIGN TABLE fdw_boost_csv_w;

CREATE FOREIGN TABLE fdw_boost_csv_r (id int, name text, val decimal(10,2))
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/csv/', format 'csv');

SELECT * FROM fdw_boost_csv_r ORDER BY id;
DROP FOREIGN TABLE fdw_boost_csv_r;

-- ============================================================
-- Test 8: Bulk data for readPolicy block distribution
-- ============================================================
CREATE FOREIGN TABLE fdw_boost_bulk_w (id int, data text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/bulk/', format 'parquet');

INSERT INTO fdw_boost_bulk_w SELECT i, repeat('x', 100) FROM generate_series(1, 2000) i;
DROP FOREIGN TABLE fdw_boost_bulk_w;

CREATE FOREIGN TABLE fdw_boost_bulk_r (id int, data text)
SERVER fdw_boost_server
OPTIONS (filePath '/warehouse/fdw-test/boost/bulk/', format 'parquet');

SELECT COUNT(*) FROM fdw_boost_bulk_r;
SELECT COUNT(*) FROM fdw_boost_bulk_r WHERE id > 1500;
SELECT id FROM fdw_boost_bulk_r ORDER BY id LIMIT 3;
DROP FOREIGN TABLE fdw_boost_bulk_r;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_boost_server;
DROP SERVER fdw_boost_server;
