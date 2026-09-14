-- Normalize the source location appended to backend error messages so the
-- test does not break when unrelated backend changes shift line numbers.
-- start_matchsubs
-- m/\(execMain\.c:\d+\)/
-- s/\(execMain\.c:\d+\)/(execMain.c:XXX)/
-- m/\(parquetReadRecordBatch\.cpp:\d+\)/
-- s/\(parquetReadRecordBatch\.cpp:\d+\)/(parquetReadRecordBatch.cpp:XXX)/
-- end_matchsubs
-- FDW Vectorized Parquet Read Test
-- Purpose: exercise the generic-parquet vectorized reader (issue #424)
-- Target: provider/parquet/read/parquetReadRecordBatch.cpp
-- Self-contained: writes Parquet to S3/MinIO then reads it back with
-- vectorization forced on, so the vectorized path (not the row fallback)
-- is what produces every result below.

LOAD 'vectorization';
SET vector.enable_vectorization = on;
SET vector.force_vectorization = on;

-- Setup FDW
DROP SERVER IF EXISTS fdw_vecpq_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_vecpq_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_vecpq_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: basic scan / filter / aggregate over Parquet
-- ============================================================
CREATE FOREIGN TABLE fdw_vecpq_w (id bigint, name text, val double precision)
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet/', format 'parquet');

INSERT INTO fdw_vecpq_w
SELECT i::bigint, 'row_' || i, (i * 1.5)::double precision
FROM generate_series(1, 200) i;
DROP FOREIGN TABLE fdw_vecpq_w;

CREATE FOREIGN TABLE fdw_vecpq_r (id bigint, name text, val double precision)
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet/', format 'parquet');

SELECT COUNT(*) FROM fdw_vecpq_r;
SELECT * FROM fdw_vecpq_r WHERE id <= 5 ORDER BY id;
SELECT SUM(val) FROM fdw_vecpq_r;
SELECT COUNT(*) FROM fdw_vecpq_r WHERE val > 200.0;

DROP FOREIGN TABLE fdw_vecpq_r;

-- ============================================================
-- Test 2: NUMERIC (the #424 numeric128 two-arrows bridge)
-- ============================================================
CREATE FOREIGN TABLE fdw_vecpq_num_w (id int, n numeric(20,4))
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet-num/', format 'parquet');

INSERT INTO fdw_vecpq_num_w VALUES
    (1, 3.1400),
    (2, -2.7100),
    (3, 0.0000),
    (4, 123456789.1234),
    (5, -999999999.9999),
    (6, NULL);
DROP FOREIGN TABLE fdw_vecpq_num_w;

CREATE FOREIGN TABLE fdw_vecpq_num_r (id int, n numeric(20,4))
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet-num/', format 'parquet');

SELECT COUNT(*) FROM fdw_vecpq_num_r;
SELECT * FROM fdw_vecpq_num_r ORDER BY id;
SELECT SUM(n), MIN(n), MAX(n) FROM fdw_vecpq_num_r;
SELECT COUNT(*) FROM fdw_vecpq_num_r WHERE n > 0;

DROP FOREIGN TABLE fdw_vecpq_num_r;

-- ============================================================
-- Test 3: T1 fixed-width + date + varchar types
-- ============================================================
CREATE FOREIGN TABLE fdw_vecpq_types_w (
    i2 smallint, i4 int, i8 bigint,
    f4 real, f8 double precision,
    d date, v varchar(20)
)
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet-types/', format 'parquet');

INSERT INTO fdw_vecpq_types_w VALUES
    (1, 100, 1000, 1.5, 2.5, '2000-01-01', 'alpha'),
    (-2, -200, -2000, -1.5, -2.5, '1970-01-01', 'beta'),
    (32767, 2147483647, 9223372036854775807, 3.5, 4.5, '2099-12-31', 'gamma'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_vecpq_types_w;

CREATE FOREIGN TABLE fdw_vecpq_types_r (
    i2 smallint, i4 int, i8 bigint,
    f4 real, f8 double precision,
    d date, v varchar(20)
)
SERVER fdw_vecpq_server
OPTIONS (filePath '/warehouse/fdw-test/vec-parquet-types/', format 'parquet');

SELECT COUNT(*) FROM fdw_vecpq_types_r;
SELECT * FROM fdw_vecpq_types_r ORDER BY i4 NULLS LAST;
SELECT SUM(i2::bigint), SUM(i4::bigint), SUM(i8) FROM fdw_vecpq_types_r;
SELECT MIN(d), MAX(d) FROM fdw_vecpq_types_r;

DROP FOREIGN TABLE fdw_vecpq_types_r;

-- Cleanup
RESET vector.enable_vectorization;
RESET vector.force_vectorization;
DROP USER MAPPING FOR gpadmin SERVER fdw_vecpq_server;
DROP SERVER fdw_vecpq_server;
