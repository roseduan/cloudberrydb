-- FDW S3 Write All Formats with More Types
-- Purpose: Exercise deeper write paths in orcWriter, parquetFileWriter, avroWriter, logicalType
-- Target: orcWriter.cpp (deeper type branches), parquetFileWriter.cpp (more type variants),
--         avroWriter.cpp (more types), logicalType.cpp (all type mappings),
--         providerWrapper.cpp, provider.cpp

DROP SERVER IF EXISTS fdw_swf_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_swf_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_swf_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Parquet with ALL types (deep parquetFileWriter + logicalType)
-- ============================================================
CREATE FOREIGN TABLE fdw_swf_pq_w (
    c_bool boolean, c_int2 smallint, c_int4 int, c_int8 bigint,
    c_float4 real, c_float8 double precision,
    c_numeric numeric(18,4), c_text text, c_varchar varchar(50),
    c_date date, c_timestamp timestamp, c_bytea bytea
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/pq_types/', format 'parquet');

INSERT INTO fdw_swf_pq_w VALUES
    (true, 1, 100, 10000, 1.5, 2.5, 12345.6789, 'hello', 'world', '2024-01-15', '2024-01-15 10:30:00', '\x0102030405'),
    (false, -1, -100, -10000, -1.5, -2.5, -12345.6789, '', '', '1970-01-01', '1970-01-01 00:00:00', '\x'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_swf_pq_w;

CREATE FOREIGN TABLE fdw_swf_pq_r (
    c_bool boolean, c_int2 smallint, c_int4 int, c_int8 bigint,
    c_float4 real, c_float8 double precision,
    c_numeric numeric(18,4), c_text text, c_varchar varchar(50),
    c_date date, c_timestamp timestamp, c_bytea bytea
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/pq_types/', format 'parquet');

SELECT c_bool, c_int2, c_int4, c_int8 FROM fdw_swf_pq_r WHERE c_bool IS NOT NULL ORDER BY c_int2;
SELECT c_float4, c_float8, c_numeric FROM fdw_swf_pq_r WHERE c_float4 IS NOT NULL ORDER BY c_float4;
SELECT c_text, c_varchar, c_date, c_timestamp FROM fdw_swf_pq_r WHERE c_text IS NOT NULL ORDER BY c_date;
DROP FOREIGN TABLE fdw_swf_pq_r;

-- ============================================================
-- Test 2: ORC with ALL types (deep orcWriter type branches)
-- ============================================================
CREATE FOREIGN TABLE fdw_swf_orc_w (
    c_bool boolean, c_int2 smallint, c_int4 int, c_int8 bigint,
    c_float4 real, c_float8 double precision,
    c_numeric numeric(15,2), c_text text, c_date date, c_timestamp timestamp
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/orc_types/', format 'orc');

INSERT INTO fdw_swf_orc_w VALUES
    (true, 32767, 2147483647, 9223372036854775807, 3.14, 2.71828, 99999999999.99, 'orc_test', '2024-06-15', '2024-06-15 12:00:00'),
    (false, -32768, -2147483648, -9223372036854775808, 0.0, 0.0, 0.00, '', '1970-01-01', '1970-01-01 00:00:00'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_swf_orc_w;

CREATE FOREIGN TABLE fdw_swf_orc_r (
    c_bool boolean, c_int2 smallint, c_int4 int, c_int8 bigint,
    c_float4 real, c_float8 double precision,
    c_numeric numeric(15,2), c_text text, c_date date, c_timestamp timestamp
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/orc_types/', format 'orc');

SELECT c_bool, c_int2, c_int4, c_int8 FROM fdw_swf_orc_r WHERE c_bool IS NOT NULL ORDER BY c_int2;
SELECT c_float4, c_float8, c_numeric FROM fdw_swf_orc_r WHERE c_numeric IS NOT NULL ORDER BY c_numeric;
DROP FOREIGN TABLE fdw_swf_orc_r;

-- ============================================================
-- Test 3: Avro with more types
-- ============================================================
CREATE FOREIGN TABLE fdw_swf_avro_w (
    c_int int, c_bigint bigint, c_float real, c_double double precision,
    c_text text, c_bool boolean
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/avro_types/', format 'avro');

INSERT INTO fdw_swf_avro_w VALUES
    (1, 100, 1.5, 2.5, 'avro_type_test', true),
    (2, -100, -1.5, -2.5, '', false),
    (NULL, NULL, NULL, NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_swf_avro_w;

CREATE FOREIGN TABLE fdw_swf_avro_r (
    c_int int, c_bigint bigint, c_float real, c_double double precision,
    c_text text, c_bool boolean
)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/avro_types/', format 'avro');

SELECT * FROM fdw_swf_avro_r WHERE c_int IS NOT NULL ORDER BY c_int;
DROP FOREIGN TABLE fdw_swf_avro_r;

-- ============================================================
-- Test 4: Large batch write (exercises buffer management)
-- ============================================================
CREATE FOREIGN TABLE fdw_swf_large_w (id int, name text, val decimal(10,2))
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/large_parquet/', format 'parquet');

INSERT INTO fdw_swf_large_w SELECT i, 'row_' || i, i * 0.99 FROM generate_series(1, 5000) i;
DROP FOREIGN TABLE fdw_swf_large_w;

CREATE FOREIGN TABLE fdw_swf_large_r (id int, name text, val decimal(10,2))
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/large_parquet/', format 'parquet');

SELECT COUNT(*) FROM fdw_swf_large_r;
SELECT MIN(id), MAX(id), SUM(val)::numeric(15,2) FROM fdw_swf_large_r;
DROP FOREIGN TABLE fdw_swf_large_r;

-- ============================================================
-- Test 5: ORC large batch
-- ============================================================
CREATE FOREIGN TABLE fdw_swf_large_orc_w (id int, val text)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/large_orc/', format 'orc');

INSERT INTO fdw_swf_large_orc_w SELECT i, repeat('x', 100) FROM generate_series(1, 2000) i;
DROP FOREIGN TABLE fdw_swf_large_orc_w;

CREATE FOREIGN TABLE fdw_swf_large_orc_r (id int, val text)
SERVER fdw_swf_server
OPTIONS (filePath '/warehouse/fdw-test/swf/large_orc/', format 'orc');

SELECT COUNT(*) FROM fdw_swf_large_orc_r;
DROP FOREIGN TABLE fdw_swf_large_orc_r;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_swf_server;
DROP SERVER fdw_swf_server;
