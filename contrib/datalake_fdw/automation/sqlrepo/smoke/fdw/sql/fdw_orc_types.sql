-- FDW ORC All Types Test
-- Purpose: Exercise all ORC type write/read branches
-- Target: orcWriter.cpp (+126), orcFileReader.cpp (+65), orcRead.cpp (+32),
--         orcReadInterface.cpp (+34), logicalType.cpp ORC branches

-- Setup FDW
DROP SERVER IF EXISTS fdw_orct_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_orct_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_orct_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: All integer types
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_int_w (
    col_small smallint,
    col_int int,
    col_bigint bigint
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/int/', format 'orc');

INSERT INTO fdw_orct_int_w VALUES (1, 100, 10000);
INSERT INTO fdw_orct_int_w VALUES (32767, 2147483647, 9223372036854775807);
INSERT INTO fdw_orct_int_w VALUES (-32768, -2147483648, -9223372036854775808);
INSERT INTO fdw_orct_int_w VALUES (0, 0, 0);
INSERT INTO fdw_orct_int_w VALUES (NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_orct_int_w;

CREATE FOREIGN TABLE fdw_orct_int_r (
    col_small smallint,
    col_int int,
    col_bigint bigint
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/int/', format 'orc');

SELECT * FROM fdw_orct_int_r ORDER BY col_small NULLS LAST;
DROP FOREIGN TABLE fdw_orct_int_r;

-- ============================================================
-- Test 2: Float types
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_float_w (
    col_real real,
    col_double double precision
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/float/', format 'orc');

INSERT INTO fdw_orct_float_w VALUES (3.14, 2.71828);
INSERT INTO fdw_orct_float_w VALUES (0.0, 0.0);
INSERT INTO fdw_orct_float_w VALUES (-1.5, -1.5);
INSERT INTO fdw_orct_float_w VALUES (NULL, NULL);
DROP FOREIGN TABLE fdw_orct_float_w;

CREATE FOREIGN TABLE fdw_orct_float_r (
    col_real real,
    col_double double precision
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/float/', format 'orc');

SELECT * FROM fdw_orct_float_r ORDER BY col_real NULLS LAST;
DROP FOREIGN TABLE fdw_orct_float_r;

-- ============================================================
-- Test 3: Boolean type
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_bool_w (col_bool boolean, col_id int)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/bool/', format 'orc');

INSERT INTO fdw_orct_bool_w VALUES (true, 1), (false, 2), (NULL, 3);
DROP FOREIGN TABLE fdw_orct_bool_w;

CREATE FOREIGN TABLE fdw_orct_bool_r (col_bool boolean, col_id int)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/bool/', format 'orc');

SELECT * FROM fdw_orct_bool_r ORDER BY col_id;
DROP FOREIGN TABLE fdw_orct_bool_r;

-- ============================================================
-- Test 4: Date and timestamp
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_date_w (
    col_date date,
    col_ts timestamp
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/date/', format 'orc');

INSERT INTO fdw_orct_date_w VALUES ('2024-01-15', '2024-01-15 10:30:00');
INSERT INTO fdw_orct_date_w VALUES ('1970-01-01', '1970-01-01 00:00:00');
INSERT INTO fdw_orct_date_w VALUES ('2099-12-31', '2099-12-31 23:59:59');
INSERT INTO fdw_orct_date_w VALUES (NULL, NULL);
DROP FOREIGN TABLE fdw_orct_date_w;

CREATE FOREIGN TABLE fdw_orct_date_r (
    col_date date,
    col_ts timestamp
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/date/', format 'orc');

SELECT * FROM fdw_orct_date_r ORDER BY col_date NULLS LAST;
DROP FOREIGN TABLE fdw_orct_date_r;

-- ============================================================
-- Test 5: Decimal/numeric types (various precision)
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_dec_w (
    col_dec5 decimal(5,2),
    col_dec15 decimal(15,4),
    col_dec38 decimal(38,10)
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/decimal/', format 'orc');

INSERT INTO fdw_orct_dec_w VALUES (123.45, 12345678.1234, 1234567890123456789.1234567890);
INSERT INTO fdw_orct_dec_w VALUES (0.00, 0.0000, 0.0000000000);
INSERT INTO fdw_orct_dec_w VALUES (-999.99, -99999.9999, -1.0000000001);
INSERT INTO fdw_orct_dec_w VALUES (NULL, NULL, NULL);
DROP FOREIGN TABLE fdw_orct_dec_w;

CREATE FOREIGN TABLE fdw_orct_dec_r (
    col_dec5 decimal(5,2),
    col_dec15 decimal(15,4),
    col_dec38 decimal(38,10)
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/decimal/', format 'orc');

SELECT * FROM fdw_orct_dec_r ORDER BY col_dec5 NULLS LAST;
DROP FOREIGN TABLE fdw_orct_dec_r;

-- ============================================================
-- Test 6: Text and string types
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_str_w (
    col_text text,
    col_varchar varchar(100)
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/string/', format 'orc');

INSERT INTO fdw_orct_str_w VALUES ('hello', 'world');
INSERT INTO fdw_orct_str_w VALUES ('', '');
INSERT INTO fdw_orct_str_w VALUES (repeat('x', 80), repeat('y', 80));
INSERT INTO fdw_orct_str_w VALUES (NULL, NULL);
DROP FOREIGN TABLE fdw_orct_str_w;

CREATE FOREIGN TABLE fdw_orct_str_r (
    col_text text,
    col_varchar varchar(100)
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/string/', format 'orc');

SELECT * FROM fdw_orct_str_r ORDER BY col_text NULLS LAST;
DROP FOREIGN TABLE fdw_orct_str_r;

-- ============================================================
-- Test 7: Mixed types bulk write (large batch exercises buffering)
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_mixed_w (
    id int,
    name text,
    val decimal(10,2),
    flag boolean,
    ts timestamp,
    col_date date,
    col_real real,
    col_big bigint
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/mixed/', format 'orc');

INSERT INTO fdw_orct_mixed_w
SELECT
    i,
    'name_' || i,
    (i * 1.23)::decimal(10,2),
    (i % 2 = 0),
    '2024-01-01'::timestamp + (i || ' hours')::interval,
    '2024-01-01'::date + i,
    (i * 0.1)::real,
    i::bigint * 1000000
FROM generate_series(1, 500) i;
DROP FOREIGN TABLE fdw_orct_mixed_w;

CREATE FOREIGN TABLE fdw_orct_mixed_r (
    id int,
    name text,
    val decimal(10,2),
    flag boolean,
    ts timestamp,
    col_date date,
    col_real real,
    col_big bigint
)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/mixed/', format 'orc');

SELECT COUNT(*) FROM fdw_orct_mixed_r;
SELECT * FROM fdw_orct_mixed_r WHERE id <= 5 ORDER BY id;
SELECT COUNT(*) FROM fdw_orct_mixed_r WHERE flag = true;
DROP FOREIGN TABLE fdw_orct_mixed_r;

-- ============================================================
-- Test 8: ORC with compression (snappy)
-- ============================================================
CREATE FOREIGN TABLE fdw_orct_snappy_w (id int, val text)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/snappy/', format 'orc', compresstype 'snappy');

INSERT INTO fdw_orct_snappy_w SELECT i, repeat('data_', 10) FROM generate_series(1, 100) i;
DROP FOREIGN TABLE fdw_orct_snappy_w;

CREATE FOREIGN TABLE fdw_orct_snappy_r (id int, val text)
SERVER fdw_orct_server
OPTIONS (filePath '/warehouse/fdw-test/orc-types/snappy/', format 'orc');

SELECT COUNT(*) FROM fdw_orct_snappy_r;
DROP FOREIGN TABLE fdw_orct_snappy_r;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER fdw_orct_server;
DROP SERVER fdw_orct_server;
