-- 01_basic_types.sql
-- Test Iceberg basic data type support

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg Basic Types');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER bt_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER bt_catalog_server;
CREATE FOREIGN CATALOG bt_catalog SERVER bt_catalog_server;
SET iceberg_default_catalog = 'bt_catalog';

CREATE SERVER bt_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://lakehouse:9100', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER bt_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'password');
CREATE FOREIGN VOLUME bt_volume SERVER bt_volume_server OPTIONS(base_path '/bt_volume/');
SET iceberg_default_volume = 'bt_volume';

-- ============================================================
-- Test 1: boolean
-- ============================================================
SELECT test_log('Test 1: boolean type');

CREATE ICEBERG TABLE bt_bool (id bigint, val boolean);
INSERT INTO bt_bool VALUES (1, true), (2, false), (3, NULL);
SELECT * FROM bt_bool ORDER BY id;
DROP TABLE bt_bool;

-- ============================================================
-- Test 2: smallint
-- ============================================================
SELECT test_log('Test 2: smallint type');

CREATE ICEBERG TABLE bt_small (id bigint, val smallint);
INSERT INTO bt_small VALUES (1, -32768), (2, 0), (3, 32767);
SELECT * FROM bt_small ORDER BY id;
DROP TABLE bt_small;

-- ============================================================
-- Test 3: int
-- ============================================================
SELECT test_log('Test 3: int type');

CREATE ICEBERG TABLE bt_int (id bigint, val int);
INSERT INTO bt_int VALUES (1, -2147483648), (2, 0), (3, 2147483647);
SELECT * FROM bt_int ORDER BY id;
DROP TABLE bt_int;

-- ============================================================
-- Test 4: bigint
-- ============================================================
SELECT test_log('Test 4: bigint type');

CREATE ICEBERG TABLE bt_big (id bigint, val bigint);
INSERT INTO bt_big VALUES (1, -9223372036854775808), (2, 0), (3, 9223372036854775807);
SELECT * FROM bt_big ORDER BY id;
DROP TABLE bt_big;

-- ============================================================
-- Test 5: real and double precision
-- ============================================================
SELECT test_log('Test 5: real and double precision');

CREATE ICEBERG TABLE bt_float (id bigint, val_r real, val_d double precision);
INSERT INTO bt_float VALUES (1, 3.14, 2.718281828459045);
INSERT INTO bt_float VALUES (2, -1.5, -99.999);
INSERT INTO bt_float VALUES (3, 0.0, 0.0);
SELECT * FROM bt_float ORDER BY id;
DROP TABLE bt_float;

-- ============================================================
-- Test 6: text and varchar
-- ============================================================
SELECT test_log('Test 6: text and varchar');

CREATE ICEBERG TABLE bt_str (id bigint, val_t text, val_v varchar(100));
INSERT INTO bt_str VALUES (1, '', '');
INSERT INTO bt_str VALUES (2, 'normal text', 'normal varchar');
INSERT INTO bt_str VALUES (3, 'a long string that contains more characters to test storage and retrieval of text data in iceberg tables', 'bounded varchar value');
SELECT * FROM bt_str ORDER BY id;
DROP TABLE bt_str;

-- ============================================================
-- Test 6b: char(N) maps to Iceberg `string`
-- Verifies that CHAR(N) columns no longer emit
--   WARNING: Unsupported PostgreSQL type OID: 1042, using string
-- and follow Snowflake / Spark / Trino / PrestoDB semantics:
-- trailing-space and right-padding are NOT preserved across
-- write/read; CHAR(N) effectively behaves like VARCHAR(N).
-- ============================================================
SELECT test_log('Test 6b: char(N) maps to Iceberg string');

CREATE ICEBERG TABLE bt_char (id bigint, val_c char(5));
INSERT INTO bt_char VALUES (1, 'A');
INSERT INTO bt_char VALUES (2, 'A   ');
INSERT INTO bt_char VALUES (3, '     ');
INSERT INTO bt_char VALUES (4, 'ABCDE');
SELECT id, '[' || val_c || ']' AS bracketed,
       length(val_c) AS chars,
       octet_length(val_c) AS bytes
FROM bt_char ORDER BY id;
DROP TABLE bt_char;

-- ============================================================
-- Test 7: All-NULL row
-- ============================================================
SELECT test_log('Test 7: All-NULL row insert and query');

CREATE ICEBERG TABLE bt_nulls (id bigint, a int, b text, c boolean, d decimal(10,2));
INSERT INTO bt_nulls VALUES (NULL, NULL, NULL, NULL, NULL);
INSERT INTO bt_nulls VALUES (1, 42, 'not null', true, 9.99);
SELECT * FROM bt_nulls ORDER BY id NULLS FIRST;
DROP TABLE bt_nulls;

-- ============================================================
-- Test 8: Mixed-type WHERE query
-- ============================================================
SELECT test_log('Test 8: Mixed-type WHERE query');

CREATE ICEBERG TABLE bt_mixed (id bigint, name text, val int, flag boolean, amount decimal(10,2));
INSERT INTO bt_mixed VALUES
    (1, 'alpha', 10, true, 100.50),
    (2, 'beta', 20, false, 200.75),
    (3, 'gamma', 30, true, 300.00),
    (4, 'delta', 40, false, 50.25);

SELECT id, name FROM bt_mixed WHERE flag = true AND amount > 100.00 ORDER BY id;
SELECT id, name FROM bt_mixed WHERE val >= 20 AND val <= 30 ORDER BY id;
DROP TABLE bt_mixed;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME bt_volume;
DROP USER MAPPING FOR current_user SERVER bt_volume_server;
DROP SERVER bt_volume_server;
DROP CATALOG bt_catalog;
DROP USER MAPPING FOR current_user SERVER bt_catalog_server;
DROP SERVER bt_catalog_server;
