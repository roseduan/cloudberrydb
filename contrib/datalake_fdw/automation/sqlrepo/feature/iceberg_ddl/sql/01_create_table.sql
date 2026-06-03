-- 01_create_table.sql
-- Test Iceberg CREATE TABLE variations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg CREATE TABLE');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER dt_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dt_catalog_server;
CREATE FOREIGN CATALOG dt_catalog SERVER dt_catalog_server;
SET iceberg_default_catalog = 'dt_catalog';

CREATE SERVER dt_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER dt_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dt_volume SERVER dt_volume_server OPTIONS(base_path '/dt_volume/');
SET iceberg_default_volume = 'dt_volume';

-- ============================================================
-- Test 1: Basic multi-column table
-- ============================================================
SELECT test_log('Test 1: Basic multi-column table');

CREATE ICEBERG TABLE dt_basic (id bigint, name text, val int);
INSERT INTO dt_basic VALUES (1, 'hello', 10), (2, 'world', 20);
SELECT * FROM dt_basic ORDER BY id;
DROP TABLE dt_basic;

-- ============================================================
-- Test 2: Single-column table
-- ============================================================
SELECT test_log('Test 2: Single-column table');

CREATE ICEBERG TABLE dt_single (id bigint);
INSERT INTO dt_single VALUES (1), (2), (3);
SELECT COUNT(*) AS cnt FROM dt_single;
DROP TABLE dt_single;

-- ============================================================
-- Test 3: Wide table (22 columns)
-- ============================================================
SELECT test_log('Test 3: Wide table with 22 columns');

CREATE ICEBERG TABLE dt_wide (
    c01 bigint, c02 bigint, c03 bigint, c04 bigint, c05 bigint,
    c06 text, c07 text, c08 text, c09 text, c10 text,
    c11 int, c12 int, c13 int, c14 int, c15 int,
    c16 boolean, c17 boolean, c18 boolean,
    c19 decimal(10,2), c20 decimal(10,2),
    c21 date, c22 timestamp
);
INSERT INTO dt_wide VALUES (
    1, 2, 3, 4, 5,
    'a', 'b', 'c', 'd', 'e',
    10, 20, 30, 40, 50,
    true, false, true,
    99.99, 123.45,
    '2024-01-15', '2024-01-15 10:30:00'
);
SELECT c01, c06, c11, c16, c19, c21 FROM dt_wide;
DROP TABLE dt_wide;

-- ============================================================
-- Test 4: All supported types in one table
-- ============================================================
SELECT test_log('Test 4: All supported types');

CREATE ICEBERG TABLE dt_alltypes (
    col_bool boolean,
    col_small smallint,
    col_int int,
    col_big bigint,
    col_real real,
    col_double double precision,
    col_decimal decimal(15,2),
    col_text text,
    col_varchar varchar(100),
    col_date date,
    col_ts timestamp,
    col_tstz timestamptz
);
INSERT INTO dt_alltypes VALUES (
    true, 32767, 2147483647, 9223372036854775807,
    3.14, 2.718281828,
    12345.67,
    'hello world', 'varchar value',
    '2024-06-15', '2024-06-15 12:30:00', '2024-06-15 12:30:00+00'
);
SELECT col_bool, col_small, col_int, col_big FROM dt_alltypes;
SELECT col_real, col_double, col_decimal FROM dt_alltypes;
SELECT col_text, col_varchar FROM dt_alltypes;
SELECT col_date, col_ts, col_tstz FROM dt_alltypes;
DROP TABLE dt_alltypes;

-- ============================================================
-- Test 5: Drop and recreate same name
-- ============================================================
SELECT test_log('Test 5: Drop table, recreate same name');

CREATE ICEBERG TABLE dt_reuse (id bigint, val int);
INSERT INTO dt_reuse VALUES (1, 100);
DROP TABLE dt_reuse;

CREATE ICEBERG TABLE dt_reuse (id bigint, val int);
INSERT INTO dt_reuse VALUES (2, 200), (3, 300);
SELECT * FROM dt_reuse ORDER BY id;
DROP TABLE dt_reuse;

-- ============================================================
-- Test 6: Multiple tables exist simultaneously
-- ============================================================
SELECT test_log('Test 6: Multiple tables simultaneously');

CREATE ICEBERG TABLE dt_multi_1 (id bigint, data text);
CREATE ICEBERG TABLE dt_multi_2 (id bigint, amount int);
CREATE ICEBERG TABLE dt_multi_3 (id bigint);

INSERT INTO dt_multi_1 VALUES (1, 'first');
INSERT INTO dt_multi_2 VALUES (2, 999);
INSERT INTO dt_multi_3 VALUES (3);

SELECT 'dt_multi_1' AS tbl, COUNT(*) FROM dt_multi_1
UNION ALL
SELECT 'dt_multi_2', COUNT(*) FROM dt_multi_2
UNION ALL
SELECT 'dt_multi_3', COUNT(*) FROM dt_multi_3
ORDER BY tbl;

DROP TABLE dt_multi_1;
DROP TABLE dt_multi_2;
DROP TABLE dt_multi_3;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME dt_volume;
DROP USER MAPPING FOR current_user SERVER dt_volume_server;
DROP SERVER dt_volume_server;
DROP CATALOG dt_catalog;
DROP USER MAPPING FOR current_user SERVER dt_catalog_server;
DROP SERVER dt_catalog_server;
