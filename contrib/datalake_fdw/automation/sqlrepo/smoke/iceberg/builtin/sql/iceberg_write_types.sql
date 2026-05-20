-- Iceberg Write Types Coverage Test
-- Purpose: Exercise parquetFileWriter type branches for all data types,
--          and additional AM handler paths (TRUNCATE, partition_selector)
-- Target: parquetFileWriter.cpp (+73), pg_iceberg_am_handler.c (+21),
--         partition_selector.c (+11), iceberg_write.cpp, fdwFunction.c

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE OR REPLACE FUNCTION __test_exec(sql text) RETURNS void AS $$
BEGIN
    EXECUTE sql;
EXCEPTION WHEN OTHERS THEN
    RAISE USING
        MESSAGE = regexp_replace(SQLERRM, '\(seg\d+[^)]*\)', '(segN)', 'g'),
        ERRCODE = SQLSTATE;
END;
$$ LANGUAGE plpgsql;

CREATE SERVER wt_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER wt_catalog_server;
CREATE FOREIGN CATALOG wt_catalog SERVER wt_catalog_server;
SET iceberg_default_catalog = 'wt_catalog';

CREATE SERVER wt_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER wt_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME wt_volume SERVER wt_volume_server OPTIONS(base_path '/wt_volume/');
SET iceberg_default_volume = 'wt_volume';

-- ============================================================
-- Test 1a: Numeric precision edge cases (decimal/numeric writer branches)
-- All columns have explicit precision so inserts should succeed.
-- ============================================================
CREATE ICEBERG TABLE wt_numerics (
    col_decimal5_2 decimal(5,2),
    col_decimal18_6 decimal(18,6),
    col_decimal38_10 decimal(38,10),
    col_numeric numeric(20,4)
);

INSERT INTO wt_numerics VALUES (123.45, 123456789012.123456, 1234567890123456789012345678.1234567890, 99999.99);
INSERT INTO wt_numerics VALUES (0.00, 0.000000, 0.0000000000, 0);
INSERT INTO wt_numerics VALUES (-999.99, -999999.999999, -1.0000000001, -1);
INSERT INTO wt_numerics VALUES (NULL, NULL, NULL, NULL);

SELECT COUNT(*) FROM wt_numerics;
SELECT * FROM wt_numerics WHERE col_decimal5_2 IS NOT NULL ORDER BY col_decimal5_2;
DROP TABLE wt_numerics;

-- ============================================================
-- Test 1b: Bare numeric without precision (expected error)
-- Parquet requires explicit precision/scale for DECIMAL columns.
-- A bare "numeric" has no typmod, so the writer rejects it at
-- schema creation time before any data is written.
-- ============================================================
CREATE ICEBERG TABLE wt_numeric_noprecision (col_numeric numeric);
SELECT __test_exec('INSERT INTO wt_numeric_noprecision VALUES (1.23)');
DROP TABLE wt_numeric_noprecision;

-- ============================================================
-- Test 2: Float/double edge values (special float writer branches)
-- ============================================================
CREATE ICEBERG TABLE wt_floats (
    col_real real,
    col_double double precision
);

INSERT INTO wt_floats VALUES (3.14159, 2.718281828459045);
INSERT INTO wt_floats VALUES (0.0, 0.0);
INSERT INTO wt_floats VALUES (-1.5, -1.5);
INSERT INTO wt_floats VALUES ('Infinity', 'Infinity');
INSERT INTO wt_floats VALUES ('-Infinity', '-Infinity');
INSERT INTO wt_floats VALUES ('NaN', 'NaN');
INSERT INTO wt_floats VALUES (NULL, NULL);

SELECT COUNT(*) FROM wt_floats;
SELECT * FROM wt_floats WHERE col_real IS NOT NULL ORDER BY col_real;
DROP TABLE wt_floats;

-- ============================================================
-- Test 3a: Date and timestamp writer branches (should succeed)
-- Date is stored as INT32, timestamp as INT64 MICROS in parquet.
-- ============================================================
CREATE ICEBERG TABLE wt_temporal (
    col_date date,
    col_ts timestamp
);

INSERT INTO wt_temporal VALUES ('2024-06-15', '2024-06-15 10:30:00');
INSERT INTO wt_temporal VALUES ('1970-01-01', '1970-01-01 00:00:00');
INSERT INTO wt_temporal VALUES ('2099-12-31', '2099-12-31 23:59:59');
INSERT INTO wt_temporal VALUES (NULL, NULL);

SELECT COUNT(*) FROM wt_temporal;
SELECT * FROM wt_temporal WHERE col_date IS NOT NULL ORDER BY col_date;
DROP TABLE wt_temporal;

-- ============================================================
-- Test 3b: Timestamptz write (expected error)
-- The parquet write path currently handles TIMESTAMPOID but not
-- TIMESTAMPTZOID in the per-row writeToField switch, so inserts
-- into a table containing timestamptz columns fail at write time.
-- ============================================================
CREATE ICEBERG TABLE wt_temporal_tstz (
    col_date date,
    col_tstz timestamptz
);
SELECT __test_exec('INSERT INTO wt_temporal_tstz VALUES (''2024-06-15'', ''2024-06-15 10:30:00+08'')');
DROP TABLE wt_temporal_tstz;

-- ============================================================
-- Test 4: Boolean and smallint branches
-- ============================================================
CREATE ICEBERG TABLE wt_bool_small (
    col_bool boolean,
    col_small smallint,
    col_int int,
    col_bigint bigint
);

INSERT INTO wt_bool_small VALUES (true, 32767, 2147483647, 9223372036854775807);
INSERT INTO wt_bool_small VALUES (false, -32768, -2147483648, -9223372036854775808);
INSERT INTO wt_bool_small VALUES (NULL, 0, 0, 0);

SELECT COUNT(*) FROM wt_bool_small;
SELECT * FROM wt_bool_small ORDER BY col_small;
DROP TABLE wt_bool_small;

-- ============================================================
-- Test 5: Text/varchar/bytea writer branches
-- ============================================================
CREATE ICEBERG TABLE wt_strings (
    col_text text,
    col_varchar varchar(200),
    col_bytea bytea
);

INSERT INTO wt_strings VALUES ('hello', 'world', '\x48454c4c4f');
INSERT INTO wt_strings VALUES ('', '', '\x');
INSERT INTO wt_strings VALUES (repeat('a', 100), repeat('b', 200), '\xDEADBEEF');
INSERT INTO wt_strings VALUES (NULL, NULL, NULL);

SELECT COUNT(*) FROM wt_strings;
SELECT col_text, col_varchar FROM wt_strings WHERE col_text IS NOT NULL ORDER BY col_text;
DROP TABLE wt_strings;

-- ============================================================
-- Test 6: TRUNCATE test (AM handler truncate path)
-- ============================================================
CREATE ICEBERG TABLE wt_truncate (id bigint, val text);
INSERT INTO wt_truncate SELECT i, 'val_' || i FROM generate_series(1, 20) i;
SELECT COUNT(*) FROM wt_truncate;

TRUNCATE wt_truncate;
SELECT COUNT(*) FROM wt_truncate;

-- Re-insert after truncate
INSERT INTO wt_truncate VALUES (1, 'after_truncate');
SELECT * FROM wt_truncate;
DROP TABLE wt_truncate;

-- ============================================================
-- Test 7: Large batch write (exercises buffering and flushing)
-- ============================================================
CREATE ICEBERG TABLE wt_bulk (
    id bigint,
    val text,
    amount decimal(10,2),
    flag boolean,
    ts timestamp
);

INSERT INTO wt_bulk
SELECT
    i,
    'row_' || i || '_' || repeat('x', (i % 50)),
    (i * 1.23)::decimal(10,2),
    (i % 2 = 0),
    '2024-01-01'::timestamp + (i || ' hours')::interval
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM wt_bulk;
SELECT COUNT(*) FROM wt_bulk WHERE flag = true;
SELECT MIN(amount), MAX(amount) FROM wt_bulk;

DROP TABLE wt_bulk;

-- ============================================================
-- Test 8: Multiple small inserts (creates multiple data files -> fragments)
-- ============================================================
CREATE ICEBERG TABLE wt_multi_insert (id bigint, data text);

INSERT INTO wt_multi_insert VALUES (1, 'batch1');
INSERT INTO wt_multi_insert VALUES (2, 'batch2');
INSERT INTO wt_multi_insert VALUES (3, 'batch3');
INSERT INTO wt_multi_insert VALUES (4, 'batch4');
INSERT INTO wt_multi_insert VALUES (5, 'batch5');
INSERT INTO wt_multi_insert VALUES (6, 'batch6');
INSERT INTO wt_multi_insert VALUES (7, 'batch7');
INSERT INTO wt_multi_insert VALUES (8, 'batch8');
INSERT INTO wt_multi_insert VALUES (9, 'batch9');
INSERT INTO wt_multi_insert VALUES (10, 'batch10');

SELECT COUNT(*) FROM wt_multi_insert;

-- VACUUM to trigger compaction (exercises plan_file_groups path)
SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM wt_multi_insert;
RESET datalake.iceberg_vacuum_compact_min_input_files;

SELECT COUNT(*) FROM wt_multi_insert;

DROP TABLE wt_multi_insert;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME wt_volume;
DROP USER MAPPING FOR current_user SERVER wt_volume_server;
DROP SERVER wt_volume_server;
DROP CATALOG wt_catalog;
DROP USER MAPPING FOR current_user SERVER wt_catalog_server;
DROP SERVER wt_catalog_server;
DROP FUNCTION IF EXISTS __test_exec;
