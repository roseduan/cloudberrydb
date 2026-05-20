-- Iceberg Coverage Boost Test
-- Purpose: Push near-80% files over the 80% threshold
-- Target: datalake_option.c, iceberg_catalog_fdw.c, iceberg_volume_fdw.c,
--         parquetFileWriter.cpp, fdwFunction.c, datalake_fdw.c,
--         am_iceberg_am_handler.c, fileMetadata.c, partition_selector.c,
--         datalake_fragment.c, readPolicy.cpp

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

CREATE SERVER cb_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cb_catalog_server;
CREATE FOREIGN CATALOG cb_catalog SERVER cb_catalog_server;
SET iceberg_default_catalog = 'cb_catalog';

CREATE SERVER cb_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER cb_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cb_volume SERVER cb_volume_server OPTIONS(base_path '/cb_volume/');
SET iceberg_default_volume = 'cb_volume';

-- ============================================================
-- Test 1a: Wide table write/read (parquetFileWriter type branches)
-- Covers: parquetFileWriter.cpp fillXXXValues for all types,
--         orcWriter.cpp writeToField type branches.
-- NOTE: timestamptz excluded here (unsupported in parquet write);
--       tested separately in Test 1b.
-- ============================================================
CREATE ICEBERG TABLE cb_all_types (
    col_bool boolean,
    col_small smallint,
    col_int int,
    col_bigint bigint,
    col_real real,
    col_double double precision,
    col_dec5 decimal(5,2),
    col_dec15 decimal(15,4),
    col_dec38 decimal(38,10),
    col_text text,
    col_varchar varchar(100),
    col_date date,
    col_ts timestamp,
    col_bytea bytea
);

-- Row with all types populated
INSERT INTO cb_all_types VALUES (
    true, 1, 100, 1000000, 3.14, 2.718281828,
    123.45, 12345678.1234, 12345678901234567890.1234567890,
    'hello world', 'varchar_val',
    '2024-06-15', '2024-06-15 10:30:00',
    '\xDEADBEEF'
);

-- Row with all NULLs (exercises NULL handling in every type branch)
INSERT INTO cb_all_types VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

-- Row with boundary values
INSERT INTO cb_all_types VALUES (
    false, -32768, -2147483648, -9223372036854775808,
    0.0, 0.0, -999.99, -99999999999.9999, -1.0000000000,
    '', '', '1970-01-01', '1970-01-01 00:00:00',
    '\x'
);

-- Large text value (exercises string buffer resizing)
INSERT INTO cb_all_types VALUES (
    true, 32767, 2147483647, 9223372036854775807,
    'Infinity', 'Infinity', 999.99, 99999999999.9999, 1.0000000000,
    repeat('x', 500), repeat('y', 100),
    '2099-12-31', '2099-12-31 23:59:59',
    '\xCAFEBABE'
);

SELECT COUNT(*) FROM cb_all_types;
SELECT col_bool, col_small, col_int, col_bigint FROM cb_all_types ORDER BY col_small NULLS LAST;
SELECT col_real, col_double, col_dec5, col_dec15 FROM cb_all_types WHERE col_dec5 IS NOT NULL ORDER BY col_dec5;
SELECT col_date, col_ts FROM cb_all_types WHERE col_date IS NOT NULL ORDER BY col_date;
SELECT length(col_text), length(col_bytea) FROM cb_all_types WHERE col_text IS NOT NULL ORDER BY length(col_text);

DROP TABLE cb_all_types;

-- ============================================================
-- Test 1b: Timestamptz column write (expected error)
-- The parquet write path handles TIMESTAMPOID but not TIMESTAMPTZOID
-- in the per-row writeToField switch (parquetFileWriter.cpp).
-- The INSERT fails with "OSS protocol not supported data type".
-- ============================================================
CREATE ICEBERG TABLE cb_tstz_test (col_tstz timestamptz);
SELECT __test_exec('INSERT INTO cb_tstz_test VALUES (''2024-06-15 10:30:00+08'')');
DROP TABLE cb_tstz_test;

-- ============================================================
-- Test 2: Multiple small tables (exercises catalog_fdw list paths,
--         deletion_queue, and datalake_option parsing)
-- ============================================================
CREATE ICEBERG TABLE cb_t1 (id bigint);
CREATE ICEBERG TABLE cb_t2 (id bigint, name text);
CREATE ICEBERG TABLE cb_t3 (id bigint, val decimal(10,2));
CREATE ICEBERG TABLE cb_t4 (id bigint, flag boolean, ts timestamp);

INSERT INTO cb_t1 SELECT i FROM generate_series(1, 10) i;
INSERT INTO cb_t2 SELECT i, 'name_' || i FROM generate_series(1, 10) i;
INSERT INTO cb_t3 SELECT i, (i * 1.5)::decimal(10,2) FROM generate_series(1, 10) i;
INSERT INTO cb_t4 SELECT i, (i%2=0), '2024-01-01'::timestamp + (i || ' days')::interval FROM generate_series(1, 10) i;

-- Scan all tables (exercises volume_fdw scan path for each)
SELECT COUNT(*) FROM cb_t1;
SELECT COUNT(*) FROM cb_t2;
SELECT COUNT(*) FROM cb_t3;
SELECT COUNT(*) FROM cb_t4;

-- Cross-table queries (exercises fragment iteration)
SELECT t1.id, t2.name FROM cb_t1 t1 JOIN cb_t2 t2 ON t1.id = t2.id WHERE t1.id <= 3 ORDER BY t1.id;

-- ANALYZE on multiple tables (exercises iceberg_catalog_fdw get_statistics)
ANALYZE cb_t1;
ANALYZE cb_t2;
ANALYZE cb_t3;
ANALYZE cb_t4;

-- VACUUM on tables with data (exercises plan_file_groups + commit paths)
SET datalake.iceberg_vacuum_compact_min_input_files = 1;
VACUUM cb_t1;
VACUUM cb_t2;
RESET datalake.iceberg_vacuum_compact_min_input_files;

-- DROP all (exercises deletion_queue multiple entries)
DROP TABLE cb_t1;
DROP TABLE cb_t2;
DROP TABLE cb_t3;
DROP TABLE cb_t4;

-- ============================================================
-- Test 3: DML variety (exercises fdwFunction insert/update/delete paths,
--         iceberg_volume_fdw modify operations)
-- ============================================================
CREATE ICEBERG TABLE cb_dml (
    id bigint,
    category text,
    amount decimal(10,2),
    active boolean,
    created_at timestamp
);

-- Single row insert
INSERT INTO cb_dml VALUES (1, 'A', 100.00, true, '2024-01-01 00:00:00');

-- Multi-row insert
INSERT INTO cb_dml VALUES
    (2, 'B', 200.00, false, '2024-01-02 00:00:00'),
    (3, 'A', 150.00, true, '2024-01-03 00:00:00'),
    (4, 'C', 300.00, true, '2024-01-04 00:00:00'),
    (5, 'B', 250.00, false, '2024-01-05 00:00:00');

-- Bulk insert
INSERT INTO cb_dml
SELECT i, chr(65 + (i % 3)), (i * 10.5)::decimal(10,2), (i % 2 = 0),
       '2024-06-01'::timestamp + (i || ' hours')::interval
FROM generate_series(6, 100) i;

SELECT COUNT(*) FROM cb_dml;

-- Various WHERE conditions (exercises readPolicy, parquetFileReader filter)
SELECT COUNT(*) FROM cb_dml WHERE category = 'A';
SELECT COUNT(*) FROM cb_dml WHERE amount > 500.00;
SELECT COUNT(*) FROM cb_dml WHERE active = true AND amount < 200.00;
SELECT COUNT(*) FROM cb_dml WHERE id BETWEEN 20 AND 40;

-- Column projection (exercises fileMetadata, parquet column selection)
SELECT id FROM cb_dml WHERE id <= 5 ORDER BY id;
SELECT category, amount FROM cb_dml WHERE id <= 3 ORDER BY id;
SELECT active, created_at FROM cb_dml WHERE id = 1;

-- Aggregates
SELECT category, COUNT(*), SUM(amount), AVG(amount)
FROM cb_dml GROUP BY category ORDER BY category;

-- UPDATE different patterns
UPDATE cb_dml SET amount = amount * 2 WHERE id <= 5;
UPDATE cb_dml SET category = 'X', active = false WHERE id = 1;
UPDATE cb_dml SET created_at = '2025-01-01 00:00:00' WHERE category = 'C';

SELECT * FROM cb_dml WHERE id <= 5 ORDER BY id;

-- DELETE different patterns
DELETE FROM cb_dml WHERE id > 90;
DELETE FROM cb_dml WHERE category = 'X';
DELETE FROM cb_dml WHERE active = false AND amount < 100.00;

SELECT COUNT(*) FROM cb_dml;

-- VACUUM after DML (exercises compaction after delete)
SET datalake.iceberg_vacuum_compact_min_input_files = 2;
VACUUM cb_dml;
RESET datalake.iceberg_vacuum_compact_min_input_files;

SELECT COUNT(*) FROM cb_dml;

DROP TABLE cb_dml;

-- ============================================================
-- Test 4: Namespace operations (exercises iceberg_catalog_fdw namespace paths)
-- ============================================================
CREATE ICEBERG TABLE cb_ns_test (id bigint, val text)
OPTIONS (namespace 'test_ns');
INSERT INTO cb_ns_test VALUES (1, 'ns_test');
SELECT * FROM cb_ns_test;
DROP TABLE cb_ns_test;

-- ============================================================
-- Test 5: Table with OPTIONS variations (exercises datalake_option parsing)
-- ============================================================
CREATE ICEBERG TABLE cb_opts1 (id bigint)
OPTIONS (table_name 'custom_name_1');
INSERT INTO cb_opts1 VALUES (1);
SELECT * FROM cb_opts1;
DROP TABLE cb_opts1;

CREATE ICEBERG TABLE cb_opts2 (id bigint, val text)
OPTIONS (base_location '/custom_base/opts2/');
INSERT INTO cb_opts2 VALUES (1, 'base_loc_test');
SELECT * FROM cb_opts2;
DROP TABLE cb_opts2;

-- ============================================================
-- Test 6: Large batch for readPolicy block distribution
-- ============================================================
CREATE ICEBERG TABLE cb_large (id bigint, data text, val decimal(10,2));

-- Multiple inserts to create multiple files
INSERT INTO cb_large SELECT i, repeat('d', 50), (i * 0.1)::decimal(10,2) FROM generate_series(1, 500) i;
INSERT INTO cb_large SELECT i, repeat('e', 50), (i * 0.2)::decimal(10,2) FROM generate_series(501, 1000) i;
INSERT INTO cb_large SELECT i, repeat('f', 50), (i * 0.3)::decimal(10,2) FROM generate_series(1001, 1500) i;

-- Full scan (exercises readPolicy block distribution across segments)
SELECT COUNT(*) FROM cb_large;
SELECT MIN(id), MAX(id) FROM cb_large;
SELECT COUNT(*) FROM cb_large WHERE id > 1000;
SELECT COUNT(*) FROM cb_large WHERE val > 100.00;

-- LIMIT queries (exercises early termination)
SELECT * FROM cb_large ORDER BY id LIMIT 5;
SELECT * FROM cb_large ORDER BY id DESC LIMIT 5;

DROP TABLE cb_large;

-- ============================================================
-- Test 7: Error handling (exercises datalake_option validation)
-- ============================================================

-- Invalid option values (caught by validation)
DO $$
BEGIN
    EXECUTE 'CREATE SERVER cb_bad_server FOREIGN DATA WRAPPER iceberg_volume_fdw OPTIONS (type ''invalid_protocol'')';
    RAISE NOTICE 'Unexpected success';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', substring(SQLERRM from 1 for 80);
END;
$$;

-- ============================================================
-- Test 8: TRUNCATE then re-insert (exercises am_handler truncate)
-- ============================================================
CREATE ICEBERG TABLE cb_trunc (id bigint, val text);
INSERT INTO cb_trunc SELECT i, 'before_' || i FROM generate_series(1, 50) i;
SELECT COUNT(*) FROM cb_trunc;

TRUNCATE cb_trunc;
SELECT COUNT(*) FROM cb_trunc;

INSERT INTO cb_trunc SELECT i, 'after_' || i FROM generate_series(1, 20) i;
SELECT COUNT(*) FROM cb_trunc;
SELECT * FROM cb_trunc WHERE id <= 3 ORDER BY id, val;

DROP TABLE cb_trunc;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME cb_volume;
DROP USER MAPPING FOR current_user SERVER cb_volume_server;
DROP SERVER cb_volume_server;
DROP CATALOG cb_catalog;
DROP USER MAPPING FOR current_user SERVER cb_catalog_server;
DROP SERVER cb_catalog_server;
DROP FUNCTION IF EXISTS __test_exec;
