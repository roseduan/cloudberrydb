-- Iceberg DDL and Data Types Test
-- Purpose: Exercise all column type mappings and DDL option paths
-- Target: pg_iceberg_ddl.c, pg_iceberg_options.c (set_option_value),
--         mapPostgresToIcebergType(), CREATE/DROP TABLE paths

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

-- catalog + volume setup
CREATE SERVER types_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER types_catalog_server;
CREATE FOREIGN CATALOG types_catalog SERVER types_catalog_server;
SET iceberg_default_catalog = 'types_catalog';

CREATE SERVER types_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER types_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME types_volume SERVER types_volume_server OPTIONS(base_path '/types_volume/');
SET iceberg_default_volume = 'types_volume';

-- ============================================================
-- Test 1a: All supported Iceberg column types (excluding timestamptz)
-- timestamptz is tested separately in 1b because the parquet write
-- path does not yet handle TIMESTAMPTZOID in writeToField.
-- ============================================================
CREATE ICEBERG TABLE types_all_columns (
    col_bool boolean,
    col_smallint smallint,
    col_int int,
    col_bigint bigint,
    col_real real,
    col_double double precision,
    col_decimal decimal(15,2),
    col_text text,
    col_varchar varchar(100),
    col_date date,
    col_timestamp timestamp
);

-- Insert data for each type
INSERT INTO types_all_columns VALUES (
    true,
    32767,
    2147483647,
    9223372036854775807,
    3.14,
    2.718281828459045,
    9999999999999.99,
    'hello world',
    'varchar test',
    '2024-06-15',
    '2024-06-15 10:30:00'
);

-- Insert with NULLs
INSERT INTO types_all_columns VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL
);

-- Insert with boundary/special values
INSERT INTO types_all_columns VALUES (
    false,
    -32768,
    -2147483648,
    -9223372036854775808,
    -0.0,
    0.0,
    0.00,
    '',
    '',
    '1970-01-01',
    '1970-01-01 00:00:00'
);

SELECT COUNT(*) FROM types_all_columns;

-- Verify types can be read back correctly
SELECT col_bool, col_smallint, col_int, col_bigint FROM types_all_columns
WHERE col_bool IS NOT NULL ORDER BY col_smallint;

SELECT col_real, col_double, col_decimal FROM types_all_columns
WHERE col_real IS NOT NULL ORDER BY col_real;

SELECT col_text, col_varchar FROM types_all_columns
WHERE col_text IS NOT NULL ORDER BY col_text;

SELECT col_date, col_timestamp FROM types_all_columns
WHERE col_date IS NOT NULL ORDER BY col_date;

DROP TABLE types_all_columns;

-- ============================================================
-- Test 1b: Timestamptz column write (expected error)
-- The parquet write path handles TIMESTAMPOID but not TIMESTAMPTZOID
-- in the per-row writeToField switch (parquetFileWriter.cpp).
-- The INSERT fails with "OSS protocol not supported data type".
-- ============================================================
CREATE ICEBERG TABLE types_tstz_only (col_tstz timestamptz);
SELECT __test_exec('INSERT INTO types_tstz_only VALUES (''2024-06-15 10:30:00+08'')');
DROP TABLE types_tstz_only;

-- ============================================================
-- Test 2: Wide table (20+ columns)
-- ============================================================
CREATE ICEBERG TABLE types_wide_table (
    c01 bigint, c02 bigint, c03 bigint, c04 bigint, c05 bigint,
    c06 text, c07 text, c08 text, c09 text, c10 text,
    c11 int, c12 int, c13 int, c14 int, c15 int,
    c16 boolean, c17 boolean, c18 boolean,
    c19 decimal(10,2), c20 decimal(10,2),
    c21 date, c22 timestamp
);

INSERT INTO types_wide_table VALUES (
    1, 2, 3, 4, 5,
    'a', 'b', 'c', 'd', 'e',
    10, 20, 30, 40, 50,
    true, false, true,
    100.50, 200.75,
    '2024-01-01', '2024-01-01 12:00:00'
);

SELECT COUNT(*) FROM types_wide_table;
SELECT c01, c06, c11, c16, c19, c21 FROM types_wide_table;

DROP TABLE types_wide_table;

-- ============================================================
-- Test 3: CREATE ICEBERG TABLE with OPTIONS
-- ============================================================
CREATE ICEBERG TABLE types_with_opts (
    id bigint,
    name text
)
OPTIONS (
    namespace 'test_ddl_ns',
    table_name 'external_name'
);

INSERT INTO types_with_opts VALUES (1, 'opt_test');
SELECT * FROM types_with_opts;
DROP TABLE types_with_opts;

-- ============================================================
-- Test 4: CREATE ICEBERG TABLE with base_location option
-- ============================================================
CREATE ICEBERG TABLE types_with_location (
    id bigint,
    val text
)
OPTIONS (
    base_location '/custom/location/'
);

INSERT INTO types_with_location VALUES (1, 'location_test');
SELECT * FROM types_with_location;
DROP TABLE types_with_location;

-- ============================================================
-- Test 5: CREATE ICEBERG TABLE IF NOT EXISTS
-- ============================================================
CREATE ICEBERG TABLE types_ifne (id bigint);
INSERT INTO types_ifne VALUES (1);

-- Should not error, should not replace existing table
CREATE ICEBERG TABLE IF NOT EXISTS types_ifne (id bigint, extra text);

-- Should still have original schema
SELECT * FROM types_ifne;

DROP TABLE types_ifne;

-- ============================================================
-- Test 6: DROP TABLE IF EXISTS on non-existent table
-- ============================================================
DROP TABLE IF EXISTS types_nonexistent_table_xyz;

-- ============================================================
-- Test 7: Table with single column
-- ============================================================
CREATE ICEBERG TABLE types_single_col (id bigint);
INSERT INTO types_single_col VALUES (1), (2), (3);
SELECT COUNT(*) FROM types_single_col;
DROP TABLE types_single_col;

-- ============================================================
-- Test 8: DROP COLUMN is rejected on Iceberg tables (issue #334)
-- DROP COLUMN used to half-apply (PG catalog updated while Iceberg
-- metadata stayed stale); ALTER COLUMN TYPE crashed the backend.
-- The Iceberg AM now rejects every ALTER subcommand in
-- datalake_ProcessUtility; the table must remain usable afterwards.
-- ============================================================
CREATE ICEBERG TABLE types_drop_col (id int, name text, age int)
    CATALOG types_catalog VOLUME types_volume;
INSERT INTO types_drop_col VALUES (1, 'a', 20), (2, 'b', 30);
ALTER TABLE types_drop_col DROP COLUMN age;
-- Table still has all three columns; INSERT must succeed against the original schema.
INSERT INTO types_drop_col VALUES (3, 'c', 40);
SELECT * FROM types_drop_col ORDER BY id;
DROP TABLE types_drop_col;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME types_volume;
DROP USER MAPPING FOR current_user SERVER types_volume_server;
DROP SERVER types_volume_server;
DROP CATALOG types_catalog;
DROP USER MAPPING FOR current_user SERVER types_catalog_server;
DROP SERVER types_catalog_server;
DROP FUNCTION IF EXISTS __test_exec;
