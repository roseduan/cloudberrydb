-- Extended Negative Tests for Iceberg
-- Purpose: Exercise error handling paths in catalog/volume options, GUC boundaries
-- Target: iceberg_catalog_option.c, iceberg_volume_option.c, pg_iceberg_options.c,
--         datalake_fdw.c (GUC boundary validation)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- ============================================================
-- Test 1: Missing iceberg_default_catalog (empty string)
-- ============================================================
DO $$
BEGIN
    EXECUTE 'SET iceberg_default_catalog = ''''';
    EXECUTE 'SET iceberg_default_volume = ''''';
    EXECUTE 'CREATE ICEBERG TABLE neg_no_catalog (id bigint)';
    RAISE NOTICE 'Unexpected: CREATE succeeded without catalog';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (no catalog): %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 2: Setup catalog but missing volume
-- ============================================================
CREATE SERVER neg_ext_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER neg_ext_catalog_server;
CREATE FOREIGN CATALOG neg_ext_catalog SERVER neg_ext_catalog_server;
SET iceberg_default_catalog = 'neg_ext_catalog';

DO $$
BEGIN
    EXECUTE 'SET iceberg_default_volume = ''''';
    EXECUTE 'CREATE ICEBERG TABLE neg_no_volume (id bigint)';
    RAISE NOTICE 'Unexpected: CREATE succeeded without volume';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (no volume): %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 3: GUC boundary errors
-- ============================================================

-- datalake.iceberg_postion_deletes_threshold: valid range 100000..10000000
DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_postion_deletes_threshold = 99';
    RAISE NOTICE 'Unexpected: SET succeeded with value below min';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (threshold too low): %', SQLERRM;
END;
$$;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_postion_deletes_threshold = 99999999';
    RAISE NOTICE 'Unexpected: SET succeeded with value above max';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (threshold too high): %', SQLERRM;
END;
$$;

-- datalake.hudi_log_merger_threshold: valid range 128..INT_MAX
DO $$
BEGIN
    EXECUTE 'SET datalake.hudi_log_merger_threshold = 1';
    RAISE NOTICE 'Unexpected: SET succeeded with value below min';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (merger threshold too low): %', SQLERRM;
END;
$$;

-- datalake.hudi_log_scale_factor: valid range 1..10
DO $$
BEGIN
    EXECUTE 'SET datalake.hudi_log_scale_factor = 0.5';
    RAISE NOTICE 'Unexpected: SET succeeded with value below min';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (scale factor too low): %', SQLERRM;
END;
$$;

DO $$
BEGIN
    EXECUTE 'SET datalake.hudi_log_scale_factor = 100';
    RAISE NOTICE 'Unexpected: SET succeeded with value above max';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (scale factor too high): %', SQLERRM;
END;
$$;

-- datalake.external_table_limit_segment_num: valid range 0..INT_MAX
-- Setting to negative should error
DO $$
BEGIN
    EXECUTE 'SET datalake.external_table_limit_segment_num = -1';
    RAISE NOTICE 'Unexpected: SET succeeded with negative value';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (negative segment num): %', SQLERRM;
END;
$$;

-- datalake.iceberg_vacuum_compact_min_input_files: valid range 1..INT_MAX
-- Setting to 0 should error
DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_vacuum_compact_min_input_files = 0';
    RAISE NOTICE 'Unexpected: SET succeeded with value below min';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (compact min files = 0): %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 4: Invalid GUC type (string into bool)
-- ============================================================
DO $$
BEGIN
    EXECUTE 'SET datalake.disable_filter_pushdown = ''not_a_bool''';
    RAISE NOTICE 'Unexpected: SET succeeded with invalid bool';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (invalid bool): %', SQLERRM;
END;
$$;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_postion_deletes_threshold = ''not_a_number''';
    RAISE NOTICE 'Unexpected: SET succeeded with non-numeric value';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (non-numeric threshold): %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 5: Setup volume for iceberg negative tests
-- ============================================================
CREATE SERVER neg_ext_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER neg_ext_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME neg_ext_volume SERVER neg_ext_volume_server OPTIONS(base_path '/neg_ext_volume/');
SET iceberg_default_volume = 'neg_ext_volume';

-- ============================================================
-- Test 6: INSERT column count mismatch
-- ============================================================
CREATE ICEBERG TABLE neg_col_mismatch (id bigint, name text);

DO $$
BEGIN
    EXECUTE 'INSERT INTO neg_col_mismatch VALUES (1, ''Alice'', ''extra'')';
    RAISE NOTICE 'Unexpected: INSERT with extra column succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (column mismatch): %', SQLERRM;
END;
$$;

-- Valid insert should still work
INSERT INTO neg_col_mismatch VALUES (1, 'Alice');
SELECT * FROM neg_col_mismatch ORDER BY id;

DROP TABLE neg_col_mismatch;

-- ============================================================
-- Test 7: DROP non-existent CATALOG / VOLUME with IF EXISTS
-- ============================================================
DROP CATALOG IF EXISTS neg_nonexistent_catalog_xyz;
DROP VOLUME IF EXISTS neg_nonexistent_volume_xyz;
DROP TABLE IF EXISTS neg_nonexistent_table_xyz;

-- ============================================================
-- Test 8: INSERT type mismatch (text into bigint)
-- ============================================================
CREATE ICEBERG TABLE neg_type_err (id bigint, val int);

DO $$
BEGIN
    EXECUTE 'INSERT INTO neg_type_err VALUES (''not_a_number'', 1)';
    RAISE NOTICE 'Unexpected: INSERT with type mismatch succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (type mismatch): %', SQLERRM;
END;
$$;

-- Valid insert should work after error
INSERT INTO neg_type_err VALUES (1, 42);
SELECT * FROM neg_type_err ORDER BY id;

DROP TABLE neg_type_err;

-- ============================================================
-- Test 9: Duplicate table creation
-- ============================================================
CREATE ICEBERG TABLE neg_dup_table (id bigint);

DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE neg_dup_table (id bigint)';
    RAISE NOTICE 'Unexpected: duplicate CREATE succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error (duplicate table): %', SQLERRM;
END;
$$;

-- IF NOT EXISTS should not error
CREATE ICEBERG TABLE IF NOT EXISTS neg_dup_table (id bigint);

DROP TABLE neg_dup_table;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME neg_ext_volume;
DROP USER MAPPING FOR current_user SERVER neg_ext_volume_server;
DROP SERVER neg_ext_volume_server;
DROP CATALOG neg_ext_catalog;
DROP USER MAPPING FOR current_user SERVER neg_ext_catalog_server;
DROP SERVER neg_ext_catalog_server;
