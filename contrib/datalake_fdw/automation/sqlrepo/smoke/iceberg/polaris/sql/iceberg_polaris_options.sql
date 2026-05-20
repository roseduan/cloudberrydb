-- Iceberg Polaris Catalog Options Test
-- Purpose: Exercise Polaris-specific option parsing paths
-- Target: iceberg_catalog_option.c (parsePolarisCatalogServerOptions,
--         parsePolarisUserMappingOptions, checkIsPolarisCatalog),
--         getIcebergPolarisCatalogOptions(), skip_create_polaris_catalog GUC

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- ============================================================
-- Test 1: Polaris catalog server with type + url
-- ============================================================
CREATE SERVER polopt_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'polaris',
    url 'http://polaris:8181/api/catalog'
);

-- ============================================================
-- Test 2: Polaris user mapping with OAuth credentials
-- ============================================================
CREATE USER MAPPING FOR current_user
SERVER polopt_catalog_server
OPTIONS (
    client_id 'root',
    client_secret 's3cr3t',
    scope 'PRINCIPAL_ROLE:ALL'
);

-- ============================================================
-- Test 3: skip_create_polaris_catalog GUC
-- ============================================================
SHOW datalake.skip_create_polaris_catalog;

-- Enable skip to avoid actual Polaris API call
SET datalake.skip_create_polaris_catalog = on;
SHOW datalake.skip_create_polaris_catalog;

-- Create catalog with skip enabled (should not call Polaris)
CREATE FOREIGN CATALOG polopt_catalog_skipped
SERVER polopt_catalog_server
OPTIONS (
    catalog_name 'polaris_skipped_catalog'
);

RESET datalake.skip_create_polaris_catalog;

-- ============================================================
-- Test 4: Foreign catalog with warehouse_location_prefix
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN CATALOG polopt_catalog_full
        SERVER polopt_catalog_server
        OPTIONS (
            catalog_name ''polaris_default_catalog'',
            default_namespace ''public'',
            enable_metadata_cache ''true'',
            metadata_cache_ttl ''300'',
            auto_refresh_metadata ''true'',
            warehouse_location_prefix ''s3://warehouse/polaris/''
        )';
    RAISE NOTICE 'Polaris catalog with full options created';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Polaris catalog creation error (may be expected if Polaris unavailable): %',
        regexp_replace(split_part(SQLERRM, E'\n', 1), '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', 'UUID', 'g');
END;
$$;

-- ============================================================
-- Test 5: Volume + table operations with Polaris
-- ============================================================
CREATE SERVER polopt_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-west-2',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER polopt_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME polopt_volume SERVER polopt_volume_server OPTIONS(base_path '/');
SET iceberg_default_volume = 'polopt_volume';

-- Try table operations (may fail if Polaris unavailable)
SET iceberg_default_catalog = 'polopt_catalog_skipped';
DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE polopt_test (id bigint, name text)
             OPTIONS (namespace ''public'', table_name ''polopt_test'')';
    EXECUTE 'INSERT INTO polopt_test VALUES (1, ''polaris_test'')';

    RAISE NOTICE 'Polaris table creation and insert succeeded';

    EXECUTE 'DROP TABLE polopt_test';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Polaris table operation error (expected if Polaris unavailable): %',
        regexp_replace(split_part(SQLERRM, E'\n', 1), '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', 'UUID', 'g');
END;
$$;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME polopt_volume;
DROP USER MAPPING FOR current_user SERVER polopt_volume_server;
DROP SERVER polopt_volume_server;

DROP CATALOG IF EXISTS polopt_catalog_full;
DROP CATALOG IF EXISTS polopt_catalog_skipped;
DROP USER MAPPING FOR current_user SERVER polopt_catalog_server;
DROP SERVER polopt_catalog_server;
