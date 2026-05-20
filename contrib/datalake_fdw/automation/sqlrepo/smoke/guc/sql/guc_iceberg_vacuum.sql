-- GUC Iceberg Vacuum Parameters Test
-- Purpose: Cover all vacuum-related GUCs in pg_iceberg_guc.c
-- and remaining untested GUCs from datalake_fdw.c
-- Target: pg_iceberg_guc.c (full coverage), datalake_fdw.c (remaining GUCs)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- ============================================================
-- Setup: Iceberg builtin catalog + S3 volume (needed for some GUC tests)
-- ============================================================
CREATE SERVER guc_vac_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER guc_vac_catalog_server;
CREATE FOREIGN CATALOG guc_vac_catalog SERVER guc_vac_catalog_server;
SET iceberg_default_catalog = 'guc_vac_catalog';

CREATE SERVER guc_vac_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER guc_vac_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME guc_vac_volume SERVER guc_vac_volume_server OPTIONS(base_path '/guc_vac_volume/');
SET iceberg_default_volume = 'guc_vac_volume';

-- ============================================================
-- Test 1: datalake.iceberg_autovacuum (PGC_SIGHUP - cannot SET in session)
-- ============================================================
SHOW datalake.iceberg_autovacuum;

-- Should error: PGC_SIGHUP context
DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_autovacuum = off';
    RAISE NOTICE 'Unexpected: SET succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on SET iceberg_autovacuum: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 2: datalake.iceberg_autovacuum_naptime (PGC_SIGHUP)
-- ============================================================
SHOW datalake.iceberg_autovacuum_naptime;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_autovacuum_naptime = 300';
    RAISE NOTICE 'Unexpected: SET succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on SET iceberg_autovacuum_naptime: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 3: datalake.iceberg_log_autovacuum_min_duration (PGC_SIGHUP)
-- ============================================================
SHOW datalake.iceberg_log_autovacuum_min_duration;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_log_autovacuum_min_duration = 1000';
    RAISE NOTICE 'Unexpected: SET succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on SET iceberg_log_autovacuum_min_duration: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 4: datalake.iceberg_max_snapshot_age (PGC_SUSET)
-- ============================================================
SHOW datalake.iceberg_max_snapshot_age;

-- PGC_SUSET: only superuser can SET
DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_max_snapshot_age = 86400';
    RAISE NOTICE 'SET iceberg_max_snapshot_age succeeded (superuser)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error on SET iceberg_max_snapshot_age: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 5: datalake.iceberg_vacuum_compact_min_input_files (PGC_USERSET)
-- ============================================================
SHOW datalake.iceberg_vacuum_compact_min_input_files;

SET datalake.iceberg_vacuum_compact_min_input_files = 2;
SHOW datalake.iceberg_vacuum_compact_min_input_files;

SET datalake.iceberg_vacuum_compact_min_input_files = 10;
SHOW datalake.iceberg_vacuum_compact_min_input_files;

RESET datalake.iceberg_vacuum_compact_min_input_files;
SHOW datalake.iceberg_vacuum_compact_min_input_files;

-- ============================================================
-- Test 6: datalake.iceberg_max_file_removals_per_vacuum (PGC_SUSET)
-- ============================================================
SHOW datalake.iceberg_max_file_removals_per_vacuum;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_max_file_removals_per_vacuum = 500';
    RAISE NOTICE 'SET succeeded (superuser)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 7: datalake.iceberg_max_compactions_per_vacuum (PGC_SUSET)
-- ============================================================
SHOW datalake.iceberg_max_compactions_per_vacuum;

DO $$
BEGIN
    EXECUTE 'SET datalake.iceberg_max_compactions_per_vacuum = 50';
    RAISE NOTICE 'SET succeeded (superuser)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected error: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 8: datalake.iceberg_vacuum_rewrite_target_file_size_mb (PGC_USERSET)
-- ============================================================
SHOW datalake.iceberg_vacuum_rewrite_target_file_size_mb;

SET datalake.iceberg_vacuum_rewrite_target_file_size_mb = 256;
SHOW datalake.iceberg_vacuum_rewrite_target_file_size_mb;

SET datalake.iceberg_vacuum_rewrite_target_file_size_mb = 1024;
SHOW datalake.iceberg_vacuum_rewrite_target_file_size_mb;

RESET datalake.iceberg_vacuum_rewrite_target_file_size_mb;
SHOW datalake.iceberg_vacuum_rewrite_target_file_size_mb;

-- ============================================================
-- Test 9: Remaining untested GUCs from datalake_fdw.c
-- ============================================================

-- datalake.external_table_new_text
SHOW datalake.external_table_new_text;
SET datalake.external_table_new_text = on;
SHOW datalake.external_table_new_text;
SET datalake.external_table_new_text = off;

-- datalake.enable_set_hdfs_user
SHOW datalake.enable_set_hdfs_user;
SET datalake.enable_set_hdfs_user = on;
SHOW datalake.enable_set_hdfs_user;
SET datalake.enable_set_hdfs_user = off;

-- datalake.enable_get_block_location
SHOW datalake.enable_get_block_location;
SET datalake.enable_get_block_location = on;
SHOW datalake.enable_get_block_location;
SET datalake.enable_get_block_location = off;

-- datalake.agent_server_url
SHOW datalake.agent_server_url;
SET datalake.agent_server_url = 'http://custom-agent:8080';
SHOW datalake.agent_server_url;
RESET datalake.agent_server_url;

-- datalake.skip_create_polaris_catalog
SHOW datalake.skip_create_polaris_catalog;
SET datalake.skip_create_polaris_catalog = on;
SHOW datalake.skip_create_polaris_catalog;
SET datalake.skip_create_polaris_catalog = off;

-- datalake.enable_iceberg_fragment_cache
SHOW datalake.enable_iceberg_fragment_cache;
SET datalake.enable_iceberg_fragment_cache = off;
SHOW datalake.enable_iceberg_fragment_cache;
SET datalake.enable_iceberg_fragment_cache = on;

-- ============================================================
-- Test 10: Functional verification with vacuum GUCs
-- ============================================================
CREATE ICEBERG TABLE guc_vac_test (id bigint, val text);

SET datalake.iceberg_vacuum_compact_min_input_files = 2;
SET datalake.iceberg_vacuum_rewrite_target_file_size_mb = 256;

INSERT INTO guc_vac_test VALUES (1, 'a');
INSERT INTO guc_vac_test VALUES (2, 'b');
INSERT INTO guc_vac_test VALUES (3, 'c');

SELECT COUNT(*) FROM guc_vac_test;

-- VACUUM with custom GUC values
VACUUM guc_vac_test;

-- Data should be intact
SELECT COUNT(*) FROM guc_vac_test;
SELECT * FROM guc_vac_test ORDER BY id;

RESET datalake.iceberg_vacuum_compact_min_input_files;
RESET datalake.iceberg_vacuum_rewrite_target_file_size_mb;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE guc_vac_test;
DROP VOLUME guc_vac_volume;
DROP USER MAPPING FOR current_user SERVER guc_vac_volume_server;
DROP SERVER guc_vac_volume_server;
DROP CATALOG guc_vac_catalog;
DROP USER MAPPING FOR current_user SERVER guc_vac_catalog_server;
DROP SERVER guc_vac_catalog_server;
