-- Iceberg Vacuum Extended Test
-- Purpose: Exercise vacuum/compaction paths and deletion queue
-- Target: pg_iceberg_deletion_queue.c, pg_iceberg_catalog.c (statistics, rewrite plan),
--         pg_iceberg_guc.c (vacuum GUC interaction)

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup
CREATE SERVER vacext_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vacext_catalog_server;
CREATE FOREIGN CATALOG vacext_catalog SERVER vacext_catalog_server;
SET iceberg_default_catalog = 'vacext_catalog';

CREATE SERVER vacext_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER vacext_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME vacext_volume SERVER vacext_volume_server OPTIONS(base_path '/vacext_volume/');
SET iceberg_default_volume = 'vacext_volume';

-- ============================================================
-- Test 1: Vacuum with many small files (compaction)
-- ============================================================
SET datalake.iceberg_vacuum_compact_min_input_files = 2;
SET datalake.iceberg_vacuum_rewrite_target_file_size_mb = 512;

CREATE ICEBERG TABLE vacext_compact (id bigint, val text);

-- Each INSERT creates a separate data file
INSERT INTO vacext_compact VALUES (1, 'row1');
INSERT INTO vacext_compact VALUES (2, 'row2');
INSERT INTO vacext_compact VALUES (3, 'row3');
INSERT INTO vacext_compact VALUES (4, 'row4');
INSERT INTO vacext_compact VALUES (5, 'row5');
INSERT INTO vacext_compact VALUES (6, 'row6');

SELECT COUNT(*) AS before_vacuum FROM vacext_compact;

-- Statistics before vacuum
SELECT
    result::json->'fragments'->>'total-records' AS total_records,
    (result::json->'fragments'->>'total-files-size')::bigint > 0 AS has_file_size
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'vacext_compact',
        'vacext_catalog_server', 'vacext_catalog',
        'vacext_volume_server', 'vacext_volume', '') AS result
) t;

-- VACUUM should compact the small files
VACUUM vacext_compact;

-- Data should be intact
SELECT COUNT(*) AS after_vacuum FROM vacext_compact;
SELECT * FROM vacext_compact ORDER BY id;

-- Statistics after vacuum
SELECT
    result::json->'fragments'->>'total-records' AS total_records,
    (result::json->'fragments'->>'total-files-size')::bigint > 0 AS has_file_size
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'get_statistics', 'public', 'vacext_compact',
        'vacext_catalog_server', 'vacext_catalog',
        'vacext_volume_server', 'vacext_volume', '') AS result
) t;

DROP TABLE vacext_compact;

-- ============================================================
-- Test 2: Vacuum on empty table
-- ============================================================
CREATE ICEBERG TABLE vacext_empty (id bigint);
VACUUM vacext_empty;
SELECT COUNT(*) FROM vacext_empty;
DROP TABLE vacext_empty;

-- ============================================================
-- Test 3: Vacuum after DELETE (position deletes)
-- ============================================================
CREATE ICEBERG TABLE vacext_del (id bigint, name text);

INSERT INTO vacext_del VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO vacext_del VALUES (6, 'f'), (7, 'g'), (8, 'h');

-- Delete some rows (creates position delete files)
DELETE FROM vacext_del WHERE id IN (2, 4, 6);

SELECT COUNT(*) AS after_delete FROM vacext_del;

-- VACUUM should clean up position delete files
VACUUM vacext_del;

-- Data should be consistent
SELECT COUNT(*) AS after_vacuum FROM vacext_del;
SELECT * FROM vacext_del ORDER BY id;

DROP TABLE vacext_del;

-- ============================================================
-- Test 4: Multiple DELETE + VACUUM cycles
-- ============================================================
CREATE ICEBERG TABLE vacext_cycles (id bigint, val int);

INSERT INTO vacext_cycles VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);

-- Cycle 1: delete + vacuum
DELETE FROM vacext_cycles WHERE id = 1;
VACUUM vacext_cycles;
SELECT COUNT(*) FROM vacext_cycles;

-- Cycle 2: insert + delete + vacuum
INSERT INTO vacext_cycles VALUES (6, 60), (7, 70);
DELETE FROM vacext_cycles WHERE id IN (3, 5);
VACUUM vacext_cycles;
SELECT COUNT(*) FROM vacext_cycles;

SELECT * FROM vacext_cycles ORDER BY id;

DROP TABLE vacext_cycles;

-- ============================================================
-- Test 5: DROP TABLE triggers deletion queue
-- ============================================================
CREATE ICEBERG TABLE vacext_drop_queue (id bigint, val text);
INSERT INTO vacext_drop_queue VALUES (1, 'to_be_dropped');
INSERT INTO vacext_drop_queue VALUES (2, 'also_dropped');

-- Verify data exists
SELECT COUNT(*) FROM vacext_drop_queue;

-- DROP TABLE should enqueue deletion of data files
DROP TABLE vacext_drop_queue;

-- Table should no longer exist
DO $$
BEGIN
    PERFORM COUNT(*) FROM vacext_drop_queue;
    RAISE NOTICE 'Unexpected: table still exists after DROP';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Table correctly dropped: %', SQLERRM;
END;
$$;

-- ============================================================
-- Test 6: plan_file_groups with custom parameters
-- ============================================================
CREATE ICEBERG TABLE vacext_plan (id bigint, val text);
INSERT INTO vacext_plan VALUES (1, 'a');
INSERT INTO vacext_plan VALUES (2, 'b');
INSERT INTO vacext_plan VALUES (3, 'c');

-- Default parameters
SELECT
    result::json->>'status' AS status,
    result::json->>'operation' AS operation
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'vacext_plan',
        'vacext_catalog_server', 'vacext_catalog',
        'vacext_volume_server', 'vacext_volume', '') AS result
) t;

-- Custom parameters
SELECT
    result::json->>'status' AS status
FROM (
    SELECT iceberg_toolkit.catalog_fdw(
        'plan_file_groups', 'public', 'vacext_plan',
        'vacext_catalog_server', 'vacext_catalog',
        'vacext_volume_server', 'vacext_volume',
        '{"minInputFiles": 2, "targetFileSizeMb": 256}') AS result
) t;

DROP TABLE vacext_plan;

RESET datalake.iceberg_vacuum_compact_min_input_files;
RESET datalake.iceberg_vacuum_rewrite_target_file_size_mb;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME vacext_volume;
DROP USER MAPPING FOR current_user SERVER vacext_volume_server;
DROP SERVER vacext_volume_server;
DROP CATALOG vacext_catalog;
DROP USER MAPPING FOR current_user SERVER vacext_catalog_server;
DROP SERVER vacext_catalog_server;
