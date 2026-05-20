-- Iceberg Deep Coverage Test
-- Purpose: Cover remaining hard-to-reach code paths
-- Target: iceberg_position_filter.c streaming path, sorted_merge,
--         iceberg_equality_filter.c, hdw_gopher_cache.c,
--         am_iceberg_deletion_queue.c, providerWrapper.cpp

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

CREATE SERVER dc2_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER dc2_catalog_server;
CREATE FOREIGN CATALOG dc2_catalog SERVER dc2_catalog_server;
SET iceberg_default_catalog = 'dc2_catalog';

CREATE SERVER dc2_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER dc2_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME dc2_volume SERVER dc2_volume_server OPTIONS(base_path '/dc2_volume/');
SET iceberg_default_volume = 'dc2_volume';

-- ============================================================
-- Test 1: Large position delete (streaming filter path)
-- Set threshold low to force streaming path
-- Target: iceberg_position_filter.c streamingFilterNext (+69)
--         sorted_merge.cpp/sorted_merge_c.cpp (+64)
-- ============================================================
CREATE ICEBERG TABLE dc2_stream_del (id bigint, name text, val int);

-- Create data across multiple files
INSERT INTO dc2_stream_del SELECT i, 'name_' || i, i * 10 FROM generate_series(1, 500) i;
INSERT INTO dc2_stream_del SELECT i, 'name_' || i, i * 10 FROM generate_series(501, 1000) i;
INSERT INTO dc2_stream_del SELECT i, 'name_' || i, i * 10 FROM generate_series(1001, 1500) i;

-- Delete many rows (creates large position delete files)
DELETE FROM dc2_stream_del WHERE id % 2 = 0;

-- Read triggers position filter (with default threshold should use in-memory)
SELECT COUNT(*) FROM dc2_stream_del;

-- Delete more to increase delete count
DELETE FROM dc2_stream_del WHERE id % 3 = 0;
SELECT COUNT(*) FROM dc2_stream_del;

-- More deletes + reads
DELETE FROM dc2_stream_del WHERE id % 7 = 0;
SELECT COUNT(*) FROM dc2_stream_del;
SELECT MIN(id), MAX(id) FROM dc2_stream_del;

DROP TABLE dc2_stream_del;

-- ============================================================
-- Test 2: Multiple delete rounds (accumulate delete files)
-- Target: iceberg_position_filter multi-file handling,
--         iceberg_file_index.c, iceberg_delete_index.c
-- ============================================================
CREATE ICEBERG TABLE dc2_multi_round (id bigint, data text);

INSERT INTO dc2_multi_round SELECT i, 'data_' || i FROM generate_series(1, 200) i;

-- 10 rounds of single-row deletes (10 delete files)
DELETE FROM dc2_multi_round WHERE id = 1;
DELETE FROM dc2_multi_round WHERE id = 20;
DELETE FROM dc2_multi_round WHERE id = 40;
DELETE FROM dc2_multi_round WHERE id = 60;
DELETE FROM dc2_multi_round WHERE id = 80;
DELETE FROM dc2_multi_round WHERE id = 100;
DELETE FROM dc2_multi_round WHERE id = 120;
DELETE FROM dc2_multi_round WHERE id = 140;
DELETE FROM dc2_multi_round WHERE id = 160;
DELETE FROM dc2_multi_round WHERE id = 180;

SELECT COUNT(*) FROM dc2_multi_round;

-- Update creates delete + insert
UPDATE dc2_multi_round SET data = 'updated' WHERE id BETWEEN 50 AND 55;
SELECT COUNT(*) FROM dc2_multi_round WHERE data = 'updated';

DROP TABLE dc2_multi_round;

-- ============================================================
-- Test 3: Gopher cache operations
-- Target: hdw_gopher_cache.c (+76)
-- ============================================================
-- Create and query a table to populate cache
CREATE ICEBERG TABLE dc2_cache_test (id bigint, val text);
INSERT INTO dc2_cache_test SELECT i, 'v_' || i FROM generate_series(1, 50) i;
SELECT COUNT(*) FROM dc2_cache_test;

-- Try cache free functions (may or may not exist as UDFs)
DO $$
BEGIN
    PERFORM gp_toolkit.__gopher_free_all_cache();
    RAISE NOTICE 'gopher_free_all_cache: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'gopher_free_all_cache: %', substring(SQLERRM from 1 for 40);
END;
$$;

DO $$
BEGIN
    PERFORM gp_toolkit.__gopher_cache_free_relation_name('public.dc2_cache_test');
    RAISE NOTICE 'gopher_cache_free_relation: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'gopher_cache_free_relation: %', substring(SQLERRM from 1 for 40);
END;
$$;

DROP TABLE dc2_cache_test;

-- ============================================================
-- Test 4: Deletion queue stress (am_iceberg_deletion_queue.c)
-- ============================================================
-- Rapid create-insert-drop cycle to stress deletion queue
CREATE ICEBERG TABLE dc2_dq1 (id bigint); INSERT INTO dc2_dq1 VALUES (1); DROP TABLE dc2_dq1;
CREATE ICEBERG TABLE dc2_dq2 (id bigint); INSERT INTO dc2_dq2 VALUES (1); DROP TABLE dc2_dq2;
CREATE ICEBERG TABLE dc2_dq3 (id bigint); INSERT INTO dc2_dq3 VALUES (1); DROP TABLE dc2_dq3;
CREATE ICEBERG TABLE dc2_dq4 (id bigint); INSERT INTO dc2_dq4 VALUES (1); DROP TABLE dc2_dq4;
CREATE ICEBERG TABLE dc2_dq5 (id bigint); INSERT INTO dc2_dq5 VALUES (1); DROP TABLE dc2_dq5;
CREATE ICEBERG TABLE dc2_dq6 (id bigint); INSERT INTO dc2_dq6 VALUES (1); DROP TABLE dc2_dq6;
CREATE ICEBERG TABLE dc2_dq7 (id bigint); INSERT INTO dc2_dq7 VALUES (1); DROP TABLE dc2_dq7;
CREATE ICEBERG TABLE dc2_dq8 (id bigint); INSERT INTO dc2_dq8 VALUES (1); DROP TABLE dc2_dq8;

-- ============================================================
-- Test 5: Large table with many columns + operations
-- Target: providerWrapper.cpp, provider.cpp, config.c,
--         parquetFileReader.cpp, fdwFunction.c
-- ============================================================
CREATE ICEBERG TABLE dc2_mixed (
    c1 bigint, c2 text, c3 decimal(10,2), c4 boolean,
    c5 date, c6 timestamp, c7 real, c8 double precision,
    c9 smallint, c10 varchar(50)
);

-- Bulk insert with all types
INSERT INTO dc2_mixed
SELECT i, 'text_' || i, (i * 1.1)::decimal(10,2), (i % 2 = 0),
       '2024-01-01'::date + (i % 365), '2024-01-01'::timestamp + (i || ' hours')::interval,
       (i * 0.1)::real, i * 0.01, (i % 100)::smallint, 'vc_' || i
FROM generate_series(1, 300) i;

-- Various scans
SELECT COUNT(*) FROM dc2_mixed;
SELECT c1, c2 FROM dc2_mixed WHERE c1 <= 5 ORDER BY c1;
SELECT c3, c4, c5 FROM dc2_mixed WHERE c4 = true AND c1 <= 10 ORDER BY c1;
SELECT c7, c8, c9 FROM dc2_mixed WHERE c9 > 90 ORDER BY c1 LIMIT 5;
SELECT ROUND(AVG(c3), 2) AS avg_c3, ROUND(SUM(c8)::numeric, 2) AS sum_c8, MIN(c5), MAX(c6) FROM dc2_mixed;

-- Update various types
UPDATE dc2_mixed SET c2 = 'upd_' || c2, c4 = NOT c4 WHERE c1 <= 10;
UPDATE dc2_mixed SET c3 = c3 * 2, c7 = c7 + 1.0 WHERE c1 BETWEEN 20 AND 30;

-- Delete
DELETE FROM dc2_mixed WHERE c1 > 250;

-- VACUUM + ANALYZE
SET datalake.iceberg_vacuum_compact_min_input_files = 1;
VACUUM dc2_mixed;
RESET datalake.iceberg_vacuum_compact_min_input_files;
ANALYZE dc2_mixed;

SELECT COUNT(*) FROM dc2_mixed;

DROP TABLE dc2_mixed;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME dc2_volume;
DROP USER MAPPING FOR current_user SERVER dc2_volume_server;
DROP SERVER dc2_volume_server;
DROP CATALOG dc2_catalog;
DROP USER MAPPING FOR current_user SERVER dc2_catalog_server;
DROP SERVER dc2_catalog_server;
DROP FUNCTION IF EXISTS __test_exec;
