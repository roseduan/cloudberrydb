-- Hive Iceberg FDW Coverage Test
-- Purpose: Trigger dlproxy Iceberg FDW path (NOT AM path)
-- Target: dlproxy/icebergConfig.c (+112), dlproxy/iceberg.c (+60),
--         dlproxy/protocol.c internal_get_external_fragments,
--         dlproxy/iceberg_fragment_cache.c (+42),
--         dlproxy/headers.c add_tuple_desc, dlproxy/libchurl.c GET path

SET client_min_messages = ERROR;
DROP FOREIGN TABLE IF EXISTS iceberg_fdw_test;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;
-- Keep the CREATE EXTENSION calls under client_min_messages=ERROR so the
-- "extension already exists, skipping" NOTICE is suppressed. In the full
-- REGRESS order hive_smoke runs first and creates the extensions, so without
-- this the NOTICE appears here and diverges from the expected output.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
RESET client_min_messages;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SELECT public.create_foreign_server('hive_server', 'gpadmin', 'datalake_fdw', 'paa_cluster');
SET datestyle = ISO, MDY;

-- ============================================================
-- Test 1: Create Iceberg foreign table via manual definition
-- This goes through dlproxy path (not AM path)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS iceberg_fdw_test;
CREATE FOREIGN TABLE iceberg_fdw_test (
    id bigint,
    name text,
    amount double precision,
    ts bigint
)
SERVER hive_server
OPTIONS (
    filePath 'default.iceberg_fdw_test',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.iceberg_fdw_test',
    format 'iceberg'
);

-- Basic scan (triggers iceberg_get_external_fragments -> internal_get_external_fragments
-- -> icebergConfig.c getIcebergConfigV1JsonString -> protocol.c datalakeDoRPC)
SELECT COUNT(*) FROM iceberg_fdw_test;

-- Full scan
SELECT * FROM iceberg_fdw_test ORDER BY id;

-- WHERE filters (triggers headers.c filter serialization path)
SELECT * FROM iceberg_fdw_test WHERE id > 5 ORDER BY id;
SELECT * FROM iceberg_fdw_test WHERE amount > 200.0 ORDER BY id;
SELECT * FROM iceberg_fdw_test WHERE name = 'Alice';
SELECT COUNT(*) FROM iceberg_fdw_test WHERE id <= 3;
SELECT COUNT(*) FROM iceberg_fdw_test WHERE id BETWEEN 2 AND 6;

-- Column projection (triggers headers.c add_projection_desc)
SELECT id FROM iceberg_fdw_test ORDER BY id;
SELECT name, amount FROM iceberg_fdw_test WHERE id <= 3 ORDER BY name;

-- Aggregates
SELECT COUNT(*), SUM(amount), AVG(amount) FROM iceberg_fdw_test;
SELECT MIN(id), MAX(id) FROM iceberg_fdw_test;

-- ============================================================
-- Test 2: Fragment cache (dlproxy/iceberg_fragment_cache.c)
-- ============================================================
SET datalake.enable_iceberg_fragment_cache = on;

-- First query: cache miss -> store
SELECT COUNT(*) FROM iceberg_fdw_test;

-- Second query: cache hit
SELECT COUNT(*) FROM iceberg_fdw_test;

-- Third query with filter: cache hit (fragments same)
SELECT * FROM iceberg_fdw_test WHERE id = 1;

SET datalake.enable_iceberg_fragment_cache = off;

-- ============================================================
-- Test 3: Multiple reads (protocol.c connection lifecycle)
-- ============================================================
SELECT COUNT(*) FROM iceberg_fdw_test;
SELECT * FROM iceberg_fdw_test WHERE id = 1;
SELECT COUNT(*) FROM iceberg_fdw_test WHERE amount > 100;
SELECT id, name FROM iceberg_fdw_test ORDER BY id LIMIT 3;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE IF EXISTS iceberg_fdw_test;
