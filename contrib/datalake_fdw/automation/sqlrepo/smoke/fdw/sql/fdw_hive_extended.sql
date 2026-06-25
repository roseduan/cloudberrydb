-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(hive_connector\.c:\d+\)/
-- s/\(hive_connector\.c:\d+\)/(hive_connector.c:XXX)/
-- end_matchsubs
-- FDW Hive Extended Coverage Test
-- Purpose: Maximize coverage of dlproxy, grammar_convert, logicalType, filters, partition_selector
-- Target: filters.c, protocol.c, libchurl.c, headers.c, grammar_convert.c,
--         logicalType.cpp, partition_selector.c, datalake_option.c, datalake_fdw.c

CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;

DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');

SELECT public.create_foreign_server('hive_server', 'gpadmin', 'datalake_fdw', 'paa_cluster');

SET datestyle = ISO, MDY;

-- ============================================================
-- Test 1: All data types (grammar_convert + logicalType coverage)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_all_types;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_all_types', 'paa_cluster', 'fdw_all_types', 'hive_server');

SELECT * FROM fdw_all_types ORDER BY col_int;
SELECT COUNT(*) FROM fdw_all_types;

-- Type-specific filters (exercises filters.c type branches)
SELECT * FROM fdw_all_types WHERE col_tinyint = 1;
SELECT * FROM fdw_all_types WHERE col_smallint > 100 ORDER BY col_smallint;
SELECT * FROM fdw_all_types WHERE col_int >= 1000 ORDER BY col_int;
SELECT * FROM fdw_all_types WHERE col_bigint < 20000 ORDER BY col_bigint;
SELECT * FROM fdw_all_types WHERE col_float > 2.0;
SELECT * FROM fdw_all_types WHERE col_double <= 3.0 ORDER BY col_double;
SELECT * FROM fdw_all_types WHERE col_decimal > 0 ORDER BY col_decimal;
SELECT * FROM fdw_all_types WHERE col_string = 'hello';
SELECT * FROM fdw_all_types WHERE col_boolean = true;
SELECT * FROM fdw_all_types WHERE col_date > '2024-03-01' ORDER BY col_date;

-- NULL filters
SELECT * FROM fdw_all_types WHERE col_string IS NULL;
SELECT * FROM fdw_all_types WHERE col_string IS NOT NULL ORDER BY col_int;

-- Combined filters (AND, OR)
SELECT * FROM fdw_all_types WHERE col_int > 500 AND col_boolean = true;
SELECT * FROM fdw_all_types WHERE col_tinyint = 1 OR col_tinyint = 2 ORDER BY col_tinyint;

-- NOT equal
SELECT * FROM fdw_all_types WHERE col_string != 'hello' AND col_string IS NOT NULL;

DROP FOREIGN TABLE fdw_all_types;

-- ============================================================
-- Test 2: Multi-partition table (partition_selector extended)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_multi_part;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_multi_part', 'paa_cluster', 'fdw_multi_part', 'hive_server');

-- Full scan
SELECT * FROM fdw_multi_part ORDER BY id;
SELECT COUNT(*) FROM fdw_multi_part;

-- Single partition filter
SELECT * FROM fdw_multi_part WHERE year = 2024 ORDER BY id;
SELECT * FROM fdw_multi_part WHERE month = '01' ORDER BY id;

-- Multi-partition filter
SELECT * FROM fdw_multi_part WHERE year = 2024 AND month = '02' ORDER BY id;
SELECT * FROM fdw_multi_part WHERE year = 2023 ORDER BY id;

-- Partition + data filter combined
SELECT * FROM fdw_multi_part WHERE year = 2024 AND id > 3 ORDER BY id;

-- IN filter on partition key
SELECT * FROM fdw_multi_part WHERE month IN ('01', '03') ORDER BY id;

-- Range filter on partition key
SELECT * FROM fdw_multi_part WHERE year >= 2024 ORDER BY id;

-- Count by partition
SELECT year, month, COUNT(*) FROM fdw_multi_part GROUP BY year, month ORDER BY year, month;

DROP FOREIGN TABLE fdw_multi_part;

-- ============================================================
-- Test 3: ORC with type filters (orcRead + filters.c)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_orc_types;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_orc_types', 'paa_cluster', 'fdw_orc_types', 'hive_server');

SELECT * FROM fdw_orc_types ORDER BY id;
SELECT * FROM fdw_orc_types WHERE val_int > 100;
SELECT * FROM fdw_orc_types WHERE val_float < 2.0;
SELECT * FROM fdw_orc_types WHERE val_str = 'orc_a';
SELECT * FROM fdw_orc_types WHERE val_bool = true;
SELECT * FROM fdw_orc_types WHERE val_date > '2024-01-01';
SELECT * FROM fdw_orc_types WHERE val_int IS NULL;

-- EXPLAIN on ORC table
EXPLAIN (COSTS OFF) SELECT * FROM fdw_orc_types WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM fdw_orc_types WHERE val_str = 'orc_a' AND val_bool = true;

DROP FOREIGN TABLE fdw_orc_types;

-- ============================================================
-- Test 4: Text with new text path (textFileRead coverage)
-- ============================================================
SET datalake.external_table_new_text = on;

DROP FOREIGN TABLE IF EXISTS fdw_text_new;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_text', 'paa_cluster', 'fdw_text_new', 'hive_server');

SELECT * FROM fdw_text_new ORDER BY id;
SELECT COUNT(*) FROM fdw_text_new;

DROP FOREIGN TABLE fdw_text_new;

-- Read same table with old text path for comparison
SET datalake.external_table_new_text = off;

DROP FOREIGN TABLE IF EXISTS fdw_text_old;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_text', 'paa_cluster', 'fdw_text_old', 'hive_server');

SELECT * FROM fdw_text_old ORDER BY id;

DROP FOREIGN TABLE fdw_text_old;

-- ============================================================
-- Test 5: Avro table sync (avroRead coverage via Hive)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_hive_avro2;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_avro', 'paa_cluster', 'fdw_hive_avro2', 'hive_server');

SELECT * FROM fdw_hive_avro2 ORDER BY id;
SELECT COUNT(*) FROM fdw_hive_avro2;
SELECT * FROM fdw_hive_avro2 WHERE id > 1 ORDER BY id;

DROP FOREIGN TABLE fdw_hive_avro2;

-- ============================================================
-- Test 6: FDW planner/cost estimation (datalake_fdw.c coverage)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_plan_test;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_parquet', 'paa_cluster', 'fdw_plan_test', 'hive_server');

-- Various EXPLAIN modes
EXPLAIN (COSTS OFF) SELECT * FROM fdw_plan_test;
EXPLAIN (COSTS OFF) SELECT id FROM fdw_plan_test WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM fdw_plan_test WHERE id > 0 AND name = 'alice';
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM fdw_plan_test;

-- Join with local table (exercises FDW planner paths)
CREATE TEMP TABLE local_join (id int, extra text);
INSERT INTO local_join VALUES (1, 'local1'), (2, 'local2'), (3, 'local3');

SELECT f.id, f.name, l.extra FROM fdw_plan_test f JOIN local_join l ON f.id = l.id ORDER BY f.id;
EXPLAIN (COSTS OFF) SELECT f.id, f.name FROM fdw_plan_test f JOIN local_join l ON f.id = l.id;

DROP TABLE local_join;
DROP FOREIGN TABLE fdw_plan_test;

-- ============================================================
-- Test 7: Multiple GUC variations on Hive tables
-- ============================================================
DROP FOREIGN TABLE IF EXISTS fdw_guc_hive;
SELECT public.sync_hive_table('hive_cluster', 'fdw_coverage_test', 'test_parquet', 'paa_cluster', 'fdw_guc_hive', 'hive_server');

-- Filter pushdown on/off
SET datalake.disable_filter_pushdown = off;
SELECT * FROM fdw_guc_hive WHERE id = 1;
SET datalake.disable_filter_pushdown = on;
SELECT * FROM fdw_guc_hive WHERE id = 1;
SET datalake.disable_filter_pushdown = off;

-- Debug mode
SET datalake.external_table_debug = on;
SELECT COUNT(*) FROM fdw_guc_hive;
SET datalake.external_table_debug = off;

-- Cache disabled
SET datalake.disable_cache_file = on;
SELECT COUNT(*) FROM fdw_guc_hive;
SET datalake.disable_cache_file = off;

-- Limit segments
SET datalake.external_table_limit_segment_num = 1;
SELECT COUNT(*) FROM fdw_guc_hive;
SET datalake.external_table_limit_segment_num = 0;

-- List in master
SET datalake.enable_list_in_master = on;
SELECT COUNT(*) FROM fdw_guc_hive;
SET datalake.enable_list_in_master = off;

-- Ignore hidden files
SET datalake.external_table_ignore_hidden_file = on;
SELECT COUNT(*) FROM fdw_guc_hive;
SET datalake.external_table_ignore_hidden_file = off;

-- ANALYZE
ANALYZE fdw_guc_hive;

DROP FOREIGN TABLE fdw_guc_hive;

-- ============================================================
-- Cleanup
-- ============================================================
DROP USER MAPPING IF EXISTS FOR gpadmin SERVER hive_server;
DROP SERVER IF EXISTS hive_server;
