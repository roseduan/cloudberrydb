-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(hive_connector\.c:\d+\)/
-- s/\(hive_connector\.c:\d+\)/(hive_connector.c:XXX)/
-- end_matchsubs
-- 02_sync_partitions.sql
-- Test Hive sync with partitioned tables

\i ../../../lib/sql/common_setup.sql
\i ../../../lib/sql/hive_server_setup.sql

DROP SCHEMA IF EXISTS feature_test CASCADE;
CREATE SCHEMA feature_test;

SELECT test_log('Feature Test: Hive Sync Partitions');

-- ============================================================
-- Test 1: Sync single-partition table
-- ============================================================
SELECT test_log('Test 1: Sync test_single_partition');

SELECT public.sync_hive_table('hive_cluster', 'default', 'test_single_partition', 'paa_cluster', 'feature_test.sync_single_part', 'hive_server');
SELECT * FROM feature_test.sync_single_part ORDER BY 1 LIMIT 10;

-- ============================================================
-- Test 2: Sync multi-partition table
-- ============================================================
SELECT test_log('Test 2: Sync test_multi_partition');

SELECT public.sync_hive_table('hive_cluster', 'default', 'test_multi_partition', 'paa_cluster', 'feature_test.sync_multi_part', 'hive_server');
SELECT * FROM feature_test.sync_multi_part ORDER BY 1 LIMIT 10;

-- ============================================================
-- Test 3: GROUP BY on partition columns
-- ============================================================
SELECT test_log('Test 3: GROUP BY on partition columns');

SELECT COUNT(*) AS total_rows FROM feature_test.sync_single_part;
SELECT COUNT(*) AS total_rows FROM feature_test.sync_multi_part;

-- ============================================================
-- Test 4: EXPLAIN for partition pruning
-- ============================================================
SELECT test_log('Test 4: EXPLAIN partition pruning');

EXPLAIN (COSTS OFF) SELECT * FROM feature_test.sync_single_part;
EXPLAIN (COSTS OFF) SELECT * FROM feature_test.sync_multi_part;

-- ============================================================
-- Test 5: Sync extreme values table
-- ============================================================
SELECT test_log('Test 5: Sync test_extreme');

SELECT public.sync_hive_table('hive_cluster', 'default', 'test_extreme', 'paa_cluster', 'feature_test.sync_extreme', 'hive_server');
SELECT COUNT(*) AS extreme_count FROM feature_test.sync_extreme;

-- ============================================================
-- Test 6: Sync empty string table
-- ============================================================
SELECT test_log('Test 6: Sync test_empty_str');

SELECT public.sync_hive_table('hive_cluster', 'default', 'test_empty_str', 'paa_cluster', 'feature_test.sync_empty_str', 'hive_server');
SELECT COUNT(*) AS empty_str_count FROM feature_test.sync_empty_str;

-- ============================================================
-- Cleanup
-- ============================================================
DROP SCHEMA feature_test CASCADE;
