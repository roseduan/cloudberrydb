-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(hive_connector\.c:\d+\)/
-- s/\(hive_connector\.c:\d+\)/(hive_connector.c:XXX)/
-- end_matchsubs
-- 01_single_partition.sql
-- Test single partition column functionality

\i ../../../lib/sql/common_setup.sql
\i ../../../lib/sql/hive_server_setup.sql

-- Create test schema
DROP SCHEMA IF EXISTS feature_test CASCADE;
CREATE SCHEMA feature_test;

SELECT test_log('Feature Test: Single Partition Column');

-- Test 1: Sync single partition table (partitioned by year)
SELECT test_log('Test 1: Sync and query single partition table');
SELECT public.sync_hive_table('hive_cluster','default','test_single_partition','paa_cluster', 'feature_test.single_part', 'hive_server');

-- Verify table structure includes partition column
\d feature_test.single_part

-- Query all data
SELECT COUNT(*) as total_rows FROM feature_test.single_part;

-- Test 2: Query specific partition
SELECT test_log('Test 2: Query specific partition (year=2024)');
SELECT COUNT(*) as rows_2024 FROM feature_test.single_part WHERE year = 2024;

-- Test 3: Query multiple partitions
SELECT test_log('Test 3: Query multiple partitions');
SELECT year, COUNT(*) as row_count
FROM feature_test.single_part
GROUP BY year
ORDER BY year;

-- Test 4: Verify partition pruning with EXPLAIN
SELECT test_log('Test 4: Verify partition pruning');
EXPLAIN (COSTS OFF) SELECT * FROM feature_test.single_part WHERE year = 2024;

-- Cleanup
DROP SCHEMA feature_test CASCADE;
