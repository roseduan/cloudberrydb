-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(hive_connector\.c:\d+\)/
-- s/\(hive_connector\.c:\d+\)/(hive_connector.c:XXX)/
-- end_matchsubs
-- 01_numeric_types.sql
-- Test numeric data type support

\i ../../../lib/sql/common_setup.sql
\i ../../../lib/sql/hive_server_setup.sql

-- Create test schema
DROP SCHEMA IF EXISTS feature_test CASCADE;
CREATE SCHEMA feature_test;

SELECT test_log('Feature Test: Numeric Data Types');

-- Test 1: Sync table with various numeric types
SELECT test_log('Test 1: Sync numeric types table');
SELECT public.sync_hive_table('hive_cluster','default','test_numeric_types','paa_cluster', 'feature_test.numeric_test', 'hive_server');

-- Verify table structure
\d feature_test.numeric_test

-- Test 2: Query and verify tinyint (int2)
SELECT test_log('Test 2: Verify tinyint values');
SELECT id, tiny_val FROM feature_test.numeric_test ORDER BY id LIMIT 5;

-- Test 3: Query and verify int (int4)
SELECT test_log('Test 3: Verify int values');
SELECT id, int_val FROM feature_test.numeric_test ORDER BY id LIMIT 5;

-- Test 4: Query and verify bigint (int8)
SELECT test_log('Test 4: Verify bigint values');
SELECT id, big_val FROM feature_test.numeric_test ORDER BY id LIMIT 5;

-- Test 5: Query and verify float/double
SELECT test_log('Test 5: Verify float/double values');
SELECT id, float_val, double_val FROM feature_test.numeric_test ORDER BY id LIMIT 5;

-- Test 6: Query and verify decimal with precision
SELECT test_log('Test 6: Verify decimal values');
SELECT id, decimal_val FROM feature_test.numeric_test ORDER BY id LIMIT 5;

-- Test 7: Arithmetic operations on numeric types
SELECT test_log('Test 7: Arithmetic operations');
SELECT
    AVG(tiny_val) as avg_tiny,
    SUM(int_val) as sum_int,
    MAX(big_val) as max_big,
    ROUND(AVG(float_val)::numeric, 2) as avg_float,
    ROUND(SUM(decimal_val)::numeric, 2) as sum_decimal
FROM feature_test.numeric_test;

-- Test 8: NULL handling in numeric types
SELECT test_log('Test 8: NULL handling');
SELECT
    COUNT(*) as total,
    COUNT(tiny_val) as non_null_tiny,
    COUNT(int_val) as non_null_int
FROM feature_test.numeric_test;

-- Cleanup
DROP SCHEMA feature_test CASCADE;
