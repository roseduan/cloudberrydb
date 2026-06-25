-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(hive_connector\.c:\d+\)/
-- s/\(hive_connector\.c:\d+\)/(hive_connector.c:XXX)/
-- end_matchsubs
-- 02_string_types.sql
-- Test string data type support

\i ../../../lib/sql/common_setup.sql
\i ../../../lib/sql/hive_server_setup.sql

-- Create test schema
DROP SCHEMA IF EXISTS feature_test CASCADE;
CREATE SCHEMA feature_test;

SELECT test_log('Feature Test: String Data Types');

-- Test 1: Sync table with string types
SELECT test_log('Test 1: Sync string types table');
SELECT public.sync_hive_table('hive_cluster','default','test_string_types','paa_cluster', 'feature_test.string_test', 'hive_server');

-- Test 2: Query string values
SELECT test_log('Test 2: Query string values');
SELECT id, str_val FROM feature_test.string_test ORDER BY id LIMIT 5;

-- Test 3: String operations
SELECT test_log('Test 3: String operations');
SELECT
    id,
    LENGTH(str_val) as str_length,
    UPPER(str_val) as uppercase,
    LOWER(str_val) as lowercase
FROM feature_test.string_test
ORDER BY id LIMIT 5;

-- Test 4: String pattern matching
SELECT test_log('Test 4: String pattern matching');
SELECT COUNT(*) as matching_rows
FROM feature_test.string_test
WHERE str_val LIKE '%test%';

-- Test 5: Empty string vs NULL
SELECT test_log('Test 5: Empty string vs NULL');
SELECT
    COUNT(CASE WHEN str_val = '' THEN 1 END) as empty_strings,
    COUNT(CASE WHEN str_val IS NULL THEN 1 END) as null_values,
    COUNT(*) as total_rows
FROM feature_test.string_test;

-- Cleanup
DROP SCHEMA feature_test CASCADE;
