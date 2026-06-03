-- 01_hudi_basic.sql
-- Test Hudi basic read operations

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Hudi Basic Reads');

-- ============================================================
-- Setup: Hudi server (HDFS-based for Hudi tables)
-- ============================================================
DROP SERVER IF EXISTS hudi_ft_server CASCADE;
CREATE SERVER hudi_ft_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (hdfs_namenodes 'hadoop', hdfs_port '8020', protocol 'hdfs',
         hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR gpadmin SERVER hudi_ft_server OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: COW table full read + COUNT
-- ============================================================
SELECT test_log('Test 1: COW table full read');

CREATE FOREIGN TABLE hb_cow (id bigint, name text, price double precision, ts bigint)
SERVER hudi_ft_server
OPTIONS (filePath 'default.hudi_cow_table', catalog_type 'hive', server_name 'hive_cluster',
         hdfs_cluster_name 'paa_cluster', table_identifier 'default.hudi_cow_table', format 'hudi');
SELECT * FROM hb_cow ORDER BY id LIMIT 10;
SELECT COUNT(*) AS cow_count FROM hb_cow;

-- ============================================================
-- Test 2: COW table ORDER BY
-- ============================================================
SELECT test_log('Test 2: COW table ORDER BY');

SELECT id, name FROM hb_cow ORDER BY id;
SELECT id, name FROM hb_cow ORDER BY name;

-- ============================================================
-- Test 3: MOR table full read + COUNT
-- ============================================================
SELECT test_log('Test 3: MOR table full read');

CREATE FOREIGN TABLE hb_mor (id bigint, name text, price double precision, ts bigint)
SERVER hudi_ft_server
OPTIONS (filePath 'default.hudi_mor_table_rt', catalog_type 'hive', server_name 'hive_cluster',
         hdfs_cluster_name 'paa_cluster', table_identifier 'default.hudi_mor_table_rt', format 'hudi');
SELECT * FROM hb_mor ORDER BY id LIMIT 10;
SELECT COUNT(*) AS mor_count FROM hb_mor;

-- ============================================================
-- Test 4: MOR table ORDER BY
-- ============================================================
SELECT test_log('Test 4: MOR table ORDER BY');

SELECT id, name FROM hb_mor ORDER BY id;

-- ============================================================
-- Test 5: NULL table read
-- ============================================================
SELECT test_log('Test 5: NULL table read');

CREATE FOREIGN TABLE hb_nulls (id bigint, name text, val int, ts bigint)
SERVER hudi_ft_server
OPTIONS (filePath 'default.hudi_null_table', catalog_type 'hive', server_name 'hive_cluster',
         hdfs_cluster_name 'paa_cluster', table_identifier 'default.hudi_null_table', format 'hudi');
SELECT * FROM hb_nulls ORDER BY id LIMIT 10;

-- ============================================================
-- Test 6: IS NULL / IS NOT NULL checks
-- ============================================================
SELECT test_log('Test 6: IS NULL / IS NOT NULL');

SELECT COUNT(*) AS null_names FROM hb_nulls WHERE name IS NULL;
SELECT COUNT(*) AS notnull_names FROM hb_nulls WHERE name IS NOT NULL;
SELECT COUNT(*) AS null_vals FROM hb_nulls WHERE val IS NULL;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE hb_cow;
DROP FOREIGN TABLE hb_mor;
DROP FOREIGN TABLE hb_nulls;
DROP USER MAPPING FOR gpadmin SERVER hudi_ft_server;
DROP SERVER hudi_ft_server;
