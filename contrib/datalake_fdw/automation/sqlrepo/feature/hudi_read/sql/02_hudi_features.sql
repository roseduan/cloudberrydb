-- 02_hudi_features.sql
-- Test Hudi advanced read features: partitions, filters, aggregates, projection

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Hudi Features');

-- ============================================================
-- Setup: Hudi server (HDFS-based for Hudi tables)
-- ============================================================
DROP SERVER IF EXISTS hudi_ff_server CASCADE;
CREATE SERVER hudi_ff_server FOREIGN DATA WRAPPER datalake_fdw
OPTIONS (hdfs_namenodes 'hadoop', hdfs_port '8020', protocol 'hdfs',
         hdfs_auth_method 'simple', hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR gpadmin SERVER hudi_ff_server OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: Partition table full read
-- ============================================================
SELECT test_log('Test 1: Partition table full read');

CREATE FOREIGN TABLE hf_part (id bigint, name text, amount double precision, region text)
SERVER hudi_ff_server
OPTIONS (filePath 'default.hudi_partition_table', catalog_type 'hive', server_name 'hive_cluster',
         hdfs_cluster_name 'paa_cluster', table_identifier 'default.hudi_partition_table', format 'hudi');
SELECT * FROM hf_part ORDER BY id LIMIT 10;
SELECT COUNT(*) AS part_count FROM hf_part;

-- ============================================================
-- Test 2: Partition filter
-- ============================================================
SELECT test_log('Test 2: Partition filter (WHERE region)');

SELECT * FROM hf_part WHERE region = 'east' ORDER BY id;

-- ============================================================
-- Test 3: GROUP BY partition column + aggregation
-- ============================================================
SELECT test_log('Test 3: GROUP BY partition column');

SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM hf_part
GROUP BY region
ORDER BY region;

-- ============================================================
-- Test 4: COW table WHERE filter
-- ============================================================
SELECT test_log('Test 4: COW table WHERE filter');

CREATE FOREIGN TABLE hf_cow (id bigint, name text, price double precision, ts bigint)
SERVER hudi_ff_server
OPTIONS (filePath 'default.hudi_cow_table', catalog_type 'hive', server_name 'hive_cluster',
         hdfs_cluster_name 'paa_cluster', table_identifier 'default.hudi_cow_table', format 'hudi');
SELECT * FROM hf_cow WHERE id > 2 ORDER BY id;

-- ============================================================
-- Test 5: SUM, COUNT aggregates
-- ============================================================
SELECT test_log('Test 5: SUM, COUNT aggregates');

SELECT COUNT(*) AS cnt, SUM(price) AS total FROM hf_cow;

-- ============================================================
-- Test 6: Column projection
-- ============================================================
SELECT test_log('Test 6: Column projection');

SELECT id FROM hf_cow ORDER BY id;
SELECT name, price FROM hf_cow ORDER BY name;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE hf_part;
DROP FOREIGN TABLE hf_cow;
DROP USER MAPPING FOR gpadmin SERVER hudi_ff_server;
DROP SERVER hudi_ff_server;
