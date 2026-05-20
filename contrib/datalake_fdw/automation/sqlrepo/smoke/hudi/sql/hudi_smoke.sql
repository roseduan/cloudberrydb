-- Hudi Smoke Test
-- Tests basic read operations on pre-loaded Hudi tables via foreign table with format 'hudi'
-- Requires: Hudi tables pre-loaded via Spark in Hive Metastore
-- Approach: CREATE FOREIGN TABLE with catalog_type='hive', format='hudi' (per hudi_read.sql pattern)

-- Clean up previous run leftovers
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- Create server directly (avoid create_foreign_server which requires gphdfs.conf on coordinator)
CREATE SERVER hudi_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        protocol 'hdfs',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
    SERVER hudi_server
    OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: Hudi COW (Copy-on-Write) table basic read
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_cow_table;
CREATE FOREIGN TABLE hudi_cow_table (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_server
OPTIONS (
    filePath 'default.hudi_cow_table',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_cow_table',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_cow_table;
SELECT * FROM hudi_cow_table ORDER BY id;
SELECT * FROM hudi_cow_table WHERE id > 2 ORDER BY id;
SELECT COUNT(*) AS cnt, SUM(price) AS total_price FROM hudi_cow_table;

-- ============================================================
-- Test 2: Hudi MOR (Merge-on-Read) table basic read
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mor_table;
CREATE FOREIGN TABLE hudi_mor_table (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_server
OPTIONS (
    filePath 'default.hudi_mor_table_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_table_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_mor_table;
SELECT * FROM hudi_mor_table ORDER BY id;
SELECT * FROM hudi_mor_table WHERE price > 100.00 ORDER BY id;

-- ============================================================
-- Test 3: Hudi partitioned table
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_partition_table;
CREATE FOREIGN TABLE hudi_partition_table (
    id bigint,
    name text,
    amount double precision,
    region text
)
SERVER hudi_server
OPTIONS (
    filePath 'default.hudi_partition_table',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_partition_table',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_partition_table;
SELECT * FROM hudi_partition_table ORDER BY id;
SELECT * FROM hudi_partition_table WHERE region = 'east' ORDER BY id;
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM hudi_partition_table
GROUP BY region ORDER BY region;

-- ============================================================
-- Test 4: Hudi NULL value handling
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_null_table;
CREATE FOREIGN TABLE hudi_null_table (
    id bigint,
    name text,
    val int,
    ts bigint
)
SERVER hudi_server
OPTIONS (
    filePath 'default.hudi_null_table',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_null_table',
    format 'hudi'
);

SELECT * FROM hudi_null_table ORDER BY id;
SELECT id, name IS NULL AS name_null, val IS NULL AS val_null
FROM hudi_null_table ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_cow_table;
DROP FOREIGN TABLE IF EXISTS hudi_mor_table;
DROP FOREIGN TABLE IF EXISTS hudi_partition_table;
DROP FOREIGN TABLE IF EXISTS hudi_null_table;
