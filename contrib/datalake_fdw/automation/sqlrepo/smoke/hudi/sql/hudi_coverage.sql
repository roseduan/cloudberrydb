-- Hudi Coverage Test
-- Purpose: Deep code coverage for HUDI module (1129 lines at 0%)
-- Target: hudi_task_reader.c, hudi_logfile_block_reader.c,
--         hudi_merged_logfile_record_reader.c, hudi_btree_merger.c,
--         hudi_hashtab_merger.c, hudi_deltalog_filter.c, hudi_read.cpp,
--         dlproxy/hudi.c, provider/common/utils.c, avro_block_reader.cpp

-- Clean up previous run leftovers
SET client_min_messages = ERROR;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;
RESET client_min_messages;

-- Common setup
SET client_min_messages = WARNING;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
RESET client_min_messages;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

CREATE SERVER hudi_cov_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        protocol 'hdfs',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
    SERVER hudi_cov_server
    OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: MOR with multiple delta logs (merger exercise)
-- Exercises: hudi_merged_logfile_record_reader, hudi_btree_merger,
--            hudi_logfile_block_reader, avro_block_reader
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mor_multi_delta;
CREATE FOREIGN TABLE hudi_mor_multi_delta (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_cov_server
OPTIONS (
    filePath 'default.hudi_mor_multi_delta_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_multi_delta_rt',
    format 'hudi'
);

-- Full scan (merges all delta logs)
SELECT COUNT(*) FROM hudi_mor_multi_delta;

-- Verify merged results (latest version wins by ts)
SELECT * FROM hudi_mor_multi_delta ORDER BY id;

-- Filter pushdown on merged data
SELECT * FROM hudi_mor_multi_delta WHERE id <= 3 ORDER BY id;
SELECT * FROM hudi_mor_multi_delta WHERE price > 50.00 ORDER BY id;

-- Aggregate on merged data
SELECT COUNT(*) AS cnt, SUM(price) AS total, AVG(price) AS avg_price
FROM hudi_mor_multi_delta;

-- ============================================================
-- Test 2: MOR bulk data with updates (large merge)
-- Exercises: hashtab_merger (large batch), utils.c record operations
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mor_bulk;
CREATE FOREIGN TABLE hudi_mor_bulk (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_cov_server
OPTIONS (
    filePath 'default.hudi_mor_bulk_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_bulk_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_mor_bulk;

-- Verify updates were merged (even IDs should be 'updated_N')
SELECT * FROM hudi_mor_bulk WHERE id <= 10 ORDER BY id;

-- Check updated vs original
SELECT COUNT(*) FROM hudi_mor_bulk WHERE name LIKE 'updated_%';
SELECT COUNT(*) FROM hudi_mor_bulk WHERE name LIKE 'name_%';

-- Aggregate
SELECT MIN(id), MAX(id), COUNT(*), SUM(price) FROM hudi_mor_bulk;

-- ============================================================
-- Test 3: COW with multiple types
-- Exercises: type handling in hudi_task_reader, utils.c datum creation
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_cow_types;
CREATE FOREIGN TABLE hudi_cow_types (
    id bigint,
    str_col text,
    big_col bigint,
    dbl_col double precision,
    bool_col boolean,
    date_col date,
    ts_col bigint,
    ts bigint
)
SERVER hudi_cov_server
OPTIONS (
    filePath 'default.hudi_cow_types',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_cow_types',
    format 'hudi'
);

SELECT * FROM hudi_cow_types ORDER BY id;
SELECT id, str_col IS NULL AS str_null, big_col IS NULL AS big_null,
       bool_col, date_col
FROM hudi_cow_types ORDER BY id;

-- ============================================================
-- Test 4: MOR partitioned with per-partition deltas
-- Exercises: partition-aware fragment scanning, delta per file group
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mor_part_delta;
CREATE FOREIGN TABLE hudi_mor_part_delta (
    id bigint,
    name text,
    price double precision,
    ts bigint,
    region text
)
SERVER hudi_cov_server
OPTIONS (
    filePath 'default.hudi_mor_part_delta_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_part_delta_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_mor_part_delta;
SELECT * FROM hudi_mor_part_delta ORDER BY id;

-- Filter by partition column
SELECT * FROM hudi_mor_part_delta WHERE region = 'east' ORDER BY id;
SELECT * FROM hudi_mor_part_delta WHERE region = 'west' ORDER BY id;

-- Cross-partition aggregate
SELECT region, COUNT(*) AS cnt, SUM(price) AS total
FROM hudi_mor_part_delta
GROUP BY region ORDER BY region;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mor_multi_delta;
DROP FOREIGN TABLE IF EXISTS hudi_mor_bulk;
DROP FOREIGN TABLE IF EXISTS hudi_cow_types;
DROP FOREIGN TABLE IF EXISTS hudi_mor_part_delta;
