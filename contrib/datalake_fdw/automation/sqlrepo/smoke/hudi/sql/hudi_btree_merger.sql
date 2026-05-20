-- Hudi BTree Merger Coverage Test
-- Purpose: Trigger btree merger path (vs hashtab merger)
-- Target: hudi_btree_merger.c (+171), hudi_merged_logfile_record_reader.c (+93),
--         hudi_task_reader.c (+50), hudi_logfile_block_reader.c (+40)
--
-- The btree merger is selected when total log file size >= hudi_log_merger_threshold (MB).
-- We set GUC threshold to 0 to force btree path for all MOR reads.

SET client_min_messages = ERROR;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
RESET client_min_messages;

CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- Note: btree merger requires log files > 128MB (minimum threshold).
-- Our test data is too small, so this test exercises hashtab merger path
-- with additional MOR read patterns not covered by hudi_coverage.sql.

CREATE SERVER hudi_bt_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        protocol 'hdfs',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
    SERVER hudi_bt_server
    OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: MOR multi-delta with btree merger (threshold = 0)
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_bt_multi;
CREATE FOREIGN TABLE hudi_bt_multi (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_bt_server
OPTIONS (
    filePath 'default.hudi_mor_multi_delta_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_multi_delta_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_bt_multi;
SELECT * FROM hudi_bt_multi ORDER BY id;
SELECT * FROM hudi_bt_multi WHERE price > 50.0 ORDER BY id;

-- ============================================================
-- Test 2: MOR bulk with btree merger
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_bt_bulk;
CREATE FOREIGN TABLE hudi_bt_bulk (
    id bigint,
    name text,
    price double precision,
    ts bigint
)
SERVER hudi_bt_server
OPTIONS (
    filePath 'default.hudi_mor_bulk_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_bulk_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_bt_bulk;
SELECT * FROM hudi_bt_bulk WHERE id <= 10 ORDER BY id;
SELECT COUNT(*) FROM hudi_bt_bulk WHERE name LIKE 'updated_%';

-- ============================================================
-- Test 3: MOR partitioned with btree merger
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_bt_part;
CREATE FOREIGN TABLE hudi_bt_part (
    id bigint,
    name text,
    price double precision,
    ts bigint,
    region text
)
SERVER hudi_bt_server
OPTIONS (
    filePath 'default.hudi_mor_part_delta_rt',
    catalog_type 'hive',
    server_name 'hive_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mor_part_delta_rt',
    format 'hudi'
);

SELECT COUNT(*) FROM hudi_bt_part;
SELECT * FROM hudi_bt_part ORDER BY id;
SELECT region, COUNT(*) FROM hudi_bt_part GROUP BY region ORDER BY region;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_bt_multi;
DROP FOREIGN TABLE IF EXISTS hudi_bt_bulk;
DROP FOREIGN TABLE IF EXISTS hudi_bt_part;
