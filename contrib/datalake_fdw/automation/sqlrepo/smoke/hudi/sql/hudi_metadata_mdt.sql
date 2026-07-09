-- Hudi metadata table regression for issue #873.
-- Explicit metadata_table_enable=true should reproduce the Hudi 0.13/Hadoop 3
-- getReadStatistics failure when the MDT has a compacted base HFile.
-- Omitting metadata_table_enable now defaults to false in datalake_fdw/dlagent
-- and should read the same table successfully through filesystem listing.

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

CREATE SERVER hudi_mdt_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        protocol 'hdfs',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
    SERVER hudi_mdt_server
    OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: explicit metadata table read reproduces issue #873
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mdt_metadata_on;
CREATE FOREIGN TABLE hudi_mdt_metadata_on (
    id bigint,
    name text,
    ts bigint
)
SERVER hudi_mdt_server
OPTIONS (
    filePath '/warehouse/hudi_mdt_repro',
    catalog_type 'hadoop',
    server_name 'paa_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mdt_repro',
    metadata_table_enable 'true',
    format 'hudi'
);

DO $$
DECLARE
    err_msg text;
    err_detail text;
    err_context text;
    err_text text;
BEGIN
    BEGIN
        PERFORM COUNT(*) FROM hudi_mdt_metadata_on;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            err_msg = MESSAGE_TEXT,
            err_detail = PG_EXCEPTION_DETAIL,
            err_context = PG_EXCEPTION_CONTEXT;
        err_text := CONCAT_WS(E'\n', err_msg, err_detail, err_context);
    END;

    IF err_text IS NULL THEN
        RAISE EXCEPTION 'expected metadata_table_enable=true to reproduce issue #873';
    END IF;

    IF err_text NOT LIKE '%NoSuchMethodError%'
        AND err_text NOT LIKE '%getReadStatistics%' THEN
        RAISE EXCEPTION 'unexpected metadata table error: %', err_text;
    END IF;

    RAISE NOTICE 'metadata_table_enable=true reproduces Hudi/Hadoop getReadStatistics failure';
END $$;

-- ============================================================
-- Test 2: default Hudi metadata setting is disabled and reads succeed
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mdt_default_off;
CREATE FOREIGN TABLE hudi_mdt_default_off (
    id bigint,
    name text,
    ts bigint
)
SERVER hudi_mdt_server
OPTIONS (
    filePath '/warehouse/hudi_mdt_repro',
    catalog_type 'hadoop',
    server_name 'paa_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mdt_repro',
    format 'hudi'
);

SELECT COUNT(*) AS cnt FROM hudi_mdt_default_off;
SELECT * FROM hudi_mdt_default_off ORDER BY id;

-- ============================================================
-- Test 3: explicit metadata_table_enable=false also reads succeed
-- ============================================================
DROP FOREIGN TABLE IF EXISTS hudi_mdt_explicit_off;
CREATE FOREIGN TABLE hudi_mdt_explicit_off (
    id bigint,
    name text,
    ts bigint
)
SERVER hudi_mdt_server
OPTIONS (
    filePath '/warehouse/hudi_mdt_repro',
    catalog_type 'hadoop',
    server_name 'paa_cluster',
    hdfs_cluster_name 'paa_cluster',
    table_identifier 'default.hudi_mdt_repro',
    metadata_table_enable 'false',
    format 'hudi'
);

SELECT COUNT(*) AS cnt FROM hudi_mdt_explicit_off;

DROP FOREIGN TABLE IF EXISTS hudi_mdt_metadata_on;
DROP FOREIGN TABLE IF EXISTS hudi_mdt_default_off;
DROP FOREIGN TABLE IF EXISTS hudi_mdt_explicit_off;
