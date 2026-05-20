-- Test 01: Connection Test
-- Purpose: Verify connectivity to HDFS

-- Clean up previous run leftovers
DROP SERVER IF EXISTS hdfs_server CASCADE;
DROP SERVER IF EXISTS hive_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup (inline to avoid \i path issues with pg_regress)
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- Test 2: Create server (HDFS)
CREATE SERVER hdfs_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        protocol 'hdfs',
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
        SERVER hdfs_server
        OPTIONS (user 'gpadmin');

-- Test 3: Create server (Hive)
CREATE SERVER hive_server
        FOREIGN DATA WRAPPER datalake_fdw OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
        SERVER hive_server
        OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Test 4: Verify servers
SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname IN ('hdfs_server', 'hive_server');
