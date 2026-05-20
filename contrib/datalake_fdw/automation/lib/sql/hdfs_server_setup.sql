-- hdfs_server_setup.sql
-- HDFS server configuration for datalake_fdw tests
-- Requires: common_setup.sql to be loaded first
--
-- This file creates HDFS namenode connection
-- Default endpoint: hadoop:8020 (HDFS namenode)

-- Create server for HDFS
CREATE SERVER IF NOT EXISTS hdfs_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        host 'hadoop',
        port '8020',
        protocol 'hdfs'
    );

-- Create user mapping
CREATE USER MAPPING IF NOT EXISTS FOR gpadmin
    SERVER hdfs_server
    OPTIONS (
        user 'gpadmin'
    );

-- Log server creation
SELECT test_log('HDFS server setup completed');
