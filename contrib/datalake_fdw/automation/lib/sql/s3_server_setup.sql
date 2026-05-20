-- s3_server_setup.sql
-- S3/MinIO server configuration for datalake_fdw tests
-- Requires: common_setup.sql to be loaded first
--
-- This file creates S3 server with MinIO defaults from docker-compose.yml
-- Default endpoint: minio:9000 (MinIO service)
-- Default credentials: admin/password

-- Create server for MinIO (S3-compatible storage)
CREATE SERVER IF NOT EXISTS minio_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        host 'hadoop',
        protocol 's3',
        isvirtual 'false',
        ishttps 'false'
    );

-- Create user mapping with credentials
CREATE USER MAPPING IF NOT EXISTS FOR gpadmin
    SERVER minio_server
    OPTIONS (
        user 'gpadmin',
        accesskey 'admin',
        secretkey 'admin12345'
    );

-- Log server creation
SELECT test_log('S3/MinIO server setup completed');
