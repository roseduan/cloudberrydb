-- iceberg_server_setup.sql
-- Iceberg catalog and volume server configuration for datalake_fdw tests
-- Requires: common_setup.sql to be loaded first
--
-- This file creates:
-- 1. Iceberg catalog server (connects to Hive Metastore or Polaris)
-- 2. Iceberg volume server (connects to S3/MinIO for data storage)
-- 3. Iceberg toolkit schema and functions

-- Create iceberg_toolkit schema if not exists
CREATE SCHEMA IF NOT EXISTS iceberg_toolkit;

-- Create Iceberg Toolkit function for catalog operations
CREATE OR REPLACE FUNCTION iceberg_toolkit.catalog_fdw(
    operation text,              -- 'create_table', 'append', 'load', 'get_fragment'
    name_space text,             -- Namespace (e.g., 'default')
    table_name text,             -- Table name
    iceberg_table_name text,     -- Iceberg table schema or name
    catalog_server_name text,    -- Catalog server name
    catalog_table_name text,     -- Catalog table name
    volume_server_name text,     -- Volume server name
    volume_table_name text       -- Volume table name
)
RETURNS text
AS '$libdir/datalake_fdw.so', 'iceberg_toolkit_catalog_fdw'
LANGUAGE C STRICT;

-- Grant permissions
GRANT USAGE ON SCHEMA iceberg_toolkit TO public;
GRANT EXECUTE ON FUNCTION iceberg_toolkit.catalog_fdw(text, text, text, text, text, text, text, text) TO public;

-- Create catalog server (Hive Metastore backend)
-- For Polaris, override with different options in test file
CREATE SERVER IF NOT EXISTS iceberg_catalog_server
    FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (
        server_type 'hive',
        hive_metastore_uri 'thrift://hive-metastore:9083'
    );

-- Create user mapping for catalog
CREATE USER MAPPING IF NOT EXISTS FOR current_user
    SERVER iceberg_catalog_server
    OPTIONS (
        username 'gpadmin',
        auth_method 'simple'
    );

-- Create foreign catalog
CREATE FOREIGN CATALOG IF NOT EXISTS iceberg_catalog
    SERVER iceberg_catalog_server
    OPTIONS (
        catalog_name 'iceberg_catalog',
        default_namespace 'default',
        enable_metadata_cache 'true',
        metadata_cache_ttl '300',
        auto_refresh_metadata 'true',
        warehouse_location_prefix 's3://warehouse/'
    );

-- Create volume server (S3/MinIO backend for Iceberg data)
CREATE SERVER IF NOT EXISTS iceberg_volume_server
    FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        server_type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );

-- Create user mapping for volume
CREATE USER MAPPING IF NOT EXISTS FOR current_user
    SERVER iceberg_volume_server
    OPTIONS (
        username 'gpadmin',
        aws_access_key_id 'admin',
        aws_secret_access_key 'admin12345'
    );

-- Create foreign volume
CREATE FOREIGN VOLUME IF NOT EXISTS iceberg_volume
    SERVER iceberg_volume_server
    OPTIONS (
        base_path '/warehouse/',
        enable_caching 'true',
        allow_writes 'true'
    );

-- Log server creation
SELECT test_log('Iceberg catalog and volume setup completed');
