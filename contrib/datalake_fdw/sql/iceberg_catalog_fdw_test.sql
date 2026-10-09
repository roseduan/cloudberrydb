-- Test file for Iceberg Catalog FDW
-- Tests the new CREATE ICEBERG TABLE syntax

-- Load the extension
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- Create test catalog server
CREATE SERVER basic_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    server_type 'hive',
    hive_metastore_uri 'thrift://hive-metastore:9083'
);

-- Create user mapping for catalog
CREATE USER MAPPING FOR current_user
SERVER basic_catalog_server
OPTIONS (
    username 'hive_user',
    auth_method 'simple'
);

CREATE FOREIGN CATALOG basic_catalog
SERVER basic_catalog_server
OPTIONS (
    catalog_name 'hive_location',
    default_namespace 'default',
    metadata_cache '5',
    warehouse_location_prefix 's3a://warehouse/hive_location/'
);

-- The deprecated cache options (enable_metadata_cache / metadata_cache_ttl /
-- auto_refresh_metadata) are unrecognized now: the DDL must still succeed --
-- they are ignored like any other unknown option -- so dumps from older
-- clusters restore cleanly.  metadata_cache alone controls caching.
CREATE FOREIGN CATALOG deprecated_options_catalog
SERVER basic_catalog_server
OPTIONS (
    catalog_name 'hive_location',
    enable_metadata_cache 'true',
    metadata_cache_ttl '300',
    auto_refresh_metadata 'true'
);
DROP CATALOG deprecated_options_catalog;

-- Create test volume server
CREATE SERVER basic_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    server_type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);

-- Create user mapping for volume
CREATE USER MAPPING FOR current_user
SERVER basic_volume_server
OPTIONS (
    username 'gpadmin',
    aws_access_key_id 'admin',
    aws_secret_access_key 'password'
);

CREATE FOREIGN VOLUME basic_volume
SERVER basic_volume_server
OPTIONS (
    base_path '/warehouse/',
    enable_caching 'true',
    allow_writes 'true'
);

-- Test 1: Basic table creation
CREATE ICEBERG TABLE sales_data (
    id bigint,
    name text,
    age int
) FOREIGN CATALOG basic_catalog
FOREIGN VOLUME basic_volume
OPTIONS (database_name 'sales_db');

-- Test 2: Table with different data types
CREATE ICEBERG TABLE user_profile (
    user_id bigint,
    username varchar(50),
    email text,
    created_at timestamp,
    is_active boolean,
    score decimal(10,2)
) FOREIGN CATALOG basic_catalog
FOREIGN VOLUME basic_volume
OPTIONS (database_name 'user_db');

-- Test 3: Simple table for basic operations
CREATE ICEBERG TABLE test_table (
    a int
) FOREIGN CATALOG basic_catalog
FOREIGN VOLUME basic_volume
OPTIONS (database_name 'test_db');

-- Test 4: iceberg_toolkit.catalog_fdw function tests
CREATE SCHEMA IF NOT EXISTS iceberg_toolkit;

-- Test create_table operation
SELECT iceberg_toolkit.catalog_fdw(
    'create_table',
    'default',
    'sales_data',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test get_fragment operation
SELECT iceberg_toolkit.catalog_fdw(
    'get_fragment',
    'default',
    'sales_data',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test load_table operation
SELECT iceberg_toolkit.catalog_fdw(
    'load_table',
    'default',
    'sales_data',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test create_table operation
SELECT iceberg_toolkit.catalog_fdw(
    'create_table',
    'default',
    'user_profile',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test get_fragment operation
SELECT iceberg_toolkit.catalog_fdw(
    'get_fragment',
    'default',
    'user_profile',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test create_table operation
SELECT iceberg_toolkit.catalog_fdw(
    'create_table',
    'default',
    'test_table',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test get_fragment operation
SELECT iceberg_toolkit.catalog_fdw(
    'get_fragment',
    'default',
    'test_table',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test load_table operation
SELECT iceberg_toolkit.catalog_fdw(
    'load_table',
    'default',
    'test_table',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
);

-- Test append operation
SELECT iceberg_toolkit.catalog_fdw(
    'append',
    'default',
    'test_table',
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume',
    '"fragments":[{"path":"/warehouse/test_table_1762855001008/data/file1.parquet","format":"PARQUET","record_count":100,"file_size_in_bytes":10240,"partition":{},"column_sizes":{"1":800,"2":1200,"3":400},"value_counts":{"1":100,"2":100,"3":95},"null_value_counts":{"1":0,"2":0,"3":5}},{"path":"/warehouse/test_table_1762855001008/data/file2.parquet","format":"PARQUET","record_count":200,"file_size_in_bytes":20480,"partition":{},"column_sizes":{"1":1600,"2":2400,"3":800},"value_counts":{"1":200,"2":200,"3":190},"null_value_counts":{"1":0,"2":0,"3":10}}]'
);


-- Test 5: iceberg_toolkit.volume_fdw function tests

-- Test read operation with row limit
SELECT * FROM iceberg_toolkit.volume_fdw(
    'read',
    'default',
    'sales_data',
    100,
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
) AS t(id bigint, name text, age int);

-- Test read operation with different table
SELECT * FROM iceberg_toolkit.volume_fdw(
    'read',
    'default',
    'user_profile',
    50,
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
) AS t(user_id bigint, username text, email text, created_at timestamp, is_active boolean, score decimal);

-- Test read operation with no limit (using 0)
SELECT * FROM iceberg_toolkit.volume_fdw(
    'read',
    'default',
    'test_table',
    0,
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
) AS t(a int);

SELECT * FROM iceberg_toolkit.volume_fdw(
    'read',
    'default',
    'test_table',
    0,
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
) AS t(a int);

SELECT * FROM iceberg_toolkit.volume_fdw(
    'append',
    'default',
    'sales_data',
    100,
    'basic_catalog_server',
    'basic_catalog',
    'basic_volume_server',
    'basic_volume'
) AS t(id bigint, name text, age int);

-- Cleanup
DROP FUNCTION IF EXISTS iceberg_toolkit.catalog_fdw(text, text, text, text, text, text, text);
DROP SCHEMA IF EXISTS iceberg_toolkit;
DROP TABLE IF EXISTS sales_data;
DROP TABLE IF EXISTS user_profile;
DROP TABLE IF EXISTS test_table;
DROP DROP FOREIGN TABLE IF EXISTS basic_catalog;
DROP DROP FOREIGN TABLE IF EXISTS basic_volume;
DROP USER MAPPING IF EXISTS FOR current_user SERVER basic_volume_server;
DROP USER MAPPING IF EXISTS FOR current_user SERVER basic_catalog_server;
DROP SERVER IF EXISTS basic_volume_server;
DROP SERVER IF EXISTS basic_catalog_server;
