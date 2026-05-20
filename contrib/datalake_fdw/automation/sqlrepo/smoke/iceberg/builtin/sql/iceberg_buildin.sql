CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER default_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER default_catalog_server;
CREATE FOREIGN CATALOG default_catalog SERVER default_catalog_server;
set iceberg_default_catalog='default_catalog';
-- volume
CREATE SERVER default_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER default_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');

CREATE FOREIGN VOLUME default_volume SERVER default_volume_server OPTIONS(base_path '/default_volume/');
set iceberg_default_volume='default_volume';
-- create iceberg table
CREATE ICEBERG TABLE iceberg_example_table (
    id bigint,
    name text);

-- Test empty table
SELECT * FROM iceberg_example_table;
SELECT COUNT(*) FROM iceberg_example_table;

-- INSERT tests
INSERT INTO iceberg_example_table VALUES (1, 'Alice');
INSERT INTO iceberg_example_table VALUES (2, 'Bob'), (3, 'Charlie');
INSERT INTO iceberg_example_table VALUES (4, NULL);
INSERT INTO iceberg_example_table SELECT 5, 'David';

-- SELECT tests
SELECT * FROM iceberg_example_table ORDER BY id;
SELECT * FROM iceberg_example_table WHERE name IS NULL;
SELECT * FROM iceberg_example_table WHERE name IS NOT NULL ORDER BY id;
SELECT COUNT(*) FROM iceberg_example_table;

-- UPDATE tests
UPDATE iceberg_example_table SET name = 'Alice Updated' WHERE id = 1;
UPDATE iceberg_example_table SET name = NULL WHERE id = 2;
UPDATE iceberg_example_table SET name = 'Updated' WHERE name IS NULL;
SELECT * FROM iceberg_example_table ORDER BY id;

-- DELETE tests
DELETE FROM iceberg_example_table WHERE id = 3;
DELETE FROM iceberg_example_table WHERE name IS NULL;
SELECT * FROM iceberg_example_table ORDER BY id;

-- cleanup
DROP TABLE iceberg_example_table;
DROP VOLUME default_volume;
DROP USER MAPPING FOR current_user SERVER default_volume_server;
DROP SERVER default_volume_server;
DROP CATALOG default_catalog;
DROP USER MAPPING FOR current_user SERVER default_catalog_server;
DROP SERVER default_catalog_server;

