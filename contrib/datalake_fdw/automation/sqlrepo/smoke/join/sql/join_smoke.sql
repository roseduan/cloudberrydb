-- Cross Foreign Table JOIN Smoke Test
-- Purpose: Verify JOINs between different foreign table types
-- Tests: Hive×S3, Iceberg×Hive, Iceberg×S3, Iceberg×Iceberg, foreign×local

-- Clean up previous run leftovers
DROP SERVER IF EXISTS join_s3_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup (inline to avoid \i path issues with pg_regress)
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: S3 server
-- ============================================================
CREATE SERVER join_s3_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'hadoop', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER join_s3_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Setup: Iceberg builtin catalog + volume
-- ============================================================
CREATE SERVER join_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER join_catalog_server;
CREATE FOREIGN CATALOG join_catalog SERVER join_catalog_server;
SET iceberg_default_catalog = 'join_catalog';

CREATE SERVER join_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER join_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME join_volume SERVER join_volume_server OPTIONS(base_path '/join_volume/');
SET iceberg_default_volume = 'join_volume';

-- ============================================================
-- Prepare: Iceberg tables
-- ============================================================
CREATE ICEBERG TABLE join_iceberg_orders (
    order_id bigint,
    customer_id int,
    amount decimal(10,2));

INSERT INTO join_iceberg_orders VALUES
    (1, 101, 500.00),
    (2, 102, 300.00),
    (3, 101, 750.00),
    (4, 103, 200.00),
    (5, 104, 100.00);

CREATE ICEBERG TABLE join_iceberg_customers (
    cust_id int,
    name text,
    region text);

INSERT INTO join_iceberg_customers VALUES
    (101, 'Alice', 'East'),
    (102, 'Bob', 'West'),
    (103, 'Charlie', 'East'),
    (105, 'Eve', 'North');

-- ============================================================
-- Prepare: Local PostgreSQL table
-- ============================================================
CREATE TABLE join_local_regions (
    region text,
    manager text);

INSERT INTO join_local_regions VALUES
    ('East', 'Manager_E'),
    ('West', 'Manager_W'),
    ('South', 'Manager_S');

-- ============================================================
-- Test 1: Iceberg × Iceberg JOIN
-- ============================================================
SELECT o.order_id, c.name, o.amount
FROM join_iceberg_orders o
JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
ORDER BY o.order_id;

-- LEFT JOIN: orders without matching customer
SELECT o.order_id, o.customer_id, c.name
FROM join_iceberg_orders o
LEFT JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
ORDER BY o.order_id;

-- RIGHT JOIN: customers without orders
SELECT c.name, o.order_id
FROM join_iceberg_orders o
RIGHT JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
ORDER BY c.name;

-- ============================================================
-- Test 2: Iceberg × Local table JOIN
-- ============================================================
SELECT c.name, r.manager
FROM join_iceberg_customers c
JOIN join_local_regions r ON c.region = r.region
ORDER BY c.name;

-- LEFT JOIN: customers in regions without managers
SELECT c.name, c.region, r.manager
FROM join_iceberg_customers c
LEFT JOIN join_local_regions r ON c.region = r.region
ORDER BY c.name;

-- ============================================================
-- Test 3: Three-way JOIN (Iceberg × Iceberg × Local)
-- ============================================================
SELECT o.order_id, c.name, o.amount, r.manager
FROM join_iceberg_orders o
JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
JOIN join_local_regions r ON c.region = r.region
ORDER BY o.order_id;

-- ============================================================
-- Test 4: Aggregation with cross-table JOIN
-- ============================================================
SELECT c.name, COUNT(*) AS order_count, SUM(o.amount) AS total_amount
FROM join_iceberg_orders o
JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
GROUP BY c.name
ORDER BY total_amount DESC;

-- By region via 3-way join
SELECT r.manager, COUNT(*) AS order_count, SUM(o.amount) AS region_total
FROM join_iceberg_orders o
JOIN join_iceberg_customers c ON o.customer_id = c.cust_id
JOIN join_local_regions r ON c.region = r.region
GROUP BY r.manager
ORDER BY r.manager;

-- ============================================================
-- Test 5: Subquery with cross-table reference
-- ============================================================
-- Customers who spent more than 400 total
SELECT c.name, c.region
FROM join_iceberg_customers c
WHERE c.cust_id IN (
    SELECT customer_id
    FROM join_iceberg_orders
    GROUP BY customer_id
    HAVING SUM(amount) > 400
)
ORDER BY c.name;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE join_iceberg_orders;
DROP TABLE join_iceberg_customers;
DROP TABLE join_local_regions;

DROP VOLUME join_volume;
DROP USER MAPPING FOR current_user SERVER join_volume_server;
DROP SERVER join_volume_server;
DROP CATALOG join_catalog;
DROP USER MAPPING FOR current_user SERVER join_catalog_server;
DROP SERVER join_catalog_server;

DROP USER MAPPING IF EXISTS FOR gpadmin SERVER join_s3_server;
DROP SERVER IF EXISTS join_s3_server;
