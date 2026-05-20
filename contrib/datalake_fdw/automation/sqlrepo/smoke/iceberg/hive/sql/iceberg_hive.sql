-- Iceberg + Hive Catalog Smoke Test
-- Tests basic Iceberg functionality using Hive Metastore as the catalog backend
-- and S3/MinIO as the storage layer.

-- Load common setup for datalake_fdw
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: Hive Catalog Server + S3 Volume Server
-- ============================================================

-- Catalog server (Hive Metastore backend)
CREATE SERVER hive_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'hive',
    url 'thrift://hive-metastore:9083'
);
CREATE USER MAPPING FOR current_user SERVER hive_catalog_server;
CREATE FOREIGN CATALOG hive_catalog SERVER hive_catalog_server;
SET iceberg_default_catalog = 'hive_catalog';

-- Volume server (S3/MinIO for data storage)
CREATE SERVER hive_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER hive_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME hive_volume SERVER hive_volume_server OPTIONS(base_path '/hive_volume/');
SET iceberg_default_volume = 'hive_volume';

-- ============================================================
-- Test 1: Basic CRUD operations
-- ============================================================
CREATE ICEBERG TABLE iceberg_hive_basic (
    id bigint,
    name text);

-- Empty table scan
SELECT * FROM iceberg_hive_basic;
SELECT COUNT(*) FROM iceberg_hive_basic;

-- INSERT: single row
INSERT INTO iceberg_hive_basic VALUES (1, 'Alice');
-- INSERT: multiple rows
INSERT INTO iceberg_hive_basic VALUES (2, 'Bob'), (3, 'Charlie');
-- INSERT: NULL value
INSERT INTO iceberg_hive_basic VALUES (4, NULL);
-- INSERT: from subquery
INSERT INTO iceberg_hive_basic SELECT 5, 'David';

-- SELECT: full scan ordered
SELECT * FROM iceberg_hive_basic ORDER BY id;
-- SELECT: NULL filter
SELECT * FROM iceberg_hive_basic WHERE name IS NULL;
-- SELECT: NOT NULL filter
SELECT * FROM iceberg_hive_basic WHERE name IS NOT NULL ORDER BY id;
-- SELECT: count
SELECT COUNT(*) FROM iceberg_hive_basic;

-- UPDATE: single row by id
UPDATE iceberg_hive_basic SET name = 'Alice Updated' WHERE id = 1;
-- UPDATE: set to NULL
UPDATE iceberg_hive_basic SET name = NULL WHERE id = 2;
-- UPDATE: by NULL condition
UPDATE iceberg_hive_basic SET name = 'Updated' WHERE name IS NULL;
SELECT * FROM iceberg_hive_basic ORDER BY id;

-- DELETE: by id
DELETE FROM iceberg_hive_basic WHERE id = 3;
-- DELETE: by NULL condition (should match no rows after above updates)
DELETE FROM iceberg_hive_basic WHERE name IS NULL;
SELECT * FROM iceberg_hive_basic ORDER BY id;

DROP TABLE iceberg_hive_basic;

-- ============================================================
-- Test 2: Multiple data types
-- ============================================================
CREATE ICEBERG TABLE iceberg_hive_types (
    id bigint,
    int_col int,
    float_col float,
    double_col double precision,
    decimal_col decimal(10,2),
    text_col text,
    bool_col boolean,
    date_col date);

INSERT INTO iceberg_hive_types VALUES
    (1, 100, 1.5, 2.5, 99.99, 'hello', true, '2024-01-15'),
    (2, -100, -1.5, -2.5, -99.99, 'world', false, '2024-06-30'),
    (3, 0, 0.0, 0.0, 0.00, '', true, '2024-12-31'),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT * FROM iceberg_hive_types ORDER BY id;

-- Type-specific filters
SELECT id, int_col FROM iceberg_hive_types WHERE int_col > 0 ORDER BY id;
SELECT id, bool_col FROM iceberg_hive_types WHERE bool_col = true ORDER BY id;
SELECT id, date_col FROM iceberg_hive_types WHERE date_col >= '2024-06-01' ORDER BY id;
SELECT id, decimal_col FROM iceberg_hive_types WHERE decimal_col < 0 ORDER BY id;
SELECT id, text_col FROM iceberg_hive_types WHERE text_col = '' ORDER BY id;

-- NULL checks across types
SELECT id FROM iceberg_hive_types WHERE int_col IS NULL AND float_col IS NULL AND text_col IS NULL ORDER BY id;

DROP TABLE iceberg_hive_types;

-- ============================================================
-- Test 3: Aggregation queries
-- ============================================================
CREATE ICEBERG TABLE iceberg_hive_agg (
    id bigint,
    category text,
    amount decimal(10,2));

INSERT INTO iceberg_hive_agg VALUES
    (1, 'A', 100.00),
    (2, 'A', 200.00),
    (3, 'B', 150.00),
    (4, 'B', 250.00),
    (5, 'C', 300.00);

-- Basic aggregates
SELECT COUNT(*) FROM iceberg_hive_agg;
SELECT category, COUNT(*) AS cnt, SUM(amount) AS total,
       MIN(amount) AS min_amt, MAX(amount) AS max_amt
FROM iceberg_hive_agg GROUP BY category ORDER BY category;

-- Filter + aggregate
SELECT category, SUM(amount) AS total
FROM iceberg_hive_agg WHERE amount > 150 GROUP BY category ORDER BY category;

-- IN filter
SELECT * FROM iceberg_hive_agg WHERE category IN ('A', 'C') ORDER BY id;

-- BETWEEN filter
SELECT * FROM iceberg_hive_agg WHERE amount BETWEEN 150.00 AND 250.00 ORDER BY id;

-- ORDER BY + LIMIT
SELECT * FROM iceberg_hive_agg ORDER BY amount DESC LIMIT 3;

DROP TABLE iceberg_hive_agg;

-- ============================================================
-- Test 4: JOIN with local table
-- ============================================================
CREATE ICEBERG TABLE iceberg_hive_orders (
    order_id bigint,
    customer text,
    total decimal(10,2));

CREATE TABLE local_customers (
    name text,
    city text);

INSERT INTO iceberg_hive_orders VALUES
    (1, 'Alice', 100.00),
    (2, 'Bob', 200.00),
    (3, 'Charlie', 150.00);

INSERT INTO local_customers VALUES
    ('Alice', 'Beijing'),
    ('Bob', 'Shanghai'),
    ('Dave', 'Shenzhen');

-- INNER JOIN
SELECT o.order_id, o.customer, o.total, c.city
FROM iceberg_hive_orders o
JOIN local_customers c ON o.customer = c.name
ORDER BY o.order_id;

-- LEFT JOIN
SELECT o.order_id, o.customer, c.city
FROM iceberg_hive_orders o
LEFT JOIN local_customers c ON o.customer = c.name
ORDER BY o.order_id;

-- RIGHT JOIN
SELECT o.order_id, c.name, c.city
FROM iceberg_hive_orders o
RIGHT JOIN local_customers c ON o.customer = c.name
ORDER BY c.name;

-- JOIN with filter
SELECT o.order_id, o.customer, o.total
FROM iceberg_hive_orders o
JOIN local_customers c ON o.customer = c.name
WHERE o.total > 100
ORDER BY o.order_id;

DROP TABLE iceberg_hive_orders;
DROP TABLE local_customers;

-- ============================================================
-- Test 5: Multiple Iceberg tables interaction
-- ============================================================
CREATE ICEBERG TABLE iceberg_hive_products (
    product_id bigint,
    product_name text,
    price decimal(10,2));

CREATE ICEBERG TABLE iceberg_hive_sales (
    sale_id bigint,
    product_id bigint,
    quantity int);

INSERT INTO iceberg_hive_products VALUES
    (1, 'Laptop', 1299.99),
    (2, 'Mouse', 29.99),
    (3, 'Keyboard', 79.99);

INSERT INTO iceberg_hive_sales VALUES
    (1, 1, 5),
    (2, 2, 20),
    (3, 1, 3),
    (4, 3, 10);

-- JOIN two Iceberg tables
SELECT p.product_name, p.price, s.quantity, (p.price * s.quantity) AS line_total
FROM iceberg_hive_products p
JOIN iceberg_hive_sales s ON p.product_id = s.product_id
ORDER BY s.sale_id;

-- Aggregate across Iceberg tables
SELECT p.product_name, SUM(s.quantity) AS total_qty
FROM iceberg_hive_products p
JOIN iceberg_hive_sales s ON p.product_id = s.product_id
GROUP BY p.product_name
ORDER BY p.product_name;

DROP TABLE iceberg_hive_sales;
DROP TABLE iceberg_hive_products;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME hive_volume;
DROP USER MAPPING FOR current_user SERVER hive_volume_server;
DROP SERVER hive_volume_server;
DROP CATALOG hive_catalog;
DROP USER MAPPING FOR current_user SERVER hive_catalog_server;
DROP SERVER hive_catalog_server;
