-- Iceberg + Polaris Catalog: Data Types and Advanced Queries Test
-- Tests multiple data types, aggregation, JOINs, and complex queries
-- using Polaris REST catalog backend and S3/MinIO storage.

-- Load common setup for datalake_fdw
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET datestyle = ISO, MDY;

-- ============================================================
-- Setup: Polaris Catalog Server + S3 Volume Server
-- ============================================================

-- Volume server
CREATE SERVER types_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-west-2',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER types_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME types_volume SERVER types_volume_server OPTIONS(base_path '/');
SET iceberg_default_volume = 'types_volume';

-- Catalog server (Polaris REST API)
CREATE SERVER types_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'polaris',
    url 'http://polaris:8181/api/catalog'
);
CREATE USER MAPPING FOR current_user
SERVER types_catalog_server
OPTIONS (
    client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL'
);
CREATE FOREIGN CATALOG types_catalog
SERVER types_catalog_server
OPTIONS (
    catalog_name 'polaris_default_catalog'
);
SET iceberg_default_catalog = 'types_catalog';

-- ============================================================
-- Test 1: Integer types
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_int_types (
    id bigint,
    int_col int,
    bigint_col bigint,
    smallint_col int)
OPTIONS (namespace 'public', table 'iceberg_polaris_int_types');

INSERT INTO iceberg_polaris_int_types VALUES
    (1, 2147483647, 9223372036854775807, 32767),
    (2, -2147483648, -9223372036854775808, -32768),
    (3, 0, 0, 0),
    (4, NULL, NULL, NULL);

SELECT * FROM iceberg_polaris_int_types ORDER BY id;

-- Boundary filters
SELECT id FROM iceberg_polaris_int_types WHERE int_col = 2147483647;
SELECT id FROM iceberg_polaris_int_types WHERE bigint_col < 0;

DROP TABLE iceberg_polaris_int_types;

-- ============================================================
-- Test 2: Floating point and decimal types
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_float_types (
    id bigint,
    float_col float,
    double_col double precision,
    decimal_col decimal(12,5))
OPTIONS (namespace 'public', table 'iceberg_polaris_float_types');

INSERT INTO iceberg_polaris_float_types VALUES
    (1, 3.14, 3.14159265358979, 12345.67890),
    (2, -0.001, -0.00000001, -99999.99999),
    (3, 0.0, 0.0, 0.00000),
    (4, NULL, NULL, NULL);

SELECT * FROM iceberg_polaris_float_types ORDER BY id;

SELECT id, decimal_col FROM iceberg_polaris_float_types
WHERE decimal_col > 0 ORDER BY id;

DROP TABLE iceberg_polaris_float_types;

-- ============================================================
-- Test 3: Text and boolean types
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_text_types (
    id bigint,
    text_col text,
    bool_col boolean)
OPTIONS (namespace 'public', table 'iceberg_polaris_text_types');

INSERT INTO iceberg_polaris_text_types VALUES
    (1, 'hello world', true),
    (2, '', false),
    (3, 'special chars: !@#$%^&*()', true),
    (4, NULL, NULL);

SELECT * FROM iceberg_polaris_text_types ORDER BY id;

-- Text filters
SELECT id, text_col FROM iceberg_polaris_text_types
WHERE text_col LIKE '%world%' ORDER BY id;
SELECT id, text_col FROM iceberg_polaris_text_types
WHERE text_col = '' ORDER BY id;
SELECT id, bool_col FROM iceberg_polaris_text_types
WHERE bool_col = true ORDER BY id;

DROP TABLE iceberg_polaris_text_types;

-- ============================================================
-- Test 4: Date type
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_date_types (
    id bigint,
    date_col date)
OPTIONS (namespace 'public', table 'iceberg_polaris_date_types');

INSERT INTO iceberg_polaris_date_types VALUES
    (1, '2024-01-01'),
    (2, '2024-06-15'),
    (3, '2024-12-31'),
    (4, NULL);

SELECT * FROM iceberg_polaris_date_types ORDER BY id;

-- Date range filter
SELECT id, date_col FROM iceberg_polaris_date_types
WHERE date_col BETWEEN '2024-01-01' AND '2024-06-30'
ORDER BY id;

DROP TABLE iceberg_polaris_date_types;

-- ============================================================
-- Test 5: Comprehensive aggregation
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_sales (
    sale_id bigint,
    region text,
    product text,
    amount decimal(10,2),
    quantity int)
OPTIONS (namespace 'public', table 'iceberg_polaris_sales');

INSERT INTO iceberg_polaris_sales VALUES
    (1, 'East', 'Widget', 100.00, 10),
    (2, 'East', 'Gadget', 250.00, 5),
    (3, 'West', 'Widget', 120.00, 8),
    (4, 'West', 'Gadget', 300.00, 3),
    (5, 'East', 'Widget', 80.00, 15),
    (6, 'West', 'Widget', 110.00, 12),
    (7, 'North', 'Gadget', 275.00, 7),
    (8, 'North', 'Widget', 90.00, 20);

-- Total by region
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total_amount
FROM iceberg_polaris_sales
GROUP BY region ORDER BY region;

-- Total by product
SELECT product, SUM(quantity) AS total_qty, SUM(amount) AS total_amount
FROM iceberg_polaris_sales
GROUP BY product ORDER BY product;

-- Two-level grouping
SELECT region, product, SUM(amount) AS total
FROM iceberg_polaris_sales
GROUP BY region, product
ORDER BY region, product;

-- HAVING filter
SELECT region, SUM(amount) AS total
FROM iceberg_polaris_sales
GROUP BY region
HAVING SUM(amount) > 400
ORDER BY region;

-- Top 3 by amount
SELECT sale_id, region, product, amount
FROM iceberg_polaris_sales
ORDER BY amount DESC LIMIT 3;

-- Distinct
SELECT DISTINCT region FROM iceberg_polaris_sales ORDER BY region;
SELECT DISTINCT product FROM iceberg_polaris_sales ORDER BY product;

DROP TABLE iceberg_polaris_sales;

-- ============================================================
-- Test 6: JOIN between Iceberg and local table
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_employees (
    emp_id bigint,
    name text,
    dept_id int)
OPTIONS (namespace 'public', table 'iceberg_polaris_employees');

CREATE TABLE local_departments (
    dept_id int,
    dept_name text);

INSERT INTO iceberg_polaris_employees VALUES
    (1, 'Alice', 10),
    (2, 'Bob', 20),
    (3, 'Charlie', 10),
    (4, 'Dave', 30),
    (5, 'Eve', NULL);

INSERT INTO local_departments VALUES
    (10, 'Engineering'),
    (20, 'Marketing'),
    (40, 'Finance');

-- INNER JOIN
SELECT e.emp_id, e.name, d.dept_name
FROM iceberg_polaris_employees e
JOIN local_departments d ON e.dept_id = d.dept_id
ORDER BY e.emp_id;

-- LEFT JOIN (unmatched employees)
SELECT e.emp_id, e.name, d.dept_name
FROM iceberg_polaris_employees e
LEFT JOIN local_departments d ON e.dept_id = d.dept_id
ORDER BY e.emp_id;

-- Aggregate with JOIN
SELECT d.dept_name, COUNT(*) AS emp_count
FROM iceberg_polaris_employees e
JOIN local_departments d ON e.dept_id = d.dept_id
GROUP BY d.dept_name
ORDER BY d.dept_name;

-- Subquery: employees in departments with more than 1 person
SELECT e.name FROM iceberg_polaris_employees e
WHERE e.dept_id IN (
    SELECT dept_id FROM iceberg_polaris_employees
    WHERE dept_id IS NOT NULL
    GROUP BY dept_id HAVING COUNT(*) > 1
)
ORDER BY e.name;

DROP TABLE iceberg_polaris_employees;
DROP TABLE local_departments;

-- ============================================================
-- Test 7: Two Iceberg tables JOIN
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_orders (
    order_id bigint,
    customer_id int,
    order_date date,
    total decimal(10,2))
OPTIONS (namespace 'public', table 'iceberg_polaris_orders');

CREATE ICEBERG TABLE iceberg_polaris_customers (
    customer_id int,
    name text,
    city text)
OPTIONS (namespace 'public', table 'iceberg_polaris_customers');

INSERT INTO iceberg_polaris_customers VALUES
    (1, 'Alice', 'Beijing'),
    (2, 'Bob', 'Shanghai'),
    (3, 'Charlie', 'Shenzhen');

INSERT INTO iceberg_polaris_orders VALUES
    (101, 1, '2024-01-10', 500.00),
    (102, 2, '2024-01-15', 300.00),
    (103, 1, '2024-02-20', 750.00),
    (104, 3, '2024-03-05', 200.00),
    (105, 2, '2024-03-10', 150.00);

-- JOIN two Iceberg tables
SELECT c.name, o.order_id, o.total
FROM iceberg_polaris_orders o
JOIN iceberg_polaris_customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id;

-- Customer order summary
SELECT c.name, c.city, COUNT(*) AS order_count, SUM(o.total) AS total_spent
FROM iceberg_polaris_orders o
JOIN iceberg_polaris_customers c ON o.customer_id = c.customer_id
GROUP BY c.name, c.city
ORDER BY total_spent DESC;

-- Orders in date range
SELECT c.name, o.order_id, o.order_date, o.total
FROM iceberg_polaris_orders o
JOIN iceberg_polaris_customers c ON o.customer_id = c.customer_id
WHERE o.order_date BETWEEN '2024-02-01' AND '2024-03-31'
ORDER BY o.order_date;

DROP TABLE iceberg_polaris_orders;
DROP TABLE iceberg_polaris_customers;

-- ============================================================
-- Test 8: UPDATE and DELETE with complex conditions
-- ============================================================
CREATE ICEBERG TABLE iceberg_polaris_inventory (
    item_id bigint,
    name text,
    category text,
    price decimal(10,2),
    stock int)
OPTIONS (namespace 'public', table 'iceberg_polaris_inventory');

INSERT INTO iceberg_polaris_inventory VALUES
    (1, 'Laptop', 'Electronics', 999.99, 50),
    (2, 'Mouse', 'Electronics', 29.99, 200),
    (3, 'Desk', 'Furniture', 299.99, 30),
    (4, 'Chair', 'Furniture', 199.99, 45),
    (5, 'Pen', 'Office', 2.99, 1000),
    (6, 'Notebook', 'Office', 5.99, 500);

-- Verify initial data
SELECT * FROM iceberg_polaris_inventory ORDER BY item_id;

-- UPDATE with complex WHERE
UPDATE iceberg_polaris_inventory SET price = price * 0.9
WHERE category = 'Electronics';
SELECT item_id, name, price FROM iceberg_polaris_inventory
WHERE category = 'Electronics' ORDER BY item_id;

-- UPDATE multiple columns
UPDATE iceberg_polaris_inventory SET stock = stock - 10, name = 'Standing Desk'
WHERE item_id = 3;
SELECT item_id, name, stock FROM iceberg_polaris_inventory
WHERE item_id = 3;

-- DELETE with IN condition
DELETE FROM iceberg_polaris_inventory WHERE category IN ('Office');
SELECT COUNT(*) FROM iceberg_polaris_inventory;

-- DELETE with comparison
DELETE FROM iceberg_polaris_inventory WHERE price > 500;
SELECT * FROM iceberg_polaris_inventory ORDER BY item_id;

DROP TABLE iceberg_polaris_inventory;

-- ============================================================
-- Cleanup
-- ============================================================
DROP VOLUME types_volume;
DROP USER MAPPING FOR current_user SERVER types_volume_server;
DROP SERVER types_volume_server;
DROP CATALOG types_catalog;
DROP USER MAPPING FOR current_user SERVER types_catalog_server;
DROP SERVER types_catalog_server;
