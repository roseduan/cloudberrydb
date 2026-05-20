-- TPC-H basic functional smoke for Iceberg AM
--
-- Verifies CREATE ICEBERG TABLE / INSERT / SELECT / UPDATE / DELETE work
-- end-to-end against a builtin catalog + S3 (MinIO) volume.

\i ../../../lib/sql/common_setup.sql

-- ===== Catalog (builtin) + Volume (S3/MinIO @ minio:9000) =====
DROP SERVER IF EXISTS am_tpch_cat_srv CASCADE;
CREATE SERVER am_tpch_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER am_tpch_cat_srv;
CREATE FOREIGN CATALOG am_tpch_cat SERVER am_tpch_cat_srv
    OPTIONS (default_namespace 'public');
SET iceberg_default_catalog = 'am_tpch_cat';

DROP SERVER IF EXISTS am_tpch_vol_srv CASCADE;
CREATE SERVER am_tpch_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER am_tpch_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME am_tpch_vol SERVER am_tpch_vol_srv
    OPTIONS (base_path '/iceberg_am_tpch/', allow_writes 'true');
SET iceberg_default_volume = 'am_tpch_vol';

-- ===== Schema =====
DROP TABLE IF EXISTS region   CASCADE;
DROP TABLE IF EXISTS nation   CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS part     CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS orders   CASCADE;
DROP TABLE IF EXISTS lineitem CASCADE;

CREATE ICEBERG TABLE region   (r_regionkey INT, r_name VARCHAR(25), r_comment VARCHAR(152));
CREATE ICEBERG TABLE nation   (n_nationkey INT, n_name VARCHAR(25), n_regionkey INT, n_comment VARCHAR(152));
CREATE ICEBERG TABLE supplier (s_suppkey INT, s_name VARCHAR(25), s_address VARCHAR(40),
                               s_nationkey INT, s_phone VARCHAR(15), s_acctbal DECIMAL(15,2),
                               s_comment VARCHAR(101));
CREATE ICEBERG TABLE part     (p_partkey INT, p_name VARCHAR(55), p_mfgr VARCHAR(25),
                               p_brand VARCHAR(10), p_type VARCHAR(25), p_size INT,
                               p_container VARCHAR(10), p_retailprice DECIMAL(15,2),
                               p_comment VARCHAR(23));
CREATE ICEBERG TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT,
                               ps_supplycost DECIMAL(15,2), ps_comment VARCHAR(199));
CREATE ICEBERG TABLE customer (c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(40),
                               c_nationkey INT, c_phone VARCHAR(15), c_acctbal DECIMAL(15,2),
                               c_mktsegment VARCHAR(10), c_comment VARCHAR(117));
CREATE ICEBERG TABLE orders   (o_orderkey BIGINT, o_custkey INT, o_orderstatus VARCHAR(1),
                               o_totalprice DECIMAL(15,2), o_orderdate DATE,
                               o_orderpriority VARCHAR(15), o_clerk VARCHAR(15),
                               o_shippriority INT, o_comment VARCHAR(79));
CREATE ICEBERG TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT,
                               l_linenumber INT, l_quantity DECIMAL(15,2),
                               l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2),
                               l_tax DECIMAL(15,2), l_returnflag VARCHAR(1),
                               l_linestatus VARCHAR(1), l_shipdate DATE,
                               l_commitdate DATE, l_receiptdate DATE,
                               l_shipinstruct VARCHAR(25), l_shipmode VARCHAR(10),
                               l_comment VARCHAR(44));

-- ===== Load (small synthetic dataset; TPC-H shaped) =====

INSERT INTO region
SELECT g, 'REGION_' || g, 'comment ' || g FROM generate_series(0, 4) g;

INSERT INTO nation
SELECT g, 'NATION_' || g, g % 5, 'comment ' || g FROM generate_series(0, 24) g;

INSERT INTO supplier
SELECT g, 'Supplier#' || lpad(g::text, 9, '0'), 'addr-' || g, g % 25,
       '22-000-' || lpad(g::text, 4, '0'), ((g * 31 % 100000) / 100.0)::decimal(15,2),
       'comment ' || g
FROM generate_series(1, 50) g;

INSERT INTO part
SELECT g, 'part ' || g, 'Mfgr#' || ((g % 5) + 1),
       'Brand#' || ((g % 25) + 1), 'TYPE_' || ((g % 15) + 1),
       (g % 50) + 1, 'CONT_' || ((g % 10) + 1),
       ((g * 17 % 200000) / 100.0)::decimal(15,2), 'c ' || g
FROM generate_series(1, 200) g;

INSERT INTO partsupp
SELECT p.g, ((p.g + s) % 50) + 1, (p.g * 7 + s * 13) % 9999,
       (((p.g * 29 + s * 11) % 100000) / 100.0)::decimal(15,2),
       'ps ' || p.g || '/' || s
FROM generate_series(1, 200) p(g), generate_series(0, 3) s;

INSERT INTO customer
SELECT g, 'Customer#' || lpad(g::text, 9, '0'), 'addr ' || g, g % 25,
       '21-000-' || lpad(g::text, 4, '0'),
       ((g * 41 % 1000000) / 100.0)::decimal(15,2),
       (ARRAY['BUILDING','AUTOMOBILE','MACHINERY','HOUSEHOLD','FURNITURE'])[((g % 5) + 1)],
       'comment ' || g
FROM generate_series(1, 150) g;

INSERT INTO orders
SELECT g::bigint, ((g * 13) % 150) + 1,
       (ARRAY['O','F','P'])[((g % 3) + 1)],
       ((g * 53 % 10000000) / 100.0)::decimal(15,2),
       DATE '1992-01-01' + ((g * 7) % 2400),
       (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[((g % 5) + 1)],
       'Clerk#' || lpad(((g % 100) + 1)::text, 9, '0'),
       (g % 3), 'order comment ' || g
FROM generate_series(1, 1500) g;

INSERT INTO lineitem
SELECT o.g::bigint, ((o.g * 17 + ln) % 200) + 1, ((o.g * 11 + ln) % 50) + 1, ln,
       (((o.g * 7 + ln) % 50) + 1)::decimal(15,2),
       (((o.g * 19 + ln * 3) % 500000) / 100.0)::decimal(15,2),
       ((((o.g * 3 + ln) % 11) / 100.0))::decimal(15,2),
       ((((o.g * 5 + ln) % 9) / 100.0))::decimal(15,2),
       (ARRAY['A','R','N'])[(((o.g * 2 + ln) % 3) + 1)],
       (ARRAY['O','F'])[((o.g % 2) + 1)],
       DATE '1992-01-01' + ((o.g * 7 + ln) % 2500),
       DATE '1992-01-01' + ((o.g * 7 + ln) % 2500) - 5,
       DATE '1992-01-01' + ((o.g * 7 + ln) % 2500) + 7,
       'DELIVER IN PERSON', 'AIR',
       'lineitem ' || o.g || '/' || ln
FROM generate_series(1, 1500) o(g), generate_series(1, 4) ln;

ANALYZE region;
ANALYZE nation;
ANALYZE supplier;
ANALYZE part;
ANALYZE partsupp;
ANALYZE customer;
ANALYZE orders;
ANALYZE lineitem;

-- ===== Functional checks =====

SELECT test_log('CHECK 1: row counts');
SELECT 'region' AS tbl, count(*) FROM region UNION ALL
SELECT 'nation',   count(*) FROM nation UNION ALL
SELECT 'supplier', count(*) FROM supplier UNION ALL
SELECT 'part',     count(*) FROM part UNION ALL
SELECT 'partsupp', count(*) FROM partsupp UNION ALL
SELECT 'customer', count(*) FROM customer UNION ALL
SELECT 'orders',   count(*) FROM orders UNION ALL
SELECT 'lineitem', count(*) FROM lineitem
ORDER BY tbl;

SELECT test_log('CHECK 2: filter + projection (lineitem)');
SELECT count(*)
FROM lineitem
WHERE l_shipdate BETWEEN DATE '1994-01-01' AND DATE '1994-12-31';

SELECT test_log('CHECK 3: star-join 4-way (lineitem x orders x customer x nation)');
SELECT n_name, count(*) AS line_count
FROM lineitem, orders, customer, nation
WHERE l_orderkey = o_orderkey
  AND o_custkey = c_custkey
  AND c_nationkey = n_nationkey
GROUP BY n_name
ORDER BY n_name
LIMIT 5;

SELECT test_log('CHECK 4: TPC-H Q1-style aggregate');
SELECT l_returnflag, l_linestatus,
       count(*)            AS count_order,
       sum(l_quantity)     AS sum_qty,
       round(avg(l_discount)::numeric, 4) AS avg_disc
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 day'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

SELECT test_log('CHECK 5: UPDATE + SELECT (ACID path)');
UPDATE lineitem SET l_discount = 0.99
WHERE l_orderkey = 1 AND l_linenumber = 1;
SELECT l_orderkey, l_linenumber, l_discount
FROM lineitem
WHERE l_orderkey = 1 AND l_linenumber = 1;

SELECT test_log('CHECK 6: DELETE + SELECT');
DELETE FROM lineitem WHERE l_orderkey = 1;
SELECT count(*) FROM lineitem WHERE l_orderkey = 1;

SELECT test_log('CHECK 7: EXPLAIN uses Iceberg custom scan');
EXPLAIN (COSTS OFF)
SELECT count(*) FROM lineitem WHERE l_shipdate > DATE '1996-01-01';

SELECT test_log('iceberg_am_tpch_basic smoke: all checks executed');
