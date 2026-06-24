-- 01_rowgroup_pushdown.sql
-- Iceberg AM row-group min/max ("zone map") predicate pushdown.
--
-- Loads data in two batches with disjoint value ranges so the table spans
-- multiple Parquet data files (each a single row group).  The native Parquet
-- reader prunes whole row groups whose column min/max cannot satisfy the
-- WHERE clause.  Correctness is asserted by comparing results with pushdown
-- enabled vs disabled -- they must be identical (prune only, never drop rows).

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg row-group min/max pushdown');

-- Builtin catalog + S3 (MinIO) volume.
DROP SERVER IF EXISTS rgp_cat_srv CASCADE;
CREATE SERVER rgp_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER rgp_cat_srv;
CREATE FOREIGN CATALOG rgp_cat SERVER rgp_cat_srv;
SET iceberg_default_catalog='rgp_cat';

DROP SERVER IF EXISTS rgp_vol_srv CASCADE;
CREATE SERVER rgp_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER rgp_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME rgp_vol SERVER rgp_vol_srv OPTIONS(base_path '/rgp_vol/');
SET iceberg_default_volume='rgp_vol';

DROP TABLE IF EXISTS rgp_t;
CREATE ICEBERG TABLE rgp_t (id int, d date, ts timestamp, nm varchar(20));

-- Batch A: ids 1..100, year 2020, nm NOT NULL.
INSERT INTO rgp_t
SELECT g, DATE '2020-01-01' + g,
       TIMESTAMP '2020-01-01' + (g || ' days')::interval, 'a' || g
FROM generate_series(1, 100) g;

-- Batch B: ids 1001..1100, year 2024, nm all NULL.
INSERT INTO rgp_t
SELECT g, DATE '2024-01-01' + (g - 1000),
       TIMESTAMP '2024-01-01' + ((g - 1000) || ' days')::interval, NULL
FROM generate_series(1001, 1100) g;

SELECT count(*) AS total_rows FROM rgp_t;

-- Helper: run the same battery of predicates and report counts.  With pushdown
-- ON, row groups for the non-matching batch are skipped via min/max; results
-- must match the pushdown-OFF baseline exactly.
\set qbattery 'SELECT count(*) AS c_int_range FROM rgp_t WHERE id BETWEEN 1001 AND 1100; SELECT count(*) AS c_int_eq FROM rgp_t WHERE id = 50; SELECT count(*) AS c_int_gt FROM rgp_t WHERE id > 5000; SELECT count(*) AS c_date_ge FROM rgp_t WHERE d >= DATE ''2024-01-01''; SELECT count(*) AS c_date_lt FROM rgp_t WHERE d < DATE ''2021-01-01''; SELECT count(*) AS c_ts_lt FROM rgp_t WHERE ts < TIMESTAMP ''2021-01-01''; SELECT count(*) AS c_isnull FROM rgp_t WHERE nm IS NULL; SELECT count(*) AS c_isnotnull FROM rgp_t WHERE nm IS NOT NULL; SELECT count(*) AS c_text_eq FROM rgp_t WHERE nm = ''a50''; SELECT count(*) AS c_text_none FROM rgp_t WHERE nm = ''zzz''; SELECT count(*) AS c_in_hit FROM rgp_t WHERE id IN (50, 1050); SELECT count(*) AS c_in_none FROM rgp_t WHERE id IN (8000, 9000);'

SELECT test_log('--- pushdown ON ---');
SET gp_external_enable_filter_pushdown = on;
:qbattery

SELECT test_log('--- pushdown OFF (control: results must be identical) ---');
SET gp_external_enable_filter_pushdown = off;
:qbattery

RESET gp_external_enable_filter_pushdown;

-- Plan shape: predicate pushed into the Iceberg Custom Scan.
EXPLAIN (COSTS OFF) SELECT count(*) FROM rgp_t WHERE id BETWEEN 1001 AND 1100;

DROP TABLE rgp_t;
DROP SERVER rgp_cat_srv CASCADE;
DROP SERVER rgp_vol_srv CASCADE;
