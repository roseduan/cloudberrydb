-- S3-catalog basic functional smoke for Iceberg AM.
--
-- Exercises CREATE / INSERT / SELECT / UPDATE / DELETE / multi-row UPDATE
-- end-to-end against an iceberg HadoopCatalog backed by S3-compatible
-- object storage (MinIO).  Regression coverage for the changes that turned
-- type='s3' from "fs.prefix not set; warehouse becomes null/" into a real
-- working catalog by:
--   - splitting `warehouse_location_prefix` into `fs.defaultFS` + `fs.prefix`
--     in `IcebergRestController.processS3OrHadoopServerResource`;
--   - passing null for `location` in `IcebergS3Catalog.createTable` so
--     iceberg's path-based HadoopCatalog computes `<warehouse>/<ns>/<table>`
--     instead of rejecting the volume's URL;
--   - setting `dataSource = <ns>.<table>` in `createRequestContext` so the
--     agent doesn't fall back to the warehouse URL when looking up tables.

\i ../../../lib/sql/common_setup.sql

-- ===== S3 catalog server + S3 volume =====
DROP SERVER IF EXISTS s3_smoke_cat_srv CASCADE;
CREATE SERVER s3_smoke_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER s3_smoke_cat_srv;
CREATE FOREIGN CATALOG s3_smoke_cat SERVER s3_smoke_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_s3_smoke/');
SET iceberg_default_catalog = 's3_smoke_cat';

DROP SERVER IF EXISTS s3_smoke_vol_srv CASCADE;
CREATE SERVER s3_smoke_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER s3_smoke_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME s3_smoke_vol SERVER s3_smoke_vol_srv
    OPTIONS (base_path '/iceberg_s3_smoke/', allow_writes 'true');
SET iceberg_default_volume = 's3_smoke_vol';

DROP TABLE IF EXISTS s3_smoke;
CREATE ICEBERG TABLE s3_smoke (id INT, name VARCHAR(50), val DECIMAL(10,2));

\echo === T1 INSERT seed (5 rows) ===
INSERT INTO s3_smoke
    VALUES (1, 'alpha',   1.50),
           (2, 'beta',    2.75),
           (3, 'gamma',   3.14),
           (4, 'delta',   4.20),
           (5, 'epsilon', 5.00);
SELECT count(*) AS after_seed FROM s3_smoke;

\echo === T2 single-row UPDATE id=2 ===
UPDATE s3_smoke SET val = 99.99 WHERE id = 2;
SELECT id, name, val FROM s3_smoke WHERE id = 2;

\echo === T3 single-row DELETE id=4 ===
DELETE FROM s3_smoke WHERE id = 4;
SELECT count(*) AS after_delete FROM s3_smoke;

\echo === T4 incremental INSERT (2 rows) ===
INSERT INTO s3_smoke VALUES (6, 'zeta', 6.66), (7, 'eta', 7.77);
SELECT count(*) AS after_second_insert FROM s3_smoke;

\echo === T5 multi-row UPDATE id<=3 (3 rows) ===
UPDATE s3_smoke SET name = name || '_v2' WHERE id <= 3;

\echo === final ordered state ===
SELECT id, name, val FROM s3_smoke ORDER BY id;

\echo === final aggregate ===
SELECT count(*) AS final_rows,
       count(*) FILTER (WHERE name LIKE '%\_v2' ESCAPE '\') AS rows_with_v2_suffix,
       sum(val) AS sum_val
FROM s3_smoke;

DROP TABLE s3_smoke;
