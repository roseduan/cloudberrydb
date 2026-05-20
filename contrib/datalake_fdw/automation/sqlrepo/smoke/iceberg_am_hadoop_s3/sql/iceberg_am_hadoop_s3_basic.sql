-- Hadoop-catalog over s3 volume basic functional smoke for Iceberg AM.
--
-- Distinct from iceberg_am_s3 in that the catalog server type is 'hadoop'
-- (hits IcebergHadoopCatalog with `fs.defaultFS + catalogLocation` warehouse
-- contract) rather than 's3' (which goes through IcebergS3Catalog with
-- `fs.defaultFS + fs.prefix`).  Same iceberg-side HadoopCatalog under the
-- hood; both code paths must produce identical layout.

\i ../../../lib/sql/common_setup.sql

DROP SERVER IF EXISTS hd_s3_cat_srv CASCADE;
CREATE SERVER hd_s3_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_s3_cat_srv;
CREATE FOREIGN CATALOG hd_s3_cat SERVER hd_s3_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_hadoop_s3_smoke/');
SET iceberg_default_catalog = 'hd_s3_cat';

DROP SERVER IF EXISTS hd_s3_vol_srv CASCADE;
CREATE SERVER hd_s3_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (
        type 's3',
        endpoint 'http://minio:9000',
        region 'us-east-1',
        bucket_name 'warehouse',
        path_style_access 'true'
    );
CREATE USER MAPPING FOR current_user SERVER hd_s3_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME hd_s3_vol SERVER hd_s3_vol_srv
    OPTIONS (base_path '/iceberg_hadoop_s3_smoke/', allow_writes 'true');
SET iceberg_default_volume = 'hd_s3_vol';

DROP TABLE IF EXISTS hd_s3_smoke;
CREATE ICEBERG TABLE hd_s3_smoke (id INT, name VARCHAR(50), val DECIMAL(10,2));

\echo === T1 INSERT seed (5 rows) ===
INSERT INTO hd_s3_smoke
    VALUES (1, 'alpha',   1.50),
           (2, 'beta',    2.75),
           (3, 'gamma',   3.14),
           (4, 'delta',   4.20),
           (5, 'epsilon', 5.00);
SELECT count(*) AS after_seed FROM hd_s3_smoke;

\echo === T2 single-row UPDATE id=2 ===
UPDATE hd_s3_smoke SET val = 99.99 WHERE id = 2;
SELECT id, name, val FROM hd_s3_smoke WHERE id = 2;

\echo === T3 single-row DELETE id=4 ===
DELETE FROM hd_s3_smoke WHERE id = 4;
SELECT count(*) AS after_delete FROM hd_s3_smoke;

\echo === T4 incremental INSERT (2 rows) ===
INSERT INTO hd_s3_smoke VALUES (6, 'zeta', 6.66), (7, 'eta', 7.77);
SELECT count(*) AS after_second_insert FROM hd_s3_smoke;

\echo === T5 multi-row UPDATE id<=3 (3 rows) ===
UPDATE hd_s3_smoke SET name = name || '_v2' WHERE id <= 3;

\echo === final ordered state ===
SELECT id, name, val FROM hd_s3_smoke ORDER BY id;

\echo === final aggregate ===
SELECT count(*) AS final_rows,
       count(*) FILTER (WHERE name LIKE '%\_v2' ESCAPE '\') AS rows_with_v2_suffix,
       sum(val) AS sum_val
FROM hd_s3_smoke;

DROP TABLE hd_s3_smoke;
