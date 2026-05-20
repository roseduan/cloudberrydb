-- Hadoop-catalog over HDFS volume basic functional smoke for Iceberg AM.
--
-- Routes through the new HDFS volume helper (processVolumeHdfsResource)
-- and the iceberg HadoopCatalog with `fs.defaultFS = hdfs://...` and
-- `RequestContext.path = <warehouse-path>` set by
-- processS3OrHadoopServerResource.

\i ../../../lib/sql/common_setup.sql

DROP SERVER IF EXISTS hd_hdfs_cat_srv CASCADE;
CREATE SERVER hd_hdfs_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_cat_srv;
CREATE FOREIGN CATALOG hd_hdfs_cat SERVER hd_hdfs_cat_srv
    OPTIONS (warehouse_location_prefix 'hdfs://hadoop:8020/iceberg_hadoop_hdfs_smoke/');
SET iceberg_default_catalog = 'hd_hdfs_cat';

DROP SERVER IF EXISTS hd_hdfs_vol_srv CASCADE;
CREATE SERVER hd_hdfs_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', endpoint 'hdfs://hadoop:8020');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_vol_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hd_hdfs_vol SERVER hd_hdfs_vol_srv
    OPTIONS (base_path '/iceberg_hadoop_hdfs_smoke/', allow_writes 'true');
SET iceberg_default_volume = 'hd_hdfs_vol';

DROP TABLE IF EXISTS hd_hdfs_smoke;
CREATE ICEBERG TABLE hd_hdfs_smoke (id INT, name VARCHAR(50), val DECIMAL(10,2));

\echo === T1 INSERT seed (5 rows) ===
INSERT INTO hd_hdfs_smoke
    VALUES (1, 'alpha',   1.50),
           (2, 'beta',    2.75),
           (3, 'gamma',   3.14),
           (4, 'delta',   4.20),
           (5, 'epsilon', 5.00);
SELECT count(*) AS after_seed FROM hd_hdfs_smoke;

\echo === T2 single-row UPDATE id=2 ===
UPDATE hd_hdfs_smoke SET val = 99.99 WHERE id = 2;
SELECT id, name, val FROM hd_hdfs_smoke WHERE id = 2;

\echo === T3 single-row DELETE id=4 ===
DELETE FROM hd_hdfs_smoke WHERE id = 4;
SELECT count(*) AS after_delete FROM hd_hdfs_smoke;

\echo === T4 incremental INSERT (2 rows) ===
INSERT INTO hd_hdfs_smoke VALUES (6, 'zeta', 6.66), (7, 'eta', 7.77);
SELECT count(*) AS after_second_insert FROM hd_hdfs_smoke;

\echo === T5 multi-row UPDATE id<=3 (3 rows) ===
UPDATE hd_hdfs_smoke SET name = name || '_v2' WHERE id <= 3;

\echo === final ordered state ===
SELECT id, name, val FROM hd_hdfs_smoke ORDER BY id;

\echo === final aggregate ===
SELECT count(*) AS final_rows,
       count(*) FILTER (WHERE name LIKE '%\_v2' ESCAPE '\') AS rows_with_v2_suffix,
       sum(val) AS sum_val
FROM hd_hdfs_smoke;

DROP TABLE hd_hdfs_smoke;
