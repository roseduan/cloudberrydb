-- Iceberg AM on Hadoop catalog (HDFS volume): same-transaction
-- read-your-own-writes regression (issue #338).
--
-- Hadoop catalog over HDFS shares the same execute_get_fragments_via_fdw
-- code path the #338 fix touched.  Even though storage is HDFS rather
-- than S3, the catalog-side RPC plumbing is the same and the bug shape
-- is the same.

CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET DateStyle = 'ISO, YMD';

DROP SERVER IF EXISTS hd_hdfs_acid_cat_srv CASCADE;
CREATE SERVER hd_hdfs_acid_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_acid_cat_srv;
CREATE FOREIGN CATALOG hd_hdfs_acid_cat SERVER hd_hdfs_acid_cat_srv
    OPTIONS (warehouse_location_prefix 'hdfs://hadoop:8020/iceberg_hadoop_hdfs_acid_double_update/');
SET iceberg_default_catalog = 'hd_hdfs_acid_cat';

DROP SERVER IF EXISTS hd_hdfs_acid_vol_srv CASCADE;
CREATE SERVER hd_hdfs_acid_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', endpoint 'hdfs://hadoop:8020');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_acid_vol_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hd_hdfs_acid_vol SERVER hd_hdfs_acid_vol_srv
    OPTIONS (base_path '/iceberg_hadoop_hdfs_acid_double_update/', allow_writes 'true');
SET iceberg_default_volume = 'hd_hdfs_acid_vol';

-- Case A: raw BEGIN/UPDATE/UPDATE/COMMIT
DROP TABLE IF EXISTS hd_hdfs_acid_raw;
CREATE ICEBERG TABLE hd_hdfs_acid_raw (id int, n int);
INSERT INTO hd_hdfs_acid_raw VALUES (1, 10);
BEGIN;
UPDATE hd_hdfs_acid_raw SET n = n + 1 WHERE id = 1;
UPDATE hd_hdfs_acid_raw SET n = n * 2 WHERE id = 1;
COMMIT;
SELECT id, n FROM hd_hdfs_acid_raw ORDER BY n;
SELECT COUNT(*) AS cnt, SUM(n) AS sumn FROM hd_hdfs_acid_raw;
DROP TABLE hd_hdfs_acid_raw;

-- Case B: plpgsql function double UPDATE
DROP TABLE IF EXISTS hd_hdfs_acid_pl;
CREATE ICEBERG TABLE hd_hdfs_acid_pl (id int, n int);
INSERT INTO hd_hdfs_acid_pl VALUES (1, 10);
CREATE OR REPLACE FUNCTION hd_hdfs_acid_upd2_pl() RETURNS bigint AS $$
DECLARE c bigint;
BEGIN
    UPDATE hd_hdfs_acid_pl SET n = n + 1 WHERE id = 1;
    UPDATE hd_hdfs_acid_pl SET n = n * 2 WHERE id = 1;
    SELECT count(*) INTO c FROM hd_hdfs_acid_pl WHERE id = 1;
    RETURN c;
END;
$$ LANGUAGE plpgsql;
SELECT hd_hdfs_acid_upd2_pl() AS rows_seen_inside_func;
SELECT id, n FROM hd_hdfs_acid_pl ORDER BY n;
SELECT COUNT(*) AS cnt, SUM(n) AS sumn FROM hd_hdfs_acid_pl;
DROP FUNCTION hd_hdfs_acid_upd2_pl();
DROP TABLE hd_hdfs_acid_pl;

-- Case C: sql function double UPDATE
DROP TABLE IF EXISTS hd_hdfs_acid_sql;
CREATE ICEBERG TABLE hd_hdfs_acid_sql (id int, n int);
INSERT INTO hd_hdfs_acid_sql VALUES (1, 10);
CREATE OR REPLACE FUNCTION hd_hdfs_acid_upd2_sql() RETURNS bigint AS $$
    UPDATE hd_hdfs_acid_sql SET n = n + 1 WHERE id = 1;
    UPDATE hd_hdfs_acid_sql SET n = n * 2 WHERE id = 1;
    SELECT count(*) FROM hd_hdfs_acid_sql WHERE id = 1;
$$ LANGUAGE sql;
SELECT hd_hdfs_acid_upd2_sql() AS rows_seen_inside_func;
SELECT id, n FROM hd_hdfs_acid_sql ORDER BY n;
SELECT COUNT(*) AS cnt, SUM(n) AS sumn FROM hd_hdfs_acid_sql;
DROP FUNCTION hd_hdfs_acid_upd2_sql();
DROP TABLE hd_hdfs_acid_sql;

-- Cleanup
DROP VOLUME hd_hdfs_acid_vol;
DROP USER MAPPING FOR current_user SERVER hd_hdfs_acid_vol_srv;
DROP SERVER hd_hdfs_acid_vol_srv;
DROP CATALOG hd_hdfs_acid_cat;
DROP USER MAPPING FOR current_user SERVER hd_hdfs_acid_cat_srv;
DROP SERVER hd_hdfs_acid_cat_srv;
