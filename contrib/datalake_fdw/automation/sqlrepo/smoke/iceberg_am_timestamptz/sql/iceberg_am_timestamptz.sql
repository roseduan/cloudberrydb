-- Iceberg AM timestamptz interop (issue #366).
--
-- CREATE ICEBERG TABLE used to declare PG timestamptz columns as Iceberg
-- "timestamp" (without zone) in the table schema, so external engines read
-- the stored UTC microseconds as a zone-less wall clock: Spark either
-- refused the scan ("Cannot handle timestamp without timezone fields") or,
-- with spark.sql.iceberg.handle-timestamp-without-timezone=true, displayed
-- a session-timezone-dependent wrong-looking value.  psql round trips never
-- noticed because our writer and reader are symmetric.
--
-- The column must be declared as Iceberg "timestamptz" (isAdjustedToUTC);
-- run.sh asserts that on the metadata.json and (when a Spark container is
-- available) cross-checks the value Spark reads.
--
-- Targets the lakehouse stack MinIO (endpoint http://minio:9000,
-- admin/admin12345) like iceberg_am_hadoop_s3.

\i ../../../lib/sql/common_setup.sql

SET client_min_messages = WARNING;
SET DateStyle = 'ISO, YMD';
SET timezone = 'Asia/Shanghai';

DROP SERVER IF EXISTS tz_cat_srv CASCADE;
CREATE SERVER tz_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER tz_cat_srv;
CREATE FOREIGN CATALOG tz_cat SERVER tz_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_am_timestamptz/');
SET iceberg_default_catalog = 'tz_cat';

DROP SERVER IF EXISTS tz_vol_srv CASCADE;
CREATE SERVER tz_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER tz_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME tz_vol SERVER tz_vol_srv
    OPTIONS (base_path '/iceberg_am_timestamptz/', allow_writes 'true');
SET iceberg_default_volume = 'tz_vol';

DROP TABLE IF EXISTS tz_repro;
CREATE ICEBERG TABLE tz_repro (id int, descr text, t timestamptz)
  OPTIONS (namespace 'demo', table 'tz_repro');

INSERT INTO tz_repro VALUES
  (1, 'New Year 2024 02:00 +08 (Beijing)', timestamptz '2024-01-01 02:00:00+08'),
  (2, 'UTC noon', timestamptz '2024-06-15 12:00:00+00'),
  (3, 'NY morning', timestamptz '2024-03-10 08:30:00-05');

SELECT id, descr, t FROM tz_repro ORDER BY id;

-- instants survive a timezone change of the reading session
SET timezone = 'UTC';
SELECT id, t FROM tz_repro ORDER BY id;
SET timezone = 'Asia/Shanghai';

-- predicate on the timestamptz column
SELECT id, t FROM tz_repro WHERE t = timestamptz '2024-01-01 02:00:00+08';
SELECT id, t FROM tz_repro WHERE t > timestamptz '2024-02-01 00:00:00+08' ORDER BY id;

-- UPDATE and DELETE keep instant semantics (delete files + new data files)
UPDATE tz_repro SET t = t + interval '1 hour' WHERE id = 2;
SELECT id, t FROM tz_repro ORDER BY id;

DELETE FROM tz_repro WHERE t < timestamptz '2024-01-02 00:00:00+08';
SELECT id, t FROM tz_repro ORDER BY id;
