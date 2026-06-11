-- Iceberg AM ANALYZE sampling regression (issue #352).
--
-- Before the fix, ANALYZE on a native Iceberg table only refreshed
-- pg_class.reltuples from metadata and left pg_statistic empty, so ORCA had no
-- per-column NDV/MCV and chose catastrophic join orders on low-cardinality
-- predicates (TPC-DS Q24: c_birth_country = upper(ca_country)).
--
-- The fix samples rows on the QEs during ANALYZE (the QD expands each table's
-- fragment list -- the datalake agent is QD-only -- and ships it to the QEs as
-- a PgIcebergAnalyzeDispatch ExtensibleNode over the normal query channel; a
-- synced GUC would ride the gang-connection startup packet and overflow
-- MAX_STARTUP_PACKET_LENGTH on large tables) and lets the kernel compute real
-- column statistics.
--
-- Volume targets the lakehouse-stack MinIO (endpoint 'http://minio:9000',
-- admin/admin12345), same as iceberg_am_s3.
\i ../../../lib/sql/common_setup.sql

-- Quiet the catalog/volume/DDL chatter so the expected output only contains
-- the deterministic assertions below.
SET client_min_messages = WARNING;

DROP SERVER IF EXISTS anlz_cat_srv CASCADE;
CREATE SERVER anlz_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER anlz_cat_srv;
CREATE FOREIGN CATALOG anlz_cat SERVER anlz_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_am_analyze/');
SET iceberg_default_catalog = 'anlz_cat';

DROP SERVER IF EXISTS anlz_vol_srv CASCADE;
CREATE SERVER anlz_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER anlz_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME anlz_vol SERVER anlz_vol_srv
    OPTIONS (base_path '/iceberg_am_analyze/', allow_writes 'true');
SET iceberg_default_volume = 'anlz_vol';

-- ============================================================
-- T1: ANALYZE populates per-column statistics (NDV) via sampling
-- ============================================================
DROP TABLE IF EXISTS anlz_t;
CREATE ICEBERG TABLE anlz_t (id int, country text, val int);
INSERT INTO anlz_t
  SELECT g, (ARRAY['US','CA','MX'])[1 + (g % 3)], g FROM generate_series(1, 1000) g;
ANALYZE anlz_t;
-- country has 3 distinct values; id/val are unique (n_distinct = -1).
SELECT attname, n_distinct FROM pg_stats WHERE tablename = 'anlz_t' ORDER BY attname;
SELECT reltuples FROM pg_class WHERE relname = 'anlz_t';

-- ============================================================
-- T2: multi-table ANALYZE samples each table correctly
-- ============================================================
DROP TABLE IF EXISTS anlz_t2;
CREATE ICEBERG TABLE anlz_t2 (id int, color text, amt int);
INSERT INTO anlz_t2
  SELECT g, (ARRAY['red','green','blue','pink'])[1 + (g % 4)], g FROM generate_series(1, 800) g;
ANALYZE anlz_t, anlz_t2;
SELECT tablename, attname, n_distinct FROM pg_stats
 WHERE tablename IN ('anlz_t','anlz_t2') AND attname IN ('country','color')
 ORDER BY tablename, attname;

-- ============================================================
-- T3: switch off => metadata-only refresh, no per-column stats, no error
-- ============================================================
SET allow_system_table_mods = on;
DELETE FROM pg_statistic WHERE starelid = 'anlz_t'::regclass;
RESET allow_system_table_mods;
SET datalake.enable_iceberg_analyze_sampling = off;
ANALYZE anlz_t;
SELECT count(*) AS cols_when_off FROM pg_stats WHERE tablename = 'anlz_t';
RESET datalake.enable_iceberg_analyze_sampling;

-- ============================================================
-- T4: only the sampling switch is user-visible (the fragment transport is
-- an ExtensibleNode dispatch, not a GUC)
-- ============================================================
SELECT name FROM pg_settings WHERE name LIKE 'datalake.%analyze%' ORDER BY name;

-- ============================================================
-- T5: mixed ANALYZE (Iceberg + heap) in one statement -- both are analyzed
-- (regression for issue #352: the iceberg rel must still be sampled, and the
--  heap rel must take the standard path; neither errors)
-- ============================================================
DROP TABLE IF EXISTS anlz_heap;
CREATE TABLE anlz_heap (a int, b text) DISTRIBUTED BY (a);
INSERT INTO anlz_heap SELECT g, (ARRAY['x','y'])[1 + g % 2] FROM generate_series(1, 500) g;
SET allow_system_table_mods = on;
DELETE FROM pg_statistic WHERE starelid = 'anlz_t'::regclass;
RESET allow_system_table_mods;
ANALYZE anlz_t, anlz_heap;
SELECT tablename, count(*) AS ncols FROM pg_stats
 WHERE tablename IN ('anlz_t','anlz_heap') GROUP BY tablename ORDER BY tablename;

-- cleanup
DROP TABLE anlz_t;
DROP TABLE anlz_t2;
DROP TABLE anlz_heap;
