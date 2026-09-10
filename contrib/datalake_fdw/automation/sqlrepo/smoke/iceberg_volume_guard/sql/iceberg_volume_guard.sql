-- Volume address guard on the QE data plane (issue #410, step 1).
--
-- getVolumeOptions() refuses to hand gopher a connection with no address,
-- because a gopher store that rejects its config exits at startup and its
-- socket then refuses every later connection until the cluster is restarted.
--
-- Which field carries the address depends on the volume type, so the guard has
-- two arms and both are covered here:
--
--   case 1  HDFS volume with `hdfs_namenodes` and no `endpoint` -- a supported
--           configuration.  gopher->host is written only from `endpoint`, so a
--           guard that checked gopher->host would reject this.  Must WORK.
--   case 2  conf-only S3 volume -- endpoint and credentials live in the
--           s3.conf section named by `server_name`.  Only the agent reads conf
--           files, so CREATE succeeds on the metadata plane while the QE data
--           plane has no address.  Must fail with a clean, named error.
--
-- Case 1 stops at INSERT on purpose: reading the table back crashes the
-- segment on an unrelated use-after-free in flatCombinedTasks() (issue #418).
--
-- The Makefile deploys the guard_* sections into the QD data directory
-- s3.conf (the datalake_agent working directory) before psql runs this file.

\i ../../../lib/sql/common_setup.sql

CREATE SERVER vg_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vg_catalog_server;
CREATE FOREIGN CATALOG vg_catalog SERVER vg_catalog_server;
SET iceberg_default_catalog = 'vg_catalog';

-- ============================================================
-- Case 1: HDFS volume addressed by hdfs_namenodes only
-- ============================================================
\echo === case 1: hdfs_namenodes without endpoint is a working volume ===
CREATE SERVER vg_hdfs_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', hdfs_namenodes 'hadoop:8020');
CREATE USER MAPPING FOR current_user SERVER vg_hdfs_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME vg_hdfs_vol SERVER vg_hdfs_srv
    OPTIONS (base_path '/iceberg_volume_guard/', allow_writes 'true');
SET iceberg_default_volume = 'vg_hdfs_vol';

CREATE ICEBERG TABLE vg_hdfs_t (id bigint, v text);
INSERT INTO vg_hdfs_t VALUES (1, 'alpha'), (2, 'beta');
DROP TABLE vg_hdfs_t;

RESET iceberg_default_volume;
DROP SERVER vg_hdfs_srv CASCADE;

-- ============================================================
-- Case 2: conf-only S3 volume -- clean error, not a dead gopher store
-- ============================================================
\echo === case 2: conf-only volume fails at INSERT naming the volume ===
CREATE SERVER vg_conf_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'guard_minio', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER vg_conf_srv;
CREATE FOREIGN VOLUME vg_conf_vol SERVER vg_conf_srv
    OPTIONS (base_path '/iceberg_volume_guard_s3/', allow_writes 'true');
SET iceberg_default_volume = 'vg_conf_vol';

-- The agent resolves the section, so the metadata plane succeeds.
CREATE ICEBERG TABLE vg_conf_t (id bigint);

-- The QE data plane has no address.  Expect the guard, and expect the volume
-- to be named so the message says which one to fix.
INSERT INTO vg_conf_t VALUES (1);

DROP TABLE vg_conf_t;
RESET iceberg_default_volume;
DROP SERVER vg_conf_srv CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
RESET iceberg_default_catalog;
DROP SERVER vg_catalog_server CASCADE;
