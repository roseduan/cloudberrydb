-- Iceberg AM many-files / many-delete-files regression (issue #362).
--
-- The fragment list used to travel inside the dispatched plan as an expanded
-- FileScanTask node tree.  nodeToString() has no notion of shared nodes, so
-- every delete-file fragment was copied into each task referencing it; on an
-- unpartitioned table every delete file applies to every older data file,
-- making the serialized plan and each QE's deserialized copy in
-- MessageContext O(data files x delete files) -- observed at ~500MB per
-- backend under zipper-style concurrent UPDATE workloads.
--
-- The fix ships the agent's deduplicated JSON wire format (one String node)
-- in CustomScan.custom_private / ModifyTable.fdwPrivLists and parses it back
-- at scan open / executor start.  This test builds a table with many small
-- data files plus accumulated delete files (each single-row INSERT is one
-- parquet file; each full-table UPDATE adds data + position-delete files)
-- and verifies the scan and modify paths stay correct end to end.
--
-- Targets the lakehouse stack MinIO (endpoint 'http://minio:9000',
-- admin/admin12345) like iceberg_am_s3.
\i ../../../lib/sql/common_setup.sql

SET client_min_messages = WARNING;

DROP SERVER IF EXISTS mf_cat_srv CASCADE;
CREATE SERVER mf_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER mf_cat_srv;
CREATE FOREIGN CATALOG mf_cat SERVER mf_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_am_many_files/');
SET iceberg_default_catalog = 'mf_cat';

DROP SERVER IF EXISTS mf_vol_srv CASCADE;
CREATE SERVER mf_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER mf_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME mf_vol SERVER mf_vol_srv
    OPTIONS (base_path '/iceberg_am_many_files/', allow_writes 'true');
SET iceberg_default_volume = 'mf_vol';

DROP TABLE IF EXISTS mf_t;
CREATE ICEBERG TABLE mf_t (id bigint, v bigint);

-- 30 single-statement INSERTs -> ~30 small data files.
INSERT INTO mf_t SELECT 1, 1;
INSERT INTO mf_t SELECT 2, 2;
INSERT INTO mf_t SELECT 3, 3;
INSERT INTO mf_t SELECT 4, 4;
INSERT INTO mf_t SELECT 5, 5;
INSERT INTO mf_t SELECT 6, 6;
INSERT INTO mf_t SELECT 7, 7;
INSERT INTO mf_t SELECT 8, 8;
INSERT INTO mf_t SELECT 9, 9;
INSERT INTO mf_t SELECT 10, 10;
INSERT INTO mf_t SELECT 11, 11;
INSERT INTO mf_t SELECT 12, 12;
INSERT INTO mf_t SELECT 13, 13;
INSERT INTO mf_t SELECT 14, 14;
INSERT INTO mf_t SELECT 15, 15;
INSERT INTO mf_t SELECT 16, 16;
INSERT INTO mf_t SELECT 17, 17;
INSERT INTO mf_t SELECT 18, 18;
INSERT INTO mf_t SELECT 19, 19;
INSERT INTO mf_t SELECT 20, 20;
INSERT INTO mf_t SELECT 21, 21;
INSERT INTO mf_t SELECT 22, 22;
INSERT INTO mf_t SELECT 23, 23;
INSERT INTO mf_t SELECT 24, 24;
INSERT INTO mf_t SELECT 25, 25;
INSERT INTO mf_t SELECT 26, 26;
INSERT INTO mf_t SELECT 27, 27;
INSERT INTO mf_t SELECT 28, 28;
INSERT INTO mf_t SELECT 29, 29;
INSERT INTO mf_t SELECT 30, 30;

-- 10 full-table UPDATE rounds -> position-delete files that attach to every
-- older data file (the O(N x M) shape from the issue).
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;
UPDATE mf_t SET v = v + 1;

-- Scan path: deletes must be applied (each id exactly once, v = id + 10).
SELECT count(*) AS cnt, min(v - id) AS min_delta, max(v - id) AS max_delta FROM mf_t;

-- Modify path with predicate.
UPDATE mf_t SET v = v + 100 WHERE id <= 10;
SELECT count(*) FROM mf_t WHERE v - id = 110;

-- Read-your-own-writes: double UPDATE of the same rows inside one txn.
BEGIN;
UPDATE mf_t SET v = v + 1000 WHERE id = 1;
UPDATE mf_t SET v = v + 1000 WHERE id = 1;
SELECT v - id AS delta_in_txn FROM mf_t WHERE id = 1;
COMMIT;
SELECT v - id AS delta_after_txn FROM mf_t WHERE id = 1;

-- DELETE path.
DELETE FROM mf_t WHERE id > 20;
SELECT count(*) FROM mf_t;

DROP TABLE mf_t;
