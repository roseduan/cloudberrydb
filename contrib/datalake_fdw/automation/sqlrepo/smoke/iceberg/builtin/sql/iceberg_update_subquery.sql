-- Iceberg UPDATE/DELETE-with-subquery Regression Test
-- Purpose: Verify cross-table subqueries (IN / NOT EXISTS) in the WHERE
--          clause of an Iceberg UPDATE/DELETE do not trip
--          "Failed to get file path for file ID 0 in Iceberg update".
-- Tracks:  issue #333.  Plan inserts a Redistribute Motion between the
--          iceberg scan and the ModifyTable, putting them in different
--          slices; the writer QE never runs the scan, so without the fix
--          its file index map is empty and any file_id lookup fails.
--          DELETE shares the same file-id decode path (Case D below).

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup (mirrors the other smoke tests in this dir)
CREATE SERVER us_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER us_catalog_server;
CREATE FOREIGN CATALOG us_catalog SERVER us_catalog_server;
SET iceberg_default_catalog = 'us_catalog';

CREATE SERVER us_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER us_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME us_volume SERVER us_volume_server OPTIONS(base_path '/us_volume/');
SET iceberg_default_volume = 'us_volume';

-- ============================================================
-- Schema: a heap baseline (`us_heap`) and an iceberg subject (`us_ice`)
-- of identical shape, plus a small filter table.  Insert ten rows; the
-- filter table picks three of them.
-- ============================================================
CREATE TABLE us_heap (id BIGINT, deposit BIGINT, end_date DATE) DISTRIBUTED BY (id);
CREATE ICEBERG TABLE us_ice  (id BIGINT, deposit BIGINT, end_date DATE);
CREATE TABLE us_pick (id BIGINT) DISTRIBUTED BY (id);

INSERT INTO us_heap SELECT g, 100, DATE '2999-12-31' FROM generate_series(1,10) g;
INSERT INTO us_ice  SELECT g, 100, DATE '2999-12-31' FROM generate_series(1,10) g;
INSERT INTO us_pick VALUES (2), (4), (6);

-- ============================================================
-- Case A: WHERE id IN (SELECT ...) — semi-join filter
-- ============================================================
UPDATE us_heap SET end_date = DATE '2020-01-01'
WHERE end_date = DATE '2999-12-31'
  AND id IN (SELECT id FROM us_pick);

UPDATE us_ice  SET end_date = DATE '2020-01-01'
WHERE end_date = DATE '2999-12-31'
  AND id IN (SELECT id FROM us_pick);

SELECT COUNT(*) AS heap_total FROM us_heap;
SELECT COUNT(*) AS ice_total  FROM us_ice;
SELECT id, end_date FROM us_heap ORDER BY id;
SELECT id, end_date FROM us_ice  ORDER BY id;

-- ============================================================
-- Case B: WHERE NOT EXISTS (SELECT ... WHERE ...) — anti-join filter
-- ============================================================
UPDATE us_heap SET end_date = DATE '2020-01-02'
WHERE end_date = DATE '2999-12-31'
  AND NOT EXISTS (SELECT 1 FROM us_pick WHERE us_pick.id = us_heap.id);

UPDATE us_ice  SET end_date = DATE '2020-01-02'
WHERE end_date = DATE '2999-12-31'
  AND NOT EXISTS (SELECT 1 FROM us_pick WHERE us_pick.id = us_ice.id);

SELECT COUNT(*) AS heap_total FROM us_heap;
SELECT COUNT(*) AS ice_total  FROM us_ice;
SELECT id, end_date FROM us_heap ORDER BY id;
SELECT id, end_date FROM us_ice  ORDER BY id;

-- ============================================================
-- Case C: transactional NOT EXISTS + IN (the zipper_change.sql shape
-- from the original bug repro).  After both UPDATEs every row should
-- carry end_date = 2020-01-03 on both the heap and iceberg sides.
-- ============================================================
TRUNCATE us_heap;
TRUNCATE us_ice;
INSERT INTO us_heap SELECT g, 100, DATE '2999-12-31' FROM generate_series(1,10) g;
INSERT INTO us_ice  SELECT g, 100, DATE '2999-12-31' FROM generate_series(1,10) g;

BEGIN;
UPDATE us_heap SET end_date = DATE '2020-01-03'
WHERE end_date = DATE '2999-12-31'
  AND NOT EXISTS (SELECT 1 FROM us_pick WHERE us_pick.id = us_heap.id);
UPDATE us_heap SET end_date = DATE '2020-01-03'
WHERE end_date = DATE '2999-12-31'
  AND id IN (SELECT id FROM us_pick);
COMMIT;

BEGIN;
UPDATE us_ice  SET end_date = DATE '2020-01-03'
WHERE end_date = DATE '2999-12-31'
  AND NOT EXISTS (SELECT 1 FROM us_pick WHERE us_pick.id = us_ice.id);
UPDATE us_ice  SET end_date = DATE '2020-01-03'
WHERE end_date = DATE '2999-12-31'
  AND id IN (SELECT id FROM us_pick);
COMMIT;

SELECT COUNT(*) AS heap_total FROM us_heap;
SELECT COUNT(*) AS ice_total  FROM us_ice;
SELECT id, end_date FROM us_heap ORDER BY id;
SELECT id, end_date FROM us_ice  ORDER BY id;

-- ============================================================
-- Case D: DELETE with subquery — the same file-id decode path as
-- UPDATE (fdwFunction.c:1483 mirrors line 1395), so this also broke
-- before the fix.
-- ============================================================
DELETE FROM us_heap WHERE id IN (SELECT id FROM us_pick);
DELETE FROM us_ice  WHERE id IN (SELECT id FROM us_pick);

SELECT COUNT(*) AS heap_total FROM us_heap;
SELECT COUNT(*) AS ice_total  FROM us_ice;
SELECT id FROM us_heap ORDER BY id;
SELECT id FROM us_ice  ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE us_heap;
DROP TABLE us_ice;
DROP TABLE us_pick;

DROP VOLUME us_volume;
DROP USER MAPPING FOR current_user SERVER us_volume_server;
DROP SERVER us_volume_server;
DROP CATALOG us_catalog;
DROP USER MAPPING FOR current_user SERVER us_catalog_server;
DROP SERVER us_catalog_server;
