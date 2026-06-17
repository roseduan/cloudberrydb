-- TRUNCATE iceberg cleanup regression (issue #344).
--
-- TRUNCATE on a builtin-catalog ICEBERG table enqueues the PRE-truncate
-- metadata.json into pg_ext_aux.pg_iceberg_deletion_queue (DELETION_TYPE_METADATA
-- = 1) so the async consumer removes the old data/metadata tree, then commits a
-- new empty snapshot.  The enqueue is asserted INSIDE the TRUNCATE transaction
-- (TRUNCATE is transactional) so the result is immune to the async autovacuum
-- consumer; no GUC toggling, no MinIO file inspection.
--
-- Also pins: empty-table TRUNCATE is a no-op (nothing enqueued).
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET DateStyle = 'ISO, YMD';
SET client_min_messages = WARNING;

-- builtin catalog + volume
CREATE SERVER tcl_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER tcl_catalog_server;
CREATE FOREIGN CATALOG tcl_catalog SERVER tcl_catalog_server;
SET iceberg_default_catalog = 'tcl_catalog';

CREATE SERVER tcl_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER tcl_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME tcl_volume SERVER tcl_volume_server OPTIONS(base_path '/tcl_volume/');
SET iceberg_default_volume = 'tcl_volume';

CREATE SCHEMA tcl;
CREATE ICEBERG TABLE tcl.t (id int, c text);
INSERT INTO tcl.t SELECT g, 'x' FROM generate_series(1, 100) g;

-- In-transaction assertion: TRUNCATE enqueues exactly one METADATA row for the
-- table and empties it.
BEGIN;
TRUNCATE tcl.t;
SELECT count(*) AS enqueued, array_agg(DISTINCT deletion_type) AS types
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE 'tcl.%';
SELECT count(*) AS rows_after FROM tcl.t;
COMMIT;

-- Empty-table TRUNCATE is a no-op: nothing new enqueued.
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'tcl.%';
SET allow_system_table_mods = off;

CREATE ICEBERG TABLE tcl.empt (id int);
TRUNCATE tcl.empt;
SELECT count(*) AS enqueued_after_empty_truncate
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE 'tcl.%';

-- Cleanup.
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'tcl.%';
SET allow_system_table_mods = off;
DROP SCHEMA tcl CASCADE;
DROP VOLUME tcl_volume;
DROP USER MAPPING FOR current_user SERVER tcl_volume_server;
DROP SERVER tcl_volume_server;
DROP CATALOG tcl_catalog;
DROP USER MAPPING FOR current_user SERVER tcl_catalog_server;
DROP SERVER tcl_catalog_server;
