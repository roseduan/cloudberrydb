-- VACUUM space-reclamation cleanup regression (issue #344).
--
-- VACUUM compacts small files (RewriteFiles); the rewritten OLD files are then
-- enqueued into pg_ext_aux.pg_iceberg_deletion_queue with DELETION_TYPE_FILE
-- (= 0) for async deletion.  VACUUM cannot run in a transaction block, so the
-- enqueue is asserted immediately after VACUUM commits -- the autovacuum-driven
-- consumer runs on its own (much longer) cycle, so it cannot drain the rows in
-- the gap between statements.  min_input_files is lowered so compaction fires
-- deterministically.  No MinIO file inspection.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET DateStyle = 'ISO, YMD';
SET client_min_messages = WARNING;
SET datalake.iceberg_vacuum_compact_min_input_files = 2;

-- builtin catalog + volume
CREATE SERVER vcl_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER vcl_catalog_server;
CREATE FOREIGN CATALOG vcl_catalog SERVER vcl_catalog_server;
SET iceberg_default_catalog = 'vcl_catalog';

CREATE SERVER vcl_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER vcl_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME vcl_volume SERVER vcl_volume_server OPTIONS(base_path '/vcl_volume/');
SET iceberg_default_volume = 'vcl_volume';

CREATE SCHEMA vcl;
CREATE ICEBERG TABLE vcl.t (id int, c text);
-- Several small appends -> several small files (>= min_input_files per segment).
INSERT INTO vcl.t SELECT g, 'a' FROM generate_series(1, 100) g;
INSERT INTO vcl.t SELECT g, 'b' FROM generate_series(101, 200) g;
INSERT INTO vcl.t SELECT g, 'c' FROM generate_series(201, 300) g;
INSERT INTO vcl.t SELECT g, 'd' FROM generate_series(301, 400) g;
INSERT INTO vcl.t SELECT g, 'e' FROM generate_series(401, 500) g;
INSERT INTO vcl.t SELECT g, 'f' FROM generate_series(501, 600) g;

VACUUM vcl.t;

-- Assert: the rewritten old files are enqueued as DELETION_TYPE_FILE (0), and
-- the row set is unchanged by compaction.
SELECT (count(*) > 0) AS old_files_enqueued,
       array_agg(DISTINCT deletion_type) AS types
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE 'vcl.%';
SELECT count(*) AS rows_after_vacuum FROM vcl.t;

-- Cleanup.
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'vcl.%';
SET allow_system_table_mods = off;
DROP SCHEMA vcl CASCADE;
DROP VOLUME vcl_volume;
DROP USER MAPPING FOR current_user SERVER vcl_volume_server;
DROP SERVER vcl_volume_server;
DROP CATALOG vcl_catalog;
DROP USER MAPPING FOR current_user SERVER vcl_catalog_server;
DROP SERVER vcl_catalog_server;
