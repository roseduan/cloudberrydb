-- Transaction ROLLBACK / COMMIT metadata-file cleanup regression (issue #399).
--
-- On a builtin-catalog ICEBERG table, a transaction that reads/scans a dirty
-- (accumulated-but-uncommitted) table -- e.g. INSERT then UPDATE -- makes the
-- read path materialize an INTERMEDIATE metadata.json + manifest + manifest-list
-- for read-your-writes. Before #399 those metadata-layer files were orphaned on
-- ROLLBACK (never deleted, never enqueued) and superseded-but-orphaned on COMMIT.
--
-- This pins the fix's deterministic, SQL-observable signals (the actual
-- object-storage deletion is covered by the MinIO integration script
-- automation/integration/iceberg_txn_cleanup.sh):
--   * COMMIT: the superseded intermediate metadata-layer files are enqueued into
--     pg_ext_aux.pg_iceberg_deletion_queue as DELETION_TYPE_FILE (0) for async
--     removal.  Asserted right after COMMIT -- the autovacuum-driven consumer
--     runs on a much longer cycle, so it cannot drain the rows in the gap
--     between statements (same technique as iceberg_vacuum_cleanup).
--   * ROLLBACK: the transaction rolls back correctly and enqueues NOTHING (the
--     abort cleanup deletes the orphans synchronously; it must not leave queue
--     cruft, and data/delete parquet is handled by the Class 1 abort path).
--   * SAVEPOINT partial rollback: correct row visibility.
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET DateStyle = 'ISO, YMD';
SET client_min_messages = WARNING;

-- builtin catalog + volume
CREATE SERVER txn_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER txn_catalog_server;
CREATE FOREIGN CATALOG txn_catalog SERVER txn_catalog_server;
SET iceberg_default_catalog = 'txn_catalog';

CREATE SERVER txn_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER txn_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME txn_volume SERVER txn_volume_server OPTIONS(base_path '/txn_volume/');
SET iceberg_default_volume = 'txn_volume';

CREATE SCHEMA txn;

-- === COMMIT: superseded intermediate metadata files are enqueued (FILE=0) ===
-- INSERT then UPDATE: the UPDATE scans the dirty table, materializing an
-- intermediate metadata tree; on COMMIT that superseded tree's files must be
-- enqueued for async removal while the finally-committed tree is kept.
CREATE ICEBERG TABLE txn.t (id int, n int);
BEGIN;
INSERT INTO txn.t VALUES (1,10),(2,20),(3,30);
UPDATE txn.t SET n = n + 1 WHERE id = 1;
COMMIT;
SELECT (count(*) > 0) AS superseded_files_enqueued,
       array_agg(DISTINCT deletion_type) AS types
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE 'txn.%';
SELECT count(*) AS rows_committed, sum(n) AS sum_n FROM txn.t;

-- reset queue for the next assertion
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'txn.%';
SET allow_system_table_mods = off;

-- === ROLLBACK: correct rollback, nothing left in the queue ===
CREATE ICEBERG TABLE txn.r (id int, n int);
INSERT INTO txn.r VALUES (1,10),(2,20);
BEGIN;
INSERT INTO txn.r VALUES (3,30),(4,40);
UPDATE txn.r SET n = n + 1 WHERE id = 1;
ROLLBACK;
SELECT count(*) AS rows_after_rollback, sum(n) AS sum_after_rollback FROM txn.r;
SELECT count(*) AS enqueued_after_rollback
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE 'txn.%';

-- === SAVEPOINT partial rollback: correct visibility ===
BEGIN;
INSERT INTO txn.r VALUES (60,600);
SAVEPOINT sp1;
INSERT INTO txn.r VALUES (61,610);
UPDATE txn.r SET n = n + 1 WHERE id = 60;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
SELECT count(*) AS rows_after_sp,
       count(*) FILTER (WHERE id = 61) AS id61_should_be_0,
       max(n) FILTER (WHERE id = 60) AS id60_n_should_be_600
  FROM txn.r;

-- Cleanup.
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'txn.%';
SET allow_system_table_mods = off;
DROP SCHEMA txn CASCADE;
-- Clean up queue entries created by DROP SCHEMA CASCADE before dropping the server.
SET allow_system_table_mods = on;
DELETE FROM pg_ext_aux.pg_iceberg_deletion_queue WHERE table_qname LIKE 'txn.%';
SET allow_system_table_mods = off;
DROP VOLUME txn_volume;
DROP USER MAPPING FOR current_user SERVER txn_volume_server;
DROP SERVER txn_volume_server;
DROP CATALOG txn_catalog;
DROP USER MAPPING FOR current_user SERVER txn_catalog_server;
DROP SERVER txn_catalog_server;
