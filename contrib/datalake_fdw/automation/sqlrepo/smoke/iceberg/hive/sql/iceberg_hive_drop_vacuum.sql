-- DROP and VACUUM must leave files a Hive catalog owns alone
-- (issues #462, #463).
--
-- A Hive table created without OPTIONS records is_internal = true, as on
-- Polaris, and both DROP and VACUUM used to queue its files for deletion on
-- that basis, while the Hive Metastore still registered the table.  VACUUM
-- also wrote its compaction only to the local metadata pointer.  The fix
-- applies to every non-builtin catalog; iceberg_polaris_drop and
-- iceberg_polaris_vacuum cover Polaris, this covers Hive.
--
-- DROP is checked inside its own transaction and VACUUM with the
-- deletion-queue consumer held off, so a wrongly queued row cannot be
-- drained before it is seen.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER hdv_cat_srv
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'hive', url 'thrift://hive-metastore:9083');
CREATE USER MAPPING FOR current_user SERVER hdv_cat_srv;
CREATE FOREIGN CATALOG hdv_cat SERVER hdv_cat_srv;
SET iceberg_default_catalog = 'hdv_cat';

CREATE SERVER hdv_vol_srv
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER hdv_vol_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME hdv_vol SERVER hdv_vol_srv OPTIONS (base_path '/hive_volume/');
SET iceberg_default_volume = 'hdv_vol';

-- ============================================================
-- DROP
-- ============================================================
CREATE ICEBERG TABLE hdv_drop (id int, name text);
INSERT INTO hdv_drop VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE ICEBERG TABLE hdv_drop_mount (id int, name text)
OPTIONS (namespace 'public', table 'hdv_drop');
SELECT c.relname, m.is_internal
  FROM pg_ext_aux.pg_iceberg_metadata m JOIN pg_class c ON c.oid = m.relid
 WHERE c.relname LIKE 'hdv_drop%'
 ORDER BY c.relname;

BEGIN;
DROP TABLE hdv_drop_mount;
DROP TABLE hdv_drop;
SELECT count(*) AS queued_by_drop
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_qname LIKE '%hdv_drop%';
COMMIT;

-- The Hive table is intact: mount it again and read it back.
CREATE ICEBERG TABLE hdv_drop_again (id int, name text)
OPTIONS (namespace 'public', table 'hdv_drop');
SELECT count(*), sum(id) FROM hdv_drop_again;
DROP TABLE hdv_drop_again;

-- ============================================================
-- VACUUM
-- ============================================================
ALTER SYSTEM SET datalake_fdw.deletion_queue_enabled = off;
SELECT pg_reload_conf();

-- One file per INSERT: six files, above the default threshold of five.
CREATE FUNCTION hdv_seed(tbl regclass) RETURNS void AS $$
BEGIN
    FOR i IN 1..6 LOOP
        EXECUTE format('INSERT INTO %s VALUES (%s, %L)', tbl, i, 'v' || i);
    END LOOP;
END $$ LANGUAGE plpgsql;

CREATE FUNCTION hdv_state(tbl regclass)
RETURNS TABLE (operation text, data_files int, records int) AS $$
    SELECT operation,
           (summary->>'total-data-files')::int,
           (summary->>'total-records')::int
      FROM iceberg_snapshot_list(tbl)
     WHERE is_current;
$$ LANGUAGE SQL;

-- Without OPTIONS.
CREATE ICEBERG TABLE hdv_vac (id int, name text);
SELECT hdv_seed('hdv_vac');
VACUUM hdv_vac;
SELECT operation, data_files < 6 AS compacted, records FROM hdv_state('hdv_vac');
SELECT count(*) AS queued_by_vacuum
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_name = 'hdv_vac'::regclass;
INSERT INTO hdv_vac VALUES (7, 'v7');
SELECT count(*) FILTER (WHERE operation = 'replace') AS replace_snapshots
  FROM iceberg_snapshot_list('hdv_vac');
SELECT operation, data_files < 7 AS still_compacted, records FROM hdv_state('hdv_vac');
SELECT count(*), sum(id) FROM hdv_vac;

-- With OPTIONS.
CREATE ICEBERG TABLE hdv_vac_src (id int, name text);
CREATE ICEBERG TABLE hdv_vac_opts (id int, name text)
OPTIONS (namespace 'public', table 'hdv_vac_src');
SELECT hdv_seed('hdv_vac_opts');
VACUUM hdv_vac_opts;
SELECT operation, data_files < 6 AS compacted, records FROM hdv_state('hdv_vac_opts');
SELECT count(*) AS queued_by_vacuum
  FROM pg_ext_aux.pg_iceberg_deletion_queue
 WHERE table_name = 'hdv_vac_opts'::regclass;
INSERT INTO hdv_vac_opts VALUES (7, 'v7');
SELECT count(*) FILTER (WHERE operation = 'replace') AS replace_snapshots
  FROM iceberg_snapshot_list('hdv_vac_opts');
SELECT operation, data_files < 7 AS still_compacted, records FROM hdv_state('hdv_vac_opts');
SELECT count(*), sum(id) FROM hdv_vac_opts;

ALTER SYSTEM RESET datalake_fdw.deletion_queue_enabled;
SELECT pg_reload_conf();

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE hdv_vac_opts;
DROP TABLE hdv_vac_src;
DROP TABLE hdv_vac;
DROP FUNCTION hdv_seed(regclass);
DROP FUNCTION hdv_state(regclass);
RESET iceberg_default_catalog;
RESET iceberg_default_volume;
DROP CATALOG hdv_cat;
DROP USER MAPPING FOR current_user SERVER hdv_cat_srv;
DROP SERVER hdv_cat_srv;
DROP VOLUME hdv_vol;
DROP USER MAPPING FOR current_user SERVER hdv_vol_srv;
DROP SERVER hdv_vol_srv;
