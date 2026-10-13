-- iceberg_toolkit.attach_metadata() (issue #457)
--
-- gprestore recreates a builtin iceberg table and then re-points it at the
-- metadata.json recorded at backup time, instead of copying data.  The table
-- shares data files with the source here, which is fine for a test but is
-- exactly the setup the feature forbids on a live second cluster.
--
-- Targets the lakehouse stack MinIO (endpoint http://minio:9000,
-- admin/admin12345) like the other builtin-catalog smoke tests.

DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
SET client_min_messages = WARNING;

-- The 1.3 upgrade must not drop what earlier versions shipped.
SELECT extversion,
       to_regprocedure('pg_catalog.iceberg_refresh_metadata(regclass)') IS NOT NULL AS refresh_fn,
       to_regprocedure('iceberg_toolkit.attach_metadata(regclass,text)') IS NOT NULL AS attach_fn
  FROM pg_extension WHERE extname = 'datalake_fdw';

DROP SERVER IF EXISTS am457_cat_srv CASCADE;
CREATE SERVER am457_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER am457_cat_srv;
CREATE FOREIGN CATALOG am457_cat SERVER am457_cat_srv;
SET iceberg_default_catalog = 'am457_cat';

DROP SERVER IF EXISTS am457_vol_srv CASCADE;
CREATE SERVER am457_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER am457_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME am457_vol SERVER am457_vol_srv
    OPTIONS (base_path '/iceberg_attach_metadata/');
SET iceberg_default_volume = 'am457_vol';

-- Source table with committed data; its pointer plays the backup snapshot.
CREATE ICEBERG TABLE am457_src (id int, name text);
INSERT INTO am457_src VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT metadata_location AS src_loc FROM pg_ext_aux.pg_iceberg_metadata
 WHERE relid = 'am457_src'::regclass \gset
-- The source moves on after the "backup": attaching src_loc restores the
-- backup-time version (RPO), and no live table points at src_loc any more.
INSERT INTO am457_src VALUES (99, 'z');

-- 1. Happy path: fresh table adopts the source snapshot, then stays writable.
CREATE ICEBERG TABLE am457_dst (id int, name text);
SELECT iceberg_toolkit.attach_metadata('am457_dst'::regclass, :'src_loc');
SELECT * FROM am457_dst ORDER BY id;
SELECT metadata_location = :'src_loc' AS points_at_src
  FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'am457_dst'::regclass;
INSERT INTO am457_dst VALUES (4, 'd');
SELECT count(*) FROM am457_dst;

-- 1b. The exact gprestore shape: CREATE + attach as ONE query (one implicit
--     transaction), so the attach sees a table created in its own xact.
CREATE ICEBERG TABLE am457_onequery (id int, name text) \; SELECT iceberg_toolkit.attach_metadata('am457_onequery'::regclass, :'src_loc');
SELECT count(*) FROM am457_onequery;
-- Commit once so it moves off the shared pointer: two live tables on the same
-- metadata.json would both enqueue that path on DROP (pkey collision), which
-- is the shared-files setup the feature forbids.
INSERT INTO am457_onequery VALUES (5, 'e');

-- 2. Refuses a table that already has committed data.
SELECT iceberg_toolkit.attach_metadata('am457_src'::regclass, :'src_loc');

-- 3. Missing metadata file -> SQLSTATE 58P01, pointer untouched.
CREATE ICEBERG TABLE am457_missing (id int, name text);
SELECT metadata_location AS v0_loc FROM pg_ext_aux.pg_iceberg_metadata
 WHERE relid = 'am457_missing'::regclass \gset
DO $$
BEGIN
    PERFORM iceberg_toolkit.attach_metadata('am457_missing'::regclass,
        (SELECT metadata_location || '.missing' FROM pg_ext_aux.pg_iceberg_metadata
          WHERE relid = 'am457_src'::regclass));
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'sqlstate %', SQLSTATE;
END $$;
SELECT metadata_location = :'v0_loc' AS untouched
  FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'am457_missing'::regclass;

-- 4. gprestore sends CREATE + attach as ONE query: a failed attach must also
--    roll the CREATE back, leaving no half-restored empty table behind.
--    (VERBOSITY sqlstate: the message embeds a random metadata UUID path.)
\set VERBOSITY sqlstate
CREATE ICEBERG TABLE am457_atomic (id int, name text) \; SELECT iceberg_toolkit.attach_metadata('am457_atomic'::regclass, :'src_loc' || '.missing');
SELECT to_regclass('am457_atomic') IS NULL AS rolled_back;

-- 5. Column mismatch -> error, nothing changed.
CREATE ICEBERG TABLE am457_badcols (id int, other text);
SELECT iceberg_toolkit.attach_metadata('am457_badcols'::regclass, :'src_loc');
\set VERBOSITY default

-- 6. Not an iceberg table.
CREATE TABLE am457_heap (id int);
SELECT iceberg_toolkit.attach_metadata('am457_heap'::regclass, :'src_loc');

-- 8. Refuses metadata another table in this database already points at:
--    two live tables on one metadata tree would delete each other's files
--    (restore next to a live source, e.g. --redirect-schema).
CREATE ICEBERG TABLE am457_dup1 (id int, name text);
SELECT iceberg_toolkit.attach_metadata('am457_dup1'::regclass, :'src_loc');
CREATE ICEBERG TABLE am457_dup2 (id int, name text);
\set VERBOSITY terse
SELECT iceberg_toolkit.attach_metadata('am457_dup2'::regclass, :'src_loc');
\set VERBOSITY default

-- 9. A dropped table whose files are still queued for deletion must not be
--    attached again: the async consumer would delete them under the restored
--    table later.  Treated as gone (58P01) so gprestore skips it.
CREATE ICEBERG TABLE am457_gone (id int, name text);
INSERT INTO am457_gone VALUES (1, 'a');
SELECT metadata_location AS gone_loc FROM pg_ext_aux.pg_iceberg_metadata
 WHERE relid = 'am457_gone'::regclass \gset
DROP TABLE am457_gone;
CREATE ICEBERG TABLE am457_gone (id int, name text);
\set VERBOSITY sqlstate
SELECT iceberg_toolkit.attach_metadata('am457_gone'::regclass, :'gone_loc');
\set VERBOSITY default
SELECT count(*) AS gone_rows FROM am457_gone;
DROP TABLE am457_gone;

-- 10. restore_table(): what gpbackup emits per iceberg table, so gprestore
--     needs no iceberg logic.  Skips an existing table, creates + attaches a
--     missing one (COMMENT/GRANT ride in the DDL), and turns a missing
--     metadata file into a WARNING + skip instead of failing the restore.
SELECT metadata_location AS dst_before FROM pg_ext_aux.pg_iceberg_metadata
 WHERE relid = 'am457_dst'::regclass \gset
SELECT iceberg_toolkit.restore_table('public.am457_dst',
    'CREATE ICEBERG TABLE public.am457_dst (id int, name text) CATALOG am457_cat VOLUME am457_vol;',
    :'src_loc');
SELECT metadata_location = :'dst_before' AS existing_untouched
  FROM pg_ext_aux.pg_iceberg_metadata WHERE relid = 'am457_dst'::regclass;
-- Own source for this case: src_loc is already held by am457_dup1 (case 8).
CREATE ICEBERG TABLE am457_rt_src (id int, name text);
INSERT INTO am457_rt_src VALUES (7, 'g');
SELECT metadata_location AS rt_loc FROM pg_ext_aux.pg_iceberg_metadata
 WHERE relid = 'am457_rt_src'::regclass \gset
INSERT INTO am457_rt_src VALUES (8, 'h');
SELECT iceberg_toolkit.restore_table('public.am457_rt',
    'CREATE ICEBERG TABLE public.am457_rt (id int, name text) CATALOG am457_cat VOLUME am457_vol;
     COMMENT ON TABLE public.am457_rt IS ''restored'';',
    :'rt_loc');
SELECT string_agg(id::text, ',' ORDER BY id) AS rt_rows,
       obj_description('am457_rt'::regclass) AS rt_comment
  FROM am457_rt;
\set VERBOSITY terse
SELECT iceberg_toolkit.restore_table('public.am457_rt_gone',
    'CREATE ICEBERG TABLE public.am457_rt_gone (id int, name text) CATALOG am457_cat VOLUME am457_vol;',
    :'rt_loc' || '.missing');
\set VERBOSITY default
SELECT to_regclass('am457_rt_gone') IS NULL AS rt_gone_skipped;
-- External-catalog tables pass no location: DDL only, no attach.
SELECT iceberg_toolkit.restore_table('public.am457_rt_ddl',
    'CREATE ICEBERG TABLE public.am457_rt_ddl (id int) CATALOG am457_cat VOLUME am457_vol;',
    NULL);
SELECT count(*) AS rt_ddl_rows FROM am457_rt_ddl;
-- A same-named relation that is NOT an iceberg table is a conflict, not
-- "already restored": skipping it would silently drop the iceberg table.
CREATE TABLE am457_rt_heap (id int);
SELECT iceberg_toolkit.restore_table('public.am457_rt_heap',
    'CREATE ICEBERG TABLE public.am457_rt_heap (id int) CATALOG am457_cat VOLUME am457_vol;',
    NULL);
-- Only attach_metadata's own 58P01 means "skip"; a 58P01 raised by the DDL
-- itself must surface.
SELECT iceberg_toolkit.restore_table('public.am457_rt_boom',
    'DO $x$ BEGIN RAISE EXCEPTION ''boom'' USING ERRCODE = ''undefined_file''; END $x$;',
    NULL);
SELECT to_regclass('am457_rt_boom') IS NULL AS rt_boom_absent;
DROP TABLE am457_rt_heap;
DROP TABLE am457_rt, am457_rt_ddl, am457_rt_src;

-- 7. Not callable by ordinary roles: the caller names an arbitrary path, and
--    builtin tables share one directory, so a table owner could otherwise
--    attach (read, and on DROP delete) another table's files.
DROP ROLE IF EXISTS am457_user;
CREATE ROLE am457_user;
SET ROLE am457_user;
SELECT iceberg_toolkit.attach_metadata('am457_dst'::regclass, :'src_loc');
SELECT iceberg_toolkit.attach_metadata_internal('am457_dst'::regclass, :'src_loc');
SELECT iceberg_toolkit.restore_table('public.am457_dst', 'SELECT 1', NULL);
RESET ROLE;
DROP ROLE am457_user;

DROP TABLE am457_dst, am457_onequery, am457_dup1, am457_dup2, am457_src, am457_missing, am457_badcols, am457_heap;
DROP SERVER am457_cat_srv CASCADE;
DROP SERVER am457_vol_srv CASCADE;
