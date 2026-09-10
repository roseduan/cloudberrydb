-- A catalog that names only a gphive.conf section (issue #410), catalog half.
--
-- The volume conf-only cases (smoke/iceberg_conf_only, iceberg_conf_only_hdfs)
-- cover the data plane, where the merged settings have to be shipped to the
-- segments.  This is the other domain: the metastore URI, auth method and
-- kerberos principals of a *catalog* live in gphive.conf, and only the
-- coordinator's agent ever needs them -- the segments never speak to a
-- metastore.  Nothing was covering it: no suite used a hive catalog, and none
-- used server_name on a catalog at all.
--
-- The volume here is fully specified on purpose, so the catalog is the only
-- thing under test.

SET client_min_messages = WARNING;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;
SET DateStyle = 'ISO, YMD';

-- The catalog under test: type and section name, nothing more.  The Makefile
-- stages the section; the metastore URI it carries is the only route to the
-- metastore.
DROP SERVER IF EXISTS cohive_cat_srv CASCADE;
CREATE SERVER cohive_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hive', server_name 'cohive_meta');
CREATE USER MAPPING FOR current_user SERVER cohive_cat_srv;
-- default_namespace targets Hive's built-in `default` database, so the test
-- needs no namespace seeded in the metastore.  Without it the namespace falls
-- back to the PostgreSQL schema name and a fresh metastore has no "public".
CREATE FOREIGN CATALOG cohive_cat SERVER cohive_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_conf_only_hive/',
             default_namespace 'default');
SET iceberg_default_catalog = 'cohive_cat';

DROP SERVER IF EXISTS cohive_vol_srv CASCADE;
CREATE SERVER cohive_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             path_style_access 'true', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER cohive_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME cohive_vol SERVER cohive_vol_srv
    OPTIONS (base_path '/iceberg_conf_only_hive/', allow_writes 'true');
SET iceberg_default_volume = 'cohive_vol';

CREATE ICEBERG TABLE cohive_t (id int, name text);

-- pg_regress recreates the database, but neither the metastore nor the bucket:
-- CREATE above re-attached to what a previous run left, so without this the
-- counts below climb (5, 8, 11...).  The DO wrapper hides the row count, which
-- is 0 on a first run; TRUNCATE is a no-op on this access method.
DO $$ BEGIN DELETE FROM cohive_t; END $$;

\echo === write ===
INSERT INTO cohive_t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
SELECT count(*) AS after_insert FROM cohive_t;
UPDATE cohive_t SET name = 'BETA' WHERE id = 2;
DELETE FROM cohive_t WHERE id = 3;

\echo === read back ===
SELECT id, name FROM cohive_t ORDER BY id;

-- cohive_t is deliberately NOT dropped: dropping it would delete the metastore
-- entry and the data, and the next run would start over -- which works, but
-- leaves nothing pinning the re-attach path that the DELETE above exercises.

-- ============================================================
-- Boundary cases: nothing to merge
-- ============================================================
-- Both fail on the metadata plane, where the conf file is read.  Only the first
-- line is asserted; the rest is a Java stack trace whose line numbers move with
-- the agent build.
\echo === negative: the named section does not exist ===
DROP SERVER IF EXISTS cohive_missing_srv CASCADE;
CREATE SERVER cohive_missing_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hive', server_name 'cohive_no_such_section');
CREATE USER MAPPING FOR current_user SERVER cohive_missing_srv;
CREATE FOREIGN CATALOG cohive_missing_cat SERVER cohive_missing_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_conf_only_hive/',
             default_namespace 'default');
SET iceberg_default_catalog = 'cohive_missing_cat';
DO $$
BEGIN
    CREATE ICEBERG TABLE cohive_missing_t (id int);
    RAISE NOTICE 'unexpected: a table was created on a section that does not exist';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'rejected: %', split_part(SQLERRM, E'\n', 1);
END $$;

\echo === negative: the named section is empty ===
DROP SERVER IF EXISTS cohive_empty_srv CASCADE;
CREATE SERVER cohive_empty_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hive', server_name 'cohive_empty');
CREATE USER MAPPING FOR current_user SERVER cohive_empty_srv;
CREATE FOREIGN CATALOG cohive_empty_cat SERVER cohive_empty_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_conf_only_hive/',
             default_namespace 'default');
SET iceberg_default_catalog = 'cohive_empty_cat';
DO $$
BEGIN
    CREATE ICEBERG TABLE cohive_empty_t (id int);
    RAISE NOTICE 'unexpected: a table was created on an empty section';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'rejected: %', split_part(SQLERRM, E'\n', 1);
END $$;

-- ============================================================
-- Cleanup -- servers only; see the note on cohive_t above.
-- ============================================================
RESET iceberg_default_volume;
RESET iceberg_default_catalog;
DROP CATALOG cohive_empty_cat;
DROP SERVER cohive_empty_srv CASCADE;
DROP CATALOG cohive_missing_cat;
DROP SERVER cohive_missing_srv CASCADE;
DROP SERVER cohive_vol_srv CASCADE;
DROP CATALOG cohive_cat;
DROP SERVER cohive_cat_srv CASCADE;
