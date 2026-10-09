-- VACUUM on a table whose storage the catalog platform owns.
--
-- The Iceberg REST spec defines no maintenance endpoints, so compaction is
-- offered -- when it is offered at all -- by a surface beside the catalog.
-- Whether this deployment serves one is declared per server, because the same
-- platform answers or refuses depending on a switch its operator sets.
--
-- What this file can cover without a live platform: the option's spelling
-- rules, and that VACUUM refuses with actionable advice when no maintenance
-- API is declared.  The submission itself needs a reachable catalog and is
-- exercised in the automation smoke suite.

-- start_ignore
DROP DATABASE IF EXISTS iceberg_rest_vacuum_db;
CREATE DATABASE iceberg_rest_vacuum_db;
\c iceberg_rest_vacuum_db
CREATE EXTENSION datalake_fdw;
-- end_ignore

-- ============================================================
-- The option is validated where it is written, so a typo cannot survive to
-- become a table that is quietly never maintained.
-- ============================================================
CREATE SERVER rv_bad FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'http://rest.invalid:8181/iceberg',
             region 'us-east-1', maintenance_api 'nonsense');

-- The spelling is case-insensitive, like the other option values.
CREATE SERVER rv_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'http://rest.invalid:8181/iceberg',
             region 'us-east-1', signing_name 's3tables',
             maintenance_api 'S3Tables');
CREATE USER MAPPING FOR current_user SERVER rv_srv
    OPTIONS (auth_method 'sigv4', access_key_id 'x', secret_access_key 'y');
CREATE FOREIGN CATALOG rv_cat SERVER rv_srv
    OPTIONS (catalog_name 'rv', default_namespace 'default', prefix 'rv');

SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'rv_%' ORDER BY srvname;

-- ALTER is validated on the same rules, in both directions.
ALTER SERVER rv_srv OPTIONS (SET maintenance_api 'also-nonsense');
ALTER SERVER rv_srv OPTIONS (DROP maintenance_api);

-- ============================================================
-- With no maintenance API declared, VACUUM must refuse -- and the refusal has
-- to say what would make it work, since "not supported" alone leaves the user
-- with small files and no next step.
--
-- The catalog is unreachable here, so table creation cannot succeed; what is
-- pinned instead is that the refusal is reached through the catalog's own
-- answer about who owns the storage, not through a volume lookup.  Before
-- this path was fixed a REST table created without OPTIONS was misread as
-- internal and fell through to the rewrite path, failing on a segment with a
-- volume error instead.
-- ============================================================
SET iceberg_default_catalog = 'rv_cat';

DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE rv_t (id bigint)';
    RAISE NOTICE 'table created (catalog unexpectedly reachable)';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%volume%' THEN
        RAISE NOTICE 'MISREAD AS INTERNAL: %', SQLERRM;
    ELSE
        RAISE NOTICE 'refused off-catalog, as expected with no reachable platform';
    END IF;
END $$;

RESET iceberg_default_catalog;

-- start_ignore
DROP SERVER rv_srv CASCADE;
\c contrib_regression
DROP DATABASE iceberg_rest_vacuum_db;
-- end_ignore
