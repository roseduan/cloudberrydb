-- Regression test for the generic Iceberg REST catalog (issue #408) DDL
-- surface.  Everything here is option validation and volume resolution, so
-- no REST server, agent or object store is contacted: the checks under test
-- all run before the first network call.  Live-catalog behaviour (SELECT,
-- INSERT, DROP semantics, VACUUM rejection on a real table) is verified
-- against a real deployment, not here.

-- start_ignore
DROP DATABASE IF EXISTS iceberg_rest_ddl_db;
CREATE DATABASE iceberg_rest_ddl_db;
\c iceberg_rest_ddl_db
CREATE EXTENSION datalake_fdw;
-- end_ignore

-- ============================================================
-- Layer 1: CREATE SERVER carries type 'rest', so the whole server-side
-- option rule set runs in the validator.
-- ============================================================

-- url is mandatory
CREATE SERVER rest_no_url FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest');

-- an empty value is a failed shell expansion, not "unset"
CREATE SERVER rest_empty_url FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url '');

CREATE SERVER rest_empty_region FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg', region '');

-- endpoint is derived from url; a url we cannot derive from must be rejected
-- rather than silently leaving the data plane unaddressed
CREATE SERVER rest_userinfo_url FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://user:pw@catalog.example.test/iceberg');

CREATE SERVER rest_scheme_url FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'ftp://catalog.example.test/iceberg');

-- ... unless the user gives the endpoint explicitly
CREATE SERVER rest_userinfo_ok FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://user:pw@catalog.example.test/iceberg',
             endpoint 'https://s3.example.test');

-- path_style_access is a strict boolean
CREATE SERVER rest_bad_pathstyle FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             path_style_access 'yes');

-- the shape that must work
CREATE SERVER rest_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             region 'us-east-1', signing_name 's3tables',
             path_style_access 'false');

SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'rest_%' ORDER BY srvname;

-- ============================================================
-- Layer 2: CREATE USER MAPPING carries no type and shares its option names
-- with other catalog types, so only the pairing rules -- safe for every
-- type -- run this early.
-- ============================================================

CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (access_key_id 'AK');

CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (secret_access_key 'SK');

CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (client_id 'CID');

CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (client_secret 'CSEC');

-- vended_credentials is a strict boolean, checked here for spelling because no
-- other catalog type defines the key
CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (auth_method 'none', vended_credentials 'yes');

CREATE USER MAPPING FOR current_user SERVER rest_srv
    OPTIONS (auth_method 'sigv4', access_key_id 'AK', secret_access_key 'SK');

CREATE FOREIGN CATALOG rest_cat SERVER rest_srv
    OPTIONS (catalog_name 'analytics', default_namespace 'public');

-- ============================================================
-- Volume resolution: a REST catalog hands out each table's location, so a
-- statically configured volume has nothing to contribute and must not be
-- allowed to override the per-table bucket.
-- ============================================================

CREATE SERVER rest_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://example.invalid:9000', region 'r',
             bucket_name 'b', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER rest_vol_srv
    OPTIONS (access_key_id 'x', secret_access_key 'y');
CREATE FOREIGN VOLUME rest_vol SERVER rest_vol_srv OPTIONS (base_path '/x/');

SET iceberg_default_catalog = 'rest_cat';

-- an explicit volume is refused, with an actionable message
CREATE ICEBERG TABLE rest_with_volume (id bigint) VOLUME rest_vol;

-- and the default volume must not sneak in either.  iceberg_default_volume
-- points at a volume that no longer exists: a catalog type that still
-- resolved volumes would fail naming it, so any *other* failure (this test
-- runs with no reachable catalog) proves the gate was bypassed.
SET iceberg_default_volume = 'rest_vol';
DROP VOLUME rest_vol;

DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE rest_no_volume (id bigint)';
    RAISE NOTICE 'volume gate bypassed (table creation reached the catalog)';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%volume%' THEN
        RAISE NOTICE 'still gated on volume: %', SQLERRM;
    ELSE
        RAISE NOTICE 'volume gate bypassed (failed later, off-catalog)';
    END IF;
END $$;

RESET iceberg_default_volume;
RESET iceberg_default_catalog;

-- ============================================================
-- The type option has two spellings.  "server_type" is the one
-- iceberg_catalog_fdw.md documents and the automation suite writes, so every
-- decision keyed on the catalog type must read it -- the option validator
-- here, and lake_table_catalog_is_rest() in the backend.  A half-migrated
-- reader creates a REST server with no url at all, then demands a volume for
-- a catalog whose runtime path never uses one.
-- ============================================================

-- the rest option rules must fire on the documented spelling too
CREATE SERVER alias_no_url FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (server_type 'rest', region 'us-east-1');

CREATE SERVER alias_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (server_type 'rest', url 'http://rest.invalid:8181/iceberg',
             region 'us-east-1');
CREATE USER MAPPING FOR current_user SERVER alias_srv
    OPTIONS (auth_method 'sigv4', access_key_id 'x', secret_access_key 'y');
CREATE FOREIGN CATALOG alias_cat SERVER alias_srv;

SELECT srvname FROM pg_foreign_server
 WHERE srvname LIKE 'alias_%' ORDER BY srvname;

-- No volume is configured anywhere, so a reader that still resolves volumes
-- fails naming one.  Any *other* failure (this test has no reachable catalog)
-- proves the catalog was recognised as rest.
DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE alias_t (id bigint) CATALOG alias_cat';
    RAISE NOTICE 'recognised as rest (table creation reached the catalog)';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%volume%' THEN
        RAISE NOTICE 'NOT recognised as rest: %', SQLERRM;
    ELSE
        RAISE NOTICE 'recognised as rest (failed later, off-catalog)';
    END IF;
END $$;

-- ============================================================
-- Layer 3: rules that need SERVER and USER MAPPING together cannot run in
-- either DDL validator, so they fire on the first full resolution.
-- ============================================================

CREATE SERVER rest_sigv4_noregion FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg');
CREATE USER MAPPING FOR current_user SERVER rest_sigv4_noregion
    OPTIONS (auth_method 'sigv4', access_key_id 'AK', secret_access_key 'SK');
CREATE FOREIGN CATALOG rest_cat_noregion SERVER rest_sigv4_noregion
    OPTIONS (catalog_name 'analytics');

SET iceberg_default_catalog = 'rest_cat_noregion';
CREATE ICEBERG TABLE rest_sigv4_bad (id bigint);
RESET iceberg_default_catalog;

CREATE SERVER rest_signing_no_sigv4 FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             region 'us-east-1', signing_name 's3tables');
CREATE USER MAPPING FOR current_user SERVER rest_signing_no_sigv4
    OPTIONS (auth_method 'none');
CREATE FOREIGN CATALOG rest_cat_signing SERVER rest_signing_no_sigv4
    OPTIONS (catalog_name 'analytics');

SET iceberg_default_catalog = 'rest_cat_signing';
CREATE ICEBERG TABLE rest_signing_bad (id bigint);
RESET iceberg_default_catalog;

CREATE SERVER rest_badauth FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg');
CREATE USER MAPPING FOR current_user SERVER rest_badauth
    OPTIONS (auth_method 'basic');
CREATE FOREIGN CATALOG rest_cat_badauth SERVER rest_badauth
    OPTIONS (catalog_name 'analytics');

SET iceberg_default_catalog = 'rest_cat_badauth';
CREATE ICEBERG TABLE rest_badauth_tbl (id bigint);
RESET iceberg_default_catalog;

-- ============================================================
-- Turning vending off means the static keys are the only source of
-- data-plane credentials, so their absence is refused at configuration time
-- rather than surfacing later as an opaque 403.
-- ============================================================

CREATE SERVER rest_novend FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg');
CREATE USER MAPPING FOR current_user SERVER rest_novend
    OPTIONS (auth_method 'none', vended_credentials 'off');
CREATE FOREIGN CATALOG rest_cat_novend SERVER rest_novend
    OPTIONS (catalog_name 'analytics');

SET iceberg_default_catalog = 'rest_cat_novend';
CREATE ICEBERG TABLE rest_novend_tbl (id bigint);
RESET iceberg_default_catalog;

-- the same thing with static keys present is accepted (it fails later, off
-- host, because this test has no reachable catalog)
CREATE SERVER rest_novend_ok FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg');
CREATE USER MAPPING FOR current_user SERVER rest_novend_ok
    OPTIONS (auth_method 'none', vended_credentials 'off',
             access_key_id 'AK', secret_access_key 'SK');
CREATE FOREIGN CATALOG rest_cat_novend_ok SERVER rest_novend_ok
    OPTIONS (catalog_name 'analytics');

SET iceberg_default_catalog = 'rest_cat_novend_ok';
DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE rest_novend_ok_tbl (id bigint)';
    RAISE NOTICE 'accepted (reached the catalog)';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%vended_credentials%' THEN
        RAISE NOTICE 'wrongly rejected: %', SQLERRM;
    ELSE
        RAISE NOTICE 'accepted (failed later, off-catalog)';
    END IF;
END $$;
RESET iceberg_default_catalog;

-- ============================================================
-- vended_credentials is accepted on the SERVER as a deployment-wide default,
-- and the USER MAPPING overrides it.  It used to be read only from the user
-- mapping, so the same option on a server was stored and silently ignored --
-- an operator who had switched vending off kept using vended credentials with
-- no error and no hint.  For a switch whose job is to narrow credential
-- scope, failing in the permissive direction is the one thing it must not do.
-- ============================================================

CREATE SERVER rest_srvoff FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             vended_credentials 'off');

-- off on the SERVER now reaches the same check as off on the USER MAPPING:
-- no static keys anywhere, so this is refused rather than quietly vending
CREATE USER MAPPING FOR current_user SERVER rest_srvoff
    OPTIONS (auth_method 'none');
CREATE FOREIGN CATALOG rest_cat_srvoff SERVER rest_srvoff
    OPTIONS (catalog_name 'analytics');
SET iceberg_default_catalog = 'rest_cat_srvoff';
CREATE ICEBERG TABLE rest_srvoff_tbl (id bigint);
RESET iceberg_default_catalog;

-- the user mapping wins over the server: 'on' here re-enables vending, so the
-- missing static keys are no longer a problem
DROP USER MAPPING FOR current_user SERVER rest_srvoff;
CREATE USER MAPPING FOR current_user SERVER rest_srvoff
    OPTIONS (auth_method 'none', vended_credentials 'on');
SET iceberg_default_catalog = 'rest_cat_srvoff';
DO $$
BEGIN
    EXECUTE 'CREATE ICEBERG TABLE rest_srvoff_override (id bigint)';
    RAISE NOTICE 'user mapping override accepted (reached the catalog)';
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%vended_credentials%' THEN
        RAISE NOTICE 'user mapping did NOT override the server: %', SQLERRM;
    ELSE
        RAISE NOTICE 'user mapping override accepted (failed later, off-catalog)';
    END IF;
END $$;
RESET iceberg_default_catalog;

-- spelling is rejected on the SERVER too, at CREATE SERVER time: a typo must
-- never read as the permissive value
CREATE SERVER rest_srvoff_typo FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             vended_credentials 'no');

-- ============================================================
-- polaris_server_realm on a generic REST server.  A Polaris deployment is a
-- spec-compliant REST catalog and is otherwise reachable over this type, but
-- one configured with a non-default realm rejects the OAuth token request
-- with 404 MissingOrInvalidRealm unless the Polaris-Realm header is sent.
-- Vendor-specific on a protocol-only type, for the same reason
-- maintenance_api is: without it such a deployment cannot be reached here at
-- all.
-- ============================================================

CREATE SERVER rest_realm FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             polaris_server_realm 'tenant-a');

-- an empty value is a failed shell expansion, not "no realm"
CREATE SERVER rest_realm_empty FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg',
             polaris_server_realm '');

SELECT srvname, srvoptions FROM pg_foreign_server
 WHERE srvname LIKE 'rest_realm%' ORDER BY srvname;

-- ============================================================
-- Cache options converged on one name: metadata_cache, minutes, on both
-- FOREIGN CATALOG and TABLE.  The old catalog-level spellings
-- (enable_metadata_cache / metadata_cache_ttl / auto_refresh_metadata) never
-- worked in any released version and are gone from the code; a DDL that still
-- carries them must succeed with the options ignored, so dumps taken from
-- older clusters restore cleanly.
-- ============================================================

CREATE SERVER cacheopt_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'rest', url 'https://catalog.example.test/iceberg');
CREATE USER MAPPING FOR current_user SERVER cacheopt_srv
    OPTIONS (auth_method 'none');

-- the surviving option, catalog level, minutes
CREATE FOREIGN CATALOG rest_cat_cache SERVER cacheopt_srv
    OPTIONS (catalog_name 'analytics', metadata_cache '5');

-- the deprecated spellings are unrecognized: accepted, stored, never parsed
CREATE FOREIGN CATALOG rest_cat_oldcache SERVER cacheopt_srv
    OPTIONS (catalog_name 'analytics',
             enable_metadata_cache 'true',
             metadata_cache_ttl '300',
             auto_refresh_metadata 'true');

SELECT fcname, fcoptions FROM pg_foreign_catalog
 WHERE fcname LIKE 'rest_cat_%cache' ORDER BY fcname;

DROP CATALOG rest_cat_oldcache;
DROP CATALOG rest_cat_cache;

-- ============================================================
-- Coexistence: adding the rest type must leave the other catalog types
-- validating exactly as they did.
-- ============================================================

CREATE SERVER coexist_hive FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (server_type 'hive', hive_metastore_uri 'thrift://hive:9083');
CREATE USER MAPPING FOR current_user SERVER coexist_hive
    OPTIONS (username 'hive_user', auth_method 'simple');

CREATE SERVER coexist_polaris FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user SERVER coexist_polaris
    OPTIONS (client_id 'pid', client_secret 'psec', scope 'PRINCIPAL_ROLE:ALL');

CREATE SERVER coexist_builtin FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER coexist_builtin;

-- a hive server never enters the rest rules, so its missing url is fine
SELECT srvname FROM pg_foreign_server
 WHERE srvname LIKE 'coexist_%' OR srvname LIKE 'rest_%'
 ORDER BY srvname;

-- no orphan relations from any of the rejected CREATE ICEBERG TABLEs
SELECT relname FROM pg_class WHERE relname LIKE 'rest_%' ORDER BY relname;

-- ============================================================
-- Blast radius: the planner hook that routes the storage envelope now walks
-- INSERT plans as well as UPDATE/DELETE, so it sees every write statement in
-- the database.  Ordinary DML must be entirely unaffected.
-- ============================================================

CREATE TABLE plain_dml (id int, payload text) DISTRIBUTED BY (id);
INSERT INTO plain_dml VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO plain_dml SELECT id + 10, payload FROM plain_dml;
UPDATE plain_dml SET payload = 'z' WHERE id = 2;
DELETE FROM plain_dml WHERE id = 3;
SELECT id, payload FROM plain_dml ORDER BY id;

-- A plain INSERT plan must carry no FDW private state.  Pin the planner: what
-- is being asserted is that the hook adds nothing, and ORCA and the Postgres
-- planner shape this same statement differently (extra Result nodes), so an
-- unpinned EXPLAIN fails on whichever cluster happens to pick the other one.
SET optimizer = off;
EXPLAIN (COSTS OFF) INSERT INTO plain_dml VALUES (99, 'x');
RESET optimizer;

CREATE TABLE plain_ctas AS SELECT id FROM plain_dml WHERE id < 12 DISTRIBUTED BY (id);
SELECT count(*) FROM plain_ctas;
DROP TABLE plain_ctas;
DROP TABLE plain_dml;

-- start_ignore
\c contrib_regression
DROP DATABASE iceberg_rest_ddl_db;
-- end_ignore
