-- Idempotent seed: builtin iceberg catalog + S3 volume (MinIO) + sales.orders (3 rows) + test roles.
--
-- Usage (the authz_sql psql variable is REQUIRED — it points at the authz layer this seed
-- includes at the end; without it pg_ext_aux.pg_iceberg_metadata stays PUBLIC-readable):
--   psql -d postgres \
--        -v authz_sql=/abs/path/to/contrib/datalake_rest_catalog/security/rest_catalog_authz.sql \
--        -f seed_builtin.sql
--
-- Based on the issue #382 analysis, corrected against the lightning-382 container (verified 2026-07-05):
-- some object/option names assumed in the earlier 2026-07-03 draft do not hold on this cluster, so this
-- reuses the real object names already present here (hd_catalog_server/hd_catalog/hd_volume_server/
-- hd_volume) instead of creating a parallel builtin_cat/minio_vol/wh set (see task-1-report.md).
-- Key verified differences:
--   * catalog/volume server names reuse the cluster's existing hd_catalog_server / hd_volume_server
--   * the S3 volume bucket option is named bucket_name (not bucket)
--   * user mapping options are access_key_id/secret_access_key (not accesskey/secretkey)
--   * the foreign volume path option is base_path (not path)
--   * CREATE SERVER / CREATE FOREIGN CATALOG / CREATE FOREIGN VOLUME / CREATE USER MAPPING /
--     CREATE ICEBERG TABLE all support IF NOT EXISTS natively (no extra DO $$ wrapper; CREATE ROLE excepted).
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- builtin catalog: iceberg_catalog_fdw server with no type option + matching foreign catalog (no warehouse_location_prefix)
CREATE SERVER IF NOT EXISTS hd_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE FOREIGN CATALOG IF NOT EXISTS hd_catalog SERVER hd_catalog_server;

-- S3 volume server (MinIO, :9000 in this container, bucket warehouse)
CREATE SERVER IF NOT EXISTS hd_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
  OPTIONS (type 's3', endpoint 'http://localhost:9000', region 'us-east-1',
           bucket_name 'warehouse', path_style_access 'true');

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER hd_volume_server
  OPTIONS (access_key_id 'minioadmin', secret_access_key 'minioadmin');

CREATE FOREIGN VOLUME IF NOT EXISTS hd_volume SERVER hd_volume_server
  OPTIONS (base_path '/hdw/');

SET iceberg_default_catalog = 'hd_catalog';
SET iceberg_default_volume  = 'hd_volume';

CREATE SCHEMA IF NOT EXISTS sales;
CREATE ICEBERG TABLE IF NOT EXISTS sales.orders (id bigint, name text);

DO $$ BEGIN
  IF (SELECT count(*) FROM sales.orders) = 0 THEN
    INSERT INTO sales.orders VALUES (1,'ada'),(2,'grace'),(3,'linus');
  END IF;
END $$;

-- Test roles: iceberg_reader has SELECT, no_access has none. CREATE ROLE has no IF NOT EXISTS, so guard with a DO block.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='iceberg_reader') THEN
    CREATE ROLE iceberg_reader LOGIN PASSWORD 'reader_pw';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='no_access') THEN
    CREATE ROLE no_access LOGIN PASSWORD 'noaccess_pw';
  END IF;
END $$;
GRANT USAGE ON SCHEMA sales TO iceberg_reader;
GRANT SELECT ON sales.orders TO iceberg_reader;
REVOKE ALL ON SCHEMA sales FROM no_access;

-- Internal iceberg metadata is no longer exposed to PUBLIC; access is gated by a SECURITY DEFINER
-- accessor function that filters by p_role.
-- See security/rest_catalog_authz.sql (pulled in with \i at the end of this seed; requires iceberg_authenticator to exist).
-- Fail loudly if the caller forgot -v authz_sql=...: silently skipping the include would leave
-- the PUBLIC grant on pg_ext_aux.pg_iceberg_metadata intact (the exact leak the authz layer closes).
\if :{?authz_sql}
\else
\warn 'FATAL: psql variable authz_sql is not set. Re-run with -v authz_sql=/abs/path/to/security/rest_catalog_authz.sql -- aborting BEFORE the authz layer, pg_ext_aux is still PUBLIC-readable.'
\quit
\endif
\i :authz_sql
