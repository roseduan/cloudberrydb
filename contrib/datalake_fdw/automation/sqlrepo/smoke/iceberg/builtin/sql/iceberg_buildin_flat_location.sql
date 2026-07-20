CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER flat_loc_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER flat_loc_catalog_server;
CREATE FOREIGN CATALOG flat_loc_catalog SERVER flat_loc_catalog_server;
set iceberg_default_catalog='flat_loc_catalog';

-- volume
CREATE SERVER flat_loc_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER flat_loc_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');

CREATE FOREIGN VOLUME flat_loc_volume SERVER flat_loc_volume_server OPTIONS(base_path '/flat_loc_vol/');
set iceberg_default_volume='flat_loc_volume';

-- ============================================================
-- Test 1: builtin tables are stored FLAT and SHARE one directory
-- ============================================================
-- After #344 the builtin storage layout drops the db/namespace/table
-- suffix: every builtin table of a volume lives directly under the
-- volume base path, so the segment right after "<base_path>/" is
-- "metadata" (Iceberg's own subdir), not "<db>/<schema>/<table>/".
-- Two tables in the same volume therefore share one root directory.
CREATE ICEBERG TABLE flat_a (id bigint, val text);
CREATE ICEBERG TABLE flat_b (id bigint, val text);

INSERT INTO flat_a VALUES (1, 'a1');
INSERT INTO flat_b VALUES (1, 'b1');

-- ok is true when ALL hold:
--   flat  : the path segment after "/flat_loc_vol/" is "metadata/..."
--           (would be "<db>/<schema>/<table>/metadata/..." pre-#344),
--   shared: both tables resolve to the same single root directory, and
--   both  : both flat_a and flat_b actually contributed a row -- guards
--           against a vacuous pass if only one table's metadata is present.
SELECT bool_and(regexp_replace(m.metadata_location, '.*/flat_loc_vol/', '')
                LIKE 'metadata/%')
       AND count(DISTINCT regexp_replace(m.metadata_location,
                                         '/metadata/[^/]*$', '')) = 1
       AND count(DISTINCT m.relid) = 2   AS ok
  FROM pg_ext_aux.pg_iceberg_metadata m
  JOIN pg_class c ON c.oid = m.relid
 WHERE c.relname IN ('flat_a', 'flat_b');

DROP TABLE flat_a;
DROP TABLE flat_b;

-- ============================================================
-- Test 2: a user-specified location is rejected for builtin tables
-- ============================================================
-- Builtin storage is derived solely from the volume base path, so an
-- explicit location has no meaning and must be rejected outright
-- rather than silently ignored.
CREATE ICEBERG TABLE flat_reject (id bigint) OPTIONS (location 'custom/path');

-- cleanup
DROP VOLUME flat_loc_volume;
DROP USER MAPPING FOR current_user SERVER flat_loc_volume_server;
DROP SERVER flat_loc_volume_server;
DROP CATALOG flat_loc_catalog;
DROP USER MAPPING FOR current_user SERVER flat_loc_catalog_server;
DROP SERVER flat_loc_catalog_server;
