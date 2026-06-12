-- Iceberg FDW foreign table over a Polaris catalog (issue #844).
--
-- The dlproxy getFragments request must deliver the gopher connection config
-- to the agent, or the agent fails to build its GopherFileIO with "Gopher
-- Worker path is required".  Four independent breaks are covered here:
--   - the fragment-cache branch of iceberg_get_external_fragments omitted the
--     config JSON request body entirely;
--   - gopher.worker_path was silently dropped from the emitted JSON
--     (offsetof(worker_path) == 0 collided with the "no mapping" sentinel);
--   - the agent had no POST /dlproxy/read mapping, so body-carrying requests
--     bounced with 404 (and were retried 10x as "dlagent not ready");
--   - the agent merged the request's legacy gopher values BEFORE the baseline
--     fileIOProps, letting empty baseline worker/connect paths clobber them.
--
-- The table is created and populated through the iceberg AM (CREATE ICEBERG
-- TABLE) and then read back through a datalake_fdw foreign table, which is
-- the dlproxy read path.
--
-- Requires the lakehouse stack (MinIO http://minio:9000 admin/admin12345) and
-- a Polaris server (http://polaris:8181, realm POLARIS, root/s3cr3t) with the
-- catalog polaris_default_catalog; see run.sh for the pre-clean.
\i ../../../lib/sql/common_setup.sql

SET client_min_messages = WARNING;

-- ===== Producer side: iceberg AM table on the Polaris catalog =====
DROP SERVER IF EXISTS fdwpol_vol_srv CASCADE;
CREATE SERVER fdwpol_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER fdwpol_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME fdwpol_vol SERVER fdwpol_vol_srv
    OPTIONS (base_path '/iceberg_fdw_polaris/', allow_writes 'true');
SET iceberg_default_volume = 'fdwpol_vol';

DROP SERVER IF EXISTS fdwpol_cat_srv CASCADE;
CREATE SERVER fdwpol_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog',
             polaris_server_realm 'POLARIS');
CREATE USER MAPPING FOR current_user SERVER fdwpol_cat_srv
    OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG fdwpol_cat SERVER fdwpol_cat_srv
    OPTIONS (catalog_name 'polaris_default_catalog', default_namespace 'public');
SET iceberg_default_catalog = 'fdwpol_cat';

DROP TABLE IF EXISTS fdwpol_t;
CREATE ICEBERG TABLE fdwpol_t (id int, v text);
INSERT INTO fdwpol_t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

-- ===== Consumer side: datalake_fdw foreign table (dlproxy read path) =====
DROP SERVER IF EXISTS fdwpol_oss_srv CASCADE;
CREATE SERVER fdwpol_oss_srv FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR current_user SERVER fdwpol_oss_srv
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

DROP SERVER IF EXISTS fdwpol_meta_srv CASCADE;
CREATE SERVER fdwpol_meta_srv FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (polaris_server_url 'http://polaris:8181/api/catalog',
             polaris_server_realm 'POLARIS');
CREATE USER MAPPING FOR current_user SERVER fdwpol_meta_srv
    OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');

DROP FOREIGN TABLE IF EXISTS fdwpol_read;
CREATE FOREIGN TABLE fdwpol_read (id int, v text)
SERVER fdwpol_oss_srv
OPTIONS (
    filePath '/warehouse/iceberg_fdw_polaris/',
    catalog_type 'polaris',
    server_name 'fdwpol_meta_srv',
    table_identifier 'polaris_default_catalog.public.fdwpol_t',
    format 'iceberg'
);

-- fragment-cache branch (default on)
SELECT * FROM fdwpol_read ORDER BY id;
SELECT count(*) FROM fdwpol_read;
SELECT id, v FROM fdwpol_read WHERE id >= 4 ORDER BY id;

-- non-cached branch (POST with config body)
SET datalake.enable_iceberg_fragment_cache = off;
SELECT count(*) FROM fdwpol_read;
RESET datalake.enable_iceberg_fragment_cache;

-- cleanup
DROP FOREIGN TABLE fdwpol_read;
DROP TABLE fdwpol_t;
