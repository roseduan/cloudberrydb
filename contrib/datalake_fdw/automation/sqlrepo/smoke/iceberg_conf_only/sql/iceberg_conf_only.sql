-- A volume that names only a conf section, and nothing else (issue #410).
--
-- The endpoint, region and credentials are in the confonly_minio section of
-- s3.conf, which the suite's Makefile stages into the coordinator's data
-- directory.  Only the agent reads that file, so everything the segments know
-- about this volume has to arrive through the load-table response.
--
-- Two things are pinned here.  The read path works: the agent returns the
-- merged section and the scan fills its gaps from it.  The write path does
-- not, and fails cleanly rather than hanging -- the guard names the volume and
-- the conf section, which is the difference between a fixable error message
-- and minutes inside a retry loop.

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

-- The volume under test: no endpoint, no credentials.
CREATE SERVER confonly_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'confonly_minio', bucket_name 'warehouse');
CREATE USER MAPPING FOR current_user SERVER confonly_srv OPTIONS (unused 'x');
CREATE FOREIGN VOLUME confonly_vol SERVER confonly_srv
    OPTIONS (base_path '/iceberg_conf_only/', allow_writes 'true');

-- A fully specified volume over the same bucket, used only to put data there.
CREATE SERVER full_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
             bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER full_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME full_vol SERVER full_srv
    OPTIONS (base_path '/iceberg_conf_only/', allow_writes 'true');

CREATE SERVER confonly_cat FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user SERVER confonly_cat
    OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG confonly_c SERVER confonly_cat
    OPTIONS (catalog_name 'polaris_default_catalog');
SET iceberg_default_catalog = 'confonly_c';

-- Seed through the fully specified volume.
SET iceberg_default_volume = 'full_vol';
CREATE ICEBERG TABLE co_seed (id int, name text)
    OPTIONS (namespace 'public', table 'co_seed');
-- CREATE attaches to the iceberg table if the catalog already has it, and
-- DROP TABLE does not purge it, so a previous run's rows would still be here
-- and the counts below would climb.  Reset instead of assuming.  The DELETE
-- is wrapped so its row count, which depends on what a previous run left,
-- stays out of the expected output.
DO $$ BEGIN DELETE FROM co_seed; END $$;
INSERT INTO co_seed VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT count(*) AS seeded FROM co_seed;

-- Read the same iceberg table through the conf-only volume.  Declared after
-- the data exists, so a stale metadata snapshot cannot make an empty result
-- look like success.
SET iceberg_default_volume = 'confonly_vol';
CREATE ICEBERG TABLE co_read (id int, name text)
    OPTIONS (namespace 'public', table 'co_seed');
SELECT id, name FROM co_read ORDER BY id;
SELECT count(*) AS read_through_conf_only FROM co_read;

-- Write through the conf-only volume too.  The coordinator resolves the conf
-- section and dispatches the result with the plan, so the segments can reach
-- object storage without the SQL OPTIONS ever naming it.
INSERT INTO co_read VALUES (4, 'd');
SELECT id, name FROM co_read ORDER BY id;

-- UPDATE and DELETE take the same route, and additionally carry the fragment
-- list, so they are worth their own assertions rather than assuming INSERT
-- covers them.
UPDATE co_read SET name = 'updated' WHERE id = 4;
DELETE FROM co_read WHERE id = 1;
SELECT id, name FROM co_read ORDER BY id;

-- cleanup
SET iceberg_default_volume = 'full_vol';
DROP TABLE co_read;
DROP TABLE co_seed;
DROP VOLUME confonly_vol;
DROP VOLUME full_vol;
DROP USER MAPPING FOR current_user SERVER confonly_srv;
DROP USER MAPPING FOR current_user SERVER full_srv;
DROP SERVER confonly_srv;
DROP SERVER full_srv;
DROP CATALOG confonly_c;
DROP USER MAPPING FOR current_user SERVER confonly_cat;
DROP SERVER confonly_cat;
