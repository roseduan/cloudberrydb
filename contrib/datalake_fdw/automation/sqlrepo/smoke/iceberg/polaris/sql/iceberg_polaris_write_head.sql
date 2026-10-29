-- Writing a table on an external catalog starts from the catalog's head,
-- whether or not the table carries a `table` OPTION (issue #461).
--
-- A table created without one on a Polaris (or Hive) catalog used to take the
-- metadata location stored when this cluster last wrote it as the base of a
-- DML.  A commit made meanwhile by another engine was then invisible to the
-- DML's own scan: DELETE FROM t missed the rows that commit added, and the
-- delete was rebased onto a head that still held them.  Every write now
-- resolves the base from the catalog, as tables with a `table` OPTION did.
--
-- There is no Spark here, so the "other engine" is a second local table bound
-- to the same Polaris table through OPTIONS (table ...).
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER writehead_vol_srv
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER writehead_vol_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME writehead_vol SERVER writehead_vol_srv OPTIONS (base_path '/');
SET iceberg_default_volume = 'writehead_vol';

CREATE SERVER writehead_cat_srv
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user
SERVER writehead_cat_srv
OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG writehead_cat
SERVER writehead_cat_srv
OPTIONS (catalog_name 'polaris_default_catalog');
SET iceberg_default_catalog = 'writehead_cat';

-- A table without a `table` OPTION, and another writer of it.
CREATE ICEBERG TABLE writehead_t (id int);
INSERT INTO writehead_t VALUES (1);

CREATE ICEBERG TABLE writehead_w (id int)
OPTIONS (namespace 'public', table 'writehead_t');
-- The other writer's commit moves the head writehead_t has stored behind;
-- the catch-up warnings name OIDs and metadata files, which vary per run.
SET client_min_messages = error;
INSERT INTO writehead_w VALUES (2);

-- The DELETE scans from the catalog's head, so it removes both rows.
DELETE FROM writehead_t;
RESET client_min_messages;

-- Read through a table bound afterwards, as another engine would.
CREATE ICEBERG TABLE writehead_check (id int)
OPTIONS (namespace 'public', table 'writehead_t');
SELECT count(*) AS rows_left FROM writehead_check;

DROP TABLE writehead_check;
DROP TABLE writehead_w;
DROP TABLE writehead_t;
DROP CATALOG writehead_cat;
DROP VOLUME writehead_vol;
DROP USER MAPPING FOR current_user SERVER writehead_cat_srv;
DROP SERVER writehead_cat_srv;
DROP USER MAPPING FOR current_user SERVER writehead_vol_srv;
DROP SERVER writehead_vol_srv;
