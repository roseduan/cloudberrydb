-- Binding an existing Hive Iceberg table (issue #461).
-- smoke/iceberg/polaris/iceberg_polaris_mount_schema covers polaris, hadoop
-- and s3; this covers Hive.
--
-- A mount (OPTIONS (table ...)) and a CREATE without OPTIONS that adopts a
-- table the catalog already holds are both refused unless the local
-- definition declares every column of the table, in its order, with types the
-- reader can decode: the reader and the writer take a column's attribute
-- number as its field-id.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER hms_cat_srv
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'hive', url 'thrift://hive-metastore:9083');
CREATE USER MAPPING FOR current_user SERVER hms_cat_srv;
CREATE FOREIGN CATALOG hms_cat SERVER hms_cat_srv;
SET iceberg_default_catalog = 'hms_cat';

CREATE SERVER hms_vol_srv
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER hms_vol_srv
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME hms_vol SERVER hms_vol_srv OPTIONS (base_path '/hive_volume/');
SET iceberg_default_volume = 'hms_vol';

-- The agent's error is followed by a Java stack trace; keep only the
-- agent's message so the expected output is stable.
CREATE FUNCTION hms_try(stmt text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE stmt;
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    RETURN 'ERROR: ' || coalesce(substring(SQLERRM FROM 'IllegalArgumentException: ([^\n]*)'),
                                 split_part(SQLERRM, E'\n', 1));
END;
$$;

-- The source table: field-ids 1 (id) and 2 (v).
CREATE ICEBERG TABLE hms_src (id int, v text);
INSERT INTO hms_src VALUES (1, 'a'), (2, 'b');

-- ============================================================
-- Refused: a definition that cannot read the table
-- ============================================================
-- A mount with another type.
SELECT hms_try($$CREATE ICEBERG TABLE hms_bad (id text, v text)
    OPTIONS (namespace 'public', table 'hms_src')$$);
-- A mount with a column the table does not have.
SELECT hms_try($$CREATE ICEBERG TABLE hms_bad (id int, v text, extra int)
    OPTIONS (namespace 'public', table 'hms_src')$$);
-- A mount with the columns in another order, or only some of them.
SELECT hms_try($$CREATE ICEBERG TABLE hms_bad (v text, id int)
    OPTIONS (namespace 'public', table 'hms_src')$$);
SELECT hms_try($$CREATE ICEBERG TABLE hms_bad (v text)
    OPTIONS (namespace 'public', table 'hms_src')$$);
SELECT to_regclass('hms_bad') IS NULL AS not_mounted;

-- A CREATE without OPTIONS of the same name, after DROP left the table in
-- Hive, adopts it -- and is refused the same way when it cannot read it.
DROP TABLE hms_src;
SELECT hms_try('CREATE ICEBERG TABLE hms_src (id bigint, v text)');
SELECT to_regclass('hms_src') IS NULL AS not_adopted;
CREATE ICEBERG TABLE hms_src (id int, v text);
SELECT * FROM hms_src ORDER BY id;

-- ============================================================
-- Bound: the table's own definition, written and read through
-- ============================================================
CREATE ICEBERG TABLE hms_m (id int, v text)
OPTIONS (namespace 'public', table 'hms_src');
INSERT INTO hms_m VALUES (3, 'c');
SELECT * FROM hms_m ORDER BY id;
SET vector.enable_vectorization = on;
SELECT * FROM hms_m ORDER BY id;
RESET vector.enable_vectorization;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE hms_m;
DROP TABLE hms_src;
DROP FUNCTION hms_try(text);
RESET iceberg_default_catalog;
RESET iceberg_default_volume;
DROP CATALOG hms_cat;
DROP USER MAPPING FOR current_user SERVER hms_cat_srv;
DROP SERVER hms_cat_srv;
DROP VOLUME hms_vol;
DROP USER MAPPING FOR current_user SERVER hms_vol_srv;
DROP SERVER hms_vol_srv;
