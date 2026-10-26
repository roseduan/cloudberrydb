-- Iceberg Polaris: binding a local table to a table Polaris already holds
-- (cloud#610)
--
-- Two ways bind a local definition to an existing table:
--   * re-CREATE of a name Polaris still holds -- DROP TABLE on an external
--     catalog removes only the local definition, and a later CREATE of the
--     same name registers the existing table (cloud#609, by design);
--   * an explicit mount, OPTIONS (table '...').
-- Either way the local definition then decides how the existing data files
-- are read, so the agent refuses the binding when the reader would decode
-- the table wrongly: a column the table does not have, or a type the reader
-- cannot read as the table's (ExistingTableSchemaCheck).  It must also
-- declare every column, in the table's order: the reader and the writer take
-- a column's attribute number as its field-id (issue #461).  Nullability may
-- differ, and column names match case-insensitively, as the reader matches
-- them.
--
-- Before the fix, (id text) over (id int) data was bound silently and the
-- SELECT read int bytes as text: garbage, or a segment SIGSEGV in the
-- parquet dictionary decoder.
--
-- DROP TABLE leaves every table this test creates in Polaris -- that is what
-- the re-CREATE cases rely on -- so the test also relies on the suite's
-- installcheck wiping the Polaris catalog before it runs (see the Makefile).
-- Run on its own against a catalog that still holds a previous run's tables,
-- the seed CREATEs adopt them and the row counts differ.

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
RESET client_min_messages;

CREATE SERVER recreate_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-west-2',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER recreate_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME recreate_volume SERVER recreate_volume_server OPTIONS (base_path '/');
SET iceberg_default_volume = 'recreate_volume';

CREATE SERVER recreate_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (type 'polaris', url 'http://polaris:8181/api/catalog');
CREATE USER MAPPING FOR current_user
SERVER recreate_catalog_server
OPTIONS (client_id 'root', client_secret 's3cr3t', scope 'PRINCIPAL_ROLE:ALL');
CREATE FOREIGN CATALOG recreate_catalog
SERVER recreate_catalog_server
OPTIONS (catalog_name 'polaris_default_catalog', default_namespace 'public');
SET iceberg_default_catalog = 'recreate_catalog';

-- The agent's error is followed by a Java stack trace; keep only the
-- agent's message so the expected output is stable.
CREATE FUNCTION recreate_try(stmt text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE stmt;
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    RETURN 'ERROR: ' || coalesce(substring(SQLERRM FROM 'IllegalArgumentException: ([^\n]*)'),
                                 split_part(SQLERRM, E'\n', 1));
END;
$$;

-- Seed: an (id int, v text) table that stays in Polaris after DROP.
CREATE ICEBERG TABLE iceberg_polaris_recreate (id int, v text);
INSERT INTO iceberg_polaris_recreate VALUES (1, 'a'), (2, 'b'), (3, 'c');
DROP TABLE iceberg_polaris_recreate;

-- ============================================================
-- re-CREATE, refused: the reader would decode the table wrongly
-- ============================================================

-- different type (the cloud#610 reproducer)
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (id text, v text)');
-- a wider integer is still a different scanner
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (id bigint, v text)');
-- a column the table does not have
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (id int, v text, extra int)');
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (other_id int, v text)');
-- another order, or only some of the columns
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (v text, id int)');
SELECT recreate_try('CREATE ICEBERG TABLE iceberg_polaris_recreate (id int)');
SELECT to_regclass('iceberg_polaris_recreate') IS NULL AS not_registered;

-- ============================================================
-- re-CREATE, bound (cloud#609 behaviour kept)
-- ============================================================

-- the same definition
CREATE ICEBERG TABLE iceberg_polaris_recreate (id int, v text);
INSERT INTO iceberg_polaris_recreate VALUES (4, 'd');
SELECT id, v FROM iceberg_polaris_recreate ORDER BY id;
DROP TABLE iceberg_polaris_recreate;

-- NOT NULL, and a name matched case-insensitively
CREATE ICEBERG TABLE iceberg_polaris_recreate ("ID" int NOT NULL, v text);
SELECT "ID", v FROM iceberg_polaris_recreate ORDER BY "ID";
DROP TABLE iceberg_polaris_recreate;

-- ============================================================
-- Explicit mount, OPTIONS (table '...'): same rules
-- ============================================================

CREATE ICEBERG TABLE iceberg_polaris_mount_src (id int, v text);
INSERT INTO iceberg_polaris_mount_src VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- refused: different type (the cloud#610 crash through the mount path), extra
-- column, another order, a subset
SELECT recreate_try($$CREATE ICEBERG TABLE iceberg_polaris_mount (id text, v text)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_src')$$);
SELECT recreate_try($$CREATE ICEBERG TABLE iceberg_polaris_mount (id int, v text, extra int)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_src')$$);
SELECT recreate_try($$CREATE ICEBERG TABLE iceberg_polaris_mount (v text, id int)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_src')$$);
SELECT recreate_try($$CREATE ICEBERG TABLE iceberg_polaris_mount ("V" text)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_src')$$);
SELECT to_regclass('iceberg_polaris_mount') IS NULL AS not_mounted;

-- bound: the same definition, written through
CREATE ICEBERG TABLE iceberg_polaris_mount (id int, v text)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_src');
INSERT INTO iceberg_polaris_mount VALUES (4, 'd');
SELECT id, v FROM iceberg_polaris_mount ORDER BY id;
DROP TABLE iceberg_polaris_mount;
DROP TABLE iceberg_polaris_mount_src;

-- a mount of a table that does not exist yet creates it, as before
CREATE ICEBERG TABLE iceberg_polaris_mount (id int, v text)
    OPTIONS (namespace 'public', table 'iceberg_polaris_mount_new');
INSERT INTO iceberg_polaris_mount VALUES (9, 'z');
SELECT id, v FROM iceberg_polaris_mount;
DROP TABLE iceberg_polaris_mount;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FUNCTION recreate_try(text);
DROP CATALOG recreate_catalog;
DROP USER MAPPING FOR current_user SERVER recreate_catalog_server;
DROP SERVER recreate_catalog_server;
DROP VOLUME recreate_volume;
DROP USER MAPPING FOR current_user SERVER recreate_volume_server;
DROP SERVER recreate_volume_server;
