-- Regression test for issue #363: a relation created with the iceberg AM
-- outside of CREATE ICEBERG TABLE has no pg_lake_table / iceberg metadata
-- entries, making it unusable and undroppable.  All such creation paths must
-- be rejected up front, and no orphan pg_class rows may be left behind.

-- start_ignore
DROP DATABASE IF EXISTS iceberg_am_misuse_db;
CREATE DATABASE iceberg_am_misuse_db;
\c iceberg_am_misuse_db
CREATE EXTENSION datalake_fdw;
-- end_ignore

SELECT count(*) FROM pg_extension WHERE extname = 'datalake_fdw';

-- plain CREATE TABLE ... USING iceberg
CREATE TABLE misuse_plain (id bigint) USING iceberg;

-- CREATE TABLE AS
CREATE TABLE misuse_ctas USING iceberg AS SELECT 1 AS id;

-- CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW misuse_mv USING iceberg AS SELECT 1 AS id;

-- via default_table_access_method
SET default_table_access_method = iceberg;
CREATE TABLE misuse_default (id int);
RESET default_table_access_method;

-- ALTER TABLE ... SET ACCESS METHOD
CREATE TABLE misuse_heap (id int);
ALTER TABLE misuse_heap SET ACCESS METHOD iceberg;
DROP TABLE misuse_heap;

-- no orphan relations may be left behind
SELECT relname FROM pg_class WHERE relname LIKE 'misuse%' ORDER BY relname;

-- ============================================================
-- issue #845: CREATE ICEBERG TABLE without a volume must fail with an
-- actionable message (it used to fail with "foreign volume with OID 0
-- does not exist" deep in the AM create path), for every catalog type
-- including Polaris.  A builtin catalog needs no external service, so it
-- is enough to drive the CreateLakeTable validation here.
-- ============================================================
CREATE SERVER misuse_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER misuse_cat_srv;
CREATE FOREIGN CATALOG misuse_cat SERVER misuse_cat_srv;
SET iceberg_default_catalog = 'misuse_cat';

-- no volume clause and no iceberg_default_volume
CREATE ICEBERG TABLE misuse_no_volume (id bigint);

-- iceberg_default_volume left pointing at a since-dropped volume must name it
CREATE SERVER misuse_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://example.invalid:9000', region 'r',
             bucket_name 'b', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER misuse_vol_srv
    OPTIONS (access_key_id 'x', secret_access_key 'y');
CREATE FOREIGN VOLUME misuse_vol SERVER misuse_vol_srv OPTIONS (base_path '/x/');
SET iceberg_default_volume = 'misuse_vol';
DROP VOLUME misuse_vol;
CREATE ICEBERG TABLE misuse_no_volume (id bigint);
RESET iceberg_default_volume;
RESET iceberg_default_catalog;

SELECT relname FROM pg_class WHERE relname LIKE 'misuse%' ORDER BY relname;

-- start_ignore
\c contrib_regression
DROP DATABASE iceberg_am_misuse_db;
-- end_ignore
