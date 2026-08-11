-- Iceberg ALTER TABLE: still-unsupported subcommands (issue #401, after #334)
-- Purpose: ADD COLUMN (bare nullable) / DROP COLUMN / DROP NOT NULL / SET NOT
-- NULL / ALTER COLUMN TYPE (widening) / RENAME COLUMN are supported (covered by
-- iceberg_alter_schema_evolution).  This test pins the subcommands that remain
-- rejected -- each must fail cleanly (FEATURE_NOT_SUPPORTED, no crash) and leave
-- the table fully usable, so #334's SIGSEGV never recurs.  NOT NULL / DEFAULT /
-- CONSTRAINT on ADD COLUMN and SET/DROP DEFAULT are not representable in
-- Iceberg's optional-column model; non-widening ALTER COLUMN TYPE (narrowing /
-- scale change) is rejected and covered in iceberg_alter_schema_evolution.

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog + volume setup
CREATE SERVER alter_neg_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER alter_neg_catalog_server;
CREATE FOREIGN CATALOG alter_neg_catalog SERVER alter_neg_catalog_server;
SET iceberg_default_catalog = 'alter_neg_catalog';

CREATE SERVER alter_neg_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER alter_neg_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');
CREATE FOREIGN VOLUME alter_neg_volume SERVER alter_neg_volume_server
    OPTIONS(base_path '/alter_neg_volume/');
SET iceberg_default_volume = 'alter_neg_volume';

-- Target table with two rows so a rewrite-triggering ALTER would have data to
-- chew through (this is what SIGSEGV'd in #334).
CREATE ICEBERG TABLE alter_block_t (id int, val int, name text);
INSERT INTO alter_block_t VALUES (1, 100, 'a'), (2, 200, 'b');

-- ============================================================
-- Each still-unsupported subcommand must be rejected cleanly (no crash).
-- ============================================================

-- SET / DROP DEFAULT: no Iceberg equivalent kept in sync.
ALTER TABLE alter_block_t ALTER COLUMN val SET DEFAULT 0;
ALTER TABLE alter_block_t ALTER COLUMN val DROP DEFAULT;

-- ADD COLUMN with NOT NULL / DEFAULT / CONSTRAINT: only bare nullable allowed.
ALTER TABLE alter_block_t ADD COLUMN bad int NOT NULL;
ALTER TABLE alter_block_t ADD COLUMN bad2 int DEFAULT 5;
ALTER TABLE alter_block_t ADD COLUMN bad3 int UNIQUE;

-- ADD table CONSTRAINT.
ALTER TABLE alter_block_t ADD CONSTRAINT alter_block_chk CHECK (val >= 0);

-- ============================================================
-- Table is still usable: schema unchanged (id int, val int, name text) and the
-- rejected DDLs left no side effect on PG or Iceberg metadata.
-- ============================================================
SELECT * FROM alter_block_t ORDER BY id;
INSERT INTO alter_block_t VALUES (3, 300, 'c');
SELECT * FROM alter_block_t ORDER BY id;

-- Cleanup
DROP TABLE alter_block_t;

DROP VOLUME alter_neg_volume;
DROP USER MAPPING FOR current_user SERVER alter_neg_volume_server;
DROP SERVER alter_neg_volume_server;
DROP CATALOG alter_neg_catalog;
DROP USER MAPPING FOR current_user SERVER alter_neg_catalog_server;
DROP SERVER alter_neg_catalog_server;
