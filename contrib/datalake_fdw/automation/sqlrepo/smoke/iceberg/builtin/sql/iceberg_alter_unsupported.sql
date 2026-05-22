-- Iceberg ALTER TABLE coverage (issue #334 + follow-up)
-- Purpose: NO ALTER TABLE subcommand is supported on Iceberg AM tables.
-- ALTER COLUMN TYPE used to SIGSEGV (empty AM stubs + uninitialized DML
-- state during table rewrite); the others silently half-applied (PG catalog
-- changed while Iceberg manifest stayed stale).  ADD COLUMN was briefly
-- allowed but offered no way to keep the Iceberg-side schema in sync, so
-- it is now rejected too -- schema evolution must go through Spark / Trino /
-- Flink.  RENAME COLUMN (T_RenameStmt) is rejected by a sibling hook.
-- This test pins the full-ban contract and confirms the table stays usable
-- after every rejection.

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

-- ============================================================
-- Set up the target table with two rows so any rewrite-triggering
-- ALTER would have data to chew through (this is what crashed in #334).
-- ============================================================
CREATE ICEBERG TABLE alter_block_t (id int, val int, name text);
INSERT INTO alter_block_t VALUES (1, 100, 'a'), (2, 200, 'b');

-- ============================================================
-- Every ALTER subcommand must be rejected with FEATURE_NOT_SUPPORTED.
-- ============================================================

-- ADD COLUMN: previously the only allowed subcommand; now also rejected.
ALTER TABLE alter_block_t ADD COLUMN note text;

-- Original repro from issue #334 (this used to SIGSEGV the backend).
ALTER TABLE alter_block_t ALTER COLUMN val TYPE bigint;

ALTER TABLE alter_block_t DROP COLUMN name;
ALTER TABLE alter_block_t ALTER COLUMN val SET NOT NULL;
ALTER TABLE alter_block_t ALTER COLUMN val DROP NOT NULL;
ALTER TABLE alter_block_t ALTER COLUMN val SET DEFAULT 0;
ALTER TABLE alter_block_t ALTER COLUMN val DROP DEFAULT;
ALTER TABLE alter_block_t ADD CONSTRAINT alter_block_chk CHECK (val >= 0);

-- RENAME COLUMN goes through RenameStmt (a separate hook branch).
ALTER TABLE alter_block_t RENAME COLUMN val TO val2;

-- ============================================================
-- Table is still usable: schema unchanged (id int, val int, name text) and
-- the rejected DDLs left no side effects on PG or Iceberg metadata.
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
