CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- catalog
CREATE SERVER default_catalog_server
FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER default_catalog_server;
CREATE FOREIGN CATALOG default_catalog SERVER default_catalog_server;
set iceberg_default_catalog='default_catalog';

-- volume
CREATE SERVER default_volume_server
FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'
);
CREATE USER MAPPING FOR current_user
SERVER default_volume_server
OPTIONS (
    access_key_id 'admin',
    secret_access_key 'admin12345');

CREATE FOREIGN VOLUME default_volume SERVER default_volume_server OPTIONS(base_path '/default_volume/');
set iceberg_default_volume='default_volume';

-- ============================================================
-- Test 1: Sequential multi-table INSERT (baseline — should pass)
-- ============================================================
-- Sequential statements in same transaction: each INSERT completes
-- its full begin→dispatch→recv→end cycle before the next starts.
-- The global variable is properly consumed within each statement.
-- This test documents that sequential INSERTs are NOT affected.

CREATE ICEBERG TABLE meta_seq_a (id bigint, val text);
CREATE ICEBERG TABLE meta_seq_b (id bigint, val text);

BEGIN;
INSERT INTO meta_seq_a VALUES (1, 'a1');
INSERT INTO meta_seq_b VALUES (10, 'b1');
INSERT INTO meta_seq_a VALUES (2, 'a2');
INSERT INTO meta_seq_b VALUES (20, 'b2');
COMMIT;

SELECT count(*) AS seq_a_count FROM meta_seq_a;
SELECT count(*) AS seq_b_count FROM meta_seq_b;

SELECT * FROM meta_seq_a ORDER BY id;
SELECT * FROM meta_seq_b ORDER BY id;

DROP TABLE meta_seq_a;
DROP TABLE meta_seq_b;

-- ============================================================
-- Test 2: Trigger-based multi-table INSERT in one dispatch
-- ============================================================
-- When a trigger on iceberg table A does INSERT INTO iceberg table B,
-- the trigger fires on QE during A's dispatch. Both A's and B's
-- FDW_SendMeta metadata flow back to QD in the SAME dispatch cycle.
-- QD's FDW_RecvMeta accumulates ALL metadata into the ONE global
-- FDW_ResultMetaList. Then end_foreign_modify(A) on QD consumes the
-- entire list (including B's metadata) and applies it to A.
-- B's metadata is never registered → B appears empty.
--
-- Note: If triggers are not supported on iceberg tables, this test
-- will report an ERROR and be skipped — that's acceptable.

CREATE ICEBERG TABLE trig_src (id bigint, val text);
CREATE ICEBERG TABLE trig_dst (id bigint, val text);

CREATE OR REPLACE FUNCTION trig_insert_to_dst() RETURNS trigger AS $$
BEGIN
    INSERT INTO trig_dst VALUES (NEW.id + 1000, NEW.val || '_cascaded');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_cascade
    AFTER INSERT ON trig_src
    FOR EACH ROW EXECUTE FUNCTION trig_insert_to_dst();

-- This single INSERT triggers writes to BOTH tables in one dispatch.
-- GPDB/Cloudberry limitation: row-level triggers on segments cannot issue
-- additional write statements (QE slice restriction). Expected to ERROR.
INSERT INTO trig_src VALUES (1, 'src1'), (2, 'src2');

-- Verify trig_src got the rows (trigger error doesn't rollback the source insert
-- in all cases, but with GPDB the whole statement fails)
SELECT count(*) AS trig_src_count FROM trig_src;
SELECT count(*) AS trig_dst_count FROM trig_dst;

DROP TRIGGER trig_cascade ON trig_src;
DROP FUNCTION trig_insert_to_dst();
DROP TABLE trig_src;
DROP TABLE trig_dst;

-- ============================================================
-- Test 3: Vacuum rewrite — serialization round-trip (Bug 1)
-- ============================================================
-- Bug 1: The vacuum rewrite path exercises:
--     pg_iceberg_modify_init_for_vacuum → BeginForeignModify
--     → scan+insert → destroyHandler() → FDW_serializeMeta
--     → FDW_SendMeta → EndForeignModify
--   If serialization field order is wrong, this crashes or corrupts data.
--
-- DEFAULT_MIN_INPUT_FILES = 5, so we must:
--   1. Lower the threshold via GUC to ensure compaction triggers
--   2. Insert enough separate files (>= threshold)

-- Lower compaction threshold to 2 files
SET datalake.iceberg_vacuum_compact_min_input_files = 2;

CREATE ICEBERG TABLE vacuum_ser_test (id bigint, val text);

-- Each INSERT creates a separate data file
INSERT INTO vacuum_ser_test VALUES (1, 'row1');
INSERT INTO vacuum_ser_test VALUES (2, 'row2');
INSERT INTO vacuum_ser_test VALUES (3, 'row3');
INSERT INTO vacuum_ser_test VALUES (4, 'row4');
INSERT INTO vacuum_ser_test VALUES (5, 'row5');

SELECT count(*) AS before_vacuum FROM vacuum_ser_test;

-- VACUUM with lowered threshold should trigger compaction rewrite.
-- The rewrite path exercises FDW_serializeMeta → FDW_SendMeta →
-- FDW_deserialize_meta_from_bytea. If field order is wrong, crash here.
VACUUM vacuum_ser_test;

-- Data should be intact after vacuum
SELECT count(*) AS after_vacuum FROM vacuum_ser_test;
SELECT * FROM vacuum_ser_test ORDER BY id;

RESET datalake.iceberg_vacuum_compact_min_input_files;
DROP TABLE vacuum_ser_test;

-- cleanup
DROP VOLUME default_volume;
DROP USER MAPPING FOR current_user SERVER default_volume_server;
DROP SERVER default_volume_server;
DROP CATALOG default_catalog;
DROP USER MAPPING FOR current_user SERVER default_catalog_server;
DROP SERVER default_catalog_server;
