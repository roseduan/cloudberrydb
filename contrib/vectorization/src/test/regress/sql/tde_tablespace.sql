-- Regression tests for tablespace-level TDE (builtin KMS provider).
--
-- Requirements:
--   tde_kms_provider          = 'builtin'
--   tde_kms_builtin_passphrase = 'regression-test-key-do-not-use-in-production'
--   allow_in_place_tablespaces = on
--
-- Run with:
--   make check TEMP_CONFIG=tde.conf EXTRA_TESTS=tde_tablespace
-- or add to a schedule that sources tde.conf via TEMP_CONFIG.

-- Create an encrypted tablespace (LOCATION '' = in-place, requires
-- allow_in_place_tablespaces = on from the temp config).
CREATE TABLESPACE enc_ts LOCATION ''
    WITH (encryption_method = 'AES256');

-- Capture OID now so we can verify .wkey presence/absence by name.
CREATE TEMP TABLE _enc_ts_oid AS
    SELECT oid FROM pg_tablespace WHERE spcname = 'enc_ts';

-- Verify the .wkey key file was created for enc_ts.
SELECT EXISTS(
    SELECT 1 FROM pg_ls_dir('pg_cryptokeys/tablespaces') f
    WHERE f = (SELECT oid::text || '.wkey' FROM _enc_ts_oid)
) AS wkey_created;

-- Load the monitoring extension and verify the status view.
CREATE EXTENSION IF NOT EXISTS pg_tde;

SELECT enc_method, loaded
    FROM pg_tde_tablespace_status
    WHERE spc_name = 'enc_ts';

-- -------------------------------------------------------------------------
-- Heap table
-- -------------------------------------------------------------------------
CREATE TABLE t_heap (id int, val text) TABLESPACE enc_ts;
INSERT INTO t_heap SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_heap;
SELECT count(*) FROM t_heap WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- AO Row table
-- -------------------------------------------------------------------------
CREATE TABLE t_ao (id int, val text)
    WITH (appendoptimized = true) TABLESPACE enc_ts;
INSERT INTO t_ao SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_ao;
SELECT count(*) FROM t_ao WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- AOCS (column-store) table
-- -------------------------------------------------------------------------
CREATE TABLE t_aocs (id int, val text)
    WITH (appendoptimized = true, orientation = column) TABLESPACE enc_ts;
INSERT INTO t_aocs SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_aocs;
SELECT count(*) FROM t_aocs WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- PAX table (columnar PORC format)
-- -------------------------------------------------------------------------
CREATE TABLE t_pax (id int, val text) USING pax TABLESPACE enc_ts;
INSERT INTO t_pax SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_pax;
SELECT count(*) FROM t_pax WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- Non-encrypted table is unaffected
-- -------------------------------------------------------------------------
CREATE TABLE t_plain (id int, val text);
INSERT INTO t_plain SELECT i, md5(i::text) FROM generate_series(1, 100) i;
SELECT count(*) FROM t_plain;

-- -------------------------------------------------------------------------
-- tde_sync_keys() should return 0 (key already loaded)
-- -------------------------------------------------------------------------
SELECT tde_sync_keys();

-- -------------------------------------------------------------------------
-- Cross-tablespace query (encrypted and plain in the same query)
-- -------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM t_heap)   AS heap_cnt,
    (SELECT count(*) FROM t_ao)     AS ao_cnt,
    (SELECT count(*) FROM t_aocs)   AS aocs_cnt,
    (SELECT count(*) FROM t_pax)    AS pax_cnt,
    (SELECT count(*) FROM t_plain)  AS plain_cnt;

-- -------------------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------------------
DROP TABLE t_pax;
DROP TABLE t_heap, t_ao, t_aocs, t_plain;
DROP TABLESPACE enc_ts;

-- Verify .wkey was removed after DROP TABLESPACE.
SELECT NOT EXISTS(
    SELECT 1 FROM pg_ls_dir('pg_cryptokeys/tablespaces') f
    WHERE f = (SELECT oid::text || '.wkey' FROM _enc_ts_oid)
) AS wkey_deleted;

-- =========================================================================
-- SM4 tablespace: verify SM4 encryption covers all storage engines
-- =========================================================================

CREATE TABLESPACE enc_ts_sm4 LOCATION ''
    WITH (encryption_method = 'SM4');

-- Capture OID for SM4 tablespace .wkey verification.
CREATE TEMP TABLE _enc_ts_sm4_oid AS
    SELECT oid FROM pg_tablespace WHERE spcname = 'enc_ts_sm4';

-- Verify the .wkey key file was created for the SM4 tablespace.
SELECT EXISTS(
    SELECT 1 FROM pg_ls_dir('pg_cryptokeys/tablespaces') f
    WHERE f = (SELECT oid::text || '.wkey' FROM _enc_ts_sm4_oid)
) AS wkey_created_sm4;

SELECT enc_method, loaded
    FROM pg_tde_tablespace_status
    WHERE spc_name = 'enc_ts_sm4';

-- -------------------------------------------------------------------------
-- Heap table in SM4 tablespace
-- -------------------------------------------------------------------------
CREATE TABLE t_sm4_heap (id int, val text) TABLESPACE enc_ts_sm4;
INSERT INTO t_sm4_heap SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_sm4_heap;
SELECT count(*) FROM t_sm4_heap WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- AO Row table in SM4 tablespace
-- -------------------------------------------------------------------------
CREATE TABLE t_sm4_ao (id int, val text)
    WITH (appendoptimized = true) TABLESPACE enc_ts_sm4;
INSERT INTO t_sm4_ao SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_sm4_ao;
SELECT count(*) FROM t_sm4_ao WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- AOCS (column-store) table in SM4 tablespace
-- -------------------------------------------------------------------------
CREATE TABLE t_sm4_aocs (id int, val text)
    WITH (appendoptimized = true, orientation = column) TABLESPACE enc_ts_sm4;
INSERT INTO t_sm4_aocs SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_sm4_aocs;
SELECT count(*) FROM t_sm4_aocs WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- PAX table in SM4 tablespace (exercises SM4-CTR position-addressable path)
-- -------------------------------------------------------------------------
CREATE TABLE t_sm4_pax (id int, val text) USING pax TABLESPACE enc_ts_sm4;
INSERT INTO t_sm4_pax SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
SELECT count(*) FROM t_sm4_pax;
SELECT count(*) FROM t_sm4_pax WHERE id BETWEEN 1 AND 100;

-- -------------------------------------------------------------------------
-- Cross-tablespace query: all four SM4 storage engines
-- -------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM t_sm4_heap)   AS sm4_heap_cnt,
    (SELECT count(*) FROM t_sm4_ao)     AS sm4_ao_cnt,
    (SELECT count(*) FROM t_sm4_aocs)   AS sm4_aocs_cnt,
    (SELECT count(*) FROM t_sm4_pax)    AS sm4_pax_cnt;

-- -------------------------------------------------------------------------
-- Cleanup SM4
-- -------------------------------------------------------------------------
DROP TABLE t_sm4_pax;
DROP TABLE t_sm4_heap, t_sm4_ao, t_sm4_aocs;
DROP TABLESPACE enc_ts_sm4;

-- Verify .wkey was removed after DROP TABLESPACE enc_ts_sm4.
SELECT NOT EXISTS(
    SELECT 1 FROM pg_ls_dir('pg_cryptokeys/tablespaces') f
    WHERE f = (SELECT oid::text || '.wkey' FROM _enc_ts_sm4_oid)
) AS wkey_deleted_sm4;
