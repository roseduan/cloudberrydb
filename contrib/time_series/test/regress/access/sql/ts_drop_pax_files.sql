-- ts_drop_pax_files.sql — DROP TABLE cleanup of PAX sidecar dirs.
--
-- ts_pax_remove_reldir() used to fire immediately inside the DROP
-- ProcessUtility hook (synchronous rmtree).  If the surrounding xact
-- then rolled back, the catalog rows came back but the on-disk
-- chunk_<N>.pax.seg<M> files were already gone — silent corruption
-- on the next read after reclaim_chunk_heaps.
--
-- The fix routes ts_pax_remove_reldir through a per-xact pending list
-- (ts_pax_register_pending_removal) processed by an XactCallback at
-- COMMIT/ABORT.  This test asserts:
--
--   Case 1. DROP TABLE (non-tx)         → PAX dir gone
--   Case 2. DROP inside BEGIN..COMMIT   → PAX dir gone
--   Case 3. DROP inside BEGIN..ROLLBACK → PAX dir survives, the table
--           keeps returning rows even after reclaim_chunk_heaps (so
--           the data really lives in PAX, not in the heap fork).
--
-- DISTRIBUTED REPLICATED so all 3 segments own the same relid and the
-- PAX dir is identical per-segment — sums are clean one-liners.

\i sql/include/setup.sql

-- Helpers test.ts_pax_files_per_seg / test.ts_pax_files_total and
-- test.ts_chunk_files_per_seg / test.ts_chunk_files_total live in
-- sql/include/setup.sql.

-- ======================================================================
-- Case 1: DROP TABLE on a compressed time_series table (non-tx)
-- ======================================================================
DROP TABLE IF EXISTS pax1_t;
CREATE TABLE pax1_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax1_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax1_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax1_t'::regclass) > 0 AS pax1_compressed;

SELECT 'pax1_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case1_pax_files_before;
DROP TABLE pax1_t;
SELECT test.ts_pax_files_total(:relid) AS case1_pax_files_after;

-- ======================================================================
-- Case 2: DROP inside BEGIN..COMMIT — same effect as Case 1
-- ======================================================================
DROP TABLE IF EXISTS pax2_t;
CREATE TABLE pax2_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax2_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax2_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax2_t'::regclass) > 0 AS pax2_compressed;

SELECT 'pax2_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case2_pax_files_before;
BEGIN;
DROP TABLE pax2_t;
COMMIT;
SELECT test.ts_pax_files_total(:relid) AS case2_pax_files_after_commit;

-- ======================================================================
-- Case 3: DROP inside BEGIN..ROLLBACK — KEY correctness case.
--
-- Run reclaim_chunk_heaps first so the table's only copy of the data
-- lives in PAX.  Before the fix, ROLLBACK left the catalog rows but
-- the rmtree had already happened, so a post-ROLLBACK read would
-- silently return zero rows.  With the fix, reads return all rows.
-- ======================================================================
DROP TABLE IF EXISTS pax3_t;
CREATE TABLE pax3_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax3_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax3_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax3_t'::regclass) > 0 AS pax3_compressed;
SELECT time_series.reclaim_chunk_heaps('pax3_t'::regclass) > 0 AS pax3_reclaimed;

SELECT 'pax3_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case3_pax_files_before;
SELECT count(*) AS case3_rows_before_xact FROM pax3_t;

BEGIN;
DROP TABLE pax3_t;
ROLLBACK;

SELECT test.ts_pax_files_total(:relid) AS case3_pax_files_after_rollback;
SELECT count(*) AS case3_rows_after_rollback FROM pax3_t;

DROP TABLE pax3_t;
SELECT test.ts_pax_files_total(:relid) AS case3_pax_files_after_final_drop;

-- ======================================================================
-- Case 4: TRUNCATE on compressed table must clean PAX side
--
-- Before the fix, TRUNCATE rotated the heap fork's relfilenode and
-- deleted ts_chunk rows, but ts_compressed_chunk rows and the on-disk
-- ts_compressed/<old_relid>/ directory were untouched — stale metadata
-- shadowed the fresh relfilenode and PAX files leaked indefinitely.
-- ======================================================================
DROP TABLE IF EXISTS pax4_t;
CREATE TABLE pax4_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax4_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax4_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax4_t'::regclass) > 0 AS pax4_compressed;

SELECT 'pax4_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case4_pax_files_before;
SELECT count(*) AS case4_compressed_chunk_rows_before
  FROM time_series.ts_compressed_chunk
 WHERE table_oid = 'pax4_t'::regclass;

TRUNCATE pax4_t;

SELECT test.ts_pax_files_total(:relid) AS case4_pax_files_after_truncate;
SELECT count(*) AS case4_compressed_chunk_rows_after
  FROM time_series.ts_compressed_chunk
 WHERE table_oid = 'pax4_t'::regclass;
SELECT count(*) AS case4_compress_config_after
  FROM time_series.ts_compress_config
 WHERE table_oid = 'pax4_t'::regclass;
-- Re-INSERT and re-compress works on the fresh state
INSERT INTO pax4_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i + 100
FROM generate_series(0, 1) i;
SELECT time_series.compress_chunks('pax4_t'::regclass) > 0 AS pax4_recompressed;
SELECT count(*) AS case4_rows_after_recompress FROM pax4_t;

DROP TABLE pax4_t;

-- ======================================================================
-- Case 5: Savepoint correctness — ROLLBACK TO SAVEPOINT must discard
-- the DROP, so the outer COMMIT must NOT rmtree the PAX dir.
-- ======================================================================
DROP TABLE IF EXISTS pax5_t;
CREATE TABLE pax5_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax5_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax5_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax5_t'::regclass) > 0 AS pax5_compressed;
SELECT time_series.reclaim_chunk_heaps('pax5_t'::regclass) > 0 AS pax5_reclaimed;

SELECT 'pax5_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case5_pax_files_before;

BEGIN;
SAVEPOINT s1;
DROP TABLE pax5_t;
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT test.ts_pax_files_total(:relid) AS case5_pax_files_after_outer_commit;
SELECT count(*) AS case5_rows_after FROM pax5_t;
DROP TABLE pax5_t;
SELECT test.ts_pax_files_total(:relid) AS case5_pax_files_after_final_drop;

-- ======================================================================
-- Case 6: Savepoint promotion — RELEASE SAVEPOINT must keep the DROP
-- live, so the outer COMMIT MUST rmtree the PAX dir.
-- ======================================================================
DROP TABLE IF EXISTS pax6_t;
CREATE TABLE pax6_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax6_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax6_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax6_t'::regclass) > 0 AS pax6_compressed;

SELECT 'pax6_t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case6_pax_files_before;

BEGIN;
SAVEPOINT s1;
DROP TABLE pax6_t;
RELEASE SAVEPOINT s1;
COMMIT;

SELECT test.ts_pax_files_total(:relid) AS case6_pax_files_after_outer_commit;

-- ======================================================================
-- Case 7: DROP SCHEMA CASCADE — the cleanup must fire for the cascaded
-- time_series table even though the top-level statement is a SCHEMA drop.
-- ProcessUtility_hook used to be the catch — it only saw DropStmt with
-- removeType=OBJECT_TABLE and missed CASCADE drops entirely.  The fix is
-- in object_access_hook (OAT_DROP).
-- ======================================================================
DROP SCHEMA IF EXISTS pax_sc CASCADE;
CREATE SCHEMA pax_sc;
CREATE TABLE pax_sc.t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax_sc.t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pax_sc.t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pax_sc.t'::regclass) > 0 AS pax7_compressed;

SELECT 'pax_sc.t'::regclass::oid AS relid \gset
SELECT test.ts_pax_files_total(:relid) AS case7_pax_files_before;
SELECT count(*) AS case7_compressed_chunk_rows_before
  FROM time_series.ts_compressed_chunk WHERE table_oid = :relid;

DROP SCHEMA pax_sc CASCADE;

SELECT test.ts_pax_files_total(:relid) AS case7_pax_files_after_cascade;
SELECT count(*) AS case7_compressed_chunk_rows_after
  FROM time_series.ts_compressed_chunk WHERE table_oid = :relid;
SELECT count(*) AS case7_ts_chunk_rows_after
  FROM time_series.ts_chunk WHERE table_oid = :relid;
SELECT count(*) AS case7_compress_config_rows_after
  FROM time_series.ts_compress_config WHERE table_oid = :relid;

-- ======================================================================
-- Case 8: DISTRIBUTED BY (k) — hash distribution, per-segment relfilenode
-- differs.  Uses a small generator (5 rows across 5 chunks) so each
-- segment carries a different relfilenode.  We capture all three
-- per-segment relfilenodes and assert each one's file count.
-- ======================================================================
DROP TABLE IF EXISTS pax8_t;
CREATE TABLE pax8_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED BY (k);
INSERT INTO pax8_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 14) i;

-- Cluster-wide count of every _ts_<N> chunk-fork file in the current
-- database, summed across segments.  Use BEFORE the DROP to capture
-- the table's footprint, and AFTER to assert "zero leftover".  We
-- can't query relfilenode after the DROP (pg_class row gone), so
-- the AFTER check is "absolutely no chunk-fork file remains" — safe
-- because the test pre-cleans every other ts table.
CREATE OR REPLACE FUNCTION test.ts_all_chunk_files_in_db()
RETURNS TABLE(seg int, cnt int)
LANGUAGE sql EXECUTE ON ALL SEGMENTS AS $$
    SELECT gp_execution_segment(),
           count(*)::int
    FROM pg_ls_dir(
            'base/' || (SELECT oid::text FROM pg_database
                         WHERE datname = current_database()),
            TRUE, FALSE)
    WHERE pg_ls_dir LIKE '%_ts_%';
$$;

-- Capture baseline including this test's table, then DROP and assert
-- the delta — robust against other tests' leftover files in this DB.
CREATE TEMP TABLE _case8_files AS
  SELECT COALESCE(sum(cnt)::int, 0) AS cnt
    FROM test.ts_all_chunk_files_in_db();

DROP TABLE pax8_t;

SELECT (SELECT cnt FROM _case8_files)
       - (SELECT COALESCE(sum(cnt)::int, 0)
            FROM test.ts_all_chunk_files_in_db())
       AS case8_chunk_files_cleaned_by_drop;
DROP TABLE _case8_files;
DROP FUNCTION test.ts_all_chunk_files_in_db();

-- ======================================================================
-- Case 9: Guardrails — operations that would rotate relfilenode WITHOUT
-- going through ts_heap_relation_set_new_filenode are blocked by
-- time_series itself.  This locks down the assumption that those paths
-- can't sneak PAX files into a leaky state.
-- ======================================================================
DROP TABLE IF EXISTS pax9_t;
CREATE TABLE pax9_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pax9_t VALUES ('2025-01-01 00:00:00+00', 1);

\set ON_ERROR_STOP 0
-- VACUUM FULL must be blocked by ts_heap_relation_copy_for_cluster
VACUUM FULL pax9_t;
-- CLUSTER without an index also routes through copy_for_cluster
-- (skipping; CLUSTER needs an index which time_series can't have)
-- ALTER TABLE SET ACCESS METHOD must be blocked by ts_ddl.c
ALTER TABLE pax9_t SET ACCESS METHOD heap;
-- ALTER TABLE SET TABLESPACE must be blocked by ts_heap_relation_copy_data
-- (defer — TABLESPACE setup in regression test is fragile; trust source code)
\set ON_ERROR_STOP 1

DROP TABLE pax9_t;

-- (Cleanup: helper functions live in sql/include/setup.sql and are
-- intentionally NOT dropped here.)
