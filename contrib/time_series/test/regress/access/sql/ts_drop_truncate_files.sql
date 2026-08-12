-- ts_drop_truncate_files.sql — DROP / TRUNCATE chunk-fork file cleanup
--
-- Verifies that the file_unlink_hook chaining installed by ts_smgr.c
-- actually removes chunk-fork files ("<relfilenode>_ts_<N>") on:
--   1. DROP TABLE (non-tx)
--   2. TRUNCATE TABLE (non-tx; rotates relfilenode)
--   3. DROP TABLE inside an explicit BEGIN..COMMIT
--   4. DROP TABLE inside BEGIN..ROLLBACK — files MUST survive
--   5. TRUNCATE inside BEGIN..COMMIT
--   6. TRUNCATE inside BEGIN..ROLLBACK — new relfilenode chunk forks
--      MUST be cleaned, old relfilenode chunk forks MUST survive
--
-- Tables are DISTRIBUTED REPLICATED so each segment owns the full
-- chunk set, but relfilenode itself is assigned per-segment (no QD
-- coordination) and can differ across segments even for the very same
-- REPLICATED relation.  Every "rfn" captured below is therefore an
-- oid[] indexed by segment id (array_agg(... ORDER BY gp_segment_id)),
-- not a single value sampled from one arbitrary segment -- see
-- test.ts_chunk_files_per_seg in include/setup.sql.

\i sql/include/setup.sql

-- Helpers test.ts_chunk_files_per_seg / test.ts_chunk_files_total live
-- in sql/include/setup.sql; both this file and ts_drop_pax_files.sql
-- consume them.

-- Shared table shape for every test.  Inline the CREATE per-test so each
-- block stands alone and a single failure doesn't cascade.
-- 5 rows × 1-minute chunks = 5 chunk forks per segment = 15 files cluster-wide.

-- ======================================================================
-- Case 1: DROP TABLE (non-tx)
-- ======================================================================
DROP TABLE IF EXISTS dt1_t;
CREATE TABLE dt1_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt1_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn
  FROM gp_dist_random('pg_class') WHERE relname = 'dt1_t' \gset

SELECT test.ts_chunk_files_total(:'rfn') AS case1_files_before;
DROP TABLE dt1_t;
SELECT test.ts_chunk_files_total(:'rfn') AS case1_files_after;

-- ======================================================================
-- Case 2: TRUNCATE TABLE (non-tx; rotates relfilenode)
-- ======================================================================
DROP TABLE IF EXISTS dt2_t;
CREATE TABLE dt2_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt2_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn_old
  FROM gp_dist_random('pg_class') WHERE relname = 'dt2_t' \gset

SELECT test.ts_chunk_files_total(:'rfn_old') AS case2_files_before_truncate;
TRUNCATE dt2_t;
SELECT test.ts_chunk_files_total(:'rfn_old') AS case2_old_relfilenode_after_truncate;

-- New filenode is fully usable (re-insert exercises the post-TRUNCATE path)
INSERT INTO dt2_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT count(*) AS case2_rows_after_reinsert FROM dt2_t;
SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn_new
  FROM gp_dist_random('pg_class') WHERE relname = 'dt2_t' \gset
SELECT test.ts_chunk_files_total(:'rfn_new') AS case2_new_relfilenode_after_reinsert;
SELECT (:'rfn_old'::oid[] <> :'rfn_new'::oid[]) AS case2_relfilenode_was_rotated;
DROP TABLE dt2_t;
SELECT test.ts_chunk_files_total(:'rfn_new') AS case2_new_relfilenode_after_drop;

-- ======================================================================
-- Case 3: DROP inside BEGIN..COMMIT
-- ======================================================================
DROP TABLE IF EXISTS dt3_t;
CREATE TABLE dt3_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt3_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn
  FROM gp_dist_random('pg_class') WHERE relname = 'dt3_t' \gset

SELECT test.ts_chunk_files_total(:'rfn') AS case3_files_before;
BEGIN;
DROP TABLE dt3_t;
COMMIT;
SELECT test.ts_chunk_files_total(:'rfn') AS case3_files_after_commit;

-- ======================================================================
-- Case 4: DROP inside BEGIN..ROLLBACK — files MUST survive,
--         table MUST be queryable afterward.
-- ======================================================================
DROP TABLE IF EXISTS dt4_t;
CREATE TABLE dt4_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt4_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn
  FROM gp_dist_random('pg_class') WHERE relname = 'dt4_t' \gset

SELECT test.ts_chunk_files_total(:'rfn') AS case4_files_before;
BEGIN;
DROP TABLE dt4_t;
ROLLBACK;
SELECT test.ts_chunk_files_total(:'rfn') AS case4_files_after_rollback;
SELECT count(*) AS case4_rows_after_rollback FROM dt4_t;
DROP TABLE dt4_t;
SELECT test.ts_chunk_files_total(:'rfn') AS case4_files_after_final_drop;

-- ======================================================================
-- Case 5: TRUNCATE inside BEGIN..COMMIT
-- ======================================================================
DROP TABLE IF EXISTS dt5_t;
CREATE TABLE dt5_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt5_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn_old
  FROM gp_dist_random('pg_class') WHERE relname = 'dt5_t' \gset

SELECT test.ts_chunk_files_total(:'rfn_old') AS case5_files_before;
BEGIN;
TRUNCATE dt5_t;
COMMIT;
SELECT test.ts_chunk_files_total(:'rfn_old') AS case5_old_relfilenode_after_commit;
DROP TABLE dt5_t;

-- ======================================================================
-- Case 6: TRUNCATE inside BEGIN..ROLLBACK
--   * Old relfilenode files MUST survive (the rotation is rolled back)
--   * Any rows the in-flight xact inserted go to the new relfilenode,
--     whose files MUST be unlinked when the xact aborts.
-- ======================================================================
DROP TABLE IF EXISTS dt6_t;
CREATE TABLE dt6_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO dt6_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 4) i;

SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn_old
  FROM gp_dist_random('pg_class') WHERE relname = 'dt6_t' \gset

SELECT test.ts_chunk_files_total(:'rfn_old') AS case6_files_before;

BEGIN;
TRUNCATE dt6_t;
-- Capture the new relfilenode (per segment) while we're inside the xact,
-- before ROLLBACK destroys the pg_class row that points at it.
SELECT array_agg(relfilenode ORDER BY gp_segment_id) AS rfn_new
  FROM gp_dist_random('pg_class') WHERE relname = 'dt6_t' \gset
-- Insert into the new relfilenode so it actually has chunk forks to
-- unlink on abort — otherwise the cleanup path is moot.
INSERT INTO dt6_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i + 1000
FROM generate_series(10, 13) i;
SELECT test.ts_chunk_files_total(:'rfn_new') AS case6_new_relfilenode_inside_xact;
ROLLBACK;

SELECT test.ts_chunk_files_total(:'rfn_old') AS case6_old_relfilenode_after_rollback;
SELECT test.ts_chunk_files_total(:'rfn_new') AS case6_new_relfilenode_after_rollback;
SELECT (:'rfn_old'::oid[] <> :'rfn_new'::oid[]) AS case6_relfilenodes_differ;
SELECT count(*) AS case6_rows_after_rollback FROM dt6_t;
DROP TABLE dt6_t;
SELECT test.ts_chunk_files_total(:'rfn_old') AS case6_old_relfilenode_after_final_drop;

-- (Cleanup: helper functions live in sql/include/setup.sql and are
-- intentionally NOT dropped here — ts_drop_pax_files.sql also uses
-- the chunk-file helper, and dropping it here would break a
-- regression suite run that orders the two tests in either direction.)
