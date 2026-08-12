-- ts_pax_mirror_repl.sql — PAX sidecar files replicate to mirrors.
--
-- Before the ts_rmgr XLOG_TS_PAX_{CREATE_DIR,WRITE,REMOVE_DIR} ops
-- were added, time_series's compress_chunks path wrote PAX files
-- directly via pax::LocalFileSystem without emitting any WAL — so
-- mirror segments never received the bytes and would silently lose
-- all compressed data on failover.
--
-- The fix wraps every pax::File that lands in MicroPartition-
-- FileFactory::CreateMicroPartitionWriter in a TsWalRecordingFile.
-- Every PWriteN emits an XLOG_TS_PAX_WRITE record; the mirror's
-- ts_wal_redo_pax_write reopens the same relative path with O_CREAT
-- (+ O_TRUNC iff offset==0) and FileWrite's the bytes.
--
-- Assertions here — via a shell helper (tools/verify_pax_mirror.sh)
-- because mirrors are not queryable, so we compare data-directory
-- trees on the same host:
--
--   Case 1  compress a chunk    → primary and mirror byte-identical
--   Case 2  TRUNCATE            → old relfilenode's PAX dir gone on
--                                 both sides (XLOG_TS_PAX_REMOVE_DIR
--                                 fires at commit via
--                                 RegisterPendingDelete)
--   Case 3  DROP                → dir gone on both sides
--   Case 4  DROP + ROLLBACK     → dir intact on both sides (the
--                                 remove-dir WAL is registered under
--                                 atCommit=true; ROLLBACK doesn't
--                                 fire it)
--   Case 5  INSERT into a COMPRESSED chunk (COMPRESSED→PARTIAL flip)
--                              → heap chunk fork on primary and mirror
--                                 both shrink; site ts_tableam.c:718
--                                 must emit ts_wal_fork_truncate so the
--                                 mirror sees the shrink (without it
--                                 mirror keeps pre-truncate blocks and
--                                 a failover double-counts them).
--   Case 6  reclaim_chunk_heaps → heap chunk fork on primary and mirror
--                                 both shrink; site ts_compress.c:2361
--                                 must emit the same WAL bracket.

\i sql/include/setup.sql

-- ============================================================
-- Wait helper: block until every mirror flush_lsn has caught up
-- to the coordinator's current WAL location.  Ten-second cap.
-- ============================================================
-- start_ignore
CREATE OR REPLACE FUNCTION test.wait_for_mirror_sync()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN 1..100 LOOP
        IF (SELECT bool_and(flush_lsn = pg_current_wal_lsn())
              FROM gp_dist_random('pg_stat_replication'))
        THEN
            RETURN;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END $$;
-- end_ignore

-- ============================================================
-- Case 1: compress + verify byte-identical replication
-- ============================================================
DROP TABLE IF EXISTS pxm_t;
CREATE TABLE pxm_t (ts timestamptz, k int, v text) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pxm_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval,
       i, repeat('z', 200)
FROM generate_series(0, 4) i;
SELECT time_series.set_compress_config('pxm_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pxm_t'::regclass) > 0 AS case1_compressed;

CHECKPOINT;
SELECT test.wait_for_mirror_sync();
SELECT 'pxm_t'::regclass::oid AS rel_id \gset
\setenv PXM_RELID :rel_id
\! bash tools/verify_pax_mirror.sh 7000 contrib_regression $PXM_RELID

-- ============================================================
-- Case 2: TRUNCATE — old relid dir cleaned on both sides
-- ============================================================
TRUNCATE pxm_t;
CHECKPOINT;
SELECT test.wait_for_mirror_sync();
\setenv PXM_RELID :rel_id
\! bash tools/verify_pax_mirror.sh 7000 contrib_regression $PXM_RELID

-- ============================================================
-- Case 3: DROP — dir gone on both sides
-- ============================================================
INSERT INTO pxm_t
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval,
       i + 100, repeat('w', 200)
FROM generate_series(0, 2) i;
SELECT time_series.compress_chunks('pxm_t'::regclass) > 0 AS case3_recompressed;
SELECT 'pxm_t'::regclass::oid AS rel_id \gset
\setenv PXM_RELID :rel_id

DROP TABLE pxm_t;
CHECKPOINT;
SELECT test.wait_for_mirror_sync();
\! bash tools/verify_pax_mirror.sh 7000 contrib_regression $PXM_RELID

-- ============================================================
-- Case 4: DROP + ROLLBACK — dir preserved on both sides
-- ============================================================
DROP TABLE IF EXISTS pxm_rb;
CREATE TABLE pxm_rb (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pxm_rb
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 2) i;
SELECT time_series.set_compress_config('pxm_rb'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pxm_rb'::regclass) > 0 AS case4_compressed;
SELECT 'pxm_rb'::regclass::oid AS rel_id \gset
\setenv PXM_RELID :rel_id

BEGIN;
DROP TABLE pxm_rb;
ROLLBACK;

CHECKPOINT;
SELECT test.wait_for_mirror_sync();
\! bash tools/verify_pax_mirror.sh 7000 contrib_regression $PXM_RELID

DROP TABLE pxm_rb;

-- ============================================================
-- Case 5: INSERT into a COMPRESSED chunk — heap fork on both
--         primary and mirror must be truncated.
--
-- The COMPRESSED→PARTIAL flip in ts_chunk_prepare_for_insert (see
-- src/access/ts_tableam.c:718) calls smgrtruncate on the chunk
-- fork.  Chunk forks live above MAX_FORKNUM and are invisible to
-- standard PG WAL; without an accompanying ts_wal_fork_truncate
-- the mirror keeps the pre-truncate blocks, and after failover
-- the same rows appear in both PAX and heap → double-count.
-- ============================================================
DROP TABLE IF EXISTS pxm_h;
CREATE TABLE pxm_h (ts timestamptz, k int, v text) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pxm_h
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval,
       i, repeat('a', 200)
FROM generate_series(0, 4) i;
SELECT time_series.set_compress_config('pxm_h'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pxm_h'::regclass) > 0 AS case5_compressed;

CHECKPOINT;
SELECT test.wait_for_mirror_sync();
SELECT 'pxm_h'::regclass::oid AS rel_id \gset
\setenv PXM_RELID :rel_id

-- Heap fork still populated (PAX authoritative, heap not yet reclaimed)
\! bash tools/verify_chunk_fork_mirror.sh 7000 contrib_regression $PXM_RELID

-- Now touch one of the COMPRESSED chunks — the flip truncates its
-- heap fork.  This is the site the fix targets.
INSERT INTO pxm_h VALUES
  ('2025-01-01 00:02:30+00'::timestamptz, 999, 'flip');

CHECKPOINT;
SELECT test.wait_for_mirror_sync();
-- Heap fork should now be shrunk on BOTH primary and mirror.
\! bash tools/verify_chunk_fork_mirror.sh 7000 contrib_regression $PXM_RELID

DROP TABLE pxm_h;

-- ============================================================
-- Case 6: reclaim_chunk_heaps — heap chunk fork truncated on
--         both sides.
--
-- Sibling site: src/compress/ts_compress.c:2361.  Same WAL
-- bracket requirement as case 5.
-- ============================================================
DROP TABLE IF EXISTS pxm_r;
CREATE TABLE pxm_r (ts timestamptz, k int, v text) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 minute',
      ts_chunk_origin     = '2025-01-01 00:00:00+00')
DISTRIBUTED REPLICATED;
INSERT INTO pxm_r
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval,
       i, repeat('r', 200)
FROM generate_series(0, 4) i;
SELECT time_series.set_compress_config('pxm_r'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('pxm_r'::regclass) > 0 AS case6_compressed;
SELECT time_series.reclaim_chunk_heaps('pxm_r'::regclass) > 0 AS case6_reclaimed;

CHECKPOINT;
SELECT test.wait_for_mirror_sync();
SELECT 'pxm_r'::regclass::oid AS rel_id \gset
\setenv PXM_RELID :rel_id
-- Reclaim drove the heap fork to 0 blocks on primary; mirror must
-- match (both sides shrunk, or both sides absent).
\! bash tools/verify_chunk_fork_mirror.sh 7000 contrib_regression $PXM_RELID

DROP TABLE pxm_r;

-- ============================================================
-- Cleanup
-- ============================================================
-- start_ignore
DROP FUNCTION test.wait_for_mirror_sync();
-- end_ignore
