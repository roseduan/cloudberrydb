-- ============================================================
-- recompress_crash_destroys_live_file.sql (isolation2)
--
-- Regression spec for the P0 data-loss bug where crash recovery
-- DESTROYS an already-committed PAX chunk file.
--
-- ── The bug this guards against (fixed; described as it was) ──
-- Recompress writes the merged PAX to "<seg_path>.new" and only
-- rename(2)s it onto the live path on success.  The PAX WAL
-- sidecar, however, USED TO STRIP the ".new" suffix before
-- recording the filename (ts_compress_pax.cc), so every
-- XLOG_TS_PAX_WRITE said "write these bytes into <seg_path>" --
-- the LIVE name.  And ts_wal_redo_pax_write (ts_wal.c) opens the
-- name it is given with O_TRUNC when offset == 0.
--
-- redo is physical and commit-BLIND: crash recovery replays every
-- FLUSHED record regardless of whether its transaction committed.
-- So a recompress killed mid-write had its half-finished ".new"
-- bytes replayed straight onto the live file:
--
--   O_TRUNC  -> the committed file is truncated to zero
--   writes   -> the partial image is laid down in its place
--
-- The chunk's ts_compressed_chunk row -- committed by the PREVIOUS,
-- successful compress -- still pointed at that path, so the damage
-- was not an orphan: every scan followed the catalog into a
-- truncated file whose postscript trailer was never written.
-- Reading it SIGSEGVs (PAX's OrcFormatReader takes the trailing 8
-- bytes as post_script_len and stack-allocates that much), and
-- because the catalog never stops pointing there, it never
-- self-heals -- observed in production as every segment crashing
-- roughly once a minute for hours after a single kill -9.
--
-- Why only recompress: a crash during the FIRST compress replays
-- the same garbage, but no committed catalog row references it, so
-- it is a harmless orphan and readers still see the heap.  The
-- damage requires a chunk that is already COMPRESSED (committed
-- file + committed catalog row) and then recompressed -- i.e.
-- status PARTIAL, which is what a late INSERT produces.
--
-- ── The fix ─────────────────────────────────────────────────
-- Record the real ".new" filename in the WAL so redo can only ever
-- touch the temporary file, and carry the rename as its own WAL
-- record emitted after the writer closed successfully.  An
-- interrupted recompress then leaves nothing but an orphan ".new"
-- -- exactly the outcome the original design assumed it already
-- had -- and the live file keeps serving the previously committed
-- data.
--
-- ── Deterministic crash via FAULT_INJECTOR ──────────────────
-- Reproducing needs the ".new" WAL flushed AND rename(2) skipped
-- at the moment the process dies.  Two adjacent fault points in
-- that window make it deterministic (see the comment on them in
-- ts_compress.c):
--
--   S1: seed + compress + reclaim            (chunk COMPRESSED)
--   S1: INSERT late row                      (chunk PARTIAL)
--   S1: suspend ts_recompress_after_pax_write
--   S2&: compress_chunks()  -> writes .new, wedges before rename
--   S1: pg_switch_wal() on the segment       (.new WAL now durable)
--   S1: arm ts_recompress_before_rename = panic, skip fts_probe
--   S1: reset the suspend -> S2 wakes, PANICs before rename
--       -> segment crash-restarts and replays the flushed WAL
--
-- ASSERTION (post-recovery): the chunk still reads back exactly what
-- was committed -- 101 rows, sum 6049, the same figures Phase 2
-- measured before the crash.
--
-- Measured failure before the fix: 102 rows / sum 7048, stable across
-- re-reads.  The mechanism is worth spelling out, because it is the
-- proof that redo touched a file it had no business touching: the
-- fault fires after the writer closed, so the WAL for ".new" is
-- COMPLETE, and redo lays that complete merged image (100 original +
-- the late row) onto the live path.  The recompress itself aborted, so
-- the chunk stays PARTIAL with the late row still in its heap fork --
-- and the reader, following the catalog, counts that row once from PAX
-- (put there by redo) and once from the heap.  Hence +1 row / +999.
--
-- Production can be worse: when the crash lands mid-write only part of
-- the WAL is flushed, so redo leaves the live file TRUNCATED with no
-- postscript trailer, and scanning it SIGSEGVs the QE in a loop that
-- never self-heals.  Same root cause, so pinning the deterministic
-- double-count variant here is enough to guard the fix.
-- ============================================================

1: SET optimizer = off;
1: SET max_parallel_workers_per_gather = 0;
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
1: SET search_path TO public, time_series;

-- A single device value pins every row onto ONE segment, so the
-- crash is confined to a single known QE.  Which segment that is
-- depends on the hash, so it is captured into crash_target below
-- rather than assumed -- and captured BEFORE the recompress is
-- wedged, because once the recompress holds the chunk's Exclusive
-- lock any scan of crash_tbl would block behind it and deadlock
-- the test.
1: CREATE TABLE crash_tbl (ts TIMESTAMPTZ NOT NULL, device INT NOT NULL, val FLOAT8)
   USING time_series WITH (
     ts_partition_column = 'ts',
     ts_chunk_interval   = '1 hour',
     ts_chunk_origin     = '2020-01-01'
   )
   DISTRIBUTED BY (device);
1: SELECT set_compress_config('crash_tbl', 'device', 'ts');

-- ============================================================
-- Phase 1: get the chunk to COMPRESSED with a committed PAX file
-- and a committed ts_compressed_chunk row.  base_sum = 5050.
-- ============================================================
1: INSERT INTO crash_tbl
   SELECT '2024-01-01 00:00:00'::timestamptz + (i * interval '1 sec'), 1, i * 1.0
   FROM generate_series(1, 100) i;
1: SELECT time_series.compress_chunks('crash_tbl');
1: SELECT time_series.reclaim_chunk_heaps('crash_tbl');
1: SELECT chunk_number, status FROM time_series.ts_chunk
   WHERE table_oid = 'crash_tbl'::regclass GROUP BY chunk_number, status;

-- ============================================================
-- Phase 2: late arrival flips COMPRESSED -> PARTIAL, so the next
-- compress_chunks() is a RECOMPRESS (merge old PAX + heap fork)
-- and therefore overwrites an already-committed file.
-- Ground truth from here on: 101 rows, sum = 5050 + 999 = 6049.
-- ============================================================
1: INSERT INTO crash_tbl VALUES ('2024-01-01 00:00:50.5'::timestamptz, 1, 999.0);
1: SELECT chunk_number, status FROM time_series.ts_chunk
   WHERE table_oid = 'crash_tbl'::regclass GROUP BY chunk_number, status;
1: SELECT count(*) AS before_crash_cnt, sum(val)::float8 AS before_crash_sum
   FROM crash_tbl;

-- Remember which segment owns the chunk.  Materialised into a
-- replicated heap table so the fault-targeting queries below never
-- have to touch crash_tbl while the recompress holds its locks.
1: CREATE TABLE crash_target AS
   SELECT gp_segment_id AS seg FROM crash_tbl LIMIT 1
   DISTRIBUTED REPLICATED;

-- ============================================================
-- Phase 3: wedge the recompress in the fatal window.
-- ============================================================
1: SELECT gp_inject_fault('ts_recompress_after_pax_write', 'suspend', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = (SELECT seg FROM crash_target);

2: SET optimizer = off;
2: SET search_path TO public, time_series;
2&: SELECT time_series.compress_chunks('crash_tbl');

1: SELECT gp_wait_until_triggered_fault('ts_recompress_after_pax_write', 1, dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = (SELECT seg FROM crash_target);

-- Force the XLOG_TS_PAX_WRITE records for ".new" out to disk.  Redo
-- replays only flushed WAL, so without this the walwriter timing
-- decides whether the bug reproduces.  Switching on every segment is
-- harmless and avoids scanning crash_tbl (whose chunk is locked by
-- the wedged recompress) just to single out the target.
1: SELECT gp_segment_id, pg_switch_wal() IS NOT NULL FROM gp_dist_random('gp_id')
   ORDER BY gp_segment_id;

-- ============================================================
-- Phase 4: kill the recompress before rename(2).  Skip FTS
-- probes first so the crash does not promote the mirror.
-- ============================================================
1: SELECT gp_inject_fault_infinite('fts_probe', 'skip', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1: SELECT gp_inject_fault('ts_recompress_before_rename', 'panic', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = (SELECT seg FROM crash_target);
1: SELECT gp_inject_fault('ts_recompress_after_pax_write', 'reset', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = (SELECT seg FROM crash_target);

-- S2 wakes inside the fault point and PANICs: rename(2) never runs,
-- the transaction never commits, and the segment restarts into
-- crash recovery -- which replays the flushed ".new" WAL.
2<:
2q:

-- The coordinator is alive, so its fts_probe skip can be lifted here.
-- The segment fault is reset in Phase 5 instead: right now that
-- segment is still crash-restarting and injecting into it would fail
-- with a connection error whose text carries the dbid and host.
1: SELECT gp_inject_fault('fts_probe', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1q:

-- ============================================================
-- Phase 5: ASSERTION.  The interrupted recompress must have left
-- the previously committed data intact and readable.
--
-- With the fix: 101 rows / 6049 -- redo only ever touched ".new", so
-- the live file is still the one the first compress committed.
-- Without the fix: 102 rows / 7048 -- redo replayed the aborted merge
-- onto the live file, so the late row is counted from both PAX and the
-- heap fork (see the header for the full mechanism).
-- ============================================================
3: SET optimizer = off;
3: SET search_path TO public, time_series;
3: SELECT count(*) AS after_recovery_cnt, sum(val)::float8 AS after_recovery_sum
   FROM crash_tbl;

-- A second read proves the state is stable rather than a one-shot
-- error: the pre-fix failure mode repeats forever because the
-- catalog keeps pointing at the destroyed file.
3: SELECT count(*) AS reread_cnt FROM crash_tbl;

-- Chunk metadata must still describe the committed file.  The
-- recompress aborted, so the chunk is still PARTIAL (status 2).
3: SELECT chunk_number, status FROM time_series.ts_chunk
   WHERE table_oid = 'crash_tbl'::regclass GROUP BY chunk_number, status;

-- Now that the segment is back, disarm the panic fault (a one-shot
-- injection is already consumed, but leave nothing armed behind).
3: SELECT gp_inject_fault('ts_recompress_before_rename', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content >= 0;

-- Cleanup
3: DROP TABLE crash_tbl;
3: DROP TABLE crash_target;
3: DROP EXTENSION time_series CASCADE;
3q:
