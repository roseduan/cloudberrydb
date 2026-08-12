-- Shared setup for time_series regression tests.
-- Include this at the top of every test file via:
--   \ir include/setup.sql

CREATE EXTENSION IF NOT EXISTS time_series;
SET optimizer = off;
SET timezone = 'UTC';
SET datestyle = 'ISO';
SET extra_float_digits = 0;

CREATE SCHEMA IF NOT EXISTS test;
-- start_ignore
-- ============================================================
-- Shared file-counting helpers (used by ts_drop_truncate_files.sql
-- and ts_drop_pax_files.sql).  Wrapped in start_ignore..end_ignore
-- so every OTHER test that \i's this setup file is unaffected by
-- the CREATE FUNCTION output added here.  gpdiff.pl recognises
-- these markers and skips the enclosed lines during diff.
-- ============================================================
-- relfilenode is assigned per-segment (GetNewRelFileNode() has no
-- cross-segment coordination, unlike oid which the QD preassigns and
-- dispatches), so even a DISTRIBUTED REPLICATED table's relfilenode
-- can legitimately differ from one segment to the next. rfiles is an
-- oid[] indexed by segment id (element 1 = segment 0, etc., built via
-- array_agg(relfilenode ORDER BY gp_segment_id)); each segment checks
-- its OWN relfilenode instead of one value sampled from a single
-- segment and assumed to hold everywhere.
--
-- Drop the old oid-parameter overloads first: CREATE OR REPLACE only
-- replaces a function whose argument types match exactly, and a
-- persistent regression database that ran an earlier version of this
-- test would otherwise keep both the old oid and new oid[] overloads
-- around, making every call site ambiguous.
DROP FUNCTION IF EXISTS test.ts_chunk_files_per_seg(oid);
DROP FUNCTION IF EXISTS test.ts_chunk_files_total(oid);

CREATE OR REPLACE FUNCTION test.ts_chunk_files_per_seg(rfiles oid[])
RETURNS TABLE(seg int, cnt int)
LANGUAGE sql EXECUTE ON ALL SEGMENTS AS $$
    SELECT gp_execution_segment(),
           count(*)::int
    FROM pg_ls_dir('base/' || (SELECT oid::text FROM pg_database
                                WHERE datname = current_database()))
    WHERE pg_ls_dir LIKE rfiles[gp_execution_segment() + 1]::text || '_ts_%';
$$;

CREATE OR REPLACE FUNCTION test.ts_chunk_files_total(rfiles oid[])
RETURNS bigint
LANGUAGE sql AS $$
    SELECT COALESCE(sum(cnt)::bigint, 0)
      FROM test.ts_chunk_files_per_seg(rfiles);
$$;

CREATE OR REPLACE FUNCTION test.ts_pax_files_per_seg(relid oid)
RETURNS TABLE(seg int, cnt int)
LANGUAGE sql EXECUTE ON ALL SEGMENTS AS $$
    SELECT gp_execution_segment(),
           (SELECT count(*)::int
              FROM pg_ls_dir(
                'base/' || (SELECT oid::text FROM pg_database
                            WHERE datname = current_database())
                || '/ts_compressed/' || relid::text,
                TRUE, FALSE));
$$;

CREATE OR REPLACE FUNCTION test.ts_pax_files_total(relid oid)
RETURNS bigint
LANGUAGE sql AS $$
    SELECT COALESCE(sum(cnt)::bigint, 0)
      FROM test.ts_pax_files_per_seg(relid);
$$;
-- end_ignore
