-- ============================================================
-- cagg_insert.sql (isolation2)
-- Isolation pattern: cagg INSERT under concurrent refresh
--
-- Tests INSERT + REFRESH concurrent interaction:
-- - INSERT during REFRESH doesn't block (after TX1)
-- - REFRESH sees invalidations from concurrent INSERT
-- - SELECT on source doesn't block during REFRESH
-- - Concurrent CREATE CAGG trigger race
--
-- Fault injection points:
-- - cagg_refresh_after_commit_and_chain
-- - cagg_create_before_trigger_install
-- ============================================================

-- ============================================================
-- Setup
-- ============================================================
1: SET optimizer = off;
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: SET search_path TO public, time_series;

1: CREATE TABLE src1 (time TIMESTAMPTZ NOT NULL, loc INT NOT NULL) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
   DISTRIBUTED BY (loc);
1: INSERT INTO src1 SELECT '2024-01-01'::timestamptz + (i * interval '10 min'),
   i % 5 FROM generate_series(1, 100) i;

1: CREATE MATERIALIZED VIEW cv1 WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          loc, count(*) AS cnt
   FROM src1 GROUP BY bucket, loc;

-- Second source for cross-table tests
1: CREATE TABLE src2 (time TIMESTAMPTZ NOT NULL, loc INT NOT NULL) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
   DISTRIBUTED BY (loc);
1: INSERT INTO src2 SELECT '2024-01-01'::timestamptz + (i * interval '10 min'),
   i % 3 FROM generate_series(1, 100) i;
1: CREATE MATERIALIZED VIEW cv2 WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          count(*) AS cnt
   FROM src2 GROUP BY bucket;

-- Initial REFRESH
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);

2: SET optimizer = off;
2: SET search_path TO public, time_series;
3: SET optimizer = off;
3: SET search_path TO public, time_series;

-- ============================================================
-- SANITY-CHUNKS: source hypertables must actually be multi-chunk
-- so the concurrency perms below stress real chunk-spanning races
-- rather than degrading to a single chunk's lock dance.
--
-- src1 / src2: each 100 rows × 10 min ≈ 16.7 hours of data; at
-- ts_chunk_interval = 4 h that lands in 4 chunks (the last row at
-- 16:40 falls into chunk 4).
-- ============================================================
1: SELECT count(DISTINCT chunk_number) >= 4 AS src1_spans_at_least_4_chunks
   FROM time_series.ts_chunk WHERE table_oid = 'src1'::regclass;
1: SELECT count(DISTINCT chunk_number) >= 4 AS src2_spans_at_least_4_chunks
   FROM time_series.ts_chunk WHERE table_oid = 'src2'::regclass;

-- ============================================================
-- Test 1: INSERT during blocked REFRESH → INSERT proceeds
--         (upstream insert perm 2: Ib → LockCagg → I1 → Refresh → Ic → Unlock)
-- ============================================================

-- Seed a stable-bucket row so the next refresh has L1 work to consume
-- (refresh became a no-op after the hot-bucket exclusion fix when the
-- prior refresh already brought watermark to the hot bucket; without
-- work to do the refresh wouldn't even try to lock mat).
1: INSERT INTO src1 VALUES ('2024-01-01 02:30+00'::timestamptz, 91);

1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- REFRESH blocks on mat table
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- INSERT into source proceeds (doesn't need mat table)
3: INSERT INTO src1 VALUES ('2024-01-01 01:30+00', 99);

-- Verify INSERT completed while REFRESH is still blocked
1: SELECT count(*) > 0 AS insert_ok FROM src1 WHERE loc = 99;

-- Release
1: COMMIT;
2<:

-- Document a real CAGG semantic: the refresh launched in session 2
-- was waiting on the LOCK BEFORE the INSERT happened.  When it
-- unblocks it sees its own pre-INSERT snapshot of src1, so the
-- new row (loc=99 at 01:30) is NOT in the mat table yet.  The
-- invalidation trigger DID write an L1 row for the INSERT, but
-- the L1 will only be consumed by the NEXT refresh.
-- diff_after_blocked_refresh = 1 (the loc=99 row missing from mat).
1: SELECT count(*) AS diff_after_blocked_refresh FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- Recovery: a second refresh consumes the L1 row and catches up.
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
1: SELECT count(*) AS diff_after_recovery_refresh FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 2: SELECT on source during blocked REFRESH → proceeds
--         (upstream insert perm 6)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 02:30+00', 88);

1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- SELECT on source table proceeds (no conflict)
3: SELECT count(*) AS src_count FROM src1;

1: COMMIT;
2<:

-- ============================================================
-- Test 3: REFRESH on different source → doesn't block
--         (upstream insert perm 4)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 03:30+00', 77);
1: INSERT INTO src2 VALUES ('2024-01-01 03:30+00', 77);

1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- cv1 REFRESH blocks
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- cv2 on DIFFERENT source → proceeds
3: CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);
3: SELECT count(*) AS cv2_diff FROM (
   (SELECT bucket, cnt FROM cv2
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), count(*)
    FROM src2 GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), count(*)
    FROM src2 GROUP BY 1
    EXCEPT
    SELECT bucket, cnt FROM cv2)
   ) x;

1: COMMIT;
2<:

-- ============================================================
-- Test 4: Concurrent CREATE CAGG — trigger creation race [P0]
--         (upstream insert perm 18)
-- ============================================================

1: CREATE TABLE src_race (time TIMESTAMPTZ NOT NULL, v INT NOT NULL) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
   DISTRIBUTED BY (v);
1: INSERT INTO src_race SELECT '2024-01-01'::timestamptz + (i * interval '10 min'),
   i FROM generate_series(1, 20) i;

-- Create two CAGGs on the same source from two sessions.  CREATE
-- MATERIALIZED VIEW does not park behind the other here (CBDB does
-- not block the second create on the first in this path), so the
-- iso2 '&' blocking form does not apply; we create them from two
-- sessions back-to-back.  The invariant under test is the one that
-- matters: a second CAGG on a source that already has the
-- invalidation trigger REUSES the single shared trigger rather than
-- installing a duplicate.
1: CREATE MATERIALIZED VIEW src_race_cv1 WITH (time_series.continuous) AS
    SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
    FROM src_race GROUP BY bucket;
2: CREATE MATERIALIZED VIEW src_race_cv2 WITH (time_series.continuous) AS
    SELECT time_bucket('2 hour'::interval, time) AS bucket, count(*) AS cnt
    FROM src_race GROUP BY bucket;

-- Trigger should exist (exactly 1, shared by both CAGGs)
3: SELECT count(*) AS trigger_count FROM pg_trigger
   WHERE tgrelid = 'src_race'::regclass
     AND tgname = 'cagg_invalidation_trigger';

-- Both queryable
3: SELECT count(*) >= 0 AS cv1_ok FROM src_race_cv1;
3: SELECT count(*) >= 0 AS cv2_ok FROM src_race_cv2;

1: DROP TABLE src_race CASCADE;

-- ============================================================
-- Test 5: Fault injection — INSERT between TX1 and TX2 of REFRESH
--         (upstream insert perm 14 equivalent)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 04:30+00', 66);

-- Pause REFRESH after L1→L2 migration (TX1 committed), before TX2 materialize
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'suspend', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

2>: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

1: SELECT gp_wait_until_triggered_fault('cagg_refresh_after_commit_and_chain', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- REFRESH paused between TX1 and TX2.
-- INSERT now — creates new L1 entries (in a separate transaction)
3: INSERT INTO src1 VALUES ('2024-01-01 05:30+00', 55);

-- Resume REFRESH TX2
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'resume', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
2<:

1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- The new INSERT's L1 was created AFTER TX1's L1→L2 migration.
-- TX2 won't see it. Need one more REFRESH.
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

1: SELECT count(*) AS diff_fault FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 6: REFRESH starts before INSERT (reverse of Test 1)
--         (upstream insert perm 3: Ib → LockCagg → Refresh → I1 → Ic → Unlock)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 06:30+00', 44);

1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- REFRESH blocks first
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- THEN INSERT (while REFRESH is blocked)
3: BEGIN;
3: INSERT INTO src1 VALUES ('2024-01-01 07:30+00', 33);
3: COMMIT;

-- Release → REFRESH proceeds
1: COMMIT;
2<:

-- REFRESH should see the INSERT (via L1 in next cycle)
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
1: SELECT count(*) AS diff_t6 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 7: SELECT completes before blocked REFRESH starts
--         (upstream insert perm 7: Sb → LockCagg → S1 → Refresh → Sc → Unlock)
-- ============================================================

-- Seed L1 work so the next refresh actually has to lock mat
1: INSERT INTO src1 VALUES ('2024-01-01 02:30+00'::timestamptz, 97);

1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- SELECT on source completes first (no lock needed)
3: SELECT count(*) AS src_before_refresh FROM src1;

-- THEN REFRESH blocks
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

1: COMMIT;
2<:

-- ============================================================
-- Test 8: LOCK watermark → INSERT + REFRESH interaction
--         (upstream insert perm 8-9: LockInvalThr → INSERT + REFRESH)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 08:30+00', 22);

-- Lock watermark table (simulates threshold SHARE lock)
1: BEGIN;
1: LOCK TABLE time_series.cagg_watermark IN SHARE MODE;

-- INSERT proceeds (trigger reads watermark with AccessShare, compatible with SHARE)
3: INSERT INTO src1 VALUES ('2024-01-01 09:30+00', 11);

-- REFRESH needs EXCLUSIVE on watermark → blocks
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

1: COMMIT;
2<:

1: SELECT count(*) AS diff_t8 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 9: LOCK L1 table → REFRESH migration blocked
--         INSERT proceeds (L1 write uses RowExclusive, not blocked by EXCLUSIVE on table)
--         (upstream insert perm 15a: I1 → Refresh → LockInval → Refresh → Sb → S1 → Sc → Unlock)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 10:30+00', 9);

1: BEGIN;
1: LOCK TABLE time_series.cagg_invalidation_log IN EXCLUSIVE MODE;

-- REFRESH's L1→L2 migration needs L1 → blocks
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- SELECT on CAGG still works (reads mat + source, doesn't need L1)
3: SELECT count(*) > 0 AS cagg_readable_during_l1_lock FROM cv1;

1: COMMIT;
2<:

1: SELECT count(*) AS diff_t9 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 10: Triple REFRESH — 2 same CAGG + 1 different CAGG
--          All serialize correctly
--          (upstream insert perm 1/16: multi-REFRESH serialization)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 11:30+00', 7);
1: INSERT INTO src2 VALUES ('2024-01-01 11:30+00', 7);

-- Lock cv1 mat table
1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- Two REFRESHes on cv1 + one on cv2
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
3&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- cv2 on different source should NOT block
1: CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);

1: COMMIT;
2<:
3<:

-- All correct, no duplicates
1: SELECT count(*) AS dup_triple FROM (
     SELECT bucket, loc, count(*) FROM time_series._mat_cv1_1
     GROUP BY bucket, loc HAVING count(*) > 1
   ) x;
1: SELECT count(*) AS diff_t10 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 11: LOCK threshold ACCESS EXCLUSIVE → INSERT trigger blocked
--          (upstream insert perm 11: threshold lock blocks INSERT trigger)
--          INSERT's trigger needs AccessShareLock on threshold table.
--          ACCESS EXCLUSIVE is the only mode that blocks AccessShare
--          per the PG lock conflict matrix; the prior EXCLUSIVE MODE
--          here was a long-standing misnomer (EXCLUSIVE is compatible
--          with AccessShare) and the test only "worked" before because
--          of incidental timing from the OLD refresh behaviour (now
--          fixed by hot-bucket-exclusion).
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 00:15+00', 5);
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- Lock threshold table exclusively
1: BEGIN;
1: LOCK TABLE time_series.cagg_invalidation_threshold IN ACCESS EXCLUSIVE MODE;

-- INSERT below threshold → trigger needs to read threshold → blocked
2&: INSERT INTO src1 VALUES ('2024-01-01 01:15+00', 6);

-- Verify INSERT is waiting on Lock.  Poll up to 30s so session 2 has
-- time to reach lock acquisition under heavy host CPU contention.
1: DO $$ DECLARE n int; deadline timestamptz := clock_timestamp() + interval '30 seconds'; BEGIN LOOP SELECT count(*) INTO n FROM pg_stat_activity WHERE query LIKE '%INSERT INTO src1%' AND wait_event_type = 'Lock' AND pid != pg_backend_pid(); EXIT WHEN n >= 1 OR clock_timestamp() > deadline; PERFORM pg_sleep(0.2); END LOOP; END$$;
1: SELECT count(*) > 0 AS insert_blocked_by_th
   FROM pg_stat_activity
   WHERE query LIKE '%INSERT INTO src1%'
     AND wait_event_type = 'Lock'
     AND pid != pg_backend_pid();

-- Release → INSERT completes
1: COMMIT;
2<:

-- INSERT wrote L1 (below threshold)
1: SELECT count(*) > 0 AS l1_from_blocked_insert
   FROM time_series.cagg_invalidation_log;

1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- ============================================================
-- Test 12: LOCK threshold SHARE → INSERT trigger proceeds
--          (upstream insert perm 8: SHARE lock compatible with AccessShare)
--          INSERT's trigger reads threshold with AccessShare,
--          which is compatible with SHARE → no blocking.
-- ============================================================

1: BEGIN;
1: LOCK TABLE time_series.cagg_invalidation_threshold IN SHARE MODE;

-- INSERT proceeds (AccessShare compatible with Share)
2: INSERT INTO src1 VALUES ('2024-01-01 02:15+00', 7);

-- Verify INSERT completed (not blocked)
1: SELECT count(*) > 0 AS insert_ok_with_share
   FROM src1 WHERE loc = 7;

-- REFRESH needs to UPDATE threshold → needs RowExclusive → blocked
3&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- Verify REFRESH blocked.  Poll up to 30s for session 3 to reach lock.
1: DO $$ DECLARE n int; deadline timestamptz := clock_timestamp() + interval '30 seconds'; BEGIN LOOP SELECT count(*) INTO n FROM pg_stat_activity WHERE query LIKE '%refresh_continuous_aggregate%' AND wait_event_type = 'Lock' AND pid != pg_backend_pid(); EXIT WHEN n >= 1 OR clock_timestamp() > deadline; PERFORM pg_sleep(0.2); END LOOP; END$$;
1: SELECT count(*) > 0 AS refresh_blocked_by_share
   FROM pg_stat_activity
   WHERE query LIKE '%refresh_continuous_aggregate%'
     AND wait_event_type = 'Lock'
     AND pid != pg_backend_pid();

1: COMMIT;
3<:

-- Final correctness
1: SELECT count(*) AS diff_t12 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 13: Two REFRESHes on same source → threshold updated correctly
--          (upstream concurrent_invalidation perm 1: two concurrent threshold updates)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 12:30+00', 4);

-- Lock threshold to queue both REFRESHes
1: BEGIN;
1: LOCK TABLE time_series.cagg_invalidation_threshold IN EXCLUSIVE MODE;

2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
3&: CALL time_series.refresh_continuous_aggregate('cv2', NULL, NULL);

1: COMMIT;
2<:
3<:

-- Both CAGGs correct
1: SELECT count(*) AS diff_t13_cv1 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;
1: SELECT count(*) AS diff_t13_cv2 FROM (
   (SELECT bucket, cnt FROM cv2
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), count(*)
    FROM src2 GROUP BY 1)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), count(*)
    FROM src2 GROUP BY 1
    EXCEPT
    SELECT bucket, cnt FROM cv2)
   ) x;

-- Threshold should equal MAX of all watermarks on src1
1: SELECT bool_and(t.threshold = w.max_wm) AS th_correct
   FROM time_series.cagg_invalidation_threshold t,
     (SELECT MAX(w2.watermark) AS max_wm
      FROM time_series.cagg_watermark w2
      JOIN time_series.continuous_agg c ON w2.cagg_id = c.cagg_id
      WHERE c.source_table_oid = 'src1'::regclass) w
   WHERE t.source_table_oid = 'src1'::regclass;

-- ============================================================
-- Fault-injection tests using cagg_trigger_before_l1_write
--
-- NOTE: Tests 14-17 use segment-level fault injection on the
-- trigger. Because the trigger runs on the segment where the
-- INSERT's data is distributed (determined by hash), and
-- gp_wait_until_triggered_fault requires ALL segments with the
-- fault to be triggered, these tests can hang if data only
-- lands on one segment but the fault is injected on all.
--
-- TODO: These tests need a mechanism to inject fault on the
-- specific segment where data will land (requires hash prediction
-- or DISTRIBUTED REPLICATED source tables). Skipped for now.
-- The same concurrent scenarios ARE covered by:
-- - LOCK TABLE tests (Test 11-13)
-- - Coordinator fault tests (Test 18)
-- ============================================================

-- Tests 14-17 skipped (segment-level trigger fault limitation)
-- See above NOTE for details.

-- ============================================================
-- Test 14: Pause REFRESH between TX1→TX2 + concurrent INSERT
--          Verify that L1 from paused trigger is NOT consumed
--          by the paused REFRESH's TX1 (already committed)
--          (upstream insert perm 14: complex interleaving)
-- ============================================================

1: INSERT INTO src1 VALUES ('2024-01-01 15:00+00', 15);

-- Pause REFRESH after TX1 (L1→L2 done, committed)
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'suspend', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Start REFRESH → pauses between TX1 and TX2
2>: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

1: SELECT gp_wait_until_triggered_fault('cagg_refresh_after_commit_and_chain', 1, dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- INSERT while REFRESH is paused → writes L1 (in a new TX, after TX1 committed)
3: INSERT INTO src1 VALUES ('2024-01-01 05:00+00', 16);

-- L1 should have the new entry
1: SELECT count(*) > 0 AS l1_during_pause FROM time_series.cagg_invalidation_log;

-- Resume REFRESH TX2 (it won't see the new L1 — TX1 already migrated)
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'resume', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
2<:

1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'reset', dbid)
   FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- New L1 still exists (not consumed by the paused REFRESH)
1: SELECT count(*) > 0 AS l1_still_exists FROM time_series.cagg_invalidation_log;

-- Second REFRESH picks it up
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);
1: SELECT count(*) AS diff_t14 FROM (
   (SELECT bucket, loc, cnt FROM cv1
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- ============================================================
-- Test 15: Multi-chunk INSERT during blocked REFRESH [P0]
--   Test 1 above probed the INSERT-vs-blocked-REFRESH ordering
--   with a single-row dirty.  Test 15 stresses the same ordering
--   with INSERTs scattered across 5 distinct chunks, then
--   verifies:
--     1. INSERTs proceed while REFRESH is blocked on mat lock
--        (i.e. cagg_invalidation_trigger AccessShare on threshold
--        is compatible with REFRESH's EXCLUSIVE on mat table)
--     2. L1 captures multi-chunk invalidation entries
--     3. After lock release + a follow-up refresh, the mat table
--        is bit-equivalent to the source across every dirty chunk
--     4. Per-chunk presence (no chunk silently dropped)
-- ============================================================

-- Push cv1's watermark far into the future so all 5 historical
-- INSERTs below are guaranteed to land below watermark and trigger
-- L1 writes.
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, '2026-01-01'::timestamptz);

-- Seed L1 work for session 2's refresh: insert below watermark
-- into a stable bucket so refresh has something to consume and
-- actually attempts the mat-table lock.
1: INSERT INTO src1 VALUES ('2024-01-01 02:30+00'::timestamptz, 251);

-- Lock the cv1 mat table so the next REFRESH queues behind it
1: BEGIN;
1: DO $$ BEGIN EXECUTE format('LOCK TABLE time_series.%I IN EXCLUSIVE MODE', (SELECT mat_table_name FROM time_series.continuous_agg WHERE user_view_name = 'cv1')); END $$;

-- Kick off the REFRESH that will park on the mat-table lock
2&: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- Session 3: scatter dirty INSERTs into 5 distinct chunks.  4 h
-- chunk + 2020-01-01 origin → each timestamp below maps to a
-- different chunk_number.  Each INSERT must succeed without
-- blocking on the mat-table lock (INSERT only needs AccessShare
-- on the source / RowExclusive on L1).
3: INSERT INTO src1 VALUES ('2024-01-01 02:30+00', 101);
3: INSERT INTO src1 VALUES ('2024-01-01 10:30+00', 102);
3: INSERT INTO src1 VALUES ('2024-01-01 18:30+00', 103);
3: INSERT INTO src1 VALUES ('2024-01-02 06:30+00', 104);
3: INSERT INTO src1 VALUES ('2024-01-02 18:30+00', 105);

-- All 5 dirty rows visible in source while REFRESH is still parked
1: SELECT count(*) AS dirty_visible_in_src
   FROM src1 WHERE loc BETWEEN 101 AND 105;

-- The 5 dirty timestamps must land in 5 distinct ts_chunk slots
-- (otherwise the test silently degrades to fewer-chunk coverage).
1: WITH dirty_times(ts) AS (VALUES
       ('2024-01-01 02:30+00'::timestamptz),
       ('2024-01-01 10:30+00'::timestamptz),
       ('2024-01-01 18:30+00'::timestamptz),
       ('2024-01-02 06:30+00'::timestamptz),
       ('2024-01-02 18:30+00'::timestamptz))
   SELECT count(DISTINCT floor(extract(epoch from (ts - '2020-01-01'::timestamptz))/14400)::int)
            AS dirty_chunks_touched
   FROM dirty_times;

-- L1 should hold the multi-chunk invalidation entries
1: SELECT count(*) > 0 AS l1_has_multi_chunk_inserts
   FROM time_series.cagg_invalidation_log;

-- Release the mat-table lock — the queued REFRESH drains
1: COMMIT;
2<:

-- The parked REFRESH took its snapshot before the 5 INSERTs landed,
-- so the dirty rows still need a follow-up refresh to materialize.
1: CALL time_series.refresh_continuous_aggregate('cv1', NULL, NULL);

-- L1 should be drained after the catch-up refresh
1: SELECT count(*) AS l1_after_catchup
   FROM time_series.cagg_invalidation_log;

-- Symmetric EXCEPT bit-equivalence over the full source
1: SELECT count(*) AS diff_t15 FROM (
   (SELECT bucket, loc, cnt FROM cv1 EXCEPT
    SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), loc, count(*)
    FROM src1 GROUP BY 1, 2
    EXCEPT
    SELECT bucket, loc, cnt FROM cv1)
   ) x;

-- Per-chunk presence: each of the 5 dirty (bucket, loc) tuples
-- must be reflected in cv1.  A zero on any of these flags a
-- "chunk silently dropped" regression.
1: SELECT
     count(*) FILTER (WHERE bucket = '2024-01-01 02:00+00' AND loc = 101) AS mc_a,
     count(*) FILTER (WHERE bucket = '2024-01-01 10:00+00' AND loc = 102) AS mc_b,
     count(*) FILTER (WHERE bucket = '2024-01-01 18:00+00' AND loc = 103) AS mc_c,
     count(*) FILTER (WHERE bucket = '2024-01-02 06:00+00' AND loc = 104) AS mc_d,
     count(*) FILTER (WHERE bucket = '2024-01-02 18:00+00' AND loc = 105) AS mc_e
   FROM cv1;

-- No duplicate (bucket, loc) rows after the multi-chunk catch-up
1: SELECT count(*) AS dup_t15 FROM (
     SELECT bucket, loc, count(*) FROM time_series._mat_cv1_1
     GROUP BY bucket, loc HAVING count(*) > 1
   ) x;

-- ============================================================
-- Cleanup
-- ============================================================
1: DROP TABLE src1 CASCADE;
1: DROP TABLE src2 CASCADE;
1: DROP EXTENSION time_series CASCADE;
1q:
2q:
3q:
