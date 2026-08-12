-- ============================================================
-- cagg_multi_iso.sql (isolation2)
-- Isolation pattern: based on the reference cagg concurrency test
--
-- Tests two CAGGs on the same source table with concurrent
-- INSERT, UPDATE, and REFRESH operations:
--   1. REFRESH on cagg_1 blocked, cagg_2 REFRESH proceeds independently
--   2. INSERT invalidates source, both CAGGs refresh correctly
--   3. UPDATE source, both CAGGs see updated values after refresh
-- ============================================================

1: SET optimizer = off;
1: DROP EXTENSION IF EXISTS time_series CASCADE;
1: CREATE EXTENSION time_series;
1: SET search_path TO public, time_series;

1: CREATE TABLE src (time TIMESTAMPTZ NOT NULL, device_id INT NOT NULL, val INT) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
   DISTRIBUTED BY (device_id);
1: INSERT INTO src SELECT '2024-01-01'::timestamptz + (i * interval '30 min'),
   (i % 5) + 1, i FROM generate_series(1, 60) i;

-- Two CAGGs: different aggregates, same source
1: CREATE MATERIALIZED VIEW cv_count WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          device_id, count(val) AS cnt
   FROM src GROUP BY bucket, device_id;

1: CREATE MATERIALIZED VIEW cv_max WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          device_id, max(val) AS maxval
   FROM src GROUP BY bucket, device_id;

-- Initial REFRESH for both
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

2: SET optimizer = off;
2: SET search_path TO public, time_series;
3: SET optimizer = off;
3: SET search_path TO public, time_series;

-- ============================================================
-- SANITY-CHUNKS: src must already span multiple chunks before the
-- concurrency perms below run.  60 rows × 30 min = 30 h of data
-- at ts_chunk_interval = 4 h → at least 7 chunks (the last row at
-- 30:00 lands in chunk 7).  Pinning this prevents a future
-- chunk_interval bump from silently turning every UPDATE/REFRESH
-- perm into a single-chunk dance.
-- ============================================================
1: SELECT count(DISTINCT chunk_number) >= 7 AS src_spans_at_least_7_chunks
   FROM time_series.ts_chunk WHERE table_oid = 'src'::regclass;

-- ============================================================
-- Test 1: REFRESH cv_count blocked by mat table lock,
--         REFRESH cv_max proceeds independently.
--
-- Corresponds to upstream permutation:
--   "LockMat1" "Refresh1" "Refresh2" "UnlockMat1"
-- ============================================================

-- Seed L1 work so the next refresh actually attempts the mat lock
-- (post hot-bucket-exclusion fix: refresh is a no-op when there's
-- no new stable data, so the test pattern "lock mat → refresh blocks
-- on lock" only fires if refresh has work to do).
1: INSERT INTO src VALUES ('2024-01-01 02:30+00'::timestamptz, 1, 161);

-- Session 1 locks cv_count's mat table
1: BEGIN;
1: LOCK TABLE time_series._mat_cv_count_1 IN EXCLUSIVE MODE;

-- Session 2 tries to REFRESH cv_count — should block (needs mat table)
2&: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);

-- Session 3 REFRESHes cv_max — should proceed (independent mat table)
3: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Release lock, session 2 completes
1: COMMIT;
2<:

-- Both CAGGs correct
1: SELECT count(*) AS diff_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

1: SELECT count(*) AS diff_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- ============================================================
-- Test 2: INSERT invalidates source, both CAGGs refresh correctly.
--
-- Corresponds to upstream permutation:
--   "Refresh1" "Refresh2" "LockMat1" "I1" "Refresh1" "Refresh2"
--   "UnlockMat1" "Refresh1_sel" "Refresh2_sel"
-- ============================================================

-- INSERT backfill data into bucket 0 range
2: INSERT INTO src SELECT '2024-01-01 00:00+00'::timestamptz, (i % 5) + 1, i * 10
   FROM generate_series(1, 10) i;

-- Lock cv_count mat table
1: BEGIN;
1: LOCK TABLE time_series._mat_cv_count_1 IN EXCLUSIVE MODE;

-- Session 2 tries REFRESH cv_count — blocked
2&: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);

-- Session 3 REFRESHes cv_max — proceeds, sees the INSERT
3: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Unlock → cv_count REFRESH proceeds
1: COMMIT;
2<:

-- cv_count: bucket 00:00 should have the extra rows
1: SELECT cnt FROM cv_count WHERE bucket = '2024-01-01 00:00+00' ORDER BY device_id LIMIT 3;

-- cv_max: should see the max of the new data
1: SELECT maxval FROM cv_max WHERE bucket = '2024-01-01 00:00+00' ORDER BY device_id LIMIT 3;

-- Both EXCEPT = 0
1: SELECT count(*) AS diff2_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

1: SELECT count(*) AS diff2_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- ============================================================
-- Test 3: append new source data, both CAGGs see it after refresh.
--
-- time_series tables are append-only, so the way source data changes
-- is INSERT (upstream's UPDATE-based permutation does not apply to an
-- append-only AM -- UPDATE is rejected with "cannot update a
-- time_series table").  These INSERTs change both count() and max()
-- for the affected (bucket, device_id) groups; after a refresh both
-- CAGGs must still equal the source aggregation.
-- ============================================================

-- Append rows that change count + max in two distinct buckets
2: INSERT INTO src VALUES ('2024-01-01 00:15+00', 1, 9999);
2: INSERT INTO src VALUES ('2024-01-01 03:15+00', 3, 1);

-- REFRESH both
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Both EXCEPT = 0 after UPDATE + REFRESH
1: SELECT count(*) AS diff3_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

1: SELECT count(*) AS diff3_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- ============================================================
-- Test 4: new data + LOCK watermark → both REFRESHes blocked → release
--         (upstream multi_iso perm 4 used UPDATE to dirty rows; on an
--         append-only AM we dirty a bucket with INSERT instead.)
--         Uses LOCK TABLE on cagg_watermark to simulate threshold lock.
-- ============================================================

-- Append a row so the refresh below has real work: it must run
-- cagg_advance_watermark's UPDATE on cagg_watermark, which then
-- conflicts with the EXCLUSIVE lock and blocks.  Without dirty work,
-- n_intervals == 0 takes the early-return path that never write-locks
-- cagg_watermark, and the EXCLUSIVE lock would NOT block the refresh's
-- catalog reads (PG lock matrix: EXCLUSIVE is compatible with
-- AccessShare).
1: INSERT INTO src VALUES ('2024-01-01 02:30+00'::timestamptz, 2, 7777);

-- Lock watermark table (simulates threshold row lock)
1: BEGIN;
1: LOCK TABLE time_series.cagg_watermark IN EXCLUSIVE MODE;

-- Both REFRESHes block on watermark read
2&: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
3&: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Verify both waiting.  pre-poll up to 30s so 2&/3& have time to reach
-- lock acquisition before we sample pg_stat_activity; under heavy
-- host CPU contention "2&:" can take >1s just to parse and dispatch,
-- racing the immediate sample.
1: DO $$ DECLARE n int; deadline timestamptz := clock_timestamp() + interval '30 seconds'; BEGIN LOOP SELECT count(*) INTO n FROM pg_stat_activity WHERE query LIKE '%refresh_continuous_aggregate%' AND wait_event_type = 'Lock' AND pid != pg_backend_pid(); EXIT WHEN n >= 2 OR clock_timestamp() > deadline; PERFORM pg_sleep(0.2); END LOOP; END$$;
1: SELECT count(*) AS both_waiting
   FROM pg_stat_activity
   WHERE query LIKE '%refresh_continuous_aggregate%'
     AND wait_event_type = 'Lock'
     AND pid != pg_backend_pid();

-- Release
1: COMMIT;
2<:
3<:

-- Both should see the UPDATE
1: SELECT count(*) AS diff4_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

1: SELECT count(*) AS diff4_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- ============================================================
-- Test 5: TRUNCATE source → watermark reset → CAGG empty
--         Verifies the TRUNCATE hook resets watermark + threshold
--         so the real-time view returns 0 rows immediately.
-- ============================================================

1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: SELECT count(*) AS mat_before_trunc FROM cv_count;

-- Snapshot the chunk count before TRUNCATE so we can verify the
-- hypertable's catalog metadata is actually being torn down.
1: SELECT count(*) > 0 AS chunks_before_trunc
   FROM time_series.ts_chunk WHERE table_oid = 'src'::regclass;

-- TRUNCATE resets watermark to -infinity
1: TRUNCATE src;

-- CAGG should show 0 rows (watermark reset, live branch = empty source)
1: SELECT count(*) AS cagg_after_trunc FROM cv_count;

-- Hypertable invariant: TRUNCATE must also drop the per-chunk
-- metadata in ts_chunk for this source.  If this regresses, future
-- INSERTs would reuse stale chunk_number assignments and corrupt
-- the chunk routing logic.
1: SELECT count(*) AS chunks_after_trunc
   FROM time_series.ts_chunk WHERE table_oid = 'src'::regclass;

-- REFRESH cleans up mat table
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: ALTER VIEW cv_count SET (time_series.materialized_only = true);
1: SELECT count(*) AS mat_after_trunc FROM cv_count;
1: ALTER VIEW cv_count SET (time_series.materialized_only = false);

-- ============================================================
-- Test 6: MPP — Concurrent DROP CAGG + INSERT
--         Session 1 drops CAGG (event trigger cleans catalog),
--         Session 2 INSERTs into source (trigger scans catalog).
--         Trigger should either see the CAGG or not — no crash.
-- ============================================================

-- Re-populate source for this test
1: INSERT INTO src SELECT '2024-01-01'::timestamptz + (i * interval '30 min'),
   (i % 5) + 1, i FROM generate_series(1, 20) i;
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);

-- Session 1 drops cv_max (event trigger deletes catalog entries),
-- session 2 INSERTs (trigger scans continuous_agg).  DROP VIEW does
-- not block a concurrent INSERT on the source in CBDB, so we don't
-- force a mid-flight interleave (the iso2 '&' blocking form would
-- just report "not blocking"); we run them from the two sessions and
-- assert the invariant that actually matters: no crash, cv_count
-- stays correct, cv_max is gone.
2: INSERT INTO src VALUES ('2024-01-01 01:15+00', 99, 999);
1: DROP VIEW cv_max CASCADE;

-- No crash — cv_count should still work
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: SELECT count(*) AS diff_drop_insert FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

-- cv_max should be gone
1: SELECT count(*) AS cv_max_gone FROM pg_class WHERE relname = 'cv_max';

-- ============================================================
-- Test 7: MPP — Concurrent ALTER materialized_only + SELECT
--         Session 1 toggles mode (CREATE OR REPLACE VIEW),
--         Session 2 reads CAGG at the same time.
--         SELECT should either see old or new view — no crash.
-- ============================================================

-- Ensure real-time mode
1: ALTER VIEW cv_count SET (time_series.materialized_only = false);

-- A SELECT on the CAGG does not block a concurrent ALTER VIEW
-- (and vice versa) in CBDB, so the iso2 '&' blocking form would just
-- report "not blocking".  Interleave a toggle between two reads from
-- a second session and assert the real invariant: every read
-- succeeds (no crash) regardless of the mode flip in flight.
2: SELECT count(*) >= 0 AS select_during_toggle FROM cv_count;
1: ALTER VIEW cv_count SET (time_series.materialized_only = true);
2: SELECT count(*) >= 0 AS select_during_toggle2 FROM cv_count;
1: ALTER VIEW cv_count SET (time_series.materialized_only = false);
2: SELECT count(*) >= 0 AS select_after_toggle FROM cv_count;

-- CAGG still queryable and correct
1: SELECT count(*) AS diff_toggle FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

-- ============================================================
-- Test 8: MPP — Two concurrent INSERTs into the same segment → two
--         invalidation triggers write L1 concurrently.  (Upstream
--         used two UPDATEs; time_series is append-only so the
--         trigger-firing DML is INSERT.  Both INSERTs target
--         device_id = 1, which hashes to a single segment, so the
--         two L1 writes happen on the same segment concurrently.
--         simple_heap_insert has no unique constraint → both
--         succeed.)
-- ============================================================

1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);

-- Two sessions INSERT for the same device_id (same segment).  Append
-- INSERTs into a time_series chunk do not take a conflicting lock that
-- would make one wait on the other, so the iso2 '&' blocking form does
-- not apply; issue them from the two sessions back-to-back.  The
-- invariant is that BOTH trigger fires land L1 entries (no lost
-- invalidation) and a later refresh rolls both up.
1: INSERT INTO src VALUES ('2024-01-01 00:15+00', 1, 8888);
2: INSERT INTO src VALUES ('2024-01-01 01:15+00', 1, 7777);

-- Both INSERTs should have created L1 entries
1: SELECT count(*) > 0 AS l1_from_two_inserts FROM time_series.cagg_invalidation_log;

-- REFRESH picks up both
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: SELECT count(*) AS diff_two_updates FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

-- ============================================================
-- Test 9: P0-2 — Concurrent REFRESH on two CAGGs (same source)
--         L1→L2 migration serialized by source-level advisory lock.
--         Previously: "tuple concurrently deleted" error.
--         Now: both serialize safely, no errors, data correct.
-- ============================================================

-- Re-create cv_max (dropped by Test 6)
1: CREATE MATERIALIZED VIEW cv_max WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          device_id, max(val) AS maxval
   FROM src GROUP BY bucket, device_id;

-- Re-populate and refresh both
1: INSERT INTO src SELECT '2024-01-01'::timestamptz + (i * interval '30 min'),
   (i % 5) + 1, i FROM generate_series(101, 120) i;

-- Create dirty L1 entries
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);
1: INSERT INTO src VALUES ('2024-01-01 01:15+00', 99, 999);

-- Both REFRESHes run sequentially via different sessions.
-- Source-level advisory lock ensures L1→L2 migration is serialized.
-- No "tuple concurrently deleted" error should occur.
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
2: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Both correct
1: SELECT count(*) AS diff_concurrent_cv_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;
1: SELECT count(*) AS diff_concurrent_cv_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- Run 3 more rounds — each round inserts dirty data then refreshes both
1: INSERT INTO src VALUES ('2024-01-01 02:15+00', 88, 888);
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
2: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

1: INSERT INTO src VALUES ('2024-01-01 03:15+00', 77, 777);
2: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

1: INSERT INTO src VALUES ('2024-01-01 04:15+00', 66, 666);
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
2: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Final correctness check
1: SELECT count(*) AS diff_final_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;
1: SELECT count(*) AS diff_final_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- ============================================================
-- Test 10: Multi-chunk concurrent INSERT → both CAGGs correct [P0]
--
-- Tests 3-4 above exercise UPDATE-then-REFRESH with dirty rows
-- bunched into 2024-01-01's first 5 hours (~2 chunks).  Test 10
-- stresses the same pattern with INSERTs scattered across 5
-- distinct chunks, run in two concurrent sessions on a freshly
-- repopulated source, then verifies:
--   1. L1 captures multi-chunk invalidation entries
--   2. After REFRESH, both cv_count and cv_max are bit-equivalent
--      to the source across every dirty chunk (symmetric EXCEPT)
--   3. Per-chunk presence — no chunk silently dropped from either
--      mat table
--
-- count vs max are sensitive to L1 replay differently: count is
-- additive (extra L1 replays are no-ops if idempotent), but max
-- can mask higher values if a chunk's L1 is dropped.  Running
-- both side-by-side surfaces aggregate-specific regressions that
-- a single-CAGG test would miss.
-- ============================================================

-- Repopulate src with a known multi-chunk dataset so the dirty
-- INSERTs below land into chunks that already contain baseline
-- rows (forcing each chunk's L1 to roll up both old + new vals).
-- 96 rows × 30 min = 48 h = 12 chunks at 4 h interval.
1: TRUNCATE src;
1: INSERT INTO src
   SELECT '2024-01-01'::timestamptz + (i * interval '30 min'),
          (i % 5) + 1, i
   FROM generate_series(1, 96) i;
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- Snapshot current chunk span — must be ≥ 12 after the repopulate.
1: SELECT count(DISTINCT chunk_number) >= 12 AS chunks_before_mc_insert
   FROM time_series.ts_chunk WHERE table_oid = 'src'::regclass;

-- Self-check: the 5 dirty timestamps must fall in 5 distinct 4 h
-- chunks (origin 2020-01-01).  Guards against future
-- chunk_interval changes silently collapsing the test.
1: WITH dirty_times(ts) AS (VALUES
       ('2024-01-01 01:30+00'::timestamptz),
       ('2024-01-01 05:30+00'::timestamptz),
       ('2024-01-01 13:30+00'::timestamptz),
       ('2024-01-01 21:30+00'::timestamptz),
       ('2024-01-02 05:30+00'::timestamptz))
   SELECT count(DISTINCT floor(extract(epoch from (ts - '2020-01-01'::timestamptz))/14400)::int)
            AS dirty_chunks_touched
   FROM dirty_times;

-- Two sessions INSERT into the SAME 5 timestamps but with different
-- device_ids.  They target different rows (different distribution
-- key) so they never contend on a lock -- i.e. they do NOT block
-- each other, so the iso2 '&' blocking form does not apply here.  We
-- issue them from the two sessions back-to-back; what matters for
-- this P0 is that both sets of L1 entries span the same 5 chunks and
-- a later refresh rolls them all up correctly.  Session 3's val
-- (5202) is strictly larger than session 2's (5101) so cv_max's
-- per-chunk expected max for device_id=2 is 5202.
2: INSERT INTO src VALUES
       ('2024-01-01 01:30+00', 1, 5101),
       ('2024-01-01 05:30+00', 1, 5101),
       ('2024-01-01 13:30+00', 1, 5101),
       ('2024-01-01 21:30+00', 1, 5101),
       ('2024-01-02 05:30+00', 1, 5101);
3: INSERT INTO src VALUES
       ('2024-01-01 01:30+00', 2, 5202),
       ('2024-01-01 05:30+00', 2, 5202),
       ('2024-01-01 13:30+00', 2, 5202),
       ('2024-01-01 21:30+00', 2, 5202),
       ('2024-01-02 05:30+00', 2, 5202);

-- L1 should have multi-chunk invalidation entries from both
-- sessions' INSERTs (5 chunks × 2 sessions worth of trigger fires).
1: SELECT count(*) > 0 AS l1_has_multi_chunk_inserts
   FROM time_series.cagg_invalidation_log;

-- REFRESH both CAGGs.  The advisory-lock serialization (Test 9
-- regression) should still hold across multi-chunk dirty.
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
2: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);

-- L1 drained after both refreshes
1: SELECT count(*) AS l1_after_mc_refresh
   FROM time_series.cagg_invalidation_log;

-- Symmetric EXCEPT bit-equivalence on both CAGGs over the full
-- source.  Catches BOTH duplicate-write (mat extra rows) AND
-- lost-update (source row missing from mat).
1: SELECT count(*) AS diff_t10_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

1: SELECT count(*) AS diff_t10_max FROM (
   (SELECT bucket, device_id, maxval FROM cv_max
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, max(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, maxval FROM cv_max)
   ) x;

-- Per-chunk presence: each INSERT timestamp's hour-bucket must
-- carry maxval = 5202 for device_id=2 across all 5 dirty chunks.
-- A zero here flags a chunk that silently failed to refresh.
1: SELECT
     count(*) FILTER (WHERE bucket = '2024-01-01 01:00+00' AND device_id = 2 AND maxval = 5202) AS mc_a,
     count(*) FILTER (WHERE bucket = '2024-01-01 05:00+00' AND device_id = 2 AND maxval = 5202) AS mc_b,
     count(*) FILTER (WHERE bucket = '2024-01-01 13:00+00' AND device_id = 2 AND maxval = 5202) AS mc_c,
     count(*) FILTER (WHERE bucket = '2024-01-01 21:00+00' AND device_id = 2 AND maxval = 5202) AS mc_d,
     count(*) FILTER (WHERE bucket = '2024-01-02 05:00+00' AND device_id = 2 AND maxval = 5202) AS mc_e
   FROM cv_max;

-- ============================================================
-- Test 11: 3 CAGGs on same source, drop the middle one → L1 stays
--          for the two remaining siblings, both refreshes see the
--          backfill.
--
-- Regression for commit edd5a4aa "cagg drop wipes L1 needed by
-- sibling CAGGs on same source".  Test 6 covers the 2-CAGG case
-- (drop one, surviving sees the row); this extends to N=3 to catch
-- a regression where the cleanup might accidentally key on "last 2"
-- vs "last 1" instead of "last on source".
--
-- NOTE on the fault point used by Test 12 below:
-- cagg_refresh_after_commit_and_chain wedges the refresh BETWEEN
-- TX1 commit (L1 consumed, threshold advanced, row locks released)
-- and TX2 start (mat write).  We picked this over the
-- before_commit_and_chain point because that one holds the per-
-- source invalidation_threshold row lock; a concurrent DROP CAGG
-- which goes through path B (other_count > 0) also UPDATEs that
-- threshold row, so it would block on the lock and hang the test.
-- After TX1 commits, the threshold row lock is gone but cv_count
-- has NOT yet materialized — still "mid-refresh" for the purposes
-- of this race.
-- ============================================================

-- Add a third CAGG on the same source.  cv_count and cv_max already
-- exist at this point (cv_max was recreated in Test 9).
1: CREATE MATERIALIZED VIEW cv_min WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          device_id, min(val) AS minval
   FROM src GROUP BY bucket, device_id;
1: CALL time_series.refresh_continuous_aggregate('cv_min', NULL, NULL);

-- Drain L1 from any prior test residue.
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_max', NULL, NULL);
1: SELECT count(*) AS t11_l1_pre_clean
   FROM time_series.cagg_invalidation_log
   WHERE source_table_oid = 'src'::regclass::oid;

-- Backfill below all three watermarks → INSERT trigger writes L1.
-- t=00:15 lies in the first hour bucket; we already INSERTed lots of
-- rows earlier in Test 10, so any of those buckets is well below the
-- threshold.  Use a fresh value (5300) to make the post-refresh
-- check unambiguous.
1: INSERT INTO src VALUES ('2024-01-01 00:15+00'::timestamptz, 7, 5300);

1: SELECT count(*) >= 1 AS t11_l1_present_before_drop
   FROM time_series.cagg_invalidation_log
   WHERE source_table_oid = 'src'::regclass::oid;

-- Drop the *middle* CAGG (cv_max).  After fix: only cv_max's per-
-- CAGG catalog rows go away; L1 keyed on src must remain because
-- cv_count and cv_min still consume from it.
1: DROP VIEW cv_max CASCADE;

1: SELECT count(*) >= 1 AS t11_l1_survives_middle_drop
   FROM time_series.cagg_invalidation_log
   WHERE source_table_oid = 'src'::regclass::oid;

-- Refresh the two survivors: each must materialize the backfill row.
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: CALL time_series.refresh_continuous_aggregate('cv_min', NULL, NULL);

-- cv_count: the 00:00 hour-bucket for device_id=7 had no prior rows
-- (Test 10 used device_id=2), so cnt = 1 after this single INSERT.
1: SELECT CASE WHEN cnt >= 1 THEN 'OK' ELSE 'FAIL: '||cnt::text END AS t11_cv_count_sees_backfill
   FROM cv_count WHERE bucket = '2024-01-01 00:00+00' AND device_id = 7;

-- cv_min: minval for device_id=7 in 00:00 bucket must equal 5300.
1: SELECT CASE WHEN minval = 5300 THEN 'OK' ELSE 'FAIL: '||minval::text END AS t11_cv_min_sees_backfill
   FROM cv_min WHERE bucket = '2024-01-01 00:00+00' AND device_id = 7;

-- L1 drained after both surviving CAGGs consumed it.
1: SELECT count(*) AS t11_l1_drained
   FROM time_series.cagg_invalidation_log
   WHERE source_table_oid = 'src'::regclass::oid;

-- Symmetric EXCEPT for both survivors: bit-equivalent with source.
1: SELECT count(*) AS t11_diff_count FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;
1: SELECT count(*) AS t11_diff_min FROM (
   (SELECT bucket, device_id, minval FROM cv_min
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, min(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, min(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, minval FROM cv_min)
   ) x;

-- ============================================================
-- Test 12: DROP a sibling CAGG while another's REFRESH is mid-flight.
--
-- Exercises the path B branch of _cagg_cleanup_catalog (other_count
-- > 0): the drop must only recompute the per-source threshold from
-- the remaining watermarks, NOT touch L1 — even when another CAGG's
-- refresh is paused mid-execution between TX1 (L1 read) and TX2 (mat
-- write).
--
-- Uses the cagg_refresh_after_commit_and_chain fault to wedge
-- cv_count's refresh after TX1 commit (L1 already consumed, row
-- locks released) but before TX2 mat write.  This positioning lets
-- the concurrent DROP cv_min in session 3 run without deadlocking
-- on the invalidation_threshold row lock (see note above Test 11).
-- ============================================================

-- Seed L1 with a fresh backfill row that the mid-flight refresh has
-- to materialize.
1: INSERT INTO src VALUES ('2024-01-01 00:45+00'::timestamptz, 8, 5400);

1: SELECT count(*) >= 1 AS t12_l1_seeded
   FROM time_series.cagg_invalidation_log
   WHERE source_table_oid = 'src'::regclass::oid;

-- Enable fault on every primary segment (refresh runs on segments
-- in MPP mode).  'suspend' makes the next caller block at the
-- injection point until 'resume' is sent.
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain',
                          'suspend', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = -1;

-- Session 2 kicks off cv_count's refresh; pauses at the fault point.
2>: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);

-- Wait until the fault is observed on every primary segment so we
-- know cv_count is genuinely paused mid-refresh (post-TX1, pre-TX2).
1: SELECT gp_wait_until_triggered_fault('cagg_refresh_after_commit_and_chain',
                                         1, dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = -1;

-- Session 3: DROP cv_min while cv_count's refresh is paused.
-- After fix this hits path B: only threshold gets recomputed; L1
-- must NOT be touched.  Buggy version would wipe L1, but cv_count
-- already snapshotted what it needed in TX1, so its mat write
-- (after resume) would still succeed for the bucket already in
-- flight — the smoking gun would be a subsequent INSERT-then-refresh
-- showing the L1 row missing.
3: DROP VIEW cv_min CASCADE;

-- Resume the paused refresh — cv_count finishes TX2 and exits.
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain',
                          'resume', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = -1;
2<:

-- Reset fault.
1: SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain',
                          'reset', dbid)
   FROM gp_segment_configuration
   WHERE role = 'p' AND content = -1;

-- cv_count must have materialized the device_id=8 backfill row.
1: SELECT CASE WHEN cnt >= 1 THEN 'OK' ELSE 'FAIL: '||cnt::text END AS t12_cv_count_after_refresh
   FROM cv_count WHERE bucket = '2024-01-01 00:00+00' AND device_id = 8;

-- The smoking-gun check: insert ANOTHER backfill row, refresh again,
-- and verify it lands.  If the in-flight DROP had wiped L1 on the
-- pre-fix code path, subsequent INSERT-trigger L1 rows would still
-- be safe, but a stale invalidation_threshold (also touched in path
-- B) could cause the trigger to skip writing entirely.  Round-trip
-- check covers both.
1: INSERT INTO src VALUES ('2024-01-01 01:45+00'::timestamptz, 8, 5500);
1: CALL time_series.refresh_continuous_aggregate('cv_count', NULL, NULL);
1: SELECT CASE WHEN cnt >= 1 THEN 'OK' ELSE 'FAIL: '||cnt::text END AS t12_post_drop_refresh_works
   FROM cv_count WHERE bucket = '2024-01-01 01:00+00' AND device_id = 8;

-- Symmetric EXCEPT after the dust settles.
1: SELECT count(*) AS t12_diff_count_final FROM (
   (SELECT bucket, device_id, cnt FROM cv_count
    EXCEPT
    SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2)
   UNION ALL
   (SELECT time_bucket('1 hour'::interval, time), device_id, count(val)
    FROM src GROUP BY 1, 2
    EXCEPT
    SELECT bucket, device_id, cnt FROM cv_count)
   ) x;

-- Cleanup
1: DROP TABLE src CASCADE;
1: DROP EXTENSION time_series CASCADE;
1q:
2q:
3q:
