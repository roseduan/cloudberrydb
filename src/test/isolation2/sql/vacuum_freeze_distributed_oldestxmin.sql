-- Regression test for freezing the xmax of an ABORTED deleter during VACUUM,
-- and its dependency on the distributed "oldest xmin".
--
-- Background
-- ----------
-- When a distributed transaction deletes a tuple and then aborts, the tuple is
-- live again but its xmax still points at the (aborted) deleter. A VACUUM with
-- vacuum_freeze_min_age = 0 is expected to freeze/clear that xmax (t_xmax = 0).
--
-- On a segment (QE) the freeze cutoff is derived from a lazily-cached value,
-- DistributedLogShared->oldestXmin. VACUUM only *reads* that value
-- (DistributedLog_GetOldestXmin); it is advanced elsewhere, as a side effect of
-- a dispatched query taking a distributed snapshot
-- (DistributedLog_AdvanceOldestXmin, via GetSnapshotData). If the cache has not
-- advanced past the aborted deleter's XID by the time VACUUM reads the cutoff,
-- the xmax cannot be frozen. This timing dependency is what made
-- isolation/heap-repeatable-read-vacuum-freeze (permutation 2) flaky.
--
-- This test pins both directions deterministically.

CREATE EXTENSION IF NOT EXISTS pageinspect;

CREATE TABLE heaptest_freeze (i int, j int);
INSERT INTO heaptest_freeze SELECT i, i FROM generate_series(0, 9) i;

-- Decode the xmax status of every heap tuple, on every segment. Guarded so a
-- segment with an empty relation does not error in get_raw_page().
-- (One line, per isolation2 test-language limitations.)
CREATE FUNCTION heaptest_freeze_flags(OUT contentid int, OUT xmax_kind text) RETURNS SETOF record AS $$ SELECT current_setting('gp_contentid')::int, CASE WHEN t_xmax = 0 THEN 'InvalidXid' ELSE 'NormalXid' END FROM heap_page_items(CASE WHEN pg_relation_size('heaptest_freeze') > 0 THEN get_raw_page('heaptest_freeze', 0) ELSE NULL END) $$ LANGUAGE sql EXECUTE ON ALL SEGMENTS;

-- ===========================================================================
-- Part 1: with the distributed oldest-xmin advanced, VACUUM MUST freeze the
-- aborted xmax. This is the behaviour the original (flaky) test intended.
-- ===========================================================================
1: BEGIN;
1: DELETE FROM heaptest_freeze;
1: ABORT;
1: SET vacuum_freeze_min_age = 0;
-- A dispatched SELECT establishes a distributed snapshot on every segment,
-- which advances DistributedLogShared->oldestXmin past the now-gone deleter.
1: SELECT count(*) FROM heaptest_freeze;
1: VACUUM heaptest_freeze;
-- Expected: xmax frozen on all 10 rows -> InvalidXid.
1: SELECT xmax_kind, count(*) FROM heaptest_freeze_flags() GROUP BY 1 ORDER BY 1;
1q:

-- ===========================================================================
-- Part 2: if the cached distributed oldest-xmin is NOT advanced, the freeze
-- cutoff sits at/below the aborted deleter's XID and VACUUM leaves the xmax
-- unfrozen. We force that state deterministically with
-- debug_disable_distributed_snapshot (QD skips the distributed snapshot, so the
-- QE takes the read-only DistributedLog_GetOldestXmin path and never advances
-- the cache). This is the exact state behind the intermittent failures.
-- ===========================================================================
2: TRUNCATE heaptest_freeze;
2: INSERT INTO heaptest_freeze SELECT i, i FROM generate_series(0, 9) i;
2: BEGIN;
2: DELETE FROM heaptest_freeze;
2: ABORT;
2: SET vacuum_freeze_min_age = 0;
2: SET debug_disable_distributed_snapshot = on;
2: VACUUM heaptest_freeze;
2: RESET debug_disable_distributed_snapshot;
-- Expected: xmax NOT frozen on all 10 rows -> NormalXid.
2: SELECT xmax_kind, count(*) FROM heaptest_freeze_flags() GROUP BY 1 ORDER BY 1;
2q:

DROP FUNCTION heaptest_freeze_flags();
DROP TABLE heaptest_freeze;
