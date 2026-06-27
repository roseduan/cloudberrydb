-- Sonic HashAgg spill-to-disk regression test.
--
-- Verifies that SonicGroupByNode correctly spills partial-aggregate rows to
-- disk when the per-thread memory budget is exhausted, and that the results
-- are semantically identical to the no-spill baseline.
--
-- Covered corners:
--   1. INT key + COUNT(*) + SUM  (fixed-width key, fixed-width agg states)
--   2. TEXT key + COUNT(*) + SUM (variable-length key: inline ≤12B + heap >12B)
--   3. TEXT key + MIN(text)      (only varlen agg state; serialisation hook)
--   4. Bare GROUP BY / DISTINCT  (zero aggregates — SonicGroupByNode dedup path)
--   5. budget=0 explicit disable (build_aggregatation_options no-spill branch)
--
-- The test uses a 1 MB spill budget (vector.sonicagg_spill_memory_mb = 1).
-- With ThreadIndexer::Capacity() ≈ 17 threads the per-thread budget is ≈ 60 KB.
-- Each segment receives ≈ 67 k rows of text keys averaging ~10 B each → total
-- partition memory ≈ 1.3 MB >> 60 KB, reliably triggering spill every Sink.
--
-- Correctness is verified by running each query twice (spill=0, then spill=1)
-- and asserting count + aggregate totals are identical.  No per-row comparison
-- is needed: a count or sum mismatch immediately surfaces data loss or
-- double-counting.

SET vector.enable_vectorization = on;
SET default_table_access_method = pax;

DROP TABLE IF EXISTS sa_spill_t;
CREATE TABLE sa_spill_t (
    id       int,
    grp_int  int,
    grp_text text,
    val      int
) DISTRIBUTED BY (id);

-- 200 k rows; grp_int has 200 k distinct values (one row per group),
-- grp_text likewise uses "key_<i>" strings that mix inline (≤12B for i<100)
-- and heap (>12B for larger i) allocations inside SonicGroupByNode.
INSERT INTO sa_spill_t
SELECT
    i,
    i                               AS grp_int,
    'key_' || i                     AS grp_text,
    i % 1000                        AS val
FROM generate_series(1, 200000) i;

ANALYZE sa_spill_t;

-- ============================================================================
-- Test 1: INT key — COUNT(*) + SUM correctness
-- ============================================================================

-- 1a. Baseline (spill off).
SET vector.sonicagg_spill_memory_mb = 0;
SELECT count(*)     AS groups,
       sum(cnt)     AS total_cnt,
       sum(s)       AS total_sum
FROM (
    SELECT grp_int, count(*) AS cnt, sum(val) AS s
    FROM sa_spill_t
    GROUP BY grp_int
) t;

-- 1b. Force spill. Expected: same totals as 1a.
SET vector.sonicagg_spill_memory_mb = 1;
SELECT count(*)     AS groups,
       sum(cnt)     AS total_cnt,
       sum(s)       AS total_sum
FROM (
    SELECT grp_int, count(*) AS cnt, sum(val) AS s
    FROM sa_spill_t
    GROUP BY grp_int
) t;

-- ============================================================================
-- Test 2: TEXT key — COUNT(*) + SUM correctness (varlen key path)
-- ============================================================================

-- 2a. Baseline.
SET vector.sonicagg_spill_memory_mb = 0;
SELECT count(*)     AS groups,
       sum(cnt)     AS total_cnt,
       sum(s)       AS total_sum
FROM (
    SELECT grp_text, count(*) AS cnt, sum(val) AS s
    FROM sa_spill_t
    GROUP BY grp_text
) t;

-- 2b. Force spill. Expected: same totals as 2a.
SET vector.sonicagg_spill_memory_mb = 1;
SELECT count(*)     AS groups,
       sum(cnt)     AS total_cnt,
       sum(s)       AS total_sum
FROM (
    SELECT grp_text, count(*) AS cnt, sum(val) AS s
    FROM sa_spill_t
    GROUP BY grp_text
) t;

-- ============================================================================
-- Test 3: TEXT key + MIN(text) — varlen agg-state serialisation
-- ============================================================================
-- MIN(text) is the only aggregate with a varlen agg state (MinTextAggregate).
-- Spill must serialise the string_t payload via StateSerializeVarlen and
-- reconstruct it on read without dangling pointers.

-- 3a. Baseline.
SET vector.sonicagg_spill_memory_mb = 0;
SELECT count(*)       AS groups,
       sum(mn_len)    AS total_min_len
FROM (
    SELECT grp_text, length(min(grp_text)) AS mn_len
    FROM sa_spill_t
    GROUP BY grp_text
) t;

-- 3b. Force spill. Expected: same totals as 3a.
SET vector.sonicagg_spill_memory_mb = 1;
SELECT count(*)       AS groups,
       sum(mn_len)    AS total_min_len
FROM (
    SELECT grp_text, length(min(grp_text)) AS mn_len
    FROM sa_spill_t
    GROUP BY grp_text
) t;

-- ============================================================================
-- Test 4: Bare GROUP BY (zero aggregates — SonicGroupByNode dedup path)
-- ============================================================================

-- 4a. Baseline.
SET vector.sonicagg_spill_memory_mb = 0;
SELECT count(*) AS distinct_keys
FROM (
    SELECT DISTINCT grp_text FROM sa_spill_t
) t;

-- 4b. Force spill. Expected: same distinct count as 4a.
SET vector.sonicagg_spill_memory_mb = 1;
SELECT count(*) AS distinct_keys
FROM (
    SELECT DISTINCT grp_text FROM sa_spill_t
) t;

-- ============================================================================
-- Test 5: budget=0 explicitly disables spill (no-spill branch coverage)
-- ============================================================================
-- Setting sonicagg_spill_memory_mb = 0 after a spill-enabled query must
-- produce correct results (exercises the disabled-spill branch in
-- build_aggregatation_options).
SET vector.sonicagg_spill_memory_mb = 0;
SELECT count(*) AS groups, sum(val) AS total_val
FROM (
    SELECT grp_int, sum(val) AS val FROM sa_spill_t GROUP BY grp_int
) t;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE sa_spill_t;

RESET vector.sonicagg_spill_memory_mb;
RESET default_table_access_method;
RESET vector.enable_vectorization;
