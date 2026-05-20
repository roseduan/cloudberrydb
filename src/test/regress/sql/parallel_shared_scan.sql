--
-- Tests for parallel SharedInputScan lifecycle management.
--
-- Validates that in parallel mode:
-- 1. Within-slice parallel ShareInputScan works correctly
-- 2. Cross-slice parallel ShareInputScan with a single consumer works
-- 3. Cross-slice parallel ShareInputScan with multiple consumers works
-- 4. DSM segment cleanup is coordinated: the producer waits for all
--    consumer slices, and within a consumer slice only the last worker
--    unpins the DSM.
--

CREATE SCHEMA parallel_shared_scan;
SET search_path = parallel_shared_scan;

SET optimizer = on;
SET enable_parallel = on;

CREATE TABLE t1 (a int, b float4) DISTRIBUTED BY (a);
INSERT INTO t1 SELECT i, i * 1.1 FROM generate_series(1, 100) i;
ANALYZE t1;

-- Larger table to increase chance of hitting parallel race conditions
CREATE TABLE t2 (a int, b float4) DISTRIBUTED BY (a);
INSERT INTO t2 SELECT i, i * 0.7 FROM generate_series(1, 5000) i;
ANALYZE t2;

-- ============================================================
-- Test 1: Within-slice parallel ShareInputScan
-- CTE referenced multiple times in the same slice.
-- ============================================================

EXPLAIN (COSTS OFF)
WITH cte AS (SELECT a, sum(b) AS total FROM t1 GROUP BY a)
SELECT count(*) FROM (
    SELECT c1.a FROM cte c1 JOIN cte c2 ON c1.a = c2.a
) sub;

WITH cte AS (SELECT a, sum(b) AS total FROM t1 GROUP BY a)
SELECT count(*) FROM (
    SELECT c1.a FROM cte c1 JOIN cte c2 ON c1.a = c2.a
) sub;

-- ============================================================
-- Test 2: Cross-slice with single consumer
-- percentile_cont forces a cross-slice ShareInputScan.
-- ============================================================

EXPLAIN (COSTS OFF)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b) FROM t1;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b) FROM t1;

-- ============================================================
-- Test 3: Cross-slice with multiple consumers
-- percentile_cont + sum creates multiple consumer slices for
-- the same shared scan.
-- ============================================================

-- sum(b::numeric)::int: float4 sum is reduce-order dependent under parallel mode.
EXPLAIN (COSTS OFF)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b),
       sum(b::numeric)::int AS sum
FROM t1;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b),
       sum(b::numeric)::int AS sum
FROM t1;

-- ============================================================
-- Test 4: Stress test — repeated execution to catch race conditions.
-- The original bug was intermittent (~20% failure rate).
-- ============================================================

-- Cross-slice multiple consumers (stress)
DO $$
BEGIN
  FOR i IN 1..50 LOOP
    PERFORM percentile_cont(0.5) WITHIN GROUP (ORDER BY b), sum(b) FROM parallel_shared_scan.t1;
  END LOOP;
END;
$$;

-- Cross-slice single consumer (stress)
DO $$
BEGIN
  FOR i IN 1..50 LOOP
    PERFORM percentile_cont(0.5) WITHIN GROUP (ORDER BY b) FROM parallel_shared_scan.t1;
  END LOOP;
END;
$$;

-- Within-slice (stress)
DO $$
BEGIN
  FOR i IN 1..50 LOOP
    PERFORM count(*) FROM (
      WITH cte AS (SELECT a, sum(b) AS total FROM parallel_shared_scan.t1 GROUP BY a)
      SELECT c1.a FROM cte c1 JOIN cte c2 ON c1.a = c2.a
    ) sub;
  END LOOP;
END;
$$;

-- ============================================================
-- Test 5: Cross-slice multiple consumers with larger dataset
-- ============================================================

EXPLAIN (COSTS OFF)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b),
       sum(b::numeric)::int AS sum
FROM t2;

SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b),
       sum(b::numeric)::int AS sum
FROM t2;

DO $$
BEGIN
  FOR i IN 1..30 LOOP
    PERFORM percentile_cont(0.5) WITHIN GROUP (ORDER BY b), sum(b) FROM parallel_shared_scan.t2;
  END LOOP;
END;
$$;

-- ============================================================
-- Cleanup
-- ============================================================

DROP SCHEMA parallel_shared_scan CASCADE;
RESET optimizer;
RESET enable_parallel;
