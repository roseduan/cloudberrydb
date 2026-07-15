-- Test WindowHashAgg with an empty input (all rows filtered out on a segment)
-- when the window aggregate's output type is resolved at runtime (COMPUTED),
-- e.g. sum()/avg() over numeric/decimal columns.
--
-- Regression for issue 394: Arrow's ParallelWindowGroupByNode::CreateEmptyResultBatch
-- used to call OutputType::type(), which is only valid for ResolveKind::FIXED
-- kernels. For ResolveKind::COMPUTED kernels (numeric/decimal sum/avg) it
-- returned a null type, and MakeArrayOfNull(nullptr, 0) crashed (SIGSEGV in a
-- release build, or an Arrow "Check failed: (FIXED) == (kind_)" assertion
-- abort in a debug/cassert build). The fix reads the already-resolved type
-- from the node's output schema instead. This coredumped TPC-DS 1TB Q12 on a
-- real cluster; the filter below reproduces the same zero-row condition
-- deterministically on a small table, regardless of segment count.

SET vector.enable_vectorization = on;
SET optimizer = on;
SET optimizer_force_window_hash_agg = on;

-- Use PAX storage (required for vectorization)
SET default_table_access_method = pax;

CREATE TABLE winagg_empty_t1 (
    k int,
    amt numeric(10,2)
) DISTRIBUTED BY (k);

INSERT INTO winagg_empty_t1 VALUES (1, 100.00), (2, 200.00), (3, 300.00);

ANALYZE winagg_empty_t1;

-- ============================================================================
-- Test 1: predicate matches zero rows -> every segment's WindowHashAgg
-- instance receives zero input rows, exercising CreateEmptyResultBatch with a
-- COMPUTED-output aggregate (sum on numeric). Must return an empty result set
-- without crashing.
-- ============================================================================

SELECT k, sum(amt) OVER (PARTITION BY k) FROM winagg_empty_t1 WHERE amt > 999999;

-- Same shape with avg(), also a COMPUTED-output kernel over numeric.

SELECT k, avg(amt) OVER (PARTITION BY k) FROM winagg_empty_t1 WHERE amt > 999999;

-- ============================================================================
-- Test 2: sanity check - non-empty input still computes the correct result.
-- Guards against a fix that merely special-cases the empty path.
-- ============================================================================

SELECT k, sum(amt) OVER (PARTITION BY k) FROM winagg_empty_t1 ORDER BY k;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE winagg_empty_t1;

RESET optimizer_force_window_hash_agg;
RESET default_table_access_method;
RESET optimizer;
RESET vector.enable_vectorization;
