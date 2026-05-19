-- Regression test for rewrite_tl_keys: when a Vec GroupAggregate's
-- targetList contains only Const entries (e.g. GPORCA pruned all the
-- group-by columns out of the SELECT list and left only a literal),
-- the old code zeroed pcontext->keys/nkey and the agg silently routed
-- to Arrow's ScalarAggregateNode with 0 kernels, which hard-codes the
-- output to ExecBatch{values={}, length=1}. Per-segment count then
-- collapsed to 1, producing wrong totals.
--
-- See execMain.c::rewrite_tl_keys.

SET vector.enable_vectorization = on;
SET optimizer = on;        -- GPORCA is required to trigger the Const-only output pruning

DROP TABLE IF EXISTS vec_ss_t_a;
DROP TABLE IF EXISTS vec_ss_t_b;
CREATE TABLE vec_ss_t_a (k int) USING pax;
CREATE TABLE vec_ss_t_b (k int) USING pax;
INSERT INTO vec_ss_t_a SELECT generate_series(1, 100);
INSERT INTO vec_ss_t_b SELECT generate_series(1, 200);
ANALYZE vec_ss_t_a;
ANALYZE vec_ss_t_b;

-- Case 1: original reproducer (Append + Const-only children).
-- Expected: A=100, B=200 (count of distinct k values per side).
SELECT label, count(*) AS n
  FROM (
    SELECT 'A' AS label, k FROM vec_ss_t_a GROUP BY k
    UNION ALL
    SELECT 'B' AS label, k FROM vec_ss_t_b GROUP BY k
  ) x
  GROUP BY label
  ORDER BY label;

-- Case 2: minimal reproducer without Append (single Const-only child).
-- Expected: A=100.
SELECT label, count(*) AS n
  FROM (SELECT 'A' AS label, k FROM vec_ss_t_a GROUP BY k) x
  GROUP BY label;

-- Case 3: Var-only targetList (case2 in rewrite_tl_keys header comment).
-- Regression check: must still return 3 sorted rows, not collapse.
SELECT k FROM vec_ss_t_a GROUP BY k ORDER BY k LIMIT 3;

-- Case 4: mixed Const + Var targetList.
-- Regression check: Const broadcast + group-by must both work.
SELECT 'X' AS lbl, k FROM vec_ss_t_a GROUP BY k ORDER BY k LIMIT 3;

-- Case 5: GROUP BY with a real aggref (not affected by rewrite_tl_keys
-- since the call site is gated on !aggInfos, but kept as a sanity guard).
SELECT k, count(*) FROM vec_ss_t_a GROUP BY k ORDER BY k LIMIT 3;

-- Case 6: nested UNION ALL where the outer agg uses a Var (sum(k)) so
-- GPORCA cannot prune k from the children. Expected sums:
--   A: 1+2+...+100  = 5050
--   B: 1+2+...+200  = 20100
SELECT label, sum(k) AS s
  FROM (
    SELECT 'A' AS label, k FROM vec_ss_t_a GROUP BY k
    UNION ALL
    SELECT 'B' AS label, k FROM vec_ss_t_b GROUP BY k
  ) x
  GROUP BY label
  ORDER BY label;

DROP TABLE vec_ss_t_a;
DROP TABLE vec_ss_t_b;
RESET vector.enable_vectorization;
RESET optimizer;
