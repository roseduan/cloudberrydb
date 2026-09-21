--
-- Tests for GROUP BY key elimination optimization
-- (remove_derived_groupby_exprs in planner.c)
--
-- Controlled by enable_expr_groupby_reduction GUC (default off).
-- This optimization removes GROUP BY expressions that are derived from
-- other simple-Var GROUP BY columns via immutable functions.
--

CREATE TABLE groupby_elim_t (
    x integer,
    y integer
) DISTRIBUTED BY (x);

INSERT INTO groupby_elim_t VALUES
    (1, 10), (2, 20), (3, 30),
    (1, 10), (2, 20), (NULL, NULL);

ANALYZE groupby_elim_t;

-- ============================================================
-- Positive cases: should eliminate derived GROUP BY keys
-- ============================================================
SET enable_expr_groupby_reduction = on;

-- Case 1: GROUP BY x, x+1 -> GROUP BY x
EXPLAIN (COSTS OFF) SELECT x, x+1, count(*) FROM groupby_elim_t GROUP BY x, x+1;
SELECT x, x+1, count(*) FROM groupby_elim_t GROUP BY x, x+1 ORDER BY x;

-- Case 2: GROUP BY x, x-1, x-2, x-3 -> GROUP BY x (multiple derived keys)
EXPLAIN (COSTS OFF) SELECT x, x-1, x-2, x-3, count(*) FROM groupby_elim_t GROUP BY x, x-1, x-2, x-3;
SELECT x, x-1, x-2, x-3, count(*) FROM groupby_elim_t GROUP BY x, x-1, x-2, x-3 ORDER BY x;

-- Case 3: GROUP BY x, abs(x) -> GROUP BY x (immutable function)
EXPLAIN (COSTS OFF) SELECT x, abs(x), count(*) FROM groupby_elim_t GROUP BY x, abs(x);
SELECT x, abs(x), count(*) FROM groupby_elim_t GROUP BY x, abs(x) ORDER BY x;

-- Case 4: GROUP BY x, abs(x+1) -> GROUP BY x (nested immutable)
EXPLAIN (COSTS OFF) SELECT x, abs(x+1), count(*) FROM groupby_elim_t GROUP BY x, abs(x+1);
SELECT x, abs(x+1), count(*) FROM groupby_elim_t GROUP BY x, abs(x+1) ORDER BY x;

-- Case 5: GROUP BY a, b, a+b -> GROUP BY a, b (multi-column expression)
EXPLAIN (COSTS OFF) SELECT x, y, x+y, count(*) FROM groupby_elim_t GROUP BY x, y, x+y;
SELECT x, y, x+y, count(*) FROM groupby_elim_t GROUP BY x, y, x+y ORDER BY x, y;

-- Case 6: GROUP BY x, x::bigint -> GROUP BY x (immutable cast)
EXPLAIN (COSTS OFF) SELECT x, x::bigint AS x_big, count(*) FROM groupby_elim_t GROUP BY x, x::bigint;
SELECT x, x::bigint AS x_big, count(*) FROM groupby_elim_t GROUP BY x, x::bigint ORDER BY x;

-- Case 7: GROUP BY x, 1+2 -> GROUP BY x (constant expression)
EXPLAIN (COSTS OFF) SELECT x, 1+2, count(*) FROM groupby_elim_t GROUP BY x, 1+2;
SELECT x, 1+2, count(*) FROM groupby_elim_t GROUP BY x, 1+2 ORDER BY x;

-- Case 8: GROUP BY x, x+1, y, y*2 -> GROUP BY x, y (mixed columns)
EXPLAIN (COSTS OFF) SELECT x, x+1, y, y*2, count(*) FROM groupby_elim_t GROUP BY x, x+1, y, y*2;
SELECT x, x+1, y, y*2, count(*) FROM groupby_elim_t GROUP BY x, x+1, y, y*2 ORDER BY x, y;

-- ============================================================
-- Negative cases: should NOT eliminate
-- ============================================================

-- Case 9: VOLATILE function -- must not eliminate random()
EXPLAIN (COSTS OFF) SELECT x, count(*) FROM groupby_elim_t GROUP BY x, random();

-- Case 10: y not in GROUP BY -- must not eliminate x*y
EXPLAIN (COSTS OFF) SELECT x, x*y, count(*) FROM groupby_elim_t GROUP BY x, x*y;

-- Case 11: no bare Var for x -- must not eliminate x+1
EXPLAIN (COSTS OFF) SELECT abs(x), x+1, count(*) FROM groupby_elim_t GROUP BY abs(x), x+1;

-- Case 12: no bare Var at all -- must not eliminate anything
EXPLAIN (COSTS OFF) SELECT x+1, x+2, count(*) FROM groupby_elim_t GROUP BY x+1, x+2;

-- Case 13: ROLLUP -- skip optimization entirely
EXPLAIN (COSTS OFF) SELECT x, x+1, count(*) FROM groupby_elim_t GROUP BY ROLLUP(x, x+1);

-- Case 14: single key -- nothing to eliminate
EXPLAIN (COSTS OFF) SELECT x, count(*) FROM groupby_elim_t GROUP BY x;

-- Case 15: pure SubLink -- must not be eliminated.
-- pull_var_clause() does not recurse into SubLink subselects, so without the
-- contain_sublinks() guard the expression would be misclassified as a
-- constant (empty Var list) and dropped from GROUP BY.
EXPLAIN (COSTS OFF) SELECT x, (SELECT 1), count(*) FROM groupby_elim_t GROUP BY x, (SELECT 1);

-- Case 16: SubLink with correlated reference to a non-group-by column --
-- must not be eliminated.  The correlated Var (t1.y) is invisible to
-- pull_var_clause; without the guard the expression would be wrongly
-- removed and rows differing in y would be incorrectly merged.
EXPLAIN (COSTS OFF) SELECT x, count(*) FROM groupby_elim_t t1 GROUP BY x, (SELECT t1.y);

-- ============================================================
-- Correctness: results must be identical with GUC on vs off
-- ============================================================

SET enable_expr_groupby_reduction = on;
SELECT x, x-1, x-2, count(*) AS c FROM groupby_elim_t GROUP BY x, x-1, x-2 ORDER BY x;

SET enable_expr_groupby_reduction = off;
SELECT x, x-1, x-2, count(*) AS c FROM groupby_elim_t GROUP BY x, x-1, x-2 ORDER BY x;

-- ============================================================
-- GUC off: optimization must not trigger
-- ============================================================

SET enable_expr_groupby_reduction = off;
EXPLAIN (COSTS OFF) SELECT x, x+1, count(*) FROM groupby_elim_t GROUP BY x, x+1;

-- Cleanup
RESET enable_expr_groupby_reduction;
DROP TABLE groupby_elim_t;
