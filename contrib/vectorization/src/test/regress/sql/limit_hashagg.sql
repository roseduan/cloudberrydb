-- Regression test for the Limit+HashAgg fusion optimization in the
-- vectorization engine.  When a Limit node sits above a vectorized
-- HashAgg (no Sort in between), build_aggregatation_options passes a
-- non-zero limit_count to Arrow's GroupByNode so it stops tracking
-- new groups after offset+count entries.  Coverage:
--   contrib/vectorization/src/backend/vecexecutor/execMain.c
--     (find_agg_parent_limit walker + build_aggregatation_options)
--   contrib/vectorization/src/backend/hook/explain.c
--     (show_hashagg_info Limit+HashAgg line)
--
-- The walker matches any Limit in the Plan tree whose outerPlan is
-- the current Agg.  Under MPP that Limit usually lives below a Gather
-- Motion (the "push limit through motion" optimization), so the
-- immediate parent Limit is segment-side, not the top of the tree.
--
-- These EXPLAINs document the plan shape we depend on as much as the
-- "Vec HashAgg Method:  Limit+HashAgg  Limit: N" line itself: if the
-- segment-side Limit ever stops being pushed, the diff will say so.
SET vector.enable_vectorization = on;
SET default_table_access_method = pax;
DROP SCHEMA IF EXISTS vec_lha CASCADE;
CREATE SCHEMA vec_lha;
SET search_path = vec_lha;

-- 5 distinct group keys, 100 rows per group.
CREATE TABLE lha_t (grp int, v int) DISTRIBUTED BY (grp);
INSERT INTO lha_t SELECT i % 5, i FROM generate_series(1, 500) i;
ANALYZE lha_t;

-- ------------------------------------------------------------------
-- Cases where fusion SHOULD fire (Limit+HashAgg line present).
-- ------------------------------------------------------------------

-- 1) Plain top-level LIMIT under ORCA.  Segment-side Limit is pushed
--    below Gather Motion and the walker finds it as the Agg's parent.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT 3;

-- 2) Same shape under the postgres planner.  Both optimizers must
--    light up fusion identically when the segment Limit is present.
SET optimizer = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT 5;
RESET optimizer;

-- 2a) LIMIT with OFFSET currently falls out of vectorization entirely.
--     Under MPP the planner rewrites the segment-side Limit as
--     count = OpExpr(int8pl, Const LIMIT, Const OFFSET), and
--     is_plan_vectorable() (planner_vec.c) rejects non-Const limit
--     expressions, so the whole plan reverts to the non-vectorized
--     path -- no "Vec" prefix, no "Vec HashAgg Method" line.  As a
--     consequence the lim_offset accumulation branch in
--     build_aggregatation_options is currently unreachable in
--     production, not just in tests: any Limit with OFFSET is filtered
--     out upstream before BuildAggregatation runs.
--
--     Pinning the non-vectorized output here documents the limitation
--     and turns a future relaxation into a deliberate decision: if
--     someone teaches is_plan_vectorable to constant-fold the OpExpr
--     (or accept OpExpr-of-Const), this expected output will change
--     and force the lim_offset path in build_aggregatation_options to
--     be reviewed and tested for real.
SET optimizer = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT 3 OFFSET 2;
RESET optimizer;

-- ------------------------------------------------------------------
-- Cases where fusion MUST NOT fire (no Limit+HashAgg line).
-- ------------------------------------------------------------------

-- 3) No LIMIT at all.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp;

-- 4) ORDER BY between Agg and Limit forces a Sort, so the Agg's
--    immediate parent in the segment slice is Sort, not Limit.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp ORDER BY grp LIMIT 3;

-- 5) LIMIT over the configured cap.  Under the planner the segment
--    Limit IS pushed (so the walker finds the parent Limit), but
--    build_aggregatation_options refuses values above
--    vector.limit_hashagg_max_total -- the per-row overhead of tracking
--    that many groups would outweigh the early stop, so fusion
--    intentionally bails out.  Default cap is 1000; we shrink it here
--    to keep the test self-contained.
SET optimizer = off;
SET vector.limit_hashagg_max_total = 100;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT 101;
RESET vector.limit_hashagg_max_total;
RESET optimizer;

-- 6) Non-constant LIMIT (prepared statement parameter) -- fusion only
--    handles Const limit/offset expressions.  Currently the prepared
--    plan also takes the non-vectorized path (no "Vec" prefix).
PREPARE lha_param AS SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT $1;
EXPLAIN (VERBOSE ON, COSTS OFF) EXECUTE lha_param(3);
DEALLOCATE lha_param;

-- 7) GUC disabled via the 0 sentinel -- the walker short-circuits
--    before it even runs.
SET vector.limit_hashagg_max_total = 0;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT grp, count(*) FROM lha_t GROUP BY grp LIMIT 3;
RESET vector.limit_hashagg_max_total;

-- ------------------------------------------------------------------
-- Correctness: fusion's truncation must not break aggregate values.
-- ------------------------------------------------------------------

-- Reference: full grouped result, ordered for stable output.
SELECT grp, count(*) AS c, sum(v) AS s
FROM lha_t
GROUP BY grp
ORDER BY grp;

-- Fusion fires (per case 1 above): exactly 3 rows are returned and
-- each row is a valid (grp, count, sum) triple from the reference.
-- Aggregated to stay deterministic across "which 3 groups land first".
SELECT count(*) AS nrows
FROM (
    SELECT grp, count(*) AS c, sum(v) AS s
    FROM lha_t
    GROUP BY grp
    LIMIT 3
) sub;

SELECT count(*) AS all_rows_valid
FROM (
    SELECT grp, count(*) AS c, sum(v) AS s
    FROM lha_t
    GROUP BY grp
    LIMIT 3
) f
JOIN (
    SELECT grp, count(*) AS c, sum(v) AS s
    FROM lha_t
    GROUP BY grp
) ref USING (grp, c, s);

-- Equivalence: with fusion ON vs OFF the result-set is the same when
-- the LIMIT does not actually clip anything (LIMIT > #groups).
SET vector.limit_hashagg_max_total = 1000;
CREATE TEMP TABLE lha_on AS
SELECT grp, count(*) AS c, sum(v) AS s
FROM lha_t
GROUP BY grp
LIMIT 100;

SET vector.limit_hashagg_max_total = 0;
CREATE TEMP TABLE lha_off AS
SELECT grp, count(*) AS c, sum(v) AS s
FROM lha_t
GROUP BY grp
LIMIT 100;
RESET vector.limit_hashagg_max_total;

SELECT
    (SELECT count(*) FROM (SELECT * FROM lha_on EXCEPT SELECT * FROM lha_off) d) AS only_in_on,
    (SELECT count(*) FROM (SELECT * FROM lha_off EXCEPT SELECT * FROM lha_on) d) AS only_in_off;

-- ------------------------------------------------------------------
-- Cleanup.
-- ------------------------------------------------------------------
DROP TABLE lha_on;
DROP TABLE lha_off;
DROP TABLE lha_t;
SET search_path = public;
DROP SCHEMA vec_lha CASCADE;
RESET default_table_access_method;
RESET vector.enable_vectorization;
