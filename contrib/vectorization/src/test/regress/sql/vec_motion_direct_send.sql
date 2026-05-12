-- Regression test for the Sonic Motion Direct-Send routing optimization
-- in the vectorization engine.  When a HASH Redistribute Motion sits
-- directly above a Sonic HashAgg, the agg pre-buckets each output
-- batch by target segment and Motion forwards each batch without
-- re-hashing.  Coverage:
--   contrib/vectorization/src/backend/vecexecutor/nodeMotion.c
--     (ExecInitVecMotion gates + hashAndSendVec_vechash fast path)
--   contrib/vectorization/src/backend/vecexecutor/execMain.c
--     (build_aggregatation_options + BuildProject passthrough rebuild)
--   contrib/vectorization/src/backend/hook/explain.c
--     (VecExplainNode "Direct Send: yes" annotation)
--
-- The "Direct Send: yes" annotation on a Redistribute Motion is the
-- user-visible signal.  Note (from the explain.c gate): the annotation
-- is an over-approximation -- it asserts only the cheap plan-shape
-- gates (HASH motion, GUC on, Agg under trivial-Result chain), not
-- the runtime gates (Case A hash-on-key, supported hash function /
-- reduce_alg).  Plans that pass the EXPLAIN gates but fail a runtime
-- gate will still execute correctly via the slow path.
SET vector.enable_vectorization = on;
SET default_table_access_method = pax;
SET optimizer = off;
DROP SCHEMA IF EXISTS vec_ds CASCADE;
CREATE SCHEMA vec_ds;
SET search_path = vec_ds;

-- 10 distinct b, 5 distinct d, distributed by a so that any GROUP BY
-- on (b) or (d) forces a Redistribute Motion above the partial agg.
CREATE TABLE t (
    a int,
    b int,
    c bigint,
    d text
) DISTRIBUTED BY (a);
INSERT INTO t SELECT i % 100, i % 10, (i % 10)::bigint, chr(65 + i % 5)
              FROM generate_series(1, 1000) i;
ANALYZE t;

-- ------------------------------------------------------------------
-- A) Direct Send fires: GUC on + HASH Redistribute Motion above
--    Sonic partial HashAgg.  Expect "Direct Send: yes" on the
--    Redistribute Motion line.
-- ------------------------------------------------------------------
SET vector.sonic_motion_direct_send = on;

-- A1) Int group key, count(*).
EXPLAIN (COSTS OFF)
SELECT b, count(*) FROM t GROUP BY b;

-- A2) Text group key, sum.
EXPLAIN (COSTS OFF)
SELECT d, sum(c) FROM t GROUP BY d;

-- A3) Bigint group key, multi-aggregate.
EXPLAIN (COSTS OFF)
SELECT c, count(*), sum(c) FROM t GROUP BY c;

-- ------------------------------------------------------------------
-- B) GUC off -> no "Direct Send" annotation, even with the same
--    plan shape.  This pins the GUC's role as the on/off switch.
-- ------------------------------------------------------------------
SET vector.sonic_motion_direct_send = off;

EXPLAIN (COSTS OFF)
SELECT b, count(*) FROM t GROUP BY b;

RESET vector.sonic_motion_direct_send;

-- ------------------------------------------------------------------
-- C) Plan shape rejects: even with GUC on, no annotation should
--    appear when the child under the HASH Motion is not an Agg.
-- ------------------------------------------------------------------
SET vector.sonic_motion_direct_send = on;

-- C1) GROUP BY distribution key (a): planner places only a Gather
--     Motion at the top, no Redistribute is needed.  The annotation
--     attaches to HASH Motions only, so it can never appear here.
EXPLAIN (COSTS OFF)
SELECT a, count(*) FROM t GROUP BY a;

-- C2) Self-join on a non-distribution key: each side gets a
--     Redistribute Motion whose child is a Seq Scan, not an Agg.
--     No annotation.
EXPLAIN (COSTS OFF)
SELECT t1.b FROM t t1 JOIN t t2 ON t1.b = t2.b;

-- ------------------------------------------------------------------
-- D) Correctness invariance: results must be identical between
--    GUC on and off, regardless of routing path.
-- ------------------------------------------------------------------

-- D1) Int group key with count(*).
SET vector.sonic_motion_direct_send = on;
SELECT b, count(*) AS n FROM t GROUP BY b ORDER BY b;

SET vector.sonic_motion_direct_send = off;
SELECT b, count(*) AS n FROM t GROUP BY b ORDER BY b;

-- D2) Text group key with sum.
SET vector.sonic_motion_direct_send = on;
SELECT d, sum(c) AS s FROM t GROUP BY d ORDER BY d;

SET vector.sonic_motion_direct_send = off;
SELECT d, sum(c) AS s FROM t GROUP BY d ORDER BY d;

-- D3) Bigint group key with multiple aggregates.
SET vector.sonic_motion_direct_send = on;
SELECT c, count(*) AS n, sum(c) AS s FROM t GROUP BY c ORDER BY c;

SET vector.sonic_motion_direct_send = off;
SELECT c, count(*) AS n, sum(c) AS s FROM t GROUP BY c ORDER BY c;

-- ------------------------------------------------------------------
-- Cleanup.
-- ------------------------------------------------------------------
RESET vector.sonic_motion_direct_send;
SET search_path = public;
DROP SCHEMA vec_ds CASCADE;
RESET optimizer;
RESET default_table_access_method;
RESET vector.enable_vectorization;
