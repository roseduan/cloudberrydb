-- Regression test for the Sonic hash aggregate routing in the
-- vectorization engine.  When an AGG_HASHED node has a sonic-compatible
-- shape (key types + agg funcs all supported, no Limit+HashAgg fusion),
-- build_aggregatation_options routes it to Arrow's SonicGroupByNode
-- (compute/sonic/exec_node.cc); otherwise it falls back to the legacy
-- normal-mode Arrow GroupByNode.  Coverage:
--   contrib/vectorization/src/backend/vecexecutor/execMain.c
--     (is_sonic_compatible + build_aggregatation_options branch select)
--   contrib/vectorization/src/backend/hook/explain.c
--     (show_hashagg_info Vec HashAgg Method line)
--
-- The "Vec HashAgg Method: Sonic | Normal | Limit+HashAgg" line in
-- EXPLAIN documents the routing decision per HashAgg node.  This test
-- asserts the routing for the supported / unsupported corners of
-- is_sonic_compatible, leaning on planner (optimizer = off) for plan
-- shape stability.  Correctness is also covered: sonic-routed sums /
-- avgs / mins must match the straightforward SQL reference.
SET vector.enable_vectorization = on;
SET default_table_access_method = pax;
SET optimizer = off;
DROP SCHEMA IF EXISTS vec_sonic CASCADE;
CREATE SCHEMA vec_sonic;
SET search_path = vec_sonic;

-- 5 distinct group keys, 100 rows per group.  Each row touches every
-- column type is_sonic_compatible inspects.  c1 ("char"), b (bool),
-- f4 (real / float4) and f8 (float8) exercise the INT8 / BOOL /
-- FLOAT32 / FLOAT64 key paths added on top of the original
-- INT16/INT32/INT64/STRING/NUMERIC128/DATE/TIME set.
CREATE TABLE sn (
    i32  int,
    i64  bigint,
    n    numeric(18,2),
    f4   float4,
    f8   float8,
    b    bool,
    c1   "char",
    t    text,
    d    date,
    ts   timestamp
) DISTRIBUTED BY (i32);
INSERT INTO sn SELECT
    i % 5,
    (i % 5)::bigint * 10 + (i / 5),
    ((i % 5) * 100 + (i / 5))::numeric(18,2),
    (i % 5 + 0.5)::float4,
    (i % 5 + 0.5)::float8,
    i % 2 = 0,
    chr(65 + i % 5)::"char",
    chr(65 + i % 5),
    date '2026-01-01' + (i % 5),
    timestamp '2026-01-01 00:00:00' + ((i % 5) * interval '1 hour')
FROM generate_series(1, 500) i;
ANALYZE sn;

-- ------------------------------------------------------------------
-- A) Sonic routing fires (expect "Vec HashAgg Method:  Sonic")
-- ------------------------------------------------------------------

-- A1) count(*) on int32 key -- hash_count reads no input column.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, count(*) FROM sn GROUP BY i32;

-- A2) sum(int8) -- two-stage: hash_sum_64 on segments, hash_sum on QD.
--     Both HashAgg nodes should be sonic-routed.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, sum(i64) FROM sn GROUP BY i32;

-- A3) sum(numeric) -- routes to SumNumeric128Aggregate.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, sum(n) FROM sn GROUP BY i32;

-- A4) avg(int8) -- hash_avg_trans on segments, hash_avg_final on QD.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, avg(i64) FROM sn GROUP BY i32;

-- A5) GROUP BY text -- sonic_supports_key_type(STRING).
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT t, count(*) FROM sn GROUP BY t;

-- A6) GROUP BY date -- DATE32 in sonic_supports_key_type.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT d, count(*) FROM sn GROUP BY d;

-- A7) GROUP BY timestamp -- TIMESTAMP in sonic_supports_key_type.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT ts, count(*) FROM sn GROUP BY ts;

-- A8) min(text) -- only hash_min variant sonic implements.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(t) FROM sn GROUP BY i32;

-- A9) GROUP BY bool -- KeyType::BOOL (bit-packed input / output, stored
--     as 1 byte in the row).  Two stages because b is not the
--     distribution key.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT b, count(*) FROM sn GROUP BY b;

-- A10) GROUP BY "char" -- KeyType::INT8.  Two stages because c1 is not
--      the distribution key.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT c1, count(*) FROM sn GROUP BY c1;

-- A11) GROUP BY float4 -- KeyType::FLOAT32.  Canonicalisation of NaN
--      payloads and -0.0 happens at Scatter / ComputeHashes /
--      RowMatcher LHS materialisation; for ordinary values like these
--      the routing is the only thing to verify here, the adversarial
--      NaN / ±0.0 / ±Inf behaviour is checked in section C.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT f4, count(*) FROM sn GROUP BY f4;

-- A12) GROUP BY float8 -- KeyType::FLOAT64.  Same canonicalisation
--      story as float4.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT f8, count(*) FROM sn GROUP BY f8;

-- A13) Bare GROUP BY with no aggregate.  BuildAggregatation would
--      synthesise a hash_distinct placeholder for the normal-mode path,
--      but SonicGroupByNode handles zero aggregates natively (it just
--      deduplicates the keys), so is_sonic_compatible accepts the
--      aggInfos == NIL case and the placeholder is skipped.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32 FROM sn GROUP BY i32;

-- A14) min(int8) + max(int8) -- MinMaxAggregate<int64_t>. Both stages are
--      hash_min / hash_max (output type == input type), so the two-stage
--      MPP merge is min/max-of-partials.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(i64), max(i64) FROM sn GROUP BY i32;

-- A15) min(numeric) + max(numeric) -- MinMaxAggregate<Numeric128>.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(n), max(n) FROM sn GROUP BY i32;

-- A16) min(date) + max(date) -- MinMaxAggregate<int32_t> with date32 output.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(d), max(d) FROM sn GROUP BY i32;

-- A17) min(float8) + max(float8) -- MinMaxAggregate<double> with PG's
--      NaN-is-greatest total order.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(f8), max(f8) FROM sn GROUP BY i32;

-- A18) sum(float8) -- SumDoubleAggregate. F_SUM_FLOAT8 maps to "sum" for
--      partial / final / single stage alike, output type == input type,
--      so one aggregate serves all stages. GROUP BY the distribution key
--      (i32) keeps this single-stage.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, sum(f8) FROM sn GROUP BY i32;

-- A19) avg(float8) -- single-stage (GROUP BY distribution key i32) maps
--      to "mean" -> MeanDoubleAggregate (double -> float64). The
--      two-stage form (avg_trans double -> struct<double,count> ->
--      avg_final) is exercised by the correctness case in section C.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, avg(f8) FROM sn GROUP BY i32;

-- A20) avg(numeric) -- single-stage AVG(numeric) maps to
--      hash_mean_numeric on a numeric128 input.  Sonic implements this
--      via AvgNumeric128Aggregate's NUMERIC128 InputKind: values are
--      read directly as Numeric128 (no integer widening) and the
--      final sum/count is divided in Numeric128.  GROUP BY i32 (the
--      distribution key) keeps the plan single-stage.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, avg(n) FROM sn GROUP BY i32;

-- Multi-column GROUP BY mixing INT8 + BOOL keys is exercised only via
-- the correctness query in section C below (its EXPLAIN's Redistribute
-- Hash Key choice depends on planner cost decisions and is too brittle
-- to assert here).

-- ------------------------------------------------------------------
-- C) Correctness: sonic-routed aggregates must match the reference.
-- ------------------------------------------------------------------

-- Reference: straightforward grouped aggregate, ordered for stable diff.
-- a_n: AVG(numeric) -- single-stage hash_mean_numeric on numeric128,
-- routed to Sonic via the NUMERIC128 InputKind.
SELECT i32,
       count(*)             AS c,
       sum(i64)             AS s_i,
       sum(n)               AS s_n,
       avg(i64)::numeric(20,4) AS a_i,
       avg(n)::numeric(20,4)   AS a_n,
       min(t)               AS mn_t
FROM sn
GROUP BY i32
ORDER BY i32;

-- MIN / MAX correctness across every pushed value type (int8, numeric,
-- date, float8).  These route to MinMaxAggregate<T>; an all-NULL group
-- would finalize to NULL, but sn has no nulls here -- the NULL path is
-- covered by the bare GROUP BY / adversarial-float cases below.
SELECT i32,
       min(i64) AS mn_i, max(i64) AS mx_i,
       min(n)   AS mn_n, max(n)   AS mx_n,
       min(d)   AS mn_d, max(d)   AS mx_d,
       min(f8)  AS mn_f, max(f8)  AS mx_f
FROM sn
GROUP BY i32
ORDER BY i32;

-- Two-stage SUM(numeric) correctness. GROUP BY a non-distribution column
-- (b, bool) forces a redistribute, so this is the two-stage shape: the
-- partial is hash_avg_trans on numeric (struct<numeric128, int64>,
-- already on Sonic) and the final is hash_sum_final
-- (SumFinalNumeric128Aggregate) — both route to Sonic. sum_final emits
-- the accumulated sum (not sum/count). Verified vs the reference.
SELECT b, sum(n) AS s_n
FROM sn
GROUP BY b
ORDER BY b;

-- GROUP BY text correctness.
SELECT t, count(*) AS c
FROM sn
GROUP BY t
ORDER BY t;

-- GROUP BY date correctness.
SELECT d, count(*) AS c, sum(i64) AS s
FROM sn
GROUP BY d
ORDER BY d;

-- GROUP BY bool correctness (KeyType::BOOL).
SELECT b, count(*) AS c
FROM sn
GROUP BY b
ORDER BY b;

-- GROUP BY "char" correctness (KeyType::INT8).
SELECT c1, count(*) AS c
FROM sn
GROUP BY c1
ORDER BY c1;

-- Multi-key (bool, "char") correctness: 2 * 5 = 10 groups, 50 rows each.
SELECT b, c1, count(*) AS c
FROM sn
GROUP BY b, c1
ORDER BY b, c1;

-- GROUP BY float4 correctness (KeyType::FLOAT32).
SELECT f4, count(*) AS c
FROM sn
GROUP BY f4
ORDER BY f4;

-- GROUP BY float8 correctness (KeyType::FLOAT64).
SELECT f8, count(*) AS c
FROM sn
GROUP BY f8
ORDER BY f8;

-- Adversarial float8 correctness: verifies that Sonic's canonicalisation
-- (NaN payloads collapsed, -0.0 → +0.0) matches PG's GROUP BY semantic
-- on float types.  PG itself treats NaN = NaN and -0.0 = +0.0 in the
-- float8 equality / hash operator family, so the routed Sonic result
-- must agree with the reference.  +Inf and -Inf remain distinct groups.
CREATE TABLE sn_float (v float8) DISTRIBUTED RANDOMLY;
INSERT INTO sn_float VALUES
    ('NaN'), ('NaN'),                 -- 2 NaN rows -> one group
    (0.0), (-0.0), (0.0),             -- 3 zero rows  -> one group
    ('Infinity'), ('Infinity'),       -- 2 +Inf rows -> one group
    ('-Infinity'),                    -- 1 -Inf      -> distinct group
    (1.5), (1.5), (1.5);              -- 3 ordinary  -> one group
ANALYZE sn_float;

SELECT v, count(*) AS c
FROM sn_float
GROUP BY v
ORDER BY v;

-- Adversarial MIN / MAX over float8: MinMaxAggregate<double> must use PG's
-- total order where NaN is the greatest value.  Per group g:
--   1: {NaN,1,2,+Inf,-Inf} -> min=-Inf, max=NaN
--   2: {1,2,+Inf}          -> min=1,    max=+Inf
--   3: {NaN,NaN}           -> min=NaN,  max=NaN
--   4: {NULL,5,NULL}       -> min=max=5 (NULLs skipped)
--   5: {NULL,NULL}         -> min=max=NULL (all-NULL group)
CREATE TABLE sn_minmax_f (g int, v float8) DISTRIBUTED RANDOMLY;
INSERT INTO sn_minmax_f VALUES
    (1,'NaN'),(1,1),(1,2),(1,'Infinity'),(1,'-Infinity'),
    (2,1),(2,2),(2,'Infinity'),
    (3,'NaN'),(3,'NaN'),
    (4,NULL),(4,5),(4,NULL),
    (5,NULL),(5,NULL);
ANALYZE sn_minmax_f;

SELECT g, min(v) AS mn, max(v) AS mx
FROM sn_minmax_f
GROUP BY g
ORDER BY g;

-- SUM(float8) correctness (SumDoubleAggregate), incl. NULL skipping, an
-- all-NULL group (-> NULL), and NaN / +Inf propagation through IEEE
-- addition (matching PG's float8 SUM). Per group g:
--   1: {1.5, 2.5, NULL}    -> 4.0   (NULL skipped)
--   2: {NaN, 1.0}          -> NaN   (NaN propagates)
--   3: {'Infinity', 1.0}   -> Inf
--   4: {NULL, NULL}        -> NULL  (all-NULL group)
CREATE TABLE sn_sum_f (g int, v float8) DISTRIBUTED RANDOMLY;
INSERT INTO sn_sum_f VALUES
    (1,1.5),(1,2.5),(1,NULL),
    (2,'NaN'),(2,1.0),
    (3,'Infinity'),(3,1.0),
    (4,NULL),(4,NULL);
ANALYZE sn_sum_f;

SELECT g, sum(v) AS s
FROM sn_sum_f
GROUP BY g
ORDER BY g;

-- AVG(float8) over a RANDOMLY-distributed table grouped by a non-key
-- column is a two-stage plan: partial hash_avg_trans (double ->
-- struct<double,count>, AvgTransDoubleAggregate) + final hash_avg_final
-- (struct<double,count> -> float64, AvgFinalDoubleAggregate). Both route
-- to Sonic. Correctness incl. NaN / +Inf propagation and an all-NULL
-- group. Per group: 1 -> 2.0, 2 -> NaN, 3 -> Inf, 4 -> NULL (all-NULL).
SELECT g, avg(v) AS a
FROM sn_sum_f
GROUP BY g
ORDER BY g;

-- STDDEV_SAMP(int4) single-stage -> hash_stddev_numeric
-- (StdDevSampNumericAggregate -> Sonic). GROUP BY the distribution key
-- (g) keeps it single-stage. Output is numeric128 (Arrow engine);
-- clean integer-result groups are used so the value matches PG exactly
-- (Sonic replicates Arrow's double->numeric128 path, which can differ
-- from PG in trailing precision on irrational results -- a pre-existing
-- vec-engine characteristic). Per group: 1 {2,4,6} -> 2, 2 {10,12,14}
-- -> 2, 3 {5} -> NULL (n<2), 4 {7,7,7} -> 0.
CREATE TABLE sn_stddev (g int, v int) DISTRIBUTED BY (g);
INSERT INTO sn_stddev VALUES
    (1,2),(1,4),(1,6),
    (2,10),(2,12),(2,14),
    (3,5),
    (4,7),(4,7),(4,7);
ANALYZE sn_stddev;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT g, stddev_samp(v) FROM sn_stddev GROUP BY g;

SELECT g, stddev_samp(v) AS sd
FROM sn_stddev
GROUP BY g
ORDER BY g;

-- Two-stage STDDEV_SAMP(int4): a low-cardinality GROUP BY over many rows
-- makes the planner pre-aggregate, so this is the two-stage shape:
--   partial hash_avg_trans_stddev (int4 -> struct<sum,count,square>,
--           AvgTransStddevAggregate)
--   final   hash_avg_final_stddev (struct<sum,count,square> -> numeric128,
--           AvgFinalStddevAggregate)
-- exercising the Sonic Struct3 framework. The final computes the stddev
-- in Numeric128, which matches PG's numeric arithmetic exactly (unlike
-- the single-stage double path above). GROUP BY a non-distribution
-- column (g) forces the redistribute.
CREATE TABLE sn_stddev2 (g int, v int) DISTRIBUTED BY (v);
INSERT INTO sn_stddev2 SELECT i % 4, i % 11 FROM generate_series(1, 8000) i;
ANALYZE sn_stddev2;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT g, stddev_samp(v) FROM sn_stddev2 GROUP BY g;

SELECT g, stddev_samp(v) AS sd
FROM sn_stddev2
GROUP BY g
ORDER BY g;

-- Bare GROUP BY (no aggregate) correctness, including a NULL group key.
-- Sonic dedups the keys via its per-row null bitmap, so an all-NULL row
-- forms its own group exactly like PG's GROUP BY.  Multi-key form mixes
-- a NULL-bearing key with a non-NULL one.
CREATE TABLE sn_distinct (a int, b text) DISTRIBUTED RANDOMLY;
INSERT INTO sn_distinct SELECT i % 4, (ARRAY['p','q',NULL])[1 + i % 3]
FROM generate_series(1, 200) i;
INSERT INTO sn_distinct VALUES (NULL, NULL);
ANALYZE sn_distinct;

SELECT a, b FROM sn_distinct GROUP BY a, b ORDER BY a, b;

-- ------------------------------------------------------------------
-- D) Sonic + Limit+HashAgg fusion are mutually exclusive: when fusion
--    fires the method is Limit+HashAgg, not Sonic (sonic does not yet
--    honor AggregateNodeOptions::limit_count).
-- ------------------------------------------------------------------
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, count(*) FROM sn GROUP BY i32 LIMIT 3;

-- ------------------------------------------------------------------
-- Cleanup.
-- ------------------------------------------------------------------
SET search_path = public;
DROP SCHEMA vec_sonic CASCADE;
RESET optimizer;
RESET default_table_access_method;
RESET vector.enable_vectorization;
