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
-- column type is_sonic_compatible inspects.
CREATE TABLE sn (
    i32  int,
    i64  bigint,
    n    numeric(18,2),
    f8   float8,
    b    bool,
    t    text,
    d    date,
    ts   timestamp
) DISTRIBUTED BY (i32);
INSERT INTO sn SELECT
    i % 5,
    (i % 5)::bigint * 10 + (i / 5),
    ((i % 5) * 100 + (i / 5))::numeric(18,2),
    (i % 5 + 0.5)::float8,
    i % 2 = 0,
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

-- ------------------------------------------------------------------
-- B) Sonic-incompatible -> Normal fallback (expect "Method:  Normal")
-- ------------------------------------------------------------------

-- B1) Bare GROUP BY with no aggregate -- BuildAggregatation synthesises
--     hash_distinct, which sonic does not implement.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32 FROM sn GROUP BY i32;

-- B2) max(int) -- sonic does not implement hash_max (only hash_min).
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, max(i64) FROM sn GROUP BY i32;

-- B3) sum(float8) -- float8 input not in sonic_supports_signed_int_input.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, sum(f8) FROM sn GROUP BY i32;

-- B4) GROUP BY bool -- bool not in sonic_supports_key_type.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT b, count(*) FROM sn GROUP BY b;

-- B5) GROUP BY float8 -- float8 not in sonic_supports_key_type.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT f8, count(*) FROM sn GROUP BY f8;

-- B6) min(int) -- sonic only supports hash_min on text/binary.
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT i32, min(i64) FROM sn GROUP BY i32;

-- ------------------------------------------------------------------
-- C) Correctness: sonic-routed aggregates must match the reference.
-- ------------------------------------------------------------------

-- Reference: straightforward grouped aggregate, ordered for stable diff.
SELECT i32,
       count(*)             AS c,
       sum(i64)             AS s_i,
       sum(n)               AS s_n,
       avg(i64)::numeric(20,4) AS a_i,
       min(t)               AS mn_t
FROM sn
GROUP BY i32
ORDER BY i32;

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
