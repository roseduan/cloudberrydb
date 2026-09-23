SET default_table_access_method=ao_column;

CREATE TABLE test_agg_stddev(
	i int,
	x int);

INSERT INTO test_agg_stddev(i, x)
SELECT n, n
FROM generate_series(1, 20) AS s(n);

ANALYZE test_agg_stddev;

explain SELECT stddev_samp(i) FROM test_agg_stddev;
explain SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i ;
explain SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i GROUP BY a.x;

-- MREGE3X_FIXME: This sql record a error
SELECT stddev_samp(i) FROM test_agg_stddev;
SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i ;

INSERT INTO test_agg_stddev(i, x)
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n);

SELECT stddev_samp(a.i) AS stddev FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i GROUP BY a.x ORDER BY stddev;

-- Zero variance: every value in a group is identical, so the sample variance
-- is exactly zero.  PostgreSQL's numeric_stddev_internal() short-circuits on
-- a non-positive numerator and returns const_zero, whose dscale is 0, without
-- running the division or the square root -- so it prints a bare "0".  A
-- vectorized finalizer that runs both steps anyway gets the right value at
-- the wrong scale, which every text or md5 comparison of a result set then
-- reports as a difference.  Group 3 is the non-zero control, and group 6 has
-- a single row, where a sample stddev is undefined.
--
-- Pin the planner: under ORCA this aggregate is routed through a
-- double-based path that diverges from row mode on its own (it reports
-- 0.00000000000000000 here and 1.0000000000000000 for group 3, both before
-- and after the numeric128 finalizers were corrected).  That divergence is
-- a separate defect; keeping it out of this case makes the case a
-- deterministic check of the numeric128 path.
SET optimizer = off;

CREATE TABLE test_agg_stddev_zero(g int, v int) DISTRIBUTED BY (g);

INSERT INTO test_agg_stddev_zero
            SELECT 1, 5          FROM generate_series(1, 3)
  UNION ALL SELECT 2, 7          FROM generate_series(1, 2)
  UNION ALL SELECT 3, n          FROM generate_series(1, 3) AS s(n)
  UNION ALL SELECT 4, 2000000000 FROM generate_series(1, 3)
  UNION ALL SELECT 5, -13        FROM generate_series(1, 3)
  UNION ALL SELECT 6, 42;

ANALYZE test_agg_stddev_zero;

explain (costs off) SELECT g, stddev_samp(v) FROM test_agg_stddev_zero GROUP BY g;
SELECT g, stddev_samp(v) FROM test_agg_stddev_zero GROUP BY g ORDER BY g;

-- Drop it: sanity_check enumerates every table in the database.
DROP TABLE test_agg_stddev_zero;
