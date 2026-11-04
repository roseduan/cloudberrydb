-- Arrow does not get the frame mode of a window: it always aggregates up to
-- the last peer of the current row, as RANGE does.  An aggregate over a ROWS
-- frame must therefore fall back, or rows that tie on the ORDER BY key get
-- the same running value.  rank() and row_number() ignore the frame and stay
-- vectorized.

SET vector.enable_vectorization = on;
SET optimizer = on;

DROP TABLE IF EXISTS vec_win_frame;
CREATE TABLE vec_win_frame (g int, o int, v int) USING pax DISTRIBUTED BY (g);
-- 2 partitions of 300 rows, each ORDER BY value shared by 3 rows.  v is
-- positive, so running sums never repeat whatever order peers come in.
INSERT INTO vec_win_frame
  SELECT i % 2, i / 6, i + 1 FROM generate_series(0, 599) i;
ANALYZE vec_win_frame;

-- Vec WindowAgg (Partition Top-K on), then Vec WindowHashAgg.
SET vector.enable_partition_topk = on;
-- ROWS: every row of a partition gets its own running count and sum.
SELECT g, count(*) AS n, count(DISTINCT c) AS distinct_c, max(c) AS max_c,
       count(DISTINCT s) AS distinct_s, max(s) AS max_s
  FROM (SELECT g, count(*) OVER w AS c, sum(v) OVER w AS s
          FROM vec_win_frame
        WINDOW w AS (PARTITION BY g ORDER BY o
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) t
 GROUP BY g ORDER BY g;
-- RANGE (the default): peers share the running count.
SELECT g, count(*) AS n, count(DISTINCT c) AS distinct_c, max(c) AS max_c
  FROM (SELECT g, count(*) OVER (PARTITION BY g ORDER BY o) AS c
          FROM vec_win_frame) t
 GROUP BY g ORDER BY g;
-- rank() and row_number() over a ROWS frame.
SELECT g, count(DISTINCT rn) AS distinct_rn, max(rn) AS max_rn,
       count(DISTINCT rk) AS distinct_rk, max(rk) AS max_rk
  FROM (SELECT g, row_number() OVER w AS rn, rank() OVER w AS rk
          FROM vec_win_frame
        WINDOW w AS (PARTITION BY g ORDER BY o
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) t
 GROUP BY g ORDER BY g;
EXPLAIN (COSTS OFF)
SELECT row_number() OVER w, rank() OVER w
  FROM vec_win_frame
WINDOW w AS (PARTITION BY g ORDER BY o
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
SET vector.enable_partition_topk = off;
-- ROWS: every row of a partition gets its own running count and sum.
SELECT g, count(*) AS n, count(DISTINCT c) AS distinct_c, max(c) AS max_c,
       count(DISTINCT s) AS distinct_s, max(s) AS max_s
  FROM (SELECT g, count(*) OVER w AS c, sum(v) OVER w AS s
          FROM vec_win_frame
        WINDOW w AS (PARTITION BY g ORDER BY o
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) t
 GROUP BY g ORDER BY g;
-- RANGE (the default): peers share the running count.
SELECT g, count(*) AS n, count(DISTINCT c) AS distinct_c, max(c) AS max_c
  FROM (SELECT g, count(*) OVER (PARTITION BY g ORDER BY o) AS c
          FROM vec_win_frame) t
 GROUP BY g ORDER BY g;
-- rank() and row_number() over a ROWS frame.
SELECT g, count(DISTINCT rn) AS distinct_rn, max(rn) AS max_rn,
       count(DISTINCT rk) AS distinct_rk, max(rk) AS max_rk
  FROM (SELECT g, row_number() OVER w AS rn, rank() OVER w AS rk
          FROM vec_win_frame
        WINDOW w AS (PARTITION BY g ORDER BY o
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) t
 GROUP BY g ORDER BY g;
EXPLAIN (COSTS OFF)
SELECT row_number() OVER w, rank() OVER w
  FROM vec_win_frame
WINDOW w AS (PARTITION BY g ORDER BY o
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

RESET vector.enable_partition_topk;
DROP TABLE vec_win_frame;
