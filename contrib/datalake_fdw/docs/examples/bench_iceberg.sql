SET optimizer = off;

\echo '========================================================'
\echo '=== T2: aggregation + GROUP BY + HAVING               ==='
\echo '========================================================'
\echo '--- S3 ---'
SELECT region, count(*) AS rows, sum(qty) AS total_qty, round(avg(price),2) AS avg_price
  FROM sales_s3 GROUP BY region HAVING sum(qty) > 100 ORDER BY region;
\echo '--- HDFS ---'
SELECT region, count(*) AS rows, sum(qty) AS total_qty, round(avg(price),2) AS avg_price
  FROM sales_hdfs GROUP BY region HAVING sum(qty) > 100 ORDER BY region;

\echo '========================================================'
\echo '=== T3: multi-column GROUP BY + ORDER BY + LIMIT      ==='
\echo '========================================================'
\echo '--- S3 ---'
SELECT region, product, sum(qty * price) AS revenue
  FROM sales_s3 WHERE returned = false
  GROUP BY region, product
  ORDER BY revenue DESC LIMIT 10;
\echo '--- HDFS ---'
SELECT region, product, sum(qty * price) AS revenue
  FROM sales_hdfs WHERE returned = false
  GROUP BY region, product
  ORDER BY revenue DESC LIMIT 10;

\echo '========================================================'
\echo '=== T4: INNER JOIN — cross-backend                    ==='
\echo '========================================================'
SELECT s.region, s.product, s.qty AS s3_qty, h.qty AS hdfs_qty,
       (s.qty = h.qty) AS qty_matches
  FROM sales_s3 s JOIN sales_hdfs h ON s.id = h.id
  ORDER BY s.id LIMIT 5;

\echo '========================================================'
\echo '=== T5: LEFT JOIN + NULL handling                     ==='
\echo '========================================================'
SELECT s.id, s.region, h.product
  FROM sales_s3 s LEFT JOIN sales_hdfs h
    ON s.id = h.id AND h.returned = true
  WHERE s.id <= 20
  ORDER BY s.id;

\echo '========================================================'
\echo '=== T6: subquery (scalar + IN)                        ==='
\echo '========================================================'
SELECT id, region, price
  FROM sales_s3
  WHERE price > (SELECT avg(price) FROM sales_s3)
    AND region IN (SELECT region FROM sales_hdfs WHERE returned = true GROUP BY region)
  ORDER BY price DESC LIMIT 5;

\echo '========================================================'
\echo '=== T7: correlated subquery (EXISTS)                  ==='
\echo '========================================================'
SELECT DISTINCT s.customer
  FROM sales_s3 s
  WHERE EXISTS (
    SELECT 1 FROM sales_hdfs h
    WHERE h.customer = s.customer AND h.qty >= 9
  )
  ORDER BY s.customer LIMIT 5;

\echo '========================================================'
\echo '=== T8: Window functions (ROW_NUMBER, RANK, LAG)      ==='
\echo '========================================================'
\echo '--- S3 top 2 per region by price ---'
SELECT region, id, price, rnk FROM (
  SELECT region, id, price,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY price DESC) AS rnk
    FROM sales_s3
) x WHERE rnk <= 2 ORDER BY region, rnk;

\echo '--- HDFS running sum and LAG ---'
SELECT id, region, price,
       sum(price) OVER (PARTITION BY region ORDER BY id) AS running_sum,
       lag(price, 1, 0::numeric) OVER (PARTITION BY region ORDER BY id) AS prev_price
  FROM sales_hdfs
  WHERE id <= 20
  ORDER BY region, id;

\echo '========================================================'
\echo '=== T9: UNION ALL / UNION / INTERSECT / EXCEPT        ==='
\echo '========================================================'
\echo '--- UNION ALL (s3 + hdfs, should be 200) ---'
SELECT count(*) FROM (
  SELECT id FROM sales_s3
  UNION ALL
  SELECT id FROM sales_hdfs
) u;

\echo '--- UNION (distinct, should be 100) ---'
SELECT count(*) FROM (
  SELECT id FROM sales_s3
  UNION
  SELECT id FROM sales_hdfs
) u;

\echo '--- INTERSECT (should be 100, all ids shared) ---'
SELECT count(*) FROM (
  SELECT id FROM sales_s3
  INTERSECT
  SELECT id FROM sales_hdfs
) u;

\echo '--- EXCEPT (should be 0) ---'
SELECT count(*) FROM (
  SELECT id FROM sales_s3
  EXCEPT
  SELECT id FROM sales_hdfs
) u;

\echo '========================================================'
\echo '=== T10: CTE + aggregation chain                      ==='
\echo '========================================================'
WITH by_customer AS (
  SELECT customer, sum(qty * price) AS total_spent
    FROM sales_s3
    GROUP BY customer
), ranked AS (
  SELECT customer, total_spent,
         rank() OVER (ORDER BY total_spent DESC) AS r
    FROM by_customer
)
SELECT customer, total_spent, r
  FROM ranked WHERE r <= 5 ORDER BY r;

\echo '========================================================'
\echo '=== T11: DELETE + re-read                             ==='
\echo '========================================================'
\echo '--- DELETE ids 1-10 from both ---'
DELETE FROM sales_s3   WHERE id <= 10;
DELETE FROM sales_hdfs WHERE id <= 10;

SELECT 's3' AS backend, count(*), min(id), max(id) FROM sales_s3
UNION ALL
SELECT 'hdfs', count(*), min(id), max(id) FROM sales_hdfs;

\echo '========================================================'
\echo '=== T12: Timestamp + boolean filter                   ==='
\echo '========================================================'
SELECT count(*) AS weekend_sold, count(*) FILTER (WHERE returned) AS returned_cnt
  FROM sales_s3
  WHERE sold_at >= timestamp '2026-01-03' AND sold_at < timestamp '2026-01-04';

SELECT count(*) AS weekend_sold, count(*) FILTER (WHERE returned) AS returned_cnt
  FROM sales_hdfs
  WHERE sold_at >= timestamp '2026-01-03' AND sold_at < timestamp '2026-01-04';

\echo '========================================================'
\echo '=== DONE ==='
\echo '========================================================'
