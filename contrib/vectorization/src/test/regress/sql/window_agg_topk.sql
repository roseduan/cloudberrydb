/*-------------------------------------------------------------------------
 *
 * Partition Top K Regression Test
 *
 * Focus:
 *   - Primary: RANK() with rank <= K → should use PartitionTopK when enabled
 *   - Secondary: ROW_NUMBER() and DENSE_RANK() → should NOT use PartitionTopK
 *                (even when optimizer_force_partition_topk = on)
 *
 * Verified via EXPLAIN that:
 *   - RANK + partition_topk=on → PartitionTopK node appears
 *   - ROW_NUMBER / DENSE_RANK + partition_topk=on → NO PartitionTopK node
 *
 *-------------------------------------------------------------------------
 */

-- Create test table: distributed by 'id', but we'll PARTITION BY 'category'
DROP TABLE IF EXISTS test_partition_topk;
CREATE TABLE test_partition_topk (
    id          INT,
    category    TEXT,      -- used in PARTITION BY (not the distribution key!)
    score       INT,       -- used in ORDER BY
    name        TEXT
) DISTRIBUTED BY (id);

-- Insert carefully crafted data to test:
--   - Ties at top (Partition A)
--   - Ties in middle (Partition B)
--   - All unique values (Partition C)
--   - Fewer than K rows (Partition D)
INSERT INTO test_partition_topk VALUES
-- Partition A: top tie (100, 100)
(1,  'A', 100, 'A1'),
(2,  'A', 100, 'A2'),
(3,  'A',  90, 'A3'),
(4,  'A',  80, 'A4'),
(5,  'A',  70, 'A5'),

-- Partition B: middle tie (90 x3)
(6,  'B',  95, 'B1'),
(7,  'B',  90, 'B2'),
(8,  'B',  90, 'B3'),
(9,  'B',  90, 'B4'),
(10, 'B',  85, 'B5'),

-- Partition C: all unique scores
(11, 'C', 200, 'C1'),
(12, 'C', 190, 'C2'),
(13, 'C', 180, 'C3'),
(14, 'C', 170, 'C4'),
(15, 'C', 160, 'C5'),

-- Partition D: only 2 rows
(16, 'D', 500, 'D1'),
(17, 'D', 400, 'D2');

-- ===================================================================
-- Test 1: RANK() with r <= 2 → SHOULD use PartitionTopK
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 2: RANK() with r <= 1 (K=1, top ties) → SHOULD use PartitionTopK
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1
ORDER BY category, r, id;


-- ===================================================================
-- Test 2b: RANK() with rn < 3 
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rn
    FROM test_partition_topk
) t
WHERE rn < 3
ORDER BY category, rn, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rn
    FROM test_partition_topk
) t
WHERE rn < 3
ORDER BY category, rn, id;

-- ===================================================================
-- Test 3: ROW_NUMBER() with rn <= 2 → SHOULD NOT use PartitionTopK
-- Even though partition_topk=on, ROW_NUMBER is not supported for optimization
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) AS rn
    FROM test_partition_topk
) t
WHERE rn <= 2
ORDER BY category, rn, id;

-- ===================================================================
-- Test 4: DENSE_RANK() with dr <= 2 → SHOULD NOT use PartitionTopK
-- Even though partition_topk=on, DENSE_RANK is not supported for optimization
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           DENSE_RANK() OVER (PARTITION BY category ORDER BY score DESC) AS dr
    FROM test_partition_topk
) t
WHERE dr <= 2
ORDER BY category, dr, id;

SELECT * FROM (
    SELECT id, category, score, name,
           DENSE_RANK() OVER (PARTITION BY category ORDER BY score DESC) AS dr
    FROM test_partition_topk
) t
WHERE dr <= 2
ORDER BY category, dr, id;

-- ===================================================================
-- Test 5: Control — RANK with partition_topk=off → NO PartitionTopK
-- GUCs: split_window=off, partition_topk=off
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO off;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 6: Multi-column ORDER BY (score DESC, name ASC)
-- Verifies tie-breaking across multiple sort columns
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC, name ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC, name ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 7: NULL values in partition keys and sort columns
-- ===================================================================
CREATE TABLE test_topk_nulls (
    id       INT,
    category TEXT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_nulls VALUES
(1, 'X', 100),
(2, 'X', NULL),
(3, 'X', 90),
(4, NULL, 200),
(5, NULL, 150),
(6, NULL, NULL),
(7, 'Y', NULL),
(8, 'Y', NULL);

SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_nulls
) t
WHERE r <= 2
ORDER BY category NULLS FIRST, r, id;

SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_nulls
) t
WHERE r <= 2
ORDER BY category NULLS FIRST, r, id;

DROP TABLE test_topk_nulls;

-- ===================================================================
-- Test 8: RANK() with ASC ordering
-- All previous tests used DESC; test ASC explicitly
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 9: Empty table
-- ===================================================================
CREATE TABLE test_topk_empty (
    id       INT,
    category TEXT,
    score    INT
) DISTRIBUTED BY (id);

SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_empty
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_empty
) t
WHERE r <= 2
ORDER BY category, r, id;

DROP TABLE test_topk_empty;

-- ===================================================================
-- Test 10: Boundary conditions — rank() <= 0 and rank() < 1
-- Both should return no rows
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

-- rank() <= 0: no rows
EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 0
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 0
ORDER BY category, r, id;

-- rank() < 1: no rows (rank starts at 1)
EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r < 1
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r < 1
ORDER BY category, r, id;

-- ===================================================================
-- Test 11: Large K value (K=100, larger than any partition)
-- Should return all rows
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 100
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 100
ORDER BY category, r, id;

-- ===================================================================
-- Test 12: Multiple window functions in same query
-- RANK() should use PartitionTopK, ROW_NUMBER() should not interfere
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC, id ASC) AS rn
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC, id ASC) AS rn
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 13: Single partition (no PARTITION BY clause)
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1
ORDER BY r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1
ORDER BY r, id;

-- ===================================================================
-- Test 14: Non-integer K value (float literal)
-- rank() <= 1.5 should behave like rank() <= 1 (rank is always integer)
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1.5
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 1.5
ORDER BY category, r, id;

-- ===================================================================
-- Test 15: Collation-aware sort on text column
-- Verify PartitionTopK handles text collation correctly
-- ORDER BY name (text) uses database default collation (en_US.UTF-8)
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY name ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY name ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 16: TOAST partition key — long text values as partition key
-- Verifies type-aware hashing (detoasts before hashing) works correctly.
-- Without the fix, TOAST-compressed values could hash differently
-- even when they represent the same logical value.
-- GUCs: split_window=on, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO on;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_toast (
    id       INT,
    category TEXT,
    score    INT
) DISTRIBUTED BY (id);

-- Insert rows with long text partition keys (> 2KB triggers TOAST)
INSERT INTO test_topk_toast VALUES
(1, repeat('A', 3000), 100),
(2, repeat('A', 3000),  90),
(3, repeat('A', 3000),  80),
(4, repeat('B', 3000), 200),
(5, repeat('B', 3000), 150);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_toast
) t
WHERE r <= 2
ORDER BY id;

SELECT * FROM (
    SELECT id, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_toast
) t
WHERE r <= 2
ORDER BY id;

DROP TABLE test_topk_toast;

-- ===================================================================
-- Test 17: RANK() with split_window=off, partition_topk=on
-- Verifies that PartitionTopK works when split window is disabled.
-- This exercises DXL serialization/deserialization path.
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 18: RANK() with split_window=off, partition_topk=on, r < 3
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rn
    FROM test_partition_topk
) t
WHERE rn < 3
ORDER BY category, rn, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rn
    FROM test_partition_topk
) t
WHERE rn < 3
ORDER BY category, rn, id;

-- ===================================================================
-- Test 19: RANK() with split_window=off, partition_topk=on, ASC order
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score ASC) AS r
    FROM test_partition_topk
) t
WHERE r <= 2
ORDER BY category, r, id;

-- ===================================================================
-- Test 20: Multiple partition columns (PARTITION BY a, b)
-- Exercises multi-column hash combining and equality matching
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_multi_part (
    id       INT,
    region   TEXT,
    dept     TEXT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_multi_part VALUES
(1, 'East', 'Sales',    100),
(2, 'East', 'Sales',    100),
(3, 'East', 'Sales',     90),
(4, 'East', 'Eng',      200),
(5, 'East', 'Eng',      150),
(6, 'West', 'Sales',     80),
(7, 'West', 'Sales',     70),
(8, 'West', 'Eng',      300);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, region, dept, score,
           RANK() OVER (PARTITION BY region, dept ORDER BY score DESC) AS r
    FROM test_topk_multi_part
) t
WHERE r <= 2
ORDER BY region, dept, r, id;

-- ORCA with PartitionTopK
SET optimizer TO on;
SELECT * FROM (
    SELECT id, region, dept, score,
           RANK() OVER (PARTITION BY region, dept ORDER BY score DESC) AS r
    FROM test_topk_multi_part
) t
WHERE r <= 2
ORDER BY region, dept, r, id;

-- Planner without PartitionTopK — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, region, dept, score,
           RANK() OVER (PARTITION BY region, dept ORDER BY score DESC) AS r
    FROM test_topk_multi_part
) t
WHERE r <= 2
ORDER BY region, dept, r, id;

SET optimizer TO on;
DROP TABLE test_topk_multi_part;

-- ===================================================================
-- Test 21: INT partition key (exercises INT4 fast-path hash/eq)
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_int_part (
    id       INT,
    grp      INT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_int_part VALUES
(1, 1, 100),
(2, 1, 100),
(3, 1,  90),
(4, 2, 200),
(5, 2, 150),
(6, 2, 150),
(7, 3,  50);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_int_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

-- ORCA with PartitionTopK
SET optimizer TO on;
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_int_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

-- Planner without PartitionTopK — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_int_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

SET optimizer TO on;
DROP TABLE test_topk_int_part;

-- ===================================================================
-- Test 22: All ties (every row in partition has same sort key)
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_all_ties (
    id       INT,
    category TEXT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_all_ties VALUES
(1, 'A', 100),
(2, 'A', 100),
(3, 'A', 100),
(4, 'B',  50),
(5, 'B',  50);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_all_ties
) t
WHERE r <= 1
ORDER BY category, id;

-- K=1: all ties at rank 1, should return all rows (ORCA)
SET optimizer TO on;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_all_ties
) t
WHERE r <= 1
ORDER BY category, id;

-- K=1: planner — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_all_ties
) t
WHERE r <= 1
ORDER BY category, id;

-- K=2: all ties at rank 1, should return all rows (ORCA)
SET optimizer TO on;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_all_ties
) t
WHERE r <= 2
ORDER BY category, id;

-- K=2: planner — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_all_ties
) t
WHERE r <= 2
ORDER BY category, id;

SET optimizer TO on;
DROP TABLE test_topk_all_ties;

-- ===================================================================
-- Test 23: Single row per partition
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_single_row (
    id       INT,
    category TEXT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_single_row VALUES
(1, 'A', 100),
(2, 'B', 200),
(3, 'C',  50);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_single_row
) t
WHERE r <= 1
ORDER BY category, id;

-- ORCA with PartitionTopK
SET optimizer TO on;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_single_row
) t
WHERE r <= 1
ORDER BY category, id;

-- Planner without PartitionTopK — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_single_row
) t
WHERE r <= 1
ORDER BY category, id;

-- K=3 with only 1 row per partition (ORCA)
SET optimizer TO on;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_single_row
) t
WHERE r <= 3
ORDER BY category, id;

-- K=3: planner — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, category, score,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS r
    FROM test_topk_single_row
) t
WHERE r <= 3
ORDER BY category, id;

SET optimizer TO on;
DROP TABLE test_topk_single_row;

-- ===================================================================
-- Test 24: BIGINT partition key (exercises INT8 fast-path hash/eq)
-- GUCs: split_window=off, partition_topk=on
-- ===================================================================
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

CREATE TABLE test_topk_bigint_part (
    id       INT,
    grp      BIGINT,
    score    INT
) DISTRIBUTED BY (id);

INSERT INTO test_topk_bigint_part VALUES
(1, 4294967296, 100),
(2, 4294967296,  90),
(3, 4294967296,  80),
(4, 8589934592, 200),
(5, 8589934592, 150);

EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_bigint_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

-- ORCA with PartitionTopK
SET optimizer TO on;
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_bigint_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

-- Planner without PartitionTopK — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, grp, score,
           RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS r
    FROM test_topk_bigint_part
) t
WHERE r <= 2
ORDER BY grp, r, id;

SET optimizer TO on;
DROP TABLE test_topk_bigint_part;

----------------------------------------------------------------------
-- Test 25: Commuted predicate form: N >= rank()
-- Verifies that PartitionTopK also fires when the comparison is
-- written as "constant >= window_func" instead of "window_func <= constant"
----------------------------------------------------------------------
SET optimizer_force_split_window_function TO off;
SET optimizer_force_partition_topk TO on;

-- EXPLAIN should show Partition Top-K with 2 >= rank()
EXPLAIN (COSTS OFF)
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rnk
    FROM test_partition_topk
) t
WHERE 2 >= rnk
ORDER BY category, rnk, id;

-- ORCA with PartitionTopK (2 >= rnk)
SET optimizer TO on;
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rnk
    FROM test_partition_topk
) t
WHERE 2 >= rnk
ORDER BY category, rnk, id;

-- Planner without PartitionTopK — results must match
SET optimizer TO off;
SELECT * FROM (
    SELECT id, category, score, name,
           RANK() OVER (PARTITION BY category ORDER BY score DESC) AS rnk
    FROM test_partition_topk
) t
WHERE 2 >= rnk
ORDER BY category, rnk, id;

SET optimizer TO on;

-- Cleanup
DROP TABLE test_partition_topk;
