-- ts_sparse_chunks.sql — sparse fork numbers (~10K-wide chunk-number range)
--
-- Defense against SMgrRelation per-fork array bugs.  Each chunk is a
-- separate fork file; jumping from fork 4 to fork 10004 forces
-- ts_ensure_fork_capacity to extend the per-fork book-keeping arrays
-- to ~10K entries.  Bugs in that path (off-by-one realloc, memory
-- leak on extension, missing init of intermediate slots) show up here
-- because intermediate forks 5..10003 do NOT exist on disk.
--
-- 4 rows, 4 distinct chunks, ChunkScan must enumerate just the present
-- chunks and not touch the absent ones.

\i sql/include/setup.sql
\i sql/include/consistency.sql

CREATE TABLE sparse_t (ts timestamptz, k int) USING time_series
WITH (ts_partition_column = 'ts',
      ts_chunk_interval   = '1 hour',
      ts_chunk_origin     = '2025-01-01')
DISTRIBUTED BY (k);

-- Hours 0 / 100 / 1000 / 10000 → chunks far apart (TS_FIRST_CHUNKNUM
-- offset; exact chunk number depends on origin but the spread is the
-- point).
INSERT INTO sparse_t
SELECT '2025-01-01'::timestamptz + (h || ' hours')::interval, k
FROM (VALUES (0, 1), (100, 2), (1000, 3), (10000, 4)) AS v(h, k);

SELECT count(*) AS total_rows FROM sparse_t;
SELECT count(DISTINCT chunk_number) AS distinct_chunks
FROM time_series.ts_chunk WHERE table_oid = 'sparse_t'::regclass;
SELECT test.ts_check_consistency('sparse_t'::regclass) AS state_active;

-- Full scan: ChunkScan must visit every present chunk and skip absent
-- intermediate fork numbers.  Result must be all 4 rows in ts order.
SELECT k FROM sparse_t ORDER BY ts;

-- Targeted scans hitting different chunks; tests that fork-pruning
-- works correctly across the sparse layout.
SELECT count(*) AS late_rows FROM sparse_t WHERE ts >= '2025-06-01';
SELECT k FROM sparse_t WHERE ts BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY ts;
SELECT k FROM sparse_t WHERE ts >= '2026-01-01' ORDER BY ts;

-- ts_extract_time_bounds: bound extraction via opfamily strategy lookup
-- ----------------------------------------------------------------------
-- These cases exercise the planner-side bound extraction without
-- relying on operator-name string matching.  Resolving the comparison
-- via get_op_btree_interpretation lets the same code path handle
-- Var-on-right ("const OP var") and bound types that share an
-- opfamily with the column type.
--
-- Var-on-right (const on left): mirrored strategy must produce the
-- same chunk pruning as Var-on-left.  Both queries hit exactly the
-- h=10000 row (= k=4).
SELECT k FROM sparse_t WHERE '2026-01-01' <= ts ORDER BY ts;
SELECT k FROM sparse_t WHERE ts >= '2026-01-01' ORDER BY ts;

-- Equality predicate: BTEqualStrategyNumber overwrites both bounds.
SELECT k FROM sparse_t WHERE ts = '2025-01-01 00:00:00+00' ORDER BY ts;

-- Both bounds combined as strict inequalities; opfamily lookup must
-- correctly distinguish BTLessStrategyNumber from BTLessEqualStrategyNumber.
-- Inclusive '2025-01-05 04:00:00' would include k=2; strict '<' excludes it.
SELECT k FROM sparse_t
WHERE ts > '2025-01-01 00:00:00+00'
  AND ts < '2025-01-05 04:00:00+00'
ORDER BY ts;
SELECT k FROM sparse_t
WHERE ts > '2025-01-01 00:00:00+00'
  AND ts <= '2025-01-05 04:00:00+00'
ORDER BY ts;

-- Mixed orientations on the same scan: one bound Var-on-left, the
-- other Var-on-right.  Result must match the canonical Var-on-left form.
SELECT k FROM sparse_t
WHERE ts >= '2025-01-01 00:00:00+00'
  AND '2025-12-31'::timestamptz > ts
ORDER BY ts;

-- Compress & re-read: state machine still works with sparse forks.
SELECT time_series.set_compress_config('sparse_t'::regclass, NULL, 'ts');
SELECT time_series.compress_chunks('sparse_t'::regclass) > 0 AS sparse_compressed;
SELECT count(*) AS rows_after_compress FROM sparse_t;
SELECT k FROM sparse_t ORDER BY ts;
SELECT test.ts_check_consistency('sparse_t'::regclass) AS state_after_compress;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid = 'sparse_t'::regclass;
DELETE FROM time_series.ts_compress_config WHERE table_oid = 'sparse_t'::regclass;
DROP TABLE sparse_t;
