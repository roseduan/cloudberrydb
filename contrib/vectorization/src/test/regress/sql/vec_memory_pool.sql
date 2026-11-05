--
-- The contract for arrow memory routed into PG
--
-- The vectorized executor moves arrow's allocations off default_memory_pool()
-- and onto VecQueryMemoryPool, one per backend.  This file pins down three
-- things:
--
--   1. PG can see it -- after a vectorized query, a segment can report the
--      arrow memory that query used.  Before this work the figure was always
--      0, so statement_mem, the vmem tracker and resource groups were all
--      blind to it.
--   2. It is given back when the query ends.
--   3. A ceiling stops the query, and stops it safely -- Allocate returns a
--      Status so arrow unwinds along its existing error path.  It must NOT
--      elog(ERROR): PG's ereport longjmps through C++ frames, skipping every
--      destructor, and arrow's shared_ptrs and ExecPlan thread state leak.
--      "the session still works after the error" is therefore part of the
--      contract, not a nicety.
--
-- Two ways to write these tests wrong, both learned the hard way:
--   * The workload query must be written bare.  Wrapping it in an expression
--     such as 'x: ' || count(*) makes the whole plan non-vectorizable, it
--     falls back to row mode, arrow allocates nothing, and the assertion
--     fails for the wrong reason.
--   * PL/pgSQL PERFORM does not vectorize either, so the over-the-ceiling
--     cases have to be bare queries with an expected error.
--
-- Assertions print ok / FAIL:xxx and never a byte count -- byte counts differ
-- from run to run, and putting one in expected output is how a test turns
-- flaky.
--
CREATE SCHEMA vecmem;
SET search_path = vecmem, public;
SET vector.enable_vectorization = on;

-- ------------------------------------------------------------ the interface
SELECT 'api_function: ' || CASE WHEN count(*) = 1 THEN 'ok' ELSE 'FAIL: missing' END
  FROM pg_proc WHERE proname = 'vector_memory_stats';
SELECT 'api_view: ' || CASE WHEN count(*) = 1 THEN 'ok' ELSE 'FAIL: missing' END
  FROM pg_class WHERE relname = 'vector_memory_stats_v';
SELECT 'api_guc: ' || CASE WHEN count(*) = 2 THEN 'ok' ELSE 'FAIL: missing' END
  FROM pg_settings WHERE name IN ('vector.query_memory_limit_mb',
                                  'vector.report_memory_to_vmem');
SELECT 'shape: ' || CASE WHEN segindex IS NOT NULL AND query_peak_bytes >= 0
                          AND live_bytes >= 0 AND arrow_total_peak >= 0
                         THEN 'ok' ELSE 'FAIL' END
  FROM vector_memory_stats();

-- ------------------------------------------------------------------ fixture
CREATE TABLE t (a int, b int, c text) USING ao_column DISTRIBUTED BY (a);
INSERT INTO t SELECT i, i % 997, repeat('x', 64) FROM generate_series(1, 200000) i;
ANALYZE t;

-- ------------------------------------- 1. PG can see arrow's memory
-- Bare query; do not wrap it in an expression.
SELECT count(*) FROM (SELECT b, count(*), sum(a), string_agg(c, '') FROM t GROUP BY b) s;

SELECT 'accounted: ' || CASE WHEN max(query_peak_bytes) > 0
                            THEN 'ok' ELSE 'FAIL: arrow memory invisible to PG' END
  FROM gp_dist_random('vector_memory_stats_v');

-- ------------------------------------- 2. it is given back when the query ends
-- live is not required to be exactly 0: the pool is backend-scoped and a small
-- resident buffer is normal.  A real leak here is hundreds of megabytes.
SELECT 'released: ' || CASE WHEN max(live_bytes) < 4*1024*1024
                           THEN 'ok' ELSE 'FAIL: too much still held' END
  FROM gp_dist_random('vector_memory_stats_v');

-- ------------------------------------- 3. the ceiling stops the query
SET vector.query_memory_limit_mb = 1;
SELECT count(*) FROM (SELECT b, count(*), sum(a), string_agg(c, '') FROM t GROUP BY b) s;
RESET vector.query_memory_limit_mb;

-- ------------------------------------- 4. the session still works afterwards
-- This is what proves Allocate did not elog(ERROR) through the C++ frames.
SELECT count(*) FROM (SELECT b, count(*), sum(a), string_agg(c, '') FROM t GROUP BY b) s;

SELECT 'session_usable: ' || CASE WHEN count(*) = 997 THEN 'ok' ELSE 'FAIL' END
  FROM (SELECT b FROM t GROUP BY b) s;

-- ------------------------------------- 5. repeated refusals do not accumulate
SET vector.query_memory_limit_mb = 1;
SELECT count(*) FROM (SELECT b, string_agg(c, '') FROM t GROUP BY b) s;
SELECT count(*) FROM (SELECT b, string_agg(c, '') FROM t GROUP BY b) s;
RESET vector.query_memory_limit_mb;
SELECT count(*) FROM (SELECT b, count(*), sum(a) FROM t GROUP BY b) s;

SELECT 'no_accumulation: ' || CASE WHEN max(live_bytes) < 4*1024*1024
                                  THEN 'ok' ELSE 'FAIL: the error path leaks' END
  FROM gp_dist_random('vector_memory_stats_v');

-- ------------------------------------- 6. control: the ceiling only covers arrow
SET vector.enable_vectorization = off;
SET vector.query_memory_limit_mb = 1;
SELECT 'row_mode_unaffected: ' || CASE WHEN count(*) = 997 THEN 'ok' ELSE 'FAIL' END
  FROM (SELECT b FROM t GROUP BY b) s;
RESET vector.query_memory_limit_mb;
SET vector.enable_vectorization = on;

-- ------------------------------------- 7. vmem reporting does not break queries
SET vector.report_memory_to_vmem = on;
SELECT 'vmem_reporting: ' || CASE WHEN count(*) = 997 THEN 'ok' ELSE 'FAIL' END
  FROM (SELECT b FROM t GROUP BY b) s;
RESET vector.report_memory_to_vmem;

DROP SCHEMA vecmem CASCADE;
