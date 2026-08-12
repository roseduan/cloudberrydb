-- ts_memory.sql
--
-- Memory stability tests for the time_series extension.
--
-- Targeted at our fork-based architecture's actual memory risks:
--
--   1. Repeated full-table scans: ChunkScanDesc allocates a chunk_list array
--      via palloc on each scan_begin; scan_end must pfree it.
--
--   2. Bulk INSERT touching many chunks: each INSERT triggers
--      ts_chunk_catalog_insert (table_open → seqscan → table_close).
--      Catalog lookup state must not accumulate.
--
--   3. Index build on many chunks: tsbt_build pallocs temporary arrays
--      (leaf entries, separator keys) per chunk and must free them.
--
--   4. Repeated index scans across different chunks.
--
--   5. Repeated COPY (multi_insert path) across many chunks.
--
-- Methodology: measure sum(total_bytes) from pg_backend_memory_contexts
-- at key points.  A >3x ratio between min and max readings within a
-- test signals a probable leak.

\i sql/include/setup.sql
SET search_path = public, time_series;

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

-- ======================================================================
-- Infrastructure
-- ======================================================================

CREATE TABLE test.mem_log (
    test_name TEXT,
    step      INT,
    bytes     BIGINT
) DISTRIBUTED BY (test_name);

-- Helper: total allocated bytes across all memory contexts.
-- Reliable in both utility mode and normal MPP mode.
CREATE OR REPLACE FUNCTION test.total_mem_bytes()
RETURNS BIGINT AS $$
    SELECT coalesce(sum(total_bytes), 0)
    FROM pg_backend_memory_contexts;
$$ LANGUAGE SQL VOLATILE;

-- Verdict: passes if max < 3 * min (allows for normal allocator noise).
CREATE OR REPLACE FUNCTION test.check_memory(p_test TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN count(*) < 2 THEN 'SKIP (not enough samples)'
        WHEN min(bytes) = 0 THEN 'SKIP (zero baseline)'
        WHEN max(bytes) <= 3 * min(bytes) THEN 'PASS'
        ELSE 'FAIL: memory grew ' ||
             round(max(bytes)::numeric / min(bytes)::numeric, 1) ||
             'x (min=' || pg_size_pretty(min(bytes)) ||
             ', max=' || pg_size_pretty(max(bytes)) || ')'
    END
    FROM test.mem_log
    WHERE test_name = p_test;
$$ LANGUAGE SQL STABLE;

-- ======================================================================
-- Test table: 1-hour chunks across 30 days → 720 chunks.
-- ======================================================================

CREATE TABLE test.ts_mem (
    ts   TIMESTAMPTZ NOT NULL,
    dev  INT,
    val  DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval   = '1 hour',
    ts_chunk_origin     = '2025-01-01'
) DISTRIBUTED BY (dev);

-- Populate: 5 devices × 720 hours = 3600 rows across 720 chunks.
INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '1 minute',
       d, d * 100.0 + h
FROM generate_series(0, 719) h,
     generate_series(1, 5)  d;

SELECT count(*) AS total_rows FROM test.ts_mem;

-- ======================================================================
-- Test 1: Repeated full-table scans
--
-- Each count(*) opens ChunkScanDesc → palloc(chunk_list) → scan → pfree.
-- 10 scans in one transaction must not accumulate.
-- ======================================================================

-- Warm up (first query populates caches).
SELECT count(*) FROM test.ts_mem;

INSERT INTO test.mem_log VALUES ('scan', 0, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 1, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 2, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 3, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 4, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 5, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 6, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 7, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 8, test.total_mem_bytes());
SELECT count(*) FROM test.ts_mem;
INSERT INTO test.mem_log VALUES ('scan', 9, test.total_mem_bytes());

SELECT test.check_memory('scan') AS repeated_scan_check;

-- ======================================================================
-- Test 2: Bulk INSERT across many chunks (catalog insert pressure)
--
-- Each INSERT into a new chunk → ts_chunk_catalog_insert.
-- 5 rounds of 720-chunk INSERTs in one transaction.
-- ======================================================================

TRUNCATE test.ts_mem;
DELETE FROM test.mem_log WHERE test_name = 'catalog';

-- Round 1: 720 chunks created for first time.
INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '1 minute',
       1, h
FROM generate_series(0, 719) h;
INSERT INTO test.mem_log VALUES ('catalog', 1, test.total_mem_bytes());

-- Rounds 2-5: same 720 chunks (catalog finds existing rows, skips insert).
INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '2 minutes',
       2, h
FROM generate_series(0, 719) h;
INSERT INTO test.mem_log VALUES ('catalog', 2, test.total_mem_bytes());

INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '3 minutes',
       3, h
FROM generate_series(0, 719) h;
INSERT INTO test.mem_log VALUES ('catalog', 3, test.total_mem_bytes());

INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '4 minutes',
       4, h
FROM generate_series(0, 719) h;
INSERT INTO test.mem_log VALUES ('catalog', 4, test.total_mem_bytes());

INSERT INTO test.ts_mem
SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '5 minutes',
       5, h
FROM generate_series(0, 719) h;
INSERT INTO test.mem_log VALUES ('catalog', 5, test.total_mem_bytes());

SELECT test.check_memory('catalog') AS catalog_insert_check;

-- ======================================================================
-- Test 3 and Test 4 (index build memory / index scan memory) are
-- disabled along with the ts_btree index AM.  Re-enable together with
-- the AM:
--
-- Test 3: Index build on many-chunk table
-- Test 4: Repeated index scans across different chunks
-- ======================================================================
-- SELECT count(*) AS rows_before_index FROM test.ts_mem;
--
-- INSERT INTO test.mem_log VALUES ('index_build', 0, test.total_mem_bytes());
-- CREATE INDEX ON test.ts_mem USING ts_btree (ts ts_btree_timestamptz_ops);
-- INSERT INTO test.mem_log VALUES ('index_build', 1, test.total_mem_bytes());
--
-- SELECT test.check_memory('index_build') AS index_build_check;
--
-- -- Warm up.
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-15 12:01:00+00';
--
-- INSERT INTO test.mem_log VALUES ('idx_scan', 0, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-02 06:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 1, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-05 12:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 2, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-08 18:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 3, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-11 00:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 4, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-14 06:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 5, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-17 12:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 6, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-20 18:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 7, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-23 00:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 8, test.total_mem_bytes());
-- SELECT count(*) FROM test.ts_mem WHERE ts = '2025-01-29 12:01:00+00';
-- INSERT INTO test.mem_log VALUES ('idx_scan', 9, test.total_mem_bytes());
--
-- SELECT test.check_memory('idx_scan') AS index_scan_check;

-- ======================================================================
-- Test 5: Repeated COPY (multi_insert path) across many chunks
--
-- ts_heap_multi_insert → ts_heap_tuple_insert per row.
-- Each COPY touches 720 chunks; verify no per-COPY leak.
-- ======================================================================

TRUNCATE test.ts_mem;
DELETE FROM test.mem_log WHERE test_name = 'copy';

-- Generate CSV source (720 rows, one per chunk).
COPY (
    SELECT '2025-01-01'::timestamptz + (h * interval '1 hour') + interval '30 minutes',
           1, h * 1.0
    FROM generate_series(0, 719) h
) TO '/tmp/ts_mem_720.csv' CSV;

-- Warm up.
COPY test.ts_mem FROM '/tmp/ts_mem_720.csv' CSV;

INSERT INTO test.mem_log VALUES ('copy', 0, test.total_mem_bytes());
COPY test.ts_mem FROM '/tmp/ts_mem_720.csv' CSV;
INSERT INTO test.mem_log VALUES ('copy', 1, test.total_mem_bytes());
COPY test.ts_mem FROM '/tmp/ts_mem_720.csv' CSV;
INSERT INTO test.mem_log VALUES ('copy', 2, test.total_mem_bytes());
COPY test.ts_mem FROM '/tmp/ts_mem_720.csv' CSV;
INSERT INTO test.mem_log VALUES ('copy', 3, test.total_mem_bytes());
COPY test.ts_mem FROM '/tmp/ts_mem_720.csv' CSV;
INSERT INTO test.mem_log VALUES ('copy', 4, test.total_mem_bytes());

SELECT test.check_memory('copy') AS copy_multi_insert_check;

-- ======================================================================
-- Cleanup
-- ======================================================================


