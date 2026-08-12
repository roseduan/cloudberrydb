-- Test: chunk compression (Phase 1)
-- Verifies set_compress_config, compress_chunks, and metadata tracking.

-- start_matchsubs
-- m/Key \(table_oid\)=\(\d+\)/
-- s/Key \(table_oid\)=\(\d+\)/Key (table_oid)=(OID)/
-- m/Key \(table_oid, chunk_number\)=\(\d+, \d+\)/
-- s/Key \(table_oid, chunk_number\)=\(\d+, \d+\)/Key (table_oid, chunk_number)=(OID, N)/
-- end_matchsubs

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: Verify new catalog tables exist
-- ======================================================================

SELECT count(*) AS config_rows FROM time_series.ts_compress_config;
SELECT count(*) AS compressed_rows FROM time_series.ts_compressed_chunk;

-- Verify ts_chunk has status column
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'time_series' AND table_name = 'ts_chunk'
    AND column_name = 'status';

-- ======================================================================
-- Section 2: set_compress_config
-- ======================================================================

CREATE TABLE ts_comp_test (
    ts timestamptz NOT NULL,
    region text NOT NULL,
    device_id integer NOT NULL,
    temperature double precision,
    humidity double precision
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- 2a: Configure compression with segmentby and orderby
SELECT time_series.set_compress_config(
    'ts_comp_test'::regclass,
    segmentby => 'region, device_id',
    orderby => 'ts DESC'
);

-- 2b: Verify config stored correctly
SELECT segmentby, orderby, orderby_desc
FROM time_series.ts_compress_config
WHERE table_oid = 'ts_comp_test'::regclass;

-- 2c: Error on non-time_series table
\set ON_ERROR_STOP 0
CREATE TABLE ts_comp_regular (ts timestamptz, val int) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_comp_regular'::regclass, segmentby => 'val');

-- 2d: Error on non-existent column
SELECT time_series.set_compress_config('ts_comp_test'::regclass, segmentby => 'nonexistent');

-- 2e: Error on overlapping segmentby/orderby
SELECT time_series.set_compress_config(
    'ts_comp_test'::regclass,
    segmentby => 'region',
    orderby => 'region'
);

-- 2f: Error on duplicate config (PK violation)
SELECT time_series.set_compress_config('ts_comp_test'::regclass, segmentby => 'region');
\set ON_ERROR_STOP 1

-- ======================================================================
-- Section 3: Insert test data spanning multiple days
-- ======================================================================

INSERT INTO ts_comp_test
SELECT
    '2025-01-01'::timestamptz + (i || ' hours')::interval,
    CASE WHEN i % 3 = 0 THEN 'east'
         WHEN i % 3 = 1 THEN 'west'
         ELSE 'north' END,
    (i % 5) + 1,
    20.0 + (i % 10),
    50.0 + (i % 20)
FROM generate_series(0, 71) AS i;

-- 3a: Verify data count
SELECT count(*) AS total_rows FROM ts_comp_test;

-- 3b: Verify chunks created (3 days = 3 chunks)
SELECT count(DISTINCT chunk_number) AS n_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_test'::regclass;

-- ======================================================================
-- Section 4: Compress chunks
-- ======================================================================

-- 4a: Compress chunks older than 1 day
SELECT time_series.compress_chunks('ts_comp_test'::regclass, '1 day'::interval);

-- 4b: Check status — first 2 days should be COMPRESSED (status=2)
SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_test'::regclass
ORDER BY chunk_number;

-- 4c: Compressed chunk metadata exists
SELECT chunk_number, numrows
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_test'::regclass
ORDER BY chunk_number;

-- 4d: Data still readable from heap (Phase 1 preserves heap data)
SELECT count(*) AS rows_after_compress FROM ts_comp_test;

-- 4e: Pruned scan still works
SELECT count(*) AS day1_count FROM ts_comp_test
WHERE ts >= '2025-01-01' AND ts < '2025-01-02';

-- 4f: Compress remaining (no older_than = all ACTIVE chunks)
SELECT time_series.compress_chunks('ts_comp_test'::regclass);

-- 4g: All chunks should be COMPRESSED now
SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_test'::regclass
ORDER BY chunk_number;

-- 4h: Re-compress already compressed chunks does nothing (returns 0)
SELECT time_series.compress_chunks('ts_comp_test'::regclass);

-- ======================================================================
-- Section 5: ts_compressed_chunk_info
-- ======================================================================

SELECT chunk_number, status, numrows
FROM time_series.ts_compressed_chunk_info('ts_comp_test'::regclass)
ORDER BY chunk_number;

-- ======================================================================
-- Section 6: ts_chunk_info includes status
-- ======================================================================

SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk_info('ts_comp_test'::regclass)
ORDER BY chunk_number;

-- ======================================================================
-- Section 7: Compression without segmentby/orderby config
-- ======================================================================

CREATE TABLE ts_comp_noseg (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_noseg
SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 47) AS i;

-- Compress without config — should still work (no segmentby/orderby sorting)
SELECT time_series.compress_chunks('ts_comp_noseg'::regclass);

SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_noseg'::regclass
ORDER BY chunk_number;

-- Data still readable
SELECT count(*) AS noseg_rows FROM ts_comp_noseg;


-- ======================================================================
-- Section 8: DROP TABLE cleans up compress metadata
-- ======================================================================

DROP TABLE ts_comp_test;

-- Verify no orphaned compress config
SELECT count(*) AS orphaned_config
FROM time_series.ts_compress_config
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- Verify no orphaned compressed chunk metadata
SELECT count(*) AS orphaned_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- ======================================================================
-- Section 9: Error cases
-- ======================================================================

\set ON_ERROR_STOP 0
-- 9a: Compress on non-time_series table
CREATE TABLE ts_comp_err (ts timestamptz, val int) DISTRIBUTED REPLICATED;
SELECT time_series.compress_chunks('ts_comp_err'::regclass);
\set ON_ERROR_STOP 1

-- ======================================================================
-- Section 10: set_compress_config — orderby-only (no segmentby)
-- ======================================================================

CREATE TABLE ts_comp_ordonly (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config(
    'ts_comp_ordonly'::regclass,
    orderby => 'ts'
);

SELECT segmentby, orderby, orderby_desc
FROM time_series.ts_compress_config
WHERE table_oid = 'ts_comp_ordonly'::regclass;


-- ======================================================================
-- Section 11: set_compress_config — one-time restriction
-- ======================================================================

CREATE TABLE ts_comp_once (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_comp_once'::regclass, orderby => 'ts');
SELECT count(*) AS config_exists
FROM time_series.ts_compress_config
WHERE table_oid = 'ts_comp_once'::regclass;

-- Second call should error
\set ON_ERROR_STOP 0
SELECT time_series.set_compress_config('ts_comp_once'::regclass, orderby => 'val');
\set ON_ERROR_STOP 1


-- ======================================================================
-- Section 12: compress_chunks — empty table (no chunks at all)
-- ======================================================================

CREATE TABLE ts_comp_empty (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- Should return 0
SELECT time_series.compress_chunks('ts_comp_empty'::regclass);


-- ======================================================================
-- Section 13: compress_chunks — single chunk
-- ======================================================================

CREATE TABLE ts_comp_single (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_single VALUES ('2025-01-01 12:00:00+00', 1);

SELECT time_series.compress_chunks('ts_comp_single'::regclass);

-- Verify compressed
SELECT chunk_number
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_single'::regclass
ORDER BY chunk_number;

-- Data still readable
SELECT val FROM ts_comp_single;


-- ======================================================================
-- Section 14: compressed chunk metadata — verify pax_file path format
-- ======================================================================

CREATE TABLE ts_comp_meta (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_meta VALUES
    ('2025-01-01 12:00:00+00', 1),
    ('2025-01-02 12:00:00+00', 2);

SELECT time_series.compress_chunks('ts_comp_meta'::regclass);

-- pax_file should match pattern ts_compressed/{relid}/chunk_{chunknum}.pax
SELECT pax_file LIKE 'ts_compressed/%/chunk_%.pax' AS path_ok
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_meta'::regclass
ORDER BY chunk_number;

-- compressed_at should be recent (within last hour)
SELECT compressed_at > now() - interval '1 hour' AS recent_ok
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_meta'::regclass
ORDER BY chunk_number;

-- range_start and range_end should match ts_chunk
SELECT cc.chunk_number,
       cc.range_start = tc.range_start AS start_match,
       cc.range_end = tc.range_end AS end_match
FROM time_series.ts_compressed_chunk cc
JOIN (SELECT DISTINCT chunk_number, range_start, range_end
      FROM time_series.ts_chunk
      WHERE table_oid = 'ts_comp_meta'::regclass) tc
  ON cc.chunk_number = tc.chunk_number
WHERE cc.table_oid = 'ts_comp_meta'::regclass
ORDER BY cc.chunk_number;


-- ======================================================================
-- Section 15: Multiple tables — compress one, other unaffected
-- ======================================================================

CREATE TABLE ts_comp_a (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

CREATE TABLE ts_comp_b (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_a SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i FROM generate_series(0, 23) AS i;
INSERT INTO ts_comp_b SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i FROM generate_series(0, 23) AS i;

-- Compress only table A
SELECT time_series.compress_chunks('ts_comp_a'::regclass);

-- Table A has compressed metadata
SELECT count(*) AS a_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_a'::regclass;

-- Table B has NO compressed metadata
SELECT count(*) AS b_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_b'::regclass;

-- Both tables' data still readable
SELECT count(*) AS a_rows FROM ts_comp_a;
SELECT count(*) AS b_rows FROM ts_comp_b;


-- ======================================================================
-- Section 16: INSERT after compression — new data works
-- ======================================================================

CREATE TABLE ts_comp_insert_after (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_insert_after VALUES ('2025-01-01 12:00:00+00', 1);

-- Compress
SELECT time_series.compress_chunks('ts_comp_insert_after'::regclass);

-- Insert into a NEW chunk (different day)
INSERT INTO ts_comp_insert_after VALUES ('2025-01-02 12:00:00+00', 2);

-- Both rows readable (compressed chunk from TSCP, new chunk from heap)
SELECT val FROM ts_comp_insert_after ORDER BY val;

-- New chunk is NOT compressed
SELECT count(*) AS compressed_chunks
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_insert_after'::regclass;

-- Compress again — only the new chunk gets compressed
SELECT time_series.compress_chunks('ts_comp_insert_after'::regclass);

SELECT count(*) AS compressed_after
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_insert_after'::regclass;


-- ======================================================================
-- Section 17: set_compress_config — multiple orderby columns
-- ======================================================================

CREATE TABLE ts_comp_multi_ord (
    ts timestamptz NOT NULL,
    region text NOT NULL,
    val1 integer,
    val2 double precision
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config(
    'ts_comp_multi_ord'::regclass,
    segmentby => 'region',
    orderby => 'ts DESC, val1'
);

SELECT segmentby, orderby, orderby_desc
FROM time_series.ts_compress_config
WHERE table_oid = 'ts_comp_multi_ord'::regclass;


-- ======================================================================
-- Section 18: ts_compressed_chunk_info — no compressed chunks shows ACTIVE
-- ======================================================================

CREATE TABLE ts_comp_info_active (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_info_active VALUES ('2025-01-01 12:00:00+00', 1);

-- All chunks should show status=0 (ACTIVE) with NULL pax_file
SELECT DISTINCT chunk_number, status, pax_file IS NULL AS no_pax
FROM time_series.ts_compressed_chunk_info('ts_comp_info_active'::regclass)
ORDER BY chunk_number;


-- ======================================================================
-- Section 19: DROP TABLE with both config AND compressed data
-- ======================================================================

CREATE TABLE ts_comp_drop_full (
    ts timestamptz NOT NULL,
    region text,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config(
    'ts_comp_drop_full'::regclass,
    segmentby => 'region'
);

INSERT INTO ts_comp_drop_full VALUES
    ('2025-01-01 12:00:00+00', 'east', 1),
    ('2025-01-02 12:00:00+00', 'west', 2);

SELECT time_series.compress_chunks('ts_comp_drop_full'::regclass);

-- Verify config and compressed metadata exist before DROP
SELECT count(*) > 0 AS has_config
FROM time_series.ts_compress_config
WHERE table_oid = 'ts_comp_drop_full'::regclass;

SELECT count(*) > 0 AS has_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_drop_full'::regclass;

DROP TABLE ts_comp_drop_full;

-- Both should be cleaned up
SELECT count(*) AS config_after_drop
FROM time_series.ts_compress_config
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

SELECT count(*) AS compressed_after_drop
FROM time_series.ts_compressed_chunk
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- ======================================================================
-- Section 20: Different chunk intervals with compression
-- ======================================================================

-- Hourly chunks
CREATE TABLE ts_comp_hourly (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 hour',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_hourly
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' minutes')::interval, i
FROM generate_series(0, 179) AS i;

-- Should have 3 chunks (3 hours of data)
SELECT count(DISTINCT chunk_number) AS hourly_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_hourly'::regclass;

SELECT time_series.compress_chunks('ts_comp_hourly'::regclass);

SELECT count(*) AS hourly_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_hourly'::regclass;

-- Data still correct
SELECT count(*) AS hourly_rows FROM ts_comp_hourly;


-- Weekly chunks
CREATE TABLE ts_comp_weekly (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 week',
    ts_chunk_origin = '2025-01-06 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_comp_weekly VALUES
    ('2025-01-06 12:00:00+00', 1),
    ('2025-01-13 12:00:00+00', 2),
    ('2025-01-20 12:00:00+00', 3);

SELECT time_series.compress_chunks('ts_comp_weekly'::regclass);

SELECT count(*) AS weekly_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid = 'ts_comp_weekly'::regclass;

SELECT count(*) AS weekly_rows FROM ts_comp_weekly;


-- ======================================================================
-- Section 21: ROLLBACK + re-INSERT catalog recovery
-- ======================================================================

CREATE TABLE ts_comp_rollback (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED BY (val);

-- INSERT then ROLLBACK — fork files left on disk, catalog rolled back
BEGIN;
INSERT INTO ts_comp_rollback
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 47) AS i;
ROLLBACK;

-- Verify table is empty after rollback
SELECT count(*) AS after_rollback FROM ts_comp_rollback;

-- Re-INSERT same data — should succeed despite orphaned fork files
INSERT INTO ts_comp_rollback
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 47) AS i;

-- All 48 rows should be visible
SELECT count(*) AS after_reinsert FROM ts_comp_rollback;

-- Chunks should be in catalog
SELECT count(DISTINCT chunk_number) AS n_chunks
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_rollback'::regclass;

-- Pruned scan works
SELECT count(*) AS day1 FROM ts_comp_rollback
WHERE ts >= '2025-01-01' AND ts < '2025-01-02';

-- Compress after rollback+reinsert — should only compress live tuples
SELECT time_series.compress_chunks('ts_comp_rollback'::regclass);

-- Count after compress should equal count before compress (no dead tuples)
SELECT count(*) AS after_compress FROM ts_comp_rollback;

-- ======================================================================
-- Section 22: Large data — rows exceeding TS_MAX_TUPLES_PER_GROUP (131072)
-- ======================================================================

CREATE TABLE ts_comp_large (
    ts timestamptz NOT NULL,
    region text NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED BY (val);

-- Insert 200K rows into a single chunk (exceeds 131072 group limit)
INSERT INTO ts_comp_large
SELECT '2025-01-01'::timestamptz + (i * interval '100 us'),
       'region_' || (i % 2), i
FROM generate_series(1, 200000) i;

SELECT count(*) AS large_before FROM ts_comp_large;

SELECT time_series.set_compress_config('ts_comp_large', segmentby => 'region', orderby => 'ts');
SELECT time_series.compress_chunks('ts_comp_large');

-- Data should be fully readable from PAX (multiple groups per region)
SELECT count(*) AS large_after FROM ts_comp_large;

SELECT region, count(*) AS cnt
FROM ts_comp_large GROUP BY region ORDER BY region;


-- ======================================================================
-- Section 23: Partial compression — INSERT into compressed chunk
-- ======================================================================

CREATE TABLE ts_comp_partial (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- Insert initial data and compress
INSERT INTO ts_comp_partial VALUES
    ('2025-01-01 06:00:00+00', 1),
    ('2025-01-01 12:00:00+00', 2),
    ('2025-01-01 18:00:00+00', 3);

SELECT time_series.compress_chunks('ts_comp_partial'::regclass);

-- Verify compressed
SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_partial'::regclass
ORDER BY chunk_number;

-- All 3 rows readable from PAX
SELECT count(*) AS compressed_rows FROM ts_comp_partial;

-- INSERT into the same compressed chunk — should succeed and mark PARTIAL
INSERT INTO ts_comp_partial VALUES
    ('2025-01-01 08:00:00+00', 10),
    ('2025-01-01 20:00:00+00', 20);

-- Chunk should now be PARTIAL (status=3)
SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_partial'::regclass
ORDER BY chunk_number;

-- All 5 rows readable (3 old from heap + 2 new from heap)
SELECT count(*) AS partial_rows FROM ts_comp_partial;
SELECT val FROM ts_comp_partial ORDER BY val;

-- Recompress — PARTIAL chunk should be recompressed with all 5 rows
SELECT time_series.compress_chunks('ts_comp_partial'::regclass);

-- Chunk should be COMPRESSED again
SELECT DISTINCT chunk_number, status
FROM time_series.ts_chunk
WHERE table_oid = 'ts_comp_partial'::regclass
ORDER BY chunk_number;

-- All 5 rows still readable from PAX
SELECT count(*) AS recompressed_rows FROM ts_comp_partial;
SELECT val FROM ts_comp_partial ORDER BY val;

-- Re-compress again does nothing (already COMPRESSED)
SELECT time_series.compress_chunks('ts_comp_partial'::regclass);


-- ======================================================================
-- Section 24: EXPLAIN shows correct chunk status counts
-- ======================================================================

CREATE TABLE ts_comp_explain (
    ts timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column = 'ts',
    ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED BY (val);

INSERT INTO ts_comp_explain
SELECT '2025-01-01'::timestamptz + (i || ' hours')::interval, i
FROM generate_series(0, 71) AS i;

-- 24a: All ACTIVE — EXPLAIN should show 3 active chunks
EXPLAIN (COSTS OFF) SELECT * FROM ts_comp_explain;

-- 24b: Compress first 2 days, EXPLAIN shows mixed
SELECT time_series.compress_chunks('ts_comp_explain'::regclass, '1 day'::interval);
EXPLAIN (COSTS OFF) SELECT * FROM ts_comp_explain;

-- 24c: EXPLAIN ANALYZE should also work
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*) FROM ts_comp_explain;


-- ======================================================================
-- Section 25: Heap-reclaim workflow
--   1. compress_chunks writes PAX + catalog row, heap fork stays populated
--   2. reclaim_chunk_heaps truncates the heap fork to 0 blocks
--   3. SELECT on COMPRESSED chunk still returns all rows (via PAX)
--   4. INSERT into a COMPRESSED chunk auto-truncates the heap fork then
--      flips status to PARTIAL, so heap holds only the new post-compress
--      rows and PARTIAL scan = PAX + heap without duplicates
--   5. recompress merges PAX + heap into a new PAX file via .new + rename
--   6. second reclaim leaves heap empty again
--
-- Regression guard for the "recompress duplicates rows" bug that would
-- appear if heap truncation and prior-PAX merge weren't coordinated.
-- ======================================================================

CREATE TABLE ts_reclaim_t (
    ts     timestamptz      NOT NULL,
    id     integer          NOT NULL,
    region text             NOT NULL,
    val    double precision NOT NULL
)
USING time_series
WITH (ts_partition_column='ts',
      ts_chunk_interval='1 day',
      ts_chunk_origin='2025-01-01');

SELECT time_series.set_compress_config(
    'ts_reclaim_t'::regclass,
    segmentby => 'region',
    orderby   => 'ts'
);

-- Load 100 rows (all land in the 2025-01-05 chunk).
INSERT INTO ts_reclaim_t
SELECT '2025-01-05 00:00+00'::timestamptz + (s * interval '1 minute'),
       s,
       CASE s%2 WHEN 0 THEN 'east' ELSE 'west' END,
       s * 0.1
FROM generate_series(1, 100) s;

SELECT count(*) AS after_insert FROM ts_reclaim_t;

-- Compress the chunk.  Heap fork stays populated (not reclaimed yet).
SELECT time_series.compress_chunks('ts_reclaim_t'::regclass) AS compressed;
SELECT count(*) AS after_compress FROM ts_reclaim_t;

-- Reclaim heap forks of COMPRESSED chunks.
SELECT time_series.reclaim_chunk_heaps('ts_reclaim_t'::regclass) AS reclaimed;

-- Data must still be visible via PAX after heap truncation.
SELECT count(*) AS after_reclaim FROM ts_reclaim_t;

-- Insert 10 new rows into the (now COMPRESSED) chunk.  Auto-truncation
-- enforces the PARTIAL invariant — heap holds only these 10 rows after
-- INSERT, not the 100 pre-compress ones.  PARTIAL scan merges PAX + heap.
INSERT INTO ts_reclaim_t
SELECT '2025-01-05 12:00+00'::timestamptz + (s * interval '30 seconds'),
       100 + s,
       CASE s%2 WHEN 0 THEN 'east' ELSE 'west' END,
       s * 0.01
FROM generate_series(1, 10) s;

SELECT DISTINCT status FROM time_series.ts_chunk
 WHERE table_oid = 'ts_reclaim_t'::regclass;

SELECT count(*) AS partial_scan_rows FROM ts_reclaim_t;     -- expect 110
SELECT min(id), max(id), count(DISTINCT id) FROM ts_reclaim_t;  -- 1..110, 110 distinct

-- Recompress the PARTIAL chunk: merges 100 prior PAX rows + 10 heap rows.
SELECT time_series.compress_chunks('ts_reclaim_t'::regclass) AS recompressed;
SELECT count(*) AS after_recompress FROM ts_reclaim_t;
SELECT min(id), max(id), count(DISTINCT id) FROM ts_reclaim_t;

-- Second reclaim — should succeed, no row changes.
SELECT time_series.reclaim_chunk_heaps('ts_reclaim_t'::regclass);
SELECT count(*) AS final FROM ts_reclaim_t;

-- Explicit duplicate check: every id must appear exactly once.
SELECT count(*) AS duplicated_ids
  FROM (
    SELECT id FROM ts_reclaim_t GROUP BY id HAVING count(*) > 1
  ) dups;

-- ---------------------------------------------------------------------
-- Coverage for the count-only fast-path + PARTIAL-chunk interaction.
-- A single-chunk test misses this because the fast path is skipped when
-- no chunk in the scan is COMPRESSED.  With one PARTIAL + multiple
-- COMPRESSED chunks in the same scan, the fast path used to emit only
-- the PARTIAL chunk's PAX row count and silently drop its heap rows.
-- ---------------------------------------------------------------------
INSERT INTO ts_reclaim_t
SELECT '2025-01-06 00:00+00'::timestamptz + (s * interval '1 minute'),
       1000 + s,
       CASE s%2 WHEN 0 THEN 'east' ELSE 'west' END,
       s * 0.001
FROM generate_series(1, 60) s;
INSERT INTO ts_reclaim_t
SELECT '2025-01-07 00:00+00'::timestamptz + (s * interval '1 minute'),
       2000 + s,
       CASE s%2 WHEN 0 THEN 'east' ELSE 'west' END,
       s * 0.001
FROM generate_series(1, 60) s;

SELECT time_series.compress_chunks('ts_reclaim_t'::regclass);
SELECT time_series.reclaim_chunk_heaps('ts_reclaim_t'::regclass);

-- Sanity: 110 (chunk 1) + 60 + 60 = 230 rows
SELECT count(*) AS three_chunks_baseline FROM ts_reclaim_t;

-- Add 5 rows to ONE chunk, leaving the other two COMPRESSED — exercises
-- the count-only path with mixed PARTIAL + COMPRESSED chunks.
INSERT INTO ts_reclaim_t
SELECT '2025-01-06 12:00+00'::timestamptz + (s * interval '1 second'),
       9000 + s, 'east', s * 0.0001
FROM generate_series(1, 5) s;

SELECT count(*) AS mixed_partial_count FROM ts_reclaim_t;  -- expect 235

-- Cleanup (see ts_batch_insert for rationale on manual catalog cleanup).
DELETE FROM time_series.ts_compressed_chunk
 WHERE table_oid = 'ts_reclaim_t'::regclass;
DELETE FROM time_series.ts_compress_config
 WHERE table_oid = 'ts_reclaim_t'::regclass;

DROP TABLE ts_reclaim_t;


-- ======================================================================
-- Section 26: COMPRESSED → INSERT in xact → ROLLBACK
--
-- Inside an INSERT to a COMPRESSED chunk we run two operations:
--   smgrtruncate(heap, 0)              -- non-transactional
--   ts_chunk_catalog_update_status(PARTIAL)  -- transactional
--
-- If the user ROLLBACKs the INSERT xact:
--   - status reverts to COMPRESSED (transactional UPDATE rolled back)
--   - heap stays truncated (smgrtruncate is non-transactional)
-- This is safe because the COMPRESSED contract is "PAX is authoritative;
-- heap may be empty or non-empty redundant".  Reader sees status=COMPRESSED
-- and reads PAX only — all rows still visible.
-- ======================================================================

CREATE TABLE ts_xact_rb (
    ts  timestamptz NOT NULL,
    id  integer
) USING time_series WITH (
    ts_partition_column = 'ts', ts_chunk_interval = '1 day',
    ts_chunk_origin = '2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_xact_rb
SELECT '2025-01-01 12:00:00+00'::timestamptz + (i * interval '1 minute'), i
FROM generate_series(1, 50) i;

SELECT count(*) AS pre_compress FROM ts_xact_rb;

-- Compress; do NOT call reclaim, so heap fork still has the 50 rows
SELECT time_series.compress_chunks('ts_xact_rb'::regclass) AS compressed;

SELECT count(*) AS post_compress FROM ts_xact_rb;

-- INSERT inside an explicit xact, then ROLLBACK
BEGIN;
INSERT INTO ts_xact_rb VALUES ('2025-01-01 23:30:00+00', 999);

-- Inside the xact, status is PARTIAL (auto-truncate path fired)
SELECT DISTINCT status AS status_inside_xact
  FROM time_series.ts_chunk
 WHERE table_oid = 'ts_xact_rb'::regclass;

ROLLBACK;

-- After ROLLBACK: status reverts to COMPRESSED
SELECT DISTINCT status AS status_after_rollback
  FROM time_series.ts_chunk
 WHERE table_oid = 'ts_xact_rb'::regclass;

-- All 50 original rows still visible — PAX is authoritative; truncated
-- heap doesn't matter for COMPRESSED-state scans.
SELECT count(*) AS post_rollback FROM ts_xact_rb;

-- Cleanup
DELETE FROM time_series.ts_compressed_chunk
 WHERE table_oid = 'ts_xact_rb'::regclass;
DROP TABLE ts_xact_rb;


-- ======================================================================
-- Section 27: multi-row INSERT into multiple COMPRESSED chunks
--
-- Bug regression: a single INSERT statement that visits multiple
-- COMPRESSED chunks must flip each chunk's status to PARTIAL exactly
-- once and not raise "tuple already updated by self".
--
-- Root cause: ts_chunk_catalog_is_compressed used GetTransactionSnapshot
-- which doesn't see own-xact uncommitted UPDATEs.  After the first row
-- of a chunk took the auto-truncate branch (which sets ts_ins_blkno =
-- InvalidBlockNumber), the next row re-entered the slow path; the
-- snapshot still returned status = COMPRESSED, the auto-truncate
-- branch fired again, and update_status hit the same ts_chunk row in
-- the same command → TM_SelfModified.
--
-- Fix: ts_chunk_catalog_is_compressed now uses SnapshotSelf, plus the
-- slow-path predicate no longer treats blkno-Invalid as "needs catalog
-- work" (only as "needs buffer refresh", handled separately by
-- ts_chunk_get_insert_buffer).
-- ======================================================================

CREATE TABLE ts_reins (
    ts          timestamptz      NOT NULL,
    device_id   integer          NOT NULL,
    region      text             NOT NULL,
    val         double precision
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config(
    'ts_reins'::regclass,
    segmentby => 'region',
    orderby   => 'ts'
);

-- 5 chunks (Jan 5..9), 5 regions, 50 rows per (chunk, region)
INSERT INTO ts_reins
SELECT '2025-01-05 00:00:00+00'::timestamptz + (m * interval '1 hour'),
       d,
       (ARRAY['us-east','us-west','eu-west','ap-south','ap-north'])[1 + ((d-1) % 5)],
       1.0
FROM generate_series(0, 5*24 - 1) m,    -- 120 hourly slots over 5 days
     generate_series(1, 50) d;          -- 50 device IDs

SELECT count(*) AS pre_compress FROM ts_reins;

SELECT time_series.compress_chunks('ts_reins'::regclass) AS chunks_compressed;

-- Re-INSERT spanning all 5 COMPRESSED chunks in ONE statement.  Used
-- to fail with "tuple already updated by self".
INSERT INTO ts_reins
SELECT '2025-01-05 00:00:00+00'::timestamptz + (m * interval '1 hour'),
       d,
       (ARRAY['us-east','us-west','eu-west','ap-south','ap-north'])[1 + ((d-1) % 5)],
       2.0
FROM generate_series(0, 5*24 - 1) m,
     generate_series(1, 50) d;

SELECT count(*) AS post_reinsert FROM ts_reins;

-- All 5 chunks should be PARTIAL (auto-truncate fired exactly once each)
SELECT DISTINCT status AS status_after_reinsert
  FROM time_series.ts_chunk
 WHERE table_oid = 'ts_reins'::regclass;

-- Recompress should merge prior PAX + new heap → no row loss, no dup
SELECT time_series.compress_chunks('ts_reins'::regclass) AS recompressed;
SELECT count(*) AS post_recompress FROM ts_reins;

-- Cleanup
DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_reins'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_reins'::regclass;
DROP TABLE ts_reins;


-- ======================================================================
-- Section 28: COPY (multi_insert path) into COMPRESSED chunks
--
-- Same auto-truncate / status-flip logic as Section 27 must apply
-- when rows arrive through ts_heap_multi_insert (COPY, INSERT…SELECT
-- with bulk insert state).  Without this, COPY into a COMPRESSED
-- chunk would silently drop the new rows: heap is appended, but
-- status stays COMPRESSED and the scan path reads PAX only.
-- ======================================================================

CREATE TABLE ts_copy (
    ts        timestamptz       NOT NULL,
    region    text              NOT NULL,
    val       double precision
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config(
    'ts_copy'::regclass,
    segmentby => 'region',
    orderby   => 'ts'
);

-- Initial load via INSERT, then compress
INSERT INTO ts_copy
SELECT '2025-01-05 00:00:00+00'::timestamptz + (m * interval '1 hour'),
       (ARRAY['us-east','us-west','eu-west'])[1 + (m % 3)],
       1.0
FROM generate_series(0, 3*24 - 1) m;       -- 3 chunks (Jan 5,6,7) × 24 hrs

SELECT count(*) AS pre_compress FROM ts_copy;
SELECT time_series.compress_chunks('ts_copy'::regclass) AS chunks_compressed;

-- COPY into the now-COMPRESSED chunks via the multi_insert path
COPY ts_copy (ts, region, val) FROM STDIN WITH (FORMAT csv);
2025-01-05 06:00:00+00,us-east,2.0
2025-01-05 07:00:00+00,us-west,2.0
2025-01-06 06:00:00+00,us-east,2.0
2025-01-06 07:00:00+00,us-west,2.0
2025-01-07 06:00:00+00,us-east,2.0
2025-01-07 07:00:00+00,us-west,2.0
\.

-- All rows must be visible: 72 (pre) + 6 (COPY) = 78
SELECT count(*) AS post_copy FROM ts_copy;

-- All 3 chunks should now be PARTIAL (auto-truncate fired in multi_insert)
SELECT DISTINCT status AS status_after_copy
  FROM time_series.ts_chunk
 WHERE table_oid = 'ts_copy'::regclass;

-- Sanity: the 6 newly-COPYed rows have val = 2.0
SELECT count(*) AS new_rows FROM ts_copy WHERE val = 2.0;

-- Recompress to merge prior PAX + new heap rows back, no loss / no dup
SELECT time_series.compress_chunks('ts_copy'::regclass) AS recompressed;
SELECT count(*) AS post_recompress FROM ts_copy;
SELECT count(*) AS new_rows_after_recompress FROM ts_copy WHERE val = 2.0;

-- Cleanup
DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_copy'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_copy'::regclass;
DROP TABLE ts_copy;


-- ======================================================================
-- Section 29: Verify no leftover metadata after all tests
-- ======================================================================

SELECT count(*) AS final_config
FROM time_series.ts_compress_config
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

SELECT count(*) AS final_compressed
FROM time_series.ts_compressed_chunk
WHERE table_oid NOT IN (SELECT oid FROM pg_class);

-- ======================================================================
-- Cleanup
-- ======================================================================

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
