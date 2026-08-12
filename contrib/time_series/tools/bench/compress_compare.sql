-- Bench: compress_chunks / reclaim_chunk_heaps throughput + compression ratio.
--
-- Setup writes 1,000,000 rows across ~100 chunks (8-hour chunks × ~33
-- days, ~10K rows per chunk) into a TSBS-style cpu table on the
-- time_series AM, then measures:
--   1. compress_chunks() wall time + chunks/sec throughput
--   2. ratio of PAX bytes / pre-compress heap bytes
--   3. heap-fork residual size (post-compress, pre-reclaim)
--   4. reclaim_chunk_heaps() wall time + chunks/sec
--   5. heap-fork size after reclaim (should be zero)
--
-- Run with:  psql -d <db> -f compress_compare.sql
-- Numbers below are the headline ones used in insert-bench-zh.md §7.
--
-- Density caveat: with very sparse chunks (<1K rows / chunk) PAX
-- per-chunk metadata dominates and the file ends up larger than the
-- source heap — pick a row rate so each chunk gets at least a few
-- thousand rows for meaningful compression numbers.

\timing on
\set ECHO none

DROP TABLE IF EXISTS cpu_ts_compress;

CREATE TABLE cpu_ts_compress (
    "time"           timestamptz NOT NULL,
    tags_id          integer,
    usage_user       double precision,
    usage_system     double precision,
    usage_idle       double precision,
    usage_nice       double precision,
    usage_iowait     double precision,
    usage_irq        double precision,
    usage_softirq    double precision,
    usage_steal      double precision,
    usage_guest      double precision,
    usage_guest_nice double precision,
    additional_tags  jsonb
)
USING time_series
WITH (ts_partition_column='time', ts_chunk_interval='8 hour', ts_chunk_origin='2025-01-01')
DISTRIBUTED BY (tags_id);

\echo
\echo '===== Setup: 1,000,000 rows across ~100 chunks (~10K rows/chunk) ====='

INSERT INTO cpu_ts_compress
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '2.88 seconds'),
       i % 100,
       random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100,
       jsonb_build_object('host', 'h' || (i % 100))
FROM generate_series(1, 1000000) i;

-- 2.88 sec × 1e6 = 2.88e6 sec ≈ 33 days; with 8-hour chunks → ~100 chunks.

SELECT count(*) AS row_count FROM cpu_ts_compress;
SELECT count(DISTINCT chunk_number) AS chunk_count
FROM time_series.ts_chunk
WHERE table_oid='cpu_ts_compress'::regclass;

\echo
\echo '===== Baseline: heap-fork size before compress ====='

SELECT pg_size_pretty(sum(nblocks * 8192::bigint)) AS heap_size_total,
       sum(nblocks * 8192::bigint) AS heap_bytes_total
FROM time_series.ts_chunk_info('cpu_ts_compress'::regclass);

\echo
\echo '===== Bench A1: compress_chunks() ====='

SELECT time_series.set_compress_config('cpu_ts_compress'::regclass, NULL, '"time"');
SELECT time_series.compress_chunks('cpu_ts_compress'::regclass) AS chunks_compressed;

\echo
\echo '===== Compression result: PAX size ====='
\echo '   (ts_compressed_chunk.uncompressed_size is not populated in v1, so the'
\echo '    ratio is computed against the heap-fork baseline measured above.)'

SELECT pg_size_pretty(sum(compressed_size)) AS pax_size_total,
       sum(compressed_size)                 AS pax_bytes_total
FROM time_series.ts_compressed_chunk
WHERE table_oid='cpu_ts_compress'::regclass;

\echo
\echo '===== Heap-fork size BEFORE reclaim (still occupies disk) ====='

SELECT pg_size_pretty(sum(nblocks * 8192::bigint)) AS heap_size_post_compress,
       sum(nblocks * 8192::bigint) AS heap_bytes_post_compress
FROM time_series.ts_chunk_info('cpu_ts_compress'::regclass);

\echo
\echo '===== Bench A2: reclaim_chunk_heaps() ====='

SELECT time_series.reclaim_chunk_heaps('cpu_ts_compress'::regclass) AS chunks_reclaimed;

\echo
\echo '===== Heap-fork size AFTER reclaim (should be ~0) ====='

SELECT pg_size_pretty(sum(nblocks * 8192::bigint)) AS heap_size_post_reclaim,
       sum(nblocks * 8192::bigint) AS heap_bytes_post_reclaim
FROM time_series.ts_chunk_info('cpu_ts_compress'::regclass);

\echo
\echo '===== Verify row count survives compress + reclaim ====='

SELECT count(*) AS row_count_after_compress_reclaim FROM cpu_ts_compress;

-- Cleanup (compress catalogs do not cascade)
DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='cpu_ts_compress'::regclass;
DELETE FROM time_series.ts_compress_config WHERE table_oid='cpu_ts_compress'::regclass;
DROP TABLE cpu_ts_compress;
