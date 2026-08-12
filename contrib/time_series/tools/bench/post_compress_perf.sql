-- Bench: read and write performance against compressed time_series tables.
--
-- The existing insert_compare.sql / load_compare.sh benchmarks measure
-- only the ACTIVE chunk path.  In production, recent chunks are ACTIVE
-- and older chunks are COMPRESSED (or COMPRESSED+reclaimed).  Inserts
-- into a previously-COMPRESSED chunk flip it to PARTIAL; reads then
-- have to walk PAX + heap.
--
-- Two tables with identical 1M-row data:
--   cpu_active     — stays ACTIVE for the whole run
--   cpu_compressed — compressed + reclaimed, then 5K rows late-inserted
--                    to flip 5 chunks to PARTIAL
--
-- Measured:
--   B1  SELECT range  on ACTIVE        (heap path)
--   B2  SELECT range  on COMPRESSED    (PAX path)
--   B3  SELECT range  on PARTIAL       (PAX + heap path)
--   B4  INSERT 10K    into ACTIVE      (multi_insert into ACTIVE chunk)
--   B5  INSERT 10K    into PARTIAL     (multi_insert into PARTIAL chunk — heap re-extend after reclaim)
--
-- Run with:  psql -d <db> -f post_compress_perf.sql

\timing on
\set ECHO none

DROP TABLE IF EXISTS cpu_active;
DROP TABLE IF EXISTS cpu_compressed;

CREATE TABLE cpu_active (
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

CREATE TABLE cpu_compressed (LIKE cpu_active INCLUDING ALL)
USING time_series
WITH (ts_partition_column='time', ts_chunk_interval='8 hour', ts_chunk_origin='2025-01-01')
DISTRIBUTED BY (tags_id);

\echo
\echo '===== Setup: 1,000,000 rows in each table (identical data) ====='

INSERT INTO cpu_active
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '2.88 seconds'),
       i % 100, random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100, jsonb_build_object('host', 'h' || (i % 100))
FROM generate_series(1, 1000000) i;

INSERT INTO cpu_compressed
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '2.88 seconds'),
       i % 100, random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100, random()*100, random()*100,
       random()*100, random()*100, jsonb_build_object('host', 'h' || (i % 100))
FROM generate_series(1, 1000000) i;

\echo
\echo '===== Bring cpu_compressed to COMPRESSED + reclaimed state ====='

SELECT time_series.set_compress_config('cpu_compressed'::regclass, NULL, '"time"');
SELECT time_series.compress_chunks('cpu_compressed'::regclass) AS chunks_compressed;
SELECT time_series.reclaim_chunk_heaps('cpu_compressed'::regclass) AS chunks_reclaimed;

\echo
\echo '===== Bench B1: SELECT range scan on ACTIVE (heap path) ====='

SELECT count(*) AS active_range_rows FROM cpu_active
WHERE "time" >= '2025-01-05 00:00+00' AND "time" < '2025-01-10 00:00+00';

\echo
\echo '===== Bench B2: SELECT range scan on COMPRESSED (PAX path) ====='

SELECT count(*) AS compressed_range_rows FROM cpu_compressed
WHERE "time" >= '2025-01-05 00:00+00' AND "time" < '2025-01-10 00:00+00';

\echo
\echo '===== Bench B2b: column-projection SELECT on COMPRESSED ====='
\echo '   (single column — should be faster than B2 thanks to PAX projection) '

SELECT avg(usage_user) AS avg_user FROM cpu_compressed
WHERE "time" >= '2025-01-05 00:00+00' AND "time" < '2025-01-10 00:00+00';

\echo
\echo '===== Bench B2c: same column-projection on ACTIVE (heap baseline) ====='

SELECT avg(usage_user) AS avg_user FROM cpu_active
WHERE "time" >= '2025-01-05 00:00+00' AND "time" < '2025-01-10 00:00+00';

\echo
\echo '===== Flip 5 chunks to PARTIAL via late INSERT into cpu_compressed ====='

INSERT INTO cpu_compressed
SELECT '2025-01-05 00:00:00+00'::timestamptz + (h * interval '8 hours') + (i * interval '1 second'),
       1000000 + h, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '{"late":"true"}'::jsonb
FROM generate_series(0, 4) h, generate_series(0, 999) i;
-- 5 hours-of-day offset × 1000 rows each = 5000 rows into 5 chunks.

SELECT count(DISTINCT chunk_number) AS partial_chunks_after_late_insert
FROM time_series.ts_chunk
WHERE table_oid='cpu_compressed'::regclass AND status = 2;

\echo
\echo '===== Bench B3: SELECT range scan on PARTIAL (PAX + heap mixed) ====='

SELECT count(*) AS partial_range_rows FROM cpu_compressed
WHERE "time" >= '2025-01-05 00:00+00' AND "time" < '2025-01-10 00:00+00';

\echo
\echo '===== Bench B4: INSERT 10,000 rows into ACTIVE (multi_insert, fresh forks) ====='

INSERT INTO cpu_active
SELECT '2025-02-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       i % 100, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"h":"v"}'::jsonb
FROM generate_series(1, 10000) i;

\echo
\echo '===== Bench B5: INSERT 10,000 rows into freshly-reclaimed (still COMPRESSED) chunk ====='
\echo '   First write after reclaim flips status COMPRESSED → PARTIAL and'
\echo '   re-extends the heap fork from 0 blocks.  Target chunk is at'
\echo '   2025-02-01 — well away from the 2025-01-05 chunks already flipped'
\echo '   to PARTIAL by the previous late-INSERT.'

INSERT INTO cpu_compressed
SELECT '2025-02-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       2000000 + i, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"h":"v"}'::jsonb
FROM generate_series(1, 10000) i;

\echo
\echo '===== Final state ====='

SELECT count(*) AS rows_in_active     FROM cpu_active;
SELECT count(*) AS rows_in_compressed FROM cpu_compressed;
SELECT count(DISTINCT chunk_number) FILTER (WHERE status = 0) AS active_chunks,
       count(DISTINCT chunk_number) FILTER (WHERE status = 1) AS compressed_chunks,
       count(DISTINCT chunk_number) FILTER (WHERE status = 2) AS partial_chunks
FROM time_series.ts_chunk
WHERE table_oid IN ('cpu_active'::regclass, 'cpu_compressed'::regclass);

-- Cleanup
DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='cpu_compressed'::regclass;
DELETE FROM time_series.ts_compress_config WHERE table_oid='cpu_compressed'::regclass;
DROP TABLE cpu_active;
DROP TABLE cpu_compressed;
