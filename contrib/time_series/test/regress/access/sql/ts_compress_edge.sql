-- ts_compress_edge.sql
--
-- Boundary conditions for compress / reclaim that aren't exercised by
-- ts_compress.sql:
--   * NULL handling on every column class (segmentby, orderby, value)
--   * NULLS FIRST / NULLS LAST orderby modifiers
--   * Mixed ASC / DESC orderby
--   * Empty chunks (declare config + compress with 0 rows)
--   * Single-row chunks (smallest possible PAX file)
--   * Extreme cardinality on segmentby (1 group vs every-row-its-own group)
--   * All-NULL rows
--
-- Mirrors what upstream compression_defaults.sql and
-- compress_unordered_sort.sql cover.

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: All-NULL rows in nullable columns
-- ======================================================================

CREATE TABLE ts_e_allnull (
    ts   timestamptz NOT NULL,
    a    integer,
    b    text,
    c    double precision
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_allnull'::regclass,
    orderby => 'ts');

INSERT INTO ts_e_allnull VALUES
    ('2025-01-01 01:00+00', NULL, NULL, NULL),
    ('2025-01-01 02:00+00', NULL, NULL, NULL),
    ('2025-01-01 03:00+00', 42,   NULL, NULL),
    ('2025-01-01 04:00+00', NULL, 'x',  NULL),
    ('2025-01-01 05:00+00', NULL, NULL, 1.5);

SELECT time_series.compress_chunks('ts_e_allnull'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_allnull'::regclass) AS reclaimed;

-- Round-trip — every column's NULL bitmap must survive the trip
SELECT a, b, c FROM ts_e_allnull ORDER BY ts;
SELECT count(*)              AS total,
       count(a)              AS a_non_null,
       count(b)              AS b_non_null,
       count(c)              AS c_non_null,
       count(*) FILTER (WHERE a IS NULL AND b IS NULL AND c IS NULL) AS all_null
FROM ts_e_allnull;

DROP TABLE ts_e_allnull;


-- ======================================================================
-- Section 2: NULL in segmentby column (groups under the NULL "value")
-- ======================================================================

CREATE TABLE ts_e_segnull (
    ts     timestamptz NOT NULL,
    region text,
    val    integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_segnull'::regclass,
    segmentby => 'region', orderby => 'ts');

INSERT INTO ts_e_segnull VALUES
    ('2025-01-01 01:00+00', 'us-east', 1),
    ('2025-01-01 02:00+00', NULL,      2),    -- NULL segment value
    ('2025-01-01 03:00+00', NULL,      3),
    ('2025-01-01 04:00+00', 'us-west', 4),
    ('2025-01-01 05:00+00', NULL,      5);

SELECT time_series.compress_chunks('ts_e_segnull'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_segnull'::regclass) AS reclaimed;

SELECT region, val FROM ts_e_segnull ORDER BY ts;
SELECT region, count(*) FROM ts_e_segnull
 GROUP BY region ORDER BY region NULLS LAST;
-- WHERE region IS NULL — sparse filter must handle the NULL segment
SELECT count(*) FROM ts_e_segnull WHERE region IS NULL;

DROP TABLE ts_e_segnull;


-- ======================================================================
-- Section 3: NULL in orderby column
-- ======================================================================

CREATE TABLE ts_e_ordnull (
    ts      timestamptz NOT NULL,
    sortkey integer,
    val     text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_ordnull'::regclass,
    orderby => 'sortkey');

INSERT INTO ts_e_ordnull VALUES
    ('2025-01-01 01:00+00',  3,   'a'),
    ('2025-01-01 02:00+00',  NULL,'b'),
    ('2025-01-01 03:00+00',  1,   'c'),
    ('2025-01-01 04:00+00',  NULL,'d'),
    ('2025-01-01 05:00+00',  2,   'e');

SELECT time_series.compress_chunks('ts_e_ordnull'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_ordnull'::regclass) AS reclaimed;

-- All five rows survive
SELECT count(*) AS total, count(sortkey) AS non_null FROM ts_e_ordnull;
SELECT sortkey, val FROM ts_e_ordnull ORDER BY sortkey NULLS LAST, val;

DROP TABLE ts_e_ordnull;


-- ======================================================================
-- Section 4: orderby modifiers (ASC, DESC, NULLS FIRST, NULLS LAST)
--
-- set_compress_config now reuses PG's own ORDER BY parser via
-- raw_parser(), so the full SQL syntax is supported:
--   col, col ASC, col DESC, col NULLS FIRST, col NULLS LAST,
--   col ASC NULLS FIRST, col DESC NULLS LAST, etc.
-- Default NULLS placement matches PG's ORDER BY default:
--   ASC → NULLS LAST, DESC → NULLS FIRST.
-- ======================================================================

-- 4a: explicit ASC (same as default)
CREATE TABLE ts_e_asc (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_asc'::regclass,
    orderby => 'sortkey ASC');
INSERT INTO ts_e_asc VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', NULL),
    ('2025-01-01 03:00+00', 3), ('2025-01-01 04:00+00', 2);
SELECT time_series.compress_chunks('ts_e_asc'::regclass);
SELECT count(*) AS total, count(sortkey) AS non_null FROM ts_e_asc;
DROP TABLE ts_e_asc;

-- 4b: DESC + default NULLS placement (DESC → NULLS FIRST)
CREATE TABLE ts_e_desc (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_desc'::regclass,
    orderby => 'sortkey DESC');
INSERT INTO ts_e_desc VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', NULL),
    ('2025-01-01 03:00+00', 3), ('2025-01-01 04:00+00', NULL),
    ('2025-01-01 05:00+00', 2);
SELECT time_series.compress_chunks('ts_e_desc'::regclass);
SELECT time_series.reclaim_chunk_heaps('ts_e_desc'::regclass);
SELECT count(*) AS total, count(sortkey) AS non_null FROM ts_e_desc;
DROP TABLE ts_e_desc;

-- 4c: ASC NULLS FIRST (override default)
CREATE TABLE ts_e_anf (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_anf'::regclass,
    orderby => 'sortkey ASC NULLS FIRST');
INSERT INTO ts_e_anf VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', NULL),
    ('2025-01-01 03:00+00', 3), ('2025-01-01 04:00+00', NULL),
    ('2025-01-01 05:00+00', 2);
SELECT time_series.compress_chunks('ts_e_anf'::regclass);
SELECT time_series.reclaim_chunk_heaps('ts_e_anf'::regclass);
SELECT count(*) AS total, count(sortkey) AS non_null FROM ts_e_anf;
-- Verify the catalog stored nullsfirst = true for this orderby column
SELECT orderby, orderby_desc, orderby_nullsfirst
  FROM time_series.ts_compress_config
 WHERE table_oid = 'ts_e_anf'::regclass;
DROP TABLE ts_e_anf;

-- 4d: DESC NULLS LAST (override default)
CREATE TABLE ts_e_dnl (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_dnl'::regclass,
    orderby => 'sortkey DESC NULLS LAST');
INSERT INTO ts_e_dnl VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', NULL),
    ('2025-01-01 03:00+00', 3), ('2025-01-01 04:00+00', 2);
SELECT time_series.compress_chunks('ts_e_dnl'::regclass);
SELECT count(*) AS total, count(sortkey) AS non_null FROM ts_e_dnl;
SELECT orderby, orderby_desc, orderby_nullsfirst
  FROM time_series.ts_compress_config
 WHERE table_oid = 'ts_e_dnl'::regclass;
DROP TABLE ts_e_dnl;

-- 4e: NULLS FIRST without explicit direction (defaults to ASC)
CREATE TABLE ts_e_nf (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_nf'::regclass,
    orderby => 'sortkey NULLS FIRST');
INSERT INTO ts_e_nf VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', NULL);
SELECT time_series.compress_chunks('ts_e_nf'::regclass);
SELECT count(*) AS total FROM ts_e_nf;
SELECT orderby_desc, orderby_nullsfirst
  FROM time_series.ts_compress_config
 WHERE table_oid = 'ts_e_nf'::regclass;
DROP TABLE ts_e_nf;

-- 4f: parser rejects garbage with a friendly message
\set ON_ERROR_STOP 0
CREATE TABLE ts_e_bad (ts timestamptz NOT NULL, sortkey integer)
USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;
SELECT time_series.set_compress_config('ts_e_bad'::regclass,
    orderby => 'sortkey ASCD');                       -- typo
SELECT time_series.set_compress_config('ts_e_bad'::regclass,
    orderby => 'sortkey + 1');                        -- expression, not col
SELECT time_series.set_compress_config('ts_e_bad'::regclass,
    orderby => 'nope DESC');                          -- column doesn't exist
\set ON_ERROR_STOP 1
DROP TABLE ts_e_bad;


-- ======================================================================
-- Section 5: Multiple orderby keys (segmentby + ascending + descending mix)
-- ======================================================================

CREATE TABLE ts_e_mixedord (
    ts     timestamptz NOT NULL,
    region text,
    rank   integer,
    val    integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- 'rank' (default ASC), 'ts DESC' — verify both directions in one config
SELECT time_series.set_compress_config('ts_e_mixedord'::regclass,
    segmentby => 'region',
    orderby   => 'rank, ts DESC');

INSERT INTO ts_e_mixedord VALUES
    ('2025-01-01 01:00+00', 'a', 1, 10),
    ('2025-01-01 02:00+00', 'a', 1, 20),
    ('2025-01-01 03:00+00', 'a', 2, 30),
    ('2025-01-01 04:00+00', 'b', 1, 40),
    ('2025-01-01 05:00+00', 'b', 1, 50);

SELECT time_series.compress_chunks('ts_e_mixedord'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_mixedord'::regclass) AS reclaimed;

SELECT region, rank, ts, val FROM ts_e_mixedord
 ORDER BY region, rank ASC, ts DESC;

DROP TABLE ts_e_mixedord;


-- ======================================================================
-- Section 6: compress on a config'd table with no rows (no chunks)
-- ======================================================================

CREATE TABLE ts_e_empty (
    ts  timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_empty'::regclass,
    orderby => 'ts');

-- No rows → no chunks → compress is a no-op returning 0
SELECT time_series.compress_chunks('ts_e_empty'::regclass)        AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_empty'::regclass)    AS reclaimed;
SELECT count(*) AS rows FROM ts_e_empty;

DROP TABLE ts_e_empty;


-- ======================================================================
-- Section 7: Single-row chunk (smallest possible PAX)
-- ======================================================================

CREATE TABLE ts_e_one (
    ts  timestamptz NOT NULL,
    val integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_one'::regclass,
    orderby => 'ts');

INSERT INTO ts_e_one VALUES ('2025-01-01 12:00+00', 42);

SELECT time_series.compress_chunks('ts_e_one'::regclass)     AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_one'::regclass) AS reclaimed;
SELECT * FROM ts_e_one;

-- INSERT after compress (PARTIAL) + recompress merging 1+1
INSERT INTO ts_e_one VALUES ('2025-01-01 13:00+00', 43);
SELECT time_series.compress_chunks('ts_e_one'::regclass) AS recompressed;
SELECT count(*) AS post_recompress FROM ts_e_one;

DROP TABLE ts_e_one;


-- ======================================================================
-- Section 8: extreme low-cardinality segmentby (single group)
-- ======================================================================

CREATE TABLE ts_e_lowcard (
    ts     timestamptz NOT NULL,
    region text,
    val    integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_lowcard'::regclass,
    segmentby => 'region', orderby => 'ts');

-- 100 rows, all same region → all in one PAX group
INSERT INTO ts_e_lowcard
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
       'only-region', i
FROM generate_series(1, 100) i;

SELECT time_series.compress_chunks('ts_e_lowcard'::regclass)     AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_lowcard'::regclass) AS reclaimed;
SELECT count(*) AS rows, count(DISTINCT region) AS regions FROM ts_e_lowcard;

DROP TABLE ts_e_lowcard;


-- ======================================================================
-- Section 9: extreme high-cardinality segmentby (every row a unique group)
-- ======================================================================

CREATE TABLE ts_e_highcard (
    ts     timestamptz NOT NULL,
    region text,
    val    integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_highcard'::regclass,
    segmentby => 'region', orderby => 'ts');

-- 50 rows, every row a unique segment value → 50 PAX groups (heavy flush)
INSERT INTO ts_e_highcard
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
       'r-' || i::text, i
FROM generate_series(1, 50) i;

SELECT time_series.compress_chunks('ts_e_highcard'::regclass)     AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_highcard'::regclass) AS reclaimed;
SELECT count(*) AS rows, count(DISTINCT region) AS regions FROM ts_e_highcard;

-- equality on a high-cardinality segment value should hit exactly one group
SELECT region, val FROM ts_e_highcard WHERE region = 'r-25';

DROP TABLE ts_e_highcard;


-- ======================================================================
-- Section 10: wide single row (multi-KB text payload)
--
-- The time_series table AM doesn't currently wire up TOAST, so any
-- single column whose serialised size exceeds the inline
-- toast_tuple_target (~2 KB) will fail with "failed to add tuple"
-- because PageAddItem can't fit a row larger than ~8 KB on a page.
-- We test a 1500-byte payload here — comfortably under the inline
-- limit but several orders of magnitude wider than typical sensor
-- rows — to verify wide-but-inline rows survive compress / recompress.
-- TOASTable wide rows (>2 KB) are a feature gap, not a test gap, and
-- are tracked in test-coverage-gaps.md.
-- ======================================================================

CREATE TABLE ts_e_widerow (
    ts      timestamptz NOT NULL,
    tag     text,
    payload text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_widerow'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_e_widerow VALUES
    ('2025-01-01 01:00+00', 'wide',  repeat('A', 1500)),
    ('2025-01-01 02:00+00', 'wide',  repeat('B', 1200)),
    ('2025-01-01 03:00+00', 'small', 'short');

SELECT time_series.compress_chunks('ts_e_widerow'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_widerow'::regclass) AS reclaimed;

-- length round-trip
SELECT tag, length(payload) AS len FROM ts_e_widerow ORDER BY ts;

-- recompress with another wide row added
INSERT INTO ts_e_widerow VALUES
    ('2025-01-01 04:00+00', 'wide', repeat('C', 1800));
SELECT time_series.compress_chunks('ts_e_widerow'::regclass) AS recompressed;
SELECT tag, length(payload) AS len FROM ts_e_widerow ORDER BY ts;

DROP TABLE ts_e_widerow;


-- ======================================================================
-- Section 11: many rows in a single PAX group — exercises
-- TS_MAX_TUPLES_PER_GROUP (131072) flush boundary.  We use a row count
-- that crosses the limit but stays small enough to keep the test fast.
-- ======================================================================

CREATE TABLE ts_e_biggroup (
    ts     timestamptz NOT NULL,
    region text,
    val    integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_e_biggroup'::regclass,
    segmentby => 'region', orderby => 'ts');

-- 200000 rows all in one chunk, all same region → exceeds the
-- TS_MAX_TUPLES_PER_GROUP cap and forces multiple group flushes
-- within a single segmentby value.
INSERT INTO ts_e_biggroup
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '100 microseconds'),
       'only-region', i
FROM generate_series(1, 200000) i;

SELECT time_series.compress_chunks('ts_e_biggroup'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_e_biggroup'::regclass) AS reclaimed;

-- All rows visible after compression
SELECT count(*)               AS rows,
       count(DISTINCT region) AS regions,
       min(val)               AS min_val,
       max(val)               AS max_val
  FROM ts_e_biggroup;

-- Range query exercises group min/max sparse filter
SELECT count(*) AS slice FROM ts_e_biggroup
 WHERE val BETWEEN 100000 AND 100100;

DROP TABLE ts_e_biggroup;


-- ======================================================================
-- Cleanup
-- ======================================================================

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
