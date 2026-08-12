-- ts_compress_ddl.sql
--
-- Interactions between DDL statements and the compress / reclaim flow.
-- Each section runs the standard ACTIVE → COMPRESSED → reclaim cycle,
-- then exercises a specific DDL operation, and verifies the data is
-- still readable / the catalog is consistent.
--
-- Mirrors what upstream compression_ddl.sql, compression_constraints.sql
-- and chunk_utils_compression.sql cover for the hypertable side.

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: ALTER TABLE ADD COLUMN on a table with COMPRESSED chunks
--
-- After the column is added, scanning the COMPRESSED chunk should
-- return NULL for the new column on rows that pre-date it (the PAX
-- file doesn't carry the column at all).  New INSERTs after the
-- ALTER should populate the column normally and survive recompress.
-- ======================================================================

CREATE TABLE ts_d_addcol (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_addcol'::regclass, orderby => 'ts');

INSERT INTO ts_d_addcol VALUES
    ('2025-01-01 01:00+00', 1), ('2025-01-01 02:00+00', 2),
    ('2025-01-01 03:00+00', 3);

SELECT time_series.compress_chunks('ts_d_addcol'::regclass);
SELECT time_series.reclaim_chunk_heaps('ts_d_addcol'::regclass);

-- Add a new column AFTER chunks are already COMPRESSED + reclaimed
ALTER TABLE ts_d_addcol ADD COLUMN tag text;

-- Old rows should report tag = NULL; row count unchanged
SELECT count(*) AS rows, count(tag) AS tag_non_null FROM ts_d_addcol;
SELECT ts, val, tag FROM ts_d_addcol ORDER BY ts;

-- INSERT new row with tag value → goes into PARTIAL state
INSERT INTO ts_d_addcol VALUES ('2025-01-01 04:00+00', 4, 'new');
SELECT count(*) AS rows, count(tag) AS tag_non_null FROM ts_d_addcol;

-- Recompress: PAX-resident old rows still see NULL, heap row sees 'new'
SELECT time_series.compress_chunks('ts_d_addcol'::regclass);
SELECT ts, val, tag FROM ts_d_addcol ORDER BY ts;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_d_addcol'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_d_addcol'::regclass;
DROP TABLE ts_d_addcol;


-- ======================================================================
-- Section 2: ALTER TABLE ALTER COLUMN TYPE — must error on compressed
--
-- Changing a column's type would invalidate the PAX file because the
-- on-disk byte format is per-column type-specialised (Gorilla for
-- floats, delta-delta for ints, dictionary/ZSTD for varlena).  Once a
-- chunk is compressed, ALTER COLUMN TYPE must error; reads must keep
-- returning the original rows from PAX.
-- ======================================================================

CREATE TABLE ts_d_altertype (
    ts   timestamptz NOT NULL,
    val  integer,
    tag  text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_altertype'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_d_altertype VALUES
    ('2025-01-01 01:00+00', 1, 'a'),
    ('2025-01-01 02:00+00', 2, 'a');

SELECT time_series.compress_chunks('ts_d_altertype'::regclass);

-- (a) Partition column ALTER TYPE is always blocked (pre-existing guard).
\set ON_ERROR_STOP 0
ALTER TABLE ts_d_altertype ALTER COLUMN ts TYPE timestamp;
\set ON_ERROR_STOP 1

-- (b) Non-partition column ALTER TYPE blocked because the table now has
--     compressed chunks (new guard).
\set ON_ERROR_STOP 0
ALTER TABLE ts_d_altertype ALTER COLUMN val TYPE bigint;
\set ON_ERROR_STOP 1

-- Reads must still work after the blocked ALTER attempts.
SELECT count(*) AS rows FROM ts_d_altertype;
SELECT val, tag FROM ts_d_altertype ORDER BY ts;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_d_altertype'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_d_altertype'::regclass;
DROP TABLE ts_d_altertype;


-- ======================================================================
-- Section 2b: ALTER TABLE SET / RESET on routing-defining reloptions
--
-- ts_partition_column / ts_chunk_interval / ts_chunk_origin determine
-- which fork an INSERT lands in.  Changing them after any chunk exists
-- would route new rows to a different fork than the existing rows for
-- the same time range — silent data split.  Must error once chunks
-- exist; must succeed on an empty table.
-- ======================================================================

CREATE TABLE ts_d_relopt (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- (a) On an empty table (no chunks yet) changing routing options is OK.
ALTER TABLE ts_d_relopt SET (ts_chunk_interval='12 hour');
ALTER TABLE ts_d_relopt SET (ts_chunk_origin='2025-02-01 00:00:00+00');

-- (b) Insert rows so a chunk gets created.
INSERT INTO ts_d_relopt VALUES ('2025-02-01 01:00+00', 1);

-- (c) With chunks present, the same SET must now error.
\set ON_ERROR_STOP 0
ALTER TABLE ts_d_relopt SET (ts_chunk_interval='1 hour');
ALTER TABLE ts_d_relopt SET (ts_chunk_origin='2025-01-01 00:00:00+00');
ALTER TABLE ts_d_relopt SET (ts_partition_column='ts');
\set ON_ERROR_STOP 1

-- (d) RESET also blocked (same routing semantics).
\set ON_ERROR_STOP 0
ALTER TABLE ts_d_relopt RESET (ts_chunk_interval);
\set ON_ERROR_STOP 1

-- Existing row still readable, options unchanged.
SELECT count(*) AS rows FROM ts_d_relopt;
SELECT ts, val FROM ts_d_relopt ORDER BY ts;

DROP TABLE ts_d_relopt;


-- ======================================================================
-- Section 3: TRUNCATE TABLE on a table with COMPRESSED chunks
--
-- TRUNCATE drops all heap forks and creates a new relfilenode.  It
-- does NOT know about our extension's PAX sidecar files or catalog
-- rows.  Verify the table appears empty after TRUNCATE and that
-- subsequent INSERT works again.
-- ======================================================================

CREATE TABLE ts_d_trunc (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_trunc'::regclass, orderby => 'ts');

INSERT INTO ts_d_trunc
SELECT '2025-01-01 00:00+00'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 10) i;

SELECT time_series.compress_chunks('ts_d_trunc'::regclass);
SELECT count(*) AS pre_truncate FROM ts_d_trunc;

TRUNCATE TABLE ts_d_trunc;
SELECT count(*) AS post_truncate FROM ts_d_trunc;

-- Insert again after truncate — should work
INSERT INTO ts_d_trunc VALUES ('2025-01-02 01:00+00', 100);
SELECT count(*) AS post_insert FROM ts_d_trunc;
SELECT * FROM ts_d_trunc;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_d_trunc'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_d_trunc'::regclass;
DROP TABLE ts_d_trunc;


-- ======================================================================
-- Section 4: VACUUM on a compressed table
--
-- VACUUM takes ShareUpdateExclusiveLock — compatible with our
-- AccessShareLock — and works against the user-relation main fork
-- (which is empty for time_series tables; data lives in extension
-- forks).  Should be a no-op for our forks but must not break SELECT.
-- ======================================================================

CREATE TABLE ts_d_vacuum (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_vacuum'::regclass, orderby => 'ts');

INSERT INTO ts_d_vacuum
SELECT '2025-01-01 00:00+00'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 10) i;

SELECT time_series.compress_chunks('ts_d_vacuum'::regclass);
SELECT time_series.reclaim_chunk_heaps('ts_d_vacuum'::regclass);

VACUUM ts_d_vacuum;
SELECT count(*) AS post_vacuum FROM ts_d_vacuum;

-- VACUUM on the extension catalog tables too
VACUUM time_series.ts_chunk;
VACUUM time_series.ts_compressed_chunk;
SELECT count(*) AS post_catalog_vacuum FROM ts_d_vacuum;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_d_vacuum'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_d_vacuum'::regclass;
DROP TABLE ts_d_vacuum;


-- ======================================================================
-- Section 5: ANALYZE on a compressed table
--
-- ANALYZE samples rows to update planner stats.  For time_series,
-- the sample comes from heap forks (our scan path); after compress
-- + reclaim heap is empty, so ANALYZE will see no rows from the
-- relation main fork — but should not error.  The ColumnarScan
-- custom-scan picks up its own per-chunk stats from
-- ts_compressed_chunk anyway.
-- ======================================================================

CREATE TABLE ts_d_analyze (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_analyze'::regclass, orderby => 'ts');

INSERT INTO ts_d_analyze
SELECT '2025-01-01 00:00+00'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 50) i;

SELECT time_series.compress_chunks('ts_d_analyze'::regclass);
ANALYZE ts_d_analyze;
SELECT count(*) AS post_analyze FROM ts_d_analyze;

DELETE FROM time_series.ts_compressed_chunk WHERE table_oid='ts_d_analyze'::regclass;
DELETE FROM time_series.ts_compress_config  WHERE table_oid='ts_d_analyze'::regclass;
DROP TABLE ts_d_analyze;


-- ======================================================================
-- Section 6: DROP COLUMN on a partition column — must be rejected
--
-- The ts_partition_column drives chunk routing; dropping it would
-- leave existing chunks orphaned and break INSERT routing.  The
-- ProcessUtility hook in ts_ddl.c (check_alter_table_drop_column
-- equivalent) rejects this.
-- ======================================================================

CREATE TABLE ts_d_dropcol (
    ts   timestamptz NOT NULL,
    val  integer,
    tag  text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

INSERT INTO ts_d_dropcol VALUES ('2025-01-01 01:00+00', 1, 'a');

\set ON_ERROR_STOP 0
ALTER TABLE ts_d_dropcol DROP COLUMN ts;
\set ON_ERROR_STOP 1

-- Dropping a non-partition column should still work
ALTER TABLE ts_d_dropcol DROP COLUMN tag;
SELECT * FROM ts_d_dropcol;

DROP TABLE ts_d_dropcol;


-- ======================================================================
-- Section 7: DROP TABLE on a table with COMPRESSED chunks (regression)
--
-- Already covered by ts_compress.sql Section 28's final orphan
-- check (final_config = 0, final_compressed = 0), but a focused
-- standalone test here makes the contract explicit.
-- ======================================================================

CREATE TABLE ts_d_drop (
    ts   timestamptz NOT NULL,
    val  integer
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_d_drop'::regclass, orderby => 'ts');

INSERT INTO ts_d_drop
SELECT '2025-01-01 00:00+00'::timestamptz + (i * interval '1 hour'), i
FROM generate_series(1, 5) i;

SELECT time_series.compress_chunks('ts_d_drop'::regclass);

-- Capture the relid before drop
SELECT 'ts_d_drop'::regclass::oid AS relid \gset

-- Verify rows exist for this relid in all three catalog tables
SELECT (SELECT count(*) FROM time_series.ts_chunk            WHERE table_oid = :relid) AS chunk_rows,
       (SELECT count(*) FROM time_series.ts_compress_config  WHERE table_oid = :relid) AS config_rows,
       (SELECT count(*) FROM time_series.ts_compressed_chunk WHERE table_oid = :relid) > 0 AS has_cchunk;

DROP TABLE ts_d_drop;

-- After DROP, all three catalogs should be cleaned up by the
-- ts_ddl.c ProcessUtility hook
SELECT (SELECT count(*) FROM time_series.ts_chunk            WHERE table_oid = :relid) AS chunk_rows_after,
       (SELECT count(*) FROM time_series.ts_compress_config  WHERE table_oid = :relid) AS config_rows_after,
       (SELECT count(*) FROM time_series.ts_compressed_chunk WHERE table_oid = :relid) AS cchunk_rows_after;


-- ======================================================================
-- Section: Permission checks on compression SQL surface
--
-- Every C-level entry point that takes a table Oid must reject calls
-- from a non-owner with ACLCHECK_NOT_OWNER, and the segment-side
-- internal helpers (prefixed `_ts_`) must be REVOKE'd from PUBLIC.
-- Without these two guards any authenticated user could compress /
-- reclaim / truncate arbitrary time_series tables (DoS + data loss
-- via _ts_truncate_chunk_fork on an uncompressed chunk).
-- ======================================================================

DROP ROLE IF EXISTS ts_noperm;
CREATE ROLE ts_noperm LOGIN;
-- Grant schema access so the caller can even NAME time_series.foo();
-- ownership on the target table stays with the owner, so every SQL
-- entry point below must reject ts_noperm.
GRANT USAGE ON SCHEMA time_series TO ts_noperm;

CREATE TABLE ts_perm_t (ts timestamptz NOT NULL, val int)
    USING time_series WITH (
        ts_partition_column='ts', ts_chunk_interval='1 day',
        ts_chunk_origin='2025-01-01 00:00:00+00'
    ) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_perm_t'::regclass, orderby => 'ts');
INSERT INTO ts_perm_t VALUES ('2025-01-01 12:00+00', 1);

-- Every call below is expected to error; keep the session alive.
\set ON_ERROR_STOP 0

-- ---- user-facing functions: non-owner must fail with must-be-owner ----
SET ROLE ts_noperm;
-- Owner-only writes
SELECT time_series.set_compress_config('ts_perm_t'::regclass, orderby => 'ts');
SELECT time_series.compress_chunks('ts_perm_t');
SELECT time_series.compress_chunk('ts_perm_t', 1);
SELECT time_series.reclaim_chunk_heaps('ts_perm_t');
-- Owner-only metadata reads
SELECT count(*) FROM time_series.ts_chunk_info('ts_perm_t');
SELECT count(*) FROM time_series.ts_compressed_chunk_info('ts_perm_t');
RESET ROLE;

-- ---- internal segment-side helpers: PUBLIC has no EXECUTE ----
SET ROLE ts_noperm;
SELECT time_series._ts_compress_write_chunks('ts_perm_t', ARRAY[1]::int[]);
SELECT time_series._ts_reclaim_chunk_heaps_segment('ts_perm_t', ARRAY[1]::int[]);
SELECT time_series._ts_truncate_chunk_fork('ts_perm_t', 1);
RESET ROLE;

\set ON_ERROR_STOP 1

-- ---- Owner still can call everything (sanity check) ----
SELECT time_series.compress_chunks('ts_perm_t') AS compressed_count;

DROP TABLE ts_perm_t;
REVOKE USAGE ON SCHEMA time_series FROM ts_noperm;
DROP ROLE ts_noperm;

-- ======================================================================
-- set_compress_config with a column name containing a single quote
--
-- PG identifiers may legally contain any character except NUL, and a
-- double-quoted identifier containing an apostrophe (e.g. "a'b") is
-- perfectly valid — the apostrophe is a literal character inside the
-- double-quoted form (only doubled double-quotes are special).
-- insert_compress_config used to build the segmentby / orderby arrays
-- via naive appendStringInfo(&cmd, "'%s'", name), which pastes the
-- apostrophe straight into the SQL text and derails SPI parsing.
-- quote_literal fixes it.  This case exercises the fix; before the
-- fix the set_compress_config call fails with a SPI syntax error.
-- ======================================================================

CREATE TABLE ts_weird_cols (
    ts timestamptz NOT NULL,
    "a'b" int         -- legal 3-char identifier: a, ', b
) USING time_series
  WITH (ts_partition_column='ts',
        ts_chunk_interval='1 hour',
        ts_chunk_origin='2025-01-01')
  DISTRIBUTED REPLICATED;

-- The SQL literal 'a''b' parses to the 3-char string a'b, which is
-- the name of the column above.  Must round-trip through the SPI
-- INSERT into ts_compress_config without a parse error.
SELECT time_series.set_compress_config(
           'ts_weird_cols'::regclass,
           'a''b',  -- SQL literal → C string a'b (matches column name)
           'ts');

-- Verify the config row landed with the correct segmentby name.
SELECT segmentby AS segmentby_after_apos
  FROM time_series.ts_compress_config
 WHERE table_oid = 'ts_weird_cols'::regclass;

DROP TABLE ts_weird_cols;


-- ======================================================================
-- Cleanup
-- ======================================================================

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
