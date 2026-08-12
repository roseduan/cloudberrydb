-- ts_compress_types.sql
--
-- Round-trip integrity for data types not covered by ts_compress.sql.
-- For each type:
--   1. CREATE table USING time_series with the type as a value column
--   2. INSERT a few rows
--   3. compress + reclaim
--   4. SELECT post-compress: equality, range/order, NULL preservation
--   5. INSERT-into-COMPRESSED (exercises the auto-truncate + PARTIAL flip
--      with the new type)
--   6. recompress (exercises the prior_reader merge for the type)
--
-- Mirrors the role upstream compression_uuid.sql, compression_bools.sql,
-- compressed_collation.sql and compressed_detoaster.sql play in their suite.

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: uuid
-- ======================================================================

CREATE TABLE ts_t_uuid (
    ts  timestamptz NOT NULL,
    id  uuid,
    tag text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_uuid'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_uuid VALUES
    ('2025-01-01 01:00+00', '00000000-0000-0000-0000-000000000001', 'a'),
    ('2025-01-01 02:00+00', '11111111-2222-3333-4444-555555555555', 'a'),
    ('2025-01-01 03:00+00', 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'b'),
    ('2025-01-01 04:00+00', NULL,                                   'b'),
    ('2025-01-01 05:00+00', '12345678-1234-1234-1234-1234567890ab', 'a');

SELECT time_series.compress_chunks('ts_t_uuid'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_uuid'::regclass) AS reclaimed;

-- Round-trip equality + NULL preservation
SELECT id, tag FROM ts_t_uuid ORDER BY ts;

-- Equality match on uuid (sparse filter would prune by min/max)
SELECT count(*) FROM ts_t_uuid
 WHERE id = 'ffffffff-ffff-ffff-ffff-ffffffffffff';

-- INSERT into COMPRESSED, then recompress
INSERT INTO ts_t_uuid VALUES
    ('2025-01-01 06:00+00', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'c');
SELECT count(*) AS partial_count FROM ts_t_uuid;
SELECT time_series.compress_chunks('ts_t_uuid'::regclass) AS recompressed;
SELECT count(*) AS recompress_count FROM ts_t_uuid;

DROP TABLE ts_t_uuid;


-- ======================================================================
-- Section 2: boolean (with NULL three-valued logic)
-- ======================================================================

CREATE TABLE ts_t_bool (
    ts    timestamptz NOT NULL,
    flag  boolean,
    tag   text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_bool'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_bool VALUES
    ('2025-01-01 01:00+00', true,  'x'),
    ('2025-01-01 02:00+00', false, 'x'),
    ('2025-01-01 03:00+00', NULL,  'y'),
    ('2025-01-01 04:00+00', true,  'y'),
    ('2025-01-01 05:00+00', false, 'y');

SELECT time_series.compress_chunks('ts_t_bool'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_bool'::regclass) AS reclaimed;

SELECT flag, count(*) FROM ts_t_bool GROUP BY flag ORDER BY flag NULLS LAST;
-- Three-valued: WHERE flag IS NULL
SELECT count(*) FROM ts_t_bool WHERE flag IS NULL;
-- WHERE flag = true
SELECT count(*) FROM ts_t_bool WHERE flag = true;

INSERT INTO ts_t_bool VALUES
    ('2025-01-01 06:00+00', NULL, 'z');
SELECT time_series.compress_chunks('ts_t_bool'::regclass) AS recompressed;
SELECT flag, count(*) FROM ts_t_bool GROUP BY flag ORDER BY flag NULLS LAST;

DROP TABLE ts_t_bool;


-- ======================================================================
-- Section 3: jsonb (varlena, may TOAST)
-- ======================================================================

CREATE TABLE ts_t_jsonb (
    ts   timestamptz NOT NULL,
    payload jsonb,
    tag  text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_jsonb'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_jsonb VALUES
    ('2025-01-01 01:00+00', '{"a": 1, "b": [1, 2, 3]}'::jsonb,           'p'),
    ('2025-01-01 02:00+00', '{"nested": {"deep": {"k": "v"}}}'::jsonb,    'p'),
    ('2025-01-01 03:00+00', NULL,                                          'q'),
    ('2025-01-01 04:00+00', '[]'::jsonb,                                    'q'),
    ('2025-01-01 05:00+00', '"unicode 中文"'::jsonb,                'q');

SELECT time_series.compress_chunks('ts_t_jsonb'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_jsonb'::regclass) AS reclaimed;

-- Equality + path query
SELECT payload->>'a' AS a_val FROM ts_t_jsonb
 WHERE payload ? 'a' ORDER BY ts;
SELECT count(*) FROM ts_t_jsonb WHERE payload IS NULL;
SELECT count(*) FROM ts_t_jsonb WHERE payload @> '{"nested":{}}'::jsonb;

INSERT INTO ts_t_jsonb VALUES
    ('2025-01-01 06:00+00', '{"new":"row"}'::jsonb, 'r');
SELECT time_series.compress_chunks('ts_t_jsonb'::regclass) AS recompressed;
SELECT count(*) AS post_recompress FROM ts_t_jsonb;

DROP TABLE ts_t_jsonb;


-- ======================================================================
-- Section 4: bytea with large TOAST (>2KB inline limit)
-- ======================================================================

CREATE TABLE ts_t_bytea (
    ts    timestamptz NOT NULL,
    blob  bytea,
    tag   text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_bytea'::regclass,
    segmentby => 'tag', orderby => 'ts');

-- repeat() with octet_length triggers TOAST when length > ~2KB
INSERT INTO ts_t_bytea VALUES
    ('2025-01-01 01:00+00', '\x00010203'::bytea,                                 'small'),
    ('2025-01-01 02:00+00', repeat('A', 4096)::bytea,                             'large'),
    ('2025-01-01 03:00+00', repeat('Z', 8192)::bytea,                             'large'),
    ('2025-01-01 04:00+00', NULL,                                                  'small'),
    ('2025-01-01 05:00+00', '\xdeadbeef'::bytea,                                  'small');

SELECT time_series.compress_chunks('ts_t_bytea'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_bytea'::regclass) AS reclaimed;

-- Verify lengths preserved (proves TOAST round-trip)
SELECT octet_length(blob) AS len FROM ts_t_bytea ORDER BY ts;
SELECT count(*) FROM ts_t_bytea WHERE blob IS NULL;

INSERT INTO ts_t_bytea VALUES
    ('2025-01-01 06:00+00', repeat('Q', 6000)::bytea, 'large');
SELECT time_series.compress_chunks('ts_t_bytea'::regclass) AS recompressed;
SELECT octet_length(blob) AS len FROM ts_t_bytea ORDER BY ts;

DROP TABLE ts_t_bytea;


-- ======================================================================
-- Section 5: numeric (arbitrary precision)
-- ======================================================================

CREATE TABLE ts_t_numeric (
    ts    timestamptz NOT NULL,
    amount numeric,
    tag   text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_numeric'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_numeric VALUES
    ('2025-01-01 01:00+00', 0,                                            'zero'),
    ('2025-01-01 02:00+00', 12345.6789012345,                             'small'),
    ('2025-01-01 03:00+00', NULL,                                          'null'),
    ('2025-01-01 04:00+00', 99999999999999999999999999999.99999999,       'big'),
    ('2025-01-01 05:00+00', -3.141592653589793238462643383279502884197,   'pi');

SELECT time_series.compress_chunks('ts_t_numeric'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_numeric'::regclass) AS reclaimed;

SELECT amount, tag FROM ts_t_numeric ORDER BY ts;
SELECT sum(amount) IS NOT NULL AS sum_works FROM ts_t_numeric WHERE amount IS NOT NULL;

DROP TABLE ts_t_numeric;


-- ======================================================================
-- Section 6: inet
-- ======================================================================

CREATE TABLE ts_t_inet (
    ts    timestamptz NOT NULL,
    addr  inet,
    tag   text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_inet'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_inet VALUES
    ('2025-01-01 01:00+00', '10.0.0.1',                          'ipv4'),
    ('2025-01-01 02:00+00', '192.168.1.0/24',                    'ipv4'),
    ('2025-01-01 03:00+00', '::1',                               'ipv6'),
    ('2025-01-01 04:00+00', '2001:db8:85a3::8a2e:370:7334',      'ipv6'),
    ('2025-01-01 05:00+00', NULL,                                'unknown');

SELECT time_series.compress_chunks('ts_t_inet'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_inet'::regclass) AS reclaimed;

SELECT addr FROM ts_t_inet ORDER BY ts;
-- Subnet containment query
SELECT count(*) FROM ts_t_inet WHERE addr <<= '192.168.0.0/16';

DROP TABLE ts_t_inet;


-- ======================================================================
-- Section 7: interval
-- ======================================================================

CREATE TABLE ts_t_interval (
    ts        timestamptz NOT NULL,
    duration  interval,
    tag       text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_interval'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_interval VALUES
    ('2025-01-01 01:00+00', INTERVAL '1 microsecond',                 'tiny'),
    ('2025-01-01 02:00+00', INTERVAL '1 day 2 hours 3 minutes',       'normal'),
    ('2025-01-01 03:00+00', INTERVAL '5 years 11 months',             'big'),
    ('2025-01-01 04:00+00', NULL,                                      'null'),
    ('2025-01-01 05:00+00', INTERVAL '-1 hour 30 minutes',             'negative');

SELECT time_series.compress_chunks('ts_t_interval'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_interval'::regclass) AS reclaimed;

SELECT duration FROM ts_t_interval ORDER BY ts;
SELECT count(*) FROM ts_t_interval WHERE duration > INTERVAL '0';

DROP TABLE ts_t_interval;


-- ======================================================================
-- Section 8: int[] / text[] arrays
-- ======================================================================

CREATE TABLE ts_t_array (
    ts        timestamptz NOT NULL,
    ints      int[],
    labels    text[],
    tag       text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_array'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_array VALUES
    ('2025-01-01 01:00+00', ARRAY[1,2,3],                  ARRAY['a','b'],       'normal'),
    ('2025-01-01 02:00+00', ARRAY[]::int[],                ARRAY[]::text[],      'empty'),
    ('2025-01-01 03:00+00', ARRAY[10,20,30,40,50],         ARRAY['x','y','z'],   'normal'),
    ('2025-01-01 04:00+00', NULL,                          NULL,                 'null'),
    ('2025-01-01 05:00+00', ARRAY[NULL,42,NULL]::int[],    ARRAY['has',NULL],    'with-nulls');

SELECT time_series.compress_chunks('ts_t_array'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_array'::regclass) AS reclaimed;

SELECT ints, labels FROM ts_t_array ORDER BY ts;
-- Array containment
SELECT count(*) FROM ts_t_array WHERE ints @> ARRAY[2];
SELECT count(*) FROM ts_t_array WHERE labels && ARRAY['x','y'];

DROP TABLE ts_t_array;


-- ======================================================================
-- Section 9: char(N) fixed-width
-- ======================================================================

CREATE TABLE ts_t_char (
    ts     timestamptz NOT NULL,
    code   char(10),
    tag    text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_char'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_char VALUES
    ('2025-01-01 01:00+00', 'ABC',           'short'),
    ('2025-01-01 02:00+00', '1234567890',    'full'),
    ('2025-01-01 03:00+00', NULL,            'null'),
    ('2025-01-01 04:00+00', 'unicode',       'utf');

SELECT time_series.compress_chunks('ts_t_char'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_char'::regclass) AS reclaimed;

-- char(N) is space-padded; preserve the padding round-trip
SELECT code, length(code), char_length(code) FROM ts_t_char ORDER BY ts;

DROP TABLE ts_t_char;


-- ======================================================================
-- Section 10: enum
-- ======================================================================

CREATE TYPE ts_t_severity AS ENUM ('low', 'medium', 'high', 'critical');

CREATE TABLE ts_t_enum (
    ts        timestamptz NOT NULL,
    severity  ts_t_severity,
    tag       text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_enum'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_enum VALUES
    ('2025-01-01 01:00+00', 'low',      'a'),
    ('2025-01-01 02:00+00', 'high',     'a'),
    ('2025-01-01 03:00+00', 'critical', 'b'),
    ('2025-01-01 04:00+00', NULL,       'b'),
    ('2025-01-01 05:00+00', 'medium',   'a');

SELECT time_series.compress_chunks('ts_t_enum'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_enum'::regclass) AS reclaimed;

-- Round-trip + ordering: enum sorts by declaration order, not text
SELECT severity, tag FROM ts_t_enum
 WHERE severity IS NOT NULL ORDER BY severity, ts;
SELECT count(*) FROM ts_t_enum WHERE severity = 'critical';
-- Range query exercises the underlying enum OID comparison
SELECT count(*) FROM ts_t_enum WHERE severity > 'low';

INSERT INTO ts_t_enum VALUES
    ('2025-01-01 06:00+00', 'critical', 'c');
SELECT time_series.compress_chunks('ts_t_enum'::regclass) AS recompressed;
SELECT count(*) AS post_recompress FROM ts_t_enum;

DROP TABLE ts_t_enum;
DROP TYPE ts_t_severity;


-- ======================================================================
-- Section 11: point (geometry)
-- ======================================================================

CREATE TABLE ts_t_point (
    ts   timestamptz NOT NULL,
    loc  point,
    tag  text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

-- point doesn't have a default btree opclass, so it can't be segmentby
-- or orderby — only a value column.
SELECT time_series.set_compress_config('ts_t_point'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_point VALUES
    ('2025-01-01 01:00+00', point '(1.0, 2.0)',   'a'),
    ('2025-01-01 02:00+00', point '(-3.5, 4.25)', 'a'),
    ('2025-01-01 03:00+00', NULL,                  'b'),
    ('2025-01-01 04:00+00', point '(0, 0)',        'b'),
    ('2025-01-01 05:00+00', point '(1e10, -1e10)', 'a');

SELECT time_series.compress_chunks('ts_t_point'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_point'::regclass) AS reclaimed;

-- Round-trip preserves the float8 components exactly
SELECT loc FROM ts_t_point ORDER BY ts;
-- Distance query exercises a real geometry op post-compression
SELECT count(*) FROM ts_t_point
 WHERE loc IS NOT NULL AND loc <-> point '(0, 0)' < 10.0;

DROP TABLE ts_t_point;


-- ======================================================================
-- Section 12: PAX external toast (>= pax.min_size_of_external_toast)
-- ======================================================================
--
-- Distinct from Section 4's bytea TOAST test: that one only crosses
-- PG's own ~2KB heap TOAST threshold.  This section crosses PAX's own,
-- much larger, internal external-toast threshold (10MB by default,
-- pax.min_size_of_external_toast), which makes OrcWriter spill the
-- value into a "<segfile>.toast" sidecar file separate from the main
-- PAX file.  Round-tripping that sidecar through compress, read-back,
-- and recompress (merging it forward via the prior-PAX reader) is what
-- ts_pax_reader_open_filtered's toast_file wiring and
-- do_compress_one_chunk's ".toast" promotion/rename exist for.

CREATE TABLE ts_t_pax_ext_toast (
    ts    timestamptz NOT NULL,
    blob  text,
    tag   text
) USING time_series WITH (
    ts_partition_column='ts', ts_chunk_interval='1 day',
    ts_chunk_origin='2025-01-01 00:00:00+00'
) DISTRIBUTED REPLICATED;

SELECT time_series.set_compress_config('ts_t_pax_ext_toast'::regclass,
    segmentby => 'tag', orderby => 'ts');

INSERT INTO ts_t_pax_ext_toast VALUES
    ('2025-01-01 01:00+00', repeat('A', 11000000), 'huge'),
    ('2025-01-01 02:00+00', 'short',                'small'),
    ('2025-01-01 03:00+00', NULL,                   'small');

SELECT time_series.compress_chunks('ts_t_pax_ext_toast'::regclass) AS compressed;
SELECT time_series.reclaim_chunk_heaps('ts_t_pax_ext_toast'::regclass) AS reclaimed;

-- Round-trip: length + full byte-for-byte content survive read-back
-- through the promoted ".toast" sidecar, not just the main PAX file.
SELECT length(blob) AS len,
       md5(blob) = md5(repeat('A', 11000000)) AS content_ok
  FROM ts_t_pax_ext_toast WHERE tag = 'huge';
SELECT blob FROM ts_t_pax_ext_toast WHERE tag = 'small' ORDER BY ts;

-- INSERT into COMPRESSED, then recompress: exercises the prior_reader
-- merge picking the externally-toasted value back up from the
-- PROMOTED live ".toast" file (not the ".toast.new" temp one that
-- immediately precedes it) and re-writing it into the next
-- generation's sidecar.
INSERT INTO ts_t_pax_ext_toast VALUES
    ('2025-01-01 04:00+00', repeat('B', 10600000), 'huge2');
SELECT time_series.compress_chunks('ts_t_pax_ext_toast'::regclass) AS recompressed;
SELECT length(blob) AS len,
       md5(blob) = md5(repeat('A', 11000000)) AS content_ok
  FROM ts_t_pax_ext_toast WHERE tag = 'huge';
SELECT length(blob) AS len,
       md5(blob) = md5(repeat('B', 10600000)) AS content_ok
  FROM ts_t_pax_ext_toast WHERE tag = 'huge2';

DROP TABLE ts_t_pax_ext_toast;


-- ======================================================================
-- Cleanup
-- ======================================================================

RESET timezone;
RESET optimizer;
RESET datestyle;
RESET extra_float_digits;
