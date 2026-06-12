-- 03_char_types.sql
-- Iceberg CHAR(N) full coverage (issue #340 regression).
--
-- Iceberg has no fixed-length CHAR type, so CHAR(N) is stored as Iceberg
-- `string` with trailing spaces stripped on disk (clean for Spark / Trino /
-- PrestoDB).  On the PG side, CHAR(N) semantics are preserved: the value is
-- blank-padded back to the declared length N on read (issue #321), mirroring
-- PG's bpchar() coercion, so length / octet_length / comparison / concat are
-- identical to a heap CHAR(N) column.  Issue #340 (empty string on SELECT) and
-- issue #321 (trailing-space loss) are both pinned by this round-trip file.
--
-- NOTE on expected output: capture expected/03_char_types.out by running this
-- file once against a real catalog/volume (`make installcheck` from this
-- automation directory).  Hand-authoring it is fragile because the psql
-- column widths depend on the actual returned bpchar bytes.

\i ../../../lib/sql/common_setup.sql

SELECT test_log('Feature Test: Iceberg CHAR(N) Full Coverage');

-- ============================================================
-- Setup: Catalog and volume
-- ============================================================
CREATE SERVER ct_catalog_server FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER ct_catalog_server;
CREATE FOREIGN CATALOG ct_catalog SERVER ct_catalog_server;
SET iceberg_default_catalog = 'ct_catalog';

CREATE SERVER ct_volume_server FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER ct_volume_server
OPTIONS (access_key_id 'admin', secret_access_key 'admin12345');
CREATE FOREIGN VOLUME ct_volume SERVER ct_volume_server OPTIONS(base_path '/ct_volume/');
SET iceberg_default_volume = 'ct_volume';

-- ============================================================
-- Test 1: default CHAR (= CHAR(1))
-- ============================================================
SELECT test_log('Test 1: default CHAR (no length spec)');

CREATE ICEBERG TABLE ct_default (id bigint, c char);
INSERT INTO ct_default VALUES (1, 'A'), (2, ' '), (3, NULL);
SELECT id, length(c) AS chars, octet_length(c) AS bytes, c IS NULL AS is_null
FROM ct_default ORDER BY id;
DROP TABLE ct_default;

-- ============================================================
-- Test 2: CHAR(1)
-- ============================================================
SELECT test_log('Test 2: CHAR(1)');

CREATE ICEBERG TABLE ct_c1 (id bigint, c char(1));
INSERT INTO ct_c1 VALUES (1, 'A'), (2, 'Z'), (3, ' '), (4, NULL);
SELECT id, length(c) AS chars, octet_length(c) AS bytes, c IS NULL AS is_null
FROM ct_c1 ORDER BY id;
DROP TABLE ct_c1;

-- ============================================================
-- Test 3: CHAR(5) -- the canonical issue #340 repro shape
-- Iceberg stores the trimmed value on disk, but the reader re-pads to N, so
-- octet_length() reflects the typmod N exactly like a heap CHAR(5) column.
-- ============================================================
SELECT test_log('Test 3: CHAR(5) basic shapes');

CREATE ICEBERG TABLE ct_c5 (label text, c char(5));
INSERT INTO ct_c5 VALUES
    ('1_full',    'ABCDE'),
    ('2_pad_1',   'A'),
    ('3_pad_4sp', 'A    '),
    ('4_all_sp',  '     '),
    ('5_empty',   ''),
    ('6_null',    NULL);
SELECT label, length(c) AS chars, octet_length(c) AS bytes, c IS NULL AS is_null
FROM ct_c5 ORDER BY label;
DROP TABLE ct_c5;

-- ============================================================
-- Test 4: CHAR(100) -- longer values exercise buffer resize paths
-- ============================================================
SELECT test_log('Test 4: CHAR(100)');

CREATE ICEBERG TABLE ct_c100 (id bigint, c char(100));
INSERT INTO ct_c100 VALUES
    (1, repeat('x', 80)),       -- 80 chars, no trailing space
    (2, repeat('y', 100)),      -- full 100 chars
    (3, repeat(' ', 100)),      -- 100 spaces -> empty on disk, re-padded to 100 on read
    (4, 'short');
SELECT id, length(c) AS chars, octet_length(c) AS bytes,
       (left(c, 5) = 'xxxxx' OR left(c, 5) = 'yyyyy' OR left(c, 5) = 'short' OR left(c, 5) = '') AS sample_ok
FROM ct_c100 ORDER BY id;
DROP TABLE ct_c100;

-- ============================================================
-- Test 5: UTF-8 / multi-byte -- length() counts characters,
-- octet_length() counts bytes.  Values are blank-padded to N chars on read,
-- so octet_length = byte_len(value) + (N - char_len) single-byte spaces.
-- ============================================================
SELECT test_log('Test 5: UTF-8 multi-byte');

CREATE ICEBERG TABLE ct_utf8 (id bigint, c char(10));
INSERT INTO ct_utf8 VALUES
    (1, '测试'),            -- 2 chars, 6 bytes
    (2, 'a测试b'),          -- 4 chars, 8 bytes
    (3, '😀'),              -- 1 char, 4 bytes
    (4, 'plain');           -- 5 chars, 5 bytes (ASCII baseline)
SELECT id, length(c) AS chars, octet_length(c) AS bytes
FROM ct_utf8 ORDER BY id;
DROP TABLE ct_utf8;

-- ============================================================
-- Test 6: special whitespace -- only trailing ASCII space (0x20) is stripped
-- on disk (then re-padded to N on read); tab / newline / CR are kept verbatim.
-- ============================================================
SELECT test_log('Test 6: tab / CR / LF preserved (only space trimmed)');

CREATE ICEBERG TABLE ct_ws (id bigint, c char(10));
INSERT INTO ct_ws VALUES
    (1, E'tab\there'),      -- 8 chars: t a b \t h e r e
    (2, E'cr\rlf\n'),       -- 6 chars: c r \r l f \n  (no trailing space)
    (3, E'mix\t '),         -- PG pads to 10; trailing space stripped on disk, re-padded to 10 on read
    (4, 'noend');           -- 5 chars baseline
SELECT id, length(c) AS chars, octet_length(c) AS bytes
FROM ct_ws ORDER BY id;
DROP TABLE ct_ws;

-- ============================================================
-- Test 7: mixed NULL / non-NULL in a single column
-- ============================================================
SELECT test_log('Test 7: mixed NULL / non-NULL');

CREATE ICEBERG TABLE ct_nulls (id bigint, c char(5));
INSERT INTO ct_nulls VALUES
    (1, 'first'),
    (2, NULL),
    (3, 'three'),
    (4, NULL),
    (5, 'fifth');
SELECT count(*) FILTER (WHERE c IS NULL)     AS null_cnt,
       count(*) FILTER (WHERE c IS NOT NULL) AS not_null_cnt,
       count(*)                              AS total
FROM ct_nulls;
DROP TABLE ct_nulls;

-- ============================================================
-- Test 8: Large batch -- crosses BATCH_WRITE_SIZE / row-group boundary
-- ============================================================
SELECT test_log('Test 8: large batch (cross batch / row-group)');

CREATE ICEBERG TABLE ct_big (id bigint, c char(5));
INSERT INTO ct_big
SELECT g,
       CASE WHEN g % 3 = 0 THEN 'ABCDE'
            WHEN g % 3 = 1 THEN 'A'
            ELSE NULL
       END
FROM generate_series(1, 5000) AS g;

SELECT count(*) AS total_rows,
       count(*) FILTER (WHERE c = 'ABCDE') AS exact_full,
       count(*) FILTER (WHERE c = 'A')     AS exact_short,
       count(*) FILTER (WHERE c IS NULL)   AS nulls
FROM ct_big;
DROP TABLE ct_big;

-- ============================================================
-- Test 9: INSERT ... SELECT from heap
-- ============================================================
SELECT test_log('Test 9: INSERT ... SELECT FROM heap');

CREATE TABLE ct_heap_src (id bigint, c char(5));
INSERT INTO ct_heap_src VALUES
    (1, 'ABCDE'), (2, 'A'), (3, '   '), (4, NULL);

CREATE ICEBERG TABLE ct_from_heap (id bigint, c char(5));
INSERT INTO ct_from_heap SELECT * FROM ct_heap_src;
SELECT id, length(c) AS chars, octet_length(c) AS bytes, c IS NULL AS is_null
FROM ct_from_heap ORDER BY id;
DROP TABLE ct_from_heap;
DROP TABLE ct_heap_src;

-- ============================================================
-- Test 10: filter / sort / group / aggregate
-- ============================================================
SELECT test_log('Test 10: filter / sort / aggregate');

CREATE ICEBERG TABLE ct_query (id bigint, c char(5));
INSERT INTO ct_query VALUES
    (1, 'ABCDE'),
    (2, 'ABCDE'),
    (3, 'A'),
    (4, 'A    '),    -- on disk equals 'A' after trim
    (5, 'BB'),
    (6, NULL);

SELECT count(*) AS eq_abcde       FROM ct_query WHERE c = 'ABCDE';
SELECT count(*) AS eq_a           FROM ct_query WHERE c = 'A';
SELECT count(*) AS is_null_cnt    FROM ct_query WHERE c IS NULL;
SELECT count(*) AS is_not_null    FROM ct_query WHERE c IS NOT NULL;
SELECT count(*) AS like_a_pct     FROM ct_query WHERE c LIKE 'A%';
SELECT count(DISTINCT c)          AS distinct_cnt,
       min(c)                     AS min_c,
       max(c)                     AS max_c
FROM ct_query;
DROP TABLE ct_query;

-- ============================================================
-- Test 11: string functions
-- ============================================================
SELECT test_log('Test 11: string functions');

CREATE ICEBERG TABLE ct_fn (id bigint, c char(5));
INSERT INTO ct_fn VALUES
    (1, 'ABCDE'),
    (2, 'aBcDe'),
    (3, 'A'),
    (4, NULL);
SELECT id,
       upper(c)                   AS up,
       lower(c)                   AS lo,
       substring(c FROM 1 FOR 3)  AS sub13
FROM ct_fn ORDER BY id;
DROP TABLE ct_fn;

-- ============================================================
-- Test 12: UPDATE / DELETE on CHAR(N)
-- ============================================================
SELECT test_log('Test 12: UPDATE / DELETE');

CREATE ICEBERG TABLE ct_dml (id bigint, label text, c char(5));
INSERT INTO ct_dml VALUES
    (1, 'a', 'ABCDE'),
    (2, 'b', 'A'),
    (3, 'c', NULL),
    (4, 'd', 'ZZZZ');

UPDATE ct_dml SET c = 'XYZ' WHERE label = 'b';
SELECT id, label, length(c) AS chars, octet_length(c) AS bytes
FROM ct_dml ORDER BY id;

DELETE FROM ct_dml WHERE c IS NULL;
SELECT count(*) AS remaining FROM ct_dml;
DROP TABLE ct_dml;

-- ============================================================
-- Test 13: mixed types in one wide table -- CHAR cells must not
-- corrupt adjacent columns.
-- ============================================================
SELECT test_log('Test 13: mixed types in one table');

CREATE ICEBERG TABLE ct_mixed (
    id   bigint,
    t    text,
    v    varchar(10),
    c5   char(5),
    c1   char,
    n    int,
    d    date
);
INSERT INTO ct_mixed VALUES
    (1, 'hello', 'world',     'ABCDE', 'X', 42,  '2026-01-15'),
    (2, NULL,    'short',     'A',     ' ', 0,   '1970-01-01'),
    (3, 'mix',   NULL,        '   ',   'Y', -1,  NULL),
    (4, '',      '',          NULL,    NULL, NULL, '2099-12-31');

SELECT id, t, v,
       length(c5) AS c5_chars, octet_length(c5) AS c5_bytes,
       length(c1) AS c1_chars, octet_length(c1) AS c1_bytes,
       n, d
FROM ct_mixed ORDER BY id;
DROP TABLE ct_mixed;

SELECT test_log('Feature Test: Iceberg CHAR(N) Full Coverage - DONE');
