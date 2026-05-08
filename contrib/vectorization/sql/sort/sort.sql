-- test sort executor

-- set vectorization on

SET vector.enable_vectorization=ON;
SET vector.max_batch_size = 13;

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT) DISTRIBUTED BY(a);

-- load data

INSERT INTO t1 SELECT 1000 - i, i * i - i  FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT * FROM t1 ORDER BY a;
SELECT * FROM t1 ORDER BY b;

-- drop table
DROP TABLE t1;

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT) DISTRIBUTED BY(a);

-- load data

INSERT INTO t1 SELECT i % 11, i * (i / 3) FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT * FROM t1 ORDER BY a,b;

-- drop table
DROP TABLE t1;

CREATE TABLE IF NOT EXISTS t1 (a INT, b TEXT) WITH(APPENDONLY=true, ORIENTATION=column)  DISTRIBUTED BY(a);

-- load data
INSERT INTO t1 SELECT i, i::text FROM GENERATE_SERIES(110, 150)i;
INSERT INTO t1 SELECT i % 7, i::text FROM GENERATE_SERIES(1, 100)i;

-- query
SELECT * FROM t1 ORDER BY b;
SELECT * FROM t1 ORDER BY a,b;

-- drop table
DROP TABLE t1;

CREATE COLLATION c1 (LOCALE = 'en_US.UTF-8');
CREATE TABLE test_locale (a char(8) COLLATE "C", b char(8) COLLATE c1) using ao_column;
INSERT INTO test_locale VALUES ('XXXX', 'A');
INSERT INTO test_locale VALUES ('ABAB', 'b');
INSERT INTO test_locale VALUES ('ABAB', 'c');
INSERT INTO test_locale VALUES ('BBBB', 'D');
INSERT INTO test_locale VALUES ('BBBB', 'e');
INSERT INTO test_locale VALUES ('bbbb', 'F');
INSERT INTO test_locale VALUES ('cccc', 'g');
INSERT INTO test_locale VALUES ('cccc', 'h');
INSERT INTO test_locale VALUES ('CCCC', 'I');
INSERT INTO test_locale VALUES ('CCCC', 'j');
select a from test_locale order by a;
select b from test_locale order by b;
drop table test_locale;
drop COLLATION c1;

-- C.utf8 / C.UTF-8 libc collations sort bytewise (same as the built-in
-- "C") and are common on databases initialized with --lc-collate=C.utf8.
-- is_sort_collation_vectorable() / get_arrow_locale_from_collation()
-- whitelist these names; without that, an ORDER BY on a column declared
-- with one of these collations forces the entire plan back to the row
-- engine.  Pin the Postgres planner so the EXPLAIN plan below is stable.
--
-- glibc < 2.35 (e.g. CentOS 7) doesn't ship a C.utf8 system locale, so
-- CREATE COLLATION would fail with "could not create locale".  Probe
-- via a subtransaction and \quit on platforms that lack it; the skip
-- path is covered by the alternate expected file sort_1.out.
SET optimizer = off;
BEGIN;
DO $$
BEGIN
    EXECUTE 'CREATE COLLATION _probe_c_utf8 (LOCALE = ''C.utf8'')';
EXCEPTION WHEN OTHERS THEN
    NULL;
END$$;
SELECT NOT EXISTS (
    SELECT 1 FROM pg_collation WHERE collname = '_probe_c_utf8'
) AS skip_test \gset
ROLLBACK;
\if :skip_test
\quit
\endif

CREATE COLLATION c_utf8 (LOCALE = 'C.utf8');
CREATE TABLE test_c_utf8_sort (a int, b text COLLATE c_utf8)
    USING pax DISTRIBUTED BY (a);
INSERT INTO test_c_utf8_sort VALUES (1, 'XXXX'), (2, 'ABAB'), (3, 'bbbb'),
                                    (4, 'CCCC'), (5, 'aaaa');

-- Plan must contain Vec Sort.  Without the C.utf8 whitelist entry the
-- planner would fall back to the row engine and the plan would have a
-- plain "Sort" node (and no "Vec ..." operators at all).
EXPLAIN (costs off) SELECT b FROM test_c_utf8_sort ORDER BY b;
SELECT b FROM test_c_utf8_sort ORDER BY b;
drop table test_c_utf8_sort;
drop COLLATION c_utf8;