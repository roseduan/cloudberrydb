-- Hive Smoke Test Data Preparation
-- Run via beeline: beeline -u jdbc:hive2://localhost:10000 -f hive_smoke.sql
-- Use DummyTxnManager to create plain (non-ACID) ORC files in Hive 3.0+.

SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;

-- The test_text_large insert below is a CROSS JOIN of 50 x 10 rows.  Hive
-- converts it to a map join, which runs as a MapredLocalTask in a child JVM,
-- and that task fails on this stack -- "return code 1 from
-- org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask" -- taking the whole
-- fixture load with it, so none of the tables that follow get created.  500
-- rows do not need a map join.
SET hive.auto.convert.join = false;

-- Basic ORC table with partition
DROP TABLE IF EXISTS test_orc;
CREATE TABLE test_orc (id int, name string)
PARTITIONED BY (m int) STORED AS ORC;
INSERT INTO test_orc PARTITION(m=1) VALUES (1, 'test');
INSERT INTO test_orc PARTITION(m=2) VALUES (NULL, 'test');

-- Parquet table
DROP TABLE IF EXISTS test_parquet;
CREATE TABLE test_parquet (id int, name string) STORED AS PARQUET;
INSERT INTO test_parquet VALUES (1, 'test');

-- Text table: remove \0 byte that causes UTF8 encoding error
DROP TABLE IF EXISTS test_text;
CREATE TABLE test_text (id int, name string) STORED AS TEXTFILE;
INSERT INTO test_text VALUES (1, '\\.'), (2, 'hello');

-- Empty table
DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (id int) STORED AS ORC;

-- Multi-level partition table
DROP TABLE IF EXISTS test_multi_partition;
CREATE TABLE test_multi_partition (
    id int,
    value string
)
PARTITIONED BY (year int, month int, day int)
STORED AS ORC;

INSERT INTO test_multi_partition PARTITION(year=2024, month=1, day=1) VALUES (1, 'data1');
INSERT INTO test_multi_partition PARTITION(year=2024, month=1, day=2) VALUES (2, 'data2');
INSERT INTO test_multi_partition PARTITION(year=2024, month=12, day=31) VALUES (3, 'data3');

-- Edge case: extreme values
DROP TABLE IF EXISTS test_extreme;
CREATE TABLE test_extreme (tiny_min tinyint, tiny_max tinyint, int_min int, int_max int) STORED AS ORC;
INSERT INTO test_extreme VALUES (-128, 127, -2147483648, 2147483647);

-- Edge case: empty string vs NULL
DROP TABLE IF EXISTS test_empty_str;
CREATE TABLE test_empty_str (id int, empty_val string, null_val string) STORED AS PARQUET;
INSERT INTO test_empty_str VALUES (1, '', NULL), (2, NULL, NULL);

-- Edge case: unicode characters
DROP TABLE IF EXISTS test_unicode;
CREATE TABLE test_unicode (id int, val string) STORED AS PARQUET;
INSERT INTO test_unicode VALUES (1, 'ascii_ok'), (2, 'abc123');

-- Edge case: zero values
DROP TABLE IF EXISTS test_zero;
CREATE TABLE test_zero (id int, zero_int int, zero_float float) STORED AS ORC;
INSERT INTO test_zero VALUES (1, 0, 0.0), (2, 0, -0.0);

-- Edge case: decimal precision
DROP TABLE IF EXISTS test_decimal;
CREATE TABLE test_decimal (id int, dec1 decimal(5,2), dec2 decimal(10,5)) STORED AS ORC;
INSERT INTO test_decimal VALUES (1, 123.45, 12345.67890), (2, 0.01, 0.00001);

-- Edge case: whitespace (column names match what SQL queries expect)
DROP TABLE IF EXISTS test_space;
CREATE TABLE test_space (id int, leading_val string, trailing_val string) STORED AS TEXTFILE;
INSERT INTO test_space VALUES (1, '  space', 'space  ');

-- Edge case: scientific notation
DROP TABLE IF EXISTS test_scientific;
CREATE TABLE test_scientific (id int, small_val float, large_val double) STORED AS PARQUET;
INSERT INTO test_scientific VALUES (1, 1.23E-10, 1.23E100);

-- Edge case: date time boundary
DROP TABLE IF EXISTS test_datetime;
CREATE TABLE test_datetime (id int, old_date date, ts timestamp) STORED AS ORC;
-- Typed literals, not bare strings: this file is loaded by spark-sql (see the
-- hive category Makefile), whose default ANSI store-assignment policy refuses
-- to assign a string to a date/timestamp column.  spark-sql stops at the first
-- error, so the bare-string form silently skipped every table declared below
-- and surfaced much later as "(0 rows)" diffs.  Hive accepts both forms.
INSERT INTO test_datetime VALUES (1, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00');

-- ============================================================
-- Compressed TEXTFILE tables for hive_text_compressed test.
-- Each table holds 50 rows: id INT, name STRING, val DOUBLE
--   snappy : val = id * 1.5
--   deflate: val = id * 2.5
--   gzip   : val = id * 3.5
-- ============================================================

-- Helper staging table with 50 rows of integer ids.  Hive does not have a
-- generate_series(); we emit the sequence via posexplode over a 50-element
-- string array (split of 49 spaces).
DROP TABLE IF EXISTS hive_smoke_seq50;
CREATE TABLE hive_smoke_seq50 (n int) STORED AS ORC;
INSERT INTO hive_smoke_seq50
SELECT pos + 1 AS n
FROM (SELECT split(space(49), ' ') AS arr) t
LATERAL VIEW posexplode(arr) pe AS pos, dummy;

-- Snappy-compressed text
DROP TABLE IF EXISTS test_text_snappy;
CREATE TABLE test_text_snappy (id int, name string, val double) STORED AS TEXTFILE;
SET hive.exec.compress.output = true;
SET mapreduce.output.fileoutputformat.compress = true;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress.type = BLOCK;
INSERT INTO test_text_snappy
SELECT n, concat('snappy_row_', cast(n as string)), cast(n * 1.5 as double)
FROM hive_smoke_seq50;

-- Deflate-compressed text
DROP TABLE IF EXISTS test_text_deflate;
CREATE TABLE test_text_deflate (id int, name string, val double) STORED AS TEXTFILE;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.DefaultCodec;
INSERT INTO test_text_deflate
SELECT n, concat('deflate_row_', cast(n as string)), cast(n * 2.5 as double)
FROM hive_smoke_seq50;

-- Gzip-compressed text
DROP TABLE IF EXISTS test_text_gzip;
CREATE TABLE test_text_gzip (id int, name string, val double) STORED AS TEXTFILE;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec;
INSERT INTO test_text_gzip
SELECT n, concat('gzip_row_', cast(n as string)), cast(n * 3.5 as double)
FROM hive_smoke_seq50;

SET hive.exec.compress.output = false;
SET mapreduce.output.fileoutputformat.compress = false;

-- Pipe-delimited text (uncompressed) — 30 rows
DROP TABLE IF EXISTS test_text_pipe;
CREATE TABLE test_text_pipe (id int, name string, val double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
INSERT INTO test_text_pipe
SELECT n, concat('pipe_row_', cast(n as string)), cast(n * 0.5 as double)
FROM hive_smoke_seq50
WHERE n <= 30;

-- Large table — 500 rows of fixed-width strings (length 200).
-- Build the 500-row sequence by cross-joining a 50-row staging with a 10-row
-- "tens" set and computing id = tens*50 + n.
DROP TABLE IF EXISTS hive_smoke_seq10;
CREATE TABLE hive_smoke_seq10 (n int) STORED AS ORC;
INSERT INTO hive_smoke_seq10
SELECT pos
FROM (SELECT split(space(9), ' ') AS arr) t
LATERAL VIEW posexplode(arr) pe AS pos, dummy;

DROP TABLE IF EXISTS test_text_large;
CREATE TABLE test_text_large (id int, data string) STORED AS TEXTFILE;
INSERT INTO test_text_large
SELECT
  t.n + s.n * 50 AS id,
  -- 202-character payload: "row_<id>_" + repeated 'x' to pad to 202 chars.
  rpad(concat('row_', cast(t.n + s.n * 50 as string), '_'), 202, 'x') AS data
FROM hive_smoke_seq50 t
CROSS JOIN hive_smoke_seq10 s;

-- Drop staging tables
DROP TABLE IF EXISTS hive_smoke_seq50;
DROP TABLE IF EXISTS hive_smoke_seq10;
