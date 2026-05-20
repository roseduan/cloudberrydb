-- S3 Performance Test Data Preparation
-- Run via beeline:
--   beeline -u 'jdbc:hive2://hiveserver2:10000/default' -n gpadmin -f s3_perf_data.sql
--
-- Creates 10K-row ORC and Parquet tables at the paths expected by
-- sqlrepo/performance/format/sql/s3_format_compare.sql and
-- sqlrepo/performance/s3/sql/s3_read_performance.sql:
--   /perf-data/orc/10k/
--   /perf-data/parquet/10k/
--   /perf-data/multifile/        (multiple small ORC files)
--   /perf-data/text/large.txt    (text format for HDFS throughput test)
--
-- IMPORTANT: Use EXTERNAL TABLE to avoid Hive 3.0 ACID transactional format.
-- Managed tables write delta directories that datalake_fdw cannot read.

-- ============================================================
-- Cleanup old data
-- ============================================================
DROP TABLE IF EXISTS perf_orc_10k;
DROP TABLE IF EXISTS perf_parquet_10k;
DROP TABLE IF EXISTS perf_multifile;
DROP TABLE IF EXISTS perf_text_large;

-- ============================================================
-- ORC 10K rows
-- ============================================================
CREATE EXTERNAL TABLE perf_orc_10k (
    id INT,
    name STRING,
    value DECIMAL(10,2)
)
STORED AS ORC
LOCATION 's3a://warehouse/perf-data/orc/10k/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_orc_10k
SELECT
    t.id,
    concat('name_', cast(t.id AS STRING)),
    cast(t.id * 1.5 AS DECIMAL(10,2))
FROM (
    SELECT explode(sequence(1, 10000)) AS id
) t;

SELECT count(*) AS orc_10k_count FROM perf_orc_10k;

-- ============================================================
-- Parquet 10K rows (same schema + data as ORC for fair comparison)
-- ============================================================
CREATE EXTERNAL TABLE perf_parquet_10k (
    id INT,
    name STRING,
    value DECIMAL(10,2)
)
STORED AS PARQUET
LOCATION 's3a://warehouse/perf-data/parquet/10k/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_parquet_10k
SELECT * FROM perf_orc_10k;

SELECT count(*) AS parquet_10k_count FROM perf_parquet_10k;

-- ============================================================
-- Multi-file ORC (for s3_read_performance multifile_scan test)
-- Split into ~10 small files by inserting in batches
-- ============================================================
CREATE EXTERNAL TABLE perf_multifile (
    id INT,
    data STRING
)
STORED AS ORC
LOCATION 's3a://warehouse/perf-data/multifile/'
TBLPROPERTIES ('external.table.purge'='true');

-- Hive will create one file per INSERT statement
INSERT INTO perf_multifile SELECT t.id, concat('data_', cast(t.id AS STRING)) FROM (SELECT explode(sequence(1, 1000)) AS id) t;
INSERT INTO perf_multifile SELECT t.id, concat('data_', cast(t.id AS STRING)) FROM (SELECT explode(sequence(1001, 2000)) AS id) t;
INSERT INTO perf_multifile SELECT t.id, concat('data_', cast(t.id AS STRING)) FROM (SELECT explode(sequence(2001, 3000)) AS id) t;
INSERT INTO perf_multifile SELECT t.id, concat('data_', cast(t.id AS STRING)) FROM (SELECT explode(sequence(3001, 4000)) AS id) t;
INSERT INTO perf_multifile SELECT t.id, concat('data_', cast(t.id AS STRING)) FROM (SELECT explode(sequence(4001, 5000)) AS id) t;

SELECT count(*) AS multifile_count FROM perf_multifile;

-- ============================================================
-- Text format (for hdfs_throughput_performance text scan test)
-- ============================================================
CREATE EXTERNAL TABLE perf_text_large (
    id INT,
    data STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3a://warehouse/perf-data/text/large/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_text_large
SELECT t.id, concat('text_row_', cast(t.id AS STRING))
FROM (
    SELECT explode(sequence(1, 10000)) AS id
) t;

SELECT count(*) AS text_large_count FROM perf_text_large;

-- ============================================================
-- Also create Hive perf tables for hive_scan_performance tests
-- ============================================================
DROP TABLE IF EXISTS perf_orc_10k_hive;
DROP TABLE IF EXISTS perf_parquet_10k_hive;
DROP TABLE IF EXISTS perf_write_test;

-- ORC table for Hive scan tests (uses default Hive warehouse, not S3 perf-data)
CREATE EXTERNAL TABLE perf_orc_10k_hive (
    id INT,
    name STRING,
    value DECIMAL(10,2),
    category STRING
)
STORED AS ORC
LOCATION 's3a://warehouse/hive-perf/orc/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_orc_10k_hive
SELECT
    t.id,
    concat('name_', cast(t.id AS STRING)),
    cast(t.id * 1.5 AS DECIMAL(10,2)),
    concat('cat_', cast(t.id % 10 AS STRING))
FROM (
    SELECT explode(sequence(1, 10000)) AS id
) t;

SELECT count(*) AS hive_orc_count FROM perf_orc_10k_hive;

-- Parquet table for Hive scan tests
CREATE EXTERNAL TABLE perf_parquet_10k_hive (
    id INT,
    name STRING,
    value DECIMAL(10,2),
    category STRING
)
STORED AS PARQUET
LOCATION 's3a://warehouse/hive-perf/parquet/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_parquet_10k_hive
SELECT * FROM perf_orc_10k_hive;

SELECT count(*) AS hive_parquet_count FROM perf_parquet_10k_hive;

-- Writable test table for hive_write_performance
CREATE EXTERNAL TABLE perf_write_test (
    id INT,
    name STRING
)
STORED AS ORC
LOCATION 's3a://warehouse/hive-perf/write-test/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO perf_write_test VALUES (1, 'seed');

SELECT count(*) AS write_test_count FROM perf_write_test;

-- ============================================================
-- Summary
-- ============================================================
SELECT 'Data preparation complete' AS status;
