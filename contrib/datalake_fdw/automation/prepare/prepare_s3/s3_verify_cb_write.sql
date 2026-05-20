-- Verify Hive can read CB-written data on S3
-- Run via beeline AFTER pg_regress s3_write_smoke completes

-- CB-written Parquet
DROP TABLE IF EXISTS cb_verify_parquet;
CREATE EXTERNAL TABLE cb_verify_parquet (id INT, name STRING, amount DECIMAL(10,2))
STORED AS PARQUET
LOCATION 's3a://warehouse/smoke-write-test/parquet/';
SELECT 'parquet' AS fmt, COUNT(*) AS cnt FROM cb_verify_parquet;

-- CB-written Avro
DROP TABLE IF EXISTS cb_verify_avro;
CREATE EXTERNAL TABLE cb_verify_avro (id INT, name STRING)
STORED AS AVRO
LOCATION 's3a://warehouse/smoke-write-test/avro/';
SELECT 'avro' AS fmt, COUNT(*) AS cnt FROM cb_verify_avro;

-- CB-written Text
DROP TABLE IF EXISTS cb_verify_text;
CREATE EXTERNAL TABLE cb_verify_text (id INT, name STRING, amount DECIMAL(10,2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3a://warehouse/smoke-write-test/text/';
SELECT 'text' AS fmt, COUNT(*) AS cnt FROM cb_verify_text;

-- CB-written CSV
DROP TABLE IF EXISTS cb_verify_csv;
CREATE EXTERNAL TABLE cb_verify_csv (id STRING, name STRING, amount STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',')
STORED AS TEXTFILE
LOCATION 's3a://warehouse/smoke-write-test/csv/';
SELECT 'csv' AS fmt, COUNT(*) AS cnt FROM cb_verify_csv;

-- Cleanup Hive metadata
DROP TABLE IF EXISTS cb_verify_parquet;
DROP TABLE IF EXISTS cb_verify_avro;
DROP TABLE IF EXISTS cb_verify_text;
DROP TABLE IF EXISTS cb_verify_csv;
