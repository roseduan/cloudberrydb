-- S3 Smoke Test Data Preparation
-- Run via beeline to create test data in MinIO for S3 interoperability testing
-- Hive writes data in multiple formats to S3, Cloudberry reads via datalake_fdw
--
-- IMPORTANT: Use EXTERNAL TABLE to avoid Hive 3.0 ACID transactional format.
-- Managed tables write delta directories that datalake_fdw cannot read.
-- Use external.table.purge=true so DROP TABLE also deletes S3 data,
-- ensuring clean state on each run.

-- Drop old tables (purge deletes S3 data too)
DROP TABLE IF EXISTS s3_test_parquet;
DROP TABLE IF EXISTS s3_test_avro;
DROP TABLE IF EXISTS s3_test_orc;
DROP TABLE IF EXISTS s3_test_text;
DROP TABLE IF EXISTS s3_test_csv;

-- Parquet
CREATE EXTERNAL TABLE s3_test_parquet (id INT, name STRING, amount DECIMAL(10,2))
STORED AS PARQUET
LOCATION 's3a://warehouse/s3-smoke-hive-parquet/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO s3_test_parquet VALUES
    (1, 'Alice', 100.50),
    (2, 'Bob', 200.75),
    (3, 'Charlie', 300.00);

SELECT * FROM s3_test_parquet;

-- Avro
CREATE EXTERNAL TABLE s3_test_avro (id INT, name STRING)
STORED AS AVRO
LOCATION 's3a://warehouse/s3-smoke-hive-avro/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO s3_test_avro VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie');

SELECT * FROM s3_test_avro;

-- ORC
CREATE EXTERNAL TABLE s3_test_orc (id INT, name STRING, amount DECIMAL(10,2))
STORED AS ORC
LOCATION 's3a://warehouse/s3-smoke-hive-orc/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO s3_test_orc VALUES
    (1, 'Alice', 100.50),
    (2, 'Bob', 200.75),
    (3, 'Charlie', 300.00);

SELECT * FROM s3_test_orc;

-- Text (Hive TEXTFILE = LazySimpleSerDe, fields delimited by \001 by default)
CREATE EXTERNAL TABLE s3_test_text (id INT, name STRING, amount DECIMAL(10,2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3a://warehouse/s3-smoke-hive-text/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO s3_test_text VALUES
    (1, 'Alice', 100.50),
    (2, 'Bob', 200.75),
    (3, 'Charlie', 300.00);

SELECT * FROM s3_test_text;

-- CSV (using OpenCSVSerde)
CREATE EXTERNAL TABLE s3_test_csv (id STRING, name STRING, amount STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',', 'quoteChar'='"')
STORED AS TEXTFILE
LOCATION 's3a://warehouse/s3-smoke-hive-csv/'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO s3_test_csv VALUES
    ('1', 'Alice', '100.50'),
    ('2', 'Bob', '200.75'),
    ('3', 'Charlie', '300.00');

SELECT * FROM s3_test_csv;
