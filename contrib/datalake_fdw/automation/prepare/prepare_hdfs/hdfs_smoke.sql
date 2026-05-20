-- HDFS Smoke Test Data Preparation
-- Run via beeline: beeline -u jdbc:hive2://hiveserver2:10000/default -n hive -f hdfs_smoke.sql
-- Creates test data on HDFS that GPHDFS external tables can read.
--
-- IMPORTANT: Use EXTERNAL TABLE with external.table.purge=true so
-- DROP TABLE also deletes HDFS data, ensuring clean state on each run.

SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;

-- ============================================================
-- Text data for 02_text_basic.sql read test
-- Path: /test/basic/text/simple (tab-delimited)
-- ============================================================
DROP TABLE IF EXISTS hdfs_smoke_text_simple;
CREATE EXTERNAL TABLE hdfs_smoke_text_simple (id INT, col STRING, col2 DECIMAL(10,2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://hadoop:8020/test/basic/text/simple'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO hdfs_smoke_text_simple VALUES
    (1, 'alice', 10.50),
    (2, 'bob', 20.75),
    (3, 'charlie', 30.00),
    (4, 'diana', 40.25),
    (5, 'eve', 50.50),
    (6, 'frank', 60.75),
    (7, 'grace', 70.00),
    (8, 'henry', 80.25),
    (9, 'iris', 90.50),
    (10, 'jack', 100.75);

SELECT 'hdfs_smoke_text_simple' AS table_name, COUNT(*) AS row_count FROM hdfs_smoke_text_simple;

-- ============================================================
-- Parquet data for 08_datatypes.sql pre-existing read test
-- Path: /test/basic/datatypes/preload_parquet
-- ============================================================
DROP TABLE IF EXISTS hdfs_smoke_datatypes;
CREATE EXTERNAL TABLE hdfs_smoke_datatypes (
    id INT,
    tiny_val TINYINT,
    small_val SMALLINT,
    int_val INT,
    big_val BIGINT,
    float_val FLOAT,
    double_val DOUBLE,
    dec_val DECIMAL(10,2),
    str_val STRING,
    bool_val BOOLEAN,
    date_val DATE,
    ts_val TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://hadoop:8020/test/basic/datatypes/preload_parquet'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO hdfs_smoke_datatypes VALUES
    (1, 1, 100, 10000, 1000000000, 1.5, 1.123456789, 99.99, 'hello', true, '2024-01-01', '2024-01-01 12:00:00'),
    (2, -1, -100, -10000, -1000000000, -1.5, -1.123456789, -99.99, 'world', false, '1970-01-01', '1970-01-01 00:00:00'),
    (3, 0, 0, 0, 0, 0.0, 0.0, 0.00, '', true, '2000-02-29', '2000-02-29 12:30:45'),
    (4, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.141592653589, 12345.67, 'max_vals', false, '2099-12-31', '2099-12-31 23:59:59'),
    (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT 'hdfs_smoke_datatypes' AS table_name, COUNT(*) AS row_count FROM hdfs_smoke_datatypes;

-- ============================================================
-- ORC data for 09_edge_cases.sql pre-existing read test
-- Path: /test/basic/edge/preload_orc
-- ============================================================
DROP TABLE IF EXISTS hdfs_smoke_edge;
CREATE EXTERNAL TABLE hdfs_smoke_edge (
    id INT,
    empty_val STRING,
    null_val STRING,
    zero_int INT,
    zero_float DOUBLE,
    dec_precise DECIMAL(18,6),
    date_boundary DATE,
    ts_boundary TIMESTAMP
)
STORED AS ORC
LOCATION 'hdfs://hadoop:8020/test/basic/edge/preload_orc'
TBLPROPERTIES ('external.table.purge'='true');

INSERT INTO hdfs_smoke_edge VALUES
    (1, '', NULL, 0, 0.0, 0.000001, '1970-01-01', '1970-01-01 00:00:00'),
    (2, NULL, NULL, -2147483648, -0.0, -999999999999.999999, '2099-12-31', '2099-12-31 23:59:59'),
    (3, 'not_empty', 'not_null', 2147483647, 1.23E100, 999999999999.999999, '2000-02-29', '2000-02-29 00:00:00'),
    (4, '  spaces  ', '  leading', 1, -1.23E-10, 0.000000, '2024-06-15', '2024-06-15 12:30:45');

SELECT 'hdfs_smoke_edge' AS table_name, COUNT(*) AS row_count FROM hdfs_smoke_edge;
