-- S3 Avro Test Data Preparation
-- Run via Spark SQL to create Avro files in MinIO/S3
-- These files are read by the s3_avro_smoke.sql test

-- ============================================================
-- Basic Avro data
-- ============================================================
DROP TABLE IF EXISTS avro_basic_temp;
CREATE TABLE avro_basic_temp (id INT, name STRING)
USING avro
LOCATION 's3a://test-data/avro/basic/';

INSERT INTO avro_basic_temp VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie');

DROP TABLE avro_basic_temp;

-- ============================================================
-- Avro with multiple types
-- ============================================================
DROP TABLE IF EXISTS avro_types_temp;
CREATE TABLE avro_types_temp (
    id INT,
    int_val INT,
    bigint_val BIGINT,
    float_val FLOAT,
    double_val DOUBLE,
    str_val STRING
)
USING avro
LOCATION 's3a://test-data/avro/types/';

INSERT INTO avro_types_temp VALUES
    (1, 100, 9999999999, 3.14, 2.718281828, 'hello'),
    (2, -100, -9999999999, -3.14, -2.718281828, 'world'),
    (3, 0, 0, 0.0, 0.0, '');

DROP TABLE avro_types_temp;

-- ============================================================
-- Avro with NULL values
-- ============================================================
DROP TABLE IF EXISTS avro_nulls_temp;
CREATE TABLE avro_nulls_temp (id INT, val1 STRING, val2 INT)
USING avro
LOCATION 's3a://test-data/avro/nulls/';

INSERT INTO avro_nulls_temp VALUES
    (1, 'has_val', 100),
    (2, NULL, 200),
    (3, 'no_int', NULL),
    (4, NULL, NULL);

DROP TABLE avro_nulls_temp;
