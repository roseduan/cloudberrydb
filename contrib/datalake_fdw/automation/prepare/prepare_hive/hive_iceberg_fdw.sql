-- Iceberg-on-Hive table fixture for the hive_iceberg_fdw / hive_iceberg_fdw_write
-- smoke sub-tests.  Run via spark-sql with the Iceberg Hive catalog enabled.
--
-- Schema (matches the foreign-table declaration in
--   sqlrepo/smoke/hive/sql/hive_iceberg_fdw.sql):
--   id     bigint
--   name   text
--   amount double precision
--   ts     bigint
--
-- Catalog "ic" is configured at spark-sql invocation:
--   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--   --conf spark.sql.catalog.ic=org.apache.iceberg.spark.SparkCatalog
--   --conf spark.sql.catalog.ic.type=hive
--   --conf spark.sql.catalog.ic.uri=thrift://hive-metastore:9083

DROP TABLE IF EXISTS ic.default.iceberg_fdw_test;

CREATE TABLE ic.default.iceberg_fdw_test (
    id     bigint,
    name   string,
    amount double,
    ts     bigint
) USING iceberg;

INSERT INTO ic.default.iceberg_fdw_test VALUES
    (CAST(1 AS bigint), 'Alice',   CAST(100.00 AS double), CAST(1000 AS bigint)),
    (CAST(2 AS bigint), 'Bob',     CAST(200.50 AS double), CAST(1001 AS bigint)),
    (CAST(3 AS bigint), 'Charlie', CAST( 50.75 AS double), CAST(1002 AS bigint)),
    (CAST(4 AS bigint), 'David',   CAST(300.00 AS double), CAST(1003 AS bigint)),
    (CAST(5 AS bigint), 'Eve',     CAST(150.25 AS double), CAST(1004 AS bigint)),
    (CAST(6 AS bigint), 'Frank',   CAST(175.00 AS double), CAST(1005 AS bigint)),
    (CAST(7 AS bigint), 'Grace',   CAST(225.50 AS double), CAST(1006 AS bigint)),
    (CAST(8 AS bigint), 'Hank',    CAST( 80.00 AS double), CAST(1007 AS bigint));
