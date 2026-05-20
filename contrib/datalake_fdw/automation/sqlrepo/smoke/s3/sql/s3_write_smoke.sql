-- S3/MinIO Write Smoke Test: CB writes -> CB reads (round-trip, all formats)
-- Also prepares data for CB->Hive interop verification (Makefile post-step)
-- Note: runs after s3_smoke in same database, FDW already exists

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- Create S3 server (MinIO)
DROP SERVER IF EXISTS s3_write_server CASCADE;
CREATE SERVER s3_write_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER s3_write_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: Parquet write + read-back
-- ============================================================
CREATE FOREIGN TABLE s3_w_parquet (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/parquet/', format 'parquet');

INSERT INTO s3_w_parquet SELECT i, 'item_' || i, i * 10.50 FROM generate_series(1, 5) i;

CREATE FOREIGN TABLE s3_r_parquet (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/parquet/', format 'parquet');

SELECT * FROM s3_r_parquet ORDER BY id;

DROP FOREIGN TABLE s3_w_parquet;
DROP FOREIGN TABLE s3_r_parquet;

-- ============================================================
-- Test 2: Avro write + read-back
-- ============================================================
CREATE FOREIGN TABLE s3_w_avro (id int, name text)
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/avro/', format 'avro');

INSERT INTO s3_w_avro SELECT i, 'avro_' || i FROM generate_series(1, 5) i;

CREATE FOREIGN TABLE s3_r_avro (id int, name text)
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/avro/', format 'avro');

SELECT * FROM s3_r_avro ORDER BY id;

DROP FOREIGN TABLE s3_w_avro;
DROP FOREIGN TABLE s3_r_avro;

-- ============================================================
-- Test 3: Text write + read-back
-- ============================================================
CREATE FOREIGN TABLE s3_w_text (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/text/', format 'text');

INSERT INTO s3_w_text SELECT i, 'txt_' || i, i * 5.25 FROM generate_series(1, 5) i;

CREATE FOREIGN TABLE s3_r_text (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/text/', format 'text');

SELECT * FROM s3_r_text ORDER BY id;

DROP FOREIGN TABLE s3_w_text;
DROP FOREIGN TABLE s3_r_text;

-- ============================================================
-- Test 4: CSV write + read-back
-- ============================================================
CREATE FOREIGN TABLE s3_w_csv (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/csv/', format 'csv');

INSERT INTO s3_w_csv SELECT i, 'csv_' || i, i * 3.14 FROM generate_series(1, 5) i;

CREATE FOREIGN TABLE s3_r_csv (id int, name text, amount decimal(10,2))
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/csv/', format 'csv');

SELECT * FROM s3_r_csv ORDER BY id;

DROP FOREIGN TABLE s3_w_csv;
DROP FOREIGN TABLE s3_r_csv;

-- ============================================================
-- Test 5: Parquet write with NULL values
-- ============================================================
CREATE FOREIGN TABLE s3_w_nulls (id int, name text, val int)
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/nulls/', format 'parquet');

INSERT INTO s3_w_nulls VALUES (1, 'has_all', 100), (2, NULL, 200), (3, 'no_val', NULL), (4, NULL, NULL);

CREATE FOREIGN TABLE s3_r_nulls (id int, name text, val int)
SERVER s3_write_server
OPTIONS (filePath '/warehouse/smoke-write-test/nulls/', format 'parquet');

SELECT * FROM s3_r_nulls ORDER BY id;
SELECT COUNT(*) AS total, COUNT(name) AS non_null_names, COUNT(val) AS non_null_vals FROM s3_r_nulls;

DROP FOREIGN TABLE s3_w_nulls;
DROP FOREIGN TABLE s3_r_nulls;

-- Cleanup
DROP USER MAPPING FOR gpadmin SERVER s3_write_server;
DROP SERVER s3_write_server;
