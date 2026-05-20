-- Compression Formats Smoke Test
-- Purpose: Verify write + read-back with various compression codecs
-- Uses HDFS external tables (gphdfs://) for write/read round-trip
-- Supported: Parquet(snappy,gzip,zstd,lz4), Text(gzip,zip), Avro(snappy)

-- Clean up previous run leftovers
DROP SERVER IF EXISTS s3_compress_server CASCADE;
DROP SERVER IF EXISTS compress_hdfs_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw CASCADE;

-- Common setup (inline to avoid \i path issues with pg_regress)
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
SET datestyle = ISO, MDY;

-- HDFS server (required for gphdfs:// external tables to resolve paa_cluster)
CREATE SERVER compress_hdfs_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        protocol 'hdfs',
        hdfs_namenodes 'hadoop',
        hdfs_port '8020',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );
CREATE USER MAPPING FOR gpadmin
    SERVER compress_hdfs_server
    OPTIONS (user 'gpadmin');

-- ============================================================
-- Test 1: Parquet + Snappy
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_parquet_snappy_w;
CREATE WRITABLE EXTERNAL TABLE compress_parquet_snappy_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_snappy hdfs_cluster_name=paa_cluster compression=snappy')
FORMAT 'parquet';

INSERT INTO compress_parquet_snappy_w SELECT i, 'snappy_' || i, i * 1.1 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_snappy_w;

DROP EXTERNAL TABLE IF EXISTS compress_parquet_snappy_r;
CREATE READABLE EXTERNAL TABLE compress_parquet_snappy_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_snappy hdfs_cluster_name=paa_cluster')
FORMAT 'parquet';

SELECT COUNT(*) FROM compress_parquet_snappy_r;
SELECT SUM(val) FROM compress_parquet_snappy_r;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_snappy_r;

-- ============================================================
-- Test 2: Parquet + Gzip
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_parquet_gzip_w;
CREATE WRITABLE EXTERNAL TABLE compress_parquet_gzip_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_gzip hdfs_cluster_name=paa_cluster compression=gzip')
FORMAT 'parquet';

INSERT INTO compress_parquet_gzip_w SELECT i, 'gzip_' || i, i * 2.2 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_gzip_w;

DROP EXTERNAL TABLE IF EXISTS compress_parquet_gzip_r;
CREATE READABLE EXTERNAL TABLE compress_parquet_gzip_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_gzip hdfs_cluster_name=paa_cluster')
FORMAT 'parquet';

SELECT COUNT(*) FROM compress_parquet_gzip_r;
SELECT SUM(val) FROM compress_parquet_gzip_r;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_gzip_r;

-- ============================================================
-- Test 3: Parquet + Zstd
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_parquet_zstd_w;
CREATE WRITABLE EXTERNAL TABLE compress_parquet_zstd_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_zstd hdfs_cluster_name=paa_cluster compression=zstd')
FORMAT 'parquet';

INSERT INTO compress_parquet_zstd_w SELECT i, 'zstd_' || i, i * 3.3 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_zstd_w;

DROP EXTERNAL TABLE IF EXISTS compress_parquet_zstd_r;
CREATE READABLE EXTERNAL TABLE compress_parquet_zstd_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_zstd hdfs_cluster_name=paa_cluster')
FORMAT 'parquet';

SELECT COUNT(*) FROM compress_parquet_zstd_r;
SELECT SUM(val) FROM compress_parquet_zstd_r;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_zstd_r;

-- ============================================================
-- Test 4: Parquet + LZ4
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_parquet_lz4_w;
CREATE WRITABLE EXTERNAL TABLE compress_parquet_lz4_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_lz4 hdfs_cluster_name=paa_cluster compression=lz4')
FORMAT 'parquet';

INSERT INTO compress_parquet_lz4_w SELECT i, 'lz4_' || i, i * 4.4 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_lz4_w;

DROP EXTERNAL TABLE IF EXISTS compress_parquet_lz4_r;
CREATE READABLE EXTERNAL TABLE compress_parquet_lz4_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/parquet_lz4 hdfs_cluster_name=paa_cluster')
FORMAT 'parquet';

SELECT COUNT(*) FROM compress_parquet_lz4_r;
SELECT SUM(val) FROM compress_parquet_lz4_r;
DROP EXTERNAL TABLE IF EXISTS compress_parquet_lz4_r;

-- ============================================================
-- Test 5: Text + Gzip
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_text_gzip_w;
CREATE WRITABLE EXTERNAL TABLE compress_text_gzip_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/text_gzip hdfs_cluster_name=paa_cluster compression=gzip')
FORMAT 'text' (delimiter ',');

INSERT INTO compress_text_gzip_w SELECT i, 'gzip_' || i, i * 5.5 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_text_gzip_w;

DROP EXTERNAL TABLE IF EXISTS compress_text_gzip_r;
CREATE READABLE EXTERNAL TABLE compress_text_gzip_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/text_gzip hdfs_cluster_name=paa_cluster')
FORMAT 'text' (delimiter ',');

SELECT COUNT(*) FROM compress_text_gzip_r;
SELECT SUM(val) FROM compress_text_gzip_r;
DROP EXTERNAL TABLE IF EXISTS compress_text_gzip_r;

-- ============================================================
-- Test 6: Text + Zip
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_text_zip_w;
CREATE WRITABLE EXTERNAL TABLE compress_text_zip_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/text_zip hdfs_cluster_name=paa_cluster compression=zip')
FORMAT 'text' (delimiter ',');

INSERT INTO compress_text_zip_w SELECT i, 'zip_' || i, i * 6.6 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_text_zip_w;

DROP EXTERNAL TABLE IF EXISTS compress_text_zip_r;
CREATE READABLE EXTERNAL TABLE compress_text_zip_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/text_zip hdfs_cluster_name=paa_cluster')
FORMAT 'text' (delimiter ',');

SELECT COUNT(*) FROM compress_text_zip_r;
SELECT SUM(val) FROM compress_text_zip_r;
DROP EXTERNAL TABLE IF EXISTS compress_text_zip_r;

-- ============================================================
-- Test 7: Avro + Snappy
-- ============================================================
DROP EXTERNAL TABLE IF EXISTS compress_avro_snappy_w;
CREATE WRITABLE EXTERNAL TABLE compress_avro_snappy_w(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/avro_snappy hdfs_cluster_name=paa_cluster compression=snappy')
FORMAT 'avro';

INSERT INTO compress_avro_snappy_w SELECT i, 'avro_snappy_' || i, i * 7.7 FROM generate_series(1, 10) i;
DROP EXTERNAL TABLE IF EXISTS compress_avro_snappy_w;

DROP EXTERNAL TABLE IF EXISTS compress_avro_snappy_r;
CREATE READABLE EXTERNAL TABLE compress_avro_snappy_r(id int, name text, val decimal(10,2))
LOCATION('gphdfs://test/compression/avro_snappy hdfs_cluster_name=paa_cluster')
FORMAT 'avro';

SELECT COUNT(*) FROM compress_avro_snappy_r;
SELECT SUM(val) FROM compress_avro_snappy_r;
DROP EXTERNAL TABLE IF EXISTS compress_avro_snappy_r;

-- ============================================================
-- Test 8: S3 foreign table with compression option
-- ============================================================
DROP SERVER IF EXISTS s3_compress_server CASCADE;
CREATE SERVER s3_compress_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER s3_compress_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- Parquet + snappy via S3 foreign table write
DROP FOREIGN TABLE IF EXISTS s3_compress_parquet_w;
CREATE FOREIGN TABLE s3_compress_parquet_w (id int, name text, val decimal(10,2))
SERVER s3_compress_server
OPTIONS (filePath '/warehouse/smoke-compression/parquet_snappy/', format 'parquet', compression 'snappy');

INSERT INTO s3_compress_parquet_w SELECT i, 's3_snappy_' || i, i * 8.8 FROM generate_series(1, 10) i;
DROP FOREIGN TABLE IF EXISTS s3_compress_parquet_w;

-- Read back
DROP FOREIGN TABLE IF EXISTS s3_compress_parquet_r;
CREATE FOREIGN TABLE s3_compress_parquet_r (id int, name text, val decimal(10,2))
SERVER s3_compress_server
OPTIONS (filePath '/warehouse/smoke-compression/parquet_snappy/', format 'parquet');

SELECT COUNT(*) FROM s3_compress_parquet_r;
SELECT SUM(val) FROM s3_compress_parquet_r;
DROP FOREIGN TABLE IF EXISTS s3_compress_parquet_r;

-- Cleanup
DROP USER MAPPING IF EXISTS FOR gpadmin SERVER s3_compress_server;
DROP SERVER IF EXISTS s3_compress_server;
DROP USER MAPPING IF EXISTS FOR gpadmin SERVER compress_hdfs_server;
DROP SERVER IF EXISTS compress_hdfs_server;
