-- FDW Multi-Format Test
-- Purpose: Exercise provider factory dispatch logic across formats
-- Target: provider.cpp (48.6%), providerWrapper.cpp (24.1%), config.c (40.9%)

-- Setup FDW
DROP SERVER IF EXISTS fdw_mf_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_mf_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_mf_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Write same data in 4 formats
-- ============================================================

-- Parquet
CREATE FOREIGN TABLE fdw_mf_pq_w (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/parquet/', format 'parquet');
INSERT INTO fdw_mf_pq_w SELECT i, 'item_' || i, i * 10.50 FROM generate_series(1, 20) i;
DROP FOREIGN TABLE fdw_mf_pq_w;

-- ORC
CREATE FOREIGN TABLE fdw_mf_orc_w (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/orc/', format 'orc');
INSERT INTO fdw_mf_orc_w SELECT i, 'item_' || i, i * 10.50 FROM generate_series(1, 20) i;
DROP FOREIGN TABLE fdw_mf_orc_w;

-- Text
CREATE FOREIGN TABLE fdw_mf_txt_w (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/text/', format 'text');
INSERT INTO fdw_mf_txt_w SELECT i, 'item_' || i, i * 10.50 FROM generate_series(1, 20) i;
DROP FOREIGN TABLE fdw_mf_txt_w;

-- Avro
CREATE FOREIGN TABLE fdw_mf_avro_w (id int, name text)
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/avro/', format 'avro');
INSERT INTO fdw_mf_avro_w SELECT i, 'item_' || i FROM generate_series(1, 20) i;
DROP FOREIGN TABLE fdw_mf_avro_w;

-- ============================================================
-- Read back and verify consistency
-- ============================================================
CREATE FOREIGN TABLE fdw_mf_pq_r (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/parquet/', format 'parquet');

CREATE FOREIGN TABLE fdw_mf_orc_r (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/orc/', format 'orc');

CREATE FOREIGN TABLE fdw_mf_txt_r (id int, name text, val decimal(10,2))
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/text/', format 'text');

CREATE FOREIGN TABLE fdw_mf_avro_r (id int, name text)
SERVER fdw_mf_server
OPTIONS (filePath '/warehouse/fdw-test/multiformat/avro/', format 'avro');

-- Count consistency
SELECT 'parquet' AS fmt, COUNT(*) AS cnt FROM fdw_mf_pq_r
UNION ALL
SELECT 'orc', COUNT(*) FROM fdw_mf_orc_r
UNION ALL
SELECT 'text', COUNT(*) FROM fdw_mf_txt_r
UNION ALL
SELECT 'avro', COUNT(*) FROM fdw_mf_avro_r
ORDER BY fmt;

-- Spot check: first 3 rows from each format
SELECT * FROM fdw_mf_pq_r WHERE id <= 3 ORDER BY id;
SELECT * FROM fdw_mf_orc_r WHERE id <= 3 ORDER BY id;
SELECT * FROM fdw_mf_txt_r WHERE id <= 3 ORDER BY id;
SELECT * FROM fdw_mf_avro_r WHERE id <= 3 ORDER BY id;

-- Cleanup
DROP FOREIGN TABLE fdw_mf_pq_r;
DROP FOREIGN TABLE fdw_mf_orc_r;
DROP FOREIGN TABLE fdw_mf_txt_r;
DROP FOREIGN TABLE fdw_mf_avro_r;
DROP USER MAPPING FOR gpadmin SERVER fdw_mf_server;
DROP SERVER fdw_mf_server;
