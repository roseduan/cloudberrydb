-- Normalize C/C++ source locations appended to backend error messages so the
-- test does not break when unrelated changes shift line numbers.
-- start_matchsubs
-- m/\(avroRead\.cpp:\d+\)/
-- s/\(avroRead\.cpp:\d+\)/(avroRead.cpp:XXX)/
-- m/\(parquetRead\.cpp:\d+\)/
-- s/\(parquetRead\.cpp:\d+\)/(parquetRead.cpp:XXX)/
-- m/\(parquetFileReader\.cpp:\d+\)/
-- s/\(parquetFileReader\.cpp:\d+\)/(parquetFileReader.cpp:XXX)/
-- m/\(orcFileReader\.cpp:\d+\)/
-- s/\(orcFileReader\.cpp:\d+\)/(orcFileReader.cpp:XXX)/
-- end_matchsubs
-- FDW Error Read Path Coverage Test
-- Purpose: Trigger error branches in file readers via corrupted/mismatched data
-- Target: parquetFileReader.cpp error paths, orcFileReader.cpp error paths,
--         avroRead.cpp error paths, providerWrapper.cpp, common.cpp,
--         readPolicy.cpp, fdwFunction.c

-- Setup FDW
DROP SERVER IF EXISTS fdw_err_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_err_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_err_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Step 1: Write valid data files first
-- ============================================================

-- Parquet with 3 columns
CREATE FOREIGN TABLE fdw_err_pq_w (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/pq3col/', format 'parquet');
INSERT INTO fdw_err_pq_w SELECT i, 'row_' || i, (i*1.5)::decimal(10,2) FROM generate_series(1,20) i;
DROP FOREIGN TABLE fdw_err_pq_w;

-- ORC with 3 columns
CREATE FOREIGN TABLE fdw_err_orc_w (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/orc3col/', format 'orc');
INSERT INTO fdw_err_orc_w SELECT i, 'row_' || i, (i*1.5)::decimal(10,2) FROM generate_series(1,20) i;
DROP FOREIGN TABLE fdw_err_orc_w;

-- Avro with 3 columns
CREATE FOREIGN TABLE fdw_err_avro_w (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/avro3col/', format 'avro');
INSERT INTO fdw_err_avro_w SELECT i, 'row_' || i, (i*1.5)::decimal(10,2) FROM generate_series(1,20) i;
DROP FOREIGN TABLE fdw_err_avro_w;

-- ============================================================
-- Test 1: Column count mismatch - more columns than file
-- (parquetFileReader: "mpp columns N is less than parquet columns M")
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE fdw_err_pq_extra (id int, name text, val decimal(10,2), extra1 text, extra2 int) SERVER fdw_err_server OPTIONS (filePath ''/warehouse/fdw-test/err/pq3col/'', format ''parquet'')';
    EXECUTE 'SELECT * FROM fdw_err_pq_extra';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pq extra cols: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE IF EXISTS fdw_err_pq_extra;

-- ORC column mismatch
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE fdw_err_orc_extra (id int, name text, val decimal(10,2), extra1 text, extra2 int) SERVER fdw_err_server OPTIONS (filePath ''/warehouse/fdw-test/err/orc3col/'', format ''orc'')';
    EXECUTE 'SELECT * FROM fdw_err_orc_extra';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'orc extra cols: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE IF EXISTS fdw_err_orc_extra;

-- ============================================================
-- Test 2: Column count mismatch - fewer columns than file
-- ============================================================
DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE fdw_err_pq_fewer (id int) SERVER fdw_err_server OPTIONS (filePath ''/warehouse/fdw-test/err/pq3col/'', format ''parquet'')';
    EXECUTE 'SELECT COUNT(*) FROM fdw_err_pq_fewer';
    RAISE NOTICE 'pq fewer cols: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pq fewer cols: %', regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...');
END;
$$;
DROP FOREIGN TABLE IF EXISTS fdw_err_pq_fewer;

DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE fdw_err_orc_fewer (id int) SERVER fdw_err_server OPTIONS (filePath ''/warehouse/fdw-test/err/orc3col/'', format ''orc'')';
    EXECUTE 'SELECT COUNT(*) FROM fdw_err_orc_fewer';
    RAISE NOTICE 'orc fewer cols: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'orc fewer cols: %', regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...');
END;
$$;
DROP FOREIGN TABLE IF EXISTS fdw_err_orc_fewer;

DO $$
BEGIN
    EXECUTE 'CREATE FOREIGN TABLE fdw_err_avro_fewer (id int) SERVER fdw_err_server OPTIONS (filePath ''/warehouse/fdw-test/err/avro3col/'', format ''avro'')';
    EXECUTE 'SELECT COUNT(*) FROM fdw_err_avro_fewer';
    RAISE NOTICE 'avro fewer cols: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'avro fewer cols: %', regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...');
END;
$$;
DROP FOREIGN TABLE IF EXISTS fdw_err_avro_fewer;

-- ============================================================
-- Test 3: Type mismatch - read int column as text, text as int
-- ============================================================

-- Read int as text (should work or trigger type conversion)
CREATE FOREIGN TABLE fdw_err_pq_type1 (id text, name text, val text)
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/pq3col/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_pq_type1 LIMIT 1';
    RAISE NOTICE 'pq int-as-text: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pq int-as-text: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_pq_type1;

-- Read text as int (should fail - type mismatch)
CREATE FOREIGN TABLE fdw_err_pq_type2 (id int, name int, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/pq3col/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_pq_type2 LIMIT 1';
    RAISE NOTICE 'pq text-as-int: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pq text-as-int: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_pq_type2;

-- ORC type mismatch
CREATE FOREIGN TABLE fdw_err_orc_type (id int, name int, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/orc3col/', format 'orc');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_orc_type LIMIT 1';
    RAISE NOTICE 'orc text-as-int: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'orc text-as-int: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_orc_type;

-- Avro type mismatch
CREATE FOREIGN TABLE fdw_err_avro_type (id int, name int, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/avro3col/', format 'avro');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_avro_type LIMIT 1';
    RAISE NOTICE 'avro text-as-int: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'avro text-as-int: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_avro_type;

-- ============================================================
-- Test 4: Read from empty directory (no data files)
-- ============================================================
CREATE FOREIGN TABLE fdw_err_empty (id int, name text)
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/empty_dir/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_empty';
    RAISE NOTICE 'empty dir: OK (0 rows)';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'empty dir: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_empty;

-- ============================================================
-- Test 5: Read from non-existent path
-- ============================================================
CREATE FOREIGN TABLE fdw_err_nopath (id int, name text)
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/does_not_exist_xyz/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_nopath';
    RAISE NOTICE 'no path: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'no path: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_nopath;

-- ============================================================
-- Test 6: Read ORC file as Parquet (format mismatch)
-- ============================================================
CREATE FOREIGN TABLE fdw_err_orc_as_pq (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/orc3col/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_orc_as_pq LIMIT 1';
    RAISE NOTICE 'orc-as-parquet: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'orc-as-parquet: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_orc_as_pq;

-- Read Parquet file as ORC
CREATE FOREIGN TABLE fdw_err_pq_as_orc (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/pq3col/', format 'orc');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_pq_as_orc LIMIT 1';
    RAISE NOTICE 'parquet-as-orc: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'parquet-as-orc: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_pq_as_orc;

-- Read Parquet as Avro
CREATE FOREIGN TABLE fdw_err_pq_as_avro (id int, name text, val decimal(10,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/pq3col/', format 'avro');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_pq_as_avro LIMIT 1';
    RAISE NOTICE 'parquet-as-avro: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'parquet-as-avro: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_pq_as_avro;

-- ============================================================
-- Test 7: Decimal precision boundary
-- ============================================================
-- Write parquet with large decimal
CREATE FOREIGN TABLE fdw_err_dec_w (id int, big_dec decimal(38,10))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/bigdec/', format 'parquet');
INSERT INTO fdw_err_dec_w VALUES (1, 1234567890123456789012345678.1234567890);
DROP FOREIGN TABLE fdw_err_dec_w;

-- Read back with smaller precision
CREATE FOREIGN TABLE fdw_err_dec_r (id int, big_dec decimal(5,2))
SERVER fdw_err_server
OPTIONS (filePath '/warehouse/fdw-test/err/bigdec/', format 'parquet');
DO $$
BEGIN
    EXECUTE 'SELECT * FROM fdw_err_dec_r';
    RAISE NOTICE 'dec precision: OK';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'dec precision: %', regexp_replace(regexp_replace(SQLERRM, '/fdw-test/err/[^ ]*', '/...file...'), '\(seg\d.*', '');
END;
$$;
DROP FOREIGN TABLE fdw_err_dec_r;

-- ============================================================
-- Cleanup
-- ============================================================
DROP USER MAPPING FOR gpadmin SERVER fdw_err_server;
DROP SERVER fdw_err_server;
