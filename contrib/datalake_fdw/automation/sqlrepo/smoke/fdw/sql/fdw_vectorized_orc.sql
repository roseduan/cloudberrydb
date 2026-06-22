-- Normalize the execMain.c source location appended to backend error
-- messages so the test does not break when unrelated backend changes
-- shift line numbers.
-- start_matchsubs
-- m/\(execMain\.c:\d+\)/
-- s/\(execMain\.c:\d+\)/(execMain.c:XXX)/
-- end_matchsubs
-- FDW Vectorized ORC Read Test
-- Purpose: Trigger orcReadRecordBatch.cpp via vectorization extension
-- Target: provider/orc/read/orcReadRecordBatch.cpp (0%, 240 lines)

-- Load vectorization extension
LOAD 'vectorization';
SET vector.enable_vectorization = on;
SET vector.force_vectorization = on;

-- Setup FDW
DROP SERVER IF EXISTS fdw_vec_server CASCADE;
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator
    OPTIONS (mpp_execute 'all segments');
CREATE SERVER fdw_vec_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (host 'minio:9000', protocol 's3', isvirtual 'false', ishttps 'false');
CREATE USER MAPPING FOR gpadmin
    SERVER fdw_vec_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'admin12345');

-- ============================================================
-- Test 1: ORC with bigint types (avoids int32/int64 mismatch in GArrow)
-- ============================================================
CREATE FOREIGN TABLE fdw_vec_orc_w (id bigint, name text, val double precision)
SERVER fdw_vec_server
OPTIONS (filePath '/warehouse/fdw-test/vec-orc/', format 'orc');

INSERT INTO fdw_vec_orc_w
SELECT i::bigint, 'row_' || i, (i * 1.5)::double precision
FROM generate_series(1, 200) i;
DROP FOREIGN TABLE fdw_vec_orc_w;

-- Vectorized read
CREATE FOREIGN TABLE fdw_vec_orc_r (id bigint, name text, val double precision)
SERVER fdw_vec_server
OPTIONS (filePath '/warehouse/fdw-test/vec-orc/', format 'orc');

SELECT COUNT(*) FROM fdw_vec_orc_r;
SELECT * FROM fdw_vec_orc_r WHERE id <= 5 ORDER BY id;
SELECT SUM(val) FROM fdw_vec_orc_r;
SELECT COUNT(*) FROM fdw_vec_orc_r WHERE val > 200.0;

DROP FOREIGN TABLE fdw_vec_orc_r;

-- ============================================================
-- Test 2: ORC with all bigint-compatible types
-- ============================================================
CREATE FOREIGN TABLE fdw_vec_types_w (
    col_bigint bigint,
    col_double double precision,
    col_text text,
    col_date date,
    col_ts timestamp
)
SERVER fdw_vec_server
OPTIONS (filePath '/warehouse/fdw-test/vec-orc-types/', format 'orc');

INSERT INTO fdw_vec_types_w VALUES
    (1, 3.14, 'hello', '2024-01-15', '2024-01-15 10:30:00'),
    (2, -2.718, 'world', '1970-01-01', '1970-01-01 00:00:00'),
    (3, 0.0, '', '2099-12-31', '2099-12-31 23:59:59'),
    (NULL, NULL, NULL, NULL, NULL);

INSERT INTO fdw_vec_types_w
SELECT i::bigint, (i * 0.01)::double precision, 'text_' || i,
       '2024-01-01'::date + i, '2024-01-01'::timestamp + (i || ' hours')::interval
FROM generate_series(4, 100) i;
DROP FOREIGN TABLE fdw_vec_types_w;

CREATE FOREIGN TABLE fdw_vec_types_r (
    col_bigint bigint,
    col_double double precision,
    col_text text,
    col_date date,
    col_ts timestamp
)
SERVER fdw_vec_server
OPTIONS (filePath '/warehouse/fdw-test/vec-orc-types/', format 'orc');

SELECT COUNT(*) FROM fdw_vec_types_r;
SELECT * FROM fdw_vec_types_r WHERE col_bigint <= 3 ORDER BY col_bigint NULLS LAST;
SELECT COUNT(*) FROM fdw_vec_types_r WHERE col_double > 0.5;
SELECT MIN(col_date), MAX(col_date) FROM fdw_vec_types_r;

DROP FOREIGN TABLE fdw_vec_types_r;

-- Cleanup
RESET vector.enable_vectorization;
RESET vector.force_vectorization;
DROP USER MAPPING FOR gpadmin SERVER fdw_vec_server;
DROP SERVER fdw_vec_server;
