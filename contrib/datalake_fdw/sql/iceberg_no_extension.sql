-- Regression test for the datalake_fdw-not-installed guard added by MR 1078
-- (#337 P0). Verifies CREATE TABLE ... USING iceberg and CREATE ICEBERG TABLE
-- both raise SQLSTATE 0A000 (feature_not_supported) with the install HINT, and
-- leave no orphan pg_class rows behind.

-- start_ignore
DROP DATABASE IF EXISTS iceberg_no_ext_db;
CREATE DATABASE iceberg_no_ext_db;
\c iceberg_no_ext_db
-- end_ignore

SELECT count(*) FROM pg_extension WHERE extname = 'datalake_fdw';
SELECT amname FROM pg_am WHERE amname = 'iceberg';

DO $$
DECLARE
    msg_text  TEXT;
    hint_text TEXT;
BEGIN
    CREATE TABLE t_no_ext_a (id int) USING iceberg;
    RAISE EXCEPTION 'guard did not fire on CREATE TABLE ... USING iceberg';
EXCEPTION WHEN feature_not_supported THEN
    GET STACKED DIAGNOSTICS
        msg_text  = MESSAGE_TEXT,
        hint_text = PG_EXCEPTION_HINT;
    RAISE NOTICE 'sqlstate=% message=% hint=%', SQLSTATE, msg_text, hint_text;
END
$$;

DO $$
DECLARE
    msg_text  TEXT;
    hint_text TEXT;
BEGIN
    CREATE ICEBERG TABLE t_no_ext_b (id int);
    RAISE EXCEPTION 'guard did not fire on CREATE ICEBERG TABLE';
EXCEPTION WHEN feature_not_supported THEN
    GET STACKED DIAGNOSTICS
        msg_text  = MESSAGE_TEXT,
        hint_text = PG_EXCEPTION_HINT;
    RAISE NOTICE 'sqlstate=% message=% hint=%', SQLSTATE, msg_text, hint_text;
END
$$;

SELECT relname FROM pg_class
 WHERE relname IN ('t_no_ext_a', 't_no_ext_b')
 ORDER BY relname;

-- start_ignore
\c contrib_regression
DROP DATABASE iceberg_no_ext_db;
-- end_ignore
