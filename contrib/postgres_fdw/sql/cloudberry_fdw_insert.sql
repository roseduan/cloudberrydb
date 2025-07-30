-- -------------------------------
-- Setup: FDW and local data
-- -------------------------------
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
CREATE EXTENSION postgres_fdw;

DROP SERVER IF EXISTS remote_srv CASCADE;
CREATE SERVER remote_srv
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '127.0.0.1', port '15432', dbname 'remotedb');

CREATE USER MAPPING FOR CURRENT_USER SERVER remote_srv OPTIONS (user 'gpadmin');

-- Local source table
DROP TABLE IF EXISTS local_source;
CREATE TABLE local_source (
  id int,
  data text
) DISTRIBUTED BY (id);

INSERT INTO local_source
SELECT g, md5(g::text) FROM generate_series(1, 10000) g;

-- Foreign table for remote_table
DROP FOREIGN TABLE IF EXISTS ft_remote_table;
CREATE FOREIGN TABLE ft_remote_table (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_table', mpp_execute 'all segments');

-- -----------------------------------
-- T01: Normal insert into remote table
-- -----------------------------------
INSERT INTO ft_remote_table
SELECT * FROM local_source;

SELECT 'T01: Number of rows inserted into remote_table:', COUNT(*) FROM ft_remote_table;

-- -----------------------------------
-- T02: Insert from empty source table
-- -----------------------------------
DROP TABLE IF EXISTS empty_source;
CREATE TABLE empty_source (
  id int,
  data text
) DISTRIBUTED BY (id);

INSERT INTO ft_remote_table
SELECT * FROM empty_source;

SELECT 'T02: Row count after empty insert:', COUNT(*) FROM ft_remote_table;

-- ----------------------------------------------------
-- T03: Edge case inserts - NULL, special chars, emoji
-- ----------------------------------------------------
INSERT INTO local_source VALUES
  (99999, NULL),
  (100001, 'Special chars test: !@#$%^&*()_+ \t\n'),
  (100002, '中文测试'),
  (100003, 'Emoji: 😀🔥💡'),
  (100004, 'special char: ☃');

INSERT INTO ft_remote_table
SELECT * FROM local_source WHERE id BETWEEN 99999 AND 100004;

SELECT 'T03: Edge case insert count:', COUNT(*) FROM ft_remote_table WHERE id BETWEEN 99999 AND 100004;

-- -----------------------------------
-- T04: Insert a large text field
-- -----------------------------------
INSERT INTO ft_remote_table VALUES
  (200000, repeat('longtext_', 10000));

SELECT 'T04: Large text field length:', length(data) FROM ft_remote_table WHERE id = 200000;

-- -----------------------------------
-- T05: Insert various data types
-- -----------------------------------
SET datestyle = ISO;
DROP TABLE IF EXISTS local_types;
CREATE TABLE local_types (
  id int,
  val_text text,
  val_bool boolean,
  val_float float,
  val_date date
) DISTRIBUTED BY (id);

INSERT INTO local_types VALUES
  (1, 'Text', true, 1.23, CURRENT_DATE),
  (2, 'Test', false, 4.56, CURRENT_DATE + 1),
  (3, NULL, NULL, NULL, NULL);

DROP FOREIGN TABLE IF EXISTS ft_remote_types;
CREATE FOREIGN TABLE ft_remote_types (
  id int,
  val_text text,
  val_bool boolean,
  val_float float,
  val_date date
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_types', mpp_execute 'all segments');

INSERT INTO ft_remote_types
SELECT * FROM local_types;

SELECT 'T05: Rows inserted into remote_types:', COUNT(*) FROM ft_remote_types;

-- -----------------------------------
-- T06: Insert with column mismatch
-- -----------------------------------
DROP FOREIGN TABLE IF EXISTS ft_remote_mismatch;
CREATE FOREIGN TABLE ft_remote_mismatch (
  data text,
  id int
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_mismatch', mpp_execute 'all segments');

BEGIN;
DO $$
BEGIN
  BEGIN
    INSERT INTO ft_remote_mismatch
    SELECT * FROM local_source LIMIT 1;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'T06: Insert failed due to structure mismatch: %', SQLERRM;
  END;
END;
$$ LANGUAGE plpgsql;
ROLLBACK;

-- -----------------------------------
-- T07: Insert 1 million rows
-- -----------------------------------
DROP TABLE IF EXISTS large_source;
CREATE TABLE large_source (id int, data text) DISTRIBUTED BY (id);

INSERT INTO large_source
SELECT g, md5(g::text) FROM generate_series(1, 1000000) g;

INSERT INTO ft_remote_table
SELECT * FROM large_source;

SELECT 'T07: Row count after 1 million inserts:', COUNT(*) FROM ft_remote_table;

-- -----------------------------------
-- T08: Insert from CTAS temp table
-- -----------------------------------
DROP TABLE IF EXISTS tmp_ctas_source;
CREATE TEMP TABLE tmp_ctas_source AS
  SELECT id, data FROM local_source WHERE id < 100;

INSERT INTO ft_remote_table
SELECT * FROM tmp_ctas_source;

SELECT 'T08: Insert from CTAS temp table:', COUNT(*) FROM ft_remote_table WHERE id < 100;

-- -----------------------------------
-- T09: Insert into remote with extra column
-- -----------------------------------
CREATE FOREIGN TABLE ft_remote_extra (
  id int,
  data text,
  extra text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_extra', mpp_execute 'all segments');

INSERT INTO ft_remote_extra (id, data)
SELECT * FROM local_source LIMIT 10;

SELECT 'T09: Insert into remote_extra with extra column:', COUNT(*) FROM ft_remote_extra;

-- -----------------------------------
-- T10: Insert with swapped column order
-- -----------------------------------
CREATE FOREIGN TABLE ft_remote_swap (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_swap', mpp_execute 'all segments');

INSERT INTO ft_remote_swap(data, id)
SELECT data, id FROM local_source LIMIT 10;

SELECT 'T10: Insert with swapped columns:', COUNT(*) FROM ft_remote_swap;

-- -----------------------------------
-- T11: Insert into table with UNIQUE constraint
-- -----------------------------------
DROP TABLE IF EXISTS local_unique;
CREATE TABLE local_unique (
  id int,
  data text
) DISTRIBUTED BY (id);

INSERT INTO local_unique VALUES (1, 'one'), (2, 'two');

DROP FOREIGN TABLE IF EXISTS ft_remote_unique;
CREATE FOREIGN TABLE ft_remote_unique (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_unique', mpp_execute 'all segments');

INSERT INTO ft_remote_unique SELECT * FROM local_unique;

SELECT 'T11: Initial insert into remote_unique:', COUNT(*) FROM ft_remote_unique;

-- -----------------------------------
-- T12: Attempt to insert duplicate key
-- -----------------------------------
BEGIN;
DO $$
BEGIN
  BEGIN
    INSERT INTO ft_remote_unique VALUES (1, 'duplicate!');
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE 'T12: Duplicate key insert failed as expected: %', SQLERRM;
  END;
END;
$$ LANGUAGE plpgsql;
ROLLBACK;

-- -----------------------------------
-- T13: Insert within transaction and rollback
-- -----------------------------------
BEGIN;
INSERT INTO ft_remote_table VALUES (999998, 'rollback_test');
ROLLBACK;

SELECT 'T13: Row still exists after rollback:', COUNT(*) FROM ft_remote_table WHERE id = 999998;

-- -----------------------------------
-- T14: Insert NULL into NOT NULL column
-- -----------------------------------
CREATE FOREIGN TABLE ft_remote_notnull (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_notnull', mpp_execute 'all segments');

BEGIN;
DO $$
BEGIN
  BEGIN
    INSERT INTO ft_remote_notnull VALUES (1, NULL);
  EXCEPTION WHEN not_null_violation THEN
    RAISE NOTICE 'T14: NULL insert into NOT NULL column failed as expected: %', SQLERRM;
  END;
END;
$$ LANGUAGE plpgsql;
ROLLBACK;

-- -----------------------------------
-- T15: Insert with partial columns
-- -----------------------------------
INSERT INTO ft_remote_table(id)
SELECT 888888;

SELECT 'T15: Partial column insert:', data IS NULL FROM ft_remote_table WHERE id = 888888;

-- -----------------------------------
-- T16: Insert into remote table with default value column
-- -----------------------------------
CREATE FOREIGN TABLE ft_remote_default (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_with_default', mpp_execute 'all segments');

INSERT INTO ft_remote_default(id) VALUES (900001);

SELECT 'T16: Insert into remote table with default value:', * FROM ft_remote_default WHERE id = 900001;

-- -----------------------------------
-- T17: Insert and use RETURNING clause
-- -----------------------------------
INSERT INTO ft_remote_table VALUES (300001, 'RETURNING test') RETURNING id, data;

-- -----------------------------------
-- T18: Insert with explicit type casting (int → text)
-- -----------------------------------
INSERT INTO ft_remote_table(id, data)
SELECT 888001, 888001::text;

SELECT 'T18: Insert with explicit type casting result:', * FROM ft_remote_table WHERE id = 888001;

-- -----------------------------------
-- T19: Insert from a view (simulating real-world abstraction)
-- -----------------------------------
CREATE VIEW view_local_source AS
SELECT * FROM local_source WHERE id < 50;

INSERT INTO ft_remote_table
SELECT * FROM view_local_source;

SELECT 'T19: Insert from view result count:', COUNT(*) FROM ft_remote_table WHERE id < 50;

-- -----------------------------------
-- T20: Insert into remote table with varchar/char types
-- -----------------------------------
DROP TABLE IF EXISTS local_text_types;
CREATE TABLE local_text_types (
  id int,
  val_char char(5),
  val_varchar varchar(20)
) DISTRIBUTED BY (id);

INSERT INTO local_text_types VALUES
  (1, 'A', 'Alpha'),
  (2, 'B', 'Beta'),
  (3, 'C', NULL);

DROP FOREIGN TABLE IF EXISTS ft_remote_text_types;
CREATE FOREIGN TABLE ft_remote_text_types (
  id int,
  val_char char(5),
  val_varchar varchar(20)
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_text_types', mpp_execute 'all segments');

INSERT INTO ft_remote_text_types
SELECT * FROM local_text_types;

SELECT 'T20: Insert into varchar/char fields:', COUNT(*) FROM ft_remote_text_types;

-- -----------------------------------
-- T21: Bytea field insert (binary data)
-- -----------------------------------
DROP TABLE IF EXISTS local_binary;
CREATE TABLE local_binary (
  id int,
  data bytea
) DISTRIBUTED BY (id);

INSERT INTO local_binary VALUES
  (1, decode('48656c6c6f20524f5345', 'hex'));

DROP FOREIGN TABLE IF EXISTS ft_remote_binary;
CREATE FOREIGN TABLE ft_remote_binary (
  id int,
  data bytea
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_binary', mpp_execute 'all segments');

INSERT INTO ft_remote_binary SELECT * FROM local_binary;

SELECT 'T21: Insert bytea field:', encode(data, 'escape') FROM ft_remote_binary;

-- -----------------------------------
-- T22: Insert into foreign table select * from foreign table
-- -----------------------------------
INSERT INTO ft_remote_table SELECT * FROM ft_remote_table LIMIT 10;

-- -----------------------------------
-- T23: Setup foreign partitioned table
-- -----------------------------------
-- Local table as source
CREATE TABLE local_partition_source (
  id int,
  data text
) DISTRIBUTED BY (id);

INSERT INTO local_partition_source
SELECT g, 'row_' || g FROM generate_series(1, 15000) g;

-- Foreign partitioned table
CREATE FOREIGN TABLE ft_remote_partitioned (
  id int,
  data text
) SERVER remote_srv
  OPTIONS (schema_name 'public', table_name 'remote_partitioned', mpp_execute 'all segments');

-- -----------------------------------
-- T24: Insert range into remote partitioned table
-- -----------------------------------
INSERT INTO ft_remote_partitioned
SELECT * FROM local_partition_source;

-- Verify counts per range
SELECT 'T24: Total rows in remote_partitioned:', COUNT(*) FROM ft_remote_partitioned;
SELECT 'T24: Rows in range 1–9999:', COUNT(*) FROM ft_remote_partitioned WHERE id BETWEEN 1 AND 9999;
SELECT 'T24: Rows in range 10000–19999:', COUNT(*) FROM ft_remote_partitioned WHERE id BETWEEN 10000 AND 19999;
SELECT 'T24: Rows in default partition (>=20000):', COUNT(*) FROM ft_remote_partitioned WHERE id >= 20000;

-- -----------------------------------
-- T25: Insert into default partition
-- -----------------------------------
INSERT INTO ft_remote_partitioned VALUES (30000, 'default_partition');

SELECT 'T25: Default partition insert result:', COUNT(*) FROM ft_remote_partitioned WHERE id = 30000;

-- -----------------------------------
-- T26: Insert duplicate into partitioned table (no constraint)
-- -----------------------------------
-- No primary key, so duplicates should be allowed
INSERT INTO ft_remote_partitioned VALUES (30000, 'duplicate_check');

SELECT 'T26: Row count for id = 30000 (should be 2):', COUNT(*) FROM ft_remote_partitioned WHERE id = 30000;
