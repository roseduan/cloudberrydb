-- ts_index.sql: regression tests for ts_btree per-chunk index

\i sql/include/setup.sql

-- Test 1: CREATE INDEX on empty table
CREATE TABLE ts_idx_empty (ts timestamptz NOT NULL, val float8)
    USING time_series
    WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2024-01-01');
CREATE INDEX ts_idx_empty_idx ON ts_idx_empty USING ts_btree (ts);
-- Insert after index creation on empty table
INSERT INTO ts_idx_empty VALUES ('2024-01-01 10:00:00+00', 1.0);
SELECT count(*) FROM ts_idx_empty;

-- Test 2: CREATE INDEX on table with existing data
CREATE TABLE ts_idx_data (ts timestamptz NOT NULL, val float8)
    USING time_series
    WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2024-01-01');
INSERT INTO ts_idx_data VALUES ('2024-01-01 10:00:00+00', 1.0);
INSERT INTO ts_idx_data VALUES ('2024-01-01 11:00:00+00', 2.0);
INSERT INTO ts_idx_data VALUES ('2024-01-02 10:00:00+00', 3.0);
INSERT INTO ts_idx_data VALUES ('2024-01-02 11:00:00+00', 4.0);
INSERT INTO ts_idx_data VALUES ('2024-01-03 10:00:00+00', 5.0);
-- Build index on existing 5 rows
CREATE INDEX ts_idx_data_idx ON ts_idx_data USING ts_btree (ts);
SELECT count(*) FROM ts_idx_data;

-- Test 3: INSERT maintains index
INSERT INTO ts_idx_data VALUES ('2024-01-03 12:00:00+00', 6.0);
INSERT INTO ts_idx_data VALUES ('2024-01-01 09:00:00+00', 0.5);
SELECT count(*) FROM ts_idx_data;
SELECT val FROM ts_idx_data ORDER BY ts;

-- Test 4: DROP INDEX and verify data intact
DROP INDEX ts_idx_data_idx;
SELECT count(*) FROM ts_idx_data;

-- Test 5: Recreate index on data with multiple chunks
CREATE INDEX ts_idx_data_idx2 ON ts_idx_data USING ts_btree (ts);
SELECT count(*) FROM ts_idx_data;

-- Test 6: Multiple indexes on same table
CREATE TABLE ts_idx_multi (ts timestamptz NOT NULL, id int4)
    USING time_series
    WITH (ts_partition_column='ts', ts_chunk_interval='1 day', ts_chunk_origin='2024-01-01');
INSERT INTO ts_idx_multi VALUES ('2024-01-01 10:00:00+00', 100);
INSERT INTO ts_idx_multi VALUES ('2024-01-01 11:00:00+00', 200);
INSERT INTO ts_idx_multi VALUES ('2024-01-02 10:00:00+00', 300);
CREATE INDEX ts_idx_multi_ts ON ts_idx_multi USING ts_btree (ts);
CREATE INDEX ts_idx_multi_id ON ts_idx_multi USING ts_btree (id);
-- Insert with both indexes in place
INSERT INTO ts_idx_multi VALUES ('2024-01-02 11:00:00+00', 400);
SELECT count(*) FROM ts_idx_multi;

-- Test 7: Verify operator classes exist for all types
SELECT amname, opcname FROM pg_opclass
    JOIN pg_am ON pg_am.oid = opcmethod
    WHERE amname = 'ts_btree'
    ORDER BY opcname;

-- Cleanup
