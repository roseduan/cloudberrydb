-- Test: time_series Table AM with fork storage
-- Requires: time_series extension with version >= 1.2

-- start_matchsubs
-- m/\(seg\d+ .*\)/
-- s/\(seg\d+ .*\)/(seg0 slice1 127.0.0.1:1234 pid=12345)/
-- end_matchsubs

\i sql/include/setup.sql

-- Test 1: Verify time_series AM registered
SELECT amname FROM pg_am WHERE amname = 'time_series';

-- Test 2: CREATE TABLE USING time_series
CREATE TABLE ts_reg (
    ts TIMESTAMPTZ NOT NULL,
    device INT,
    value DOUBLE PRECISION
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
  DISTRIBUTED BY (device);

SELECT reloptions FROM pg_class WHERE relname = 'ts_reg';

-- Test 3: Verify access method on the table
SELECT amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid
    WHERE c.relname = 'ts_reg';

-- Test 4: INSERT routing to forks
INSERT INTO ts_reg VALUES
    ('2025-01-10 12:00:00+00', 1, 10.0),
    ('2025-01-20 08:00:00+00', 2, 20.0),
    ('2025-02-15 16:00:00+00', 3, 30.0),
    ('2025-03-05 10:00:00+00', 4, 40.0);

-- Test 5: Full scan
SELECT device, value FROM ts_reg ORDER BY device;

-- Test 6: Time-pruned scan (February only)
SELECT device, value FROM ts_reg
    WHERE ts >= '2025-02-01' AND ts < '2025-03-01'
    ORDER BY device;

-- Test 7: Time-pruned scan (January only)
SELECT device, value FROM ts_reg
    WHERE ts >= '2025-01-01' AND ts < '2025-02-01'
    ORDER BY device;

-- Test 8: EXPLAIN shows ChunkScan
EXPLAIN (COSTS OFF) SELECT * FROM ts_reg
    WHERE ts >= '2025-02-01' AND ts < '2025-03-01';

-- Test 9: INSERT...RETURNING works
INSERT INTO ts_reg VALUES ('2025-04-01 00:00:00+00', 5, 50.0) RETURNING device;

-- Test 10: Count after all inserts
SELECT count(*) FROM ts_reg;

-- Test 11: NULL timestamp rejected
CREATE TABLE ts_null_test (
    ts TIMESTAMPTZ,
    v INT
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
  DISTRIBUTED BY (v);

INSERT INTO ts_null_test VALUES (NULL, 1);


-- Test 12: Missing ts_partition_column rejected
CREATE TABLE ts_bad (
    id INT,
    v DOUBLE PRECISION
) USING time_series WITH (ts_chunk_interval='1 month')
  DISTRIBUTED BY (id);

-- Test 13: Wrong column type rejected (Cloudberry rejects non-heap AM for
-- distributed tables with incompatible column types at the AM layer)
CREATE TABLE ts_bad_type (
    ts TEXT NOT NULL,
    v INT
) USING time_series WITH (ts_partition_column='ts', ts_chunk_interval='1 month')
  DISTRIBUTED BY (v);

-- Cleanup
