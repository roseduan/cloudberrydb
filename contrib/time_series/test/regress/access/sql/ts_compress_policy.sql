-- Test: auto-compression policy
-- Verifies add_compression_policy / remove_compression_policy /
-- policy_compression / compression_policy_stats end-to-end.

-- start_matchsubs
-- m/oid=\d+/
-- s/oid=\d+/oid=OID/
-- m/job_id=\d+/
-- s/job_id=\d+/job_id=N/
-- m/^\s*\d+\s*$/
-- s/^\s*\d+\s*$/N/
-- end_matchsubs

\i sql/include/setup.sql

-- ======================================================================
-- Section 1: setup test table + compress config
-- ======================================================================
DROP TABLE IF EXISTS test.policy_metrics;
CREATE TABLE test.policy_metrics (
    "time"     timestamptz NOT NULL,
    sensor_id  integer,
    val        double precision
)
USING time_series
WITH (ts_partition_column='time', ts_chunk_interval='1 hour',
      ts_chunk_origin='2025-01-01')
DISTRIBUTED BY (sensor_id);

INSERT INTO test.policy_metrics
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
       (i % 10), random()*100
FROM generate_series(1, 500) i;

SELECT time_series.set_compress_config('test.policy_metrics'::regclass,
                                       'sensor_id', '"time"');

-- ======================================================================
-- Section 2: add_compression_policy + view
-- ======================================================================
SELECT time_series.add_compression_policy(
    table_name        => 'test.policy_metrics'::regclass,
    compress_after    => INTERVAL '1 second',
    schedule_interval => INTERVAL '12 hours'
) IS NOT NULL AS registered;

-- View should expose the policy
SELECT table_name, schedule_interval, compress_after, active
FROM time_series.compression_policy_stats
WHERE table_name = 'test.policy_metrics'::regclass;

-- ======================================================================
-- Section 3: validation errors
-- ======================================================================

-- 3a. compress_after = 0 → reject (avoid compressing in-flight chunks)
DO $$ BEGIN
    PERFORM time_series.add_compression_policy(
        'test.policy_metrics'::regclass, INTERVAL '0 second');
    RAISE EXCEPTION 'should have thrown';
EXCEPTION WHEN invalid_parameter_value THEN
    RAISE NOTICE 'compress_after=0 → rejected (expected)';
END $$;

-- 3b. compress_after = negative → reject
DO $$ BEGIN
    PERFORM time_series.add_compression_policy(
        'test.policy_metrics'::regclass, INTERVAL '-1 day');
    RAISE EXCEPTION 'should have thrown';
EXCEPTION WHEN invalid_parameter_value THEN
    RAISE NOTICE 'compress_after<0 → rejected (expected)';
END $$;

-- 3c. schedule_interval = 0 → reject (would busy-loop the scheduler)
DO $$ BEGIN
    PERFORM time_series.add_compression_policy(
        'test.policy_metrics'::regclass,
        compress_after    => INTERVAL '1 hour',
        schedule_interval => INTERVAL '0 second');
    RAISE EXCEPTION 'should have thrown';
EXCEPTION WHEN invalid_parameter_value THEN
    RAISE NOTICE 'schedule_interval=0 → rejected (expected)';
END $$;

-- ======================================================================
-- Section 4: duplicate policy detection
-- ======================================================================

-- 4a. Plain duplicate → error
DO $$ BEGIN
    PERFORM time_series.add_compression_policy(
        'test.policy_metrics'::regclass, INTERVAL '7 days');
    RAISE EXCEPTION 'should have thrown';
EXCEPTION WHEN duplicate_object OR sqlstate 'P0001' THEN
    RAISE NOTICE 'duplicate add → rejected (expected)';
END $$;

-- 4b. if_not_exists → returns existing job_id, no error
SELECT time_series.add_compression_policy(
    'test.policy_metrics'::regclass,
    compress_after  => INTERVAL '1 second',
    if_not_exists   => true
) IS NOT NULL AS reused_existing;

-- After both attempts there should still be exactly one row
SELECT count(*) AS n_policies
FROM time_series.compression_policy_stats
WHERE table_name = 'test.policy_metrics'::regclass;

-- ======================================================================
-- Section 5: synchronous fire via run_job → chunks compressed
-- ======================================================================

-- Capture job_id for run_job (CALL can't take a subquery).
SELECT job_id FROM time_series.compression_policy_stats
WHERE table_name = 'test.policy_metrics'::regclass \gset
CALL time_series.run_job(:job_id);

-- All chunks for this table should now be COMPRESSED (status = 1).
SELECT count(DISTINCT chunk_number) AS chunks,
       bool_and(status = 1) AS all_compressed
FROM time_series.ts_chunk
WHERE table_oid = 'test.policy_metrics'::regclass;

-- Stats reflect the successful run.
SELECT total_runs >= 1 AS ran,
       total_successes >= 1 AS succeeded,
       total_failures = 0   AS no_failures,
       last_run_success
FROM time_series.compression_policy_stats
WHERE table_name = 'test.policy_metrics'::regclass;

-- ======================================================================
-- Section 6: remove_compression_policy
-- ======================================================================

SELECT time_series.remove_compression_policy('test.policy_metrics'::regclass);

-- View empty for this table
SELECT count(*) AS remaining
FROM time_series.compression_policy_stats
WHERE table_name = 'test.policy_metrics'::regclass;

-- Remove on missing → error
DO $$ BEGIN
    PERFORM time_series.remove_compression_policy('test.policy_metrics'::regclass);
    RAISE EXCEPTION 'should have thrown';
EXCEPTION WHEN sqlstate 'P0001' THEN
    RAISE NOTICE 'remove on missing → rejected (expected)';
END $$;

-- Remove with if_exists → silent no-op
SELECT time_series.remove_compression_policy(
    'test.policy_metrics'::regclass, if_exists => true);

-- ======================================================================
-- Section 7: auto-disable when target table is dropped
-- ======================================================================
CREATE TABLE test.policy_orphan (t timestamptz NOT NULL, v int)
USING time_series
WITH (ts_partition_column='t', ts_chunk_interval='1 day')
DISTRIBUTED REPLICATED;

INSERT INTO test.policy_orphan
SELECT '2025-01-01'::timestamptz + (i * interval '1 sec'), i
FROM generate_series(1, 50) i;

SELECT time_series.set_compress_config(
    'test.policy_orphan'::regclass, NULL, '"t"');

SELECT time_series.add_compression_policy(
    'test.policy_orphan'::regclass, INTERVAL '1 second') AS orphan_job \gset

DROP TABLE test.policy_orphan;

-- After DROP TABLE the policy row still exists but firing it should
-- auto-disable instead of crashing.
CALL time_series.run_job(:orphan_job);

-- active = false after auto-disable
SELECT active AS still_active
FROM time_series.compression_policy_stats
WHERE job_id = :orphan_job;

-- remove_compression_policy can still clean up the disabled row by oid.
SELECT time_series.remove_compression_policy(
    (SELECT table_name FROM time_series.compression_policy_stats
     WHERE job_id = :orphan_job),
    if_exists => true);

SELECT count(*) AS remaining_disabled
FROM time_series.compression_policy_stats
WHERE job_id = :orphan_job;

-- ======================================================================
-- Cleanup
-- ======================================================================
DROP TABLE IF EXISTS test.policy_metrics;
DROP SCHEMA IF EXISTS test CASCADE;
