-- ============================================================
-- cagg_fault_tolerance.sql
-- Test: CAGG behavior under fault injection — crash recovery
-- and segment error scenarios.
--
-- Uses gp_inject_fault to simulate:
--   1. Coordinator crash (PANIC) between TX1 and TX2 of REFRESH
--   2. Segment error during REFRESH materialization
--   3. Segment error during watermark advance
--   4. Error during L1→L2 migration
--
-- Requires: CBDB compiled with --enable-cassert (FAULT_INJECTOR)
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

-- gp_inject_fault extension must be installed for fault injection
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- Setup: source table + CAGG + initial data
CREATE TABLE ft_src (time TIMESTAMPTZ NOT NULL, device INT NOT NULL, val FLOAT8) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
)
  DISTRIBUTED BY (device);
INSERT INTO ft_src SELECT '2024-01-01'::timestamptz + (i * interval '10 min'),
  (i % 5) + 1, i * 0.5 FROM generate_series(1, 100) i;

CREATE MATERIALIZED VIEW cv_ft WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         device, count(*) AS cnt, avg(val) AS avg_val
  FROM ft_src GROUP BY bucket, device;

CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

-- Baseline: fully materialized, EXCEPT = 0
SELECT count(*) AS baseline_rows FROM cv_ft;
SELECT count(*) AS diff_baseline FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- ============================================================
-- FT-01: ERROR between TX1 and TX2
--        TX1 (L1→L2 migration) commits via SPI_commit_and_chain.
--        ERROR raised → TX2 aborted (materialization not done).
--        Result: L2 has entries, mat table stale.
--        Next REFRESH should fix everything.
-- ============================================================
\echo '=== FT-01: error between TX1 and TX2 ==='

-- Insert backfill data to create dirty intervals
INSERT INTO ft_src VALUES ('2024-01-01 01:30+00', 1, 999.0);

-- Inject PANIC after TX1 commits, before TX2 starts
SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- This REFRESH will crash after TX1 (L1→L2 done) but before TX2
\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
\set ON_ERROR_STOP 1

-- No reconnect needed (ERROR doesn't kill the backend)

-- Reset fault
SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Verify state after crash:
-- L2 should have entries (TX1 committed)
SELECT count(*) AS l2_after_crash FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_ft');

-- Mat table is stale (TX2 never ran) — but data should be recoverable
-- Another REFRESH should fix it
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

SELECT count(*) AS diff_after_recovery FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- L2 should be clean after recovery
SELECT count(*) AS l2_after_recovery FROM time_series.cagg_materialization_log
WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_ft');

-- ============================================================
-- FT-02: Segment ERROR during materialization (TX2)
--        REFRESH's INSERT INTO mat_table fails on one segment.
--        Entire distributed transaction rolls back.
--        Next REFRESH should succeed.
-- ============================================================
\echo '=== FT-02: segment error during materialization ==='

INSERT INTO ft_src VALUES ('2024-01-01 02:30+00', 2, 888.0);

-- Inject error before watermark advance (mid-TX2)
SELECT gp_inject_fault('cagg_refresh_before_watermark_advance', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
\set ON_ERROR_STOP 1

-- Reset fault
SELECT gp_inject_fault('cagg_refresh_before_watermark_advance', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- TX2 rolled back — mat table unchanged, L1 should still exist
-- (TX1 committed L1→L2, but TX2 error might have rolled back the SPI context)
-- Next REFRESH should fix it
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

SELECT count(*) AS diff_after_seg_error FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- ============================================================
-- FT-03: ERROR during L1→L2 migration (TX1)
--        TX1 fails → everything rolls back (L1 preserved).
--        Next REFRESH should succeed from scratch.
-- ============================================================
\echo '=== FT-03: error during L1→L2 migration ==='

INSERT INTO ft_src VALUES ('2024-01-01 03:30+00', 3, 777.0);

-- Inject error before TX1 commits
SELECT gp_inject_fault('cagg_refresh_before_commit_and_chain', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
\set ON_ERROR_STOP 1

SELECT gp_inject_fault('cagg_refresh_before_commit_and_chain', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- TX1 rolled back → L1 should still have the dirty entry
SELECT count(*) > 0 AS l1_preserved FROM time_series.cagg_invalidation_log;

-- Next REFRESH from scratch should succeed
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

SELECT count(*) AS diff_after_tx1_error FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- ============================================================
-- FT-04: Repeated ERROR + recovery cycle
--        Error twice, recover twice → data still correct.
-- ============================================================
\echo '=== FT-04: repeated error recovery ==='

INSERT INTO ft_src VALUES ('2024-01-01 04:30+00', 4, 666.0);

-- First crash
SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
\set ON_ERROR_STOP 1

SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Insert more data before second error
INSERT INTO ft_src VALUES ('2024-01-01 05:30+00', 5, 555.0);

-- Second crash
SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

\set ON_ERROR_STOP 0
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
\set ON_ERROR_STOP 1

SELECT gp_inject_fault('cagg_refresh_after_commit_and_chain', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Recovery: single REFRESH should fix everything
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

SELECT count(*) AS diff_double_crash FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- ============================================================
-- FT-05: Trigger error during INSERT (segment-level fault)
--        Trigger fails on one segment → INSERT rolls back.
--        Source table unchanged, L1 unchanged.
-- ============================================================
\echo '=== FT-05: trigger error ==='

CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);
SELECT count(*) AS rows_before_trigger_err FROM ft_src;

-- Inject error in trigger's L1 write
SELECT gp_inject_fault('cagg_trigger_before_l1_write', 'error', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content >= 0;

\set ON_ERROR_STOP 0
INSERT INTO ft_src VALUES ('2024-01-01 00:15+00', 1, 111.0);
\set ON_ERROR_STOP 1

SELECT gp_inject_fault('cagg_trigger_before_l1_write', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content >= 0;

-- INSERT rolled back → source unchanged
SELECT count(*) AS rows_after_trigger_err FROM ft_src;

-- Normal INSERT should work now
INSERT INTO ft_src VALUES ('2024-01-01 00:15+00', 1, 111.0);
CALL time_series.refresh_continuous_aggregate('cv_ft', NULL, NULL);

SELECT count(*) AS diff_trigger_err FROM (
  (  SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device, count(*),
   round(avg(val)::numeric, 10)
   FROM ft_src GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device, cnt, round(avg_val::numeric, 10) FROM cv_ft
   )
) x;

-- ============================================================
-- FT-DDL: DELETE/UPDATE on time_series source with CAGG must
--         produce clean ERRORs, NOT assertion crash / undefined
--         behavior.
--
-- Regression for the bug where cagg_invalidation_trigger was
-- registered for BEFORE INSERT OR UPDATE OR DELETE — PG's
-- ExecBRDeleteTriggers ran the trigger.c:2572 assertion
-- (HeapTupleIsValid(fdw_trigtuple) ^ ItemPointerIsValid(tupleid))
-- BEFORE the tableam's "cannot delete from a time_series table"
-- check fired, crashing the segment on debug builds and entering
-- undefined behavior on release.  Trigger is now BEFORE INSERT
-- only; DELETE/UPDATE skip trigger framework and reach the
-- tableam-level ereport cleanly.
-- ============================================================
CREATE TABLE ft_ddl (time TIMESTAMPTZ NOT NULL, device INT, val FLOAT8)
USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval = '1 hour',
    ts_chunk_origin = '2024-01-01'
) DISTRIBUTED BY (device);

CREATE MATERIALIZED VIEW cv_ft_ddl WITH (time_series.continuous) AS
   SELECT time_bucket('1 hour'::interval, time) AS bucket,
          device,
          avg(val) AS avg_val
   FROM ft_ddl GROUP BY 1, 2;

INSERT INTO ft_ddl VALUES ('2024-01-01 00:30:00+00', 1, 10.0);

-- Verify trigger now fires only on INSERT, not on UPDATE/DELETE.
SELECT pg_get_triggerdef(oid) FROM pg_trigger
 WHERE tgrelid = 'ft_ddl'::regclass AND NOT tgisinternal;

\set ON_ERROR_STOP 0
\echo '=== FT-DDL-DELETE: DELETE on time_series source with CAGG ==='
DELETE FROM ft_ddl WHERE device = 1 AND time = '2024-01-01 00:30:00+00';

\echo '=== FT-DDL-UPDATE: UPDATE on time_series source with CAGG ==='
UPDATE ft_ddl SET val = 99.9 WHERE device = 1 AND time = '2024-01-01 00:30:00+00';
\set ON_ERROR_STOP 1

-- The row is still there (DML failed cleanly, no partial state).
SELECT count(*) AS row_intact FROM ft_ddl WHERE device = 1;

DROP TABLE ft_ddl CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE ft_src CASCADE;

\echo '=== FAULT TOLERANCE TESTS DONE ==='
