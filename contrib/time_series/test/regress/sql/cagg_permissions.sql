-- ============================================================
-- cagg_permissions.sql
-- Test: CAGG permission checks — multi-role access control
--
-- Validates that PostgreSQL's native permission model correctly
-- governs CAGG operations: SELECT, REFRESH, ALTER, DROP.
--
-- Roles:
--   gpadmin          — superuser, CAGG owner
--   ts_user_reader   — granted SELECT only
--   ts_user_noperm   — no grants at all
-- ============================================================

SET optimizer = off;
SET timezone = 'UTC';

DROP EXTENSION IF EXISTS time_series CASCADE;
CREATE EXTENSION time_series;
SET search_path TO public, time_series;

-- ============================================================
-- Setup: create test roles and source table
-- ============================================================
\echo '=== Setup: roles and data ==='

-- Clean up roles from previous failed runs
DROP ROLE IF EXISTS ts_user_reader;
DROP ROLE IF EXISTS ts_user_noperm;
DROP ROLE IF EXISTS ts_user_writer;

CREATE ROLE ts_user_reader LOGIN;
CREATE ROLE ts_user_noperm LOGIN;
CREATE ROLE ts_user_writer LOGIN;

CREATE TABLE sensor (
    time        TIMESTAMPTZ       NOT NULL,
    device_id   INT               NOT NULL,
    temperature DOUBLE PRECISION
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (device_id);

INSERT INTO sensor
SELECT '2024-01-01 00:00+00'::timestamptz + (i || ' hour')::interval,
       (i % 5) + 1, 20.0 + i * 0.5
FROM generate_series(1, 50) i;

CREATE MATERIALIZED VIEW sensor_hourly
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         device_id, count(*) AS cnt, avg(temperature) AS avg_temp
  FROM sensor
  GROUP BY bucket, device_id;

CALL time_series.refresh_continuous_aggregate('sensor_hourly', NULL, NULL);

-- ============================================================
-- PERM-01: Owner can SELECT, REFRESH, ALTER — baseline
-- ============================================================
\echo '=== PERM-01: owner baseline ==='
SELECT count(*) AS owner_select FROM sensor_hourly;

CALL time_series.refresh_continuous_aggregate('sensor_hourly', NULL, NULL);

ALTER VIEW sensor_hourly SET (time_series.materialized_only = true);
SELECT materialized_only FROM time_series.continuous_agg
WHERE user_view_name = 'sensor_hourly';
ALTER VIEW sensor_hourly SET (time_series.materialized_only = false);

-- ============================================================
-- PERM-02: Unprivileged user cannot SELECT CAGG
--          (PG native: no SELECT grant on view)
-- ============================================================
\echo '=== PERM-02: no-perm user cannot SELECT ==='
\set ON_ERROR_STOP 0
SET ROLE ts_user_noperm;
SELECT count(*) FROM sensor_hourly;
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-03: GRANT SELECT → reader can query CAGG
-- ============================================================
\echo '=== PERM-03: GRANT SELECT → reader can query ==='
GRANT SELECT ON sensor_hourly TO ts_user_reader;
-- Also need SELECT on source table for real-time branch
GRANT SELECT ON sensor TO ts_user_reader;
-- Need USAGE on time_series schema for cagg_watermark() in view
GRANT USAGE ON SCHEMA time_series TO ts_user_reader;
-- Grant SELECT on mat table (referenced by the view)
GRANT SELECT ON time_series._mat_sensor_hourly_1 TO ts_user_reader;

SET ROLE ts_user_reader;
SELECT count(*) AS reader_select FROM sensor_hourly;
RESET ROLE;

-- ============================================================
-- PERM-04: Reader cannot REFRESH (not the owner)
--          REFRESH writes to mat table — requires INSERT/DELETE
-- ============================================================
\echo '=== PERM-04: reader cannot REFRESH ==='
\set ON_ERROR_STOP 0
SET ROLE ts_user_reader;
CALL time_series.refresh_continuous_aggregate('sensor_hourly', NULL, NULL);
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-05: Reader cannot ALTER materialized_only (not the owner)
-- ============================================================
\echo '=== PERM-05: reader cannot ALTER ==='
\set ON_ERROR_STOP 0
SET ROLE ts_user_reader;
ALTER VIEW sensor_hourly SET (time_series.materialized_only = true);
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-06: Reader cannot DROP the CAGG
-- ============================================================
\echo '=== PERM-06: reader cannot DROP ==='
\set ON_ERROR_STOP 0
SET ROLE ts_user_reader;
DROP VIEW sensor_hourly;
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-07: GRANT preserved across materialized_only toggle
--          (Already RT-19, but verify with actual SET ROLE query)
-- ============================================================
\echo '=== PERM-07: GRANT preserved across toggle ==='
-- Toggle to mat-only and back
ALTER VIEW sensor_hourly SET (time_series.materialized_only = true);
ALTER VIEW sensor_hourly SET (time_series.materialized_only = false);

-- Reader should still be able to query
SET ROLE ts_user_reader;
SELECT count(*) AS reader_after_toggle FROM sensor_hourly;
RESET ROLE;

-- ============================================================
-- PERM-08: REVOKE SELECT → reader can no longer query
-- ============================================================
\echo '=== PERM-08: REVOKE SELECT ==='
REVOKE SELECT ON sensor_hourly FROM ts_user_reader;

\set ON_ERROR_STOP 0
SET ROLE ts_user_reader;
SELECT count(*) FROM sensor_hourly;
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-09: View uses OWNER permissions for underlying tables
--          (PG standard: view owner's privileges are used, not
--          the querying user's. Reader needs only SELECT on view.)
-- ============================================================
\echo '=== PERM-09: view owner permissions ==='
-- Re-grant SELECT on view and mat table, but NOT on source
GRANT SELECT ON sensor_hourly TO ts_user_reader;
-- Explicitly revoke source table access
REVOKE SELECT ON sensor FROM ts_user_reader;

-- Move watermark back so real-time branch scans source table
UPDATE time_series.cagg_watermark
   SET watermark = '2024-01-01 10:00:00+00'::timestamptz
 WHERE cagg_id = (SELECT cagg_id FROM time_series.continuous_agg
                  WHERE user_view_name = 'sensor_hourly');

-- Real-time mode: reader can still query because the VIEW runs
-- with gpadmin's (owner) permissions on the source table.
SET ROLE ts_user_reader;
SELECT count(*) AS rt_via_owner_perms FROM sensor_hourly;
RESET ROLE;

-- Mat-only mode: also works via view owner permissions
ALTER VIEW sensor_hourly SET (time_series.materialized_only = true);
SET ROLE ts_user_reader;
SELECT count(*) AS mo_via_owner_perms FROM sensor_hourly;
RESET ROLE;

-- Restore
ALTER VIEW sensor_hourly SET (time_series.materialized_only = false);
CALL time_series.refresh_continuous_aggregate('sensor_hourly', NULL, NULL);

-- ============================================================
-- PERM-10: Writer with INSERT on source → trigger fires correctly
--          (trigger runs as table owner, not the inserting user)
-- ============================================================
\echo '=== PERM-10: writer INSERT triggers invalidation ==='
GRANT INSERT ON sensor TO ts_user_writer;
GRANT USAGE ON SCHEMA time_series TO ts_user_writer;

SET ROLE ts_user_writer;
INSERT INTO sensor VALUES ('2024-01-01 01:30+00', 1, 99.0);
RESET ROLE;

-- L1 should have an entry from the writer's INSERT
SELECT count(*) > 0 AS has_l1_from_writer
FROM time_series.cagg_invalidation_log;

-- REFRESH (as owner) should pick up the writer's invalidation
CALL time_series.refresh_continuous_aggregate('sensor_hourly', NULL, NULL);
SELECT count(*) AS diff_after_writer FROM (
  (  SELECT bucket, device_id, cnt, round(avg_temp::numeric, 10) FROM sensor_hourly
   EXCEPT
   SELECT time_bucket('1 hour'::interval, time), device_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM sensor GROUP BY 1, 2)
  UNION ALL
  (SELECT time_bucket('1 hour'::interval, time), device_id,
   count(*), round(avg(temperature)::numeric, 10)
   FROM sensor GROUP BY 1, 2
   EXCEPT
   SELECT bucket, device_id, cnt, round(avg_temp::numeric, 10) FROM sensor_hourly
   )
) x;

-- ============================================================
-- PERM-11: Non-owner cannot TRUNCATE source table
--          (prevents accidental CAGG invalidation by other users)
-- ============================================================
\echo '=== PERM-11: non-owner cannot TRUNCATE source ==='
\set ON_ERROR_STOP 0
SET ROLE ts_user_writer;
TRUNCATE sensor;
RESET ROLE;
\set ON_ERROR_STOP 1

-- ============================================================
-- PERM-12: Schema-level isolation — CAGG in different schema
-- ============================================================
\echo '=== PERM-12: schema isolation ==='
CREATE SCHEMA restricted_schema;

CREATE TABLE restricted_schema.metrics (
    time TIMESTAMPTZ NOT NULL, v INT NOT NULL, val FLOAT8
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '4 hour',
    ts_chunk_origin     = '2020-01-01'
) DISTRIBUTED BY (v);
INSERT INTO restricted_schema.metrics VALUES
    ('2024-01-01 00:30+00', 1, 10.0),
    ('2024-01-01 01:30+00', 2, 20.0);

CREATE MATERIALIZED VIEW restricted_schema.hourly
WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket, count(*) AS cnt
  FROM restricted_schema.metrics GROUP BY bucket;

CALL time_series.refresh_continuous_aggregate('restricted_schema.hourly', NULL, NULL);

-- Without USAGE on schema, user cannot see it
\set ON_ERROR_STOP 0
SET ROLE ts_user_noperm;
SELECT count(*) FROM restricted_schema.hourly;
RESET ROLE;
\set ON_ERROR_STOP 1

-- Grant USAGE + SELECT → can query
GRANT USAGE ON SCHEMA restricted_schema TO ts_user_reader;
GRANT SELECT ON restricted_schema.hourly TO ts_user_reader;
GRANT SELECT ON restricted_schema.metrics TO ts_user_reader;
-- Need mat table access too
DO $$
DECLARE
  mat_name text;
BEGIN
  SELECT mat_table_name INTO mat_name
  FROM time_series.continuous_agg
  WHERE user_view_name = 'hourly' AND user_view_schema = 'restricted_schema';
  EXECUTE format('GRANT SELECT ON time_series.%I TO ts_user_reader', mat_name);
END $$;

SET ROLE ts_user_reader;
SELECT count(*) AS schema_reader FROM restricted_schema.hourly;
RESET ROLE;

DROP TABLE restricted_schema.metrics CASCADE;
DROP SCHEMA restricted_schema CASCADE;

-- ============================================================
-- Cleanup
-- ============================================================
\echo '=== Cleanup ==='
REVOKE ALL ON sensor FROM ts_user_reader, ts_user_writer;
REVOKE ALL ON sensor_hourly FROM ts_user_reader;
REVOKE USAGE ON SCHEMA time_series FROM ts_user_reader, ts_user_writer;

DROP TABLE sensor CASCADE;
DROP ROLE ts_user_reader;
DROP ROLE ts_user_noperm;
DROP ROLE ts_user_writer;

\echo '=== PERMISSION TESTS DONE ==='
