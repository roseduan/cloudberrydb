/* contrib/datalake_fdw/datalake_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION datalake_fdw UPDATE TO '1.2'" to load this file. \quit

-- ------------------------------------------------------------------
-- Keeping the local metadata mirror in step with an external catalog
--
-- pg_ext_aux.pg_iceberg_metadata mirrors the external catalog's head.  The
-- #393 read path re-resolves that head on a plain SELECT once the table's
-- metadata_cache TTL has elapsed and uses it for that query -- but it never
-- writes it back, whether or not the reading backend could: an ordinary SELECT
-- does not mutate shared catalog state as a side effect.  So metadata_cached_at
-- never advances on its own, and every later read that finds an expired stamp
-- pays the external-catalog round trip again.
--
-- These two functions are the only writers of that stamp.  Both resolve
-- force-fresh through dlagent and store the answer without rolling
-- previous_metadata_location: an external writer's commit must not enter this
-- cluster's own commit lineage, which the orphan-file bookkeeping reads.
-- ------------------------------------------------------------------

-- ------------------------------------------------------------------
-- 1. The batch sweep: refresh every table whose stamp is due.
--
-- By default this cycle runs on its own, from a link in core's
-- AutoVacWorkerPostHook chain -- nothing to schedule, but the cadence is then
-- autovacuum_naptime (default 60s), so TTLs of a couple of minutes or less go
-- on expiring between cycles.  For those, turn the hook off and drive the same
-- cycle from the task scheduler on a period you pick, so the two do not both
-- spend round trips on the same tables:
--
--   ALTER SYSTEM SET datalake_fdw.metadata_refresh_enabled = off;
--   CREATE TASK iceberg_metadata_refresh SCHEDULE '30 SECONDS'
--     AS $$ SELECT iceberg_refresh_metadata_cache() $$;
--
-- (The scheduler accepts 'N SECONDS' only below a minute; use cron syntax --
--  '*/5 * * * *' and the like -- for anything coarser.  Run history then lands
--  in pg_task_run_history rather than in the server log.)
--
-- Every external-catalog table carrying metadata_cache > 0 is a candidate from
-- the moment it is created -- read or not.  A NULL stamp means "never
-- refreshed", which counts as infinitely stale, so the first cycle after
-- CREATE seeds it.  A mounted table nobody queries therefore costs one round
-- trip per TTL; leave metadata_cache unset on such a table to take it out of
-- the candidate set entirely.  Returns the number of tables actually
-- refreshed.
--
-- Coordinator-only -- it writes the QD-only pg_iceberg_metadata and resolves
-- heads through dlagent.  As with pg_iceberg_load_metadata_json(), that is NOT
-- expressible as EXECUTE ON COORDINATOR: validate_sql_exec_location() rejects
-- the clause for anything that is not set-returning ("EXECUTE ON COORDINATOR
-- is only supported for set-returning functions"), and this returns a scalar
-- count.  Adding it makes CREATE EXTENSION fail outright -- which is why every
-- EXECUTE ON ... function in this extension (datalake_acquire_sample_rows,
-- iceberg_snapshot_scan) is an SRF.  Reshaping a counter into an SRF just to
-- earn the annotation is not worth it, so the rule is enforced by the Gp_role
-- check in the C entry point.
-- ------------------------------------------------------------------
CREATE FUNCTION pg_catalog.iceberg_refresh_metadata_cache(max_tables int DEFAULT 100)
RETURNS int
AS 'MODULE_PATHNAME', 'iceberg_refresh_metadata_cache'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION pg_catalog.iceberg_refresh_metadata_cache(int) IS
'Refresh the cached external-catalog head of every Iceberg table whose metadata_cache TTL is close to expiring';

-- Writes a shared catalog and issues external-catalog calls: default-deny, the
-- DBA grants it to whichever role runs the scheduled task.
REVOKE ALL ON FUNCTION pg_catalog.iceberg_refresh_metadata_cache(int) FROM PUBLIC;


-- ------------------------------------------------------------------
-- 2. The single-table escape hatch: refresh one named table, now.
--
-- The paths that do not go through the read cache -- DML, time travel,
-- iceberg_snapshot_list() -- read the metadata.json the mirror names, which is
-- what gives a transaction read-your-own-writes.  When a storage platform
-- maintains the table on its own side (compaction, snapshot expiry,
-- orphan-file cleanup) the mirror can keep naming metadata whose manifests are
-- gone, and those paths fail on a missing file.  A table that still gets
-- written recovers on its next write; one that does not had no deliberate way
-- back before this function.
--
-- Unlike the sweep above it ignores the TTL and the candidate filters
-- entirely: the caller named the relation.
-- Returns previous_head and current_head rather than a bare boolean, because
-- the operator's next question after a broken table is how far the mirror had
-- drifted.  A built-in-catalog table is a defined no-op, not an error.
--
-- Owner-only, unlike iceberg_snapshot_list(): this rewrites state that every
-- session's reads depend on.  The C function enforces that too; revoking here
-- keeps the intent visible in \df+.
-- ------------------------------------------------------------------
CREATE FUNCTION pg_catalog.iceberg_refresh_metadata(
    tbl regclass,
    OUT refreshed bool,
    OUT previous_head text,
    OUT current_head text)
RETURNS record
AS 'MODULE_PATHNAME', 'iceberg_refresh_metadata'
LANGUAGE C STRICT VOLATILE;

COMMENT ON FUNCTION pg_catalog.iceberg_refresh_metadata(regclass) IS
'Re-point one Iceberg table''s local metadata mirror at its catalog''s current head';

REVOKE ALL ON FUNCTION pg_catalog.iceberg_refresh_metadata(regclass) FROM PUBLIC;
