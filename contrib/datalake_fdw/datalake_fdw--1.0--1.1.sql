/* contrib/datalake_fdw/datalake_fdw--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION datalake_fdw UPDATE TO '1.1'" to load this file. \quit

-- ------------------------------------------------------------------
-- Iceberg time travel (issue #397)
--
-- iceberg_snapshot_scan(table, snapshot_id) reads a historical snapshot.
-- The result column set is resolved at parse analysis by the DESCRIBE
-- callback; the third argument carries the QD-resolved fragment list and the
-- fourth the snapshot schema's field-ids (csv) -- both injected by the
-- planner hook, users never supply them.
--
-- Added in 1.1 as an extension upgrade so existing 1.0 installs pick the
-- functions up via ALTER EXTENSION datalake_fdw UPDATE.
-- ------------------------------------------------------------------
CREATE FUNCTION iceberg_snapshot_describe(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'iceberg_snapshot_describe'
LANGUAGE C;

-- Planner support: supply the snapshot's real row count so a schema-changed
-- historical read (FunctionScan fallback) is not estimated at the default 1000.
CREATE FUNCTION iceberg_snapshot_scan_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'iceberg_snapshot_scan_support'
LANGUAGE C STRICT;

CREATE FUNCTION iceberg_snapshot_scan(regclass, bigint, text DEFAULT NULL,
                                      text DEFAULT NULL)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'iceberg_snapshot_scan'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS
WITH (describe = iceberg_snapshot_describe);

ALTER FUNCTION iceberg_snapshot_scan(regclass, bigint, text, text)
    SUPPORT iceberg_snapshot_scan_support;

-- AS OF TIMESTAMP: same C function, selector argument typed timestamptz.  The
-- instant is folded on the QD to the newest snapshot committed at or before it
-- (Iceberg TableScan.asOfTime semantics), then the snapshot-id machinery runs
-- unchanged.  Sharing the C symbol keeps both overloads recognized by the
-- planner hook and the relation rewrite, which identify the function by symbol.
CREATE FUNCTION iceberg_snapshot_scan(regclass, timestamptz, text DEFAULT NULL,
                                      text DEFAULT NULL)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'iceberg_snapshot_scan'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS
WITH (describe = iceberg_snapshot_describe);

ALTER FUNCTION iceberg_snapshot_scan(regclass, timestamptz, text, text)
    SUPPORT iceberg_snapshot_scan_support;

-- ------------------------------------------------------------------
-- Snapshot discovery
--
-- Without this a user has no way to learn which snapshot ids exist short of
-- reading metadata.json or querying another engine.  Reads the same PINNED
-- metadata a time-travel read would, so every id listed here is selectable.
-- ------------------------------------------------------------------
CREATE FUNCTION iceberg_snapshot_list_json(regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'iceberg_snapshot_list_json'
LANGUAGE C STRICT;

CREATE FUNCTION iceberg_snapshot_list(tbl regclass)
RETURNS TABLE (
    snapshot_id  bigint,
    committed_at timestamptz,
    operation    text,
    schema_id    int,
    parent_id    bigint,
    is_current   bool,
    summary      jsonb
)
AS $$
    SELECT (s->>'snapshotId')::bigint,
           -- Iceberg records commit time as Unix epoch milliseconds.
           to_timestamp((s->>'timestampMs')::bigint / 1000.0),
           NULLIF(s->>'operation', ''),
           -- The agent reports -1 / 0 for "the snapshot records none".
           NULLIF((s->>'schemaId')::int, -1),
           NULLIF((s->>'parentId')::bigint, 0),
           (s->>'snapshotId')::bigint = (j->>'currentSnapshotId')::bigint,
           (s->>'summaryJson')::jsonb
      FROM (SELECT iceberg_snapshot_list_json(tbl)::jsonb AS j) t,
           LATERAL jsonb_array_elements(t.j->'snapshots') AS s;
$$ LANGUAGE SQL STABLE;

-- ------------------------------------------------------------------
-- Iceberg volume configuration on the QE data plane (issue #410)
--
-- Added in 1.1 for the same reason as the functions above: a 1.0
-- install picks it up through ALTER EXTENSION datalake_fdw UPDATE
-- rather than only on a fresh CREATE EXTENSION.
-- ------------------------------------------------------------------
-- Renders the s3.conf / gphdfs.conf section a volume's server_name selects,
-- as the QD parsed it.  Its reason to exist is the differential test that
-- holds this parser to the agent's SiteConfigLoader: both sides dump the
-- same section and the fixtures compare them field by field.  It doubles as
-- the way to answer "what did the coordinator actually read" on a live
-- system.  Superuser only -- base_dir would otherwise let any user aim the
-- backend at an arbitrary YAML file and read pieces of it back.
CREATE FUNCTION pg_catalog.pg_iceberg_site_config(server_name text,
                                                  volume_type text,
                                                  base_dir text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_iceberg_site_config'
LANGUAGE C;

REVOKE ALL ON FUNCTION pg_catalog.pg_iceberg_site_config(text, text, text)
    FROM PUBLIC;

-- ------------------------------------------------------------------
-- RENAME COLUMN on an Iceberg partition column (issue #444)
--
-- The PARTITION BY declaration lives in pg_lake_table.ltoptions as a
-- comma-separated list of column names.  pg_attribute.attname is dispatched
-- to the segments by the standard ALTER, but pg_lake_table is not, so the QD
-- rewrites the option and dispatches this function to keep every segment's
-- copy in step.  Internal helper, called only from C.
--
-- EXECUTE is deliberately NOT revoked from PUBLIC, for the same reason spelled
-- out over pg_iceberg_alter_column_type_local and followed by the sibling
-- pg_iceberg_upsert_location_option_local: the QD dispatches this to every
-- segment in the invoking user's own session, so a non-superuser table owner
-- running ALTER TABLE ... RENAME COLUMN has to be able to call it.  The gate
-- is the ownership check in the C body instead.
-- ------------------------------------------------------------------
CREATE FUNCTION pg_catalog.pg_iceberg_upsert_partition_by_option_local(oid, text)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_iceberg_upsert_partition_by_option_local'
LANGUAGE C STRICT;
