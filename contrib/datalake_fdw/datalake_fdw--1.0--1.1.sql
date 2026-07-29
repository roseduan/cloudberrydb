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
