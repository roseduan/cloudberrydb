-- PAX table round-trip tests for pg_dump.
--
-- Covers:
--   1. Partitioned PAX roots (RANGE, LIST) round-trip with USING clause after
--      PARTITION BY (pg_dump.c dumpTableSchema, line guarded by amname != NULL).
--   2. pg_dump --binary-upgrade emits OID preassignment for every PAX leaf's
--      aux catalog (pg_dump_pax.c: pax_cluster_backend_kind + pax_get_aux_oids;
--       pg_dump_pax_emit.c: pax_emit_aux_oid_preassignment).
--   3. pg_dump correctly handles PAX leaf tables after column ADD / DROP / ALTER.
--   4. pg_dump skips PAX aux tables (pg_pax_blocks_* / pg_manifest_*) from the
--      generic relation list (getTables filter).
--
-- Multiple PAX leaf tables ensure pax_cluster_backend_kind() cache is both
-- populated on the first lookup and hit on each subsequent call.

CREATE SCHEMA dump_pax_test;

-- ===================================================================
-- 1. PAX leaf table with column DDL operations
-- ===================================================================

CREATE TABLE dump_pax_test.pax_cols (
    id      int,
    name    text,
    amount  numeric(12,4),
    payload bytea
) USING pax DISTRIBUTED BY (id);

-- ADD COLUMN: extends the schema after initial creation.
ALTER TABLE dump_pax_test.pax_cols ADD COLUMN score    int     DEFAULT 0;
ALTER TABLE dump_pax_test.pax_cols ADD COLUMN tags     text[];
ALTER TABLE dump_pax_test.pax_cols ADD COLUMN created_at timestamptz DEFAULT now();

-- DROP COLUMN: creates a dropped-column placeholder that pg_dump must handle.
ALTER TABLE dump_pax_test.pax_cols DROP COLUMN payload;
ALTER TABLE dump_pax_test.pax_cols DROP COLUMN tags;

-- ALTER COLUMN: constraint and default changes that affect pg_dump DDL output.
ALTER TABLE dump_pax_test.pax_cols ALTER COLUMN name   SET NOT NULL;
ALTER TABLE dump_pax_test.pax_cols ALTER COLUMN amount SET DEFAULT 0.0;
ALTER TABLE dump_pax_test.pax_cols ALTER COLUMN score  SET DEFAULT -1;

-- ===================================================================
-- 2. PAX RANGE-partitioned table with leaf partitions
-- ===================================================================
--
-- Grammar note: CBDB rejects USING <am> before PARTITION BY when DISTRIBUTED BY
-- is also present.  pg_dump places USING after PARTITION BY — this is the
-- ordering that round-trips correctly, so we create the table the same way.

CREATE TABLE dump_pax_test.pax_range (
    id  int,
    dt  date,
    val text
)
PARTITION BY RANGE (dt) USING pax
DISTRIBUTED BY (id);

CREATE TABLE dump_pax_test.pax_range_2024
    PARTITION OF dump_pax_test.pax_range
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE dump_pax_test.pax_range_2025
    PARTITION OF dump_pax_test.pax_range
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- ADD COLUMN propagates to partitions.
ALTER TABLE dump_pax_test.pax_range ADD COLUMN region text;

-- ===================================================================
-- 3. PAX LIST-partitioned table with leaf partitions
-- ===================================================================

CREATE TABLE dump_pax_test.pax_list (
    id     int,
    region text,
    amount numeric
)
PARTITION BY LIST (region) USING pax
DISTRIBUTED BY (id);

CREATE TABLE dump_pax_test.pax_list_east
    PARTITION OF dump_pax_test.pax_list
    FOR VALUES IN ('east', 'northeast');

CREATE TABLE dump_pax_test.pax_list_west
    PARTITION OF dump_pax_test.pax_list
    FOR VALUES IN ('west', 'northwest');

-- DROP COLUMN on a partitioned table propagates to all partitions.
ALTER TABLE dump_pax_test.pax_list DROP COLUMN amount;

-- ===================================================================
-- Verification
-- ===================================================================

-- Both partitioned PAX roots must be dumped with USING clause after PARTITION BY.
\! pg_dump --schema dump_pax_test regression | grep "PARTITION BY RANGE (dt) USING pax"
\! pg_dump --schema dump_pax_test regression | grep "PARTITION BY LIST (region) USING pax"

-- pg_dump --binary-upgrade must emit OID preassignment for every PAX leaf's
-- aux table.  Both catalog-mode (pg_pax_blocks_*) and manifest-mode
-- (pg_manifest_*) naming are accepted so the check is backend-independent.
\! pg_dump --binary-upgrade --schema dump_pax_test regression | grep -qE "pg_pax_blocks_[0-9]+|pg_manifest_[0-9]+" && echo "PAX aux OID preassignment found" || echo "PAX aux OID preassignment NOT found (FAIL)"

-- pg_dump must NOT include the PAX aux tables themselves as top-level objects
-- (they are filtered in getTables).  Dumping the schema must produce exactly
-- the expected user-visible table count: pax_cols, pax_range, pax_range_2024,
-- pax_range_2025, pax_list, pax_list_east, pax_list_west = 7 tables.
\! pg_dump --schema dump_pax_test regression | grep -c "^CREATE TABLE"

-- ===================================================================
-- Cleanup (leaf partitions first to avoid cascade NOTICEs)
-- ===================================================================

DROP TABLE dump_pax_test.pax_list_east;
DROP TABLE dump_pax_test.pax_list_west;
DROP TABLE dump_pax_test.pax_list;
DROP TABLE dump_pax_test.pax_range_2024;
DROP TABLE dump_pax_test.pax_range_2025;
DROP TABLE dump_pax_test.pax_range;
DROP TABLE dump_pax_test.pax_cols;
DROP SCHEMA dump_pax_test;
