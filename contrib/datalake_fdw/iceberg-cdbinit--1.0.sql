-- Preinstall the 'iceberg' table access method with a stable OID.
--
-- The OID is hardcoded in src/include/utils/rel.h (ICEBERG_AM_OID 8320) and
-- used by the RelationIsIceberg() macro on hot paths. Installing the AM here
-- via initdb's cdb_init.d hook guarantees the OID is exactly 8320 on every
-- node, the same mechanism PAX uses for PAX_AM_OID 7047.
--
-- Inserting directly into pg_proc / pg_am (instead of going through
-- CREATE ACCESS METHOD in the datalake_fdw extension SQL) avoids the
-- "allocate a random OID, then UPDATE pg_am.oid to 8320" rewrite trick
-- that left orphan rows in pg_depend (see issue #320).

-- Handler function: pg_iceberg_tableam_handler(internal) -> table_am_handler
-- Columns follow Cloudberry's 32-column pg_proc layout (see pg_proc.h).
INSERT INTO pg_proc VALUES(8321,'pg_iceberg_tableam_handler',11,10,13,1,0,0,0,'f','f','f','t','f','v','u',1,0,269,'2281',null,null,null,null,null,'pg_iceberg_tableam_handler','$libdir/datalake_fdw',null,null,null,'n','a');

-- Access method: iceberg, TYPE TABLE (t), handler at OID 8321.
INSERT INTO pg_am VALUES(8320,'iceberg',8321,'t');

COMMENT ON FUNCTION pg_iceberg_tableam_handler(internal) IS 'iceberg table access method handler for datalake_fdw';

-- ============================================================================
-- Iceberg native catalog tables
--
-- Installed here (cdb_init.d, runs once per node at initdb time) instead of
-- the datalake_fdw extension SQL so that:
--
--   * The tables exist on every node from the moment initdb finishes -- no
--     CREATE EXTENSION required, no CdbDispatchUtilityStatement involved.
--     This matches pax's pg_pax_fastsequence model and is the same effect
--     as PostgreSQL's built-in BKI catalogs (e.g. pg_foreign_data_wrapper).
--
--   * They live in the same OID space as the AM (8320..8339) and are
--     reached from C through stable OID constants (see
--     contrib/datalake_fdw/src/am_iceberg/include/iceberg_oids.h), so the
--     hot-path catalog lookups skip get_relname_relid() entirely and the
--     name-drift bug class (#324 follow-up) cannot recur.
--
--   * pg_dump's LOCK TABLE finds the tables on every segment without any
--     dispatch trick.
--
-- OID layout (must stay in sync with iceberg_oids.h):
--     8330  pg_iceberg_metadata              table
--     8331  pg_iceberg_metadata_pkey         unique index (PRIMARY KEY)
--     8334  pg_iceberg_deletion_queue        table
--     8335  pg_iceberg_deletion_queue_pkey   unique index (PRIMARY KEY)
--     8336  pg_iceberg_deletion_failed       table (dead-letter queue)
--     8337  pg_iceberg_deletion_failed_pkey  unique index (PRIMARY KEY)
--
-- The tables live in pg_ext_aux, the PostgreSQL/Cloudberry built-in schema
-- (postgres.bki entry, oid 7094) reserved for extension auxiliary catalogs.
-- This is the same schema pax uses for pg_pax_fastsequence.  Reusing it --
-- rather than creating a "pg_iceberg" schema with the "pg_" prefix trick --
-- buys three things a self-rolled schema cannot:
--   * pg_dump's selectDumpableNamespace already skips pg_ext_aux exactly
--     like any built-in catalog, so restore never tries to recreate these
--     tables.
--   * heap.c:1680 skips array type creation for pg_ext_aux relations and
--     namespace.c:3084 forbids ALTER SCHEMA RENAME -- giving the schema
--     true system-namespace semantics rather than user-namespace lookalike.
--   * Downstream tools that filter by namespace whitelist (e.g.
--     postgresql-anonymizer's pg_identifiers view, which casts relname to
--     REGCLASS without a schema qualifier) automatically ignore objects
--     here.  A "pg_iceberg" schema with the same oid range is still seen
--     as user namespace by these tools and breaks them.
-- ============================================================================

-- pg_iceberg_metadata: one row per Iceberg table, points at the current
-- manifest list.  Mirror of what datalake_fdw--1.0.sql used to create.
--
-- text columns use COLLATE "C": these are system catalogs (oid < 16384) and
-- get cloned from template0 into every database, so they must not carry a
-- collation-sensitive ordering -- otherwise a database created with a
-- non-C collation would inherit a mismatched catalog (opr_sanity enforces
-- this; see "Check for system catalogs with collation-sensitive ordering").
CREATE TABLE pg_ext_aux.pg_iceberg_metadata (
    relid                       oid   PRIMARY KEY,
    metadata_location           text  COLLATE "C",
    previous_metadata_location  text  COLLATE "C",
    is_internal                 bool,
    default_spec_id             int4
);

-- Pin OIDs to 8330 (table) and 8331 (pkey index) following the same
-- catalog-rewrite recipe pax-cdbinit uses for pg_pax_fastsequence.
UPDATE pg_type      SET typrelid  = 8330 WHERE typname = 'pg_iceberg_metadata';
UPDATE pg_depend    SET refobjid  = 8330 WHERE refobjid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata');
UPDATE pg_depend    SET objid     = 8330 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata');
UPDATE pg_depend    SET objid     = 8331 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata_pkey');
UPDATE pg_attribute SET attrelid  = 8330 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata');
UPDATE pg_attribute SET attrelid  = 8331 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata_pkey');
UPDATE pg_index     SET indexrelid = 8331, indrelid = 8330
    WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata_pkey')
      AND indrelid   = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata');
UPDATE pg_constraint SET conrelid = 8330 WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata');
UPDATE pg_constraint SET conindid = 8331 WHERE conindid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_metadata_pkey');
UPDATE pg_class     SET oid       = 8330 WHERE relname = 'pg_iceberg_metadata';
UPDATE pg_class     SET oid       = 8331 WHERE relname = 'pg_iceberg_metadata_pkey';

-- pg_iceberg_deletion_queue: pending orphan-file deletions waiting for the
-- autovacuum-driven consumer (see pg_iceberg_av_consumer.c).
--
-- Columns beyond (path, table_name, orphaned_at, retry_count, deletion_type)
-- carry the credential context that the consumer needs to call dlagent's
-- /v1/files/cleanup-from-metadata endpoint after DROP TABLE has already
-- removed the foreign volume / foreign server lookup path through pg_lake_table:
--
--   volume_name     -- pg_foreign_volume.fvname  → reconstruct fileIOConfig
--   server_name     -- pg_foreign_server.srvname → reconstruct fileIOConfig
--   owner_username  -- rolname of the DROP executor → pg_user_mapping lookup key
--   table_qname     -- "nspname.relname" snapshot for audit/log (oid is dead post-DROP)
--   last_error      -- textual reason of the most recent retry failure
CREATE TABLE pg_ext_aux.pg_iceberg_deletion_queue (
    path           text COLLATE "C" PRIMARY KEY,
    table_name     regclass,
    orphaned_at    timestamptz,
    retry_count    int4,
    deletion_type  int4,
    volume_name    text COLLATE "C",
    server_name    text COLLATE "C",
    owner_username text COLLATE "C",
    table_qname    text COLLATE "C",
    last_error     text COLLATE "C"
);

-- Pin OIDs to 8334 (table) and 8335 (pkey index).
UPDATE pg_type      SET typrelid  = 8334 WHERE typname = 'pg_iceberg_deletion_queue';
UPDATE pg_depend    SET refobjid  = 8334 WHERE refobjid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue');
UPDATE pg_depend    SET objid     = 8334 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue');
UPDATE pg_depend    SET objid     = 8335 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue_pkey');
UPDATE pg_attribute SET attrelid  = 8334 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue');
UPDATE pg_attribute SET attrelid  = 8335 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue_pkey');
UPDATE pg_index     SET indexrelid = 8335, indrelid = 8334
    WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue_pkey')
      AND indrelid   = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue');
UPDATE pg_constraint SET conrelid = 8334 WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue');
UPDATE pg_constraint SET conindid = 8335 WHERE conindid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_queue_pkey');
UPDATE pg_class     SET oid       = 8334 WHERE relname = 'pg_iceberg_deletion_queue';
UPDATE pg_class     SET oid       = 8335 WHERE relname = 'pg_iceberg_deletion_queue_pkey';

-- pg_iceberg_deletion_failed: dead-letter queue (DLQ) for entries that
-- exceeded datalake_fdw.deletion_queue_max_retry attempts.  Schema mirrors
-- pg_iceberg_deletion_queue plus a failed_at timestamp recording when the
-- row was pushed to the DLQ.  Operators triage by inspecting last_error
-- (often clustered) and replay rows with
--   SELECT pg_ext_aux.pg_iceberg_retry_failed_deletion(path).
CREATE TABLE pg_ext_aux.pg_iceberg_deletion_failed (
    path           text COLLATE "C" PRIMARY KEY,
    table_name     regclass,
    orphaned_at    timestamptz,
    retry_count    int4,
    deletion_type  int4,
    volume_name    text COLLATE "C",
    server_name    text COLLATE "C",
    owner_username text COLLATE "C",
    table_qname    text COLLATE "C",
    last_error     text COLLATE "C",
    failed_at      timestamptz
);

-- Pin OIDs to 8336 (table) and 8337 (pkey index).
UPDATE pg_type      SET typrelid  = 8336 WHERE typname = 'pg_iceberg_deletion_failed';
UPDATE pg_depend    SET refobjid  = 8336 WHERE refobjid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed');
UPDATE pg_depend    SET objid     = 8336 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed');
UPDATE pg_depend    SET objid     = 8337 WHERE objid    = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed_pkey');
UPDATE pg_attribute SET attrelid  = 8336 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed');
UPDATE pg_attribute SET attrelid  = 8337 WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed_pkey');
UPDATE pg_index     SET indexrelid = 8337, indrelid = 8336
    WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed_pkey')
      AND indrelid   = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed');
UPDATE pg_constraint SET conrelid = 8336 WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed');
UPDATE pg_constraint SET conindid = 8337 WHERE conindid = (SELECT oid FROM pg_class WHERE relname = 'pg_iceberg_deletion_failed_pkey');
UPDATE pg_class     SET oid       = 8336 WHERE relname = 'pg_iceberg_deletion_failed';
UPDATE pg_class     SET oid       = 8337 WHERE relname = 'pg_iceberg_deletion_failed_pkey';
