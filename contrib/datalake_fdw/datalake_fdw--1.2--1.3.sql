/* contrib/datalake_fdw/datalake_fdw--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION datalake_fdw UPDATE TO '1.3'" to load this file. \quit

-- ------------------------------------------------------------------
-- gprestore metadata-only restore of builtin iceberg tables (issue #457)
--
-- gprestore recreates the table, then attach_metadata() re-points it at the
-- metadata.json recorded at backup time.  It is a function rather than a
-- table OPTION on purpose: unknown OPTIONs are silently ignored, so restoring
-- onto an older server would quietly produce an empty table, whereas a
-- missing function fails loudly.
--
-- Superuser only (REVOKE below, and checked again in C before any lock):
-- the caller names an arbitrary metadata path, and builtin tables of a
-- volume share one directory, so a table owner could otherwise attach --
-- read, and on DROP delete -- another table's files.  gprestore runs as a
-- superuser.
-- ------------------------------------------------------------------
CREATE FUNCTION iceberg_toolkit.attach_metadata_internal(regclass, text)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_iceberg_attach_metadata_internal'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION iceberg_toolkit.attach_metadata(tbl regclass,
                                                metadata_location text)
RETURNS void
LANGUAGE plpgsql STRICT VOLATILE
AS $$
DECLARE
    doc           jsonb;
    iceberg_cols  text[];
    table_cols    text[];
BEGIN
    doc := iceberg_toolkit.attach_metadata_internal(tbl, metadata_location)::jsonb;

    -- Spec evolution is unsupported, so a builtin table always writes with
    -- spec 0 (see pg_iceberg_ddl.c); anything else cannot be appended to.
    IF coalesce((doc->>'default-spec-id')::int, 0) <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'feature_not_supported',
            MESSAGE = format('iceberg metadata "%s" uses partition spec %s, only spec 0 can be attached',
                             metadata_location, doc->>'default-spec-id');
    END IF;

    SELECT array_agg(f->>'name' ORDER BY t.ord) INTO iceberg_cols
      FROM jsonb_array_elements(doc->'schemas') s,
           jsonb_array_elements(s->'fields') WITH ORDINALITY AS t(f, ord)
     WHERE (s->>'schema-id')::int = (doc->>'current-schema-id')::int;

    SELECT array_agg(attname::text ORDER BY attnum) INTO table_cols
      FROM pg_catalog.pg_attribute
     WHERE attrelid = tbl AND attnum > 0 AND NOT attisdropped;

    IF iceberg_cols IS DISTINCT FROM table_cols THEN
        RAISE EXCEPTION USING
            ERRCODE = 'datatype_mismatch',
            MESSAGE = format('columns of %s do not match iceberg metadata "%s"',
                             tbl, metadata_location),
            DETAIL = format('table: %s, metadata: %s', table_cols, iceberg_cols);
    END IF;
END
$$;

REVOKE ALL ON FUNCTION iceberg_toolkit.attach_metadata_internal(regclass, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION iceberg_toolkit.attach_metadata(regclass, text) FROM PUBLIC;

-- ------------------------------------------------------------------
-- restore_table(tbl, ddl, metadata_location): the one statement gpbackup
-- emits per iceberg table, so gprestore needs no iceberg-specific logic.
--   * tbl already exists      -> NOTICE, skip (its files are live); a
--                                same-named non-iceberg relation is an error
--   * otherwise               -> run ddl (CREATE ICEBERG TABLE plus the
--                                table's COMMENT/GRANT statements) and, for
--                                builtin tables, attach metadata_location
--   * metadata file gone or table pending deletion (58P01)
--                             -> WARNING, roll the table back, skip it;
--                                the rest of the restore continues
-- External-catalog tables pass a NULL metadata_location: their catalog owns
-- the metadata, and CREATE adopts the existing table.  Any other error
-- (e.g. metadata already used by another table) is raised.
-- ------------------------------------------------------------------
CREATE FUNCTION iceberg_toolkit.restore_table(tbl text, ddl text,
                                              metadata_location text)
RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    existing  regclass := to_regclass(tbl);
    -- Set right before attach_metadata: a plpgsql variable keeps its value
    -- when the block's subtransaction rolls back, so the handler can tell
    -- attach_metadata's 58P01 from one raised by the DDL without matching
    -- message text.
    attaching bool := false;
BEGIN
    IF existing IS NOT NULL THEN
        -- Only an iceberg table counts as "already restored"; anything else
        -- of that name is a conflict, and skipping would silently lose the
        -- iceberg table.
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_lake_table
                        WHERE ltrelid = existing AND lttable_type = 'ICEBERG') THEN
            RAISE EXCEPTION 'relation % already exists and is not an iceberg table', tbl
                USING ERRCODE = 'duplicate_table';
        END IF;
        RAISE NOTICE 'iceberg table % already exists, skipping', tbl;
        RETURN;
    END IF;

    BEGIN
        EXECUTE ddl;
        IF metadata_location IS NOT NULL THEN
            attaching := true;
            PERFORM iceberg_toolkit.attach_metadata(tbl::regclass, metadata_location);
        END IF;
    EXCEPTION WHEN undefined_file THEN
        -- Only attach_metadata's 58P01 (file gone / pending deletion) means
        -- "skip"; a 58P01 raised by the DDL must surface.
        IF NOT attaching THEN
            RAISE;
        END IF;
        RAISE WARNING 'iceberg table % was not restored: its metadata file no longer exists or is pending deletion', tbl
            USING DETAIL = SQLERRM;
    END;
END
$$;

REVOKE ALL ON FUNCTION iceberg_toolkit.restore_table(text, text, text) FROM PUBLIC;
