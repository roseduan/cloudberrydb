/* contrib/datalake_fdw/datalake_fdw--1.0.sql */

\echo Use "CREATE EXTENSION datalake_fdw" to load this file. \quit

CREATE FUNCTION datalake_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION datalake_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Iceberg Catalog FDW functions
CREATE FUNCTION iceberg_catalog_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION iceberg_catalog_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;


CREATE FOREIGN DATA WRAPPER iceberg_catalog_fdw
    HANDLER iceberg_catalog_fdw_handler
    VALIDATOR iceberg_catalog_fdw_validator;

-- Iceberg Volume FDW functions
CREATE FUNCTION iceberg_volume_fdw_handler()
RETURNS fdw_handler
AS '$libdir/datalake_fdw.so'
LANGUAGE C STRICT;

CREATE FUNCTION iceberg_volume_fdw_validator(text[], oid)
RETURNS void
AS '$libdir/datalake_fdw.so'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_volume_fdw
    HANDLER iceberg_volume_fdw_handler
    VALIDATOR iceberg_volume_fdw_validator;


CREATE FUNCTION gp_toolkit.__gopher_cache_free_relation_name(text)
RETURNS bool
AS '$libdir/datalake_fdw.so' , 'hdw_gopher_cache_free_relation_name_wrapper'
LANGUAGE C STRICT;
GRANT EXECUTE ON FUNCTION gp_toolkit.__gopher_cache_free_relation_name(text) TO public;


CREATE FUNCTION gp_toolkit.__gopher_free_all_cache()
RETURNS bool
AS '$libdir/datalake_fdw.so' , 'hdw_gopher_free_all_cache_wrapper'
LANGUAGE C STRICT;
GRANT EXECUTE ON FUNCTION gp_toolkit.__gopher_free_all_cache() TO public;

CREATE FUNCTION datalake_acquire_sample_rows(oid, int, boolean, text)
RETURNS setof record
AS 'MODULE_PATHNAME','datalake_acquire_sample_rows'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

-- Iceberg Toolkit Functions
-- Core functions for Iceberg catalog and volume operations

-- Create schema
CREATE SCHEMA IF NOT EXISTS iceberg_toolkit;

-- Catalog operations (create_table, get_fragment)
CREATE OR REPLACE FUNCTION iceberg_toolkit.catalog_fdw(
    operation text,
    name_space text,
    table_name text,
    catalog_server_name text,
    catalog_table_name text,
    volume_server_name text,
    volume_table_name text,
    append_json_string text
)
RETURNS text
AS '$libdir/datalake_fdw.so', 'iceberg_toolkit_catalog_fdw'
LANGUAGE C STRICT;

-- Volume operations (read data)
CREATE OR REPLACE FUNCTION iceberg_toolkit.volume_fdw(
    operation text,
    name_space text,
    table_name text,
    row_limit integer,
    catalog_server_name text,
    catalog_table_name text,
    volume_server_name text,
    volume_table_name text
)
RETURNS SETOF record
AS '$libdir/datalake_fdw.so', 'iceberg_toolkit_volume_fdw'
LANGUAGE C STRICT;

-- Convenience functions
CREATE OR REPLACE FUNCTION iceberg_toolkit.create_table(
    table_name text,
    name_space text,
    catalog_server text,
    catalog_table text,
    volume_server text,
    volume_table text
)
RETURNS text
AS $$
    SELECT iceberg_toolkit.catalog_fdw(
        'create_table', name_space, table_name,
        catalog_server, catalog_table, volume_server, volume_table, ""
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION iceberg_toolkit.get_fragments(
    table_name text,
    name_space text,
    catalog_server text,
    catalog_table text,
    volume_server text,
    volume_table text
)
RETURNS text
AS $$
    SELECT iceberg_toolkit.catalog_fdw(
        'get_fragment', name_space, table_name,
        catalog_server, catalog_table, volume_server, volume_table, ""
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION iceberg_toolkit.polaris_list_catalogs(
    datalake_agent_url text,
    polaris_url text,
    client_id text,
    client_secret text,
    scope text DEFAULT 'PRINCIPAL_ROLE:ALL'
)
RETURNS text
AS '$libdir/datalake_fdw.so', 'polaris_list_catalogs'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION iceberg_toolkit.polaris_list_namespaces(
    datalake_agent_url text,
    polaris_url text,
    client_id text,
    client_secret text,
    catalog_name text,
    scope text DEFAULT 'PRINCIPAL_ROLE:ALL'
)
RETURNS text
AS '$libdir/datalake_fdw.so', 'polaris_list_namespaces'
LANGUAGE C STRICT;

-- Permissions
GRANT USAGE ON SCHEMA iceberg_toolkit TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA iceberg_toolkit TO public;

-- -- Grant permissions
-- GRANT USAGE ON SCHEMA iceberg_toolkit TO public;
-- GRANT EXECUTE ON FUNCTION iceberg_toolkit.catalog_fdw(text, text, text, text, text, text) TO public;

-- ============================================================================
-- Iceberg Schema and Metadata Table Setup
-- ============================================================================

-------------------------------------
-- Note: the 'iceberg' access method (OID 8320) and its handler
-- pg_iceberg_tableam_handler (OID 8321) are preinstalled at initdb time
-- via iceberg-cdbinit--1.0.sql, so no CREATE ACCESS METHOD here.

-- Internal helper used by C code to dispatch location option upsert to QEs
CREATE FUNCTION pg_catalog.pg_iceberg_upsert_location_option_local(oid, text)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_iceberg_upsert_location_option_local'
LANGUAGE C STRICT;


-- Create iceberg schema to organize all iceberg-related objects
CREATE SCHEMA iceberg;

-- Catalog tables for iceberg metadata.
--
-- These were previously created from C via heap_create_with_catalog(), invoked
-- from `SELECT iceberg.pg_iceberg_create_*_table()` calls inside this script.
-- That pattern only ran on the QD (a SELECT of a plain function is not a
-- utility statement and is not dispatched to QEs), so the tables ended up
-- existing on the coordinator but missing on every segment.  pg_dump's
-- LOCK TABLE iceberg.pg_iceberg_metadata then failed on segments.  See #324.
--
-- Using plain CREATE TABLE makes this go through ProcessUtility ->
-- CdbDispatchUtilityStatement, so QD and all QEs stay in sync.  The C-level
-- access paths (pg_iceberg_add_metadata / get_metadata_info / deletion queue
-- helpers) keep looking up the relation by name, so no OID needs to be fixed.
CREATE TABLE iceberg.pg_iceberg_metadata (
    relid                       oid  PRIMARY KEY,
    metadata_location           text,
    previous_metadata_location  text,
    is_internal                 bool,
    default_spec_id             int4
) DISTRIBUTED BY (relid);

CREATE TABLE iceberg.pg_iceberg_deletion_queue (
    path           text     PRIMARY KEY,
    table_name     regclass,
    orphaned_at    timestamptz,
    retry_count    int4,
    deletion_type  int4
) DISTRIBUTED BY (path);
