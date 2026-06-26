/* contrib/pg_tde/pg_tde--1.0.sql */

--
-- pg_tde_status() — report every tablespace DEK currently loaded in shmem.
--
CREATE FUNCTION pg_tde_status(
    OUT spc_oid     oid,
    OUT spc_name    text,
    OUT enc_method  text,
    OUT loaded      bool
)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'pg_tde_status'
LANGUAGE C STRICT PARALLEL SAFE;

-- Convenience view
CREATE VIEW pg_tde_tablespace_status AS
    SELECT * FROM pg_tde_status();

--
-- tde_open_tablespace(name) — unlock a tablespace whose DEK was not loaded
-- at startup (e.g. because the KMS was temporarily unreachable).
-- Returns true on success, false if the KMS is still unavailable.
--
CREATE FUNCTION tde_open_tablespace(name text)
RETURNS bool
AS 'MODULE_PATHNAME', 'tde_open_tablespace'
LANGUAGE C STRICT;

--
-- tde_sync_keys() — rescan pg_cryptokeys/tablespaces/ and load any .wkey
-- files that are not yet in shared memory.
-- Returns the count of newly loaded DEKs.
--
CREATE FUNCTION tde_sync_keys()
RETURNS integer
AS 'MODULE_PATHNAME', 'tde_sync_keys'
LANGUAGE C STRICT;

-- Restrict to superusers only
REVOKE ALL ON FUNCTION pg_tde_status() FROM PUBLIC;
REVOKE ALL ON TABLE pg_tde_tablespace_status FROM PUBLIC;
REVOKE ALL ON FUNCTION tde_open_tablespace(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION tde_sync_keys() FROM PUBLIC;
