-- Sub-project C, security item B: remove the blanket PUBLIC grant on the internal iceberg
-- metadata catalog and expose an authz-gated accessor instead. Apply as superuser.
--
-- NOTE (review Minor): these two REVOKEs only revert the grant the e2e seed itself made
-- (seed_builtin.sql grants pg_iceberg_metadata/pg_ext_aux to PUBLIC for test convenience).
-- pg_ext_aux is owner-only by default — no PUBLIC grant is baked into pg_namespace.dat for it —
-- so this does NOT reach into or affect PAX or other datalake_fdw subsystems: those access the
-- aux tables by OID from C code, not via schema-qualified SQL, and never depended on a PUBLIC
-- grant here.
REVOKE SELECT ON pg_ext_aux.pg_iceberg_metadata FROM PUBLIC;
REVOKE USAGE  ON SCHEMA pg_ext_aux            FROM PUBLIC;

-- Definer function: runs as owner (can read pg_ext_aux) but filters by the *caller-supplied*
-- p_role. current_user inside a SECURITY DEFINER body is the OWNER, so we CANNOT use it for the
-- filter (that is the D3 trap); the trusted authenticator passes the JWT-authenticated role.
--
-- Hardening beyond the base design: SET search_path pins name resolution to pg_catalog +
-- pg_ext_aux. A SECURITY DEFINER function owned by a superuser with an unpinned search_path is a
-- classic privilege-escalation surface (a caller-controlled search_path could shadow pg_class /
-- pg_namespace / has_schema_privilege with attacker-defined objects in a schema earlier in the
-- caller's path). Every unqualified reference below resolves only against pg_catalog/pg_ext_aux
-- regardless of the caller's search_path.
CREATE OR REPLACE FUNCTION pg_ext_aux.iceberg_visible_tables(p_role name)
RETURNS TABLE (nspname name, relname name, relid oid, metadata_location text, is_internal boolean)
LANGUAGE sql SECURITY DEFINER STABLE
SET search_path = pg_catalog, pg_ext_aux
AS $$
  SELECT n.nspname, c.relname, c.oid, im.metadata_location, im.is_internal
  FROM pg_lake_table lt
  JOIN pg_class c     ON c.oid = lt.ltrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_ext_aux.pg_iceberg_metadata im ON im.relid = lt.ltrelid
  WHERE lt.lttable_type = 'ICEBERG' AND im.is_internal = true
    AND has_schema_privilege(p_role, n.oid, 'USAGE')
    AND has_table_privilege(p_role, c.oid, 'SELECT')
    -- Review item I-2 (defense-in-depth): has_*_privilege() is unconditionally TRUE for a
    -- superuser/bypassrls role, so without this guard a compromised authenticator credential
    -- combined with a forged/superuser p_role claim would see every table. Reject such a p_role
    -- outright, mirroring decision D2's mint-time superuser rejection at the auth boundary.
    AND NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname = p_role AND (r.rolsuper OR r.rolbypassrls))
$$;

-- Only the gateway's authenticator may call it (it alone is trusted to pass a vetted p_role).
-- A direct-DB attacker is neither the authenticator nor grantee of pg_ext_aux -> fully blocked.
--
-- NOTE (found while wiring this up, not in the original design note): schema-qualified name
-- resolution requires USAGE on the *schema* independently of EXECUTE on the function — Postgres
-- checks USAGE to even look up "pg_ext_aux.iceberg_visible_tables" by name. Without this grant the
-- authenticator gets "permission denied for schema pg_ext_aux" despite holding EXECUTE. This USAGE
-- grant does NOT reopen the leak: it only lets the authenticator resolve names in the schema, and
-- pg_iceberg_metadata's own SELECT stays revoked from everyone except the (superuser) function
-- owner, so the authenticator still cannot read the table directly — only via the definer function.
REVOKE ALL ON FUNCTION pg_ext_aux.iceberg_visible_tables(name) FROM PUBLIC;

-- Review item I-1: a bare GRANT ... TO iceberg_authenticator silently errors (and psql keeps
-- going, since we don't run with ON_ERROR_STOP by default) if the role doesn't exist yet — e.g.
-- this script is re-run/applied before security/create_authenticator.sql, or against the wrong
-- database. That leaves the function ungranted with no loud signal, and the gateway then fails
-- at request time with an opaque "permission denied for function". Fail fast and loud instead.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iceberg_authenticator') THEN
    RAISE EXCEPTION 'role iceberg_authenticator does not exist — run security/create_authenticator.sql first';
  END IF;
  GRANT USAGE ON SCHEMA pg_ext_aux TO iceberg_authenticator;
  GRANT EXECUTE ON FUNCTION pg_ext_aux.iceberg_visible_tables(name) TO iceberg_authenticator;
END $$;
