-- Dedicated authenticator for the datalake_rest_catalog gateway (sub-project C, P0-2).
-- NOINHERIT + NOSUPERUSER: the pool connection holds no privileges of its own; it only
-- SET ROLEs / passes p_role to reach an end user's authorized view. NEVER use gpadmin.
-- Apply as a superuser: psql -p 7000 postgres -v pw="<strong-secret>" -f create_authenticator.sql
-- NOTE: pass the raw secret to -v pw=... (no embedded quotes) — :'pw' below already quotes it
-- as a SQL literal; wrapping it yourself (e.g. -v pw="'<secret>'") double-quotes the value and
-- bakes literal quote characters into the stored password.
-- Note: :'pw' is routed through a GUC because psql does not interpolate colon-variables
-- inside a dollar-quoted DO $$ ... $$ body (it would otherwise be sent to the server verbatim).
SELECT set_config('datalake_rest_catalog.tmp_pw', :'pw', false);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='iceberg_authenticator') THEN
    EXECUTE format('CREATE ROLE iceberg_authenticator LOGIN NOINHERIT NOSUPERUSER '
                   'NOCREATEDB NOCREATEROLE PASSWORD %L',
                   current_setting('datalake_rest_catalog.tmp_pw'));
  END IF;
END $$;
RESET datalake_rest_catalog.tmp_pw;

-- The authenticator must be a MEMBER of every role the gateway will SET ROLE to.
GRANT iceberg_reader TO iceberg_authenticator;
-- Add one GRANT line per additional reader role that should be reachable via the gateway.
