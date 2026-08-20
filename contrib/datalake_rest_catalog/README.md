# datalake_rest_catalog

Read-only Apache Iceberg REST Catalog gateway over PostgreSQL-native (builtin) Iceberg tables
(HashData issue #382). Migrated from the standalone prototype `postgres-iceberg-restful/gateway`.

## Build (REQUIRES JDK >= 11 — sibling datalake_agent is Java 8; this module targets release 11)
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk && export PATH=$JAVA_HOME/bin:$PATH
    mvn -DskipTests package          # -> target/datalake-rest-catalog-1.0.0.jar (runnable uber-jar)
    make install                     # -> $(pkglibdir)/java/datalake-rest-catalog-1.0.0.jar

## Prerequisite: the authorization layer must be applied to the target database
    psql -v ON_ERROR_STOP=1 -f security/rest_catalog_authz.sql postgres

Creates the SECURITY DEFINER accessors the gateway reads through, including
`pg_ext_aux.iceberg_load_metadata(p_role, p_nspname, p_relname)`, which returns the table's
metadata.json document gated by the same predicate as `iceberg_visible_tables`. Without it the
gateway can authenticate but every `loadTable` returns 404.

## Run (standalone)
    java -jar target/datalake-rest-catalog-1.0.0.jar   # HTTPS :8443 by default, HTTP :8181 if disabled

**The gateway needs no object storage credentials.** metadata.json is read through the database
and data files are read by the client with its own credentials, so there is no `S3_ENDPOINT` /
`S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` to set — those config keys no longer exist. What it
does need is the authenticator's database password (`PG_AUTHENTICATOR_PASSWORD`).

Config: `src/main/resources/gateway.properties` (env-overridable). See design spec in
`postgres-iceberg-restful/docs/superpowers/specs/2026-07-05-migrate-into-datalake-contrib-design.md`.
Deployment to coordinator = sub-project B; production security hardening = sub-project C.
