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

Both accessors filter to **builtin-catalog** tables. An iceberg table registered against an
external Polaris/Hive catalog is not served here, and is not listed either — the two accessors
share one predicate so `listTables` can never advertise a table whose `loadTable` would fail.

This script depends on `pg_catalog.pg_iceberg_load_metadata_json()`, which ships in
`datalake_fdw`. Note that `datalake_fdw` has no extension upgrade script, so after a binary
upgrade an already-installed extension must be re-created before this script will apply — it
fails at `CREATE FUNCTION` time with `function ... does not exist` if you skip that, rather than
at runtime.

## Run (standalone)
    java -jar target/datalake-rest-catalog-1.0.0.jar   # HTTPS :8443 by default, HTTP :8181 if disabled

**The gateway needs no object storage credentials.** metadata.json is read through the database
and data files are read by the client with its own credentials, so there is no `S3_ENDPOINT` /
`S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` to set — those config keys no longer exist. What it
does need is the authenticator's database password (`PG_AUTHENTICATOR_PASSWORD`).

Documents are cached in-process by metadata location (`catalog.metadataCache.*` in
gateway.properties). Safe by construction: an Iceberg commit writes a new metadata file rather
than rewriting one, so a location maps to one immutable document and a commit invalidates the
cache by simply not matching it. The per-role visibility lookup still runs on every request, so
nothing about authorization is cached.

## Multi-database mode (issue #935)

By default one gateway serves the single database in `pg.jdbcUrl`. Setting
`pg.jdbcUrlTemplate` (env `PG_JDBCURLTEMPLATE`, e.g.
`jdbc:postgresql://<coordinator>:5432/{db}?ssl=false`) lets one gateway serve every database:
the client's Iceberg REST `warehouse` names the database, the gateway hands it back as the
spec's `prefix`, and every later call is routed `/v1/{db}/...`. Clients without a `warehouse`
keep getting the default database (`pg.jdbcUrl`), exactly as in single-database mode.

    Spark      spark.sql.catalog.<c>.warehouse=<db>
    Trino      iceberg.rest-catalog.warehouse=<db>
    PyIceberg  load_catalog(..., warehouse="<db>")
    DuckDB     ATTACH '<db>' AS <c> (TYPE ICEBERG, ENDPOINT ...)

Setup — order matters, each step depends on the previous one:

    # 1. roles (cluster-wide, once): create_authenticator.sql grants iceberg_reader, so it must exist
    psql postgres -c "CREATE ROLE iceberg_reader LOGIN PASSWORD '<pw>'"
    psql postgres -v pw="<authenticator-secret>" -f security/create_authenticator.sql
    # 2. template1 (once): databases created from it afterwards inherit extension, functions, grants
    psql template1 -c "CREATE EXTENSION IF NOT EXISTS datalake_fdw"
    psql template1 -v ON_ERROR_STOP=1 -f security/rest_catalog_authz.sql
    # 3. every database that already exists: same two commands as step 2
    # 4. databases whose CONNECT was revoked from PUBLIC (database ACLs are not inherited)
    psql postgres -c "GRANT CONNECT ON DATABASE <db> TO iceberg_authenticator"

What the gateway enforces, and why:

- **End user's CONNECT.** The session that connects is the authenticator's, so PostgreSQL never
  checks the end user's CONNECT; the gateway does (`403`). A database that does not exist also
  answers `403`, so the API cannot be used to probe database names. `/v1/config` echoes any
  `warehouse` back unvalidated for the same reason — it is unauthenticated.
- **Never a template database** (`404`). A live connection to template1 makes every
  `CREATE DATABASE` in the cluster fail with `source database "template1" is being accessed by
  other users`.
- **Per-database pools drain to zero when idle** (`pg.pool.idleTimeoutSeconds`). An idle pooled
  connection blocks `DROP DATABASE` / `ALTER DATABASE ... RENAME` on that database. Hikari
  evicts on a 30 s housekeeping cycle, so allow up to idle timeout + 30 s, or use
  `DROP DATABASE ... WITH (FORCE)`.
- A database without the authz layer, or one the authenticator cannot connect to, answers `400`
  naming the fix, rather than an empty catalog.

**Turning multi-database mode on changes what an existing `warehouse` means.** Single-database
mode ignores it; multi-database mode treats it as a database name. A client already configured
with an arbitrary warehouse gets `403` after the switch — DuckDB always sends one, since
`ATTACH` requires it (the four-engine smoke defaults to `ATTACH 'warehouse'`) — so update
clients to the real database name, or leave `warehouse` unset to keep using the default database.

A database named exactly like a spec route segment (`namespaces`, `tables`, `views`,
`transactions`, `config`, `oauth`, `oauth2`) cannot be served: `/v1/config` refuses it with
`400`, because `/v1/namespaces/...` could not be told apart from the route itself.

Per-database pools are never evicted. A dropped or renamed database keeps its slot under
`pg.pool.maxDatabases` (default 32), so a gateway that has served that many distinct names
answers `503` for any new one until it is restarted. Every prefixed request also checks the
end user's CONNECT through the default database's pool, so size `pg.pool.maxSize` for the
total request rate, not only for token issuance.
**End-user passwords must be checked on the gateway's path.** A token is issued after logging
the end user in to the default database from the gateway's own address. A gateway beside the
coordinator comes in over loopback, which cloud clusters initialise as
`host all all 127.0.0.1/24 trust`, and `security/pg_hba.rest_catalog.fragment` hardens only the
authenticator: on such a path any password logs any user in. The gateway therefore also tries a
wrong password after every successful login and refuses the token if that gets in too (logged as
`Refusing a token for role ...`); a canary that fails for any reason other than an
authentication failure also refuses it. To let end users in, put a password rule for the
gateway's source address above the trust rule, next to the fragment:

    host    all    all        127.0.0.1/32    scram-sha-256

This covers IPv4 only: write `127.0.0.1`, not `localhost`, in `pg.jdbcUrl` (see the IPv6 note
in the deployment guide), or add the same rule for `::1/128`. It also ends passwordless TCP
loopback for every role, gpadmin included; such tooling should use the Unix socket (`local`
rules are untouched) rather than get a trust line on the gateway's source, which the fragment
forbids for gpadmin.

Each successful token request logs in twice and leaves one `password authentication failed`
line in the server log. On a role whose profile sets a low `FAILED_LOGIN_ATTEMPTS`, those
canary failures count towards it; we could not make sequential or 8-way concurrent requests
lock such a role on cloud, but keep gateway users on profiles without a tight limit.

Limits worth knowing: token issuance still checks the end user's password against the default
database, so every gateway user needs CONNECT there; per-database `pg_hba.conf` rules for end
users do not apply through the gateway (`GRANT CONNECT` is the database-level control); and
template1 is copied only at `CREATE DATABASE`, so a later change to `rest_catalog_authz.sql` or
a `datalake_fdw` upgrade has to be re-applied to template1 and to every existing database.

Config: `src/main/resources/gateway.properties` (env-overridable). See design spec in
`postgres-iceberg-restful/docs/superpowers/specs/2026-07-05-migrate-into-datalake-contrib-design.md`.
Deployment to coordinator = sub-project B; production security hardening = sub-project C.
