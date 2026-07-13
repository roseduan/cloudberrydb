# datalake_rest_catalog

Read-only Apache Iceberg REST Catalog gateway over PostgreSQL-native (builtin) Iceberg tables
(HashData issue #382). Migrated from the standalone prototype `postgres-iceberg-restful/gateway`.

## Build (REQUIRES JDK 17 — sibling datalake_agent is Java 8; this module is Java 17)
    export JAVA_HOME=/opt/jdk17 && export PATH=$JAVA_HOME/bin:$PATH
    mvn -DskipTests package          # -> target/datalake-rest-catalog-1.0.0.jar (runnable uber-jar)
    make install                     # -> $(pkglibdir)/java/datalake-rest-catalog-1.0.0.jar

## Run (standalone; inject S3 creds via env)
    export S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
    java -jar target/datalake-rest-catalog-1.0.0.jar   # listens on :8181

Config: `src/main/resources/gateway.properties` (env-overridable). See design spec in
`postgres-iceberg-restful/docs/superpowers/specs/2026-07-05-migrate-into-datalake-contrib-design.md`.
Deployment to coordinator = sub-project B; production security hardening = sub-project C.
