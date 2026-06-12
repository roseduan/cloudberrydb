#!/bin/bash
# Iceberg FDW foreign table over a Polaris catalog (issue #844).
#
# Pre-cleans both layers of state so each run is hermetic:
#   - the MinIO warehouse/iceberg_fdw_polaris/ prefix (DROP TABLE does not
#     purge Iceberg object-store data);
#   - the Polaris-side table entry (the Polaris server keeps the table
#     registered even after the PG database is dropped; re-creating the same
#     name would resurrect old data or fail with "Location does not exist").
#
# Usage:
#   ./run.sh                     # default database iceberg_fdw_polaris_smoke
#   DB=mydb ./run.sh             # custom database name
#   CONTAINER=<name> ./run.sh    # custom CBDB container
set -eu

DB="${DB:-iceberg_fdw_polaris_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CREDS="${POLARIS_CREDS:-root:s3cr3t}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_fdw_polaris"

# --- Pre-clean MinIO objects under the test's warehouse prefix ---
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_fdw_polaris/ >/dev/null 2>&1
' >/dev/null 2>&1 \
  || docker exec minio bash -c '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_fdw_polaris/ >/dev/null 2>&1
' >/dev/null 2>&1 \
  || true

# --- Pre-clean the Polaris-side table entry ---
docker exec -e POLARIS_CREDS="$POLARIS_CREDS" "$CONTAINER" bash -c "
  TOKEN=\$(curl -s -X POST $POLARIS_URL/api/catalog/v1/oauth/tokens \
      -H 'Polaris-Realm: $POLARIS_REALM' \
      -d 'grant_type=client_credentials' \
      -d \"client_id=\${POLARIS_CREDS%%:*}\" \
      -d \"client_secret=\${POLARIS_CREDS#*:}\" \
      -d 'scope=PRINCIPAL_ROLE:ALL' | sed -n 's/.*\"access_token\":\"\([^\"]*\)\".*/\1/p')
  curl -s -X DELETE \"$POLARIS_URL/api/catalog/v1/polaris_default_catalog/namespaces/public/tables/fdwpol_t?purgeRequested=false\" \
      -H 'Polaris-Realm: $POLARIS_REALM' -H \"Authorization: Bearer \$TOKEN\" -o /dev/null
" >/dev/null 2>&1 || true

# --- Run the test SQL ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_fdw_polaris.sql 2>&1 | tail -60
"
