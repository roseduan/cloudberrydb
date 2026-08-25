#!/bin/bash
# FDW-iceberg vectorized read (FDW-iceberg vectorization).
#
# Pre-cleans both layers of state so each local re-run is hermetic (CI runs on
# a fresh lakehouse, so it needs no pre-clean):
#   - the MinIO warehouse/iceberg_fdw_vectorized/ prefix (DROP TABLE does not
#     purge Iceberg object-store data);
#   - the Polaris-side ivkbt table entry (Polaris keeps the table registered
#     even after the PG database is dropped; re-creating the same name would
#     resurrect old data or fail with "Location does not exist").
#
# Usage:
#   ./run.sh                     # runs pg_regress (make installcheck)
#   CONTAINER=<name> ./run.sh    # custom CBDB container
set -eu

CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CREDS="${POLARIS_CREDS:-root:s3cr3t}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_fdw_vectorized"

# --- Pre-clean MinIO objects under the test's warehouse prefix ---
docker exec minio bash -c '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_fdw_vectorized/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

# --- Pre-clean the Polaris-side table entry ---
docker exec -e POLARIS_CREDS="$POLARIS_CREDS" "$CONTAINER" bash -c "
  TOKEN=\$(curl -s -X POST $POLARIS_URL/api/catalog/v1/oauth/tokens \
      -H 'Polaris-Realm: $POLARIS_REALM' \
      -d 'grant_type=client_credentials' \
      -d \"client_id=\${POLARIS_CREDS%%:*}\" \
      -d \"client_secret=\${POLARIS_CREDS#*:}\" \
      -d 'scope=PRINCIPAL_ROLE:ALL' | sed -n 's/.*\"access_token\":\"\([^\"]*\)\".*/\1/p')
  curl -s -X DELETE \"$POLARIS_URL/api/catalog/v1/polaris_default_catalog/namespaces/public/tables/ivkbt?purgeRequested=false\" \
      -H 'Polaris-Realm: $POLARIS_REALM' -H \"Authorization: Bearer \$TOKEN\" -o /dev/null
" >/dev/null 2>&1 || true

# --- Run the test via pg_regress ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/greenplum_path.sh
  export PGPORT=$PGPORT
  cd $HERE_IN_CONTAINER
  make installcheck
"
