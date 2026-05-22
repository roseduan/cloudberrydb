#!/bin/bash
# Run the Polaris-catalog basic functional smoke for Iceberg AM inside the
# CBDB development container. Requires the singlecluster stack (Polaris on
# polaris:8181 + MinIO on minio:9000) running and reachable from the
# container's network.
#
# Before running this script the test cleans any stale "polaris_smoke"
# table registration in Polaris and any leftover MinIO objects so the run
# is hermetic.
#
# Usage:
#   ./run.sh                      # default database "iceberg_am_polaris_smoke"
#   DB=mydb ./run.sh              # custom database name
#   CONTAINER=<name> ./run.sh     # custom CBDB container
set -eu

DB="${DB:-iceberg_am_polaris_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
POLARIS="${POLARIS:-singlecluster-polaris-1}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_polaris"

# --- 1. Pre-clean Polaris table registrations (best effort, ignore 404) ---
docker exec "$POLARIS" bash -lc '
    TOKEN=$(curl -s -X POST http://127.0.0.1:8181/api/catalog/v1/oauth/tokens \
      -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
      | sed -n "s/.*\"access_token\":\"\\([^\"]*\\)\".*/\\1/p")
    for t in polaris_smoke polaris_acid_raw polaris_acid_pl polaris_acid_sql; do
        curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
          "http://127.0.0.1:8181/api/catalog/v1/polaris_default_catalog/namespaces/public/tables/$t" \
          -o /dev/null
    done
' >/dev/null 2>&1 || true

# --- 2. Pre-clean MinIO objects under each test's table prefix ---
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/public/polaris_smoke/ >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/public/polaris_acid_raw/ >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/public/polaris_acid_pl/  >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/public/polaris_acid_sql/ >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/polaris_acid_double_update/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

# --- 3. Run the test SQL ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  # Hermetic-env guard: the polaris smoke must pass on a clean environment.
  # AWS_* env vars masking a regression in the Polaris -> Gopher routing is
  # exactly what GitLab #318 was about.
  if env | grep -q '^AWS_'; then
      echo 'FAIL: AWS_* env vars must be unset for hermetic test' >&2
      env | grep '^AWS_' >&2
      exit 1
  fi
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_polaris_basic.sql 2>&1 | tail -120
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_polaris_acid_double_update.sql 2>&1 | tail -120
"
