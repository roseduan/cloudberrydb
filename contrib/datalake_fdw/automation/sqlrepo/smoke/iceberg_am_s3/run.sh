#!/bin/bash
# Run the s3-catalog basic smoke for Iceberg AM inside the CBDB development
# container. Requires the singlecluster lakehouse stack (MinIO @ 9100) up.
#
# Pre-cleans MinIO objects under the test prefix so each run is hermetic.
#
# Usage:
#   ./run.sh                     # default database iceberg_am_s3_smoke
#   DB=mydb ./run.sh             # custom database name
#   CONTAINER=<name> ./run.sh    # custom CBDB container
set -eu

DB="${DB:-iceberg_am_s3_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_s3"

# --- Pre-clean MinIO objects under the warehouse prefix ---
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_s3_smoke/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

# --- Run the test SQL ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_s3_basic.sql 2>&1 | tail -100
"
