#!/bin/bash
# Iceberg AM ANALYZE sampling regression (issue #352).
#
# Pre-cleans the warehouse/iceberg_am_analyze/ prefix so each run is hermetic
# (DROP TABLE does not purge Iceberg object-store data), then runs the test.
#
# Usage:
#   ./run.sh                     # default database iceberg_am_analyze_smoke
#   DB=mydb ./run.sh             # custom database name
#   CONTAINER=<name> ./run.sh    # custom CBDB container
set -eu

DB="${DB:-iceberg_am_analyze_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_analyze"

# --- Pre-clean MinIO objects under the test's warehouse prefix ---
# Prefer mc against the lakehouse stack (CI); fall back to removing the
# fs-backend objects directly inside the CBDB container's local MinIO (dev).
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_am_analyze/ >/dev/null 2>&1
' >/dev/null 2>&1 \
  || docker exec "$CONTAINER" bash -lc 'rm -rf /data/minio/warehouse/iceberg_am_analyze' >/dev/null 2>&1 \
  || true

# --- Run the test SQL ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_analyze.sql 2>&1 | tail -80
"
