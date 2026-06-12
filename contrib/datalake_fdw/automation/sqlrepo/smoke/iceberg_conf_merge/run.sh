#!/bin/bash
# Run the conf-file merge smoke inside the CBDB development container.
# Requires the singlecluster lakehouse stack (MinIO @ minio:9000) up.
#
# Deploys the merge_* sections into the QD data directory's s3.conf (the
# datalake_agent working directory), backing up and restoring any existing
# file, then drives the SQL cases.
#
# Usage:
#   ./run.sh                     # default database iceberg_conf_merge_smoke
#   DB=mydb ./run.sh             # custom database name
#   CONTAINER=<name> ./run.sh    # custom CBDB container
set -eu

DB="${DB:-iceberg_conf_merge_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-minio}"
PGPORT="${PGPORT:-7000}"
QD_DATADIR="${QD_DATADIR:-/workspace/deploy/database/datadirs/qddir/demoDataDir-1}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_conf_merge"

# --- Pre-clean MinIO objects under the test prefix ---
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_conf_merge/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

# --- Deploy the merge_* sections into the agent-visible s3.conf ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  cd $QD_DATADIR
  if [ -f s3.conf ] && [ ! -f s3.conf.confmerge.bak ]; then
      cp s3.conf s3.conf.confmerge.bak
  fi
  cat > s3.conf <<'YAML'
merge_minio:
    type: s3
    endpoint: http://minio:9000
    region: us-east-1
    path_style_access: true
    access_key_id: admin
    secret_access_key: admin12345

merge_badcreds:
    type: s3
    endpoint: http://minio:9000
    region: us-east-1
    path_style_access: true
    access_key_id: admin
    secret_access_key: wrong-secret

merge_legacy:
    fs.s3a.endpoint: http://minio:9000
    fs.s3a.access.key: admin
    fs.s3a.secret.key: admin12345
    fs.s3a.path.style.access: true
YAML
"

restore_conf() {
  docker exec -u gpadmin "$CONTAINER" bash -c "
    cd $QD_DATADIR
    if [ -f s3.conf.confmerge.bak ]; then
        mv s3.conf.confmerge.bak s3.conf
    else
        rm -f s3.conf
    fi
  " || true
}
trap restore_conf EXIT

# --- Run the test SQL ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_conf_merge.sql 2>&1 | tail -120
"
