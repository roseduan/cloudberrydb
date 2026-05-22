#!/bin/bash
# Run the hadoop-catalog over s3 volume basic smoke for Iceberg AM inside
# the CBDB development container.
set -eu

DB="${DB:-iceberg_am_hadoop_s3_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_hadoop_s3"

docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_hadoop_s3_smoke/ >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_hadoop_s3_acid_double_update/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_hadoop_s3_basic.sql 2>&1 | tail -100
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_hadoop_s3_acid_double_update.sql 2>&1 | tail -100
"
