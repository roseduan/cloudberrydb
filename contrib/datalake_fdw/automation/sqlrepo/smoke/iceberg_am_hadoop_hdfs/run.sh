#!/bin/bash
# Run the hadoop-catalog over HDFS volume basic smoke for Iceberg AM.
# Pre-cleans the HDFS warehouse path and ensures it exists with writable
# permissions for gpadmin.
set -eu

DB="${DB:-iceberg_am_hadoop_hdfs_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
LAKEHOUSE="${LAKEHOUSE:-lakehouse}"
PGPORT="${PGPORT:-7000}"
HDFS_WAREHOUSE="${HDFS_WAREHOUSE:-/iceberg_hadoop_hdfs_smoke}"
HDFS_WAREHOUSE_ACID="${HDFS_WAREHOUSE_ACID:-/iceberg_hadoop_hdfs_acid_double_update}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_hadoop_hdfs"

# --- Pre-clean each test's HDFS warehouse path and ensure it's writable ---
docker exec "$LAKEHOUSE" bash -lc "
    for d in $HDFS_WAREHOUSE $HDFS_WAREHOUSE_ACID; do
        /opt/hadoop/bin/hdfs dfs -fs hdfs://hadoop:8020 -rm -r -f \$d >/dev/null 2>&1 || true
        /opt/hadoop/bin/hdfs dfs -fs hdfs://hadoop:8020 -mkdir -p \$d >/dev/null 2>&1
        /opt/hadoop/bin/hdfs dfs -fs hdfs://hadoop:8020 -chmod -R 777 \$d >/dev/null 2>&1
    done
" >/dev/null 2>&1 || true

docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_hadoop_hdfs_basic.sql 2>&1 | tail -100
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_hadoop_hdfs_acid_double_update.sql 2>&1 | tail -100
"
