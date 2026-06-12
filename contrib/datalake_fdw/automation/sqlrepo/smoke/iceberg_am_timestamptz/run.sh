#!/bin/bash
# Iceberg AM timestamptz interop smoke (issue #366).
#
# Runs the SQL round trip, then asserts the Iceberg table schema declares the
# column as "timestamptz" (the bug declared it "timestamp", making external
# engines read UTC microseconds as a zone-less wall clock).  If a Spark
# container is available, also cross-checks the value Spark reads.
#
# Usage:
#   ./run.sh                          # default database iceberg_am_tz_smoke
#   DB=mydb ./run.sh                  # custom database name
#   SMOKE_TZ_SKIP_SPARK=1 ./run.sh    # skip the Spark cross-check
set -eu

DB="${DB:-iceberg_am_tz_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark-master}"
PGPORT="${PGPORT:-7000}"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_timestamptz"

# --- Pre-clean the MinIO prefix (DROP TABLE does not purge object storage) ---
docker exec lakehouse bash -lc '
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_am_timestamptz/ >/dev/null 2>&1
' >/dev/null 2>&1 \
  || docker exec minio bash -c '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_am_timestamptz/ >/dev/null 2>&1
' >/dev/null 2>&1 \
  || true

# --- Run the SQL round trip ---
docker exec -u gpadmin "$CONTAINER" bash -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/iceberg_am_timestamptz.sql 2>&1 | tail -50
"

# --- Assert the Iceberg schema type of column t is timestamptz ---
# (the minio image has no grep; resolve the current metadata via version-hint)
TYPE_LINE=$(docker exec minio bash -c '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    BASE=local/warehouse/iceberg_am_timestamptz/demo/tz_repro/metadata
    V=$(mc cat "$BASE/version-hint.text")
    mc cat "$BASE/v$V.metadata.json"
' | python3 -c '
import sys, json
d = json.load(sys.stdin)
schemas = d.get("schemas") or [d.get("schema", {})]
for s in schemas:
    for f in s.get("fields", []):
        if f["name"] == "t":
            print(f["type"])
' | tail -1)
if [ "$TYPE_LINE" = "timestamptz" ]; then
    echo "PASS: iceberg schema declares t as timestamptz"
else
    echo "FAIL: iceberg schema declares t as '$TYPE_LINE' (expected timestamptz)"
    exit 1
fi

# --- Optional Spark cross-check ---
if [ "${SMOKE_TZ_SKIP_SPARK:-0}" != "1" ] && docker ps --format '{{.Names}}' | grep -qx "$SPARK_CONTAINER"; then
    SPARK_OUT=$(docker exec "$SPARK_CONTAINER" bash -c '/opt/spark/bin/spark-sql --master "local[1]" \
      --conf spark.eventLog.enabled=false \
      --conf spark.sql.catalog.hd=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.hd.type=hadoop \
      --conf spark.sql.catalog.hd.warehouse=s3a://warehouse/iceberg_am_timestamptz \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=admin \
      --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3.access.key=admin \
      --conf spark.hadoop.fs.s3.secret.key=admin12345 \
      --conf spark.hadoop.fs.s3.path.style.access=true \
      --conf spark.sql.session.timeZone=Asia/Shanghai \
      -e "SELECT id, t FROM hd.demo.tz_repro ORDER BY id;" 2>/dev/null')
    # after the SQL DML: id=2 -> 2024-06-15 21:00:00 +08, id=3 -> 2024-03-10 21:30:00 +08
    if echo "$SPARK_OUT" | grep -q "2024-06-15 21:00:00" \
       && echo "$SPARK_OUT" | grep -q "2024-03-10 21:30:00"; then
        echo "PASS: Spark reads the same instants in Asia/Shanghai"
    else
        echo "FAIL: Spark output mismatch:"
        echo "$SPARK_OUT"
        exit 1
    fi
else
    echo "SKIP: Spark cross-check (container '$SPARK_CONTAINER' not running or skipped)"
fi
