#!/bin/bash
# Iceberg AM time-travel field-id regression.
#
# Verifies that iceberg_snapshot_scan() reads a historical snapshot under that
# snapshot's OWN schema (field-id based): a column renamed or type-promoted
# after the snapshot still reads back with its old name / old type / correct
# values.  See sql/iceberg_am_time_travel.sql for the assertions.
#
# CBDB blocks in-place ALTER on Iceberg tables, so the multi-schema fixture is
# built with Spark (the supported schema-evolution path) into the shared MinIO
# warehouse, then read back through a native Iceberg AM table pinned to the
# evolved metadata.  Snapshot ids are Spark-assigned (run-varying), so run.sh
# captures the pre-rename snapshot id and the current metadata path from Spark
# and passes them to the SQL; the SQL emits values compared against expected/.
#
# Requires: the singlecluster lakehouse stack (MinIO bucket "warehouse") and a
# Spark container with the Iceberg runtime.
#
# Usage:
#   ./run.sh                       # defaults below
#   CONTAINER=<name> ./run.sh      # custom CBDB container
#   SPARK_CONTAINER=<name> ./run.sh
set -eu

DB="${DB:-iceberg_am_time_travel_smoke}"
CONTAINER="${CONTAINER:-lightning-run}"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark-master}"
LAKEHOUSE="${LAKEHOUSE:-minio}"
PGPORT="${PGPORT:-7000}"
# Spark writes through Hadoop's S3A connector, whose scheme is s3a by
# requirement -- WRITER side only.  The CBDB catalog in the SQL file uses the
# standard s3 scheme (our recommended form); the agent resolves each URI by
# its own scheme, so the mixed fixture reads fine.
WH="s3a://warehouse/iceberg_am_time_travel"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/iceberg_am_time_travel"
HERE="$(cd "$(dirname "$0")" && pwd)"

# This test orchestrates Docker from the HOST (a Spark container to build the
# multi-schema fixture + the CBDB container to read it).  The generic smoke
# runner auto-discovers this directory and may invoke it inside the CBDB
# container (where there is no docker) or in an environment whose container
# names differ.  Skip cleanly in that case instead of failing the suite;
# override CONTAINER / SPARK_CONTAINER to run it where those containers live.
if ! command -v docker >/dev/null 2>&1 \
   || ! docker inspect "$CONTAINER" >/dev/null 2>&1 \
   || ! docker inspect "$SPARK_CONTAINER" >/dev/null 2>&1; then
    echo "SKIP: iceberg_am_time_travel (needs host docker + containers '$CONTAINER' and '$SPARK_CONTAINER'; set CONTAINER/SPARK_CONTAINER to run)"
    exit 0
fi

# --- Pre-clean MinIO objects under the test's warehouse prefix (best-effort;
# Spark DROP+CREATE and the unique table name provide the real idempotency) ---
docker exec "$LAKEHOUSE" bash -lc '
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/iceberg_am_time_travel/ >/dev/null 2>&1
' >/dev/null 2>&1 || true

# --- Build the multi-schema fixture with Spark (hadoop catalog on the same warehouse) ---
docker exec "$SPARK_CONTAINER" bash -lc "cat > /tmp/tt_fixture.sql <<'SPARKSQL'
DROP TABLE IF EXISTS hc.public.tt_src;
CREATE TABLE hc.public.tt_src (id int, note string, amt int) USING iceberg;
INSERT INTO hc.public.tt_src VALUES (1,'note1',100),(2,'note2',200),(3,'note3',300);
ALTER TABLE hc.public.tt_src RENAME COLUMN note TO memo;
ALTER TABLE hc.public.tt_src ALTER COLUMN amt TYPE bigint;
INSERT INTO hc.public.tt_src VALUES (4,'memo4',400),(5,'memo5',500);
SELECT concat('SNAP_A=', snapshot_id) FROM hc.public.tt_src.snapshots ORDER BY committed_at LIMIT 1;
SELECT concat('HEAD_META=', file) FROM hc.public.tt_src.metadata_log_entries ORDER BY timestamp DESC LIMIT 1;
SPARKSQL"

SPARK_OUT=$(docker exec "$SPARK_CONTAINER" bash -lc "
/opt/spark/bin/spark-sql \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.catalog.hc=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hc.type=hadoop \
  --conf spark.sql.catalog.hc.warehouse=$WH \
  -f /tmp/tt_fixture.sql 2>&1")

SNAP_A=$(printf '%s\n' "$SPARK_OUT" | sed -n 's/^SNAP_A=//p' | tail -1)
HEAD_META=$(printf '%s\n' "$SPARK_OUT" | sed -n 's/^HEAD_META=//p' | tail -1)

if [ -z "${SNAP_A:-}" ] || [ -z "${HEAD_META:-}" ]; then
    echo "FIXTURE SETUP FAILED: could not capture snapshot id / metadata path from Spark" >&2
    printf '%s\n' "$SPARK_OUT" | tail -40 >&2
    exit 1
fi
echo "fixture ready: SNAP_A=$SNAP_A HEAD_META=$HEAD_META"

# --- Run the test SQL against a fresh database ---
mkdir -p "$HERE/results"
docker exec -u gpadmin "$CONTAINER" bash -lc "
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 -c 'DROP DATABASE IF EXISTS $DB' -c 'CREATE DATABASE $DB'
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -v snap_a=$SNAP_A -v head_meta='$HEAD_META' -f sql/iceberg_am_time_travel.sql
" > "$HERE/results/iceberg_am_time_travel.out" 2>&1

# --- Check the assertions (robust to surrounding psql/setup output) ---
RES="$HERE/results/iceberg_am_time_travel.out"
npass=$(grep -c 'ASSERT .*: PASS' "$RES" || true)
if grep -q 'ASSERT .*: FAIL' "$RES" || [ "$npass" -ne 5 ]; then
    echo "FAIL: iceberg_am_time_travel"
    cat "$RES"
    exit 1
fi
echo "PASS: iceberg_am_time_travel"
