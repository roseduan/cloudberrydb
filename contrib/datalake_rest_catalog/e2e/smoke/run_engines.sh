#!/usr/bin/env bash
# Four-engine read smoke for the datalake_rest_catalog gateway (issue #382).
#
# Drives four unmodified, standard Iceberg REST clients -- PyIceberg, Spark, Trino, DuckDB --
# against a running gateway (default HTTPS :8443) as the SELECT-only 'iceberg_reader' role, and
# asserts each reads the builtin table sales.orders back as exactly the 3 seeded rows
# [(1,ada),(2,grace),(3,linus)].  Prints a per-engine PASS/FAIL table and exits non-zero if any
# requested engine fails.
#
# Multi-engine counterpart of e2e/reader_real_https.py (PyIceberg only). The CI smoke
# (test_datalake_rest_catalog_smoke) runs this inside the consolidated engine-tester image
# (ubuntu; all four clients + JDK) via `docker run --network host`; it also runs standalone on
# any host that already has the engine runtimes (e.g. the dev container). Runtimes are picked up
# from the paths under "Runtime locations" below (see README.md). It writes no fixtures; seed the
# cluster (seed_builtin.sql) first.
#
# Usage:
#   run_engines.sh [engine ...]      # default: pyiceberg spark trino duckdb
# Common overrides (env):
#   DRC_URI            gateway origin, no /v1 suffix     (default https://localhost:8443)
#   DRC_READER_CRED    OAuth2 client-credentials         (default iceberg_reader:reader_pw)
#   DRC_S3_ENDPOINT/DRC_S3_KEY/DRC_S3_SECRET             (default http://localhost:9000 / minioadmin)
#   DRC_NS/DRC_TABLE   table to read                     (default sales/orders)
#   SPARK_HOME, TRINO_HOME, TRINO_CLI, DUCKDB_IMAGE      (see "Runtime locations")
#   DRC_JAVA_HOME (JDK17 for Trino), SPARK_JAVA_HOME (JDK8/11/17 for Spark)
set -uo pipefail

# --- config ---------------------------------------------------------------------------------
DRC_URI="${DRC_URI:-https://localhost:8443}"
DRC_READER_CRED="${DRC_READER_CRED:-iceberg_reader:reader_pw}"
DRC_S3_ENDPOINT="${DRC_S3_ENDPOINT:-http://localhost:9000}"
DRC_S3_KEY="${DRC_S3_KEY:-minioadmin}"
DRC_S3_SECRET="${DRC_S3_SECRET:-minioadmin}"
DRC_NS="${DRC_NS:-sales}"
DRC_TABLE="${DRC_TABLE:-orders}"
EXPECT_ROWS=3                       # sales.orders: ids 1,2,3 / names ada,grace,linus

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${DRC_SMOKE_WORK:-/tmp/drc-smoke-engines}"
mkdir -p "$WORK"

# Runtime locations (override in CI to match the image layout).
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
TRINO_HOME="${TRINO_HOME:-/opt/trino-server-435}"
TRINO_CLI="${TRINO_CLI:-/opt/trino-cli.jar}"
DRC_JAVA_HOME="${DRC_JAVA_HOME:-/opt/jdk17}"       # Trino 435 needs JDK17
SPARK_JAVA_HOME="${SPARK_JAVA_HOME:-$DRC_JAVA_HOME}" # Spark 3.3.4 runs on JDK 8/11/17
DUCKDB_IMAGE="${DUCKDB_IMAGE:-}"                    # ubuntu-based image w/ duckdb+extensions; empty => skip w/ error

ENGINES=("$@"); [ ${#ENGINES[@]} -eq 0 ] && ENGINES=(pyiceberg spark trino duckdb)

# --- shared helpers -------------------------------------------------------------------------
host_port="${DRC_URI#*://}"; DRC_HOST="${host_port%%:*}"; DRC_PORT="${host_port##*:}"
PEM="$WORK/drc.pem"; JKS="$WORK/drc-truststore.jks"

export_cert() {   # export the gateway's live (self-signed dev) cert to PEM
  openssl s_client -connect "$DRC_HOST:$DRC_PORT" -servername "$DRC_HOST" </dev/null 2>/dev/null \
    | openssl x509 -out "$PEM"
  [ -s "$PEM" ] || { echo "ERROR: could not export TLS cert from $DRC_URI (gateway up?)"; return 1; }
}
build_truststore() {   # JKS for the JVM engines (Spark/Trino)
  rm -f "$JKS"
  "$DRC_JAVA_HOME/bin/keytool" -importcert -noprompt -alias drc -file "$PEM" \
    -keystore "$JKS" -storepass changeit >/dev/null 2>&1
}

declare -A RESULT
record() { RESULT["$1"]="$2"; }   # engine, PASS|FAIL:<reason>

# --- PyIceberg (latest, HTTPS) --------------------------------------------------------------
smoke_pyiceberg() {
  # DRC_PYI_VENV lets an image pre-bake the venv (offline-friendly); pip install below is then
  # a no-op that just honors the requirements pin.
  local venv="${DRC_PYI_VENV:-$WORK/pyi-venv}" py=""
  for c in "${DRC_PYTHON:-}" /usr/local/python-3.10.16/bin/python3.10 python3.12 python3.11 python3.10 python3.9; do
    [ -n "$c" ] && command -v "$c" >/dev/null 2>&1 && py="$c" && break
  done
  [ -n "$py" ] || { record pyiceberg "FAIL:no python>=3.9"; return 1; }
  [ -x "$venv/bin/python" ] || "$py" -m venv "$venv"
  "$venv/bin/pip" install -q -r "$SCRIPT_DIR/../requirements-pyiceberg-latest.txt" ${DRC_PIP_PROXY:+--proxy "$DRC_PIP_PROXY"} || { record pyiceberg "FAIL:pip"; return 1; }
  cat > "$WORK/pyi_read.py" <<PY
import os, sys
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog("hd", uri="$DRC_URI", credential="$DRC_READER_CRED", **{
    "ssl": {"cabundle": "$PEM"},
    "s3.endpoint": "$DRC_S3_ENDPOINT", "s3.access-key-id": "$DRC_S3_KEY",
    "s3.secret-access-key": "$DRC_S3_SECRET", "s3.path-style-access": "true", "s3.region": "us-east-1"})
t = cat.load_table("$DRC_NS.$DRC_TABLE"); r = t.scan().to_arrow()
ids = sorted(r.column("id").to_pylist())
print("pyiceberg rows=%d ids=%s" % (r.num_rows, ids))
sys.exit(0 if (r.num_rows == $EXPECT_ROWS and ids == [1,2,3]) else 1)
PY
  # centos7: pyiceberg 0.11 pyarrow needs a newer libstdc++ (GLIBCXX_3.4.29) if present
  local ld="${DRC_LIBSTDCXX_DIR:-/usr/local/toolchain/lib64}"
  if LD_LIBRARY_PATH="$ld:${LD_LIBRARY_PATH:-}" "$venv/bin/python" "$WORK/pyi_read.py"; then record pyiceberg PASS; else record pyiceberg "FAIL:read"; fi
}

# --- Spark 3.3.4 (SparkCatalog type=rest, S3FileIO) -----------------------------------------
smoke_spark() {
  [ -x "$SPARK_HOME/bin/spark-sql" ] || { record spark "SKIP:no SPARK_HOME ($SPARK_HOME) -- add Spark to the image"; return 0; }
  [ -s "$JKS" ] || { record spark "FAIL:no JKS truststore (keytool/DRC_JAVA_HOME issue)"; return 1; }
  local ts="-Djavax.net.ssl.trustStore=$JKS -Djavax.net.ssl.trustStorePassword=changeit"
  local out="$WORK/spark.out"
  JAVA_HOME="$SPARK_JAVA_HOME" PATH="$SPARK_JAVA_HOME/bin:$PATH" SPARK_LOCAL_IP=127.0.0.1 \
  "$SPARK_HOME/bin/spark-sql" --conf spark.ui.enabled=false \
    --conf spark.sql.catalog.hd=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.hd.type=rest \
    --conf spark.sql.catalog.hd.uri="$DRC_URI" \
    --conf spark.sql.catalog.hd.credential="$DRC_READER_CRED" \
    --conf spark.sql.catalog.hd.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.hd.s3.endpoint="$DRC_S3_ENDPOINT" \
    --conf spark.sql.catalog.hd.s3.access-key-id="$DRC_S3_KEY" \
    --conf spark.sql.catalog.hd.s3.secret-access-key="$DRC_S3_SECRET" \
    --conf spark.sql.catalog.hd.s3.path-style-access=true \
    --conf spark.sql.catalog.hd.client.region=us-east-1 \
    --conf spark.sql.catalog.hd.rest-metrics-reporting-enabled=false \
    --conf "spark.driver.extraJavaOptions=$ts" --conf "spark.executor.extraJavaOptions=$ts" \
    -e "SELECT id,name FROM hd.$DRC_NS.$DRC_TABLE ORDER BY id;" > "$out" 2>"$WORK/spark.err"
  local n; n=$(grep -cE '^[0-9]+\s' "$out")
  if [ "$n" = "$EXPECT_ROWS" ] && grep -q $'1\tada' "$out" && grep -q $'3\tlinus' "$out"; then record spark PASS; else record spark "FAIL:rows=$n"; fi
}

# --- Trino 435 (iceberg REST OAUTH2, native S3) ---------------------------------------------
smoke_trino() {
  [ -d "$TRINO_HOME" ] && [ -f "$TRINO_CLI" ] || { record trino "SKIP:no TRINO_HOME/CLI -- add Trino 435 to the image"; return 0; }
  [ -s "$JKS" ] || { record trino "FAIL:no JKS truststore (keytool/DRC_JAVA_HOME issue)"; return 1; }
  local etc="$WORK/trino-etc"
  mkdir -p "$etc/catalog"
  cat > "$etc/config.properties" <<CFG
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
catalog.config-dir=$etc/catalog
CFG
  cat > "$etc/node.properties" <<CFG
node.environment=test
node.id=drc-smoke-trino
node.data-dir=$WORK/trino-data
CFG
  cp "$TRINO_HOME/etc/jvm.config" "$etc/jvm.config" 2>/dev/null || printf -- '-server\n-Xmx2G\n' > "$etc/jvm.config"
  printf -- '\n-Djavax.net.ssl.trustStore=%s\n-Djavax.net.ssl.trustStorePassword=changeit\n' "$JKS" >> "$etc/jvm.config"
  cat > "$etc/catalog/iceberg.properties" <<CFG
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=$DRC_URI
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=$DRC_READER_CRED
fs.native-s3.enabled=true
s3.endpoint=$DRC_S3_ENDPOINT
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=$DRC_S3_KEY
s3.aws-secret-key=$DRC_S3_SECRET
CFG
  # start server fresh (a stale server caches an expired OAuth token)
  "$DRC_JAVA_HOME/bin/java" -cp "$TRINO_HOME/lib/*" -Dplugin.dir="$TRINO_HOME/plugin" \
    $(grep -vE '^\s*#' "$etc/jvm.config" | tr '\n' ' ') \
    -Dnode.environment=test -Dnode.id=drc-smoke-trino -Dnode.data-dir="$WORK/trino-data" \
    -Dconfig="$etc/config.properties" -Dcatalog.config-dir="$etc/catalog" \
    io.trino.server.TrinoServer > "$WORK/trino.log" 2>&1 &
  local pid=$!
  local ready=""; for _ in $(seq 1 30); do sleep 4; curl -s -m 6 http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false' && { ready=1; break; }; done
  if [ -z "$ready" ]; then record trino "FAIL:server-not-ready"; kill -9 $pid 2>/dev/null; return 1; fi
  # bound the query so a hung server yields a per-engine FAIL, not a whole-job timeout
  local TO=""; command -v timeout >/dev/null 2>&1 && TO="timeout 120"
  local out; out=$($TO "$DRC_JAVA_HOME/bin/java" -jar "$TRINO_CLI" --server http://localhost:8080 --catalog iceberg \
    --execute "SELECT id,name FROM $DRC_NS.$DRC_TABLE ORDER BY id" 2>&1 | grep -aE '"[0-9]+","')
  local n; n=$(printf '%s\n' "$out" | grep -c '","')
  if [ "$n" = "$EXPECT_ROWS" ] && printf '%s' "$out" | grep -q '"1","ada"' && printf '%s' "$out" | grep -q '"3","linus"'; then record trino PASS; else record trino "FAIL:rows=$n"; fi
  kill -9 $pid 2>/dev/null
}

# --- DuckDB 1.3.2 ---------------------------------------------------------------------------
# Runs natively when a `duckdb` binary is on PATH (the engine-tester image, ubuntu glibc 2.35);
# otherwise falls back to an ubuntu sidecar via `docker run` (for a centos7 runner whose glibc
# 2.17 is too old to run duckdb itself). Both trust the gateway's self-signed dev cert via the
# system CA store.
smoke_duckdb() {
  cat > "$WORK/duck.sql" <<SQL
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET ice (TYPE ICEBERG, CLIENT_ID '${DRC_READER_CRED%%:*}', CLIENT_SECRET '${DRC_READER_CRED#*:}', OAUTH2_SERVER_URI '$DRC_URI/v1/oauth/tokens');
CREATE SECRET s3sec (TYPE S3, KEY_ID '$DRC_S3_KEY', SECRET '$DRC_S3_SECRET', ENDPOINT '${DRC_S3_ENDPOINT#*://}', URL_STYLE 'path', USE_SSL false);
ATTACH 'warehouse' AS hd (TYPE ICEBERG, ENDPOINT '$DRC_URI');
SELECT id,name FROM hd.$DRC_NS.$DRC_TABLE ORDER BY id;
SQL
  local out
  if command -v duckdb >/dev/null 2>&1; then
    cp "$PEM" /usr/local/share/ca-certificates/drc.crt 2>/dev/null && update-ca-certificates >/dev/null 2>&1 || true
    out=$(duckdb -init /dev/null < "$WORK/duck.sql" 2>&1)
  elif [ -n "$DUCKDB_IMAGE" ] && command -v docker >/dev/null 2>&1; then
    out=$(docker run --rm --network host -v "$PEM:/tmp/drc.pem:ro" -v "$WORK/duck.sql:/tmp/duck.sql:ro" \
      "$DUCKDB_IMAGE" bash -lc '
        cp /tmp/drc.pem /usr/local/share/ca-certificates/drc.crt && update-ca-certificates >/dev/null 2>&1
        duckdb -init /dev/null < /tmp/duck.sql' 2>&1)
  else
    record duckdb "SKIP:no local duckdb binary and no DUCKDB_IMAGE sidecar"; return 0
  fi
  local n; n=$(printf '%s\n' "$out" | grep -cE '\b(ada|grace|linus)\b')
  if [ "$n" = "$EXPECT_ROWS" ]; then record duckdb PASS; else record duckdb "FAIL:rows=$n"; fi
}

# --- run ------------------------------------------------------------------------------------
export_cert || exit 1
# JKS is only consumed by the JVM engines (Spark/Trino). Don't abort the whole run if it
# fails (e.g. no JDK/keytool in a PyIceberg-only image) -- warn here, and let smoke_spark/
# smoke_trino turn a missing JKS into a clear per-engine FAIL instead of an opaque SSL error.
build_truststore || echo "WARN: JKS truststore build failed (keytool exit $?); Spark/Trino will FAIL if run" >&2
for e in "${ENGINES[@]}"; do
  echo "=== smoke: $e ==="
  case "$e" in
    pyiceberg) smoke_pyiceberg ;;
    spark)     smoke_spark ;;
    trino)     smoke_trino ;;
    duckdb)    smoke_duckdb ;;
    *) record "$e" "FAIL:unknown engine" ;;
  esac
done

echo; echo "==== datalake_rest_catalog four-engine read smoke ($DRC_NS.$DRC_TABLE, expect $EXPECT_ROWS rows) ===="
# A present-but-wrong engine (FAIL) fails the job; a missing runtime (SKIP) only warns, so the
# job stays green until the CI image is extended with Spark/Trino/DuckDB (see README).  PyIceberg
# is always available (python is in the base image) and is the hard always-on gate.
fail=0; skipped=0
for e in "${ENGINES[@]}"; do
  r="${RESULT[$e]:-FAIL:not-run}"; printf "  %-10s %s\n" "$e" "$r"
  case "$r" in
    PASS) ;;
    SKIP:*) skipped=1 ;;
    *) fail=1 ;;
  esac
done
# Require the always-on gate (pyiceberg) to have actually passed, if it was requested.
if printf '%s\n' "${ENGINES[@]}" | grep -qx pyiceberg && [ "${RESULT[pyiceberg]:-}" != PASS ]; then fail=1; fi
[ $skipped -eq 1 ] && echo "NOTE: one or more engines SKIPPED (runtime not in image yet) -- see README."
[ $fail -eq 0 ] && echo "READ-SMOKE: PASS" || echo "READ-SMOKE: FAIL"
exit $fail
