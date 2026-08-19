#!/bin/bash
# Iceberg namespace resolution on a non-builtin catalog (issue #411).
#
# The SQL round trip alone proves the INSERT no longer dies at COMMIT.  This
# script additionally asserts WHERE each table landed on object storage, which
# is the part a passing INSERT cannot tell you: a tier-3 fallback that
# hardcoded "public" would let every statement succeed while quietly writing
# nsfb_schema.nsfb_tier3 into the public namespace, on top of a different
# table of the same name.
#
# Usage:
#   ./run.sh                       # default database iceberg_am_nsfb_smoke
#   DB=mydb ./run.sh               # custom database name
set -eu

DB="${DB:-iceberg_am_nsfb_smoke}"
CONTAINER="${CONTAINER:-hashdata-lightning-umbrella-hashdata-1}"
PGPORT="${PGPORT:-7000}"
PREFIX="iceberg_am_namespace_fallback"
HERE_IN_CONTAINER="/workspace/database/contrib/datalake_fdw/automation/sqlrepo/smoke/$PREFIX"

# --- Pre-clean the MinIO prefix (DROP TABLE does not purge object storage) ---
docker exec lakehouse bash -lc "
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/$PREFIX/ >/dev/null 2>&1
" >/dev/null 2>&1 \
  || docker exec minio bash -c "
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc rm --recursive --force local/warehouse/$PREFIX/ >/dev/null 2>&1
" >/dev/null 2>&1 \
  || true

# --- Run the SQL round trip ---
# -eo pipefail on the inner shell: without pipefail the psql below reports
# tail's exit code, so a SQL error or a refused connection would leave
# docker exec returning 0 and the storage assertions below running anyway --
# against whatever the best-effort pre-clean left behind.
docker exec -u gpadmin "$CONTAINER" bash -eo pipefail -c "
  source /workspace/dist/database/cloudberry-env.sh
  export PGPORT=$PGPORT
  psql -d postgres -v ON_ERROR_STOP=1 <<SQL
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB;
SQL
  cd $HERE_IN_CONTAINER
  psql -d $DB -v ON_ERROR_STOP=1 -f sql/$PREFIX.sql 2>&1 | tail -50
"

# --- Assert each table landed under the iceberg namespace it should have ---
# tier 1 -> OPTIONS namespace; tier 2 -> catalog default_namespace;
# tier 3 -> the table's PG schema (public, and nsfb_schema for the second one).
#
# Both deployments the pre-clean knows about, in the same order.  The trailing
# `|| true` matters: mc exits non-zero on an empty or unreachable prefix, and
# under `set -e` the assignment would abort the script before a single
# assertion ran -- turning an informative "expected iceberg table at ..." into
# a bare abort.  An empty listing instead reaches check_path, which fails with
# the paths it looked for.
LISTING=$(docker exec lakehouse bash -lc "
    mc alias set local http://127.0.0.1:9100 admin admin12345 >/dev/null 2>&1
    mc ls --recursive local/warehouse/$PREFIX/
" 2>/dev/null \
  || docker exec minio bash -c "
    mc alias set local http://127.0.0.1:9000 admin admin12345 >/dev/null 2>&1
    mc ls --recursive local/warehouse/$PREFIX/
" 2>/dev/null \
  || true)

rc=0
check_path() {
    if echo "$LISTING" | grep -q "$1/metadata/"; then
        echo "PASS: $2 -> namespace ${1%%/*}"
    else
        echo "FAIL: $2 -> expected iceberg table at $PREFIX/$1, not found"
        rc=1
    fi
}

check_path "nsfb_explicit/nsfb_tier1" "tier 1 (OPTIONS namespace)"
check_path "nsfb_catdefault/nsfb_tier2" "tier 2 (catalog default_namespace)"
check_path "public/nsfb_tier3"          "tier 3 (PG schema public)"
check_path "nsfb_schema/nsfb_tier3"     "tier 3 (PG schema nsfb_schema)"

# A hardcoded "public" fallback would have merged the two tier-3 tables.
if [ "$rc" -ne 0 ]; then
    echo "--- object listing under warehouse/$PREFIX/ ---"
    echo "$LISTING"
fi
exit "$rc"
