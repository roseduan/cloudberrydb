#!/bin/bash
# run_smoke_tests.sh
# Orchestrate execution of smoke tests.
#
# Usage:
#   run_smoke_tests.sh                     # run all available categories
#   run_smoke_tests.sh iceberg/builtin     # run only iceberg/builtin
#   run_smoke_tests.sh guc negative        # run guc and negative
#   run_smoke_tests.sh --list              # list categories and required services
#
# Environment:
#   SMOKE_CATEGORIES  — space-separated list of categories to run (overrides args)
#   SKIP_SERVICE_CHECK=1 — skip the service availability check

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMATION_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Put CLI tools (mc, hdfs, beeline, spark-sql) on PATH so each test category's
# Makefile cleanup step (e.g. `mc rm ... || true`, `hdfs dfs -rm ... || true`)
# actually runs instead of silently failing and leaving stale data between
# runs.  install-tools drops them into tools/bin.
export PATH="${AUTOMATION_DIR}/tools/bin:${PATH}"
# hdfs CLI also needs HADOOP_CONF_DIR to point at our cluster config.
export HADOOP_CONF_DIR="${AUTOMATION_DIR}/tools/conf/hadoop"
# The dev container ships with an HTTP/HTTPS_PROXY (10.13.11.1:1080) for
# fetching jars/etc; that proxy routes external traffic only and returns 502
# for in-cluster service hosts (minio, hadoop, hive-metastore, polaris,
# hiveserver2).  Make sure cleanup-step requests stay on the direct path so
# `mc alias set`, `hdfs dfs`, etc. don't silently fail with "Bad Gateway".
export NO_PROXY="${NO_PROXY:+${NO_PROXY},}minio,hadoop,hive-metastore,hiveserver2,polaris,localhost,127.0.0.1"
export no_proxy="$NO_PROXY"

# Source configuration and common functions
source "${AUTOMATION_DIR}/config/test_config.env"
source "${AUTOMATION_DIR}/scripts/utils/common_functions.sh"

# ===================================================================
# Service requirement map
# Each category maps to the services it needs beyond the database.
# Categories not listed here are assumed to need only the database.
# ===================================================================
declare -A CATEGORY_REQUIRES
CATEGORY_REQUIRES=(
    [hdfs]="minio hdfs hive"
    [hive]="minio hive"
    [hudi]="minio hdfs hive"
    [s3]="minio hive"
    [compression]="minio hdfs"
    [parallel]="minio hdfs"
    [fdw]="minio"
    [iceberg/builtin]="minio"
    [iceberg/hive]="minio hive"
    [iceberg/polaris]="minio polaris"
)
# Categories with no entry (guc, negative, pushdown, explain, datatypes, join)
# need only the database.

# ===================================================================
# --list: show categories and their requirements, then exit
# ===================================================================
if [ "${1:-}" = "--list" ]; then
    echo "Smoke test categories and required services:"
    echo "=============================================="
    ALL_CATS=$(find "${SQLREPO_DIR}/smoke" -name "Makefile" -type f \
        | sed "s|${SQLREPO_DIR}/smoke/||;s|/Makefile||" | sort)
    for cat in ${ALL_CATS}; do
        reqs="${CATEGORY_REQUIRES[$cat]:-database-only}"
        echo "  ${cat}  [requires: ${reqs}]"
    done
    exit 0
fi

log_info "==================================================================="
log_info "Running Datalake FDW Smoke Tests"
log_info "==================================================================="

# ===================================================================
# Detect available services
# ===================================================================
export SERVICE_DATABASE=false
export SERVICE_MINIO=false
export SERVICE_HIVE=false
export SERVICE_HDFS=false
export SERVICE_POLARIS=false

if [ "${SKIP_SERVICE_CHECK:-0}" != "1" ]; then
    log_info "Checking services..."
    # Source (not exec) so the SERVICE_* exports propagate to this shell
    source "${AUTOMATION_DIR}/scripts/setup/check_services.sh" || true
else
    log_warn "Service check skipped (SKIP_SERVICE_CHECK=1)"
    SERVICE_DATABASE=true
fi

if [ "${SERVICE_DATABASE}" != "true" ]; then
    log_error "Database is not accessible. Aborting."
    exit 1
fi

# Helper: check if a category's required services are all available
category_services_available() {
    local cat=$1
    local reqs="${CATEGORY_REQUIRES[$cat]:-}"

    # No special requirements — only needs database
    if [ -z "${reqs}" ]; then
        return 0
    fi

    for svc in ${reqs}; do
        case "${svc}" in
            minio)   [ "${SERVICE_MINIO}"   = "true" ] || return 1 ;;
            hive)    [ "${SERVICE_HIVE}"    = "true" ] || return 1 ;;
            hdfs)    [ "${SERVICE_HDFS}"    = "true" ] || return 1 ;;
            polaris) [ "${SERVICE_POLARIS}" = "true" ] || return 1 ;;
        esac
    done
    return 0
}

# ===================================================================
# Determine which categories to run
# ===================================================================
if [ -n "${SMOKE_CATEGORIES:-}" ]; then
    # Env var takes priority
    CATEGORIES_TO_RUN="${SMOKE_CATEGORIES}"
elif [ $# -gt 0 ]; then
    # Command-line arguments
    CATEGORIES_TO_RUN="$*"
else
    # Auto-discover all categories (Makefile locations)
    CATEGORIES_TO_RUN=$(find "${SQLREPO_DIR}/smoke" -name "Makefile" -type f \
        | sed "s|${SQLREPO_DIR}/smoke/||;s|/Makefile||" | sort)
fi

# Create report directory
SMOKE_REPORT_DIR="${REPORTS_DIR}/smoke/${REPORT_TIMESTAMP}"
ensure_dir "${SMOKE_REPORT_DIR}"

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

declare -a FAILED_TEST_LIST
declare -a SKIPPED_TEST_LIST

# ===================================================================
# Run tests
# ===================================================================
for category in ${CATEGORIES_TO_RUN}; do
    test_dir="${SQLREPO_DIR}/smoke/${category}"

    if [ ! -d "${test_dir}" ]; then
        log_warn "Directory not found, skipping: ${category}"
        continue
    fi

    # Check if required services are available
    if ! category_services_available "${category}"; then
        reqs="${CATEGORY_REQUIRES[$category]:-}"
        log_warn "Skipping ${category} — required services not available: ${reqs}"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
        SKIPPED_TEST_LIST+=("${category}")
        continue
    fi

    log_info "-------------------------------------------------------------------"
    log_info "Running ${category} smoke tests..."
    log_info "-------------------------------------------------------------------"

    # Find all test Makefiles in this category
    local_makefiles=$(find "${test_dir}" -name "Makefile" -type f)

    if [ -z "${local_makefiles}" ]; then
        log_warn "No Makefiles found in ${test_dir}"
        continue
    fi

    for makefile in ${local_makefiles}; do
        test_subdir=$(dirname "${makefile}")
        test_name=$(basename "${test_subdir}")

        TOTAL_TESTS=$((TOTAL_TESTS + 1))

        log_info "Running test: ${category}/${test_name}"

        pushd "${test_subdir}" > /dev/null

        # Terminate stale connections to the regression database
        psql -p "${PGPORT:-7000}" -d postgres -c \
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'regression' AND pid <> pg_backend_pid();" \
            >/dev/null 2>&1 || true

        # Run the test
        if make installcheck > "${SMOKE_REPORT_DIR}/${category//\//_}_${test_name}.log" 2>&1; then
            log_success "Test passed: ${category}/${test_name}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            log_error "Test failed: ${category}/${test_name}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            FAILED_TEST_LIST+=("${category}/${test_name}")

            # Copy regression.diffs if it exists
            if [ -f "regression.diffs" ]; then
                cp "regression.diffs" "${SMOKE_REPORT_DIR}/${category//\//_}_${test_name}.diffs"
                log_error "Regression diffs saved to: ${SMOKE_REPORT_DIR}/${category//\//_}_${test_name}.diffs"
            fi
        fi

        popd > /dev/null
    done
done

# ===================================================================
# Summary
# ===================================================================
log_info "==================================================================="
log_info "Smoke Test Summary"
log_info "==================================================================="

SUMMARY_FILE="${SMOKE_REPORT_DIR}/summary.txt"

TOTAL_WITH_SKIP=$((TOTAL_TESTS + SKIPPED_TESTS))
PASS_RATE=0
if [ ${TOTAL_TESTS} -gt 0 ]; then
    PASS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))
fi

cat > "${SUMMARY_FILE}" <<EOF
================================================================================
Test Execution Summary
================================================================================
Total Executed: ${TOTAL_TESTS}
Passed:         ${PASSED_TESTS}
Failed:         ${FAILED_TESTS}
Skipped:        ${SKIPPED_TESTS}  (services unavailable)
Pass Rate:      ${PASS_RATE}%  (of executed)
================================================================================
EOF

cat "${SUMMARY_FILE}"

if [ ${SKIPPED_TESTS} -gt 0 ]; then
    log_warn "Skipped categories (missing services):"
    for skipped in "${SKIPPED_TEST_LIST[@]}"; do
        log_warn "  - ${skipped}"
    done
fi

if [ ${FAILED_TESTS} -gt 0 ]; then
    log_error "Failed tests:"
    for failed_test in "${FAILED_TEST_LIST[@]}"; do
        log_error "  - ${failed_test}"
    done
fi

log_info "==================================================================="
log_info "Report saved to: ${SMOKE_REPORT_DIR}"
log_info "==================================================================="

# Exit with appropriate code
if [ ${FAILED_TESTS} -gt 0 ]; then
    exit 1
else
    exit 0
fi
