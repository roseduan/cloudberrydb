#!/bin/bash
# run_feature_tests.sh
# Orchestrate execution of feature tests (optionally by category)

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMATION_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source configuration and common functions
source "${AUTOMATION_DIR}/config/test_config.env"
source "${AUTOMATION_DIR}/scripts/utils/common_functions.sh"

# Parse arguments
CATEGORY_FILTER=""
if [ $# -gt 0 ]; then
    CATEGORY_FILTER="$1"
    log_info "Running feature tests for category: ${CATEGORY_FILTER}"
fi

log_info "==================================================================="
log_info "Running Datalake FDW Feature Tests"
log_info "==================================================================="

# Check if services are ready
log_info "Checking services..."
if ! bash "${AUTOMATION_DIR}/scripts/setup/check_services.sh"; then
    log_error "Services are not ready. Aborting tests."
    exit 1
fi

# Create report directory
FEATURE_REPORT_DIR="${REPORTS_DIR}/feature/${REPORT_TIMESTAMP}"
ensure_dir "${FEATURE_REPORT_DIR}"

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Array to store failed tests
declare -a FAILED_TEST_LIST

# Function to run a feature test category
run_feature_category() {
    local category=$1
    local test_dir="${SQLREPO_DIR}/feature/${category}"

    if [ ! -d "${test_dir}" ]; then
        log_warn "Feature test directory not found: ${test_dir}"
        return 0
    fi

    # Check if Makefile exists
    if [ ! -f "${test_dir}/Makefile" ]; then
        log_warn "No Makefile found in ${test_dir}"
        return 0
    fi

    log_info "-------------------------------------------------------------------"
    log_info "Running ${category} feature tests..."
    log_info "-------------------------------------------------------------------"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    log_info "Running feature test: ${category}"

    pushd "${test_dir}" > /dev/null

    # Run the test
    if make installcheck > "${FEATURE_REPORT_DIR}/${category}.log" 2>&1; then
        log_success "Feature test passed: ${category}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        log_error "Feature test failed: ${category}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_TEST_LIST+=("${category}")

        # Copy regression.diffs if it exists
        if [ -f "regression.diffs" ]; then
            cp "regression.diffs" "${FEATURE_REPORT_DIR}/${category}.diffs"
            log_error "Regression diffs saved to: ${FEATURE_REPORT_DIR}/${category}.diffs"
        fi
    fi

    popd > /dev/null
}

# Discover feature test categories
log_info "Discovering feature test categories..."

if [ -n "${CATEGORY_FILTER}" ]; then
    # Run specific category
    if [ -d "${SQLREPO_DIR}/feature/${CATEGORY_FILTER}" ]; then
        run_feature_category "${CATEGORY_FILTER}"
    else
        log_error "Category not found: ${CATEGORY_FILTER}"
        exit 1
    fi
else
    # Run all categories, in a fixed order.  find returns directories in
    # whatever order the filesystem hands them over, so without the sort the
    # suites run in a different sequence on different machines -- and these
    # suites are not independent: one that leaves the storage endpoint wedged
    # (feature/negative points at an unreachable host on purpose) slows down
    # whichever suite happens to follow it.  A failure that depends on the
    # order is one nobody can reproduce.
    FEATURE_CATEGORIES=$(find "${SQLREPO_DIR}/feature" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort)

    if [ -z "${FEATURE_CATEGORIES}" ]; then
        log_error "No feature test categories found in ${SQLREPO_DIR}/feature"
        exit 1
    fi

    for category in ${FEATURE_CATEGORIES}; do
        run_feature_category "${category}"
    done
fi

# Generate summary report
log_info "==================================================================="
log_info "Feature Test Summary"
log_info "==================================================================="

SUMMARY_FILE="${FEATURE_REPORT_DIR}/summary.txt"
generate_test_summary ${TOTAL_TESTS} ${PASSED_TESTS} ${FAILED_TESTS} "${SUMMARY_FILE}"

cat "${SUMMARY_FILE}"

# List failed tests
if [ ${FAILED_TESTS} -gt 0 ]; then
    log_error "Failed tests:"
    for failed_test in "${FAILED_TEST_LIST[@]}"; do
        log_error "  - ${failed_test}"
    done
fi

log_info "==================================================================="
log_info "Report saved to: ${FEATURE_REPORT_DIR}"
log_info "==================================================================="

# Exit with appropriate code
if [ ${FAILED_TESTS} -gt 0 ]; then
    exit 1
else
    exit 0
fi
