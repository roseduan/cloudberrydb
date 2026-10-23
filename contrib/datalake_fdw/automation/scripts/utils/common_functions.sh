#!/bin/bash
# common_functions.sh
# Common utility functions for datalake_fdw automation tests
# Source this file in other scripts: source "$(dirname "$0")/../utils/common_functions.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Logging Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $*" >&2
}

log_debug() {
    if [ "${LOG_LEVEL}" = "DEBUG" ]; then
        echo -e "[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') - $*"
    fi
}

# ============================================================================
# Cluster Settings
# ============================================================================

# Switch the cluster to the given datalake.agent_transport (rest or grpc)
# and wait until the matching agent is running, so a run that meant to test
# one transport cannot silently test the other.
set_agent_transport() {
    local transport=$1
    local main_class
    local deadline

    case "${transport}" in
        rest) main_class='dlagent\.service\.ServiceApplication' ;;
        grpc) main_class='dlagent\.grpc\.Main' ;;
        *)
            log_error "Unknown datalake.agent_transport: ${transport}"
            return 1
            ;;
    esac

    log_info "Switching datalake.agent_transport to ${transport}"
    gpconfig -c datalake.agent_transport -v "${transport}" --skipvalidation || return 1
    gpstop -ar || return 1

    if [ "$(psql -X -d postgres -Atc 'SHOW datalake.agent_transport')" != "${transport}" ]; then
        log_error "datalake.agent_transport did not change to ${transport}"
        return 1
    fi

    # Match /proc/*/cmdline rather than `pgrep -f`: the main class follows a
    # classpath of about 28 KB, and CentOS 7's pgrep looks at only the first
    # 4 KB of a command line.  The backslashes in the pattern keep this grep
    # from matching its own command line.
    deadline=$(( $(date +%s) + 60 ))
    until grep -qs -- "${main_class}" /proc/[0-9]*/cmdline; do
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            log_error "No ${transport} agent running 60s after the restart"
            return 1
        fi
        sleep 1
    done
    log_success "datalake.agent_transport=${transport}, agent running"
}

# ============================================================================
# Service Check Functions
# ============================================================================

check_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local timeout=${4:-5}

    log_debug "Checking service: ${service_name} at ${host}:${port}"

    if command -v nc >/dev/null 2>&1; then
        if nc -z -w${timeout} "${host}" "${port}" 2>/dev/null; then
            return 0
        fi
    elif command -v timeout >/dev/null 2>&1; then
        if timeout ${timeout} bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null; then
            return 0
        fi
    else
        log_warn "Neither nc nor timeout available, skipping service check"
        return 0
    fi

    return 1
}

# Returns 1 when the service never comes up.  Only the caller knows
# whether that is fatal (load_hive_data.sh) or just skips some tests
# (check_services.sh), so the caller logs the outcome at its own level.
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-30}
    local sleep_time=${5:-2}

    log_info "Waiting for ${service_name} at ${host}:${port}..."

    local attempt=1
    while [ ${attempt} -le ${max_attempts} ]; do
        if check_service "${service_name}" "${host}" "${port}"; then
            log_success "${service_name} is ready"
            return 0
        fi

        log_debug "Attempt ${attempt}/${max_attempts} - ${service_name} not ready yet"
        sleep ${sleep_time}
        attempt=$((attempt + 1))
    done

    log_info "${service_name} not reachable after ${max_attempts} attempts"
    return 1
}

# ============================================================================
# Database Functions
# ============================================================================

run_sql() {
    local sql_file=$1
    local output_file=${2:-}

    log_debug "Executing SQL file: ${sql_file}"

    if [ ! -f "${sql_file}" ]; then
        log_error "SQL file not found: ${sql_file}"
        return 1
    fi

    if [ -n "${output_file}" ]; then
        psql -f "${sql_file}" > "${output_file}" 2>&1
    else
        psql -f "${sql_file}"
    fi

    local exit_code=$?
    if [ ${exit_code} -eq 0 ]; then
        log_debug "SQL execution completed successfully"
    else
        log_error "SQL execution failed with exit code: ${exit_code}"
    fi

    return ${exit_code}
}

run_sql_query() {
    local query=$1
    log_debug "Executing SQL query: ${query}"
    psql -c "${query}"
}

# ============================================================================
# Test Execution Functions
# ============================================================================

run_regress_test() {
    local test_dir=$1
    local test_name=$2

    log_info "Running regression test: ${test_name} in ${test_dir}"

    if [ ! -d "${test_dir}" ]; then
        log_error "Test directory not found: ${test_dir}"
        return 1
    fi

    pushd "${test_dir}" > /dev/null || return 1

    # Run the test using pg_regress
    make installcheck REGRESS="${test_name}"
    local exit_code=$?

    popd > /dev/null

    if [ ${exit_code} -eq 0 ]; then
        log_success "Test ${test_name} passed"
    else
        log_error "Test ${test_name} failed"

        # Check for regression.diffs
        if [ -f "${test_dir}/regression.diffs" ]; then
            log_error "Regression differences found in ${test_dir}/regression.diffs"
        fi
    fi

    return ${exit_code}
}

parse_test_results() {
    local results_dir=$1
    local test_name=$2

    local result_file="${results_dir}/${test_name}.out"
    local expected_file="${results_dir}/../expected/${test_name}.out"

    if [ ! -f "${result_file}" ]; then
        log_error "Result file not found: ${result_file}"
        return 1
    fi

    if [ ! -f "${expected_file}" ]; then
        log_warn "Expected file not found: ${expected_file}"
        return 0
    fi

    if diff -u "${expected_file}" "${result_file}" > /dev/null 2>&1; then
        log_success "Test ${test_name} output matches expected"
        return 0
    else
        log_error "Test ${test_name} output differs from expected"
        return 1
    fi
}

# ============================================================================
# Report Generation Functions
# ============================================================================

generate_test_summary() {
    local total_tests=$1
    local passed_tests=$2
    local failed_tests=$3
    local output_file=$4

    local pass_rate=0
    if [ ${total_tests} -gt 0 ]; then
        pass_rate=$((passed_tests * 100 / total_tests))
    fi

    cat > "${output_file}" <<EOF
================================================================================
Test Execution Summary
================================================================================
Total Tests:  ${total_tests}
Passed:       ${passed_tests}
Failed:       ${failed_tests}
Pass Rate:    ${pass_rate}%
================================================================================
EOF

    if [ ${failed_tests} -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}${failed_tests} test(s) failed${NC}"
    fi
}

# ============================================================================
# File System Functions
# ============================================================================

ensure_dir() {
    local dir=$1
    if [ ! -d "${dir}" ]; then
        log_debug "Creating directory: ${dir}"
        mkdir -p "${dir}"
    fi
}

cleanup_test_artifacts() {
    local test_dir=$1

    log_info "Cleaning up test artifacts in ${test_dir}"

    find "${test_dir}" -name "results" -type d -exec rm -rf {} + 2>/dev/null || true
    find "${test_dir}" -name "regression.diffs" -delete 2>/dev/null || true
    find "${test_dir}" -name "regression.out" -delete 2>/dev/null || true
    find "${test_dir}" -name "*.tmp" -delete 2>/dev/null || true

    log_success "Cleanup completed"
}

# ============================================================================
# Docker Helper Functions
# ============================================================================

docker_exec() {
    local container_name=$1
    shift
    local command="$*"

    log_debug "Executing in container ${container_name}: ${command}"
    docker exec -i "${container_name}" bash -c "${command}"
}

check_docker_container() {
    local container_name=$1

    if ! docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        log_error "Docker container not running: ${container_name}"
        return 1
    fi

    log_debug "Docker container is running: ${container_name}"
    return 0
}

# ============================================================================
# Validation Functions
# ============================================================================

validate_environment() {
    log_info "Validating test environment..."

    local errors=0

    # Check required commands
    for cmd in psql docker make; do
        if ! command -v ${cmd} >/dev/null 2>&1; then
            log_error "Required command not found: ${cmd}"
            errors=$((errors + 1))
        fi
    done

    # Check environment variables
    if [ -z "${PGDATABASE}" ]; then
        log_error "PGDATABASE not set"
        errors=$((errors + 1))
    fi

    if [ ${errors} -gt 0 ]; then
        log_error "Environment validation failed with ${errors} error(s)"
        return 1
    fi

    log_success "Environment validation passed"
    return 0
}

# ============================================================================
# Export Functions
# ============================================================================

export -f log_info
export -f log_success
export -f log_warn
export -f log_error
export -f log_debug
export -f check_service
export -f wait_for_service
export -f run_sql
export -f run_sql_query
export -f run_regress_test
export -f parse_test_results
export -f generate_test_summary
export -f ensure_dir
export -f cleanup_test_artifacts
export -f docker_exec
export -f check_docker_container
export -f validate_environment
