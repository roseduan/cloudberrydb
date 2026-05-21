#!/bin/bash
# load_hive_data.sh
# Load test data into Hive tables for smoke and feature tests

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMATION_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source configuration and common functions
source "${AUTOMATION_DIR}/config/test_config.env"
source "${AUTOMATION_DIR}/scripts/utils/common_functions.sh"

log_info "==================================================================="
log_info "Loading Hive Test Data"
log_info "==================================================================="

# Check if beeline is available
if ! command -v beeline >/dev/null 2>&1; then
    log_error "beeline command not found. Please install it first:"
    log_error "  bash ${AUTOMATION_DIR}/scripts/setup/install_beeline.sh"
    exit 1
fi

# Check if Hive is accessible.  beeline talks to HiveServer2; in split-
# container topologies HiveServer2 lives on HIVE_SERVER_HOST while the
# metastore lives on HIVE_HOST.  Fall back to HIVE_HOST for the legacy
# all-in-one image where both services share a host.
HIVE_HS2_HOST="${HIVE_SERVER_HOST:-$HIVE_HOST}"
if ! wait_for_service "Hive Server" "${HIVE_HS2_HOST}" "${HIVE_PORT}" 3 1; then
    log_error "Hive Server is not accessible at ${HIVE_HS2_HOST}:${HIVE_PORT}"
    exit 1
fi

# Function to run beeline command
run_beeline() {
    local sql_file=$1
    log_info "Executing Hive SQL: ${sql_file}"

    beeline -u "jdbc:hive2://${HIVE_HS2_HOST}:${HIVE_PORT}/default" \
            -n "${HIVE_USER}" \
            -f "${sql_file}"

    if [ $? -eq 0 ]; then
        log_success "Hive SQL executed successfully"
    else
        log_error "Hive SQL execution failed"
        return 1
    fi
}

# Load smoke test data
SMOKE_DATA_FILE="${AUTOMATION_DIR}/prepare/prepare_hive/hive_smoke.sql"
if [ -f "${SMOKE_DATA_FILE}" ]; then
    log_info "Loading smoke test data..."
    run_beeline "${SMOKE_DATA_FILE}"
else
    log_warn "Smoke test data file not found: ${SMOKE_DATA_FILE}"
fi

# Load additional test data (if exists)
# Add more data loading here as needed

log_info "==================================================================="
log_success "Hive test data loading completed"
log_info "==================================================================="
