#!/bin/bash
# check_services.sh
# Check which services are available and export availability flags.
# Does NOT block on missing services — callers decide what is required.

# Only set -e when executed directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -e
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMATION_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source configuration and common functions
source "${AUTOMATION_DIR}/config/test_config.env"
source "${AUTOMATION_DIR}/scripts/utils/common_functions.sh"

# ===================================================================
# Phase 1: CLI tools (optional — only installed when INSTALL_TOOLS=1)
# ===================================================================
if [ "${INSTALL_TOOLS:-0}" = "1" ]; then
    source "${SCRIPT_DIR}/install_tools.sh"
    TOOL_INSTALL_FAILURES=0
    ensure_tools || TOOL_INSTALL_FAILURES=$?
    if [ ${TOOL_INSTALL_FAILURES} -gt 0 ]; then
        log_warn "${TOOL_INSTALL_FAILURES} tool installation(s) failed — see above"
    fi
else
    log_info "Skipping CLI tool installation (set INSTALL_TOOLS=1 to enable)"
fi

log_info "==================================================================="
log_info "Checking Datalake FDW Test Environment Services"
log_info "==================================================================="

# Track failures (only database is mandatory)
FAILURES=0

# ===================================================================
# Service availability flags (exported for use by test runners)
# ===================================================================
export SERVICE_DATABASE=false
export SERVICE_MINIO=false
export SERVICE_HIVE=false
export SERVICE_HDFS=false
export SERVICE_POLARIS=false

# Check PostgreSQL/Greenplum (REQUIRED)
log_info "Checking PostgreSQL/Greenplum Database..."
if psql -c "SELECT version();" > /dev/null 2>&1; then
    VERSION=$(psql -t -c "SELECT version();" | head -n1 | xargs)
    log_success "Database is accessible: ${VERSION}"
    SERVICE_DATABASE=true
else
    log_error "Cannot connect to database"
    FAILURES=$((FAILURES + 1))
fi

# Check MinIO (optional)
log_info "Checking MinIO S3 service..."
if wait_for_service "MinIO" "${MINIO_HOST}" "${MINIO_PORT}" 3 1; then
    log_success "MinIO is accessible at ${MINIO_HOST}:${MINIO_PORT}"
    SERVICE_MINIO=true
else
    log_warn "MinIO is not accessible — S3/Iceberg tests will be skipped"
fi

# Check Hive Metastore (optional)
log_info "Checking Hive Metastore..."
if wait_for_service "Hive Metastore" "${HIVE_HOST}" "${HIVE_METASTORE_PORT}" 3 1; then
    log_success "Hive Metastore is accessible at ${HIVE_HOST}:${HIVE_METASTORE_PORT}"
    # Also check Hive Server (may be a separate container from metastore)
    if wait_for_service "Hive Server" "${HIVE_SERVER_HOST:-$HIVE_HOST}" "${HIVE_PORT}" 3 1; then
        log_success "Hive Server is accessible at ${HIVE_SERVER_HOST:-$HIVE_HOST}:${HIVE_PORT}"
        SERVICE_HIVE=true
    else
        log_warn "Hive Server is not accessible — Hive tests will be skipped"
    fi
else
    log_warn "Hive Metastore is not accessible — Hive tests will be skipped"
fi

# Check HDFS (optional)
log_info "Checking HDFS Namenode..."
if wait_for_service "HDFS" "${HDFS_NAMENODE}" "${HDFS_PORT}" 3 1; then
    log_success "HDFS is accessible at ${HDFS_NAMENODE}:${HDFS_PORT}"
    SERVICE_HDFS=true
else
    log_warn "HDFS is not accessible — HDFS tests will be skipped"
fi

# Check Polaris (optional)
log_info "Checking Polaris catalog service..."
if wait_for_service "Polaris" "${POLARIS_HOST}" "${POLARIS_PORT}" 3 1; then
    log_success "Polaris is accessible at ${POLARIS_HOST}:${POLARIS_PORT}"
    SERVICE_POLARIS=true
else
    log_warn "Polaris is not accessible — Polaris tests will be skipped"
fi

# Check Docker container (optional, informational)
if [ -n "${DOCKER_CONTAINER:-}" ]; then
    log_info "Checking Docker container..."
    if check_docker_container "${DOCKER_CONTAINER}"; then
        log_success "Docker container is running: ${DOCKER_CONTAINER}"
    else
        log_warn "Docker container is not running: ${DOCKER_CONTAINER}"
    fi
fi

# ===================================================================
# CLI Tools (connectivity verification, non-blocking, only if present)
# ===================================================================
if [ "${INSTALL_TOOLS:-0}" = "1" ]; then
    log_info "==================================================================="
    log_info "Checking CLI Tools"
    log_info "==================================================================="

    if command -v mc >/dev/null 2>&1; then
        if mc ls local/ >/dev/null 2>&1; then
            log_success "mc is available and connected to MinIO"
        else
            log_warn "mc is installed but cannot connect to MinIO"
        fi
    fi

    if command -v hdfs >/dev/null 2>&1; then
        if hdfs dfs -ls / >/dev/null 2>&1; then
            log_success "hdfs is available and connected to HDFS"
        else
            log_warn "hdfs is installed but cannot connect to HDFS"
        fi
    fi

    if command -v beeline >/dev/null 2>&1; then
        local_jdbc_url="jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}/default"
        if beeline -u "${local_jdbc_url}" -n "${HIVE_USER}" \
            -e "SELECT 1;" --silent=true >/dev/null 2>&1; then
            log_success "beeline is available and connected to HiveServer2"
        else
            log_warn "beeline is installed but cannot connect to HiveServer2"
        fi
    fi
fi

# ===================================================================
# Summary
# ===================================================================
log_info "==================================================================="
log_info "Service Availability Summary"
log_info "==================================================================="
log_info "  Database: ${SERVICE_DATABASE}"
log_info "  MinIO:    ${SERVICE_MINIO}"
log_info "  Hive:     ${SERVICE_HIVE}"
log_info "  HDFS:     ${SERVICE_HDFS}"
log_info "  Polaris:  ${SERVICE_POLARIS}"
log_info "==================================================================="

# Only fail if the database itself is down
if [ "${SERVICE_DATABASE}" != "true" ]; then
    log_error "Database is not accessible — cannot run any tests"
    # Use return when sourced, exit when executed directly
    return 1 2>/dev/null || exit 1
fi

log_success "Service check complete (database OK, other services optional)"
return 0 2>/dev/null || exit 0
