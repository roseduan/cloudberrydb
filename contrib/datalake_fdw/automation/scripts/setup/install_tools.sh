#!/bin/bash
# install_tools.sh
# Install and configure CLI tools (mc, beeline, hdfs, spark-sql) for datalake_fdw tests.
#
# Strategy per tool:
#   1. Already in PATH? → skip
#   2. download_cache has the artifact? → install from cache
#   3. Otherwise → download from internet
#
# All tools are installed into $AUTOMATION_DIR/tools/ (project-local, no sudo
# except for Java).
#
# Usage:
#   source install_tools.sh   # then call ensure_tools
#   bash   install_tools.sh   # standalone

set -euo pipefail

# ============================================================================
# Directory Setup
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTOMATION_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Source config & helpers (guard against double-source)
if [ -z "${_COMMON_FUNCTIONS_LOADED:-}" ]; then
    source "${AUTOMATION_DIR}/config/test_config.env"
    source "${AUTOMATION_DIR}/scripts/utils/common_functions.sh"
fi

TOOLS_DIR="${TOOLS_DIR:-${AUTOMATION_DIR}/tools}"
TOOLS_BIN="${TOOLS_DIR}/bin"
TOOLS_CONF="${TOOLS_DIR}/conf"
DOWNLOAD_CACHE="${DOWNLOAD_CACHE_DIR:-${AUTOMATION_DIR}/docker/singlecluster/download_cache}"

HADOOP_VERSION="${HADOOP_VERSION:-3.0.0}"
HIVE_VERSION="${HIVE_VERSION:-3.0.0}"
SPARK_VERSION="${SPARK_VERSION:-3.3.3}"
HUDI_VERSION="${HUDI_VERSION:-0.13.1}"
# Filename differs from the 0.11.x line: spark3.3-bundle (not spark3-bundle).
HUDI_JAR_NAME="${HUDI_JAR_NAME:-hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar}"

# ============================================================================
# Helpers
# ============================================================================

setup_tools_dir() {
    mkdir -p "${TOOLS_BIN}" "${TOOLS_CONF}"
    if [[ ":${PATH}:" != *":${TOOLS_BIN}:"* ]]; then
        export PATH="${TOOLS_BIN}:${PATH}"
    fi
}

# Detect JAVA_HOME from the java binary if not already set.
detect_java_home() {
    if [ -n "${JAVA_HOME:-}" ] && [ -d "${JAVA_HOME}" ]; then
        return 0
    fi
    local java_bin
    java_bin="$(command -v java 2>/dev/null || true)"
    if [ -z "${java_bin}" ]; then
        return 1
    fi
    # Resolve symlinks: /usr/bin/java -> /usr/lib/jvm/java-8-xxx/bin/java
    java_bin="$(readlink -f "${java_bin}")"
    export JAVA_HOME="${java_bin%/bin/java}"
    log_debug "Detected JAVA_HOME=${JAVA_HOME}"
}

# ============================================================================
# Java
# ============================================================================

install_java() {
    if command -v java >/dev/null 2>&1; then
        detect_java_home
        log_success "Java already installed: $(java -version 2>&1 | head -1) (JAVA_HOME=${JAVA_HOME:-unset})"
        return 0
    fi

    log_info "Installing Java (OpenJDK 8)..."

    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq openjdk-8-jdk-headless 2>&1 | tail -3
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y -q java-1.8.0-openjdk-devel
    elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y -q java-1.8.0-openjdk-devel
    elif command -v apk >/dev/null 2>&1; then
        sudo apk add --no-cache openjdk8
    else
        log_error "No supported package manager found. Please install Java 8+ manually."
        return 1
    fi

    if ! command -v java >/dev/null 2>&1; then
        log_error "Java installation failed"
        return 1
    fi

    detect_java_home
    log_success "Java installed: $(java -version 2>&1 | head -1) (JAVA_HOME=${JAVA_HOME:-unset})"
}

# ============================================================================
# mc (MinIO Client)
# ============================================================================

install_mc() {
    if command -v mc >/dev/null 2>&1; then
        log_success "mc already installed: $(mc --version 2>&1 | head -1 || echo 'unknown')"
        return 0
    fi

    log_info "Installing mc (MinIO Client)..."

    if [ -f "${DOWNLOAD_CACHE}/mc" ]; then
        log_info "  Using cached binary: ${DOWNLOAD_CACHE}/mc"
        cp "${DOWNLOAD_CACHE}/mc" "${TOOLS_BIN}/mc"
    else
        log_info "  Downloading mc for linux-amd64..."
        curl -fSL "https://dl.min.io/client/mc/release/linux-amd64/mc" \
            -o "${TOOLS_BIN}/mc"
    fi
    chmod +x "${TOOLS_BIN}/mc"

    log_success "mc installed: ${TOOLS_BIN}/mc"
}

configure_mc() {
    log_info "Configuring mc aliases for MinIO at ${MINIO_HOST}:${MINIO_PORT}..."

    local endpoint="http://${MINIO_HOST}:${MINIO_PORT}"

    # 'local' alias — compatible with existing Makefile scripts
    mc alias set local "${endpoint}" \
        "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" >/dev/null 2>&1 || true

    # 'lakehouse' alias — explicit name
    mc alias set lakehouse "${endpoint}" \
        "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" >/dev/null 2>&1 || true

    # Verify connectivity
    if mc ls local/ >/dev/null 2>&1; then
        log_success "mc → MinIO connected (${endpoint})"
    else
        log_warn "mc aliases configured but MinIO not reachable yet (${endpoint})"
    fi
}

# ============================================================================
# Hadoop / hdfs CLI
# ============================================================================

install_hadoop() {
    local hadoop_dir="${TOOLS_DIR}/hadoop-${HADOOP_VERSION}"

    if [ ! -d "${hadoop_dir}" ]; then
        local tarball="hadoop-${HADOOP_VERSION}.tar.gz"
        log_info "Installing Hadoop ${HADOOP_VERSION}..."

        if [ -f "${DOWNLOAD_CACHE}/${tarball}" ]; then
            log_info "  Extracting from cache: ${DOWNLOAD_CACHE}/${tarball}"
            tar -xzf "${DOWNLOAD_CACHE}/${tarball}" -C "${TOOLS_DIR}"
        else
            log_info "  Downloading ${tarball}..."
            curl -fSL \
                "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/${tarball}" \
                | tar -xz -C "${TOOLS_DIR}"
        fi
    else
        log_info "Hadoop already extracted: ${hadoop_dir}"
    fi

    # Wrapper scripts (not symlinks) so hadoop scripts resolve paths correctly
    rm -f "${TOOLS_BIN}/hdfs" "${TOOLS_BIN}/hadoop"

    cat > "${TOOLS_BIN}/hdfs" <<WRAPPER
#!/bin/bash
exec "${hadoop_dir}/bin/hdfs" "\$@"
WRAPPER
    chmod +x "${TOOLS_BIN}/hdfs"

    cat > "${TOOLS_BIN}/hadoop" <<WRAPPER
#!/bin/bash
exec "${hadoop_dir}/bin/hadoop" "\$@"
WRAPPER
    chmod +x "${TOOLS_BIN}/hadoop"

    export HADOOP_HOME="${hadoop_dir}"
    log_success "Hadoop installed: ${hadoop_dir}"
}

configure_hadoop() {
    # Write core-site.xml with HDFS + S3A configuration
    local conf_dir="${TOOLS_CONF}/hadoop"
    mkdir -p "${conf_dir}"

    cat > "${conf_dir}/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name>
    <value>hdfs://${HDFS_NAMENODE}:${HDFS_PORT}</value></property>
  <property><name>fs.s3a.endpoint</name>
    <value>http://${MINIO_HOST}:${MINIO_PORT}</value></property>
  <property><name>fs.s3a.access.key</name>
    <value>${MINIO_ACCESS_KEY}</value></property>
  <property><name>fs.s3a.secret.key</name>
    <value>${MINIO_SECRET_KEY}</value></property>
  <property><name>fs.s3a.path.style.access</name>
    <value>true</value></property>
  <property><name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value></property>
  <property><name>hadoop.proxyuser.root.hosts</name>
    <value>*</value></property>
  <property><name>hadoop.proxyuser.root.groups</name>
    <value>*</value></property>
</configuration>
EOF

    # Write hdfs-site.xml — client-side HDFS settings
    cat > "${conf_dir}/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

    export HADOOP_CONF_DIR="${conf_dir}"
    log_info "HADOOP_CONF_DIR=${conf_dir}  (fs.defaultFS=hdfs://${HDFS_NAMENODE}:${HDFS_PORT})"

    # Verify connectivity
    if hdfs dfs -ls / >/dev/null 2>&1; then
        log_success "hdfs → HDFS connected (hdfs://${HDFS_NAMENODE}:${HDFS_PORT})"
    else
        log_warn "hdfs configured but HDFS not reachable yet"
    fi
}

# ============================================================================
# Hive / beeline
# ============================================================================

install_beeline() {
    local hive_dir="${TOOLS_DIR}/apache-hive-${HIVE_VERSION}-bin"

    if [ ! -d "${hive_dir}" ]; then
        local tarball="apache-hive-${HIVE_VERSION}-bin.tar.gz"
        log_info "Installing Apache Hive ${HIVE_VERSION} (for beeline)..."

        if [ -f "${DOWNLOAD_CACHE}/${tarball}" ]; then
            log_info "  Extracting from cache: ${DOWNLOAD_CACHE}/${tarball}"
            tar -xzf "${DOWNLOAD_CACHE}/${tarball}" -C "${TOOLS_DIR}"
        else
            log_info "  Downloading ${tarball}..."
            curl -fSL \
                "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${tarball}" \
                | tar -xz -C "${TOOLS_DIR}"
        fi
    else
        log_info "Hive already extracted: ${hive_dir}"
    fi

    # Wrapper script (not symlink) so beeline can find its sibling 'hive' script
    rm -f "${TOOLS_BIN}/beeline"
    cat > "${TOOLS_BIN}/beeline" <<WRAPPER
#!/bin/bash
exec "${hive_dir}/bin/beeline" "\$@"
WRAPPER
    chmod +x "${TOOLS_BIN}/beeline"

    export HIVE_HOME="${hive_dir}"
    log_success "beeline installed: ${hive_dir}"
}

configure_beeline() {
    # beeline targets HiveServer2; HIVE_SERVER_HOST is set when metastore
    # and HiveServer2 live in separate containers, fall back to HIVE_HOST
    # for the legacy all-in-one image.
    local hs2_host="${HIVE_SERVER_HOST:-$HIVE_HOST}"
    local jdbc_url="jdbc:hive2://${hs2_host}:${HIVE_PORT}/default"
    log_info "Verifying beeline → HiveServer2 at ${hs2_host}:${HIVE_PORT}..."

    if beeline -u "${jdbc_url}" -n "${HIVE_USER}" \
        -e "SELECT 1;" --silent=true >/dev/null 2>&1; then
        log_success "beeline → HiveServer2 connected (${jdbc_url})"
    else
        log_warn "beeline configured but HiveServer2 not reachable yet (${jdbc_url})"
    fi
}

# ============================================================================
# Spark / spark-sql (with Hudi + Iceberg support)
# ============================================================================

install_spark() {
    local spark_dir="${TOOLS_DIR}/spark-${SPARK_VERSION}-bin-hadoop3"

    if [ ! -d "${spark_dir}" ]; then
        local tarball="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
        log_info "Installing Spark ${SPARK_VERSION}..."

        if [ -f "${DOWNLOAD_CACHE}/${tarball}" ]; then
            log_info "  Extracting from cache: ${DOWNLOAD_CACHE}/${tarball}"
            tar -xzf "${DOWNLOAD_CACHE}/${tarball}" -C "${TOOLS_DIR}"
        else
            log_info "  Downloading ${tarball}..."
            curl -fSL \
                "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${tarball}" \
                | tar -xz -C "${TOOLS_DIR}"
        fi
    else
        log_info "Spark already extracted: ${spark_dir}"
    fi

    # Copy Hudi bundle JAR into Spark jars/.
    # Purge any stale bundles (e.g. older 0.11.x line) so the Spark classloader
    # does not pick up two conflicting versions and trip
    # NoSuchMethodError: ParserUtils$.withOrigin on Spark 3.3.
    find "${spark_dir}/jars" -maxdepth 1 -name 'hudi-spark*-bundle_*.jar' ! -name "${HUDI_JAR_NAME}" -print -delete 2>/dev/null || true
    if [ -f "${DOWNLOAD_CACHE}/${HUDI_JAR_NAME}" ] && [ ! -f "${spark_dir}/jars/${HUDI_JAR_NAME}" ]; then
        log_info "  Copying Hudi bundle: ${HUDI_JAR_NAME}"
        cp "${DOWNLOAD_CACHE}/${HUDI_JAR_NAME}" "${spark_dir}/jars/"
    fi

    # Copy Iceberg runtime JAR into Spark jars/ (if available)
    local iceberg_jar
    iceberg_jar="$(ls "${DOWNLOAD_CACHE}"/iceberg-spark-runtime-*.jar 2>/dev/null | head -1 || true)"
    if [ -n "${iceberg_jar}" ]; then
        local iceberg_basename
        iceberg_basename="$(basename "${iceberg_jar}")"
        if [ ! -f "${spark_dir}/jars/${iceberg_basename}" ]; then
            log_info "  Copying Iceberg runtime: ${iceberg_basename}"
            cp "${iceberg_jar}" "${spark_dir}/jars/"
        fi
    fi

    # Copy AWS SDK JAR for S3A support (if available)
    local aws_jar
    aws_jar="$(ls "${DOWNLOAD_CACHE}"/aws-java-sdk-bundle-*.jar 2>/dev/null | head -1 || true)"
    if [ -n "${aws_jar}" ]; then
        local aws_basename
        aws_basename="$(basename "${aws_jar}")"
        if [ ! -f "${spark_dir}/jars/${aws_basename}" ]; then
            log_info "  Copying AWS SDK: ${aws_basename}"
            cp "${aws_jar}" "${spark_dir}/jars/"
        fi
    fi

    # Copy hadoop-aws JAR for S3A support (if available)
    local hadoop_aws_jar
    hadoop_aws_jar="$(ls "${DOWNLOAD_CACHE}"/hadoop-aws-*.jar 2>/dev/null | head -1 || true)"
    if [ -n "${hadoop_aws_jar}" ]; then
        local hadoop_aws_basename
        hadoop_aws_basename="$(basename "${hadoop_aws_jar}")"
        if [ ! -f "${spark_dir}/jars/${hadoop_aws_basename}" ]; then
            log_info "  Copying hadoop-aws: ${hadoop_aws_basename}"
            cp "${hadoop_aws_jar}" "${spark_dir}/jars/"
        fi
    fi

    # Wrapper scripts (not symlinks) so spark can find its own home directory.
    # Remove any existing symlinks first to avoid writing through them
    # and corrupting the original Spark scripts.
    rm -f "${TOOLS_BIN}/spark-sql" "${TOOLS_BIN}/spark-submit"

    cat > "${TOOLS_BIN}/spark-sql" <<WRAPPER
#!/bin/bash
exec "${spark_dir}/bin/spark-sql" "\$@"
WRAPPER
    chmod +x "${TOOLS_BIN}/spark-sql"

    cat > "${TOOLS_BIN}/spark-submit" <<WRAPPER
#!/bin/bash
exec "${spark_dir}/bin/spark-submit" "\$@"
WRAPPER
    chmod +x "${TOOLS_BIN}/spark-submit"

    export SPARK_HOME="${spark_dir}"
    log_success "Spark installed: ${spark_dir}"
}

configure_spark() {
    local spark_dir="${TOOLS_DIR}/spark-${SPARK_VERSION}-bin-hadoop3"
    local conf_dir="${spark_dir}/conf"
    mkdir -p "${conf_dir}"

    # spark-defaults.conf — client-mode defaults
    cat > "${conf_dir}/spark-defaults.conf" <<EOF
spark.master                           spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}
spark.submit.deployMode                client
spark.driver.memory                    512m
spark.executor.memory                  512m
spark.driver.host                      $(hostname)

# Hive Metastore integration
spark.sql.catalogImplementation        hive
spark.hadoop.hive.metastore.uris       thrift://${HIVE_HOST}:${HIVE_METASTORE_PORT}

# S3A / MinIO configuration
spark.hadoop.fs.s3a.endpoint           http://${MINIO_HOST}:${MINIO_PORT}
spark.hadoop.fs.s3a.access.key         ${MINIO_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key         ${MINIO_SECRET_KEY}
spark.hadoop.fs.s3a.path.style.access  true
spark.hadoop.fs.s3a.connection.ssl.enabled  false
spark.hadoop.fs.s3a.impl               org.apache.hadoop.fs.s3a.S3AFileSystem

# Serializer (required by Hudi DataSource API)
spark.serializer                       org.apache.spark.serializer.KryoSerializer

# HDFS defaults
spark.hadoop.fs.defaultFS              hdfs://${HDFS_NAMENODE}:${HDFS_PORT}
EOF

    # hive-site.xml — point to the remote Hive Metastore
    cat > "${conf_dir}/hive-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${HIVE_HOST}:${HIVE_METASTORE_PORT}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/warehouse</value>
  </property>
</configuration>
EOF

    export SPARK_HOME="${spark_dir}"
    export SPARK_CONF_DIR="${conf_dir}"
    log_info "SPARK_HOME=${spark_dir}"
    log_info "Spark master: spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
    log_info "Hive Metastore: thrift://${HIVE_HOST}:${HIVE_METASTORE_PORT}"
    log_info "S3A endpoint: http://${MINIO_HOST}:${MINIO_PORT}"

    # Verify connectivity to Spark Master
    if nc -z -w2 "${SPARK_MASTER_HOST}" "${SPARK_MASTER_PORT}" 2>/dev/null; then
        log_success "spark-sql → Spark Master reachable (${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT})"
    else
        log_warn "Spark Master not reachable at ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
    fi
}

# ============================================================================
# Environment Summary — export all vars so child processes inherit them
# ============================================================================

export_tool_env() {
    # Ensure JAVA_HOME is set
    detect_java_home || true

    cat <<EOF

============== Tool Environment ==============
PATH includes:       ${TOOLS_BIN}
JAVA_HOME:           ${JAVA_HOME:-<not set>}
HADOOP_HOME:         ${HADOOP_HOME:-<not set>}
HADOOP_CONF_DIR:     ${HADOOP_CONF_DIR:-<not set>}
HIVE_HOME:           ${HIVE_HOME:-<not set>}
SPARK_HOME:          ${SPARK_HOME:-<not set>}
==============================================
EOF
}

persist_tool_env() {
    local bashrc="${HOME}/.bashrc"
    local marker="# >>> datalake-tools >>>"
    local marker_end="# <<< datalake-tools <<<"
    local spark_dir="${TOOLS_DIR}/spark-${SPARK_VERSION}-bin-hadoop3"
    local hadoop_dir="${TOOLS_DIR}/hadoop-${HADOOP_VERSION}"
    local hive_dir="${TOOLS_DIR}/apache-hive-${HIVE_VERSION}-bin"

    # If marker block already exists, remove the old one
    if grep -q "${marker}" "${bashrc}" 2>/dev/null; then
        sed -i "/${marker}/,/${marker_end}/d" "${bashrc}"
    fi

    cat >> "${bashrc}" <<BASHRC
${marker}
export JAVA_HOME="${JAVA_HOME:-}"
export HADOOP_HOME="${hadoop_dir}"
export HADOOP_CONF_DIR="${TOOLS_CONF}/hadoop"
export HIVE_HOME="${hive_dir}"
export SPARK_HOME="${spark_dir}"
export SPARK_CONF_DIR="${spark_dir}/conf"
export PATH="${TOOLS_BIN}:${hadoop_dir}/bin:${hive_dir}/bin:${spark_dir}/bin:\${PATH}"
${marker_end}
BASHRC

    log_success "Environment persisted to ${bashrc}"
}

# ============================================================================
# Main Entry Point
# ============================================================================

ensure_download_cache() {
    local needed_files=("mc" "hadoop-${HADOOP_VERSION}.tar.gz" "apache-hive-${HIVE_VERSION}-bin.tar.gz" "spark-${SPARK_VERSION}-bin-hadoop3.tgz")
    local missing=0
    for f in "${needed_files[@]}"; do
        [ -f "${DOWNLOAD_CACHE}/${f}" ] || missing=$((missing + 1))
    done
    if [ ${missing} -gt 0 ]; then
        local dl_script="${DOWNLOAD_CACHE}/download.sh"
        if [ -f "${dl_script}" ]; then
            log_info "download_cache missing ${missing} artifact(s), running download.sh ..."
            bash "${dl_script}"
        else
            log_warn "download_cache incomplete and download.sh not found at ${dl_script}"
        fi
    fi
}

ensure_tools() {
    log_info "==================================================================="
    log_info "Ensuring CLI Tools: java, mc, hdfs, beeline, spark-sql"
    log_info "  download_cache: ${DOWNLOAD_CACHE}"
    log_info "  tools dir:      ${TOOLS_DIR}"
    log_info "==================================================================="

    setup_tools_dir
    ensure_download_cache

    local failures=0

    # 1. Java — prerequisite for beeline, hdfs, and spark
    install_java    || failures=$((failures + 1))

    # 2. mc (MinIO Client)
    install_mc      || failures=$((failures + 1))
    configure_mc

    # 3. Hadoop / hdfs CLI
    install_hadoop  || failures=$((failures + 1))
    configure_hadoop

    # 4. Hive / beeline
    install_beeline || failures=$((failures + 1))
    configure_beeline

    # 5. Spark / spark-sql (with Hudi + Iceberg JARs)
    install_spark   || failures=$((failures + 1))
    configure_spark

    # 6. TPC tools (opt-in): dbgen for TPC-H, dsdgen for TPC-DS. Gated by
    #    INSTALL_TPC_TOOLS=1 because the scale-tpc suites are the only
    #    consumers; hive/hudi/s3 smoke flows do not need them and clones
    #    + builds add ~15-30s even when cached.
    if [ "${INSTALL_TPC_TOOLS:-0}" = "1" ]; then
        bash "${SCRIPT_DIR}/install_tpc_tools.sh" || failures=$((failures + 1))
    fi

    # 7. Export environment for child processes
    export_tool_env

    # 8. Persist environment for future login sessions
    persist_tool_env

    log_info "==================================================================="
    if [ ${failures} -eq 0 ]; then
        log_success "All CLI tools are ready (${failures} failures)"
    else
        log_error "${failures} tool installation(s) failed"
    fi

    return ${failures}
}

# Allow both sourcing and standalone execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    ensure_tools
fi
