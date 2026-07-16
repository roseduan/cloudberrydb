# Consolidated four-engine Iceberg-client tester image for the datalake_rest_catalog read smoke.
#
# One ubuntu image carrying PyIceberg + Spark + Trino + DuckDB so the CI job can validate all
# four standard Iceberg REST clients WITHOUT bloating the centos-7 build image. The GitLab job
# brings up the cluster/gateway/seed on the runner, then runs the read step inside this image:
#
#   docker run --rm --network host \
#     -e http_proxy -e https_proxy -e no_proxy \
#     -v "$CI_PROJECT_DIR:/work:ro" \
#     <this-image> \
#     bash /work/database/contrib/datalake_rest_catalog/e2e/smoke/run_engines.sh pyiceberg spark trino duckdb
#
# --network host lets localhost:8443 / :9000 reach the gateway + MinIO running on the runner.
# The repo is mounted (not baked) so run_engines.sh always matches the branch under test; this
# image provides only the (rarely-changing) engine runtimes.
#
# ubuntu 22.04 = glibc 2.35 + modern libstdc++, so PyIceberg 0.11 needs no GLIBCXX/LD hacks and
# DuckDB runs natively (its extensions require glibc >= 2.28, which the centos-7 base lacks).
#
# Build BOTH arches and push to the internal registry, e.g.:
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -f smoke/iceberg-engine-tester.Dockerfile \
#     -t docker.hashdata.dev/<ns>/iceberg-engine-tester:1.0 --push \
#     contrib/datalake_rest_catalog/e2e            # build context = e2e/ (for the requirements file)
# Needs network to Maven Central + the DuckDB GitHub release + a pip index at build time
# (--build-arg PIP_INDEX_URL=<internal mirror> if Maven/PyPI are only reachable internally).
FROM ubuntu:22.04

ARG TARGETARCH                         # amd64 | arm64 (buildkit-provided; matches DuckDB asset naming)
ARG SPARK_VERSION=3.3.4
ARG TRINO_VERSION=435
ARG DUCKDB_VERSION=1.3.2
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.3.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262
ARG AWS_SDK_V2_BUNDLE_VERSION=2.20.18
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2
ARG PIP_INDEX_URL=
ENV DEBIAN_FRONTEND=noninteractive

# Acquire::Retries hardens apt against flaky proxies / mirror blips.
RUN printf 'Acquire::Retries "8";\nAcquire::http::Timeout "30";\n' > /etc/apt/apt.conf.d/80-retries \
 && apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl unzip openssl \
      python3 python3-venv python3-pip \
      openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

# Stable JAVA_HOME across arches (Trino 435 requires JDK17; Spark 3.3.4 runs on 8/11/17).
RUN ln -s "/usr/lib/jvm/java-17-openjdk-${TARGETARCH}" /opt/jdk17
ENV JAVA_HOME=/opt/jdk17 PATH=/opt/jdk17/bin:$PATH

# --- Spark 3.3.4 + the jars S3FileIO/REST need -------------------------------------------
RUN curl --retry 6 --retry-all-errors --retry-delay 3 -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
 && mkdir -p /opt/spark \
 && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
 && rm /tmp/spark.tgz \
 && cd /opt/spark/jars \
 && curl --retry 6 --retry-all-errors --retry-delay 3 -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_SPARK_RUNTIME_VERSION}/iceberg-spark-runtime-3.3_2.12-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
 && curl --retry 6 --retry-all-errors --retry-delay 3 -fsSLO "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl --retry 6 --retry-all-errors --retry-delay 3 -fsSLO "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar" \
 && curl --retry 6 --retry-all-errors --retry-delay 3 -fsSLO "${MAVEN_CENTRAL}/software/amazon/awssdk/bundle/${AWS_SDK_V2_BUNDLE_VERSION}/bundle-${AWS_SDK_V2_BUNDLE_VERSION}.jar"
ENV SPARK_HOME=/opt/spark

# --- Trino 435 server + cli --------------------------------------------------------------
RUN curl --retry 6 --retry-all-errors --retry-delay 3 -fsSL "${MAVEN_CENTRAL}/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz" \
      -o /tmp/trino.tgz \
 && tar -xzf /tmp/trino.tgz -C /opt && rm /tmp/trino.tgz \
 && curl --retry 6 --retry-all-errors --retry-delay 3 -fsSL "${MAVEN_CENTRAL}/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar" \
      -o /opt/trino-cli.jar
ENV TRINO_HOME=/opt/trino-server-435 TRINO_CLI=/opt/trino-cli.jar

# --- DuckDB 1.3.2 CLI + extensions (native; ubuntu glibc is new enough) ------------------
# DuckDB's per-arch asset is named linux-amd64 / linux-arm64 (NOT -aarch64).
RUN curl --retry 6 --retry-all-errors --retry-delay 3 -fsSL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${TARGETARCH}.zip" \
      -o /tmp/duckdb.zip \
 && unzip -o /tmp/duckdb.zip -d /usr/local/bin && rm /tmp/duckdb.zip \
 && duckdb --version \
 && duckdb -c "INSTALL iceberg; INSTALL httpfs;"

# --- PyIceberg venv (pinned; run_engines.sh reuses it via DRC_PYI_VENV, no runtime download) ---
COPY requirements-pyiceberg-latest.txt /opt/requirements-pyiceberg-latest.txt
RUN python3 -m venv /opt/pyi-venv \
 && /opt/pyi-venv/bin/pip install -q --upgrade pip \
 && /opt/pyi-venv/bin/pip install -q ${PIP_INDEX_URL:+--index-url "$PIP_INDEX_URL"} \
      -r /opt/requirements-pyiceberg-latest.txt \
 && /opt/pyi-venv/bin/python -c "import pyiceberg,pyarrow;print('pyiceberg',pyiceberg.__version__,'pyarrow',pyarrow.__version__)"
ENV DRC_PYI_VENV=/opt/pyi-venv

# run_engines.sh picks these up as defaults.
ENV DRC_JAVA_HOME=/opt/jdk17 SPARK_JAVA_HOME=/opt/jdk17

# No ENTRYPOINT: the CI job / callers invoke `docker run --entrypoint bash IMAGE
# /work/.../run_engines.sh ...`. (If an image DOES set ENTRYPOINT ["/bin/bash"], pass the
# script path WITHOUT a leading "bash", else it becomes `bash bash script`.)
