#!/usr/bin/env bash
#
# Pre-download all packages into this directory from the company-internal
# OBS mirror. Overridable via DOWNLOAD_BASE_URL if the mirror host ever moves.
#
# Usage:
#   bash download.sh                  # download for all supported archs (amd64 + arm64)
#   bash download.sh --arch amd64     # only amd64-specific binaries (minio/mc + ubuntu tar)
#   bash download.sh --arch arm64     # only arm64-specific binaries
#   bash download.sh --arch all       # explicit "both" (default)
#   bash download.sh --no-checksum    # skip SHA256SUMS generation
#
# The OBS bucket layout mirrors the fetch.sh output:
#   ${DOWNLOAD_BASE_URL}/hadoop-3.0.0.tar.gz
#   ${DOWNLOAD_BASE_URL}/linux-amd64/minio
#   ${DOWNLOAD_BASE_URL}/ubuntu-22.04-amd64.tar
#   ...
#
set -euo pipefail

# Resolve our own absolute path BEFORE any cd, so --help can read the header.
SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

ARCH_ARG="all"
GEN_CHECKSUM=1
while [ $# -gt 0 ]; do
  case "$1" in
    --arch)    ARCH_ARG="$2"; shift 2 ;;
    --arch=*)  ARCH_ARG="${1#--arch=}"; shift ;;
    --no-checksum) GEN_CHECKSUM=0; shift ;;
    -h|--help)
      sed -n '2,20p' "$SELF"
      exit 0
      ;;
    *) echo "[error] unknown arg: $1" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$SELF")"

HADOOP_VERSION="${HADOOP_VERSION:-3.0.0}"
HIVE_VERSION="${HIVE_VERSION:-3.0.0}"
SPARK_VERSION="${SPARK_VERSION:-3.3.3}"
HUDI_VERSION="${HUDI_VERSION:-0.13.1}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.1.0}"
POLARIS_VERSION="${POLARIS_VERSION:-1.3.0-incubating}"
AWS_SDK_VERSION="${AWS_SDK_VERSION:-1.11.1026}"
UBUNTU_VERSION="${UBUNTU_VERSION:-22.04}"

# Default mirror: company-internal Huawei Cloud OBS bucket.
# Override by exporting DOWNLOAD_BASE_URL before invoking the script.
DOWNLOAD_BASE_URL="${DOWNLOAD_BASE_URL:-https://hashdata-download.obs.cn-north-4.myhuaweicloud.com/lightning-ci/packages}"

case "$ARCH_ARG" in
  all)              ARCHES=(amd64 arm64) ;;
  amd64|arm64)      ARCHES=("$ARCH_ARG") ;;
  amd64,arm64|arm64,amd64) ARCHES=(amd64 arm64) ;;
  *) echo "[error] --arch must be one of: amd64, arm64, all" >&2; exit 2 ;;
esac

download() {
  local file="$1" url="$2"
  if [ -s "$file" ]; then
    echo "[skip] $file already exists"
    return
  fi
  mkdir -p "$(dirname "$file")"
  echo "[downloading] $file"
  # -C -  resume from .part if present; --retry-all-errors for flaky networks.
  local n=0
  while :; do
    n=$((n+1))
    if curl -fL -C - --retry 5 --retry-delay 5 --retry-all-errors \
            -o "${file}.part" "$url"; then
      mv "${file}.part" "$file"
      echo "[done] $file"
      return
    fi
    if [ $n -ge 20 ]; then
      echo "[fail] $file after $n attempts" >&2
      exit 1
    fi
    echo "[retry] $file attempt $n, resuming in 5s..."
    sleep 5
  done
}

# ---------------------------------------------------------------
# Common (architecture-neutral) artifacts
# ---------------------------------------------------------------
echo "[stage] common artifacts"

download "hadoop-${HADOOP_VERSION}.tar.gz" \
  "${DOWNLOAD_BASE_URL}/hadoop-${HADOOP_VERSION}.tar.gz"

download "apache-hive-${HIVE_VERSION}-bin.tar.gz" \
  "${DOWNLOAD_BASE_URL}/apache-hive-${HIVE_VERSION}-bin.tar.gz"

download "spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
  "${DOWNLOAD_BASE_URL}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"

download "polaris-bin-${POLARIS_VERSION}.tgz" \
  "${DOWNLOAD_BASE_URL}/polaris-bin-${POLARIS_VERSION}.tgz"

download "hudi-spark3-bundle_2.12-${HUDI_VERSION}.jar" \
  "${DOWNLOAD_BASE_URL}/hudi-spark3-bundle_2.12-${HUDI_VERSION}.jar"

download "hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar" \
  "${DOWNLOAD_BASE_URL}/hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar"

download "iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar" \
  "${DOWNLOAD_BASE_URL}/iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar"

download "hadoop-aws-${HADOOP_VERSION}.jar" \
  "${DOWNLOAD_BASE_URL}/hadoop-aws-${HADOOP_VERSION}.jar"

download "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
  "${DOWNLOAD_BASE_URL}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ---------------------------------------------------------------
# Arch-specific artifacts:  MinIO server/client + ubuntu docker image tar
# ---------------------------------------------------------------
for ARCH in "${ARCHES[@]}"; do
  echo "[stage] linux-${ARCH}"
  mkdir -p "linux-${ARCH}"
  ( cd "linux-${ARCH}" && \
      download "minio" "${DOWNLOAD_BASE_URL}/linux-${ARCH}/minio" && \
      chmod +x minio && \
      download "mc" "${DOWNLOAD_BASE_URL}/linux-${ARCH}/mc" && \
      chmod +x mc )

  # Ubuntu base image tar (one per arch), stored flat at top level so
  # build-bundle.sh can `docker load -i ubuntu-22.04-${ARCH}.tar` directly.
  download "ubuntu-${UBUNTU_VERSION}-${ARCH}.tar" \
    "${DOWNLOAD_BASE_URL}/ubuntu-${UBUNTU_VERSION}-${ARCH}.tar"
done

# Backward-compat: legacy Dockerfile looks for /tmp/cache/minio directly.
# Symlink the host's native arch (or amd64 as safe fallback) at top level so
# a plain `docker compose build` on the build host keeps working.
HOST_ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "$HOST_ARCH" in
  amd64|x86_64)   LEGACY_ARCH=amd64 ;;
  arm64|aarch64)  LEGACY_ARCH=arm64 ;;
  *)              LEGACY_ARCH=amd64 ;;
esac
if [ -f "linux-${LEGACY_ARCH}/minio" ]; then
  ln -sf "linux-${LEGACY_ARCH}/minio" minio
  ln -sf "linux-${LEGACY_ARCH}/mc" mc
  echo "[legacy-compat] top-level minio/mc -> linux-${LEGACY_ARCH}"
fi

# ---------------------------------------------------------------
# Integrity manifest
# ---------------------------------------------------------------
if [ "$GEN_CHECKSUM" = "1" ]; then
  echo "[stage] generating SHA256SUMS"
  if command -v sha256sum >/dev/null 2>&1; then SHA_CMD="sha256sum"; else SHA_CMD="shasum -a 256"; fi
  find . -type f \
    \! -name SHA256SUMS \
    \! -name '.gitignore' \
    \! -name 'download.sh' \
    \! -name '*.part' \
    -print0 | LC_ALL=C sort -z | xargs -0 $SHA_CMD > SHA256SUMS
  echo "[done] SHA256SUMS ($(wc -l < SHA256SUMS) entries)"
fi

echo ""
echo "All downloads complete. Next: run ../scripts/build-bundle.sh"
