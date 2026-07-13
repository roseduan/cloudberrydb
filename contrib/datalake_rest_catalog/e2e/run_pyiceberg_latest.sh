#!/usr/bin/env bash
# Run INSIDE lightning-382 (or an equivalent dev container with the gateway up on HTTPS).
#
# Sets up (or reuses) a dedicated venv for the LATEST PyIceberg (pinned in
# requirements-pyiceberg-latest.txt), exports the gateway's live self-signed dev cert, and
# runs test_pyiceberg_latest.py. Exits non-zero if setup or the test fails.
#
# The system python3 in this container is 3.8 (rh-python38), but PyIceberg >=0.11 requires
# Python >= 3.9, so this script looks for a newer interpreter (python3.10 is present at
# /usr/local/python-3.10.16/bin in the lightning-382 image) rather than assuming `python3`.
#
# Idempotent: if the venv already exists it is reused; `pip install` is re-run each time
# (a no-op if requirements are already satisfied) so the pin in requirements-pyiceberg-latest.txt
# is always honored even if the venv predates a bump to that file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DRC_URI="${DRC_URI:-https://localhost:8443}"
VENV_DIR="${DRC_PYI_LATEST_VENV:-/tmp/drc-pyi-latest-venv}"
CERT_PATH="${DRC_CA_BUNDLE:-/tmp/drc-pyi-latest.pem}"
# Optional pip proxy: unset by default so direct-internet environments (CI, external
# contributors) work out of the box; set DRC_PIP_PROXY when pypi is only reachable
# through a proxy (e.g. some corporate dev containers).
PIP_PROXY="${DRC_PIP_PROXY:-}"
REQ_FILE="$SCRIPT_DIR/requirements-pyiceberg-latest.txt"

# --- 1. pick a Python interpreter >= 3.9 (PyIceberg 0.11 dropped 3.8) ---------------------
find_python() {
  local candidates=(
    "${DRC_PYTHON:-}"
    /usr/local/python-3.10.16/bin/python3.10
    python3.12 python3.11 python3.10 python3.9
  )
  for c in "${candidates[@]}"; do
    [ -z "$c" ] && continue
    if command -v "$c" >/dev/null 2>&1; then
      local ver; ver="$("$c" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || true)"
      if [ -n "$ver" ]; then
        local maj="${ver%%.*}" min="${ver##*.}"
        if [ "$maj" -gt 3 ] || { [ "$maj" -eq 3 ] && [ "$min" -ge 9 ]; }; then
          command -v "$c"
          return 0
        fi
      fi
    fi
  done
  return 1
}

PYTHON_BIN="$(find_python)" || {
  echo "ERROR: no Python >= 3.9 found (PyIceberg 0.11+ requires it); set DRC_PYTHON to point" \
       "at one" >&2
  exit 1
}
echo "[run] using interpreter: $PYTHON_BIN ($("$PYTHON_BIN" -V 2>&1))"

# --- 2. create (or reuse) the venv ---------------------------------------------------------
if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "[run] creating venv at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  echo "[run] reusing existing venv at $VENV_DIR"
fi

if [ -n "$PIP_PROXY" ]; then
  echo "[run] installing $REQ_FILE (proxy=$PIP_PROXY)"
  "$VENV_DIR/bin/pip" install -q --proxy "$PIP_PROXY" -r "$REQ_FILE"
else
  echo "[run] installing $REQ_FILE (no proxy)"
  "$VENV_DIR/bin/pip" install -q -r "$REQ_FILE"
fi
"$VENV_DIR/bin/python" -c "import pyiceberg; print('[run] pyiceberg installed:', pyiceberg.__version__)"

# --- 3. export the gateway's live self-signed dev cert -------------------------------------
# The dev keystore backing this cert is a random temp file regenerated per gateway restart
# (server.https.keystore.path blank -> auto-generated self-signed cert), so we always export
# fresh from the live TLS socket rather than pointing at a fixed keystore path.
host_port="${DRC_URI#*://}"
host="${host_port%%:*}"
port="${host_port##*:}"
echo "[run] exporting live TLS cert from $host:$port -> $CERT_PATH"
if ! openssl s_client -connect "$host:$port" -servername "$host" </dev/null 2>/dev/null \
    | openssl x509 -out "$CERT_PATH"; then
  echo "ERROR: failed to export TLS cert from $DRC_URI -- is the gateway up? Try:" \
       "bin/datalake_rest_catalog_ctl.sh start" >&2
  exit 1
fi
[ -s "$CERT_PATH" ] || { echo "ERROR: exported cert $CERT_PATH is empty" >&2; exit 1; }

# --- 4. run the test -------------------------------------------------------------------------
echo "[run] running test_pyiceberg_latest.py against $DRC_URI"
DRC_URI="$DRC_URI" DRC_CA_BUNDLE="$CERT_PATH" "$VENV_DIR/bin/python" "$SCRIPT_DIR/test_pyiceberg_latest.py"
