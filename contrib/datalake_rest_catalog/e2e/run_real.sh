#!/usr/bin/env bash
set -euo pipefail
# Run INSIDE lightning-382. This is the HTTP-mode proof: it assumes the gateway is up in
# plaintext mode on :8181 (the HTTPS-default proof is run via reader_real_https.py -> :8443).
# Assumes sales.orders is seeded. Override the interpreter with DRC_PYTHON (defaults to the
# dev-container venv at /tmp/pyi) and the gateway origin with DRC_URI (see reader_real.py).
"${DRC_PYTHON:-/tmp/pyi/bin/python}" "$(dirname "$0")/reader_real.py"
