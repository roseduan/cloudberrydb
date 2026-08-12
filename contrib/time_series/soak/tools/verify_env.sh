#!/bin/bash
# tools/verify_env.sh — Verify the CBDB cluster meets all soak prerequisites.
#
# Run this BEFORE `bash soak.sh` to catch environment issues early.
# Each prerequisite is printed with ✓ or ✗; the script exits 0 if all pass,
# non-zero otherwise.  The soak framework itself does NOT provision the
# environment — that's the operator's responsibility.  This script is just
# a fast sanity check.
#
# Usage:
#   bash tools/verify_env.sh
#   SOAK_HOST=cdw SOAK_PORT=7000 SOAK_USER=gpadmin bash tools/verify_env.sh
#
# Environment: all connection / path values come from conf/soak_params.sh
# (override any of them with SOAK_X=y as usual).  SOAK_VERIFY_DB
# (default postgres) is only used for the connectivity check.
#
# Exit codes:
#   0  all prerequisites met — ready to run soak.sh
#   1  one or more prerequisites failed — fix the issues listed

set -u

# Verify against the SAME parameter set the driver will actually use —
# a check that validates a different TSBS path / port than the run uses
# is worse than no check (this exact drift shipped once: verify_env
# defaulted to /home/gpadmin/timedb/tsbs/bin while the runs used
# /home/gpadmin/tsbs_bin).
SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/conf/soak_params.sh"

HOST="$SOAK_HOST"
PORT="$SOAK_PORT"
USER="$SOAK_USER"
DB="${SOAK_VERIFY_DB:-postgres}"
TSBS_BIN="$SOAK_TSBS_BIN"

FAIL=0
pass() { printf '  ✓ %s\n' "$*"; }
fail() { printf '  ✗ %s\n' "$*" >&2; FAIL=$((FAIL+1)); }

echo "=== soak prerequisites check ==="
echo "  target  : $USER@$HOST:$PORT/$DB"
echo "  tsbs_bin: $TSBS_BIN"
echo

# Helper: run psql with timeout to avoid hanging if cluster is unresponsive.
PSQL_OPTS="-h $HOST -p $PORT -U $USER -d $DB -At -X -v ON_ERROR_STOP=1"
psql_q() {
  PGCONNECT_TIMEOUT="${SOAK_VERIFY_TIMEOUT:-5}" psql $PSQL_OPTS -c "$1" 2>/dev/null
}

# ─────────────────────────────────────────────────────────────────
# 1. psql connectivity
# ─────────────────────────────────────────────────────────────────
if psql_q 'SELECT 1' >/dev/null; then
  pass "psql connects to $HOST:$PORT as $USER (db=$DB)"
else
  fail "psql cannot connect — check SOAK_HOST/SOAK_PORT/SOAK_USER + pg_hba.conf"
  echo
  echo "=== ABORTING — no point checking other items without a connection ==="
  exit 1
fi

# ─────────────────────────────────────────────────────────────────
# 2. shared_preload_libraries
# ─────────────────────────────────────────────────────────────────
PRELOAD=$(psql_q "SHOW shared_preload_libraries")
if [[ "$PRELOAD" == *time_series* ]]; then
  pass "shared_preload_libraries contains 'time_series' (current: $PRELOAD)"
else
  fail "shared_preload_libraries='$PRELOAD' — must include 'time_series'"
  echo "      Fix: gpconfig -c shared_preload_libraries -v 'time_series' && gpstop -ar"
fi

# ─────────────────────────────────────────────────────────────────
# 2b. running processes actually loaded the INSTALLED time_series.so
# ─────────────────────────────────────────────────────────────────
# Because time_series is in shared_preload_libraries, the postmaster maps
# the .so at startup and every backend inherits that mapping through
# fork().  Replacing the file on disk therefore changes NOTHING until the
# cluster is restarted -- a whole soak round was once burned running the
# previous build against a freshly-copied .so.  Comparing the inode of
# the installed file with what the live processes have mapped catches it
# in seconds.
SO_PATH=$(psql_q "SELECT setting || '/time_series.so' FROM pg_settings WHERE name = 'dynamic_library_path'" 2>/dev/null)
# dynamic_library_path is usually '$libdir'; resolve it via pg_config.
if [[ -z "$SO_PATH" || "$SO_PATH" == *'$libdir'* ]]; then
  SO_PATH="$(pg_config --pkglibdir 2>/dev/null)/time_series.so"
fi
if [[ -r "$SO_PATH" ]]; then
  DISK_INODE=$(stat -c %i "$SO_PATH" 2>/dev/null)
  # Any live postgres process will do; they all inherit the same mapping.
  MAPPED_INODE=""
  for p in $(pgrep -x postgres 2>/dev/null | head -20); do
    MAPPED_INODE=$(awk '/time_series\.so/ {print $5; exit}' "/proc/$p/maps" 2>/dev/null)
    [[ -n "$MAPPED_INODE" ]] && break
  done
  if [[ -z "$MAPPED_INODE" ]]; then
    pass "time_series.so inode check skipped (no readable /proc/*/maps)"
  elif [[ "$DISK_INODE" == "$MAPPED_INODE" ]]; then
    pass "running processes have the installed time_series.so mapped (inode $DISK_INODE)"
  else
    fail "STALE .so: installed inode=$DISK_INODE but processes mapped inode=$MAPPED_INODE"
    echo "      The cluster is running an OLD build.  Fix: gpstop -ar"
  fi
else
  pass "time_series.so inode check skipped (path not readable: $SO_PATH)"
fi

# ─────────────────────────────────────────────────────────────────
# 3. time_series extension available
# ─────────────────────────────────────────────────────────────────
if psql_q "
  CREATE EXTENSION IF NOT EXISTS time_series;
  DROP EXTENSION time_series CASCADE;
" >/dev/null 2>&1; then
  pass "time_series extension is installable + droppable"
else
  fail "time_series extension cannot be created — check time_series.so + .control are installed"
fi

# ─────────────────────────────────────────────────────────────────
# 4. gp_inject_fault extension available (required by chaos § 6)
# ─────────────────────────────────────────────────────────────────
if psql_q "
  CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
  DROP EXTENSION gp_inject_fault CASCADE;
" >/dev/null 2>&1; then
  pass "gp_inject_fault extension is installable (chaos requires it)"
else
  fail "gp_inject_fault extension cannot be created"
  echo "      Fix: cd gpcontrib/gp_inject_fault && make USE_PGXS=1 install"
  echo "           CBDB must be configured with --enable-cassert"
fi

# ─────────────────────────────────────────────────────────────────
# 5. PAX access method is registered (auto-compression § A.4 depends on it)
# ─────────────────────────────────────────────────────────────────
if [[ "$(psql_q "SELECT 1 FROM pg_am WHERE amname='pax'")" == "1" ]]; then
  pass "PAX access method is registered (auto-compression backing storage)"
else
  fail "PAX access method NOT in pg_am — rebuild CBDB with --enable-pax"
fi

# ─────────────────────────────────────────────────────────────────
# 6. CREATEDB privilege (driver needs to DROP/CREATE soak_test_a, _b)
# ─────────────────────────────────────────────────────────────────
if [[ "$(psql_q "SELECT rolcreatedb FROM pg_roles WHERE rolname='$USER'")" == "t" ]]; then
  pass "user $USER has CREATEDB privilege"
else
  fail "user $USER lacks CREATEDB — driver creates soak_test_a / soak_test_b"
  echo "      Fix: ALTER ROLE $USER CREATEDB;"
fi

# ─────────────────────────────────────────────────────────────────
# 7. All segments are up
# ─────────────────────────────────────────────────────────────────
BAD=$(psql_q "SELECT count(*) FROM gp_segment_configuration WHERE status <> 'u'")
if [[ "$BAD" == "0" ]]; then
  pass "all segments status='u' in gp_segment_configuration"
else
  fail "$BAD segment(s) NOT up — check gp_segment_configuration before running soak"
fi

# ─────────────────────────────────────────────────────────────────
# 8. TSBS binaries (required by setup/02 + workload/)
# ─────────────────────────────────────────────────────────────────
for bin in tsbs_generate_data tsbs_load_timescaledb; do
  path="$TSBS_BIN/$bin"
  if [[ ! -x "$path" ]]; then
    fail "TSBS binary missing: $path"
    echo "      Fix: build TSBS for THIS host's OS/arch:"
    echo "           cd /path/to/tsbs && make $bin"
    echo "           export SOAK_TSBS_BIN=/path/to/tsbs/bin"
    continue
  fi
  # Try to execute --help.  We don't care about its exit code or
  # output; we only care that the process started (no Exec format error).
  # Exec format error (wrong CPU/OS) causes bash to set $? to 126.
  "$path" --help >/dev/null 2>&1
  rc=$?
  if [[ $rc -ne 126 && $rc -ne 127 ]]; then
    pass "TSBS binary OK: $bin (at $path)"
  else
    fail "TSBS binary at $path won't execute (rc=$rc) — wrong CPU/OS?"
    file "$path" 2>/dev/null | sed 's/^/      /'
  fi
done

# ─────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────
echo
if [[ "$FAIL" -eq 0 ]]; then
  echo "=== ALL CHECKS PASSED ==="
  echo "Ready to run: bash soak.sh --preset=smoke (5min) / bash soak.sh (12h default)"
  exit 0
else
  echo "=== $FAIL CHECK(S) FAILED ==="

# ── judge ↔ CSV wiring lint ──────────────────────────────────────────
# Regression guard for the 2026-07-23 path bug: after collectors moved
# their CSVs into $RESULTS/data/, two judges kept reading the results
# ROOT — silently reporting "collector has not fired yet" while the
# panic early-stop gate sat inert for a full 12h run.  Any judge
# referencing a CSV outside data/ fails preflight.
WIRING_BAD=$(grep -nE '\$(RESULTS|results)/[a-z_]+\.csv' \
      "$SOAK_DIR"/judge/verdicts/*.sh "$SOAK_DIR"/judge/lib/*.sh 2>/dev/null \
    | grep -v '/data/' || true)
if [[ -n "$WIRING_BAD" ]]; then
  fail "judge reads CSV outside \$RESULTS/data/ (path-bug regression): $WIRING_BAD"
else
  pass "judge CSV wiring (all reads under data/)"
fi

  echo "Fix the issues above before running soak.sh."
  exit 1
fi
