#!/usr/bin/env bash
# Re-runs the issue #382 PoC against a running gateway. Non-zero if any bypass survives.
#
# Covers:
#  [1] P0-1/P0-2 auth bypass (Task 1 verify() hardening + Task 2 dedicated
#      iceberg_authenticator/scram pg_hba) — gpadmin + wrong password must never
#      yield an OAuth token from the gateway.
#  [2] Item B direct-DB metadata leak (Task 3 SECURITY DEFINER gate on
#      pg_ext_aux) — a *legitimately authenticated*, non-superuser reader must
#      still be denied a direct SELECT against pg_ext_aux.pg_iceberg_metadata.
set -uo pipefail
# Default to the gateway's own default transport: HTTPS on 8443 (issue #382
# production-readiness). Defaulting to http://:8181 (plaintext) got connection-refused
# against a default HTTPS-only gateway, and the error-suppression below then misreported
# that as "OK: refused" -- a false pass. Pass an explicit URL to override.
B="${1:-https://127.0.0.1:8443}"
fail=0

echo "[1] gpadmin + wrong password must NOT yield a token"
# -k: the gateway's default deployment is HTTPS-only with an auto-generated self-signed cert
# (issue #382 production-readiness/scale work). Without -k, curl's TLS verification failure on
# an https $B silently produces empty output here -- which this script's own error-suppression
# (2>/dev/null on the python parse) then reports as "OK: refused" even though the request never
# reached the gateway at all. That is a false pass, not a real bypass-check result.
tok=$(curl -sk -X POST "$B/v1/oauth2/tokens" -d "credential=gpadmin:WRONG-$RANDOM" \
      | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
if [ -n "$tok" ]; then echo "  FAIL: got a token for gpadmin"; fail=1; else echo "  OK: refused"; fi

# [2] uses iceberg_reader/reader_pw rather than the brief's original "no_access" role.
# Task 2's pg_hba fragment only added scram-sha-256 entries for iceberg_authenticator
# and iceberg_reader; "no_access" has no pg_hba line at all, so `psql -U no_access`
# would fail at connection time with "no pg_hba.conf entry for host ..." rather than
# reaching the permission check inside the database -- a false negative that would
# not actually exercise the pg_ext_aux lockdown from Task 3.
#
# iceberg_reader is the stronger, correct invariant to check here: it is a real,
# legitimately-authenticated non-superuser role that CAN log in and DOES have SELECT
# on sales.orders, yet it was never granted USAGE on schema pg_ext_aux nor EXECUTE on
# pg_ext_aux.iceberg_visible_tables (see rest_catalog_authz.sql). So a direct SELECT
# against pg_ext_aux.pg_iceberg_metadata as iceberg_reader must still be denied.
echo "[2] iceberg_reader direct-DB read of pg_ext_aux must be denied"
# Coordinator port: DRC_PGPORT wins, then the ambient PGPORT, then the lightning-382
# dev-cluster default (7000) for backward compatibility. Same override pattern for the
# reader password (DRC_READER_PW), whose seed default is reader_pw.
out=$(PGPASSWORD="${DRC_READER_PW:-reader_pw}" psql -h 127.0.0.1 -p "${DRC_PGPORT:-${PGPORT:-7000}}" -U iceberg_reader postgres \
      -Atc "SELECT count(*) FROM pg_ext_aux.pg_iceberg_metadata" 2>&1)
case "$out" in *"permission denied"*) echo "  OK: denied" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

# [3]-[7] Task 3: pg_ext_aux.iceberg_load_metadata SECURITY DEFINER gate.
#
# These use SET ROLE from a gpadmin unix-socket session rather than a TCP login as the
# target role. pg_hba.conf has no TCP entries for iceberg_authenticator's callees under
# test here beyond what Task 2 already wired (iceberg_authenticator, iceberg_reader), and
# "no_access" has no pg_hba entry at all -- a TCP `psql -U no_access` would fail at
# connection time ("no pg_hba.conf entry for host ...") rather than exercising the
# permission check inside the database, which would be a false negative. ACL checks key
# off the *current* role after SET ROLE, so a gpadmin session that SET ROLEs to an
# unprivileged role sees exactly the same GRANT/REVOKE-driven denials or successes as a
# real login by that role would -- these are genuine authorization checks, not simulated.
PGP="${DRC_PGPORT:-${PGPORT:-7000}}"

echo "[3] iceberg_load_metadata: no_access must not execute (permission denied)"
out=$(psql -X -p "$PGP" -d postgres -Atc \
      "SET ROLE no_access; SELECT * FROM pg_ext_aux.iceberg_load_metadata('no_access','sales','orders');" 2>&1)
case "$out" in *"permission denied"*) echo "  OK: denied" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

echo "[4] iceberg_load_metadata: underlying reader pg_iceberg_load_metadata_json_local must stay revoked from PUBLIC"
out=$(psql -X -p "$PGP" -d postgres -Atc \
      "SET ROLE no_access; SELECT * FROM pg_catalog.pg_iceberg_load_metadata_json_local(0);" 2>&1)
case "$out" in *"permission denied"*) echo "  OK: denied" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

echo "[5] iceberg_load_metadata: iceberg_authenticator on behalf of iceberg_reader must return the seeded table's metadata"
out=$(psql -X -p "$PGP" -d postgres -Atc \
      "SET ROLE iceberg_authenticator; SELECT (length(metadata_json) > 0) AND (metadata_location LIKE '%.metadata.json') FROM pg_ext_aux.iceberg_load_metadata('iceberg_reader','sales','orders');" 2>&1)
case "$out" in t) echo "  OK: returned metadata for visible table" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

echo "[6] iceberg_load_metadata: a superuser p_role must yield zero rows"
out=$(psql -X -p "$PGP" -d postgres -Atc \
      "SET ROLE iceberg_authenticator; SELECT count(*) FROM pg_ext_aux.iceberg_load_metadata('gpadmin','sales','orders');" 2>&1)
case "$out" in 0) echo "  OK: superuser p_role rejected" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

echo "[7] iceberg_load_metadata: iceberg_reader itself must not have EXECUTE (no grant)"
out=$(psql -X -p "$PGP" -d postgres -Atc \
      "SET ROLE iceberg_reader; SELECT * FROM pg_ext_aux.iceberg_load_metadata('iceberg_reader','sales','orders');" 2>&1)
case "$out" in *"permission denied"*) echo "  OK: denied" ;; *) echo "  FAIL: $out"; fail=1 ;; esac

[ "$fail" -eq 0 ] && echo "ALL BYPASS CHECKS PASSED" || echo "BYPASS DETECTED"
exit "$fail"
