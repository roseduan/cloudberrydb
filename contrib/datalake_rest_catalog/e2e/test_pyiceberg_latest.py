#!/usr/bin/env python3
"""
Permanent regression test (issue #382 production readiness): does the LATEST PyIceberg
release (0.11.1, pinned in requirements-pyiceberg-latest.txt) correctly read through the
datalake_rest_catalog gateway over HTTPS, using genuine REST-capability negotiation (no
_supported_endpoints workaround)?

This promotes the one-off validation done in /tmp/reader_snaps_newver.py and
/tmp/reader_snaps_noworkaround.py (2026-07-08 session) to a durable, repeatable e2e test:

  * PyIceberg >=0.11 added strict endpoint-capability negotiation: it parses the
    "endpoints" field from GET /v1/config into a set of Capability path-templates like
    "GET /v1/{prefix}/namespaces", and refuses to call any endpoint whose template isn't in
    that set. The gateway used to advertise endpoint strings WITHOUT the "{prefix}" segment
    (e.g. "GET /v1/namespaces"), which never matched PyIceberg's literal constants, so
    cat._supported_endpoints ended up empty and every call raised NotImplementedError.
  * Commit 3e5391adcb7 ("Fix: advertise canonical {prefix}-templated endpoints in /v1/config
    for strict REST clients") fixed the gateway to emit the canonical templated form.
  * This test proves that fix by connecting WITHOUT forcing _supported_endpoints and
    confirming both a simple read and, critically, correct merge-on-read (position-delete)
    behavior on a table that has had UPDATE/DELETE applied -- the scenario that would
    silently return stale/raw data if delete-file application were broken on a new client.

Assertions (each is a hard regression gate -- non-zero exit on any failure):
  1. sales.orders            -- simple read: 3 rows, ids [1,2,3]
  2. scale.snaps             -- merge-on-read/delete-history: exactly 3 post-delete rows
                                 (ids 1,2,4), NOT the 8 raw pre-delete rows
  3. scale.big               -- large metadata read: count == 1,000,000
  4. scale.wide (optional)   -- NULL row (id=4) and max-precision/unicode row (id=5) survive
  5. genuine capability negotiation -- cat._supported_endpoints is populated FROM THE REAL
     /v1/config response (no manual override anywhere in this file) and is non-empty
  6. scale.secret            -- B3 RBAC negative: not granted to iceberg_reader, so the
     gateway must hide it (load_table raises NoSuchTableError)

Usage:
  DRC_CA_BUNDLE=/tmp/drc-pyi-latest.pem python3 test_pyiceberg_latest.py
Exit 0 + "== PYICEBERG LATEST E2E PASSED ==" on success; non-zero + traceback/mismatch on
failure. Intended to be invoked via run_pyiceberg_latest.sh, which manages the venv and the
cert export.
"""
import os
import sys

import pyiceberg
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

# Same doubling gotcha as reader_real.py / reader_real_https.py: PyIceberg's
# RestCatalog.url() always appends "v1/" itself, so `uri` must be the bare gateway origin
# without a "/v1" suffix.
GATEWAY_URI = os.environ.get("DRC_URI", "https://localhost:8443")
CA_BUNDLE = os.environ.get("DRC_CA_BUNDLE", "/tmp/drc-pyi-latest.pem")

MIN_VERSION = (0, 11)


def _version_tuple(v: str):
    parts = []
    for p in v.split(".")[:2]:
        digits = "".join(c for c in p if c.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def connect() -> RestCatalog:
    if not os.path.isfile(CA_BUNDLE):
        raise SystemExit(
            f"CA bundle not found at {CA_BUNDLE!r}; export the live gateway cert first "
            "(see run_pyiceberg_latest.sh) and set DRC_CA_BUNDLE."
        )
    cat = RestCatalog(
        "hd",
        uri=GATEWAY_URI,
        credential=os.environ.get("DRC_READER_CRED", "iceberg_reader:reader_pw"),
        **{
            "ssl": {"cabundle": CA_BUNDLE},
            # The client brings its OWN storage credentials -- the gateway vends none and, since
            # the kernel-side metadata change, holds none. Env-overridable because the MinIO
            # root credentials differ per dev container (a container seeded with admin/admin12345
            # made this test fail with S3 ACCESS_DENIED while the gateway itself was fine).
            # Must match the credentials of the volume the tables actually live on:
            #   SELECT umoptions FROM pg_user_mapping ...
            "s3.endpoint": os.environ.get("DRC_S3_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.environ.get("DRC_S3_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.environ.get("DRC_S3_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
    # NO WORKAROUND: unlike the earlier one-off /tmp/reader_snaps_newver.py, this file never
    # touches cat._supported_endpoints. If the gateway's /v1/config regresses to non-{prefix}
    # endpoint strings, PyIceberg's real negotiation will raise NotImplementedError on the
    # very first catalog call below, failing this test loudly.
    return cat


def check_capability_negotiation(cat: RestCatalog):
    endpoints = getattr(cat, "_supported_endpoints", None)
    print("[test] negotiated _supported_endpoints (from real /v1/config, no override):",
          sorted(str(e) for e in endpoints) if endpoints else endpoints)
    assert endpoints, (
        "cat._supported_endpoints is empty -- PyIceberg's strict endpoint-capability "
        "negotiation did not accept the gateway's advertised endpoints (regression in "
        "the {prefix}-templated /v1/config fix, issue #382)"
    )


def check_simple_read(cat: RestCatalog):
    print("[test] namespaces:", cat.list_namespaces())
    print("[test] tables in sales:", cat.list_tables("sales"))

    tbl = cat.load_table("sales.orders")
    rows = tbl.scan().to_arrow()
    pairs = sorted(zip(rows.column("id").to_pylist(), rows.column("name").to_pylist()))
    print(f"[test] sales.orders: {rows.num_rows} rows -> {pairs}")

    expected = [(1, "ada"), (2, "grace"), (3, "linus")]
    assert rows.num_rows == 3 and pairs == expected, (
        f"sales.orders mismatch: got {rows.num_rows} rows {pairs}, expected {expected}"
    )


def check_merge_on_read(cat: RestCatalog):
    """scale.snaps: 3 rows after INSERT(1,2,3)->UPDATE(1,2)->DELETE(3)->INSERT(4,5)->
    UPDATE(4)->DELETE(5). If position-delete application is broken, a client may instead
    see stale/raw data (e.g. all 8 rows ever written, or pre-UPDATE values)."""
    tbl = cat.load_table("scale.snaps")
    print("[test] scale.snaps current snapshot:", tbl.current_snapshot())

    rows = tbl.scan().to_arrow()
    pairs = sorted(zip(rows.column("id").to_pylist(), rows.column("v").to_pylist()))
    print(f"[test] scale.snaps: {rows.num_rows} rows -> {pairs}")

    expected = [(1, "a-v2"), (2, "b-v2"), (4, "d-v2")]
    assert rows.num_rows == 3 and pairs == expected, (
        f"scale.snaps mismatch: got {rows.num_rows} rows {pairs}, expected {expected} "
        "(merge-on-read / position-delete regression)"
    )


def check_large_table(cat: RestCatalog):
    tbl = cat.load_table("scale.big")
    rows = tbl.scan(selected_fields=("id",)).to_arrow()
    print(f"[test] scale.big: {rows.num_rows} rows")
    assert rows.num_rows == 1_000_000, (
        f"scale.big mismatch: got {rows.num_rows} rows, expected 1,000,000"
    )


def check_rbac_negative(cat: RestCatalog):
    """B3 RBAC negative: scale.secret has no SELECT grant for iceberg_reader, so
    iceberg_visible_tables() must exclude it and the gateway must answer 404, which
    PyIceberg surfaces as NoSuchTableError. A successful load here means the gateway
    leaked an unauthorized table."""
    try:
        cat.load_table("scale.secret")
    except NoSuchTableError:
        print("[test] scale.secret: hidden from iceberg_reader as expected (404/NoSuchTableError)")
        return
    raise AssertionError(
        "RBAC bypass: iceberg_reader loaded scale.secret despite having no SELECT grant"
    )


def check_wide_types(cat: RestCatalog):
    """Optional spot check: NULL row (id=4) and unicode/max-precision row (id=5) survive
    a round trip through the gateway with the latest PyIceberg."""
    tbl = cat.load_table("scale.wide")
    rows = tbl.scan(row_filter="id in (4, 5)").to_arrow()
    by_id = {rid: i for i, rid in enumerate(rows.column("id").to_pylist())}
    print(f"[test] scale.wide id in (4,5): {rows.num_rows} rows, ids={sorted(by_id)}")

    assert 4 in by_id and 5 in by_id, f"expected rows id=4 and id=5, got ids={sorted(by_id)}"

    i4 = by_id[4]
    c_text_4 = rows.column("c_text")[i4].as_py()
    assert c_text_4 is None, f"scale.wide id=4 c_text expected NULL, got {c_text_4!r}"

    i5 = by_id[5]
    c_text_5 = rows.column("c_text")[i5].as_py()
    assert c_text_5 is not None and "unicode" in c_text_5 and "\U0001F389" in c_text_5, (
        f"scale.wide id=5 c_text expected unicode/emoji text, got {c_text_5!r}"
    )
    c_num_5 = rows.column("c_numeric_38_10")[i5].as_py()
    print(f"[test] scale.wide id=5: c_text={c_text_5!r} c_numeric_38_10={c_num_5}")
    assert c_num_5 is not None and str(c_num_5).startswith("9999999999999999999999999999"), (
        f"scale.wide id=5 c_numeric_38_10 expected max-precision value, got {c_num_5!r}"
    )


def main():
    print(f"[test] pyiceberg version: {pyiceberg.__version__}")
    assert _version_tuple(pyiceberg.__version__) >= MIN_VERSION, (
        f"expected pyiceberg >= {'.'.join(map(str, MIN_VERSION))} (see "
        f"requirements-pyiceberg-latest.txt), got {pyiceberg.__version__}"
    )

    cat = connect()

    check_capability_negotiation(cat)
    check_simple_read(cat)
    check_merge_on_read(cat)
    check_large_table(cat)
    check_wide_types(cat)
    check_rbac_negative(cat)

    print("== PYICEBERG LATEST E2E PASSED == "
          "(simple read + merge-on-read/delete-history + 1M-row scan + wide-type spot check "
          "+ RBAC negative, "
          f"pyiceberg {pyiceberg.__version__}, genuine capability negotiation)")
    sys.exit(0)


if __name__ == "__main__":
    main()
