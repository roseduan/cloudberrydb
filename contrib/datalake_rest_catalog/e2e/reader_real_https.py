#!/usr/bin/env python3
"""
Task PR-A2 end-to-end HTTPS proof for issue #382.

HTTPS variant of reader_real.py: validates that PyIceberg (an external, unmodified Iceberg
engine) can still read the builtin sales.orders table through the datalake_rest_catalog
gateway now that HTTPS is the default transport (Task A: server.https.enabled=true by
default, auto-generated self-signed cert when server.https.keystore.path is blank).

Since this is a dev self-signed certificate (not CA-signed), the client must be told to
trust it explicitly. PyIceberg's RestCatalog supports this via the `ssl.cabundle` property
(a CA bundle / cert file path passed straight to the underlying `requests.Session.verify`) --
that's the documented trust-a-specific-cert mechanism for this PyIceberg version (0.7.1);
there is no separate "skip verification" toggle for the REST endpoint itself, so pointing
ssl.cabundle at the gateway's exported self-signed cert PEM is the equivalent of a dev
skip-verify override without disabling verification of the wire format entirely.

The cert PEM must be exported ahead of time from the gateway's auto-generated keystore, e.g.:
  KS=$(ls -t /tmp/drc-selfsigned-*.p12 | head -1)
  $JAVA_HOME/bin/keytool -exportcert -alias drc -keystore "$KS" -storepass changeit \
      -storetype PKCS12 -rfc -file /tmp/drc-selfsigned.pem

Usage: DRC_CA_BUNDLE=/tmp/drc-selfsigned.pem python reader_real_https.py
Exit 0 + "== REAL E2E (HTTPS) PASSED ==" on success.
"""
import os
import sys

from pyiceberg.catalog.rest import RestCatalog

# Same doubling gotcha as reader_real.py: PyIceberg's RestCatalog.url() always appends
# "v1/" itself, so `uri` must be the bare gateway origin without a "/v1" suffix.
GATEWAY_URI = os.environ.get("DRC_URI", "https://localhost:8443")
CA_BUNDLE = os.environ.get("DRC_CA_BUNDLE", "/tmp/drc-selfsigned.pem")


def main():
    cat = RestCatalog(
        "hd",
        uri=GATEWAY_URI,
        credential=os.environ.get("DRC_READER_CRED", "iceberg_reader:reader_pw"),
        **{
            "ssl": {"cabundle": CA_BUNDLE},
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",       # client brings its own storage credentials
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )

    print("[reader] namespaces:", cat.list_namespaces())
    print("[reader] tables:", cat.list_tables("sales"))

    tbl = cat.load_table("sales.orders")
    print("[reader] loaded table metadata; current snapshot:", tbl.current_snapshot())

    rows = tbl.scan().to_arrow()
    ids = sorted(rows.column("id").to_pylist())
    names = rows.column("name").to_pylist()
    print(f"[reader] scanned {rows.num_rows} rows: ids={ids} names={names}")

    assert rows.num_rows == 3 and ids == [1, 2, 3], "expected 3 rows ids=[1,2,3]"
    print("== REAL E2E (HTTPS) PASSED ==")
    sys.exit(0)


if __name__ == "__main__":
    main()
