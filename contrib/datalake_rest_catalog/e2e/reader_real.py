#!/usr/bin/env python3
"""
Task 7 end-to-end proof for issue #382.

Connects PyIceberg (an external, unmodified Iceberg engine) to the gateway's REST catalog
against the REAL backend (Task 1-6): builtin iceberg table sales.orders, backed by real
pg_lake_table metadata and real Parquet data files in MinIO. The gateway does NOT vend
storage credentials (Task 6 security fix) -- the client brings its own MinIO creds via the
s3.* FileIO properties, exactly like a real external consumer would.

Auth: OAuth client-credentials exchange where client_id:client_secret == PG role:password
(iceberg_reader:reader_pw), a real least-privilege role with USAGE on sales + SELECT on
sales.orders only.

Usage: python reader_real.py
Exit 0 + "== REAL E2E PASSED ==" on success.
"""
import os
import sys

from pyiceberg.catalog.rest import RestCatalog

# NOTE: PyIceberg's RestCatalog.url() always appends "v1/" itself (see
# pyiceberg/catalog/rest.py Catalog.url), so the configured `uri` must be the bare
# gateway origin WITHOUT a "/v1" suffix -- otherwise every request 404s/405s against
# a doubled "/v1/v1/..." path. (The task brief's uri="http://localhost:8181/v1" example
# does not match this PyIceberg version's behavior; verified against pyiceberg 0.7.1.)
# Override with DRC_URI (same convention as reader_real_https.py); default is the
# plaintext dev gateway on :8181 -- for a default HTTPS gateway use reader_real_https.py.
GATEWAY_URI = os.environ.get("DRC_URI", "http://localhost:8181")


def main():
    cat = RestCatalog(
        "hd",
        uri=GATEWAY_URI,
        credential=os.environ.get("DRC_READER_CRED", "iceberg_reader:reader_pw"),
        **{
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
    print("== REAL E2E PASSED ==")
    sys.exit(0)


if __name__ == "__main__":
    main()
