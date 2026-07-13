# e2e — external-engine validation for the datalake_rest_catalog gateway

Real, non-mocked integration checks: an unmodified external Iceberg client (PyIceberg) reads
through the gateway's REST catalog against the real backend (builtin Iceberg tables, real
pg_lake_table metadata, real Parquet data in MinIO). Run inside a dev container with the
gateway and the HashData cluster up (e.g. `lightning-382`).

## Fixtures

* `seed_builtin.sql` — builtin catalog/volume setup + `sales.orders` (3 rows) + test roles
  `iceberg_reader` (SELECT-only) / `no_access` (none).
* `seed_complex.sql` — `scale` schema: `scale.wide` (edge-case column types incl. NULL row
  and a unicode/max-precision row), `scale.parted`, `scale.snaps` (merge-on-read: 3 rows
  after a sequence of INSERT/UPDATE/DELETE), `scale.big` (1,000,000 rows), `scale.secret`
  (3 rows, no `iceberg_reader` grant — B3 RBAC negative case).
* `seed_complex_baseline.md` — authoritative expected values for everything in
  `seed_complex.sql`, captured directly from HashData (source of truth for e2e assertions).

Run both seeds once against the target cluster's `postgres` database before running the
scripts below (idempotent — safe to re-run). `seed_builtin.sql` **requires** the `authz_sql`
psql variable — it includes the authz layer at the end and aborts loudly if the variable is
missing (a silent skip would leave `pg_ext_aux.pg_iceberg_metadata` PUBLIC-readable):

```bash
cd contrib/datalake_rest_catalog/e2e
psql -d postgres -v authz_sql="$PWD/../security/rest_catalog_authz.sql" -f seed_builtin.sql
psql -d postgres -f seed_complex.sql
```

## Scripts

| Script | Client | Transport | Purpose |
|---|---|---|---|
| `reader_real.py` / `run_real.sh` | PyIceberg 0.7.1 | HTTP :8181 | original Task 7 proof: builtin `sales.orders` read |
| `reader_real_https.py` | PyIceberg 0.7.1 | HTTPS :8443 | HTTPS-default proof (Task A), same read |
| `test_pyiceberg_latest.py` / `run_pyiceberg_latest.sh` | **latest PyIceberg (0.11.1, pinned)** | HTTPS :8443 | permanent regression test — see below |

### `run_pyiceberg_latest.sh` — latest-PyIceberg regression test

Promotes a one-off validation (2026-07-08 session, `/tmp/reader_snaps_newver.py` and
`/tmp/reader_snaps_noworkaround.py`) into a durable, repeatable check. PyIceberg >=0.11 added
strict REST endpoint-capability negotiation (parses `GET /v1/config`'s `endpoints` field into
`{prefix}`-templated path constants and refuses calls outside that set); this test proves the
gateway's `/v1/config` fix (advertising canonical `{prefix}`-templated endpoint strings, see
commit `3e5391adcb7`) works with a real, unmodified newest-generation client — **no
`_supported_endpoints` client-side workaround anywhere in the test**.

It asserts, against real seeded data:

1. **Simple read** — `sales.orders` → 3 rows, `[(1,'ada'),(2,'grace'),(3,'linus')]`.
2. **Merge-on-read / delete-history** — `scale.snaps` → exactly the 3 post-delete rows
   (ids 1, 2, 4; values `a-v2`/`b-v2`/`d-v2`), not stale/raw pre-delete data. This is the key
   regression: a client that can't apply position-deletes would return 8 rows or wrong values.
3. **Large table** — `scale.big` → `count == 1,000,000` (validates large-metadata reads).
4. **Wide types (spot check)** — `scale.wide` id=4 (all-NULL row) and id=5
   (unicode/emoji text + max-precision `numeric(38,10)`) both round-trip correctly.
5. **Genuine capability negotiation** — `cat._supported_endpoints` is populated from the
   real `/v1/config` response; the test never overrides it.
6. **RBAC negative (B3)** — `scale.secret` has no `iceberg_reader` grant; the gateway must
   hide it (`load_table` → 404 / `NoSuchTableError`).

Run:

```bash
cd contrib/datalake_rest_catalog/e2e
./run_pyiceberg_latest.sh
```

The script is idempotent: it creates (or reuses) a venv at `/tmp/drc-pyi-latest-venv` using a
Python >= 3.9 interpreter (PyIceberg 0.11 dropped 3.8 support; the container's system
`python3` is 3.8, so the script probes for `python3.10`/`python3.9`/etc — override with
`DRC_PYTHON` if needed), installs the pin from `requirements-pyiceberg-latest.txt` (proxy via
`DRC_PIP_PROXY`; unset by default — set it when PyPI is only reachable via proxy), exports the gateway's *live* self-signed
dev cert straight from the TLS socket (the dev keystore path is a random temp file
regenerated per restart, so this is always re-exported rather than cached), and runs
`test_pyiceberg_latest.py`. Exits non-zero on any setup or assertion failure.

Override with env vars: `DRC_URI` (default `https://localhost:8443`), `DRC_CA_BUNDLE`
(default `/tmp/drc-pyi-latest.pem`), `DRC_PYI_LATEST_VENV`, `DRC_PYTHON`, `DRC_PIP_PROXY`.

To bump the pinned PyIceberg version later: update `requirements-pyiceberg-latest.txt`,
re-run the script (it re-installs into the same venv), and update this note if behavior
changes.
