# datalake_rest_catalog — CI smoke test design (issue #382)

**Date:** 2026-07-12
**Branch:** `feature/datalake_rest_catalog-smoke` (cut from `feature/datalake_rest_catalog`)
**Status:** implemented; pending real-pipeline validation

## Goal

Add a GitLab CI smoke test that proves the datalake_rest_catalog REST gateway works
end-to-end: an unmodified external Iceberg client (latest PyIceberg, over the gateway's
default HTTPS) can read a **builtin** Iceberg table that HashData wrote internally.

Modeled on the existing `test_datalake_smoke` job (`.gitlab-ci.yml`).

## Scope (agreed)

- **Form:** one new job in `.gitlab-ci.yml` — `test_datalake_rest_catalog_smoke`. No new
  committed scripts; a thin wrapper over the committed `contrib/datalake_rest_catalog`
  e2e/security/bin/deploy assets.
- **Depth:** minimal smoke — read `sales.orders` and assert exactly 3 rows
  `ids=[1,2,3]` / `[(1,ada),(2,grace),(3,linus)]`. No merge-on-read / 1M-row / RBAC
  coverage (those stay in `test_pyiceberg_latest.py`).
- **Client/transport:** latest PyIceberg (0.11, pinned in
  `requirements-pyiceberg-latest.txt`) over HTTPS on `:8443`.

## Job flow

Loopback-only, so the gateway's default auto-generated self-signed cert (SAN
`localhost`,`127.0.0.1`) is valid — no CA-signed cert needed.

1. **MinIO** — dedicated container from the mirrored image
   `docker.hashdata.dev/minio/minio:latest` on `localhost:9000`
   (`minioadmin/minioadmin`), bucket `warehouse` created via
   `docker.hashdata.dev/minio/mc:latest`. Matches the hardcoded values in
   `e2e/seed_builtin.sql`.
2. **Gateway jar** — ensure `$(pg_config --pkglibdir)/java/datalake-rest-catalog-1.0.0.jar`
   exists; build+install via `make -C contrib/datalake_rest_catalog install` (JDK17 +
   Maven on PATH) if the build cache didn't already carry it.
3. **Cluster** — `make create-demo-cluster` (gpdemo, single node). The actual coordinator
   port is read from `gpdemo-env.sh` and injected into the gateway via the `PG_JDBCURL`
   env override (no hard 7000 assumption).
4. **Hardening** — order is dependency-driven: (a) create `iceberg_reader` first, because
   `create_authenticator.sql` GRANTs it to the authenticator; (b)
   `security/create_authenticator.sql` (dedicated NOINHERIT/NOSUPERUSER
   `iceberg_authenticator`); (c) prepend `security/pg_hba.rest_catalog.fragment` to the
   coordinator `pg_hba.conf` (first-match-wins) + `gpstop -u`. Roles are created with
   `PGOPTIONS=-c password_encryption=scram-sha-256` so the scram HBA lines can authenticate.
   `rest_catalog_authz.sql` (bundled in step 6) RAISEs unless `iceberg_authenticator` exists,
   which is why the authenticator is created here (before the seed). Required: AuthPreflight
   refuses to start under a trust HBA or a superuser authenticator. All psql runs use
   `ON_ERROR_STOP=1` so a broken setup/seed fails the job instead of a misleading green read.
5. **Start gateway** — `bin/datalake_rest_catalog_ctl.sh start`
   (`DRC_JAVA_HOME=/opt/jdk17`, S3 creds + `PG_AUTHENTICATOR_PASSWORD` via env).
6. **Write builtin table** — `psql -v authz_sql=security/rest_catalog_authz.sql -f
   e2e/seed_builtin.sql` → `sales.orders` (3 rows) + authz layer.
7. **External read (assertion)** — Python ≥3.9 venv, `pip install
   requirements-pyiceberg-latest.txt`, export the live self-signed cert to PEM, run the
   committed `e2e/reader_real_https.py` (`DRC_URI`, `DRC_CA_BUNDLE`). It asserts 3 rows
   `ids=[1,2,3]` and exits 0 with `== REAL E2E (HTTPS) PASSED ==`.
8. **Teardown** (`after_script`) — stop gateway, `destroy-demo-cluster`, remove MinIO.

## Reuse (no new logic)

`reader_real_https.py`, `seed_builtin.sql`, `security/{create_authenticator.sql,
pg_hba.rest_catalog.fragment,rest_catalog_authz.sql}`, `bin/datalake_rest_catalog_ctl.sh`,
`requirements-pyiceberg-latest.txt` — all already committed and validated.

## Pre-validation (done before writing the job)

Against the live `lightning-382` container (which already runs cluster+gateway+MinIO+seed):
- Minimal PyIceberg 0.11.1 read of `sales.orders` over HTTPS:8443 → `[(1,ada),(2,grace),
  (3,linus)]`. ✅
- The committed `reader_real_https.py` under the 0.11 venv → `== REAL E2E (HTTPS) PASSED ==`,
  exit 0. ✅
- `.gitlab-ci.yml` passes the GitLab CI-lint API (`valid: true`).

## Residual risk (only resolvable on the real runner)

- MinIO/mc image pull + host-network `mc` reachability on the CI runner.
- `make create-demo-cluster` succeeding in the CI job context (vs. the umbrella `make
  test-*` wrappers).
- Whether the pipeline build already installs the gateway jar / builtin-Iceberg support
  (mitigated by the step-2 build-if-missing guard).

These are validated by running the pipeline and iterating, as with the other #382 rounds.

---

## Update 2026-07-14 — extended to four engines via a consolidated tester image (option B)

The smoke now drives **four** unmodified Iceberg REST clients: PyIceberg 0.11.1, Spark 3.3.4,
Trino 435, DuckDB 1.3.2 — each reads `sales.orders` back as the 3 seeded rows.

- **Driver:** committed `e2e/smoke/run_engines.sh` (+ `README.md`,
  `iceberg-engine-tester.Dockerfile`). Exports the gateway's live self-signed cert, builds a JKS
  truststore for the JVM engines, runs each engine as `iceberg_reader` over HTTPS `:8443`.
  Per-engine PASS/FAIL/SKIP; non-zero exit on any FAIL.
- **One consolidated tester image (option B):** rather than bloat the centos-7 build image with
  Spark/Trino (and fight its old glibc/libstdc++), CI runs the reads inside a single ubuntu
  `iceberg-engine-tester` image carrying all four clients + JDK17. The job still builds
  cluster+gateway+seed on the runner, then `docker run --rm --network host -v $CI_PROJECT_DIR:/work
  $ENGINE_TESTER_IMAGE bash /work/.../run_engines.sh …` (host network → localhost:8443/:9000 reach
  the gateway/MinIO; repo mounted so the script matches the branch). ubuntu glibc 2.35 → PyIceberg
  needs no GLIBCXX/LD hack and DuckDB runs natively. **No product-code and no build-image change.**
  The image is built + pushed by infra/devops; the Dockerfile + a requirements table are in
  `e2e/smoke/README.md`. CI variable `ENGINE_TESTER_IMAGE` points the job at the pushed image.
- **Job change:** the read step moved out of the gpadmin body into the root main script (after
  cluster+gateway+seed are up) — `docker run` needs root; the reads only talk to the gateway over
  localhost. The body now just brings up + seeds and returns.
- **Fail-soft / runner fallback:** unset `ENGINE_TESTER_IMAGE` → the job runs `run_engines.sh` on
  the runner: PyIceberg passes (always-on gate), Spark/Trino/DuckDB **SKIP** (no runtime). Green
  today; a hard four-engine gate once the image is built + the variable set.
- **Local validation (2026-07-13, lightning-382):** `run_engines.sh pyiceberg spark trino` → all
  PASS; DuckDB read recipe validated separately. Also used to validate the file-scoped
  position-delete write fix (Trino delete-history read 4→3 rows).

> Depth stays a minimal read gate (`sales.orders`, 3 rows); MOR / RBAC / large-table coverage
> remains in `test_pyiceberg_latest.py`.
