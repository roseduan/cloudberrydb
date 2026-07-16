# e2e/smoke — four-engine read smoke for the datalake_rest_catalog gateway

`run_engines.sh` drives four **unmodified, standard Iceberg REST clients** — PyIceberg,
Spark, Trino, DuckDB — against a running gateway (default HTTPS `:8443`) as the SELECT-only
`iceberg_reader` role, and asserts each reads the builtin table `sales.orders` back as exactly
the 3 seeded rows `[(1,ada),(2,grace),(3,linus)]`. Per-engine PASS/FAIL/SKIP table; non-zero
exit on any FAIL. It writes no fixtures — run `seed_builtin.sql` first (the CI job does).

## How CI runs it — one consolidated engine-tester image

The `test_datalake_rest_catalog_smoke` GitLab job brings up the cluster/gateway/seed on the
centos-7 runner, then runs the reads inside **one ubuntu "engine-tester" image** that carries
all four clients + their JDK:

```
docker run --rm --network host \
  -e http_proxy -e https_proxy -e no_proxy \
  -v "$CI_PROJECT_DIR:/work:ro" \
  --entrypoint bash \
  "$ENGINE_TESTER_IMAGE" \
  /work/database/contrib/datalake_rest_catalog/e2e/smoke/run_engines.sh pyiceberg spark trino duckdb
```

(Use `--entrypoint bash` and pass the script path directly — if the image sets
`ENTRYPOINT ["/bin/bash"]`, a leading `bash` arg would run `bash bash script`.)

- `--network host` → `localhost:8443`/`:9000` reach the gateway + MinIO on the runner.
- repo **mounted, not baked** → `run_engines.sh` always matches the branch; the image ships only
  the (rarely-changing) runtimes.
- ubuntu 22.04 (glibc 2.35) → PyIceberg needs no `GLIBCXX`/`LD` hack and DuckDB runs natively
  (its extensions need glibc ≥2.28, which the centos-7 base lacks).

**Runner fallback:** if `ENGINE_TESTER_IMAGE` (CI variable) is unset, the job runs on the runner
— PyIceberg passes (always-on gate) and Spark/Trino/DuckDB **SKIP** (no runtime). Green today;
a hard four-engine gate once the image is built + `ENGINE_TESTER_IMAGE` set.

## Engine-tester image — build requirements (for whoever builds/pushes it)

`iceberg-engine-tester.Dockerfile` in this dir is a ready-to-build reference. The image must:

| # | Requirement | Concrete |
|---|---|---|
| 1 | Base | `ubuntu:22.04` (glibc 2.35 + modern libstdc++). **Build both `linux/amd64` and `linux/arm64`** (CI matrix). |
| 2 | JDK | JDK **17** (Trino 435 requires 17; Spark 3.3.4 runs on it), exposed at `/opt/jdk17`. |
| 3 | Python | `python3` ≥3.9 + `venv` + `pip`. |
| 4 | Spark | `spark-3.3.4-bin-hadoop3` at **`/opt/spark`**; add to `$SPARK_HOME/jars/`: `iceberg-spark-runtime-3.3_2.12-1.3.0.jar`, `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`, `bundle-2.20.18.jar` (AWS SDK v2). |
| 5 | Trino | `trino-server-435` at **`/opt/trino-server-435`** (+ its `plugin/`, `lib/`); `trino-cli-435-executable.jar` at **`/opt/trino-cli.jar`**. |
| 6 | DuckDB | `duckdb` CLI **1.3.2** on PATH (asset `duckdb_cli-linux-{amd64,arm64}.zip` — note **`arm64`**, not `aarch64`); pre-install extensions: `duckdb -c "INSTALL iceberg; INSTALL httpfs;"`. |
| 7 | PyIceberg | venv at **`/opt/pyi-venv`** with `requirements-pyiceberg-latest.txt` installed (`pyiceberg[s3fs,pyarrow]==0.11.1`, `pyarrow==20.0.0`). |
| 8 | Tools | `ca-certificates curl unzip openssl`. |
| 9 | Env baked | `DRC_PYI_VENV=/opt/pyi-venv`, `SPARK_HOME=/opt/spark`, `TRINO_HOME=/opt/trino-server-435`, `TRINO_CLI=/opt/trino-cli.jar`, `DRC_JAVA_HOME=/opt/jdk17`, `SPARK_JAVA_HOME=/opt/jdk17`. |

Contract: `docker run --rm --network host -v <repo>:/work:ro --entrypoint bash $IMAGE
/work/<...>/run_engines.sh pyiceberg spark trino duckdb` must print `READ-SMOKE: PASS` against a
gateway on `localhost:8443` seeded with `sales.orders`. Push to the internal registry and set the CI variable
`ENGINE_TESTER_IMAGE` to it. Trino **435** (not 463) is deliberate — 463 doesn't fix the
delete-history matrix and would force JDK23.

## Env knobs (run_engines.sh)

| Env | Default | Purpose |
|---|---|---|
| `DRC_URI` | `https://localhost:8443` | gateway origin, **no** `/v1` suffix |
| `DRC_READER_CRED` | `iceberg_reader:reader_pw` | OAuth2 client-credentials |
| `DRC_S3_ENDPOINT`/`DRC_S3_KEY`/`DRC_S3_SECRET` | `http://localhost:9000` / `minioadmin` | client-supplied storage creds |
| `SPARK_HOME` / `SPARK_JAVA_HOME` | `/opt/spark` / `$DRC_JAVA_HOME` | Spark 3.3.4 |
| `TRINO_HOME` / `TRINO_CLI` | `/opt/trino-server-435` / `/opt/trino-cli.jar` | Trino 435 |
| `DRC_JAVA_HOME` | `/opt/jdk17` | JDK for keytool + Trino |
| `DRC_PYI_VENV` | `$WORK/pyi-venv` | reuse a pre-baked PyIceberg venv (tester sets `/opt/pyi-venv`) |
| `DUCKDB_IMAGE` | *(unset)* | DuckDB sidecar image when no local `duckdb` (runner fallback only) |
| `DRC_LIBSTDCXX_DIR` | `/usr/local/toolchain/lib64` | libstdc++ w/ `GLIBCXX_3.4.29` for PyIceberg on the centos-7 runner fallback |

DuckDB auto-selects: local `duckdb` binary (tester) → `DUCKDB_IMAGE` sidecar → SKIP.

Validated 2026-07-13 on lightning-382 (`pyiceberg spark trino` → PASS); also used to confirm the
file-scoped position-delete fix makes Trino read a delete-history table correctly (3 rows, was 4).
