# datalake_rest_catalog — production deployment & operations guide

Consolidates the operational decisions from issue #382 sub-project C (production
readiness). Read this alongside `security/README.md` (the sub-project C privilege-escalation/authz hardening
runbook — do that first; this doc assumes it's done) before exposing the gateway
outside a single trusted dev host.

## C1 — Startup / process management

Current form is a manual control script, **not** an OS service:

```
contrib/datalake_rest_catalog/bin/datalake_rest_catalog_ctl.sh {start|stop|status|restart}
```

- Installed to `pg_config --pkglibdir`'s sibling `bin/` via `make install` (or run in-tree).
- `start` resolves the jar (`$DRC_JAR`, or `$(pg_config --pkglibdir)/java/datalake-rest-catalog-1.0.0.jar`),
  requires `DRC_JAVA_HOME` (default `/opt/jdk17`, any JDK >= 11), sources `deploy/datalake_rest_catalog.env`
  for `S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY`, backgrounds the JVM with `nohup`, writes a pidfile, and
  polls `/v1/config` (HTTPS-aware — see below) for up to 30s before declaring success.
- `stop` sends SIGTERM, waits up to 20s, then SIGKILL.
- `status` reports pid + a live `/v1/config` health check.
- Logs go to `$DRC_RUNDIR/datalake_rest_catalog.log` (`$DRC_RUNDIR` defaults to `/tmp` — **override
  this in production**; `/tmp` is not durable/rotated and may be world-writable).

**Autostart is a deferred decision — not implemented.** There is no `gpstart` hook and no systemd
unit in this batch. Until one exists, the recommended production expectation is:

- Treat the gateway as an operator-managed sidecar process alongside the coordinator: start it
  explicitly after `gpstart`/cluster bring-up, stop it before `gpstop`/maintenance windows.
- Wrap `datalake_rest_catalog_ctl.sh {start|stop}` in whatever process supervisor the deployment
  already uses (systemd unit calling the ctl script, a cluster-manager hook, etc.) rather than
  relying on manual invocation for anything but dev/test.
- Set `DRC_RUNDIR` to a persistent, log-rotated directory and `DRC_ENV`/`PG_AUTHENTICATOR_PASSWORD`
  to come from the deployment's actual secret store, not the example env file.
- A first-class autostart integration (gpstart hook or systemd unit) is tracked as future work; do
  not assume one exists when writing runbooks.

## HTTPS (production — cross-reference A2 finding)

HTTPS is **on by default** (`server.https.enabled=true`, port 8443) because the OAuth2
`client_secret` is the end-user's PostgreSQL password — plaintext HTTP would leak it on the wire.

- With no keystore configured, the gateway **auto-generates a throwaway self-signed PKCS12 cert**
  (`CN=localhost`, SAN `dns:localhost, ip:127.0.0.1` only) and logs a loud `WARN`. This is fine for
  a single-host dev loopback but **breaks for every other client**: the A2 validation run found
  DuckDB reaching the gateway via a docker-bridge IP failed hostname verification even after
  trusting the CA (`SSL: no alternative certificate subject name matches target host name
  '172.17.0.2'`), and any client connecting by real hostname or a non-loopback IP hits the same
  failure. **The self-signed fallback is loopback-shaped, not "any client on the network" shaped.**
- **Production MUST set `server.https.keystore.path`** (PKCS12/JKS) to a **CA-signed certificate
  whose SAN covers the actual hostname/IP(s) clients use to reach the gateway** — not just
  `localhost`. Set `server.https.keystore.password` alongside it.
- **Production MUST set `server.https.required=true`** so the gateway fails closed (refuses to
  start) instead of silently falling back to the self-signed dev cert if the keystore path is ever
  blank — never let a misconfiguration silently downgrade to a cert no real client can verify.
- Set `server.https.enabled=false` only if the gateway sits entirely behind a TLS-terminating
  reverse proxy that itself enforces the same SAN/hostname correctness (see the LB caveat in C3 —
  a proxy in front of this gateway has its own client-IP implications for rate limiting).

## C2 — JWT secret

- **Production MUST set `jwt.hmacSecretBase64`** to a persistent, `>=32`-byte (256-bit) base64
  secret, e.g. `openssl rand -base64 32`. Leave it empty and the gateway (a) logs a `WARN` at
  startup and (b) signs tokens with an **ephemeral key generated on boot** — every outstanding
  token is invalidated on every restart, and two gateway processes (e.g. behind a naive LB) would
  each mint tokens the other can't verify.
- **Production MUST set `jwt.requireConfiguredSecret=true`.** This flips `AuthPreflight`'s JWT
  check to fail-closed: startup aborts (`System.exit(1)`) if `jwt.hmacSecretBase64` is blank,
  instead of the default (`false`) dev-friendly fallback to the ephemeral key.
- **Rotation**: change `jwt.hmacSecretBase64` and restart the gateway. There is no hot rotation /
  `kid` / keyset support in this batch — outstanding tokens signed with the old secret simply stop
  verifying immediately on restart, but any token issued before rotation and still within its TTL
  window before the restart remains valid until it naturally expires or the process restarts,
  whichever comes first. In practice a restart invalidates everything at once (no in-flight grace
  period), so plan rotations for low-traffic windows.
- Token lifetime is `jwt.ttlSeconds` (default `900`s / 15 min) — this bounds how long a
  credential-stuffed or leaked token stays usable and how long clients can keep using a token
  minted before a secret rotation.

## C3 — Rate limiting + load balancer caveat

OAuth token-endpoint (`POST /v1/oauth2/tokens`) rate limiting slows password brute-forcing:

| Key | Default | Meaning |
|---|---|---|
| `oauth.rateLimit.maxAttempts` | 20 | Max token requests per key within the trailing window (failures count). |
| `oauth.rateLimit.windowSeconds` | 60 | Sliding window size, in seconds. |
| `oauth.rateLimit.maxKeys` | 100000 | Hard cap on distinct tracked keys (LRU-evicted beyond this) — bounds attacker-controlled memory growth from `client_id` churn. |

Tuning guidance: lower `maxAttempts`/`windowSeconds` for stricter brute-force resistance if your
client population is small and known; raise `maxKeys` only if you have many thousands of
legitimate distinct `client_id`+IP pairs and are seeing false evictions (evictions are
access-recency based, not window-state based — see `RateLimiter` javadoc for the accepted
tradeoff).

### ⚠️ REQUIRED prerequisite: direct client connections, or the limiter breaks

The rate limiter's key is `client_id + "|" + request.getRemoteAddr()` — **the raw TCP socket
remote address**. `X-Forwarded-For` is **NOT currently consulted**.

**If the gateway is fronted by a load balancer or reverse proxy, every client's `remoteAddr` is
the proxy's IP.** Consequences:

- All clients behind the proxy share **one** rate-limit bucket per `client_id` — one noisy or
  malicious client can exhaust the budget and get everyone else 429'd.
- An attacker gets no extra advantage from IP rotation (good), but legitimate multi-tenant use
  through a shared proxy is effectively unusable at the default `maxAttempts=20`.

**Required for production**: either

1. Deploy the gateway so clients connect **directly** (no proxy/LB in front of it) — the simplest,
   currently-supported-correctly option, or
2. Treat XFF-aware keying as a **required follow-up** before fronting the gateway with a proxy.

**Security note for that follow-up**: if XFF support is added, it must only be trusted when the
immediate peer is a known/allowlisted proxy IP. Otherwise an attacker can set an arbitrary
`X-Forwarded-For` header on a direct request and get a **fresh rate-limit key on every single
request**, bypassing the limiter entirely — worse than not having XFF support at all.

## C4 — Observability: security events to monitor

The following events are logged via SLF4J and should be shipped to your log aggregator with
alerting on volume/rate, not just presence (most fire under normal operation too):

| Event | Class | Level | Log line (paraphrased) |
|---|---|---|---|
| OAuth auth failure (`invalid_client`) | `OAuthTokenEndpoint` | WARN | `OAuth token request denied (invalid_client): client_id=... remoteAddr=...` |
| OAuth rate-limit exceeded (429) | `OAuthTokenEndpoint` | WARN | `OAuth token request rate-limited: client_id=... remoteAddr=...` |
| Missing bearer token (401) | `GatewayFilter` | WARN | `401 missing bearer token: METHOD path from remoteAddr` |
| Invalid/expired JWT (401) | `GatewayFilter` | WARN | `401 JWT validation failed: METHOD path from remoteAddr: <reason>` |
| RBAC denial / not-found on `loadTable` | `PgIcebergCatalog` | INFO | `loadTable denied or not found: role=... table=...` — deliberately indistinguishable from a genuine 404 to the *client* (anti-enumeration, §6.2), but recorded server-side for audit. Fires on ordinary not-found lookups too, so treat as an audit trail, not an alarm by itself. |
| AuthPreflight startup failure (auth or JWT) | `AuthPreflight` | ERROR + process exit(1) | `STARTUP ABORTED — auth preflight failed: <reason>` / `STARTUP ABORTED — JWT preflight failed: <reason>` |
| AuthPreflight startup success | `AuthPreflight` | INFO | `auth preflight OK ...` / `JWT preflight OK ...` |
| Ephemeral JWT key in use (no persistent secret) | `AuthPreflight` | WARN | `jwt.hmacSecretBase64 is empty — using an EPHEMERAL JWT key ...` |
| Self-signed HTTPS cert fallback in use | `GatewayServer` | WARN | `server.https.enabled=true but server.https.keystore.path is empty — serving HTTPS with an auto-generated ... certificate ... THIS IS NOT SUITABLE FOR PRODUCTION` |

Recommended alerting:
- Alert on **any** `STARTUP ABORTED` (process won't come up) and on the self-signed-cert / ephemeral-JWT
  `WARN`s appearing in a production log stream at all (they should never fire once configured correctly).
- Alert on **spikes** in `invalid_client` / rate-limited / 401 lines from a single `client_id` or
  IP — that's the brute-force / credential-stuffing signal these logs exist to surface.
- `loadTable denied or not found` is high-volume by design (normal 404s included); use it for
  forensic lookback after an incident, not a standing alert.

## Known limitations / engine compatibility matrix

### Platform limitations (not gateway bugs — apply to any consumer of builtin Iceberg tables)

- **No Iceberg partitioning.** `CREATE ICEBERG TABLE` has no `PARTITION BY`/transform grammar and
  no partition-spec evolution at the platform level. Any table served through this gateway is
  necessarily unpartitioned; do not expect partition-pruning behavior from external engines.
- **Nested/uuid types unsupported.** `array`/`struct`/`map`/`jsonb` and `uuid` columns fail at
  write time (parquet type-mismatch errors) even though `CREATE` accepts them with a misleading
  "using string" warning. Only flat scalar types work end-to-end (int family, explicit-precision
  `numeric(p,s)`, text/varchar/char, date/timestamp/timestamptz, boolean, float4/float8, bytea).
- **`time` and `interval` are broken on read** — separate platform bugs, not gateway issues:
  `time` columns silently return corrupted values (no error); `interval` columns crash the read
  path (`unsupported column type oid: 1186`). Do not use either type in tables served by this
  gateway until fixed upstream.

### Engine read compatibility (builtin Iceberg tables, validated via this gateway)

| Table shape | PyIceberg 0.7.1 | Spark 3.3.4 | Trino 435 | DuckDB 1.3.2 |
|---|---|---|---|---|
| Append-only (incl. 1M-row scale test) | **PASS** | **PASS** | **PASS** | **PASS** |
| UPDATE/DELETE history (merge-on-read position deletes) | **FAILS silently** — does not apply position-deletes at all; returns every physical row version (e.g. 8 rows instead of 3) | **PASS** — exact match | **FAILS** — applies most but not all position-deletes; returns 1 stale pre-update row alongside the current one | **PASS** — exact match |

- **Append-only tables are safe with all four engines**, including at scale (1,000,000-row table
  validated with PyIceberg and Trino, both sub-second).
- **Tables with any UPDATE/DELETE history are only safe to read with Spark or DuckDB.** Trino and
  PyIceberg both return incorrect data for such tables — this was cross-validated as a *client*
  defect in each case (Spark and DuckDB read the identical REST-served metadata correctly), not a
  gateway or metadata defect:
  - **PyIceberg 0.7.1** does not apply Iceberg merge-on-read position-delete files during
    `scan().to_arrow()` at all — a known capability gap in this client version.
  - **Trino 435**'s Iceberg connector applies most but not all position-deletes for a table with
    multiple snapshots (missed 1 of 5 deletes in testing, reproducible across retries) — likely a
    delete-file-to-data-file matching or path/URI-scheme normalization bug in Trino's connector.
- **Recommendation**: file a follow-up issue against Trino's Iceberg connector for the missed
  position-delete (real, reproducible defect); until resolved, do not present tables with
  UPDATE/DELETE history to Trino consumers of this gateway, and note the PyIceberg 0.7.1
  limitation in client-facing docs (may be resolved in a newer PyIceberg release — not verified
  here).
