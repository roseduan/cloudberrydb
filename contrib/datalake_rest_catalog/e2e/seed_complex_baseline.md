# scale.* builtin-iceberg fixture baselines (HashData-direct, authoritative)

Captured 2026-07-08 on container `lightning-382` (port 7000, database `postgres`,
session `TimeZone = Asia/Shanghai`) after running `e2e/seed_complex.sql`. These are
the values B2 (external-engine validation) should assert against when reading the
same tables through Trino/Spark/etc. via the REST catalog gateway.

Checksum method: `sum(hashtext(t::text))::bigint` over the whole row (`t` is the
table alias), computed directly in HashData with `psql`. This is sensitive to
column order and to Postgres's own text formatting of each type (numeric trailing
zeros, timestamptz normalized to session TimeZone, etc.) — if an external engine
formats values differently, comparing per-column sums/min/max/counts (also given
below) is more robust than reproducing this exact hashtext value byte-for-byte.

## scale.wide (20 rows)

```
row_count = 20
checksum_hashtext_sum = -3075353334
null_rows (c_text IS NULL) = 1        -- row id=4, all columns NULL except id
sum(c_int)             = 2147678647
sum(c_bigint)          = 9223372231854775807
sum(c_numeric_38_10)   = 10000000000000000000000000199.6499999999
sum(c_numeric_20_6)    = 123456854.123457
```

Rows: id 1-5 are hand-crafted edge cases (typical, negative, zero/empty-string,
all-NULL, and max-precision/unicode); id 6-20 are generated via
`generate_series(6,20)`. Row id=4 has every column NULL except `id`.

## scale.parted (120 rows, NOT partitioned — see note below)

```
row_count            = 120
checksum_hashtext_sum = -3951517158
sum(amount)           = 2104.20
min(event_date)       = 2025-01-02
max(event_date)       = 2025-12-07
distinct_categories   = 3   (electronics, grocery, apparel)
distinct_regions      = 3   (us-east, us-west, eu-central)
```

**Iceberg partitioning is not supported on this platform** (confirmed at the
grammar level — `CreateLakeTableStmt` in `src/backend/parser/gram.y` has no
`PARTITION BY` production for `CREATE ICEBERG TABLE`). `scale.parted` is built as
an ordinary (unpartitioned) iceberg table; the data is deliberately spread across
12 months / 3 categories / 3 regions so B2 can still compare range-scan and
group-by results across engines, but this table does **not** exercise Iceberg
partition-spec/partition-pruning behavior — that scope item is unsupported and out
of scope for B1/B2 until the platform adds partition-spec support.

## scale.snaps (3 rows after 6 DML operations / snapshots)

```
row_count             = 3
checksum_hashtext_sum = -2484134675
```

Final state (session TimeZone = Asia/Shanghai, so timestamptz below renders as
+08; stored values are UTC per the literals in seed_complex.sql):

| id | v      | updated_at (local +08)      |
|----|--------|------------------------------|
| 1  | a-v2   | 2026-01-02 08:00:00+08       |
| 2  | b-v2   | 2026-01-02 08:00:00+08       |
| 4  | d-v2   | 2026-01-04 08:00:00+08       |

Operation sequence executed (each produced a new snapshot, confirmed via
`mc find local/warehouse ... /metadata/` showing 6 `snap-*.avro` + corresponding
`*.metadata.json` files during capability probing on an equivalent throwaway
table): INSERT(1,2,3) -> UPDATE(1,2) -> DELETE(3) -> INSERT(4,5) -> UPDATE(4) ->
DELETE(5). Rows 3 and 5 are expected to be permanently gone; rows 1/2/4 carry the
`-v2` suffix from their UPDATE.

## scale.big (1,000,000 rows)

```
row_count             = 1000000
checksum_hashtext_sum = -20332369210
sum(amt)               = 3649270073.00
min(created_at)        = 2020-01-01
max(created_at)        = 2029-12-28
min(id) / max(id)      = 1 / 1000000
```

Generated via `INSERT ... SELECT ... FROM generate_series(1, 1000000)`. Load took
under 2 seconds on this cluster (200k-row timed benchmark: 0.65s; full 1M run not
separately timed post-fix but the seed script's whole run, including this insert,
completed in well under 1s of wall time per `\timing`-observed pattern from the
200k benchmark — see task-PR-B1-report.md for exact numbers). This table is
readable by `iceberg_reader` so the e2e large-table scan runs as the normal
reader role.

## scale.secret (3 rows)

Tiny table with intentionally **no** `iceberg_reader` GRANT (see below), kept as
the B3 RBAC negative test case: the gateway must exclude it from
`iceberg_visible_tables()` for `iceberg_reader` and answer 404 on `load_table`.

## Grants (verified via `\dp scale.*`)

| table         | iceberg_reader | notes                                    |
|---------------|----------------|------------------------------------------|
| scale.wide    | SELECT (r)     | readable                                 |
| scale.parted  | SELECT (r)     | readable                                 |
| scale.snaps   | SELECT (r)     | readable                                 |
| scale.big     | SELECT (r)     | readable — used by the 1M-row e2e scan   |
| scale.secret  | none           | intentional — B3 RBAC negative test case |

`no_access` role has no privileges on schema `scale` (schema-level `REVOKE ALL`
applied; matches the `sales` schema convention in `seed_builtin.sql`).
