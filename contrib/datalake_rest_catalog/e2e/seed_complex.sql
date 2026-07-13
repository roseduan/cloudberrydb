-- Idempotent seed: complex builtin-iceberg table fixtures (scale schema), used for later
-- consistency checks between external engines (Trino/Spark/...) and this cluster's direct reads
-- (issue #382 production-readiness checklist item B1).
--
-- Depends on the objects already created by e2e/seed_builtin.sql: hd_catalog_server / hd_catalog /
-- hd_volume_server / hd_volume / iceberg_reader / no_access. This file only adds tables under the
-- scale schema.
--
-- Platform capability matrix, verified 2026-07-08 on the lightning-382 container (details in
-- .superpowers/sdd/task-PR-B1-report.md):
--   * Scalar types: int2/int4/int8, numeric(p,s) (precision/scale MUST be given explicitly --
--     bare numeric errors on write with "The precision of numeric in foreign tables with parquet
--     format should be specified explicitly.", so it is unusable), text, varchar(n), char(n), date,
--     timestamp, timestamptz, boolean, float4/float8, bytea -- all write + read back correctly.
--   * uuid: CREATE warns "using string", INSERT fails outright with Type Mismatch -- unusable.
--   * time: writes/reads back but the value is corrupted (read value differs from written, e.g.
--     12:34:56 comes back as 78186:18:01.743368) -- treated as unusable, not used here.
--   * interval: writes, but SELECT crashes at execution ("unsupported column type oid: 1186")
--     -- unusable, not used here.
--   * Nested types: array (text[]), composite/struct (CREATE TYPE), and jsonb all pass
--     CREATE ICEBERG TABLE (only a "using string" warning), but INSERT fails with
--     "Datalake foreign table Type Mismatch ... not supported data type".
--     I.e. this platform currently supports no nested Iceberg column types for real read/write;
--     only scalar types are usable.
--   * Partitioning: the CREATE ICEBERG TABLE grammar (CreateLakeTableStmt in
--     src/backend/parser/gram.y) has no PARTITION BY / partition-transform
--     (identity/bucket/truncate/year/month/day/hour) productions at all -- only a column list +
--     FOREIGN CATALOG/VOLUME + options + a fixed DISTRIBUTED RANDOMLY.
--     I.e. this platform currently does not support partitioned Iceberg tables. scale.parted is
--     therefore a non-partitioned table; it only spreads event_date/event_ts across months in the
--     data so engines can do relative range/partition-pruning-style comparisons, not to validate
--     real partitioning.
--   * DML: UPDATE / DELETE both work on builtin iceberg tables, and every INSERT/UPDATE/DELETE
--     produces a new snapshot under MinIO's metadata/ directory (snap-*.avro + *.metadata.json);
--     6 consecutive DMLs were observed to produce 6 snapshots / 5 data files (on a probe table;
--     scale.snaps below reproduces the same sequence).
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

SET iceberg_default_catalog = 'hd_catalog';
SET iceberg_default_volume  = 'hd_volume';

CREATE SCHEMA IF NOT EXISTS scale;

-- ---------------------------------------------------------------------------
-- scale.wide: covers every scalar type verified usable on this platform + boundary values
-- (NULL/negative/zero/long text/high-precision decimal). No uuid/time/interval/nested types
-- (all confirmed unusable, see the header).
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS scale.wide (
  id               bigint,
  c_smallint       int2,
  c_int            int4,
  c_bigint         bigint,
  c_numeric_38_10  numeric(38,10),
  c_numeric_20_6   numeric(20,6),
  c_text           text,
  c_varchar        varchar(200),
  c_char           char(10),
  c_date           date,
  c_timestamp      timestamp,
  c_timestamptz    timestamptz,
  c_bool           boolean,
  c_float4         float4,
  c_float8         float8,
  c_bytea          bytea
);

DO $$ BEGIN
  IF (SELECT count(*) FROM scale.wide) = 0 THEN
    INSERT INTO scale.wide VALUES
      -- 1: typical normal values
      (1, 7, 12345, 9223372036854775, 12345.6789012345, 3.14159265358979,
       'hello world', 'typical varchar', 'char10',
       date '2026-01-15', timestamp '2026-01-15 10:30:00', timestamptz '2026-01-15 10:30:00+00',
       true, 1.5, 2.718281828, E'\\xDEADBEEF'),
      -- 2: negative values
      (2, -7, -12345, -9223372036854775, -12345.6789012345, -3.14159265358979,
       'negative row', 'neg varchar', 'char10',
       date '2020-02-29', timestamp '2020-02-29 23:59:59', timestamptz '2020-02-29 23:59:59+08',
       false, -1.5, -2.718281828, E'\\x00'),
      -- 3: zero / empty-string boundary
      (3, 0, 0, 0, 0.0, 0,
       '', '', '',
       date '1970-01-01', timestamp '1970-01-01 00:00:00', timestamptz '1970-01-01 00:00:00+00',
       false, 0, 0, E'\\x'),
      -- 4: everything except id is NULL
      (4, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, NULL,
       NULL, NULL, NULL,
       NULL, NULL, NULL, NULL),
      -- 5: high-precision decimal boundary (38,10 full) + unicode + long text
      (5, 32767, 2147483647, 9223372036854775807, 9999999999999999999999999999.9999999999, 123456789.123456789,
       'unicode test emoji 🎉', repeat('x', 190), 'z',
       date '2999-12-31', timestamp '2999-12-31 23:59:59.999999', timestamptz '2999-12-31 23:59:59.999999+00',
       true, 3.4e37, 1.7e308, E'\\xFFFFFFFFFFFFFFFF')
    ;
    -- 6..20: generate_series bulk fill with varied values
    INSERT INTO scale.wide
      SELECT
        i,
        (i % 32767)::int2,
        (i * 1000)::int4,
        (i::bigint * 1000000000),
        (i || '.' || i)::numeric(38,10),
        (i / 3.0)::numeric(20,6),
        'row_' || i,
        'varchar_' || i,
        lpad(i::text, 8, '0'),
        date '2021-01-01' + (i * 30),
        timestamp '2021-01-01 00:00:00' + (i || ' days')::interval,
        timestamptz '2021-01-01 00:00:00+00' + (i || ' hours')::interval,
        (i % 2 = 0),
        i * 1.1,
        i * 2.2,
        decode(lpad(to_hex(i), 8, '0'), 'hex')
      FROM generate_series(6, 20) AS i;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- scale.parted: Iceberg partitioning is not supported on this platform (see the header;
-- CreateLakeTableStmt has no PARTITION BY production). This is a plain (non-partitioned) table,
-- but the data is deliberately spread across 12 months, 3 categories and 3 regions so engines can
-- compare date-range / group-by query results (not to validate partition pruning).
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS scale.parted (
  id          bigint,
  event_date  date,
  event_ts    timestamptz,
  category    text,
  region      text,
  amount      numeric(18,2)
);

DO $$ BEGIN
  IF (SELECT count(*) FROM scale.parted) = 0 THEN
    INSERT INTO scale.parted
      SELECT
        i,
        date '2025-01-01' + ((m * 30) + (i % 28)),
        (timestamptz '2025-01-01 00:00:00+00' + ((m * 30) + (i % 28) || ' days')::interval
                                               + ((i % 24) || ' hours')::interval),
        (ARRAY['electronics','grocery','apparel'])[1 + (i % 3)],
        (ARRAY['us-east','us-west','eu-central'])[1 + (i % 3)],
        (10 + i * 1.37)::numeric(18,2)
      FROM generate_series(1, 10) AS i, generate_series(0, 11) AS m;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- scale.snaps: a sequence of consecutive INSERT/UPDATE/DELETE to produce >=3 snapshots plus
-- some deletes. During probing, 6 consecutive DMLs produced 6 snap-*.avro + matching
-- *.metadata.json under MinIO metadata/; this reproduces the same 6-step sequence. Fixed
-- timestamps (not now()) keep the baseline reproducible across reruns / environments.
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS scale.snaps (
  id          bigint,
  v           text,
  updated_at  timestamptz
);

DO $$ BEGIN
  IF (SELECT count(*) FROM scale.snaps) = 0 THEN
    -- snapshot 1
    INSERT INTO scale.snaps VALUES
      (1, 'a', timestamptz '2026-01-01 00:00:00+00'),
      (2, 'b', timestamptz '2026-01-01 00:00:00+00'),
      (3, 'c', timestamptz '2026-01-01 00:00:00+00');
    -- snapshot 2
    UPDATE scale.snaps SET v = v || '-v2', updated_at = timestamptz '2026-01-02 00:00:00+00'
      WHERE id IN (1, 2);
    -- snapshot 3
    DELETE FROM scale.snaps WHERE id = 3;
    -- snapshot 4
    INSERT INTO scale.snaps VALUES
      (4, 'd', timestamptz '2026-01-03 00:00:00+00'),
      (5, 'e', timestamptz '2026-01-03 00:00:00+00');
    -- snapshot 5
    UPDATE scale.snaps SET v = v || '-v2', updated_at = timestamptz '2026-01-04 00:00:00+00'
      WHERE id = 4;
    -- snapshot 6
    DELETE FROM scale.snaps WHERE id = 5;
    -- final expected state: (1,'a-v2'), (2,'b-v2'), (4,'d-v2') -- 3 rows.
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- scale.big: >=1,000,000 rows, to exercise read memory/latency under large metadata / many data
-- files. Readable by iceberg_reader (see the GRANT section at the end of this file) so the
-- e2e large-table scan runs as the normal reader role; the B3 RBAC negative case is served
-- by the dedicated scale.secret table below.
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS scale.big (
  id          bigint,
  val         text,
  amt         numeric(12,2),
  created_at  date
);

DO $$ BEGIN
  IF (SELECT count(*) FROM scale.big) = 0 THEN
    INSERT INTO scale.big
      SELECT i, 'row-' || i, (i % 10000)::numeric / 1.37, date '2020-01-01' + (i % 3650)
      FROM generate_series(1, 1000000) AS i;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- scale.secret: tiny table deliberately NOT granted SELECT to iceberg_reader, kept as the
-- B3 RBAC negative case ("an authenticated user's access to an unauthorized table must be
-- denied"). The gateway must exclude it from iceberg_visible_tables() for iceberg_reader,
-- so a load_table() on it surfaces as 404 / NoSuchTableError.
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS scale.secret (
  id   bigint,
  note text
);

DO $$ BEGIN
  IF (SELECT count(*) FROM scale.secret) = 0 THEN
    INSERT INTO scale.secret VALUES (1, 'top'), (2, 'secret'), (3, 'rows');
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Privileges: scale.wide / scale.parted / scale.snaps / scale.big are readable by
-- iceberg_reader; scale.secret is not granted (B3 RBAC negative case). no_access stays
-- with no privileges.
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA scale TO iceberg_reader;
GRANT SELECT ON scale.wide, scale.parted, scale.snaps, scale.big TO iceberg_reader;
REVOKE ALL ON SCHEMA scale FROM no_access;
