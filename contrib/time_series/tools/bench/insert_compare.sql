-- Compare single-row / multi-row INSERT performance: heap vs time_series.
-- Same column structure as TSBS cpu (13 columns + jsonb tags).

\timing on
\set ECHO none

-- ============================================================
-- Setup: two tables, same structure
-- ============================================================

DROP TABLE IF EXISTS cpu_heap;
DROP TABLE IF EXISTS cpu_ts;

CREATE TABLE cpu_heap (
    "time"           timestamptz NOT NULL,
    tags_id          integer,
    usage_user       double precision,
    usage_system     double precision,
    usage_idle       double precision,
    usage_nice       double precision,
    usage_iowait     double precision,
    usage_irq        double precision,
    usage_softirq    double precision,
    usage_steal      double precision,
    usage_guest      double precision,
    usage_guest_nice double precision,
    additional_tags  jsonb
)
DISTRIBUTED BY (tags_id);

CREATE TABLE cpu_ts (
    "time"           timestamptz NOT NULL,
    tags_id          integer,
    usage_user       double precision,
    usage_system     double precision,
    usage_idle       double precision,
    usage_nice       double precision,
    usage_iowait     double precision,
    usage_irq        double precision,
    usage_softirq    double precision,
    usage_steal      double precision,
    usage_guest      double precision,
    usage_guest_nice double precision,
    additional_tags  jsonb
)
USING time_series
WITH (ts_partition_column='time', ts_chunk_interval='8 hour', ts_chunk_origin='2025-01-01')
DISTRIBUTED BY (tags_id);

-- ============================================================
-- Bench 1: Single-row INSERT in a plpgsql loop
--   (per-row overhead: parse/plan/dispatch/insert/commit)
-- ============================================================

\echo
\echo '===== Bench 1: 10 000 single-row INSERTs in a loop ====='

\echo '--- heap ---'
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO cpu_heap VALUES
      ('2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       i % 100, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"host":"h1"}'::jsonb);
  END LOOP;
END$$;

\echo '--- time_series ---'
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO cpu_ts VALUES
      ('2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       i % 100, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"host":"h1"}'::jsonb);
  END LOOP;
END$$;

TRUNCATE cpu_heap;
TRUNCATE cpu_ts;

-- ============================================================
-- Bench 2: Multi-row VALUES INSERT (1 statement = 100 rows × 100 stmts)
-- ============================================================

\echo
\echo '===== Bench 2: 100 statements × 100 rows VALUES INSERT ====='

\echo '--- heap ---'
DO $$
DECLARE i int; j int; vals text;
BEGIN
  FOR i IN 1..100 LOOP
    vals := '';
    FOR j IN 1..100 LOOP
      IF j > 1 THEN vals := vals || ','; END IF;
      vals := vals || format(
        '(''2025-01-01 00:00:00+00''::timestamptz + (%s * interval ''1 second''), %s, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, ''{"h":"v"}''::jsonb)',
        (i-1)*100 + j, ((i-1)*100 + j) % 100);
    END LOOP;
    EXECUTE format('INSERT INTO cpu_heap VALUES %s', vals);
  END LOOP;
END$$;

\echo '--- time_series ---'
DO $$
DECLARE i int; j int; vals text;
BEGIN
  FOR i IN 1..100 LOOP
    vals := '';
    FOR j IN 1..100 LOOP
      IF j > 1 THEN vals := vals || ','; END IF;
      vals := vals || format(
        '(''2025-01-01 00:00:00+00''::timestamptz + (%s * interval ''1 second''), %s, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, ''{"h":"v"}''::jsonb)',
        (i-1)*100 + j, ((i-1)*100 + j) % 100);
    END LOOP;
    EXECUTE format('INSERT INTO cpu_ts VALUES %s', vals);
  END LOOP;
END$$;

TRUNCATE cpu_heap;
TRUNCATE cpu_ts;

-- ============================================================
-- Bench 3: INSERT ... SELECT (single big query: 1M rows)
-- ============================================================

\echo
\echo '===== Bench 3: INSERT...SELECT 1 000 000 rows from generate_series ====='

\echo '--- heap ---'
INSERT INTO cpu_heap
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       i % 1000, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"h":"v"}'::jsonb
FROM generate_series(1, 1000000) i;

\echo '--- time_series ---'
INSERT INTO cpu_ts
SELECT '2025-01-01 00:00:00+00'::timestamptz + (i * interval '1 second'),
       i % 1000, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
       '{"h":"v"}'::jsonb
FROM generate_series(1, 1000000) i;

-- ============================================================
-- Final sizes
-- ============================================================

\echo
\echo '===== Final size (1M rows each, before cleanup) ====='
SELECT 'heap'        AS storage, count(*) AS rows, pg_size_pretty(pg_total_relation_size('cpu_heap')) AS size FROM cpu_heap
UNION ALL
SELECT 'time_series' AS storage, count(*) AS rows, pg_size_pretty(pg_total_relation_size('cpu_ts'))   AS size FROM cpu_ts;

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE cpu_heap;
DROP TABLE cpu_ts;
