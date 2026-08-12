/* contrib/time_series/time_series--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION time_series" to load this file. \quit

-- ============================================================
-- time_bucket: time bucketing functions (Apache 2.0 from upstream)
-- ============================================================

-- time_bucket(smallint, smallint)
CREATE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT)
RETURNS SMALLINT
AS 'MODULE_PATHNAME', 'ts_int16_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(smallint, smallint, smallint)
CREATE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT)
RETURNS SMALLINT
AS 'MODULE_PATHNAME', 'ts_int16_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(int, int)
CREATE FUNCTION time_bucket(bucket_width INT, ts INT)
RETURNS INT
AS 'MODULE_PATHNAME', 'ts_int32_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(int, int, int)
CREATE FUNCTION time_bucket(bucket_width INT, ts INT, "offset" INT)
RETURNS INT
AS 'MODULE_PATHNAME', 'ts_int32_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(bigint, bigint)
CREATE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT)
RETURNS BIGINT
AS 'MODULE_PATHNAME', 'ts_int64_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(bigint, bigint, bigint)
CREATE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT)
RETURNS BIGINT
AS 'MODULE_PATHNAME', 'ts_int64_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamp)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP)
RETURNS TIMESTAMP
AS 'MODULE_PATHNAME', 'ts_timestamp_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamp, timestamp)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP)
RETURNS TIMESTAMP
AS 'MODULE_PATHNAME', 'ts_timestamp_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamp, interval) -- offset variant
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
RETURNS TIMESTAMP
AS 'MODULE_PATHNAME', 'ts_timestamp_offset_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamptz)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ts_timestamptz_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamptz, timestamptz)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ts_timestamptz_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamptz, interval) -- offset variant
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ts_timestamptz_offset_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, timestamptz, text, timestamptz, interval) -- timezone variant
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT,
                            origin TIMESTAMPTZ DEFAULT NULL, "offset" INTERVAL DEFAULT NULL)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ts_timestamptz_timezone_bucket'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, date)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE)
RETURNS DATE
AS 'MODULE_PATHNAME', 'ts_date_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, date, date)
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, origin DATE)
RETURNS DATE
AS 'MODULE_PATHNAME', 'ts_date_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- time_bucket(interval, date, interval) -- offset variant
CREATE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
RETURNS DATE
AS 'MODULE_PATHNAME', 'ts_date_offset_bucket'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- ============================================================
-- time_bucket_gapfill: gap-filling time bucket functions
-- ============================================================

CREATE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP,
                                     start TIMESTAMP DEFAULT NULL,
                                     finish TIMESTAMP DEFAULT NULL)
RETURNS TIMESTAMP
AS 'MODULE_PATHNAME', 'ht_gapfill_timestamp_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ,
                                     start TIMESTAMPTZ DEFAULT NULL,
                                     finish TIMESTAMPTZ DEFAULT NULL)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ht_gapfill_timestamptz_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT,
                                     start SMALLINT DEFAULT NULL,
                                     finish SMALLINT DEFAULT NULL)
RETURNS SMALLINT
AS 'MODULE_PATHNAME', 'ht_gapfill_int16_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width INT, ts INT,
                                     start INT DEFAULT NULL,
                                     finish INT DEFAULT NULL)
RETURNS INT
AS 'MODULE_PATHNAME', 'ht_gapfill_int32_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width BIGINT, ts BIGINT,
                                     start BIGINT DEFAULT NULL,
                                     finish BIGINT DEFAULT NULL)
RETURNS BIGINT
AS 'MODULE_PATHNAME', 'ht_gapfill_int64_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts DATE,
                                     start DATE DEFAULT NULL,
                                     finish DATE DEFAULT NULL)
RETURNS DATE
AS 'MODULE_PATHNAME', 'ht_gapfill_date_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ,
                                     timezone TEXT,
                                     start TIMESTAMPTZ DEFAULT NULL,
                                     finish TIMESTAMPTZ DEFAULT NULL)
RETURNS TIMESTAMPTZ
AS 'MODULE_PATHNAME', 'ht_gapfill_timestamptz_timezone_bucket'
LANGUAGE C VOLATILE PARALLEL SAFE;

-- ============================================================
-- locf: last observation carried forward (gap fill marker)
-- ============================================================

CREATE FUNCTION locf(value ANYELEMENT)
RETURNS ANYELEMENT
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

-- ============================================================
-- interpolate: linear interpolation (gap fill marker)
-- ============================================================

CREATE FUNCTION interpolate(value SMALLINT)
RETURNS SMALLINT
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION interpolate(value INT)
RETURNS INT
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION interpolate(value BIGINT)
RETURNS BIGINT
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION interpolate(value REAL)
RETURNS REAL
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION interpolate(value DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE FUNCTION interpolate(value NUMERIC)
RETURNS NUMERIC
AS 'MODULE_PATHNAME', 'ht_gapfill_marker'
LANGUAGE C VOLATILE PARALLEL SAFE;

-- ============================================================
-- Function comments for discoverability (\df+)
-- ============================================================

COMMENT ON FUNCTION time_bucket(SMALLINT, SMALLINT) IS
  'Bucket a smallint value into fixed-width intervals';
COMMENT ON FUNCTION time_bucket(SMALLINT, SMALLINT, SMALLINT) IS
  'Bucket a smallint value into fixed-width intervals with offset';
COMMENT ON FUNCTION time_bucket(INT, INT) IS
  'Bucket an integer value into fixed-width intervals';
COMMENT ON FUNCTION time_bucket(INT, INT, INT) IS
  'Bucket an integer value into fixed-width intervals with offset';
COMMENT ON FUNCTION time_bucket(BIGINT, BIGINT) IS
  'Bucket a bigint value into fixed-width intervals';
COMMENT ON FUNCTION time_bucket(BIGINT, BIGINT, BIGINT) IS
  'Bucket a bigint value into fixed-width intervals with offset';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMP) IS
  'Bucket a timestamp into fixed-width time intervals';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMP, TIMESTAMP) IS
  'Bucket a timestamp into fixed-width time intervals with custom origin';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMP, INTERVAL) IS
  'Bucket a timestamp into fixed-width time intervals with offset';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ) IS
  'Bucket a timestamptz into fixed-width time intervals';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ) IS
  'Bucket a timestamptz into fixed-width time intervals with custom origin';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ, INTERVAL) IS
  'Bucket a timestamptz into fixed-width time intervals with offset';
COMMENT ON FUNCTION time_bucket(INTERVAL, TIMESTAMPTZ, TEXT, TIMESTAMPTZ, INTERVAL) IS
  'Bucket a timestamptz into fixed-width time intervals with timezone, optional origin and offset';
COMMENT ON FUNCTION time_bucket(INTERVAL, DATE) IS
  'Bucket a date into fixed-width time intervals';
COMMENT ON FUNCTION time_bucket(INTERVAL, DATE, DATE) IS
  'Bucket a date into fixed-width time intervals with custom origin';
COMMENT ON FUNCTION time_bucket(INTERVAL, DATE, INTERVAL) IS
  'Bucket a date into fixed-width time intervals with offset';

COMMENT ON FUNCTION time_bucket_gapfill(INTERVAL, TIMESTAMP, TIMESTAMP, TIMESTAMP) IS
  'Bucket timestamps with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) IS
  'Bucket timestamptz values with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(SMALLINT, SMALLINT, SMALLINT, SMALLINT) IS
  'Bucket smallint values with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(INT, INT, INT, INT) IS
  'Bucket integer values with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(BIGINT, BIGINT, BIGINT, BIGINT) IS
  'Bucket bigint values with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(INTERVAL, DATE, DATE, DATE) IS
  'Bucket date values with automatic gap detection and synthetic row generation';
COMMENT ON FUNCTION time_bucket_gapfill(INTERVAL, TIMESTAMPTZ, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) IS
  'Bucket timestamptz values with timezone-aware gap detection and synthetic row generation';

COMMENT ON FUNCTION locf(ANYELEMENT) IS
  'Last observation carried forward — fills gaps with the most recent non-NULL value';
COMMENT ON FUNCTION interpolate(SMALLINT) IS
  'Linear interpolation for smallint — fills gaps between known data points';
COMMENT ON FUNCTION interpolate(INT) IS
  'Linear interpolation for integer — fills gaps between known data points';
COMMENT ON FUNCTION interpolate(BIGINT) IS
  'Linear interpolation for bigint — fills gaps between known data points';
COMMENT ON FUNCTION interpolate(REAL) IS
  'Linear interpolation for real — fills gaps between known data points';
COMMENT ON FUNCTION interpolate(DOUBLE PRECISION) IS
  'Linear interpolation for double precision — fills gaps between known data points';
COMMENT ON FUNCTION interpolate(NUMERIC) IS
  'Linear interpolation for numeric — fills gaps between known data points';

-- ============================================================
-- Table Access Method: time_series
-- ============================================================

CREATE FUNCTION time_series.ts_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'ts_tableam_handler'
LANGUAGE C;

COMMENT ON FUNCTION time_series.ts_tableam_handler(internal)
IS 'time-series heap table access method handler';

CREATE ACCESS METHOD time_series TYPE TABLE
    HANDLER time_series.ts_tableam_handler;

COMMENT ON ACCESS METHOD time_series
IS 'heap-based time-series storage with fork partitioning';

-- ============================================================
-- Chunk catalog table
-- ============================================================

CREATE TABLE time_series.ts_chunk (
    table_oid oid NOT NULL,
    chunk_number integer NOT NULL,
    range_start timestamptz NOT NULL,
    range_end timestamptz NOT NULL,
    creation_time timestamptz NOT NULL DEFAULT now(),
    status smallint NOT NULL DEFAULT 0
) DISTRIBUTED RANDOMLY;

COMMENT ON TABLE time_series.ts_chunk
IS 'Catalog of time-series chunks. Each segment tracks its own chunks.';

-- Composite btree powering every catalog access pattern in ts_catalog.c.
-- Point-lookup scans on (table_oid, chunk_number) used by:
--   - ts_chunk_catalog_insert        : SnapshotSelf existence check before insert
--   - ts_chunk_catalog_is_compressed : per-chunk status probe
--   - ts_chunk_catalog_update_status : per-chunk status flip
--   - ts_chunk_catalog_lock_if_compressed
--   - ts_chunk_catalog_get_chunks_with_status (range on chunk_number)
-- Prefix-only scans on (table_oid) used by:
--   - ts_chunk_catalog_has_any
--   - ts_chunk_catalog_get_chunks
--   - ts_chunk_catalog_delete
-- A single composite index covers both: btree prefix-scan serves the leading
-- column on its own.  Replaces the per-insert O(N) heap seqscan with an
-- O(log N) btree probe; at N=65K chunks/segment the difference is ~4 orders
-- of magnitude.
CREATE INDEX ts_chunk_oid_chunknum_idx ON time_series.ts_chunk (table_oid, chunk_number);

-- ============================================================
-- Chunk info function
-- ============================================================

CREATE OR REPLACE FUNCTION time_series.ts_chunk_info(
    rel regclass,
    OUT segment_id integer,
    OUT chunk_number integer,
    OUT nblocks bigint,
    OUT range_start timestamptz,
    OUT range_end timestamptz,
    OUT status smallint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'ts_chunk_info'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

COMMENT ON FUNCTION time_series.ts_chunk_info(regclass)
IS 'Returns per-segment chunk info including compression status.';

-- ============================================================
-- Compression configuration
-- ============================================================

CREATE TABLE time_series.ts_compress_config (
    table_oid           oid     NOT NULL,
    segmentby           text[],
    orderby             text[],
    orderby_desc        bool[],
    orderby_nullsfirst  bool[],
    CONSTRAINT ts_compress_config_pkey PRIMARY KEY (table_oid)
) DISTRIBUTED REPLICATED;

COMMENT ON TABLE time_series.ts_compress_config
IS 'Per-table compression config: segmentby and orderby column arrays.';

CREATE TABLE time_series.ts_compressed_chunk (
    table_oid          oid          NOT NULL,
    chunk_number       integer      NOT NULL,
    range_start        timestamptz  NOT NULL,
    range_end          timestamptz  NOT NULL,
    pax_file           text         NOT NULL,
    compressed_at      timestamptz  NOT NULL DEFAULT now(),
    numrows            bigint,
    num_groups        integer,
    compressed_size    bigint,
    uncompressed_size  bigint
) DISTRIBUTED RANDOMLY;

COMMENT ON TABLE time_series.ts_compressed_chunk
IS 'Metadata for compressed chunks: PAX file location, row counts, sizes.';

CREATE OR REPLACE FUNCTION time_series.set_compress_config(
    rel        regclass,
    segmentby  text DEFAULT NULL,
    orderby    text DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'ts_set_compress_config'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION time_series.set_compress_config(regclass, text, text)
IS 'Configure segmentby and orderby columns for chunk compression.';

CREATE OR REPLACE FUNCTION time_series.compress_chunks(
    rel        regclass,
    older_than interval DEFAULT NULL
)
RETURNS integer
AS 'MODULE_PATHNAME', 'ts_compress_chunks'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION time_series.compress_chunks(regclass, interval)
IS 'Compress ACTIVE chunks older than the given interval. Returns number of chunks compressed.';

-- Per-chunk variant.  Counterpart of TimescaleDB compress_chunk(REGCLASS):
-- TSDB chunks have their own pg_class OID; ours are extension forks
-- identified by (table_oid, chunk_number), so the signature takes the
-- pair instead.  Returns 1 if (re)compressed, 0 if skipped.
CREATE OR REPLACE FUNCTION time_series.compress_chunk(
    rel               regclass,
    chunk_number      integer,
    if_not_compressed boolean DEFAULT true
)
RETURNS integer
AS 'MODULE_PATHNAME', 'ts_compress_chunk'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION time_series.compress_chunk(regclass, integer, boolean)
IS 'Compress a single chunk identified by (table_oid, chunk_number). Counterpart of TimescaleDB''s compress_chunk(REGCLASS).';

-- Segment-side writer.  The segment derives each PAX file path and chunk
-- time range from (relid, chunk_number) + its local copy of the table's
-- config, so the coordinator only needs to dispatch chunk numbers.  Each
-- segment writes its own PAX file and ts_compressed_chunk row per
-- (chunk, segment).  Cloudberry requires EXECUTE ON ALL SEGMENTS to be
-- set-returning, so we emit a single seg_id row per segment.
CREATE OR REPLACE FUNCTION time_series._ts_compress_write_chunks(
    rel           regclass,
    chunk_numbers integer[]
)
RETURNS SETOF integer
AS 'MODULE_PATHNAME', 'ts_compress_write_chunks'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;

-- Reclaim disk space from heap forks of already-COMPRESSED chunks.  Runs
-- in its own transaction (separate from compress_chunks) so a failure
-- cannot leave the catalog and filesystem in inconsistent states.  Call
-- after compress_chunks completes:
--     SELECT time_series.compress_chunks('t');
--     SELECT time_series.reclaim_chunk_heaps('t');
CREATE OR REPLACE FUNCTION time_series.reclaim_chunk_heaps(
    rel regclass
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ts_reclaim_chunk_heaps'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION time_series.reclaim_chunk_heaps(regclass)
IS 'Truncate heap forks of COMPRESSED chunks to reclaim disk.  Safe to run any time; call after compress_chunks to actually free space.';

CREATE OR REPLACE FUNCTION time_series._ts_reclaim_chunk_heaps_segment(
    rel           regclass,
    chunk_numbers integer[]
)
RETURNS SETOF integer
AS 'MODULE_PATHNAME', 'ts_reclaim_chunk_heaps_segment'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;

-- One-call wrapper: compress chunks, then reclaim their heap forks.
-- The intermediate COMMIT is required because heap-fork truncation is a
-- non-transactional filesystem op; running it in the same xact as the
-- compress would risk truncating the heap before catalog rows are
-- durable.  Splitting at COMMIT ensures reclaim only sees chunks whose
-- PAX content is already persisted.
--
-- Usage:
--     CALL time_series.compress_and_reclaim('sensor_data');
--     CALL time_series.compress_and_reclaim('sensor_data', INTERVAL '7 days');
CREATE OR REPLACE PROCEDURE time_series.compress_and_reclaim(
    rel        regclass,
    older_than interval DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    n_compressed integer;
    n_reclaimed  bigint;
BEGIN
    SELECT time_series.compress_chunks(rel, older_than)
      INTO n_compressed;
    COMMIT;
    RAISE NOTICE 'compress_and_reclaim: compressed % chunk(s)', n_compressed;

    SELECT time_series.reclaim_chunk_heaps(rel)
      INTO n_reclaimed;
    COMMIT;
    RAISE NOTICE 'compress_and_reclaim: reclaimed % chunk heap(s)', n_reclaimed;
END $$;

COMMENT ON PROCEDURE time_series.compress_and_reclaim(regclass, interval)
IS 'Compress eligible chunks and immediately reclaim their heap-fork disk space.  Splits the work across two transactions to keep heap truncation safe.';

-- Internal: truncate a chunk's heap fork after compression
CREATE OR REPLACE FUNCTION time_series._ts_truncate_chunk_fork(
    rel          regclass,
    chunk_number integer
)
RETURNS SETOF integer
AS 'MODULE_PATHNAME', 'ts_truncate_chunk_fork'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION time_series.ts_compressed_chunk_info(
    rel        regclass,
    OUT chunk_number    integer,
    OUT range_start     timestamptz,
    OUT range_end       timestamptz,
    OUT status          smallint,
    OUT pax_file        text,
    OUT numrows         bigint,
    OUT compressed_at   timestamptz
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'ts_compressed_chunk_info'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

COMMENT ON FUNCTION time_series.ts_compressed_chunk_info(regclass)
IS 'Returns compression metadata for each chunk of the given hypertable.';

-- =========================================================================
--  Continuous Aggregate (CAGG) Catalog Tables
-- =========================================================================

-- 1. continuous_agg — CAGG main registry (REPLICATED)
CREATE TABLE time_series.continuous_agg (
    cagg_id             SERIAL PRIMARY KEY,
    user_view_schema    name NOT NULL,
    user_view_name      name NOT NULL,
    source_table_schema name NOT NULL,
    source_table_name   name NOT NULL,
    source_table_oid    oid  NOT NULL,
    mat_table_schema    name NOT NULL DEFAULT '',
    mat_table_name      name NOT NULL DEFAULT '',
    partial_view_schema name NOT NULL DEFAULT '',
    partial_view_name   name NOT NULL DEFAULT '',
    direct_view_schema  name NOT NULL DEFAULT '',
    direct_view_name    name NOT NULL DEFAULT '',
    bucket_width        interval NOT NULL,
    bucket_column       name NOT NULL,
    materialized_only   bool NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED REPLICATED;

-- 2. cagg_watermark — per-segment materialization progress (RANDOMLY)
CREATE TABLE time_series.cagg_watermark (
    cagg_id     int         NOT NULL,
    watermark   timestamptz NOT NULL
) DISTRIBUTED RANDOMLY;

-- 3. cagg_bucket_function — bucket parameters (REPLICATED)
CREATE TABLE time_series.cagg_bucket_function (
    cagg_id         int       PRIMARY KEY,
    bucket_func     text      DEFAULT 'time_bucket',
    bucket_width    interval  NOT NULL,
    bucket_origin   timestamptz,
    bucket_offset   interval,
    bucket_timezone text,
    time_type       oid       NOT NULL
) DISTRIBUTED REPLICATED;

-- 4. cagg_invalidation_log — L1 shared invalidation log (RANDOMLY)
CREATE TABLE time_series.cagg_invalidation_log (
    source_table_oid    oid         NOT NULL,
    lowest_modified     timestamptz NOT NULL,
    greatest_modified   timestamptz NOT NULL
) DISTRIBUTED RANDOMLY;

-- 5. cagg_materialization_log — L2 per-CAGG invalidation log (RANDOMLY)
CREATE TABLE time_series.cagg_materialization_log (
    cagg_id             int         NOT NULL,
    lowest_modified     timestamptz NOT NULL,
    greatest_modified   timestamptz NOT NULL
) DISTRIBUTED RANDOMLY;

-- NOTE: No index on L2 — _cagg_move_l1_to_l2 uses simple_heap_insert
-- which bypasses index maintenance.  L2 is consumed by REFRESH via
-- sequential scan with WHERE cagg_id = $1; rows are short-lived.

-- 5b. cagg_invalidation_threshold — per-source threshold (RANDOMLY)
-- Stores MAX(watermark) across all CAGGs on the same source table.
-- Pre-computed during REFRESH so trigger only needs one heap scan.
-- One row per source per segment (DISTRIBUTED RANDOMLY).
CREATE TABLE time_series.cagg_invalidation_threshold (
    source_table_oid    oid         NOT NULL,
    threshold           timestamptz NOT NULL DEFAULT '-infinity'
) DISTRIBUTED RANDOMLY;

-- NOTE: There is NO separate cagg_policy table.  Policy parameters
-- (schedule_interval / start_offset / end_offset / cagg_name) live
-- exclusively in bgw_job:
--   - bgw_job.schedule_interval     ── how often to fire
--   - bgw_job.config (jsonb)        ── start_offset / end_offset / cagg_name
--   - bgw_job.scheduled (bool)      ── active flag
--   - bgw_job.hypertable_id         ── cagg_id (column name kept from upstream)
-- This matches upstream design (single source of truth).  The
-- cagg_policy_stats view (defined later) joins bgw_job ↔ continuous_agg
-- and exposes the jsonb fields as relational columns for convenience.

-- =========================================================================
--  BGW Job Scheduling Tables (generic framework, matches upstream bgw_job)
-- =========================================================================

-- 7. bgw_job — Generic background job definitions (REPLICATED)
-- One row per scheduled job. CAGG refresh policies create rows here
-- with proc_name='cagg_refresh_policy'.
CREATE SEQUENCE IF NOT EXISTS time_series.bgw_job_id_seq;

CREATE TABLE time_series.bgw_job (
    id                  int       NOT NULL DEFAULT nextval('time_series.bgw_job_id_seq'),
    application_name    name      NOT NULL,
    schedule_interval   interval  NOT NULL,
    max_runtime         interval  NOT NULL DEFAULT '0'::interval,
    max_retries         int       NOT NULL DEFAULT -1,
    retry_period        interval  NOT NULL DEFAULT '5 minutes'::interval,
    proc_schema         name      NOT NULL,
    proc_name           name      NOT NULL,
    owner               regrole   NOT NULL DEFAULT current_role::regrole,
    scheduled           bool      NOT NULL DEFAULT true,
    fixed_schedule      bool      NOT NULL DEFAULT true,
    initial_start       timestamptz,
    hypertable_id       int,
    config              jsonb,
    check_schema        name,
    check_name          name,
    timezone            text,
    CONSTRAINT bgw_job_pkey PRIMARY KEY (id)
) DISTRIBUTED REPLICATED;

-- 8. bgw_job_stat — Job execution statistics (REPLICATED)
-- One row per job, tracks runs/failures/crashes and next_start.
CREATE TABLE time_series.bgw_job_stat (
    job_id                  int       NOT NULL,
    last_start              timestamptz NOT NULL DEFAULT '-infinity',
    last_finish             timestamptz NOT NULL DEFAULT '-infinity',
    next_start              timestamptz NOT NULL DEFAULT '-infinity',
    last_successful_finish  timestamptz NOT NULL DEFAULT '-infinity',
    last_run_success        bool      NOT NULL DEFAULT true,
    total_runs              bigint    NOT NULL DEFAULT 0,
    total_duration          interval  NOT NULL DEFAULT '0'::interval,
    total_duration_failures interval  NOT NULL DEFAULT '0'::interval,
    total_successes         bigint    NOT NULL DEFAULT 0,
    total_failures          bigint    NOT NULL DEFAULT 0,
    total_crashes           bigint    NOT NULL DEFAULT 0,
    consecutive_failures    int       NOT NULL DEFAULT 0,
    consecutive_crashes     int       NOT NULL DEFAULT 0,
    flags                   int       NOT NULL DEFAULT 0,
    CONSTRAINT bgw_job_stat_pkey PRIMARY KEY (job_id)
    -- job_id conceptually references time_series.bgw_job(id): every
    -- bgw_job_stat row is created/deleted alongside its bgw_job row.
    -- No FOREIGN KEY here -- Cloudberry doesn't enforce FK constraints,
    -- so one would be decorative only (and trigger a WARNING on every
    -- CREATE EXTENSION); the relationship is maintained entirely by
    -- this file's own job-delete code paths instead.
)
-- Hash-distributed on job_id (NOT REPLICATED).  REPLICATED writes
-- escalate to a table-level ExclusiveLock on every segment, which
-- bottlenecks every BGW worker to single-writer throughput.  Hash
-- distribution lets concurrent UPDATE/INSERT on different job_ids
-- proceed in parallel.  scheduler SELECTs gather across segments,
-- which for a small table is negligible.
DISTRIBUTED BY (job_id);

-- 9. bgw_job_stat_history — Per-execution audit log
-- Each row records one job invocation: start/finish, success, snapshot
-- of the job config and (on failure) the captured error data.
--
-- V1 always records every job execution (no opt-in GUC).  See
-- src/bgw/job_stat_history.c file header for why we don't follow
-- upstream optional/track-only-errors design.
--
-- The `data` JSONB column has the shape
--   {"job": {<snapshot of bgw_job row>}, "error_data": {<edata>}?}
-- so users can debug *which* version of the policy ran when, and
-- correlate failures with specific config changes.
CREATE SEQUENCE IF NOT EXISTS time_series.bgw_job_stat_history_id_seq;

CREATE TABLE time_series.bgw_job_stat_history (
    id                  bigint    NOT NULL DEFAULT
                            nextval('time_series.bgw_job_stat_history_id_seq'),
    job_id              int       NOT NULL,
    pid                 int,
    execution_start     timestamptz NOT NULL,
    execution_finish    timestamptz,
    succeeded           bool,
    data                jsonb
    -- No PK: an exclusive table lock on a REPLICATED table serializes
    -- every BGW worker INSERT (PG's relation-level ExclusiveLock for
    -- REPLICATED writes blocks parallel writers).  History writes are
    -- append-only audit data — distribute by id and let writes go to
    -- whichever segment owns the partition.  Queries scan all segments
    -- via standard MPP gather, no consistency issue.
) DISTRIBUTED BY (id);

CREATE INDEX bgw_job_stat_history_job_id_idx
    ON time_series.bgw_job_stat_history (job_id, execution_start DESC);

-- User-facing view: most useful columns extracted from `data` JSONB +
-- duration computed.  Excludes still-running rows (execution_finish IS
-- NULL).  Mirrors the standard job_history view —
-- including the owner filter, the security_barrier option, and the
-- "let the database owner see everything" carve-out:
--
--   * pg_has_role(current_user, owner, 'MEMBER') — job owner and
--     anyone in their role chain (incl. superuser) sees their rows.
--   * pg_has_role(current_user, <db owner>, 'MEMBER') — the DBA who
--     owns this database also sees every row, satisfying the typical
--     operational need of "let DBA inspect any job's history".
--   * WITH (security_barrier = true) — keep PG's optimizer from
--     pushing user-supplied predicates inside the WHERE clause, which
--     would let a malicious caller leak rows it shouldn't see via
--     side-effects of a custom function reading config / error_data
--     before the owner check has filtered the row out.
CREATE VIEW time_series.job_history
WITH (security_barrier = true) AS
SELECT h.id,
       h.job_id,
       h.pid,
       h.execution_start,
       h.execution_finish,
       h.execution_finish - h.execution_start AS duration,
       h.succeeded,
       -- A "ghost" row: mark_start ran (execution_start written) but
       -- mark_end never did (execution_finish stays NULL forever).
       -- Diagnostic: worker was SIGKILL'd / coordinator restarted /
       -- OOM-killer fired before the policy proc finished.  These rows
       -- have NULL data + NULL succeeded; surfacing the distinction lets
       -- operators tell "policy raised ERROR" apart from "process died".
       (h.execution_finish IS NULL AND h.execution_start IS NOT NULL)
                                             AS is_crashed,
       (h.data->'job'->>'proc_schema')::name AS proc_schema,
       (h.data->'job'->>'proc_name')::name   AS proc_name,
       h.data->'job'->'config'               AS config,
       h.data->'error_data'                  AS error_data
  FROM time_series.bgw_job_stat_history h
  LEFT JOIN time_series.bgw_job j ON j.id = h.job_id
 WHERE pg_catalog.pg_has_role(current_user::name,
                              (SELECT pg_catalog.pg_get_userbyid(datdba)
                                 FROM pg_catalog.pg_database
                                WHERE datname = current_database()),
                              'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user::name, j.owner, 'MEMBER') IS TRUE;

-- bgw_job_stat_history grows unbounded by design: every job execution
-- appends one row, including a JSONB snapshot of the bgw_job + edata
-- on failure (commonly a few KB).  At a 1-minute schedule across 100
-- policies that's ~150 MB/day.  V1 has no built-in retention policy
-- (mirrors upstream upstream's posture), so DBAs need to prune the table
-- themselves.  This helper does the prune in one statement, returning
-- the row count so cron / pg_cron can log progress.
--
-- Usage:
--   SELECT time_series.bgw_job_stat_history_purge('30 days');
--   -- → returns count of rows deleted with execution_finish older
--   --   than 30 days
--
-- Rows whose execution_finish is NULL (still running, or row written
-- by mark_start before mark_end) are deliberately preserved.
CREATE OR REPLACE FUNCTION time_series.bgw_job_stat_history_purge(
    p_older_than interval
) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_deleted bigint;
BEGIN
    IF p_older_than IS NULL OR p_older_than <= '0'::interval THEN
        RAISE EXCEPTION 'p_older_than must be a positive interval';
    END IF;

    -- Two clauses:
    --   (a) closed rows: execution_finish set by mark_end → compare it.
    --   (b) ghost rows:  worker crashed (SIGKILL / coordinator restart /
    --       OOM) before mark_end ran, so execution_finish stays NULL
    --       forever.  Without clause (b), every crash leaves a permanent
    --       row that retention can never delete — bgw_job_stat_history
    --       grows unbounded across long-stability runs even with the
    --       retention policy enabled.  Compare execution_start instead
    --       (set by mark_start before the policy proc fired).
    DELETE FROM time_series.bgw_job_stat_history
     WHERE (execution_finish IS NOT NULL
            AND execution_finish < now() - p_older_than)
        OR (execution_finish IS NULL
            AND execution_start  < now() - p_older_than);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END $$;

-- Restrict to superuser by default (the table is hash-distributed +
-- contains every user's job history; an ordinary user purging it
-- could destroy other users' audit logs).  Operators can GRANT to
-- a dedicated maintenance role if needed.
REVOKE EXECUTE ON FUNCTION time_series.bgw_job_stat_history_purge(interval) FROM PUBLIC;

-- =========================================================================
--  CAGG Functions
-- =========================================================================

-- Row-level trigger function: writes dirty time ranges to L1 (cagg_insert.c)
CREATE FUNCTION time_series.cagg_invalidation_trigfn()
RETURNS trigger LANGUAGE C AS 'MODULE_PATHNAME', 'cagg_invalidation_trigfn';

-- Segment-local watermark initialization (called on each segment via
-- SELECT _cagg_init_segment_watermark(cagg_id) FROM gp_dist_random('gp_id'))
CREATE FUNCTION time_series._cagg_init_segment_watermark(cagg_id int)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'cagg_init_segment_watermark';

-- Segment-local threshold initialization (called on each segment via
-- SELECT _cagg_init_segment_threshold(source_oid) FROM gp_dist_random('gp_id'))
CREATE FUNCTION time_series._cagg_init_segment_threshold(source_oid oid)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'cagg_init_segment_threshold';

-- Segment-local L1 → L2 migration function (called internally by REFRESH;
-- dispatched to each segment via SELECT ... FROM cagg_watermark trick)
CREATE FUNCTION time_series._cagg_move_l1_to_l2(source_oid oid)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'cagg_segment_move_l1_to_l2';

-- REFRESH procedure for continuous aggregates (cagg_refresh.c)
--
-- force=TRUE forces re-materialization of every bucket in [start, end)
-- even when L2 is empty / watermark already covers it.  Recovery
-- escape hatch for data drift (trigger missed an INSERT, mat table
-- got manipulated directly, etc.).  Mirrors upstream
-- @extschema@.refresh_continuous_aggregate(..., force BOOLEAN = FALSE).
CREATE PROCEDURE time_series.refresh_continuous_aggregate(
    continuous_aggregate regclass,
    window_start timestamptz DEFAULT NULL,
    window_end   timestamptz DEFAULT NULL,
    force        boolean     DEFAULT false
) LANGUAGE C AS 'MODULE_PATHNAME', 'cagg_refresh';

-- Watermark function: returns per-segment watermark for real-time UNION ALL.
--
-- Implemented in C with direct heap scan (no SPI) because:
--   1. CBDB segment QEs cannot execute SPI on distributed tables
--      (cagg_watermark is DISTRIBUTED RANDOMLY).
--   2. LANGUAGE SQL gets inlined + constant-folded by eval_const_expressions.
--   3. LANGUAGE plpgsql uses SPI internally → same QE restriction.
--
-- MUST be VOLATILE (not STABLE) to prevent eval_const_expressions from
-- evaluating it at plan time on QD.  VOLATILE guarantees each segment's
-- executor calls it at runtime, reading the LOCAL cagg_watermark row.
CREATE FUNCTION time_series.cagg_watermark(cagg_id int)
RETURNS timestamptz LANGUAGE C VOLATILE
AS 'MODULE_PATHNAME', 'cagg_watermark_fn';

-- ============================================================
-- materialized_only mode toggle
--
-- User-facing syntax:
--   ALTER VIEW cv_name SET (time_series.materialized_only = true);
--
-- The ALTER VIEW command is intercepted by the ProcessUtility hook
-- (cagg_create.c) which runs the toggle logic in C via SPI.  No public
-- function or procedure is exposed.
-- ============================================================

-- ============================================================
-- CAGG cleanup (event trigger)
--
-- Handles two DROP scenarios:
--
-- 1. DROP TABLE source_table CASCADE — PostgreSQL only cascades to objects
--    that hold an explicit pg_depend reference to the source; partial/direct
--    views get dropped, but the user view, mat table, and catalog rows do
--    NOT.  We match by source_table_oid.
--
-- 2. DROP VIEW user_view CASCADE — the user drops the CAGG user view
--    directly.  We match by (schema, name) against continuous_agg and
--    clean up the mat table, catalog rows, and source-table trigger.
--
-- ============================================================
CREATE FUNCTION time_series.cagg_handle_source_drop()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj      record;
    cagg_rec record;
    other_count int;
BEGIN
    FOR obj IN
        SELECT objid, object_type, schema_name, object_name
        FROM pg_event_trigger_dropped_objects()
        WHERE object_type IN ('table', 'view')
    LOOP
        IF obj.object_type = 'table' THEN
            -- Source table dropped: clean up all CAGGs that reference it
            FOR cagg_rec IN
                SELECT * FROM time_series.continuous_agg
                WHERE source_table_oid = obj.objid
            LOOP
                EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE',
                    cagg_rec.user_view_schema, cagg_rec.user_view_name);
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                    cagg_rec.mat_table_schema, cagg_rec.mat_table_name);
                PERFORM time_series._cagg_cleanup_catalog(cagg_rec.cagg_id,
                    cagg_rec.source_table_oid);
            END LOOP;

        ELSIF obj.object_type = 'view' THEN
            -- User view dropped: clean up matching CAGG
            FOR cagg_rec IN
                SELECT * FROM time_series.continuous_agg
                WHERE user_view_schema = obj.schema_name
                  AND user_view_name   = obj.object_name
            LOOP
                -- Lock-order: drop the mat table FIRST, then the internal
                -- views.  Concurrent refresh runs DELETE FROM _mat (RowEx on
                -- mat) THEN INSERT INTO _mat SELECT * FROM _partial_view
                -- (AccessShare on partial_view) inside one transaction, i.e.
                -- mat → partial_view.  If we instead dropped partial_view
                -- first here we would take AccessExclusive on partial_view
                -- BEFORE mat, giving refresh × drop an ABBA deadlock cycle
                -- that postgres's per-segment detector kills the refresh BGW
                -- for (silent worker exit, mat coverage lag).  Match refresh
                -- order to break the cycle.  The internal views depend on
                -- the source table, not on _mat, so DROP TABLE _mat CASCADE
                -- does NOT cascade to them — they still need explicit drops.
                EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                    cagg_rec.mat_table_schema, cagg_rec.mat_table_name);
                EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE',
                    cagg_rec.partial_view_schema, cagg_rec.partial_view_name);
                EXECUTE format('DROP VIEW IF EXISTS %I.%I CASCADE',
                    cagg_rec.direct_view_schema, cagg_rec.direct_view_name);
                PERFORM time_series._cagg_cleanup_catalog(cagg_rec.cagg_id,
                    cagg_rec.source_table_oid);
            END LOOP;
        END IF;
    END LOOP;
END
$$;

-- Helper: clean catalog rows and optionally remove source trigger
CREATE FUNCTION time_series._cagg_cleanup_catalog(
    p_cagg_id int, p_source_oid oid
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    other_count int;
BEGIN
    DELETE FROM time_series.cagg_watermark
        WHERE cagg_id = p_cagg_id;
    DELETE FROM time_series.cagg_bucket_function
        WHERE cagg_id = p_cagg_id;
    -- NOTE: do NOT delete cagg_invalidation_log here.  L1 is keyed by
    -- source_table_oid (one row per source, fed by the INSERT trigger),
    -- and shared by ALL CAGGs on the source.  Wiping it on a single
    -- CAGG drop would silently strip pending invalidations from every
    -- other CAGG on the same source, causing missed refresh coverage.
    -- Aligns with TSDB upstream: hyper_invalidation_log persists until
    -- the source table is dropped.  See the `IF other_count = 0` block
    -- below for the actual L1 cleanup (last-CAGG-on-source path).
    DELETE FROM time_series.cagg_materialization_log
        WHERE cagg_id = p_cagg_id;
    -- Delete BGW jobs associated with this CAGG (cascades to bgw_job_stat)
    DELETE FROM time_series.bgw_job
        WHERE hypertable_id = p_cagg_id;
    DELETE FROM time_series.continuous_agg
        WHERE cagg_id = p_cagg_id;

    -- Broadcast relcache inval so the scheduler drops the deleted job(s)
    -- from its in-memory scheduled_jobs cache.  Without it, the cached
    -- entries keep getting dispatched until the next unrelated bgw_job
    -- DDL triggers a reload — the dispatched policy_refresh_cagg then
    -- fails with "continuous aggregate not found" on every tick.  This
    -- function runs from the sql_drop event trigger (cagg_handle_source_drop),
    -- so the parent transaction is still open and the inval is delivered
    -- alongside the catalog deletes.
    PERFORM time_series.bgw_invalidate_cache();

    -- Remove trigger from source table if no other CAGGs reference it
    SELECT count(*) INTO other_count
    FROM time_series.continuous_agg
    WHERE source_table_oid = p_source_oid;

    IF other_count = 0 AND p_source_oid IS NOT NULL THEN
        -- Last CAGG on this source → clean up threshold rows and L1
        -- (no more consumers of L1 entries for this source).
        DELETE FROM time_series.cagg_invalidation_threshold
            WHERE source_table_oid = p_source_oid;
        DELETE FROM time_series.cagg_invalidation_log
            WHERE source_table_oid = p_source_oid;

        -- Only attempt DROP TRIGGER if source table still exists
        IF EXISTS (SELECT 1 FROM pg_class WHERE oid = p_source_oid) THEN
            EXECUTE format(
                'DROP TRIGGER IF EXISTS cagg_invalidation_trigger ON %s',
                p_source_oid::regclass);
        END IF;
    ELSIF other_count > 0 AND p_source_oid IS NOT NULL THEN
        -- Other CAGGs remain → recalculate threshold from remaining watermarks
        -- (the dropped CAGG may have had the highest watermark)
        UPDATE time_series.cagg_invalidation_threshold
        SET threshold = COALESCE((
            SELECT MAX(w.watermark)
            FROM time_series.cagg_watermark w
            JOIN time_series.continuous_agg c ON w.cagg_id = c.cagg_id
            WHERE c.source_table_oid = p_source_oid
        ), '-infinity'::timestamptz)
        WHERE source_table_oid = p_source_oid;
    END IF;
END
$$;

CREATE EVENT TRIGGER cagg_source_drop_handler
    ON sql_drop
    EXECUTE FUNCTION time_series.cagg_handle_source_drop();

-- ============================================================
-- Note: RENAME COLUMN of a CAGG bucket_column is handled by the
-- ProcessUtility hook in cagg_create.c — it auto-updates the
-- continuous_agg.bucket_column registry to follow the rename
-- (more user-friendly than blocking).  DROP COLUMN and
-- ALTER COLUMN TYPE on the bucket column are blocked by the
-- same hook because they cannot be safely followed.
-- ============================================================

-- ============================================================
-- TRUNCATE invalidation: handled via ProcessUtility hook in
-- cagg_create.c (not via triggers).  CBDB blocks both STATEMENT
-- triggers and event triggers for TRUNCATE, so the hook intercepts
-- TruncateStmt on QD and writes {-infinity, +infinity} to L1
-- before passing through to the standard TRUNCATE handler.
-- ============================================================

-- =========================================================================
--  BGW Functions
-- =========================================================================

-- BGW job worker entry point (called by dynamic background workers)
CREATE FUNCTION time_series.bgw_job_entrypoint(int4)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_job_entrypoint';

-- BGW scheduler main (called by the static background worker)
CREATE FUNCTION time_series.bgw_scheduler_main(int4)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_scheduler_main';

-- CAGG refresh policy procedure (called by BGW workers)
CREATE PROCEDURE time_series.policy_refresh_cagg(job_id int, config jsonb)
LANGUAGE C AS 'MODULE_PATHNAME', 'policy_refresh_cagg';

-- Broadcast a relcache invalidation for bgw_job so the scheduler reloads
-- its job list on the next iteration.  Called from add/remove/alter after
-- INSERT/UPDATE/DELETE on bgw_job.
CREATE FUNCTION time_series.bgw_invalidate_cache()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_invalidate_cache';
-- Note: deliberately NOT REVOKE-d from PUBLIC.  add/remove/alter
-- policy plpgsql functions run SECURITY INVOKER and PERFORM this
-- inside their own body — revoking PUBLIC EXECUTE would break the
-- normal user-driven call path.  Theoretical DoS surface (call in
-- a tight loop to flood the SI queue) exists but is not worse than
-- equivalent direct catalog DML the same user could already perform.

-- =========================================================================
--  CAGG Policy Management API
-- =========================================================================

-- Resolve a possibly-unqualified continuous-aggregate name into a
-- single (cagg_id, schema, name) row using PG's search_path semantics.
--
-- Earlier versions did `WHERE user_view_name = cagg_name` which is
-- ambiguous when two different schemas hold a CAGG with the same view
-- name — SELECT INTO would pick whichever row sorted first, silently
-- targeting the wrong CAGG.  Route the name through to_regclass so PG
-- itself applies the search_path resolution rules (consistent with
-- every other SQL command that references a relation).  Returns NULL
-- in v_cagg_id if the name doesn't resolve to a known CAGG; callers
-- decide whether that's an error or a no-op.
CREATE OR REPLACE FUNCTION time_series._resolve_cagg_id(p_cagg_name text)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    v_view_oid    oid;
    v_view_schema name;
    v_view_name   name;
    v_cagg_id     int;
BEGIN
    IF p_cagg_name IS NULL OR p_cagg_name = '' THEN
        RAISE EXCEPTION 'cagg_name must be non-empty';
    END IF;

    -- to_regclass: schema-qualified names resolve directly; bare names
    -- resolve via search_path; non-existent names return NULL (no
    -- ERROR — we want a clean caller-driven message instead).
    v_view_oid := to_regclass(p_cagg_name)::oid;
    IF v_view_oid IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT n.nspname, c.relname INTO v_view_schema, v_view_name
      FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE c.oid = v_view_oid;

    SELECT cagg_id INTO v_cagg_id
      FROM time_series.continuous_agg
     WHERE user_view_schema = v_view_schema
       AND user_view_name   = v_view_name;

    RETURN v_cagg_id;
END $$;

-- Shared validation for policy_refresh_cagg config.  Used by both
-- add_continuous_aggregate_policy (which builds the config) and alter_job
-- (which lets the user replace it wholesale).  Without this, alter_job
-- would silently accept ghost cagg_names, empty/inverted refresh windows,
-- and windows narrower than 2 × bucket_width — letting users break a
-- working policy without any feedback until the BGW worker fails to run.
CREATE OR REPLACE FUNCTION time_series._validate_cagg_policy_config(
    p_cagg_name      text,
    p_start_offset   interval,
    p_end_offset     interval
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_cagg_id      int;
    v_bucket_width interval;
BEGIN
    -- 1. cagg_name resolves to an existing CAGG (search_path-aware)
    v_cagg_id := time_series._resolve_cagg_id(p_cagg_name);
    IF v_cagg_id IS NULL THEN
        RAISE EXCEPTION 'continuous aggregate "%" does not exist', p_cagg_name;
    END IF;

    -- 2. fetch bucket_width for the resolved cagg
    SELECT bucket_width INTO v_bucket_width
      FROM time_series.continuous_agg WHERE cagg_id = v_cagg_id;

    -- 3. start_offset > end_offset (window non-empty)
    IF p_start_offset IS NOT NULL AND p_end_offset IS NOT NULL
       AND p_start_offset <= p_end_offset THEN
        RAISE EXCEPTION
            'start_offset must be greater than end_offset (refresh window must be non-empty)'
            USING HINT = 'Set start_offset to an earlier time than end_offset, '
                         'e.g. start_offset => ''2 days'', end_offset => ''0''.';
    END IF;

    -- 4. window covers at least 2 buckets
    IF p_start_offset IS NOT NULL AND p_end_offset IS NOT NULL
       AND v_bucket_width IS NOT NULL
       AND (p_start_offset - p_end_offset) < (v_bucket_width * 2) THEN
        RAISE EXCEPTION
            'refresh window (%) must cover at least 2 buckets (2 × % = %)',
            p_start_offset - p_end_offset,
            v_bucket_width,
            v_bucket_width * 2
            USING HINT = 'Increase start_offset or decrease end_offset '
                         'so the window spans more than two buckets.';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION time_series.add_continuous_aggregate_policy(
    continuous_aggregate      regclass,
    start_offset              interval,
    end_offset                interval,
    schedule_interval         interval,
    if_not_exists             bool        DEFAULT false,
    initial_start             timestamptz DEFAULT NULL,
    timezone                  text        DEFAULT NULL,
    buckets_per_batch         int         DEFAULT 10,
    max_batches_per_execution int         DEFAULT 0,
    refresh_newest_first      bool        DEFAULT true
) RETURNS int AS $$
DECLARE
    cagg_name   text;
    v_cagg_id   int;
    v_job_id    int;
    v_app_name  text;
    v_config    jsonb;
BEGIN
    -- Resolve the regclass to a fully schema-qualified, quote_ident'd
    -- name.  Everything below (and the stored config) uses this text
    -- form: it round-trips through to_regclass / regclassin regardless
    -- of the BGW worker's search_path, so the policy can re-resolve the
    -- CAGG at execution time.  Accepting a regclass (matching upstream)
    -- also validates existence at call time and handles quoting.
    SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
      INTO cagg_name
      FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE c.oid = continuous_aggregate;

    -- Look up CAGG (schema-qualified via to_regclass to disambiguate
    -- same-named CAGGs in different schemas — see _resolve_cagg_id).
    v_cagg_id := time_series._resolve_cagg_id(cagg_name);

    IF v_cagg_id IS NULL THEN
        RAISE EXCEPTION 'continuous aggregate "%" does not exist', cagg_name;
    END IF;

    -- Owner check: caller must be a member of the CAGG view's owner role
    -- (which transitively includes the owner itself and superuser).
    -- Without this, in a multi-tenant setup where the DBA has granted
    -- table-level INSERT/UPDATE/DELETE on bgw_job + bgw_job_stat to PUBLIC
    -- (a typical "let users manage their own policies" configuration), an
    -- unrelated user could squat on another user's CAGG by adding a policy
    -- before its real owner does — taking ownership of the policy and
    -- blocking the real owner (one-policy-per-CAGG) from adding their own.
    -- Mirrors the owner check in alter_job / remove_continuous_aggregate_policy
    -- and the C-side bgw_job_permission_check.
    DECLARE
        v_view_schema name;
        v_view_name   name;
        v_view_owner  regrole;
    BEGIN
        SELECT user_view_schema, user_view_name
          INTO v_view_schema, v_view_name
          FROM time_series.continuous_agg WHERE cagg_id = v_cagg_id;

        SELECT relowner::regrole INTO v_view_owner
          FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
         WHERE n.nspname = v_view_schema AND c.relname = v_view_name;

        IF NOT pg_has_role(current_user::name, v_view_owner, 'MEMBER') THEN
            RAISE EXCEPTION USING
                ERRCODE = 'insufficient_privilege',
                MESSAGE = format('insufficient permissions to add policy '
                                 'on continuous aggregate "%s"', cagg_name),
                DETAIL  = format('Continuous aggregate is owned by role "%s" '
                                 'but user "%s" does not belong to that role.',
                                 v_view_owner, current_user);
        END IF;
    END;

    -- Validate offset semantics: when both are non-NULL, the refresh window
    -- spans (now() - start_offset, now() - end_offset], so we require
    -- start_offset > end_offset (i.e. start_time < end_time).  Mirrors
    -- upstream add_continuous_aggregate_policy validation.
    IF start_offset IS NOT NULL AND end_offset IS NOT NULL
       AND start_offset <= end_offset THEN
        RAISE EXCEPTION
            'start_offset must be greater than end_offset (refresh window must be non-empty)'
            USING HINT = 'Set start_offset to an earlier time than end_offset, '
                         'e.g. start_offset => ''2 days'', end_offset => ''0''.';
    END IF;

    -- Validate refresh window covers at least 2 buckets when both offsets
    -- are non-NULL.  A window smaller than 2*bucket_width can refresh at
    -- most one bucket and is almost always a configuration mistake.
    -- Mirrors upstream "refresh window too small" check
    --.
    --
    -- Rules:
    --   either offset NULL  → open window, allowed (no validation)
    --   start - end >= 2 * bucket_width → accept
    --   start - end <  2 * bucket_width → reject
    IF start_offset IS NOT NULL AND end_offset IS NOT NULL THEN
        DECLARE
            v_bucket_width interval;
        BEGIN
            SELECT bucket_width INTO v_bucket_width
              FROM time_series.continuous_agg
             WHERE cagg_id = v_cagg_id;

            IF v_bucket_width IS NOT NULL
               AND (start_offset - end_offset) < (v_bucket_width * 2) THEN
                RAISE EXCEPTION
                    'refresh window (%) must cover at least 2 buckets '
                    '(2 × % = %)',
                    start_offset - end_offset,
                    v_bucket_width,
                    v_bucket_width * 2
                    USING HINT = 'Increase start_offset or decrease end_offset '
                                 'so the window spans more than two buckets.';
            END IF;
        END;
    END IF;

    -- Validate timezone string by attempting to set it.  Mirrors
    -- upstream "invalid timezone" check in policy_utils.c — gives a
    -- clear error at policy-creation time instead of failing later inside
    -- the BGW worker when the scheduler tries to compute next_start.
    IF timezone IS NOT NULL THEN
        BEGIN
            PERFORM set_config('timezone', timezone, true);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid timezone "%"', timezone
                USING HINT = 'Pick a value from pg_timezone_names.';
        END;
    END IF;

    -- Check for existing policy (one policy per CAGG)
    SELECT id INTO v_job_id
    FROM time_series.bgw_job
    WHERE hypertable_id = v_cagg_id
      AND proc_name = 'policy_refresh_cagg';

    IF v_job_id IS NOT NULL THEN
        IF if_not_exists THEN
            RAISE NOTICE 'policy already exists for "%", skipping', cagg_name;
            RETURN v_job_id;
        END IF;
        RAISE EXCEPTION 'refresh policy already exists for continuous aggregate "%"', cagg_name;
    END IF;

    -- Validate batching params.
    --
    -- buckets_per_batch controls window splitting at policy execution
    -- time: when > 0, the policy slices its computed window into N
    -- bucket-aligned sub-windows and refreshes each in an independent
    -- transaction.  Each sub-window's results become visible immediately
    -- on commit, so a long historical backfill no longer holds the mat
    -- table lock for minutes nor loses progress on mid-run crash.
    --
    -- max_batches_per_execution caps how many sub-windows one bgw
    -- worker tick processes; the remaining sub-windows are picked up on
    -- the next schedule_interval tick.  This bounds how long a single
    -- policy invocation holds the bgw worker (matters when many
    -- policies share a small worker pool).
    --
    -- Defaults mirror upstream exactly (continuous_aggregate_api.c
    -- DEFAULT_BUCKETS_PER_BATCH=10, DEFAULT_MAX_BATCHES_PER_EXECUTION=0):
    -- split the window into 10-bucket batches and keep going until the
    -- window is drained.  Both settings describe the SAME work, but the
    -- transaction granularity differs sharply: batching commits every 10
    -- buckets, so a refresh interrupted by a crash or worker kill keeps
    -- everything already committed, and the per-chunk locks it holds are
    -- released and re-taken between batches instead of spanning the whole
    -- window.  0 buckets_per_batch (the previous default) ran the entire
    -- window as one transaction -- longer lock hold, and an interruption
    -- lost the lot.  Negative values are rejected with the same
    -- message/detail/hint wording as upstream
    -- (continuous_aggregate_api.c).
    IF buckets_per_batch < 0 THEN
        RAISE EXCEPTION 'invalid buckets per batch'
            USING DETAIL = format('buckets_per_batch: %s', buckets_per_batch),
                  HINT   = 'The buckets per batch should be greater than or equal to zero.';
    END IF;
    IF max_batches_per_execution < 0 THEN
        RAISE EXCEPTION 'invalid max batches per execution'
            USING DETAIL = format('max_batches_per_execution: %s', max_batches_per_execution),
                  HINT   = 'The max batches per execution should be greater than or equal to zero.';
    END IF;

    -- Build JSONB config.  buckets_per_batch / max_batches_per_execution
    -- are stored unconditionally so existing rows can be migrated by
    -- jsonb_set without per-row branching.
    v_config := jsonb_build_object(
        'cagg_name', cagg_name,
        'start_offset', start_offset::text,
        'end_offset', end_offset::text,
        'buckets_per_batch', buckets_per_batch,
        'max_batches_per_execution', max_batches_per_execution,
        'refresh_newest_first', refresh_newest_first
    );

    -- Insert job
    v_app_name := 'Refresh CAGG Policy [' || cagg_name || ']';
    -- max_runtime / max_retries default to upstream-compatible values
    -- ('0' = no timeout, -1 = unlimited retries) but operators can
    -- override via the time_series.cagg_default_max_runtime and
    -- time_series.cagg_default_max_retries GUCs (see scheduler.c).
    --
    -- initial_start / timezone:
    --   - NULL initial_start  → anchor to now() so the first run fires
    --     immediately (legacy behavior).
    --   - non-NULL            → align fixed-schedule ticks at that anchor.
    --   - timezone is honored by the scheduler's next_start computation
    --     (see job_stat.c: calculate_next_start_on_success/failure).
    INSERT INTO time_series.bgw_job
        (application_name, schedule_interval, max_runtime, max_retries,
         retry_period, proc_schema, proc_name, scheduled, fixed_schedule,
         initial_start, hypertable_id, config, timezone)
    VALUES
        (v_app_name::name,
         schedule_interval,
         current_setting('time_series.cagg_default_max_runtime')::interval,
         current_setting('time_series.cagg_default_max_retries')::int,
         '5 minutes'::interval, 'time_series'::name, 'policy_refresh_cagg'::name,
         true, true,
         COALESCE(initial_start, now()), v_cagg_id, v_config, timezone)
    RETURNING id INTO v_job_id;

    -- Initialize job stat.  When the caller provided an explicit
    -- initial_start in the future, honor it so the worker doesn't fire
    -- immediately; otherwise default to now() to mirror the legacy
    -- "fire on first tick" behavior.
    INSERT INTO time_series.bgw_job_stat (job_id, next_start)
    VALUES (v_job_id, COALESCE(initial_start, now()));

    -- Broadcast relcache invalidation so the scheduler reloads on next tick
    PERFORM time_series.bgw_invalidate_cache();

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION time_series.remove_continuous_aggregate_policy(
    continuous_aggregate regclass,
    if_exists   bool DEFAULT false
) RETURNS void AS $$
DECLARE
    cagg_name   text;
    v_cagg_id   int;
    v_job_id    int;
BEGIN
    -- Schema-qualified, quote_ident'd name from the regclass (matching
    -- upstream's regclass signature).  Note: a non-existent relation
    -- errors at the regclass cast before this body runs, so if_exists
    -- only governs the "relation exists but has no policy" case.
    SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
      INTO cagg_name
      FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE c.oid = continuous_aggregate;

    -- Resolve via search_path-aware helper (see _resolve_cagg_id).
    v_cagg_id := time_series._resolve_cagg_id(cagg_name);

    IF v_cagg_id IS NULL THEN
        IF if_exists THEN RETURN; END IF;
        RAISE EXCEPTION 'continuous aggregate "%" does not exist', cagg_name;
    END IF;

    DECLARE
        v_owner regrole;
    BEGIN
        SELECT id, owner INTO v_job_id, v_owner
        FROM time_series.bgw_job
        WHERE hypertable_id = v_cagg_id
          AND proc_name = 'policy_refresh_cagg';

        IF v_job_id IS NULL THEN
            IF if_exists THEN RETURN; END IF;
            RAISE EXCEPTION 'no refresh policy exists for continuous aggregate "%"', cagg_name;
        END IF;

        -- Owner check: caller must be a member of the policy's owner role.
        IF NOT pg_has_role(current_user::name, v_owner, 'MEMBER') THEN
            RAISE EXCEPTION USING
                ERRCODE = 'insufficient_privilege',
                MESSAGE = format('insufficient permissions to remove policy on continuous aggregate "%s"', cagg_name),
                DETAIL  = format('Policy is owned by role "%s" but user "%s" does not belong to that role.',
                                 v_owner, current_user);
        END IF;
    END;

    DELETE FROM time_series.bgw_job_stat WHERE job_id = v_job_id;
    DELETE FROM time_series.bgw_job WHERE id = v_job_id;

    -- Broadcast relcache invalidation so the scheduler drops the removed job
    PERFORM time_series.bgw_invalidate_cache();
END;
$$ LANGUAGE plpgsql;

-- =========================================================================
--  Compression policy
--
--  Auto-compress eligible chunks (data older than `compress_after`) on a
--  schedule.  Mirrors TSDB add_compression_policy / add_columnstore_policy:
--  stores the policy as a row in bgw_job with proc_name='policy_compression'
--  and a jsonb config carrying (table_oid, compress_after).  Each tick
--  calls compress_and_reclaim which compresses eligible chunks and reclaims
--  their heap-fork disk space in two committed phases.
--
--  Defensive cleanup: if the target table was dropped without first calling
--  remove_compression_policy, the policy_compression proc detects the
--  missing pg_class row, deletes its own bgw_job/bgw_job_stat rows, and
--  returns — avoiding an infinite-retry loop on a stale relid.
-- =========================================================================
-- Parameter names use `_` prefix to avoid colliding with the `job_id`
-- column in bgw_job_stat / bgw_job — PL/pgSQL flags the bare `job_id`
-- reference in WHERE as ambiguous otherwise.  Types are what add_job
-- inspects; names are free to differ from TSDB's signature.
CREATE OR REPLACE PROCEDURE time_series.policy_compression(_job_id int, _config jsonb)
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid       oid;
    v_table_regclass  regclass;
    v_compress_after  interval;
BEGIN
    IF _config IS NULL THEN
        RAISE EXCEPTION 'policy_compression: config is NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_table_oid      := (_config->>'table_oid')::oid;
    v_compress_after := (_config->>'compress_after')::interval;

    -- Lazy guard: if the target table is gone, disable the policy and
    -- exit.  We don't DELETE the bgw_job row here because the BGW
    -- dispatcher's mark_end step (separate xact, runs after this proc
    -- returns) still needs to find the matching bgw_job_stat row to
    -- record the run.  Setting scheduled=false stops future ticks; the
    -- operator can then remove the dangling row via
    -- remove_compression_policy when convenient.
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = v_table_oid) THEN
        UPDATE time_series.bgw_job
           SET scheduled = false
         WHERE id = _job_id;
        -- Broadcast relcache inval so the scheduler drops this row from
        -- its in-memory scheduled_jobs cache.  Without it, the cached
        -- entry still carries scheduled=true and the BGW dispatcher
        -- keeps firing this proc every schedule_interval until the next
        -- unrelated bgw_job DDL triggers a cache reload — a noisy busy
        -- loop in pg_stat_activity / bgw_job_stat_history.
        PERFORM time_series.bgw_invalidate_cache();
        RAISE NOTICE 'compression policy disabled: target table (oid=%) no longer exists; '
                     'run remove_compression_policy() to clean up',
                     v_table_oid;
        RETURN;
    END IF;

    v_table_regclass := v_table_oid::regclass;

    -- Delegate to compress_and_reclaim, which handles the
    -- compress → COMMIT → reclaim → COMMIT split that compress_chunks's
    -- PreventInTransactionBlock requires.
    CALL time_series.compress_and_reclaim(v_table_regclass, v_compress_after);
END $$;

COMMENT ON PROCEDURE time_series.policy_compression(int, jsonb)
IS 'Internal: BGW dispatch target for add_compression_policy.';

CREATE OR REPLACE FUNCTION time_series.add_compression_policy(
    table_name         regclass,
    compress_after     interval,
    schedule_interval  interval     DEFAULT INTERVAL '12 hours',
    if_not_exists      boolean      DEFAULT false,
    initial_start      timestamptz  DEFAULT NULL,
    timezone           text         DEFAULT NULL
) RETURNS int AS $$
DECLARE
    v_job_id    int;
    v_owner     regrole;
    v_app_name  name;
    v_config    jsonb;
BEGIN
    -- compress_after must be positive: a recurring policy that compresses
    -- the current chunk before it is written-out would race INSERT/compress
    -- and isn't a useful default.
    IF compress_after IS NULL OR compress_after <= '0'::interval THEN
        RAISE EXCEPTION 'compress_after must be a positive interval'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    IF schedule_interval IS NULL OR schedule_interval <= '0'::interval THEN
        RAISE EXCEPTION 'schedule_interval must be a positive interval'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Owner check: caller must be a member of the target table's owner role.
    -- Mirrors add_continuous_aggregate_policy.  Without this, in a multi-
    -- tenant cluster where bgw_job/bgw_job_stat INSERT is granted to PUBLIC,
    -- an unrelated user could squat on another user's table by registering
    -- a policy before its real owner does.
    SELECT relowner::regrole INTO v_owner
      FROM pg_class WHERE oid = table_name;
    IF NOT pg_has_role(current_user::name, v_owner, 'MEMBER') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'insufficient_privilege',
            MESSAGE = format('insufficient permissions to add compression policy on "%s"',
                             table_name),
            DETAIL  = format('Table is owned by role "%s" but user "%s" does not belong to that role.',
                             v_owner, current_user);
    END IF;

    -- Validate timezone string by attempting to set it (same approach
    -- as add_continuous_aggregate_policy).
    IF timezone IS NOT NULL THEN
        BEGIN
            PERFORM set_config('timezone', timezone, true);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid timezone "%"', timezone
                USING HINT = 'Pick a value from pg_timezone_names.';
        END;
    END IF;

    -- One-policy-per-table: a table with two competing schedules would
    -- racy / waste work.  Look up by oid stored in config (matches the
    -- exact relid; cagg pattern uses the typed hypertable_id column).
    SELECT id INTO v_job_id
      FROM time_series.bgw_job
     WHERE proc_schema = 'time_series'
       AND proc_name   = 'policy_compression'
       AND (config->>'table_oid')::oid = table_name::oid;

    IF v_job_id IS NOT NULL THEN
        IF if_not_exists THEN
            RAISE NOTICE 'compression policy already exists for "%", skipping', table_name;
            RETURN v_job_id;
        END IF;
        RAISE EXCEPTION 'compression policy already exists for "%"', table_name;
    END IF;

    v_config := jsonb_build_object(
        'table_oid',      table_name::oid::int4,
        'compress_after', compress_after::text
    );

    v_app_name := ('Compression Policy [' || table_name::text || ']')::name;

    -- max_runtime = 0 (no timeout): compress can run for minutes/hours on
    -- backlog catch-up; killing it mid-run risks orphan PAX files until
    -- the next successful run cleans them up.
    -- max_retries = -1, retry_period = 1h: matches TSDB compression
    -- policy defaults.
    INSERT INTO time_series.bgw_job
        (application_name, schedule_interval, max_runtime, max_retries,
         retry_period, proc_schema, proc_name, scheduled, fixed_schedule,
         initial_start, hypertable_id, config, timezone)
    VALUES
        (v_app_name,
         schedule_interval,
         '0'::interval,
         -1,
         '1 hour'::interval,
         'time_series'::name, 'policy_compression'::name,
         true, true,
         COALESCE(initial_start, now()),
         table_name::oid::int4,
         v_config,
         timezone)
    RETURNING id INTO v_job_id;

    INSERT INTO time_series.bgw_job_stat (job_id, next_start)
    VALUES (v_job_id, COALESCE(initial_start, now()));

    -- Wake the scheduler so it picks up the new job without waiting for
    -- the next tick.
    PERFORM time_series.bgw_invalidate_cache();

    RETURN v_job_id;
END $$ LANGUAGE plpgsql;

COMMENT ON FUNCTION time_series.add_compression_policy(regclass, interval, interval, boolean, timestamptz, text)
IS 'Register an auto-compression policy: chunks whose data is older than compress_after are compressed and reclaimed on each tick of schedule_interval.';

CREATE OR REPLACE FUNCTION time_series.remove_compression_policy(
    table_name regclass,
    if_exists  boolean DEFAULT false
) RETURNS void AS $$
DECLARE
    v_job_id int;
    v_owner  regrole;
BEGIN
    SELECT id, owner INTO v_job_id, v_owner
      FROM time_series.bgw_job
     WHERE proc_schema = 'time_series'
       AND proc_name   = 'policy_compression'
       AND (config->>'table_oid')::oid = table_name::oid;

    IF v_job_id IS NULL THEN
        IF if_exists THEN RETURN; END IF;
        RAISE EXCEPTION 'no compression policy exists for "%"', table_name;
    END IF;

    IF NOT pg_has_role(current_user::name, v_owner, 'MEMBER') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'insufficient_privilege',
            MESSAGE = format('insufficient permissions to remove compression policy on "%s"',
                             table_name),
            DETAIL  = format('Policy is owned by role "%s" but user "%s" does not belong to that role.',
                             v_owner, current_user);
    END IF;

    DELETE FROM time_series.bgw_job_stat WHERE job_id = v_job_id;
    DELETE FROM time_series.bgw_job WHERE id = v_job_id;

    PERFORM time_series.bgw_invalidate_cache();
END $$ LANGUAGE plpgsql;

COMMENT ON FUNCTION time_series.remove_compression_policy(regclass, boolean)
IS 'Remove a compression policy registered via add_compression_policy.';

-- =========================================================================
--  Job Stat History Retention Policy (built-in)
-- =========================================================================
--
-- 1:1 port of upstream sql/job_stat_history_log_retention.sql.
--
-- Design points (verbatim from upstream):
--   - Built-in policy registered at extension install (id = 1) — there is
--     no add_*/remove_* helper.  Operators tune it via alter_job(1, ...).
--   - policy proc is a FUNCTION RETURNS integer (deleted row count), not
--     a PROCEDURE.  Generic dispatcher in bgw_job_execute_real handles
--     both prokinds.
--   - Separate _check function validates config and is wired through
--     bgw_job.check_schema/check_name (see the reference policy job
--     ::policy_invoke_check).
--   - SET search_path TO pg_catalog, pg_temp inside both functions —
--     standard PG hardening so an attacker who controls search_path on
--     a calling session can't shadow built-in functions.
CREATE OR REPLACE FUNCTION time_series.policy_job_stat_history_retention(
    job_id integer, config jsonb) RETURNS integer
LANGUAGE plpgsql AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
BEGIN
    drop_after := config->>'drop_after';

    -- Delete BOTH closed rows (execution_finish set by mark_end) AND
    -- ghost rows (worker crashed mid-run; execution_finish stays NULL
    -- forever).  upstream upstream retention only checks execution_finish,
    -- which leaves crashed-run rows permanently in the table — a real
    -- bug under sustained operation when coordinator restarts / OOM
    -- kills accumulate over time.  We diverge from upstream here on purpose;
    -- the divergence is a single OR clause and is documented inline.
    DELETE
    FROM time_series.bgw_job_stat_history
    WHERE (execution_finish < (now() - drop_after))
       OR (execution_finish IS NULL
           AND execution_start < (now() - drop_after));

    GET DIAGNOSTICS numrows = ROW_COUNT;

    RETURN numrows;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION time_series.policy_job_stat_history_retention_check(
    config jsonb) RETURNS VOID
LANGUAGE plpgsql AS
$BODY$
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;

    IF config->>'drop_after' IS NULL THEN
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Insert the singleton retention job at install time.  ON CONFLICT (id)
-- DO NOTHING is the idempotency guard for re-installs / future upgrade
-- migrations (mirrors upstream).
INSERT INTO time_series.bgw_job (
    id,
    application_name,
    schedule_interval,
    max_runtime,
    max_retries,
    retry_period,
    proc_schema,
    proc_name,
    owner,
    scheduled,
    config,
    check_schema,
    check_name,
    fixed_schedule,
    initial_start
)
VALUES
(
    1,
    'Job History Log Retention Policy [1]',
    INTERVAL '1 month',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    'time_series',
    'policy_job_stat_history_retention',
    pg_catalog.quote_ident(current_role)::regrole,
    true,
    '{"drop_after":"1 month"}',
    'time_series',
    'policy_job_stat_history_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;

-- Advance the user-job sequence past the reserved built-in id range so
-- nextval() doesn't collide with the id=1 retention job.  Safe even on
-- re-install: setval is idempotent w.r.t. is_called=true.
SELECT pg_catalog.setval('time_series.bgw_job_id_seq', 1000, true);

-- Policy status view — analogous to the standard job_stats view.
--
-- Intentionally NOT filtered by ownership.  upstream job_stats / jobs /
-- continuous_aggregates views are public to anyone with SELECT, on the
-- design judgment that policy metadata (cagg_name, schedule, run counts)
-- is operational status info, not sensitive data.  job_history /
-- job_errors are the views that DO filter, because they expose config
-- jsonb and error messages.  We mirror that split: this view is
-- unfiltered; job_history above filters by owner + database owner.
CREATE VIEW time_series.cagg_policy_stats AS
SELECT ca.user_view_schema || '.' || ca.user_view_name AS cagg_name,
       j.id AS job_id,
       j.schedule_interval,
       j.max_runtime,
       j.max_retries,
       j.config->>'start_offset' AS start_offset,
       j.config->>'end_offset' AS end_offset,
       j.scheduled AS active,
       s.last_start,
       s.last_finish,
       s.last_successful_finish,
       s.next_start,
       s.last_run_success,
       s.total_runs,
       s.total_successes,
       s.total_failures,
       s.total_crashes,
       s.consecutive_failures
FROM time_series.bgw_job j
JOIN time_series.continuous_agg ca ON ca.cagg_id = j.hypertable_id
LEFT JOIN time_series.bgw_job_stat s ON s.job_id = j.id
WHERE j.proc_name = 'policy_refresh_cagg';

-- =========================================================================
--  compression_policy_stats: per-table operational status of compression
--  policies registered via add_compression_policy.  Sister view to
--  cagg_policy_stats; same column shape with table_name and compress_after
--  in place of the cagg-specific columns.
-- =========================================================================
CREATE VIEW time_series.compression_policy_stats AS
SELECT (j.config->>'table_oid')::oid::regclass         AS table_name,
       j.id                                            AS job_id,
       j.schedule_interval,
       j.max_runtime,
       j.max_retries,
       (j.config->>'compress_after')::interval         AS compress_after,
       j.scheduled                                     AS active,
       s.last_start,
       s.last_finish,
       s.last_successful_finish,
       s.next_start,
       s.last_run_success,
       s.total_runs,
       s.total_successes,
       s.total_failures,
       s.total_crashes,
       s.consecutive_failures
FROM time_series.bgw_job j
LEFT JOIN time_series.bgw_job_stat s ON s.job_id = j.id
WHERE j.proc_name = 'policy_compression';

COMMENT ON VIEW time_series.compression_policy_stats
IS 'Operational status of compression policies registered via add_compression_policy.';

-- =========================================================================
--  continuous_aggregates: per-CAGG overview view
--
--  Mirrors the standard continuous_aggregates.
--  One row per CAGG with all the diagnostic dimensions a DBA needs:
--    * Identity (schema, name, source table)
--    * Bucketing parameters (width, origin, offset, timezone)
--    * Watermark range (min / max across segments — uneven across MPP
--      segments would indicate L1->L2 migration drift)
--    * Real-time mode (materialized_only)
--    * Refresh policy (active or not, schedule, last/next run)
--
--  The watermark min/max comes from cagg_watermark, which is per-segment
--  in CBDB; min<max means some segments are behind, useful for spotting
--  partial-refresh issues.  upstream single-node version reports a single
--  watermark — we expose both bounds so MPP-specific drift is visible.
-- =========================================================================
CREATE VIEW time_series.continuous_aggregates
WITH (security_barrier = true) AS
SELECT ca.user_view_schema::name      AS view_schema,
       ca.user_view_name::name        AS view_name,
       ca.user_view_schema || '.' || ca.user_view_name AS view_full_name,
       ca.cagg_id,
       (ca.source_table_oid::regclass)::text AS source_table,
       ca.bucket_column,
       ca.materialized_only,
       ca.created_at,
       bf.bucket_width,
       bf.bucket_origin,
       bf.bucket_offset,
       bf.bucket_timezone,
       (SELECT min(watermark) FROM time_series.cagg_watermark
         WHERE cagg_id = ca.cagg_id)  AS min_watermark,
       (SELECT max(watermark) FROM time_series.cagg_watermark
         WHERE cagg_id = ca.cagg_id)  AS max_watermark,
       j.id                            AS policy_job_id,
       j.scheduled                     AS policy_active,
       j.schedule_interval             AS policy_schedule_interval,
       (j.config->>'start_offset')::interval AS policy_start_offset,
       (j.config->>'end_offset')::interval   AS policy_end_offset,
       s.last_start                    AS policy_last_start,
       s.last_finish                   AS policy_last_finish,
       s.last_successful_finish        AS policy_last_successful_finish,
       s.next_start                    AS policy_next_start,
       s.last_run_success              AS policy_last_run_success,
       s.total_runs                    AS policy_total_runs,
       s.total_successes               AS policy_total_successes,
       s.total_failures                AS policy_total_failures,
       s.consecutive_failures          AS policy_consecutive_failures
  FROM time_series.continuous_agg ca
  LEFT JOIN time_series.cagg_bucket_function bf
         ON bf.cagg_id = ca.cagg_id
  LEFT JOIN time_series.bgw_job j
         ON j.hypertable_id = ca.cagg_id
        AND j.proc_name = 'policy_refresh_cagg'
  LEFT JOIN time_series.bgw_job_stat s
         ON s.job_id = j.id
 WHERE pg_catalog.pg_has_role(current_user::name,
                              (SELECT pg_catalog.pg_get_userbyid(datdba)
                                 FROM pg_catalog.pg_database
                                WHERE datname = current_database()),
                              'MEMBER') IS TRUE
    OR pg_catalog.has_table_privilege(current_user::name,
                                      ca.source_table_oid,
                                      'SELECT') IS TRUE;

GRANT SELECT ON time_series.continuous_aggregates TO PUBLIC;

-- =========================================================================
--  job_errors: convenience view filtering job_history to failures only
--
--  Mirrors the standard job_errors.  Equivalent
--  to `SELECT * FROM job_history WHERE NOT succeeded` but gives DBAs a
--  more discoverable name and pulls out the most useful error fields
--  to top level so they don't have to ->> through JSONB by hand.
-- =========================================================================
CREATE VIEW time_series.job_errors
WITH (security_barrier = true) AS
SELECT h.id,
       h.job_id,
       h.proc_schema,
       h.proc_name,
       h.pid,
       h.execution_start    AS start_time,
       h.execution_finish   AS finish_time,
       h.duration,
       h.is_crashed,
       h.error_data->>'sqlerrcode'   AS sqlerrcode,
       h.error_data->>'message'      AS err_message,
       h.error_data->>'detail'       AS err_detail,
       h.error_data->>'hint'         AS err_hint,
       h.error_data->>'funcname'     AS err_funcname,
       h.error_data->>'filename'     AS err_filename,
       h.error_data->>'lineno'       AS err_lineno,
       h.error_data->>'schema_name'  AS err_schema_name,
       h.error_data->>'table_name'   AS err_table_name,
       h.error_data                  AS error_data,
       h.config
  FROM time_series.job_history h
 WHERE h.succeeded IS NOT TRUE;

GRANT SELECT ON time_series.job_errors TO PUBLIC;

-- =========================================================================
--  alter_job: modify a policy's schedule / config in place
--
--  Supports the parameters most commonly needed for BGW policy maintenance.
--  All COALESCE-style: pass NULL (or omit) to leave a field untouched.
--
--  Notes:
--   - For CAGG policies, modifying `config` re-validates that `cagg_name`
--     is still present.  Other fields are not validated structurally.
--   - When `schedule_interval` is changed, the next_start in bgw_job_stat is
--     recomputed to (now() + schedule_interval) so the change takes effect on
--     the next scheduler tick.
--   - `next_start` overrides any other recomputation when explicitly given.
-- =========================================================================
CREATE OR REPLACE FUNCTION time_series.alter_job(
    job_id              int,
    schedule_interval   interval     DEFAULT NULL,
    max_runtime         interval     DEFAULT NULL,
    max_retries         int          DEFAULT NULL,
    retry_period        interval     DEFAULT NULL,
    scheduled           bool         DEFAULT NULL,
    config              jsonb        DEFAULT NULL,
    next_start          timestamptz  DEFAULT NULL,
    if_exists           bool         DEFAULT false,
    fixed_schedule      bool         DEFAULT NULL,
    initial_start       timestamptz  DEFAULT NULL,
    timezone            text         DEFAULT NULL
) RETURNS time_series.bgw_job AS $$
DECLARE
    v_job time_series.bgw_job;
BEGIN
    SELECT * INTO v_job FROM time_series.bgw_job WHERE id = alter_job.job_id;

    IF NOT FOUND THEN
        IF if_exists THEN
            RAISE NOTICE 'job % does not exist, skipping', job_id;
            RETURN NULL;
        END IF;
        RAISE EXCEPTION 'job % does not exist', job_id;
    END IF;

    -- Owner check: caller must be a member of the job's owner role
    -- (which includes the owner itself and superuser).  Mirrors upstream
    -- bgw_job_permission_check in src/bgw/job.c.
    IF NOT pg_has_role(current_user::name, v_job.owner, 'MEMBER') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'insufficient_privilege',
            MESSAGE = format('insufficient permissions to alter job %s', job_id),
            DETAIL  = format('Job %s is owned by role "%s" but user "%s" does not belong to that role.',
                             job_id, v_job.owner, current_user);
    END IF;

    -- Validate config for known proc types.  The user-supplied config
    -- replaces the existing one wholesale (see COALESCE below), so we
    -- must check it as a complete spec — anything alter_job accepts
    -- here, the BGW worker will execute next tick.  Without these checks
    -- a typo (ghost cagg_name) or a too-narrow window silently breaks
    -- a working policy and the failure only surfaces in mark_end as
    -- last_run_success=false, which DBAs find hard to diagnose.
    IF config IS NOT NULL AND v_job.proc_name = 'policy_refresh_cagg' THEN
        PERFORM time_series._validate_cagg_policy_config(
            config->>'cagg_name',
            (config->>'start_offset')::interval,
            (config->>'end_offset')::interval);
    END IF;

    -- Validate timezone if supplied (mirrors add_continuous_aggregate_policy
    -- and upstream policy_utils.c): fail fast at alter time instead of later
    -- inside the BGW worker when it computes next_start.
    IF alter_job.timezone IS NOT NULL THEN
        BEGIN
            PERFORM set_config('timezone', alter_job.timezone, true);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid timezone "%"', alter_job.timezone
                USING HINT = 'Pick a value from pg_timezone_names.';
        END;
    END IF;

    -- Apply COALESCE-style updates.  fixed_schedule / initial_start /
    -- timezone mirror upstream alter_job so a CAGG (or any) policy can
    -- change its schedule anchoring after creation, not only at add time.
    UPDATE time_series.bgw_job
       SET schedule_interval = COALESCE(alter_job.schedule_interval, bgw_job.schedule_interval),
           max_runtime       = COALESCE(alter_job.max_runtime,       bgw_job.max_runtime),
           max_retries       = COALESCE(alter_job.max_retries,       bgw_job.max_retries),
           retry_period      = COALESCE(alter_job.retry_period,      bgw_job.retry_period),
           scheduled         = COALESCE(alter_job.scheduled,         bgw_job.scheduled),
           config            = COALESCE(alter_job.config,            bgw_job.config),
           fixed_schedule    = COALESCE(alter_job.fixed_schedule,    bgw_job.fixed_schedule),
           initial_start     = COALESCE(alter_job.initial_start,     bgw_job.initial_start),
           timezone          = COALESCE(alter_job.timezone,          bgw_job.timezone)
     WHERE id = alter_job.job_id;

    -- Recompute next_start.  Precedence: an explicit next_start wins;
    -- otherwise a newly-supplied initial_start re-anchors the schedule
    -- (matches upstream, where changing initial_start moves the next
    -- fire to that anchor); otherwise a modified schedule_interval slides
    -- next_start forward.
    IF alter_job.next_start IS NOT NULL THEN
        UPDATE time_series.bgw_job_stat
           SET next_start = alter_job.next_start
         WHERE bgw_job_stat.job_id = alter_job.job_id;
    ELSIF alter_job.initial_start IS NOT NULL THEN
        UPDATE time_series.bgw_job_stat
           SET next_start = alter_job.initial_start
         WHERE bgw_job_stat.job_id = alter_job.job_id;
    ELSIF alter_job.schedule_interval IS NOT NULL THEN
        UPDATE time_series.bgw_job_stat
           SET next_start = now() + alter_job.schedule_interval
         WHERE bgw_job_stat.job_id = alter_job.job_id;
    END IF;

    SELECT * INTO v_job FROM time_series.bgw_job WHERE id = alter_job.job_id;

    -- Broadcast relcache invalidation so the scheduler picks up changes
    PERFORM time_series.bgw_invalidate_cache();

    RETURN v_job;
END;
$$ LANGUAGE plpgsql;

-- =========================================================================
--  run_job: synchronously run a policy in the calling backend
--
--  Equivalent to having the BGW worker fire once now: invokes
--  mark_start → bgw_job_execute → mark_end in the current session.
--
--  This is intentionally a thin wrapper around the same C path used by the
--  real BGW worker (direct C call to cagg_refresh, see
--  doc/feature/cagg/cagg_bgw_debug_journey.md for design rationale).  It
--  enables deterministic regression testing of the worker code path
--  without requiring scheduler tick timing.
--
--  Difference from a real BGW run: this function runs in the caller's
--  session, so SET options (search_path, optimizer, etc.) inherit from
--  the caller.  The bgw_job_stat row is updated identically.
-- =========================================================================
-- Defined as PROCEDURE so that bgw_job_execute() can control transactions
-- (StartTransactionCommand / CommitTransactionCommand) the same way the BGW
-- worker entrypoint does.  Call as: CALL time_series.run_job(jid);
CREATE PROCEDURE time_series.run_job(job_id int)
LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_run_job';

-- =========================================================================
--  add_job / delete_job: generic BGW job management
--
--  Counterparts of upstream add_job / delete_job.  Lets users register
--  and remove arbitrary (int4, jsonb) procedures/functions as scheduled
--  BGW jobs without going through a specific add_*_policy wrapper.
--
--  Security model:
--   - Owner is set to the calling role via bgw_job.owner DEFAULT
--     current_role.  A user cannot impersonate another role, since the
--     BGW worker connects as bgw_job.owner when firing.
--   - add_job: caller must already have EXECUTE on `proc`.  Without
--     this check, a low-priv user could "borrow" the BGW dispatcher to
--     run code they otherwise could not call.
--   - add_job: `proc` signature must be (int4, jsonb) -> void or be a
--     PROCEDURE.  The BGW dispatcher's expression builder will SIGSEGV
--     if fed a mismatched signature, so we reject early.
--   - delete_job: same owner rule as alter_job.
-- =========================================================================
CREATE OR REPLACE FUNCTION time_series.add_job(
    proc              regproc,
    schedule_interval interval,
    config            jsonb        DEFAULT NULL,
    initial_start     timestamptz  DEFAULT NULL,
    scheduled         boolean      DEFAULT true,
    check_config      regproc      DEFAULT NULL,
    fixed_schedule    boolean      DEFAULT true,
    timezone          text         DEFAULT NULL
) RETURNS int AS $$
DECLARE
    v_job_id        int;
    v_proc_schema   name;
    v_proc_name     name;
    v_proc_argtypes oid[];
    v_proc_rettype  oid;
    v_proc_kind     "char";
    v_check_schema  name;
    v_check_name    name;
BEGIN
    -- 1. schedule_interval must be a positive interval (avoid busy-loop)
    IF schedule_interval IS NULL OR schedule_interval <= '0'::interval THEN
        RAISE EXCEPTION 'schedule_interval must be a positive interval'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- 2. proc must exist; we need its details for the signature check.
    -- (REGPROC parsing already errored out if `proc` doesn't resolve at
    -- all, but we still want to fetch schema/name and prokind here.)
    SELECT n.nspname, p.proname, p.proargtypes::oid[],
           p.prorettype, p.prokind
      INTO v_proc_schema, v_proc_name, v_proc_argtypes,
           v_proc_rettype, v_proc_kind
      FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
     WHERE p.oid = proc;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'function or procedure % not found', proc
            USING ERRCODE = 'undefined_function';
    END IF;

    -- 3. Signature must be (int4, jsonb).  The BGW dispatcher in
    -- bgw_job_execute_real builds a FuncExpr with these two argument
    -- types and passes it to ExecuteCallStmt / ExecEvalExpr.
    --
    -- pg_proc.proargtypes::oid[] is *zero-based* (the cast preserves the
    -- underlying oidvector's lower bound of 0).  Index by 0 and 1, not
    -- the usual 1 and 2.
    IF coalesce(array_length(v_proc_argtypes, 1), 0) <> 2
       OR v_proc_argtypes[0] <> 'pg_catalog.int4'::regtype::oid
       OR v_proc_argtypes[1] <> 'pg_catalog.jsonb'::regtype::oid THEN
        RAISE EXCEPTION 'job proc must accept exactly two arguments (int4, jsonb)'
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = format('"%s.%s" has a different signature.',
                                v_proc_schema, v_proc_name);
    END IF;

    -- 4. For FUNCTIONs (prokind = 'f'), require return type void.
    -- PROCEDUREs ('p') return whatever; ExecuteCallStmt handles it.
    IF v_proc_kind = 'f'
       AND v_proc_rettype <> 'pg_catalog.void'::regtype::oid THEN
        RAISE EXCEPTION 'job function must return void'
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = format('"%s.%s" returns %s; declare it RETURNS void '
                                'or define it as a PROCEDURE.',
                                v_proc_schema, v_proc_name,
                                format_type(v_proc_rettype, NULL));
    END IF;

    -- 5. Caller must have EXECUTE on proc.
    IF NOT pg_catalog.has_function_privilege(current_user::text, proc, 'EXECUTE') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'insufficient_privilege',
            MESSAGE = format('permission denied for function %s.%s',
                             v_proc_schema, v_proc_name),
            DETAIL  = format('User "%s" does not have EXECUTE on the proc.',
                             current_user);
    END IF;

    -- 6. Resolve check_config (if supplied), require EXECUTE, run it.
    IF check_config IS NOT NULL THEN
        SELECT n.nspname, p.proname
          INTO v_check_schema, v_check_name
          FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
         WHERE p.oid = check_config;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'check_config function % not found', check_config
                USING ERRCODE = 'undefined_function';
        END IF;

        IF NOT pg_catalog.has_function_privilege(current_user::text,
                                                 check_config, 'EXECUTE') THEN
            RAISE EXCEPTION USING
                ERRCODE = 'insufficient_privilege',
                MESSAGE = format('permission denied for function %s.%s',
                                 v_check_schema, v_check_name);
        END IF;

        EXECUTE format('SELECT %I.%I($1)', v_check_schema, v_check_name)
          USING config;
    END IF;

    -- 7. Validate timezone string by attempting to set it.
    IF timezone IS NOT NULL THEN
        BEGIN
            PERFORM set_config('timezone', timezone, true);
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid timezone "%"', timezone
                USING HINT = 'Pick a value from pg_timezone_names.';
        END;
    END IF;

    -- 8. INSERT the job.  hypertable_id = 0 marks it as a generic job
    -- (CAGG policies set this to a real cagg_id).
    INSERT INTO time_series.bgw_job
        (application_name, schedule_interval, max_runtime, max_retries,
         retry_period, proc_schema, proc_name, scheduled, fixed_schedule,
         initial_start, hypertable_id, config,
         check_schema, check_name, timezone)
    VALUES
        (('User-Defined Action [' || proc::text || ']')::name,
         schedule_interval,
         '0'::interval,    -- no per-job runtime cap by default
         -1,               -- unlimited retries by default
         '5 minutes'::interval,
         v_proc_schema, v_proc_name,
         scheduled, fixed_schedule,
         COALESCE(initial_start, now()),
         0,                -- hypertable_id reserved for cagg policies
         config,
         v_check_schema, v_check_name,
         timezone)
    RETURNING id INTO v_job_id;

    INSERT INTO time_series.bgw_job_stat (job_id, next_start)
    VALUES (v_job_id, COALESCE(initial_start, now()));

    PERFORM time_series.bgw_invalidate_cache();

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION time_series.delete_job(job_id int)
RETURNS void AS $$
DECLARE
    v_job time_series.bgw_job;
BEGIN
    SELECT * INTO v_job FROM time_series.bgw_job
     WHERE id = delete_job.job_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'job % does not exist', job_id;
    END IF;

    -- Owner check: same rule as alter_job / remove_continuous_aggregate_policy.
    IF NOT pg_has_role(current_user::name, v_job.owner, 'MEMBER') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'insufficient_privilege',
            MESSAGE = format('insufficient permissions to delete job %s', job_id),
            DETAIL  = format('Job %s is owned by role "%s" but user "%s" '
                             'does not belong to that role.',
                             job_id, v_job.owner, current_user);
    END IF;

    DELETE FROM time_series.bgw_job_stat
     WHERE bgw_job_stat.job_id = delete_job.job_id;
    DELETE FROM time_series.bgw_job
     WHERE bgw_job.id          = delete_job.job_id;

    PERFORM time_series.bgw_invalidate_cache();
END;
$$ LANGUAGE plpgsql;

-- =========================================================================
--  BGW scheduler control plane: start / stop / restart
--
--  Mirror upstream TimescaleDB's
--    _timescaledb_functions.{start,stop,restart}_background_workers()
--  exactly.  Each sends a single STOP / START / RESTART message to the
--  launcher via the shared-memory bgw_message_queue and synchronously
--  waits for the launcher's ack.  The launcher transitions our DB's
--  hash entry through the 4-state machine (DISABLED/ENABLED/ALLOCATED/
--  STARTED) and persists the operator intent across launcher polls;
--  no separate ALTER DATABASE SET is required (and none is racy).
--
--  All three require superuser.
-- =========================================================================

CREATE FUNCTION time_series.stop_background_workers()
RETURNS bool
AS 'MODULE_PATHNAME', 'ts_bgw_db_workers_stop'
LANGUAGE C VOLATILE;

CREATE FUNCTION time_series.start_background_workers()
RETURNS bool
AS 'MODULE_PATHNAME', 'ts_bgw_db_workers_start'
LANGUAGE C VOLATILE;

CREATE FUNCTION time_series.restart_background_workers()
RETURNS bool
AS 'MODULE_PATHNAME', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

-- =========================================================================
--  sql_drop event trigger: terminate scheduler synchronously on
--  DROP EXTENSION time_series.
--
--  Without this trigger, the scheduler bgworker keeps an idle session
--  on its DB after DROP EXTENSION and only exits asynchronously on its
--  next wake-up via relcache invalidation.  That idle session blocks a
--  subsequent DROP DATABASE (pg_regress's standard cleanup path).
--
--  The trigger fires inside the DROP EXTENSION transaction.  We do NOT
--  call stop_background_workers() here: that would record a persistent
--  ALTER DATABASE SET on a GUC the extension is about to unregister,
--  leaving an orphaned per-DB setting.  Just SIGTERM the scheduler;
--  the launcher will not respawn because the extension is gone (the
--  scheduler's startup-time check `extension_installed_in_this_db()`
--  exits cleanly).
-- =========================================================================
CREATE FUNCTION time_series.on_extension_drop()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    n   int;
BEGIN
    FOR obj IN
        SELECT 1 FROM pg_event_trigger_dropped_objects()
         WHERE object_type = 'extension'
           AND object_identity = 'time_series'
    LOOP
        PERFORM pg_terminate_backend(pid)
          FROM pg_stat_activity
         WHERE backend_type = 'time_series scheduler'
           AND datname      = current_database();

        -- Wait up to ~1 s for the scheduler to actually exit so a
        -- follow-up DROP DATABASE in the same connection does not race
        -- the SIGTERM delivery.  Same pg_stat_clear_snapshot caveat as
        -- stop_background_workers().
        FOR i IN 1..10 LOOP
            PERFORM pg_stat_clear_snapshot();
            SELECT count(*) INTO n
              FROM pg_stat_activity
             WHERE backend_type = 'time_series scheduler'
               AND datname      = current_database();
            EXIT WHEN n = 0;
            PERFORM pg_sleep(0.1);
        END LOOP;
        RETURN;
    END LOOP;
END $$;

CREATE EVENT TRIGGER time_series_on_extension_drop
    ON sql_drop
    EXECUTE FUNCTION time_series.on_extension_drop();

-- =========================================================================
--  Mock-time test infrastructure (Apache 2.0; ported from upstream)
--
--  These functions and tables are used ONLY by the regression test suite
--  to drive a virtual-clock BGW scheduler in <1 second per test case.
--  They are unconditionally installed (just like other test helpers) so
--  cagg_bgw_mock.sql does not need a separate setup step.
--
--  Setup pattern in tests:
--    SELECT time_series.bgw_params_create();   -- once per test
--    -- run a mock scheduler tick:
--    SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(60000000);
--    SELECT * FROM public.sorted_bgw_log;          -- inspect events
-- =========================================================================

-- Test scheduler / params functions (from test/src/bgw/scheduler_mock.c
-- and test/src/bgw/params.c).
CREATE FUNCTION time_series.bgw_db_scheduler_test_main()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_db_scheduler_test_main';

CREATE FUNCTION time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(int4)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME',
'bgw_db_scheduler_test_run_and_wait_for_scheduler_finish';

CREATE FUNCTION time_series.bgw_db_scheduler_test_run(int4)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_db_scheduler_test_run';

CREATE FUNCTION time_series.bgw_db_scheduler_test_wait_for_scheduler_finish()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME',
'bgw_db_scheduler_test_wait_for_scheduler_finish';

CREATE FUNCTION time_series.bgw_params_create()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_params_create';

CREATE FUNCTION time_series.bgw_params_destroy()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_params_destroy';

CREATE FUNCTION time_series.bgw_params_reset_time(int8, bool)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_params_reset_time';

CREATE FUNCTION time_series.bgw_params_mock_wait_returns_immediately(int4)
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME',
'bgw_params_mock_wait_returns_immediately';

-- 1:1-with-upstream test infrastructure: synthetic job dispatcher used by
-- the heavy bgw_db_scheduler tests.  Tests register jobs in bgw_job
-- with proc_name in {bgw_test_job_1, bgw_test_job_2_error,
-- bgw_test_job_3_long, bgw_test_job_4} and the dispatcher routes to
-- the matching local function.  bgw_job_execute_test is the
-- entrypoint installed by the mock scheduler.

CREATE FUNCTION time_series.bgw_job_execute_test()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_job_execute_test';

CREATE FUNCTION time_series.bgw_test_job_sleep()
RETURNS void LANGUAGE C AS 'MODULE_PATHNAME', 'bgw_test_job_sleep';

CREATE FUNCTION time_series.test_next_scheduled_execution_slot(
    schedule_interval interval,
    finish_time       timestamptz,
    initial_start     timestamptz,
    timezone          text DEFAULT NULL)
RETURNS timestamptz LANGUAGE C AS 'MODULE_PATHNAME',
'test_next_scheduled_execution_slot';

-- Lock down mock-time test infrastructure to superuser only.  These
-- functions exist for the regression test suite to drive a synthetic
-- BGW scheduler against mocked time, but they are not meant for end
-- users:
--   * bgw_db_scheduler_test_run / _and_wait_for_scheduler_finish
--     fork a real BackgroundWorker that reads bgw_job and dispatches
--     synthetic test_job_* — anyone can consume worker slots.
--   * bgw_test_job_sleep blocks for an interval, holding a slot.
--   * bgw_params_* mutate process-shared mock state.
--
-- Tests run as superuser and are unaffected.  Production users have
-- no legitimate reason to call any of these.
REVOKE EXECUTE ON FUNCTION time_series.bgw_db_scheduler_test_main()                              FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(int4) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_db_scheduler_test_run(int4)                           FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_db_scheduler_test_wait_for_scheduler_finish()         FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_params_create()                                       FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_params_destroy()                                      FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_params_reset_time(int8, bool)                         FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_params_mock_wait_returns_immediately(int4)            FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_job_execute_test()                                    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.bgw_test_job_sleep()                                      FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series.test_next_scheduled_execution_slot(interval, timestamptz, timestamptz, text) FROM PUBLIC;

-- Lock down segment-side / internal compression helpers.  Each is a
-- destructive primitive that the user-facing wrappers (compress_chunks,
-- reclaim_chunk_heaps) dispatch to via CdbDispatchCommand under the
-- caller's identity; direct invocation is never appropriate.
--   * _ts_compress_write_chunks       — writes PAX + ts_compressed_chunk
--     row per segment; called from ts_compress_chunks after eligibility
--     check.  Direct call would create orphan PAX files.
--   * _ts_reclaim_chunk_heaps_segment — smgrtruncates heap forks on the
--     local segment.  Direct call could truncate not-yet-compressed data.
--   * _ts_truncate_chunk_fork         — same, per-chunk primitive.
--     Direct call on an uncompressed chunk permanently loses data.
--
-- C entry points also verify pg_class_ownercheck as defence-in-depth.
REVOKE EXECUTE ON FUNCTION time_series._ts_compress_write_chunks(regclass, integer[])       FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series._ts_reclaim_chunk_heaps_segment(regclass, integer[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION time_series._ts_truncate_chunk_fork(regclass, integer)           FROM PUBLIC;

-- =========================================================================
--  Notify the launcher we exist.
--
--  This file runs once per CREATE EXTENSION time_series.  The launcher
--  poll already discovers our DB via pg_database, but its hash entry
--  for a never-before-seen DB starts DISABLED (mirrors upstream so
--  STOP semantics are persistent across launcher polls).  Sending a
--  RESTART message here is the conventional way to flip the entry to
--  ENABLED and have the launcher spawn a scheduler immediately.
--
--  Mirrors TimescaleDB's sql/bgw_startup.sql one-liner.
-- =========================================================================
SELECT time_series.restart_background_workers();
