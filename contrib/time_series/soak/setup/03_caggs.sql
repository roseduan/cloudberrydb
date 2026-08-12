-- scripts/soak/setup/03_caggs.sql
--
-- 4 continuous aggregates over the cpu source table pre-created (as
-- a hypertable) by setup/02_seed_via_tsbs.sh.  The cpu schema is:
--
--   CREATE TABLE cpu (
--       time             timestamptz NOT NULL,
--       tags_id          integer,
--       usage_user       double precision,
--       usage_system     double precision,
--       usage_idle       double precision,
--       usage_nice       double precision,
--       usage_iowait     double precision,
--       usage_irq        double precision,
--       usage_softirq    double precision,
--       usage_steal      double precision,
--       usage_guest      double precision,
--       usage_guest_nice double precision,
--       additional_tags  jsonb           -- ignored by the CAGGs below
--   ) USING time_series WITH (
--       ts_partition_column = 'time',
--       ts_chunk_interval   = '1 day',
--       ts_chunk_origin     = '<seed start, day-aligned>'
--   );
--
-- Why hypertable on the source (since time_series_v1 integration):
--   - CAGG live branch reads filter by bucket → time predicate → chunk
--     prune.  Realtime queries that previously scanned the whole cpu
--     heap now touch only the trailing chunk(s).
--   - BGW refresh's watermark-clamp query (`SELECT max("time") FROM cpu`)
--     becomes O(log N) via the per-chunk fork's btree on the routed
--     forks instead of a global seq scan.
--   - chunk_interval='1 hour' (soak default; production may use 1 day):
--     enough chunks per soak run to actually exercise compression policy
--     and chunk-routing paths; for cv_1hour pruning the planner still
--     prunes hourly chunks cleanly since chunk boundaries align with the
--     1-hour bucket.
--
-- We aggregate three of the ten metrics (usage_user, usage_system,
-- usage_idle) covering avg / sum / min / max so reconcile across
-- different rollup styles is exercised.
--
-- schedule_interval >= bucket_width on every CAGG, matching how
-- production BI / monitoring pipelines actually configure refresh
-- (see env-setup doc §8.1 for rationale; the alternative "stress"
-- profile with schedule << bucket was dropped — same code paths
-- but 6× call-rate doesn't surface different bugs over 7 days).

SET optimizer = off;
SET search_path TO public, time_series;

DROP MATERIALIZED VIEW IF EXISTS cv_1min  CASCADE;
DROP MATERIALIZED VIEW IF EXISTS cv_5min  CASCADE;
DROP MATERIALIZED VIEW IF EXISTS cv_1hour CASCADE;

-- ── cv_1min ──────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW cv_1min
  WITH (time_series.continuous) AS
  SELECT time_bucket('1 min'::interval, time) AS bucket,
         tags_id,
         count(*)              AS cnt,
         avg(usage_user)       AS avg_user,
         max(usage_system)     AS max_system
    FROM cpu
   GROUP BY bucket, tags_id;

-- ── cv_5min ──────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW cv_5min
  WITH (time_series.continuous) AS
  SELECT time_bucket('5 min'::interval, time) AS bucket,
         tags_id,
         count(*)              AS cnt,
         sum(usage_user)       AS sum_user,
         min(usage_idle)       AS min_idle,
         max(usage_system)     AS max_system
    FROM cpu
   GROUP BY bucket, tags_id;

-- ── cv_1hour ─────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW cv_1hour
  WITH (time_series.continuous) AS
  SELECT time_bucket('1 hour'::interval, time) AS bucket,
         tags_id,
         count(*)              AS cnt,
         avg(usage_user)       AS avg_user,
         max(usage_system)     AS max_system,
         min(usage_idle)       AS min_idle
    FROM cpu
   GROUP BY bucket, tags_id;

-- ── Policies (schedule >= bucket, with safety margin) ────────────
--
-- start_offset MUST cover the deepest late_arrival reach (bulk now-5h)
-- or L2 entries for those backfill writes never fall inside any
-- refresh window -- cagg_trim_l2 only deletes L2 entries that are
-- fully INSIDE the just-refreshed window (refresh.c:932).  L2
-- accumulates monotonically forever for those buckets, the cagg view
-- serves stale mat data for them forever, and the view_correctness
-- check (decidable iff no pending L1/L2 overlaps the bucket) hits
-- 37/38 cycles of "thin coverage" warnings.  Set start_offset >=
-- MAX_LATE_DEPTH_SEC (6h) on every CAGG whose mat-window probe sits
-- below the deepest bulk-write reach.  cv_1hour (6h) already meets
-- this; cv_1min and cv_5min did not until we set start_offset = 6h.
-- All three policies run batched + newest-first (the upstream default
-- and the harder combination): newest-first batching can leave mat
-- islands+holes when an execution is interrupted, so keeping it on
-- everywhere makes the soak continuously exercise the healing
-- machinery -- the ledger-driven policy expand-back (arrival-side
-- holes), the wm-based expand-back, and the in-window unmaterialized-
-- region re-cover (refresh-side holes).  If any heal path regresses,
-- the soak's watermark_lag / view_correctness judges will catch it.
-- Batching everywhere also exercises the per-batch commit path on
-- every CAGG cadence (1min / 5min / 1hour), not just the hourly one.
-- See doc/bugs/cagg-refresh-stuck-across-gap.md §9.10 (hole taxonomy).
SELECT add_continuous_aggregate_policy('cv_1min',
    start_offset              => INTERVAL '6 hours',
    end_offset                => INTERVAL '2 min',
    schedule_interval         => INTERVAL '1 min',
    buckets_per_batch         => 2,
    max_batches_per_execution => 10,
    refresh_newest_first      => true);

SELECT add_continuous_aggregate_policy('cv_5min',
    start_offset              => INTERVAL '6 hours',
    end_offset                => INTERVAL '10 min',
    schedule_interval         => INTERVAL '5 min',
    buckets_per_batch         => 2,
    max_batches_per_execution => 10,
    refresh_newest_first      => true);

-- cv_1hour: schedule_interval = bucket_width (1 hour), per the design
-- rule.  Previously this was set to 10 min (sub-bucket) for mat
-- freshness, but with the recommended preset's chaos + BGW pressure
-- it just meant 5/6 fires were empty work.  cv_1min already provides
-- high-frequency BGW stress; cv_1hour stays aligned with its bucket.
--
-- Batched refresh: the refresh window (start_offset -> end_offset,
-- intersected with invalidations) is sliced into 2-bucket sub-windows;
-- batches chain-commit (a batch's materialization is committed by the
-- next batch's SPI_commit_and_chain -- verified by iso2
-- cagg_hole_batch_boundary).  max_batches_per_execution=10 is the
-- upstream default, a safety cap; typical per-tick batch count is 1-3
-- so the cap is rarely binding.
SELECT add_continuous_aggregate_policy('cv_1hour',
    start_offset              => INTERVAL '6 hours',
    end_offset                => INTERVAL '2 hours',
    schedule_interval         => INTERVAL '1 hour',
    buckets_per_batch         => 2,
    max_batches_per_execution => 10,
    refresh_newest_first      => true);

-- ── Bootstrap: initial full materialization ──────────────────────
--
-- A CAGG with a finite start_offset policy only materializes a small
-- recent window per refresh.  Without an initial NULL,NULL refresh,
-- the watermark stays at -infinity forever: every subsequent BGW
-- refresh hits the gap-check branch (refresh.c:~1605), which scans
-- the entire source table looking for buckets not yet in the mat
-- table — finds tons of them, refuses to advance the watermark, and
-- the cycle repeats on every schedule tick (= permanent O(N) cost
-- per refresh on a soak with multi-day source).
--
-- One synchronous full refresh seeds the mat tables across the
-- entire source history and lets subsequent policy refreshes hit
-- the fast path (window_start <= current_watermark → advance_ok).
CALL refresh_continuous_aggregate('cv_1min',  NULL, NULL);
CALL refresh_continuous_aggregate('cv_5min',  NULL, NULL);
CALL refresh_continuous_aggregate('cv_1hour', NULL, NULL);

-- ── Compression policy on the source hypertable ──────────────────
--
-- The cpu source table is a hypertable; each closed (non-current)
-- chunk older than :compress_after gets compressed into a PAX-backed
-- segment by the policy_compression BGW job (commit 8b46c6c).  This
-- exercises the v1 auto-compression path end-to-end:
--
--   policy_compression (BGW tick) → compress_and_reclaim
--     → compress_chunk (PAX writer)
--     → reclaim_chunk_heap_fork
--
-- Why this matters for soak:
--   1. The auto-compression BGW job runs concurrently with refresh
--      BGWs, the workload INSERT loop, and (when enabled) chaos
--      faults.  This 3-way interaction (compression × refresh ×
--      INSERT) has no regress coverage.
--   2. Compressed chunks must remain readable by the CAGG live
--      branch (live reads cpu WHERE bucket >= w_start, which now
--      crosses PAX chunks).  view_correctness keeps this invariant
--      under live compression.
--   3. The view_correctness check (§4) reads source through PAX
--      after some chunks compress — any PAX-vs-heap row visibility
--      divergence becomes a MISMATCH at the next 30-min check.
--
-- compress_after / schedule are profile-dependent (set via psql -v
-- by soak.sh apply_profile()).  Fall back to conservative
-- defaults when the variables are not bound (e.g. running this SQL
-- script directly without the driver).
\if :{?compress_after}
\else
  \set compress_after '6 hours'
\endif
\if :{?compress_schedule}
\else
  \set compress_schedule '1 hour'
\endif

\echo Adding compression policy: compress_after=:'compress_after' schedule=:'compress_schedule'

SELECT add_compression_policy(
    table_name        => 'cpu'::regclass,
    compress_after    => (:'compress_after')::interval,
    schedule_interval => (:'compress_schedule')::interval
);
