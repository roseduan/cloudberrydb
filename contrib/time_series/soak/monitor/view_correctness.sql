-- ============================================================================
-- monitor/view_correctness.sql  (v2 — decidable-region checking)
--
-- THE correctness check for CAGG: the user-facing view must return
-- EXACTLY what a direct aggregation of the source table returns — for
-- every bucket where that comparison is DECIDABLE.
--
-- ── Why "decidable" (the general fix for timing false-positives) ──
--
-- The product's contract is eventual consistency:
--
--     view = mat (< watermark)  UNION ALL  live (>= watermark)
--
-- and mat lags behind source wherever an UNPROCESSED invalidation
-- exists (late-arriving INSERT below the watermark → L1/L2 entry →
-- BGW re-materializes on a later policy run).  Until that happens the
-- view legitimately serves stale mat for those buckets — comparing
-- them against source is NOT a correctness test, it is a race with
-- the background worker.  Three framework incidents came from exactly
-- this class of implicit timing assumption (round-3 blind spot, the
-- late-arrival false MISMATCH, the 2026-06-10 plain-window false
-- MISMATCH).
--
-- The general rule, applied uniformly to every block below:
--
--   A bucket is DECIDABLE  ⟺  bucket >= watermark        (live: always)
--                           OR no pending L1/L2 range overlaps it
--
-- Decidability is evaluated in the SAME REPEATABLE READ snapshot as
-- the data comparison, so there is no gate-vs-check race.  Buckets
-- ruled out are counted in the new excluded_buckets column — the
-- report tracks coverage, and a MISMATCH on a decidable bucket is a
-- REAL BUG with no timing excuse, regardless of how any SOAK_*
-- cadence parameter is tuned.
--
-- CSV row format (8 columns — schema v2):
--   ts, db, cagg, window, total_rows, mismatch_count, excluded_buckets, verdict
--
-- Windows are bucket-aligned via time_bucket() so the generated bucket
-- grid matches the aggregation exactly.  Mat-window edge cases:
-- watermark = -infinity / NULL ⇒ empty bucket set ⇒ 0 rows, match.
-- ============================================================================

SET optimizer = off;
SET timezone = 'UTC';
SET search_path TO public, time_series;

\pset tuples_only on
\pset format unaligned
\pset fieldsep ','
\pset footer off

-- One transaction, one snapshot: data, watermark and L1/L2 all agree.
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- ── cv_1min : live: last 30 min ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 minute'::interval, now()) - INTERVAL '30 min' AS w_start,
         time_bucket('1 minute'::interval, now()) AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 minute'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 minute'::interval, '1 minute'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 minute'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.avg_user, c.max_system
    FROM cv_1min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.avg_user - v.avg_user) > 1e-4 OR src.max_system != v.max_system
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_1min', '30min',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- Diff-rows dump (cv_1min live).  Runs in the SAME REPEATABLE READ
-- snapshot as the summary above so race resolutions (BGW refresh
-- between the summary commit and a follow-up query) cannot mask which
-- buckets actually differed.  Emits one row per differing tuple, with
-- a `_DIFF_` first column so view_correctness_scrape.sh's 8-col CSV parser can
-- filter it from the summary rows.  LIMIT 20: we want fingerprints,
-- not the entire diff under heavy load.
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 minute'::interval, now()) - INTERVAL '30 min' AS w_start,
         time_bucket('1 minute'::interval, now()) AS w_end
    FROM wm
),
dirty AS (
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  SELECT g.b AS bucket, wm.watermark,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 minute'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 minute'::interval, '1 minute'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, b.watermark, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 minute'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2, 3
),
v AS (
  SELECT c.bucket, b.watermark, c.tags_id, c.cnt, c.avg_user, c.max_system
    FROM cv_1min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
)
SELECT '_DIFF_', 'cv_1min', '30min',
       COALESCE(src.bucket, v.bucket)::text,
       COALESCE(src.tags_id, v.tags_id)::text,
       CASE WHEN src.bucket IS NULL THEN 'view-only'
            WHEN v.bucket   IS NULL THEN 'src-only'
            ELSE 'differ' END,
       -- mat vs live branch: bucket >= watermark => live (source-on-the-fly),
       -- bucket < watermark => mat (materialised table).  Tells us at a
       -- glance whether the failure is in the live branch or the mat
       -- table.
       CASE WHEN COALESCE(src.bucket, v.bucket)
                 >= COALESCE(src.watermark, v.watermark, '-infinity'::timestamptz)
            THEN 'live' ELSE 'mat' END,
       'src cnt='||COALESCE(src.cnt::text,'NULL')||
       ' avg_user='||COALESCE(src.avg_user::text,'NULL')||
       ' max_system='||COALESCE(src.max_system::text,'NULL')||
       ' | v cnt='||COALESCE(v.cnt::text,'NULL')||
       ' avg_user='||COALESCE(v.avg_user::text,'NULL')||
       ' max_system='||COALESCE(v.max_system::text,'NULL')
  FROM src FULL OUTER JOIN v
    ON src.bucket = v.bucket AND src.tags_id = v.tags_id
 WHERE src.bucket IS NULL OR v.bucket IS NULL
    OR src.cnt != v.cnt
    OR abs(src.avg_user - v.avg_user) > 1e-4
    OR src.max_system != v.max_system
 ORDER BY 4, 5
 LIMIT 20;

-- ── cv_5min : live: last 1 h ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_5min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('5 minutes'::interval, now()) - INTERVAL '1 hour' AS w_start,
         time_bucket('5 minutes'::interval, now()) AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '5 minutes'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '5 minutes'::interval, '5 minutes'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, sum(usage_user) AS sum_user, min(usage_idle) AS min_idle, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('5 minutes'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.sum_user, c.min_idle, c.max_system
    FROM cv_5min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.sum_user - v.sum_user) > 1e-4 OR src.min_idle != v.min_idle OR src.max_system != v.max_system
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_5min', '1h',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- ── cv_1hour : live: last 6 h ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1hour'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 hour'::interval, now()) - INTERVAL '6 hours' AS w_start,
         time_bucket('1 hour'::interval, now()) AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 hour'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 hour'::interval, '1 hour'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system, min(usage_idle) AS min_idle
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 hour'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.avg_user, c.max_system, c.min_idle
    FROM cv_1hour c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.avg_user - v.avg_user) > 1e-4 OR src.max_system != v.max_system OR src.min_idle != v.min_idle
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_1hour', '6h',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- Diff-rows dump (cv_1hour live).  Runs in the SAME REPEATABLE READ
-- snapshot as the summary above so race resolutions (BGW refresh
-- between the summary commit and a follow-up query) cannot mask which
-- buckets actually differed.  See cv_1min for full rationale.
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1hour'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 hour'::interval, now()) - INTERVAL '6 hours' AS w_start,
         time_bucket('1 hour'::interval, now()) AS w_end
    FROM wm
),
dirty AS (
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  SELECT g.b AS bucket, wm.watermark,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 hour'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 hour'::interval, '1 hour'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, b.watermark, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system, min(usage_idle) AS min_idle
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 hour'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2, 3
),
v AS (
  SELECT c.bucket, b.watermark, c.tags_id, c.cnt, c.avg_user, c.max_system, c.min_idle
    FROM cv_1hour c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
)
SELECT '_DIFF_', 'cv_1hour', '6h',
       COALESCE(src.bucket, v.bucket)::text,
       COALESCE(src.tags_id, v.tags_id)::text,
       CASE WHEN src.bucket IS NULL THEN 'view-only'
            WHEN v.bucket   IS NULL THEN 'src-only'
            ELSE 'differ' END,
       CASE WHEN COALESCE(src.bucket, v.bucket)
                 >= COALESCE(src.watermark, v.watermark, '-infinity'::timestamptz)
            THEN 'live' ELSE 'mat' END,
       'src cnt='||COALESCE(src.cnt::text,'NULL')||
       ' avg_user='||COALESCE(src.avg_user::text,'NULL')||
       ' max_system='||COALESCE(src.max_system::text,'NULL')||
       ' min_idle='||COALESCE(src.min_idle::text,'NULL')||
       ' | v cnt='||COALESCE(v.cnt::text,'NULL')||
       ' avg_user='||COALESCE(v.avg_user::text,'NULL')||
       ' max_system='||COALESCE(v.max_system::text,'NULL')||
       ' min_idle='||COALESCE(v.min_idle::text,'NULL')
  FROM src FULL OUTER JOIN v
    ON src.bucket = v.bucket AND src.tags_id = v.tags_id
 WHERE src.bucket IS NULL OR v.bucket IS NULL
    OR src.cnt != v.cnt
    OR abs(src.avg_user - v.avg_user) > 1e-4
    OR src.max_system != v.max_system
    OR src.min_idle != v.min_idle
 ORDER BY 4, 5
 LIMIT 20;

-- ── cv_1min_mat : mat: 30 min ending watermark - 2 min ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  -- Window [wm-37min, wm-7min].  Old upper bound wm-2min got dirtied by
  -- the 'near' late mode (writes now-300s..now-30s ≈ down to wm-4min, as
  -- cv_1min watermark sits ~3min behind now) → those buckets excluded.
  -- wm-7min stays clear of near's deepest reach across watermark-lag swings.
  SELECT time_bucket('1 minute'::interval, wm.watermark) - INTERVAL '37 min' AS w_start,
         time_bucket('1 minute'::interval, wm.watermark) - INTERVAL '7 min' AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 minute'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 minute'::interval, '1 minute'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 minute'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.avg_user, c.max_system
    FROM cv_1min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.avg_user - v.avg_user) > 1e-4 OR src.max_system != v.max_system
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_1min_mat', '30min<wm',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- Diff-rows dump (cv_1min mat window).  See the rationale at the
-- corresponding emission in the cv_1min live block.
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 minute'::interval, wm.watermark) - INTERVAL '37 min' AS w_start,
         time_bucket('1 minute'::interval, wm.watermark) - INTERVAL '7 min' AS w_end
    FROM wm
),
dirty AS (
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  SELECT g.b AS bucket, wm.watermark,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 minute'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 minute'::interval, '1 minute'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, b.watermark, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 minute'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2, 3
),
v AS (
  SELECT c.bucket, b.watermark, c.tags_id, c.cnt, c.avg_user, c.max_system
    FROM cv_1min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
)
SELECT '_DIFF_', 'cv_1min_mat', '30min<wm',
       COALESCE(src.bucket, v.bucket)::text,
       COALESCE(src.tags_id, v.tags_id)::text,
       CASE WHEN src.bucket IS NULL THEN 'view-only'
            WHEN v.bucket   IS NULL THEN 'src-only'
            ELSE 'differ' END,
       CASE WHEN COALESCE(src.bucket, v.bucket)
                 >= COALESCE(src.watermark, v.watermark, '-infinity'::timestamptz)
            THEN 'live' ELSE 'mat' END,
       'src cnt='||COALESCE(src.cnt::text,'NULL')||
       ' avg_user='||COALESCE(src.avg_user::text,'NULL')||
       ' max_system='||COALESCE(src.max_system::text,'NULL')||
       ' | v cnt='||COALESCE(v.cnt::text,'NULL')||
       ' avg_user='||COALESCE(v.avg_user::text,'NULL')||
       ' max_system='||COALESCE(v.max_system::text,'NULL')
  FROM src FULL OUTER JOIN v
    ON src.bucket = v.bucket AND src.tags_id = v.tags_id
 WHERE src.bucket IS NULL OR v.bucket IS NULL
    OR src.cnt != v.cnt
    OR abs(src.avg_user - v.avg_user) > 1e-4
    OR src.max_system != v.max_system
 ORDER BY 4, 5
 LIMIT 20;

-- ── cv_5min_mat : mat: 1 h ending watermark - 10 min ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_5min'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  -- Window moved DOWN to a settled segment [wm-130min, wm-70min].
  -- The old [wm-70min, wm-10min] overlapped late_arrival's now-1h
  -- backfill: cv_5min's watermark sits ~16min behind now, so now-1h
  -- landed at ~wm-44min, INSIDE that window.  Every cycle the late
  -- INSERT dirtied a bucket there → marked not-decidable → excluded,
  -- so the mat branch got ZERO real coverage (36/36 cycles thin in the
  -- 2026-06 run).  [wm-130min, wm-70min] stays older than now-1h across
  -- watermark-lag swings, so late-arrival invalidations never touch it.
  -- (Assumes default SOAK_LATE_OFFSET_SEC=3600; widen if that grows.)
  SELECT time_bucket('5 minutes'::interval, wm.watermark) - INTERVAL '130 min' AS w_start,
         time_bucket('5 minutes'::interval, wm.watermark) - INTERVAL '70 min' AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '5 minutes'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '5 minutes'::interval, '5 minutes'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, sum(usage_user) AS sum_user, min(usage_idle) AS min_idle, max(usage_system) AS max_system
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('5 minutes'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.sum_user, c.min_idle, c.max_system
    FROM cv_5min c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.sum_user - v.sum_user) > 1e-4 OR src.min_idle != v.min_idle OR src.max_system != v.max_system
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_5min_mat', '1h<wm',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- ── cv_1hour_mat : mat: 6 h ending watermark - 2 h ──────────────────────────────────────────────
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1hour'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  -- Window [wm-10h, wm-4h].  Old upper bound wm-2h got dirtied by the
  -- 'bulk' late mode (writes now-3h..now-5h ≈ wm-1h..wm-3h, as cv_1hour
  -- watermark sits ~2h behind now) → those buckets excluded.  wm-4h
  -- stays older than bulk's shallowest reach across watermark-lag swings.
  SELECT time_bucket('1 hour'::interval, wm.watermark) - INTERVAL '10 hours' AS w_start,
         time_bucket('1 hour'::interval, wm.watermark) - INTERVAL '4 hours' AS w_end
    FROM wm
),
dirty AS (
  -- Pending invalidations in this snapshot: L1 (per-source) ∪ L2
  -- (per-cagg).  Tiny rowset; evaluated once.
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  -- The window's bucket grid + per-bucket decidability.
  SELECT g.b AS bucket,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 hour'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 hour'::interval, '1 hour'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system, min(usage_idle) AS min_idle
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 hour'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2
),
v AS (
  SELECT c.bucket, c.tags_id, c.cnt, c.avg_user, c.max_system, c.min_idle
    FROM cv_1hour c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL
              OR v.bucket   IS NULL
              OR src.cnt != v.cnt OR abs(src.avg_user - v.avg_user) > 1e-4 OR src.max_system != v.max_system OR src.min_idle != v.min_idle
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now()::text, current_database(), 'cv_1hour_mat', '6h<wm',
       diff.total_rows, diff.mismatch_count,
       (SELECT count(*) FROM buckets WHERE NOT decidable),
       CASE WHEN diff.mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;

-- Diff-rows dump (cv_1hour mat window).  See the rationale at the
-- corresponding emission in the cv_1min live block.  Window matches
-- the cv_1hour_mat summary: [wm-10h, wm-4h], deep enough that the
-- 'bulk' late mode (writes up to ~wm-3h) cannot dirty it.
WITH ids AS (
  SELECT cagg_id FROM time_series.continuous_agg WHERE user_view_name = 'cv_1hour'
),
wm AS (
  -- NULLIF('-infinity'): a CAGG that has not refreshed yet has
  -- watermark = -infinity.  Watermark-relative MAT windows below would
  -- then compute w_start=w_end=-infinity and feed generate_series an
  -- infinite bound, which runs UNBOUNDED (spills the coordinator's
  -- pgsql_tmp until the disk fills).  Mapping -infinity → NULL makes
  -- those windows' generate_series yield zero rows, i.e. the MAT check
  -- is skipped until the CAGG has actually materialized something.
  -- LIVE (now()-relative) windows are unaffected: their only use of the
  -- watermark is COALESCE(watermark,'-infinity'), unchanged by NULL.
  SELECT NULLIF(MIN(watermark), '-infinity'::timestamptz) AS watermark FROM time_series.cagg_watermark
   WHERE cagg_id = (SELECT cagg_id FROM ids)
),
p AS (
  SELECT time_bucket('1 hour'::interval, wm.watermark) - INTERVAL '10 hours' AS w_start,
         time_bucket('1 hour'::interval, wm.watermark) - INTERVAL '4 hours' AS w_end
    FROM wm
),
dirty AS (
  SELECT il.lowest_modified AS lo, il.greatest_modified AS hi
    FROM time_series.cagg_invalidation_log il
   WHERE il.source_table_oid = 'public.cpu'::regclass
  UNION ALL
  SELECT ml.lowest_modified, ml.greatest_modified
    FROM time_series.cagg_materialization_log ml
   WHERE ml.cagg_id = (SELECT cagg_id FROM ids)
),
buckets AS (
  SELECT g.b AS bucket, wm.watermark,
         (g.b >= COALESCE(wm.watermark, '-infinity'::timestamptz)
          OR NOT EXISTS (SELECT 1 FROM dirty d
                          WHERE d.hi >= g.b
                            AND d.lo <  g.b + '1 hour'::interval)) AS decidable
    FROM p, wm,
         LATERAL generate_series(p.w_start, p.w_end - '1 hour'::interval, '1 hour'::interval) AS g(b)
   WHERE p.w_start IS NOT NULL AND p.w_end IS NOT NULL
),
src AS (
  SELECT b.bucket, b.watermark, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system, min(usage_idle) AS min_idle
    FROM cpu, p, buckets b
   WHERE time >= p.w_start AND time < p.w_end
     AND time_bucket('1 hour'::interval, time) = b.bucket
     AND b.decidable
   GROUP BY 1, 2, 3
),
v AS (
  SELECT c.bucket, b.watermark, c.tags_id, c.cnt, c.avg_user, c.max_system, c.min_idle
    FROM cv_1hour c JOIN buckets b ON c.bucket = b.bucket
   WHERE b.decidable
)
SELECT '_DIFF_', 'cv_1hour_mat', '6h<wm',
       COALESCE(src.bucket, v.bucket)::text,
       COALESCE(src.tags_id, v.tags_id)::text,
       CASE WHEN src.bucket IS NULL THEN 'view-only'
            WHEN v.bucket   IS NULL THEN 'src-only'
            ELSE 'differ' END,
       CASE WHEN COALESCE(src.bucket, v.bucket)
                 >= COALESCE(src.watermark, v.watermark, '-infinity'::timestamptz)
            THEN 'live' ELSE 'mat' END,
       'src cnt='||COALESCE(src.cnt::text,'NULL')||
       ' avg_user='||COALESCE(src.avg_user::text,'NULL')||
       ' max_system='||COALESCE(src.max_system::text,'NULL')||
       ' min_idle='||COALESCE(src.min_idle::text,'NULL')||
       ' | v cnt='||COALESCE(v.cnt::text,'NULL')||
       ' avg_user='||COALESCE(v.avg_user::text,'NULL')||
       ' max_system='||COALESCE(v.max_system::text,'NULL')||
       ' min_idle='||COALESCE(v.min_idle::text,'NULL')
  FROM src FULL OUTER JOIN v
    ON src.bucket = v.bucket AND src.tags_id = v.tags_id
 WHERE src.bucket IS NULL OR v.bucket IS NULL
    OR src.cnt != v.cnt
    OR abs(src.avg_user - v.avg_user) > 1e-4
    OR src.max_system != v.max_system
    OR src.min_idle != v.min_idle
 ORDER BY 4, 5
 LIMIT 20;

COMMIT;
