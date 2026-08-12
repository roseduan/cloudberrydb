#!/bin/bash
# monitor/watermark_lag.sh
# SOAK-LOOP: scope=per-db interval=60 interval_var=SOAK_WATERMARK_LAG_EVERY order=40
#
# Samples per-CAGG watermark lag = (source max time) - MIN(watermark) and
# appends one row per (db, cagg) to watermark_lag.csv:
#
#   ts, db, cagg, watermark, source_max, lag_seconds, end_offset_seconds, exceeded
#
# WHY THIS EXISTS (round 3 retrospective):
#   Round 3 soak ran 20+ hours with cv_1min/5min/1hour watermarks stuck
#   at the seed boundary (~27 hours behind source).  view_correctness saw
#   only the live branch and reported "all green", so the failure was
#   invisible to the framework.  Without an explicit lag monitor, the
#   framework cannot tell whether the BGW refresh policy is actually
#   advancing the materialization — a foundational invariant for CAGG.
#
#   The end_offset column is the policy-configured tolerance: a healthy
#   watermark should be no more than (end_offset + a small jitter
#   margin) behind source_max.  `exceeded` is t/f for fast filtering;
#   the analyst can post-hoc decide on stricter thresholds.
#
# Usage (invoked by lib/loops.sh):
#   watermark_lag.sh <db> <results_dir>          one sample pass

set -uo pipefail

# ── Collection mode ──────────────────────────────────────────────────
DB="${1:?usage: watermark_lag.sh <db> <results_dir>}"
RESULTS="${2:?usage: watermark_lag.sh <db> <results_dir>}"
PORT="${SOAK_PORT:-7000}"
HOST="${SOAK_HOST:-localhost}"
USER="${SOAK_USER:-gpadmin}"

CSV="$RESULTS/data/watermark_lag.csv"
ERR="$RESULTS/errors/watermark_lag.err"

# Header (once)
if [[ ! -f "$CSV" ]]; then
  echo "ts,db,cagg,watermark,source_max,lag_seconds,end_offset_seconds,exceeded" > "$CSV"
fi

TS=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

# Per-CAGG lag in one query.  end_offset comes from the policy job's
# config JSONB so we don't have to hard-code it here.  source_max is
# computed via the chunk-aware two-step pattern (see refresh.c) so the
# probe doesn't itself trigger a full source scan on every cycle: read
# the latest chunk's range_start, then max(time) >= that bound.
#
# NULL handling: a never-refreshed CAGG has watermark = -infinity, which
# produces a huge lag_seconds — that's correct, the analyst sees it and
# knows BGW never ran.  source_max NULL (empty source) → lag NULL.
PGOPTIONS='--client-min-messages=warning' \
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
     -At -F ',' -X -v ON_ERROR_STOP=1 \
     -c "
WITH src_chunk AS (
  -- Latest chunk's range_start (cheap, catalog-only)
  SELECT range_start FROM time_series.ts_chunk
   WHERE table_oid = 'public.cpu'::regclass
   ORDER BY chunk_number DESC LIMIT 1
),
src AS (
  -- max(time) bounded by latest chunk → chunk pruning works
  SELECT max(time) AS source_max
    FROM public.cpu
   WHERE time >= (SELECT range_start FROM src_chunk)
),
wm AS (
  -- Exclude ephemeral CAGGs created by ddl_churn (cv_scratch) and the
  -- iso2 repro fixture (repro_cv).  These are dropped/recreated every
  -- cycle, so their watermark is legitimately -infinity 98%+ of the
  -- time — noise the report layer already filters, but keeping it out
  -- of the CSV entirely means ad-hoc CSV analysis no longer has to.
  SELECT ca.user_view_name AS cagg,
         MIN(cw.watermark) AS watermark
    FROM time_series.continuous_agg ca
    JOIN time_series.cagg_watermark  cw ON cw.cagg_id = ca.cagg_id
   WHERE ca.user_view_name NOT IN ('cv_scratch', 'repro_cv')
   GROUP BY ca.user_view_name
),
policy AS (
  -- end_offset (interval) lives in the BGW policy job's config JSONB.
  -- Use a LEFT JOIN so a CAGG without a policy yields NULL end_offset
  -- rather than a missing row.
  --
  -- MATCH BOTH NAME FORMS (2026-07-24): add_continuous_aggregate_policy
  -- stores config->>'cagg_name' SCHEMA-QUALIFIED ('public.cv_1min'),
  -- while user_view_name is bare ('cv_1min').  The old bare-only match
  -- never joined, end_offset stayed NULL on every row, and the
  -- 'exceeded' flag downstream was hard-wired 'f' -- the judge blind
  -- spot that let soak_test_a's frozen cv_1hour (SOAK-20260723_170208,
  -- crashed-job livelock) ride 68+ min at max_lag=31930s under a green
  -- verdict.
  SELECT ca.user_view_name AS cagg,
         (config->>'end_offset')::interval AS end_offset,
         j.schedule_interval AS schedule_interval,
         ca.bucket_width AS bucket_width
    FROM time_series.continuous_agg ca
    LEFT JOIN time_series.bgw_job j
      ON j.config->>'cagg_name' IN (ca.user_view_name,
                                    ca.user_view_schema || '.' || ca.user_view_name)
   WHERE j.proc_name = 'policy_refresh_cagg'
)
-- -infinity guard: a freshly-created CAGG whose BGW hasn't run yet has
-- watermark = -infinity.  PG raises 'cannot subtract infinite timestamps'
-- on src.source_max - watermark, so we have to gate every subtraction
-- on watermark being finite.  Treating that case as lag = NULL (instead
-- of MAXINT or 'huge number') is the honest signal: the CAGG simply
-- hasn't been refreshed yet, lag is not defined.  Once BGW first refresh
-- lands, watermark becomes finite and lag values start flowing.
SELECT '$TS' AS ts,
       '$DB' AS db,
       wm.cagg,
       wm.watermark::text,
       src.source_max::text,
       CASE
         WHEN wm.watermark = '-infinity'::timestamptz THEN NULL
         WHEN src.source_max IS NULL THEN NULL
         ELSE EXTRACT(epoch FROM (src.source_max - wm.watermark))::bigint
       END AS lag_seconds,
       COALESCE(EXTRACT(epoch FROM policy.end_offset)::bigint, NULL) AS end_offset_seconds,
       CASE
         WHEN policy.end_offset IS NULL THEN 'f'
         WHEN src.source_max IS NULL THEN 'f'
         WHEN wm.watermark = '-infinity'::timestamptz THEN 'f'
         -- Threshold = STRUCTURAL FLOOR × SLACK_MULT, where the floor is
         -- everything a healthy CAGG is *supposed* to lag by:
         --
         --   end_offset         refresh deliberately stops this far from now
         -- + bucket_width       the boundary bucket is only materialised
         --                      once it is fully closed
         -- + schedule_interval  lag saws up by a whole period between ticks
         --
         -- Leaving bucket_width out of the floor (as this did until
         -- 2026-07-29) understates it badly on CAGGs whose bucket is large
         -- relative to end_offset, and produced a CRITICAL verdict on
         -- cv_5min in EVERY soak run to date: floor is 10m+5m+5m = 1200s,
         -- the old threshold was 10m*2+5m = 1500s, and observed healthy
         -- peaks reached ~2050s.  With the real floor and SLACK_MULT=2 the
         -- bar sits at 2400s, so ordinary saw-tooth peaks stop firing while
         -- genuinely stalled materialisation (the failure mode this monitor
         -- exists for) still does.
         WHEN (src.source_max - wm.watermark)
              > ((policy.end_offset
                  + COALESCE(policy.bucket_width, INTERVAL '0')
                  + COALESCE(policy.schedule_interval, INTERVAL '0'))
                 * ${SOAK_WATERMARK_SLACK_MULT:-2}) THEN 't'
         ELSE 'f'
       END AS exceeded
  FROM wm
  LEFT JOIN policy ON policy.cagg = wm.cagg
  CROSS JOIN src
 ORDER BY wm.cagg;
" 2>>"$ERR" >> "$CSV"
