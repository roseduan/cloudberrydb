#!/bin/bash
# scripts/soak/setup/02_seed_via_tsbs.sh
#
# Seed phase — pre-creates `cpu` as a `USING time_series` hypertable
# (chunk-partitioned by `time`), then uses TSBS to generate 8 days of
# synthetic CPU metric data and load it into the existing table via
# tsbs_load_timescaledb (with --create-metrics-table=false, so TSBS
# does not redefine the table).
#
# Why pre-create as hypertable (since time_series_v1 integration):
#   - `cpu` is the CAGG source; with hypertable chunking, the live
#     branch of cv_* views can prune chunks by time → drops realtime
#     query latency from seconds to milliseconds (see soak doc §九).
#   - Chunk-level eligibility also speeds up BGW refresh: the watermark
#     clamp query and the actual_boundary query both benefit from
#     per-chunk index scans on the routed forks.
#   - Per-chunk btree (ts_btree) — not built here; CAGG path scans by
#     time predicate which the chunk-routing already prunes.
#
# Why TSBS instead of hand-rolled INSERTs:
#   - Wide cpu schema (10 numeric metrics per row) matches industrial
#     telemetry shape, not a 4-column toy.
#   - Tag table with 10 dimensions (hostname/region/datacenter/...) lets
#     reconcile + future CAGGs cover multi-dim GROUP BY paths if needed.
#   - Deterministic PRNG (`--seed=$SOAK_SEED_SEED`) means TSBS output is
#     reproducible across cluster recreations.
#
# Tunables (env vars, all optional):
#   SOAK_TSBS_BIN           tsbs binaries dir (default: /home/gpadmin/tsbs_bin)
#   SOAK_SEED_SCALE         hosts in seed (default: 400 — matches SOAK_WORKLOAD_SCALE so
#                           continuous workload re-touches every seeded host, no tag bloat)
#   SOAK_SEED_DAYS          days of history (default: 8; 0 = cold-start, schema only)
#   SOAK_SEED_INTERVAL      log interval (default: 10s; gives ~28M rows for scale=400, 8d)
#   SOAK_SEED_SEED          PRNG seed (default: 123)
#   SOAK_CHUNK_INTERVAL     hypertable chunk width (default: 1 hour)
#                           — short profiles (tiny/small/medium) only
#                           accumulate 1-3 chunks at 1 day, which leaves
#                           the compression policy nothing to compress.
#                           1 hour matches IoT/SCADA/network-monitoring
#                           production tuning and gives soak enough
#                           chunk-routing/compress coverage in every
#                           profile.  Override with SOAK_CHUNK_INTERVAL
#                           to mirror generic-observability deployments
#                           (1 day) or finer-grained workloads.
#
# Connection vars come from SOAK_HOST/PORT/USER/DB (set by soak.sh).

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$SOAK_DIR/lib/helpers.sh"

TSBS_BIN="${SOAK_TSBS_BIN:-/home/gpadmin/tsbs_bin}"
SCALE="${SOAK_SEED_SCALE:-400}"
DAYS="${SOAK_SEED_DAYS:-8}"
INTERVAL="${SOAK_SEED_INTERVAL:-10s}"
SEED="${SOAK_SEED_SEED:-123}"
CHUNK_INTERVAL="${SOAK_CHUNK_INTERVAL:-1 hour}"

# Sanity-check the binaries before doing anything expensive
for b in tsbs_generate_data tsbs_load_timescaledb; do
  if [[ ! -x "$TSBS_BIN/$b" ]]; then
    echo "FATAL: missing $TSBS_BIN/$b — build with 'make' in tsbs/" >&2
    exit 1
  fi
done

# Time window: [now - DAYS, now).  TSBS expects RFC3339 UTC.
#
# 2026-06 fix: seed end is now() (was: previous UTC midnight).  The
# original day-aligned end was meant to keep wide-bucket CAGG boundaries
# "clean", but bucket alignment is governed by time_bucket() origin
# inside the CAGG, not by seed END — a partial last bucket just means the
# tail bucket has fewer rows, never a correctness issue.
#
# The day alignment created a real problem instead: workload begins at
# the driver's wall-clock NOW (typically several hours past midnight),
# so seed and workload were separated by a multi-hour data gap.  In
# round 3 that gap pinned watermark advancement (refresh policy uses a
# sliding window that never revisits the gap region), so CAGG mat
# stayed at the seed-end timestamp for the whole run.
NOW_EPOCH=$(date -u +%s)
END_EPOCH=$NOW_EPOCH
START_EPOCH=$(( END_EPOCH - DAYS * 86400 ))
TS_START=$(date -u -d "@$START_EPOCH" '+%Y-%m-%dT%H:%M:%SZ')
TS_END=$(date -u -d "@$END_EPOCH" '+%Y-%m-%dT%H:%M:%SZ')
# Hypertable chunk origin: a fixed time-axis anchor independent of the
# seed window.  Set 30 days before NOW so late_arrival, gateway-buffered
# writes, etc., never fall below the origin.  See the long comment near
# the CREATE TABLE for the forknum-headroom rationale.
CHUNK_ORIGIN=$(date -u -d "@$(( NOW_EPOCH - 30 * 86400 ))" '+%Y-%m-%dT%H:%M:%SZ')

soak_log "[seed] TSBS generate + load"
soak_log "  scale          = $SCALE"
soak_log "  duration       = $DAYS days"
soak_log "  log interval   = $INTERVAL"
soak_log "  window         = $TS_START → $TS_END"
soak_log "  chunk interval = $CHUNK_INTERVAL (hypertable)"
# Row estimate derives from the ACTUAL log interval (strip the 's'
# suffix), not a hardcoded /10 that silently assumed INTERVAL=10s.
INTERVAL_SEC="${INTERVAL%s}"
soak_log "  estimated rows = $(( SCALE * DAYS * 86400 / ${INTERVAL_SEC:-10} ))"

# ── Pre-create cpu as a hypertable (USING time_series) + tags table ──
#
# TSBS' default DDL is `CREATE TABLE cpu (...) DISTRIBUTED BY (tags_id)`
# (regular heap).  We override by pre-creating cpu as a chunk-partitioned
# hypertable here and then passing `--create-metrics-table=false` /
# `--partition-index=false` / `--time-index=false` to the loader, so TSBS
# only INSERTs rows.  Row routing to chunks happens in the time_series
# table-AM transparently to TSBS.
#
# `ts_chunk_origin` is fixed at hypertable creation and acts as the
# time-axis floor: any INSERT with timestamp < ts_chunk_origin is
# rejected by chunk routing ("timestamp is before the chunk origin",
# ts_tableam.c:629).  Scenarios that need timestamps before NOW:
#   - historical-data backfill / migration
#   - gateway-buffered late writes (sensor offline, replays on reconnect)
#   - device clock skew (a few seconds to hours behind real time)
#   - the soak's own late_arrival loop writing 3-5h into the past
#
# We set ts_chunk_origin to NOW - 30 days (rather than to the seed
# start, which broke the cold-start case).  Why not even earlier?
# time_series chunk forknum is uint16 with TS_MAX_CHUNK_FORKNUM=65532
# (ts_tableam.c).  At chunk_interval=1h that caps the time axis at
# ~7.48 years from origin; 1970-01-01 would overflow immediately.
# NOW - 30 days:
#   - past capacity:   720 chunks (30d × 24h)   → covers MAX_LATE_DEPTH=6h
#                                                  by 120x; covers cv_1hour's
#                                                  6-hour start_offset
#   - future capacity: 64812 chunks (~7.4 years) → far more than any soak
#   - production guidance: pick a static origin that covers your widest
#                          expected backfill window AND leaves enough
#                          forknum headroom for your hypertable's lifetime
#                          (or use a larger chunk_interval).
soak_log "[seed] pre-create cpu (hypertable) + tags table"
PGOPTIONS='--client-min-messages=warning' \
psql -h "${SOAK_HOST:-localhost}" \
     -p "${SOAK_PORT:?}" \
     -U "${SOAK_USER:-gpadmin}" \
     -d "${SOAK_DB:-soak_test}" \
     -X -v ON_ERROR_STOP=1 <<EOF
DROP TABLE IF EXISTS public.cpu CASCADE;
DROP TABLE IF EXISTS public.tags CASCADE;

-- Hypertable on time; columns mirror TSBS' cpu-only use case exactly.
CREATE TABLE public.cpu (
    time             timestamptz NOT NULL,
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
    additional_tags  JSONB DEFAULT NULL
) USING time_series WITH (
    ts_partition_column = 'time',
    ts_chunk_interval   = '$CHUNK_INTERVAL',
    ts_chunk_origin     = '$CHUNK_ORIGIN'
)
DISTRIBUTED BY (tags_id);
-- WHY explicit DISTRIBUTED BY (tags_id) here:
--   Without this clause CBDB defaults to first column = "time", which means
--   rows for the same host end up on different segments (different time →
--   different hash) and the CAGG live-branch query
--     GROUP BY time_bucket('1h', time), tags_id
--   forces a Redistribute Motion (~400ms per query at 28M-row scale) to
--   collocate rows by group key.  With tags_id as distribution key, every
--   row for the same host lives on the same segment → standard PG planner
--   recognizes "distkey is subset of group keys" and emits a two-stage Partial+Gather
--   plan with no Motion, dropping cv_1hour_24h from ~1100ms to ~750ms and
--   eliminating the dominant cost item in the live branch.
--   Chunk routing is independent: ts_partition_column='time' still organizes
--   rows into daily chunks within each segment.

-- Tags table — TSBS-standard 10 dimensions.  REPLICATED so all segments
-- can resolve tags_id locally during CAGG / reconcile joins.
CREATE TABLE public.tags (
    id          SERIAL PRIMARY KEY,
    hostname    text,
    region      text,
    datacenter  text,
    rack        text,
    os          text,
    arch        text,
    team        text,
    service     text,
    service_version text,
    service_environment text
) DISTRIBUTED REPLICATED;
EOF
PSQL_RC=$?
if [[ $PSQL_RC -ne 0 ]]; then
  soak_log "[seed] hypertable pre-create failed (rc=$PSQL_RC)"
  exit $PSQL_RC
fi

# Cold-start: schema is created but no historical data is streamed.
# 03_caggs.sql still needs cpu / tags to exist; the workload loop will
# populate cpu from T=0 onwards.
if [[ "$DAYS" -eq 0 ]]; then
  soak_log "[seed] DAYS=0 (cold-start) — schema created, skipping TSBS data generation"
  exit 0
fi

# ── Stream TSBS generate → load into existing tables ──
# --create-metrics-table=false : skip TSBS' `CREATE TABLE cpu` DDL
# --partition-index=false       : skip TSBS' (tags_id, time DESC) index
# --time-index=false            : skip TSBS' (time DESC) index
#   ↑ both indexes are not needed: hypertable's chunk routing prunes by
#     time at fork level, and tags_id-driven queries are not the CAGG
#     access pattern; we save ~1.5 GB of disk on a scale=400 seed
#     (~4 GB at scale=1000, scales linearly).
# --do-create-db=false : soak.sh already CREATEd the database.
# --do-abort-on-exist=false : tolerant of re-runs.
"$TSBS_BIN/tsbs_generate_data" \
    --use-case=cpu-only \
    --scale="$SCALE" \
    --seed="$SEED" \
    --timestamp-start="$TS_START" \
    --timestamp-end="$TS_END" \
    --log-interval="$INTERVAL" \
    --format=timescaledb \
| "$TSBS_BIN/tsbs_load_timescaledb" \
    --cloudberry-mpp=true \
    --host="${SOAK_HOST:-localhost}" \
    --port="${SOAK_PORT:?}" \
    --user="${SOAK_USER:-gpadmin}" \
    --db-name="${SOAK_DB:-soak_test}" \
    --do-create-db=false \
    --do-abort-on-exist=false \
    --create-metrics-table=false \
    --partition-index=false \
    --time-index=false \
    --workers="${SOAK_SEED_WORKERS:-4}" \
    --batch-size="${SOAK_SEED_BATCH_SIZE:-10000}"

RC=$?
if [[ $RC -ne 0 ]]; then
  soak_log "[seed] tsbs pipeline failed (rc=$RC)"
  exit $RC
fi

soak_log "[seed] complete (cpu is hypertable, chunk_interval=$CHUNK_INTERVAL)"
