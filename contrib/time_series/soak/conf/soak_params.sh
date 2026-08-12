#!/bin/bash
# conf/soak_params.sh — THE soak parameter file.  One flat list, one
# value per parameter, zero control flow.
#
# ┌─────────────────────────────────────────────────────────────────┐
# │ These values ARE the best-practice set, shared by every round.  │
# │ We iterate THIS file toward the combo that finds the most real  │
# │ bugs; its git history is the tuning journal.                    │
# └─────────────────────────────────────────────────────────────────┘
#
# Capacity (how BIG a round is) is not decided here — the run_*.sh
# launchers (run_tiny / run_medium / run_large) set scale & duration
# and then defer to this file for everything behavioral.
#
# Priority (every line is set-if-unset):
#
#   user env  >  run_*.sh launcher  >  THIS FILE  >  per-script fallback
#
# Deviating from this file = explicit SOAK_X=y on the command line,
# and manifest.env records the deviation for the run.

# ═════════════════════════════════════════════════════════════════════
# PARAM LOCATIONS MAP — where each configurable value lives
# ═════════════════════════════════════════════════════════════════════
#
# Configurable values in the soak framework live in 8 places.  THIS
# FILE is the primary entry point (93 SOAK_* knobs); the other 7 hold
# values of a different nature.  Consult this map before changing
# anything to avoid "fix one, break three" or accidentally treating a
# test-subject definition as a tunable.
#
# ──── (1) Operator knobs — env-overridable, tuned frequently ─────────
#
#   [1] THIS FILE (conf/soak_params.sh)
#         Alarm / fail thresholds, monitor cadences, chaos level,
#         judge min_samples, framework internal timeouts.  Adjusting
#         these tunes "what counts as OK" without changing WHAT is
#         tested.  This is the file iterated most often when hunting
#         real bugs.
#
#   [2] soak.sh preset case branch (roughly lines 96-125)
#         Per-preset defaults: DURATION / SEED_SCALE / WORKLOAD_SCALE
#         / COMPRESS_AFTER / COMPRESS_SCHEDULE / SEED_DAYS.  Two
#         presets: smoke (5min, scale=100) and recommended (24h,
#         scale=500).  To adjust: edit the case branch, or override
#         via env at launch time
#         (SOAK_WORKLOAD_SCALE=n bash soak.sh --preset=X).
#
# ──── (2) Test-subject definitions — code-review only, not env-tunable
#
#   [3] setup/03_caggs.sql
#         CAGG test subject: cv_1min / cv_5min / cv_1hour bucket
#         width, aggregate columns, policy (start_offset, end_offset,
#         schedule_interval).  Editing these changes WHAT is tested;
#         runs with different CAGG shapes are not comparable.
#
#   [4] monitor/view_correctness.sql
#         Correctness check spec: 30min / 1h / 6h mat-check windows,
#         1e-4 / 1e-2 float tolerances.  This is the definition of
#         "view = source", not a tunable.
#
#   [5] monitor/perf_probe.sh
#         Perf baseline queries: fixed SQL over 1h / 24h / 7d
#         windows.  These are the baselines for long-term cross-run
#         diffs; changing them breaks the comparison chain.
#
#   [6] workload/query_load.sh
#         Concurrent-read workload content: 6 fixed SELECT statements.
#         Defines WHAT the workload reads.  Frequency knobs live in
#         [1]; the queries themselves live here.
#
#   [8] chaos/fault_catalog.csv
#         Chaos fault catalog: fault_name / level / target /
#         mechanism / active_s / recovery_budget_s / weight.  Table
#         structure: add/remove a fault = add/remove a row.
#
# ──── (3) Derived values — should follow [3], currently hardcoded ────
#
#   [7] judge/verdicts/refresh_perf.sh sched[] constants
#         sched["cv_1min"]=60*1000, cv_5min=300*1000,
#         cv_1hour=3600*1000.  ⚠ **SYNC CONSTANT** with [3]'s
#         schedule_interval: editing [3] REQUIRES a matching edit
#         here, or the verdict will apply a wrong threshold (silent
#         bug).  Known design smell.  Could later become a runtime
#         read from the catalog (bgw_job.config->>'schedule_interval')
#         to eliminate the double-write; for now the hand-coupled
#         hardcode is the contract.
#
# ──── Quick lookup: "where do I edit if I want to ..." ───────────────
#
#   Loosen / tighten a monitor threshold  →  [1] THIS FILE
#   Change run size / duration / chaos    →  [2] soak.sh preset, or
#                                            --scale / --duration /
#                                            --chaos flag
#   Add a new CAGG or reshape one         →  [3] + sync [7] sched[]
#                                            + re-audit [4] check
#                                            windows for alignment
#   Change correctness verdict criteria   →  [4]
#   Add / remove / tune a chaos fault     →  [8]
#   Extend the read workload pattern      →  [6] (add query) +
#                                            [1] (adjust cadence)
#
# See also the footer of THIS FILE: the "DELIBERATELY NOT PARAMETERS"
# section explains WHY [3]-[8] are intentionally kept out of env vars.
# ═════════════════════════════════════════════════════════════════════

# ── Connection / paths ───────────────────────────────────────────────
: "${SOAK_HOST:=localhost}"
: "${SOAK_PORT:=7000}"
: "${SOAK_USER:=gpadmin}"
# Two DBs: exercises BGW launcher cross-DB scheduling + catalog cache
# isolation.  One DB hides a whole class of launcher bugs.
: "${SOAK_DBS:=soak_test_a soak_test_b}"
: "${SOAK_TSBS_BIN:=/home/gpadmin/tsbs_bin}"
: "${SOAK_DATA_DIR:=/home/gpadmin/gpdata}"      # auto-detected at runtime when possible

# ── Capacity (canonical = the 24 h dev-nightly round) ────────────────
# soak.sh presets override these; user env overrides presets;
# you exactly the canonical round.
: "${SOAK_SEED_DAYS:=3}"
: "${SOAK_SEED_SCALE:=200}"
: "${SOAK_WORKLOAD_SCALE:=200}"
: "${SOAK_CHUNK_INTERVAL:=1 hour}"       # hypertable chunk width
# Backstop defaults only — both presets override these (smoke 2min/1min,
# recommended 30min/10min).  recommended deliberately runs compress_after
# SHORTER than MAX_LATE_DEPTH so late writes land on already-compressed
# chunks and exercise the PARTIAL → recompress path (the crown jewel of
# compression soak coverage).  Single-op compress/recompress correctness
# is owned by regression (test/regress/.../ts_reclaim_then_insert.sql),
# not by soak.
: "${SOAK_COMPRESS_AFTER:=12 hours}"
: "${SOAK_COMPRESS_SCHEDULE:=1 hour}"

# ── Workload shape (the write pressure) ──────────────────────────────
: "${SOAK_WORKLOAD_INTERVAL:=1}"        # s per TSBS batch — 1s = continuous stream
: "${SOAK_WORKLOAD_LOG_INTERVAL:=1s}"   # one sample per host per second
: "${SOAK_WORKLOAD_WORKERS:=2}"         # tsbs_load parallel writers per batch
: "${SOAK_WORKLOAD_BATCH_SIZE:=1000}"   # rows per INSERT batch (streaming load)
: "${SOAK_SEED_INTERVAL:=10s}"          # seed sampling sparser to keep size sane
: "${SOAK_SEED_WORKERS:=4}"             # seed load is bulk → more writers
: "${SOAK_SEED_BATCH_SIZE:=10000}"      # and bigger batches than streaming
# Fixed PRNG seed: deterministic hostnames keep the tags table bounded
# (varying seed ⇒ new hosts every batch ⇒ tags grows without limit).
: "${SOAK_SEED_SEED:=123}"

# ── Late arrival (invalidation → re-materialize coverage) ────────────
# The single most bug-productive workload we have: pure-append never
# exercises re-materialization at all (rounds 1-3 lesson).
#
# ITERATION 2026-06-10: 3600 → 900.  Unblocked by the decidable-region
# check in view_correctness.sql v2 — timing params can no longer cause
# false MISMATCH, only coverage exclusions (which the report counts).
# ITERATION 2026-06-16: single fixed now-1h batch → 3-MODE ROTATION
# (near/mid/bulk) so we cover the whole late-arrival spectrum instead
# of one point.  Each cycle rotates one mode; rows scatter via random()
# across the window (genuine out-of-order, multi-bucket).
: "${SOAK_LATE_ARRIVAL_EVERY:=900}"
#   near — jitter straddling the watermark (now-300s..now-30s); most
#          sensitive (part live / part mat → stresses watermark + clamp)
: "${SOAK_LATE_NEAR_OFFSET:=30}"
: "${SOAK_LATE_NEAR_SPAN:=270}"
: "${SOAK_LATE_NEAR_ROWS:=40}"
#   mid  — the classic settled now-1h single-bucket batch
: "${SOAK_LATE_OFFSET_SEC:=3600}"       # mid offset: how far back (1 bucket-hour ago)
: "${SOAK_LATE_MID_SPAN:=60}"
: "${SOAK_LATE_BATCH_ROWS:=60}"         # mid rows: small, tests path without stressing
#   bulk — large multi-hour backfill (device reconnect dumping buffered data)
: "${SOAK_LATE_BULK_OFFSET:=10800}"     # now-3h
: "${SOAK_LATE_BULK_SPAN:=7200}"        # spanning back to now-5h
: "${SOAK_LATE_BULK_ROWS:=2000}"
# MAX_LATE_DEPTH = deepest point ANY late mode writes (bulk offset+span)
# plus a buffer.  THE coupling anchor: view_correctness's mat-check
# windows stay OLDER than this, so a late write can never touch a
# mat-check bucket (→ coverage exclusion).  Change bulk offset/span ⇒
# bump this.
: "${SOAK_MAX_LATE_DEPTH_SEC:=21600}"   # 6h = bulk 5h deep + 1h buffer

# ── Query load (concurrent CAGG reads) ───────────────────────────────
# Read pressure concurrent with write/refresh/compress — the read↔refresh
# contention that pure-write soak never exercised.
: "${SOAK_QUERY_LOAD_CONCURRENCY:=4}"   # resident reader workers per DB (0 = disable)
: "${SOAK_QUERY_LOAD_PACE_MS:=100}"     # sleep between a worker's queries
: "${SOAK_QUERY_LOAD_FLUSH:=25}"        # queries per aggregate CSV row (bounds CSV size)
: "${SOAK_QUERY_LOAD_SLOW_MS:=2000}"    # wall-clock "slow query" threshold

# ── DDL churn (CAGG create/drop under concurrent load) ───────────────
: "${SOAK_DDL_CHURN_EVERY:=900}"        # 15 min — enough cycles/day to arm create-path faults
# The scratch CAGG that ddl_churn re-creates each cycle:
: "${SOAK_DDL_SCRATCH_BUCKET:=5 min}"
: "${SOAK_DDL_SCRATCH_START_OFFSET:=30 min}"
: "${SOAK_DDL_SCRATCH_END_OFFSET:=5 min}"
: "${SOAK_DDL_SCRATCH_SCHEDULE:=5 min}"

# ── BGW job-history retention (setup/01_extension.sql, job id=1) ─────
: "${SOAK_JOBLOG_SCHEDULE:=1 hour}"
: "${SOAK_JOBLOG_DROP_AFTER:=7 days}"   # matches the longest standard round (run_large 7d)

# ── Monitor cadences ─────────────────────────────────────────────────
: "${SOAK_VIEW_CORRECTNESS_EVERY:=1800}"  # the correctness check; heavy (full-window scan)
: "${SOAK_VIEW_STATEMENT_TIMEOUT:=10min}" # per-check SQL budget (passed via PGOPTIONS).
: "${SOAK_VIEW_RECHECK_SETTLE_SEC:=180}"  # targeted delayed re-check (§4): a MISMATCH bucket is
                                          # confirmed only if, >= this many seconds after it was
                                          # first seen, a re-query of THAT bucket still diverges
                                          # (and is decidable).  Decouples confirmation from the
                                          # sliding mat window, so a persistent divergence is
                                          # caught even when the window slid past the bucket
                                          # (single-cycle observation); a late-write transient
                                          # resolves within the settle delay -> not failed.
# Measured cv_1hour src-agg ~0.7s/3.3M heap rows (4.7M rows/s, MPP 3-seg);
# steady 6h window ~86M rows (mostly PAX) ⇒ ~18s isolated, ~1min under full
# 2-DB contention.  3min had headroom but a single load-spike incomplete
# costs a FALSE exit-2 (perf jitter masquerading as a correctness MISMATCH
# via VIEW_INCOMPLETE_FAIL_AFTER); ITERATION 2026-07-03: raised 5min → 10min
# after cold-start warmup on the 2026-07-02 12h scale=500 run consistently
# tripped the first cycle (T+30min, 1/271 verdicts became view_incomplete).
# 10min is still well below EVERY/VIEW_SLOW_DIV = 1800/4 = 450s slow-warn.
: "${SOAK_PERF_PROBE_EVERY:=300}"         # also drives refresh/compress scrapes
: "${SOAK_METRICS_EVERY:=20}"             # also the disk_full early-stop cadence
                                          # (judge/framework_health reads latest disk_pct)
: "${SOAK_WATERMARK_LAG_EVERY:=60}"       # cheap (~50ms); aggressive cadence caught the
                                          # cross-DB scheduler-kill stall (2026-06-09)
: "${SOAK_INVALIDATION_LOG_EVERY:=60}"    # L1/L2 bloat + drain-health sampling

# ── Escalation thresholds ────────────────────────────────────────────
: "${SOAK_DISK_KILL_PCT:=90}"             # panic.disk_full above this
# Soft alarm: peak disk_pct above this raises a report-time ⚠ WARN
# without killing the run.  Bridges the gap between "clean" (steady
# state, e.g. 38%) and "kill" (90%): the 2026-07-02 12h run peaked at
# 78% during the cold-start seed and returned to 38% — the spike was
# real but invisible unless you grepped system_metrics.csv by hand.
: "${SOAK_DISK_WARN_PCT:=70}"
# BGW memory-leak trend: compare last-quartile mean RSS to first-quartile
# mean (robust to the startup materialization spike).  README §0's first
# soak rationale is "scheduler memory leaks" — peak-only reporting was
# blind to a slow climb; this turns it into a signal.
: "${SOAK_RSS_GROWTH_WARN_PCT:=30}"       # last-Q vs first-Q growth → ⚠ WARN
: "${SOAK_RSS_GROWTH_FAIL_PCT:=100}"      # ... → fail.perf (doubled = real leak)
: "${SOAK_RSS_TREND_MIN_SAMPLES:=20}"     # need this many samples to judge a trend
# Invalidation log (L1/L2) health:
: "${SOAK_INVAL_NONEMPTY_WARN_PCT:=60}"   # L1 non-empty in > this % of samples → ⚠ WARN
: "${SOAK_INVAL_NONEMPTY_FAIL_PCT:=95}"   # ... → fail (invalidations not draining = stuck)
# ITERATION 2026-06-16: dropped the L1+L2 absolute-floor bloat test (it
# fired on L2's healthy monotonic accumulation — "+517% to 604 rows"
# right next to "L1 drains OK").  Now: L1 backlog trend (must drain) +
# L2 slope ACCELERATION (linear growth is fine, only acceleration leaks).
: "${SOAK_INVAL_GROWTH_WARN_PCT:=100}"    # L1 backlog up / L2 slope accel → bloat WARN
: "${SOAK_INVAL_L2_ACCEL_FLOOR:=0.05}"    # L2 slope (rows/sample) below this = noise floor
: "${SOAK_INVAL_TREND_MIN_SAMPLES:=20}"   # min samples to judge the bloat trend
: "${SOAK_WATERMARK_FAIL_AFTER:=5}"       # exceeded samples per cagg → fail.perf
: "${SOAK_WM_PINNED_FAIL_AFTER:=60}"      # -infinity-wm samples (source non-empty) per cagg → fail.perf.watermark_pinned
                                          # 60 ≈ 84min at the ~84s sample cadence: must exceed the slowest CAGG
                                          # cadence (cv_1hour, 1h) — a cold-started hourly CAGG legitimately
                                          # holds -infinity until its first tick WITH data (up to schedule +
                                          # empty-first-tick), which a 20-sample (~28min) limit false-flagged
                                          # at T+22min of SOAK-20260723_101159.
: "${SOAK_BGW_FAIL_PCT:=20}"              # non-chaos BGW run failure-rate %% → fail.perf.{refresh,compress}
: "${SOAK_WATERMARK_SLACK_MULT:=2}"       # lag > end_offset × this → 'exceeded' sample
: "${SOAK_VIEW_INCOMPLETE_FAIL_AFTER:=3}" # incomplete cycles → fail at report time
: "${SOAK_VIEW_INCOMPLETE_ABORT:=4}"      # CONSECUTIVE incompletes → panic (early stop)
: "${SOAK_VIEW_SLOW_DIV:=4}"              # check taking > EVERY/this → SLOW warning
: "${SOAK_VIEW_COVERAGE_WARN_PCT:=50}"    # cagg excluded in > this % of cycles → coverage WARN
# Hourly/daily buckets are structurally sparse in short runs: a 12h run
# only fits ~12 hourly buckets, so a handful of pending exclusions trips
# the default 50% threshold every time (2026-07-02: 4/6 hourly-CAGG
# WARNs, all structural, none actionable).  Use a looser threshold for
# hourly and daily bucket CAGGs.  Detection is name-based: user_view
# name containing '1hour' or '1day' → this pct, else default.
: "${SOAK_VIEW_COVERAGE_WARN_HOURLY_PCT:=85}"
: "${SOAK_PLAN_DRIFT_MIN_CYCLES:=4}"      # suppress drift diagnostics below this sample count
# NOTE: plan_drift is diagnostic-only — no fail flag.  If a probe shows
# "PLAN DRIFT + EXCESS SLOWDOWN" in report.txt, dig into
# plan_snapshots/<db>_<probe>_drift_*.txt to see what changed.
# Perf verdict thresholds (report time):
: "${SOAK_PERF_CRITICAL_RATIO:=2.0}"      # probe p95 > baseline × this → fail.perf
: "${SOAK_PERF_DEGRADED_RATIO:=1.5}"      # probe p95 > baseline × this → DEGRADED warn
: "${SOAK_PERF_SPEEDUP_NOTE_RATIO:=2.0}"  # cagg-vs-source speedup below this → NOTE
: "${SOAK_PERF_SLOWDOWN_FLOOR_MS:=50}"    # plan-drift slowdown only actionable when last-Q
                                          # mean exceeds this (a "2x" on sub-10ms fast probes
                                          # is sampling noise, not a regression)
: "${SOAK_BGW_P95_CRIT_MULT:=2}"          # refresh/compress p95 > schedule × this → fail.perf

# ── Framework internals ──────────────────────────────────────────────
: "${SOAK_STOP_GRACE_SEC:=2}"             # TERM → KILL gap at shutdown; cheap
                                          # cleanup (log lines, single psql) must
                                          # fit inside it — see chaos_loop cleanup
: "${SOAK_HEALTH_GATE_EVERY:=60}"         # driver wait-loop poll of signals/panic.*
: "${SOAK_CLUSTER_PORT_SPAN:=9}"          # our cluster's port range = PORT..PORT+span
                                          # (coordinator + gpdemo segments); watchdog
                                          # only cleans sockets in this range
: "${SOAK_VERIFY_TIMEOUT:=5}"             # verify_env connectivity-check timeout (s)
# Loop liveness: every oneshot wrapper touches a .beat file per cycle;
# the driver's health gate has two thresholds (no in-between "warn"):
#   age > STALE_MULT × interval           → soak_log a note only
#                                            (short blip: SSH hiccup,
#                                            OOM burst, load spike)
#   age > PERMANENTLY_DEAD_MULT × interval → raise
#                                            signals/panic.loop_permanently_dead.<name>
#                                            (health gate stops the run;
#                                            a monitor stayed dead for
#                                            that long → observation
#                                            gap so wide the rest of the
#                                            run has no visibility)
: "${SOAK_LOOP_BEAT_STALE_MULT:=3}"
: "${SOAK_LOOP_PERMANENTLY_DEAD_MULT:=10}"
# De-phasing: oneshot wrappers sleep a random 0..min(interval, this)
# seconds before their first cycle, so same-interval loops don't fire
# in lock-step every cycle (the synchronized stampede polluted
# perf_probe latency samples).  0 = disabled.
: "${SOAK_LOOP_STAGGER_MAX:=60}"

# ── Judge (business verdict engine) ─────────────────────────────────
# The judge/ module reads monitor CSVs and raises fail.* / chaos.*
# verdicts.  ON by default now that the first verdict (bgw_scheduler_
# health) has migrated here — that dimension's monitor --report is a
# no-op, so judge MUST run for it to be judged at all.  Verdicts not
# yet migrated still self-judge in their own monitor --report; the two
# coexist during the migration (see README.zh.md §3.6).
: "${SOAK_JUDGE_ENABLED:=1}"              # 1 = judge daemon does business verdicts; 0 = off
: "${SOAK_JUDGE_INTERVAL:=30}"            # verdict tick (s) — streaming re-evaluation cadence
# Minimum-evidence gate (shared by every threshold verdict).  A ratio or
# percentile verdict must NOT raise fail/WARN until it has at least this
# many NON-chaos samples.  Early in a chaos run the denominator is tiny
# (a single overdue sample out of 2 = 50%), so one chaos after-effect that
# lands in a window gap can trip a threshold the full run then disproves.
# Generalizes the #37 lesson (don't judge on insufficient evidence) from
# view_correctness to bgw_scheduler_health / refresh_perf / compress_perf.
: "${SOAK_JUDGE_MIN_SAMPLES:=10}"         # non-chaos samples required before a threshold verdict may fail/WARN

# ── Chaos ────────────────────────────────────────────────────────────
# SOAK_CHAOS_LEVEL is a binary switch:
#   0 = off  — no fault injection
#   1 = on   — inject every fault in chaos/fault_catalog.csv
#              (BGW worker kills + full cluster crash)
# Per-fault attributes (weight, active_s, recovery_budget_s) live in
# chaos/fault_catalog.csv.  Adjust the mix by editing weights there,
# or restrict to specific faults with SOAK_CHAOS_ONLY=<name>[,<name>...].
# Frequency is SOAK_CHAOS_AVG_INTERVAL — mean seconds between faults,
# jittered ±1/JITTER_DIV to avoid phase-locking with BGW schedules.
# 900s ≈ 96 faults/24h: lands often enough to matter without drowning
# the correctness signal.  Worker-kill faults are poll-then-kill
# (chaos_loop catches an in-flight refresh/compress/scheduler worker),
# so a hit is not a one-shot lottery.
: "${SOAK_CHAOS_LEVEL:=1}"
: "${SOAK_CHAOS_AVG_INTERVAL:=900}"
: "${SOAK_CHAOS_JITTER_DIV:=3}"
# Recovery measurement: after each RESET, chaos polls whether the
# target DB's managed-CAGG watermark is back within tolerance, timing
# how long recovery takes.  This is the THIRD leg of a chaos test
# (inject → damage → RECOVER) that was previously unmeasured — "did
# the next view_check happen to pass" is not a recovery time.  The
# poll runs during the quiescence dead-time, so it costs no extra
# wall-clock.  Recovery slower than the timeout → fail.view_mismatch
# (the cluster did not heal from an injected, supposedly-recoverable
# fault).
: "${SOAK_CHAOS_RECOVERY_TIMEOUT:=120}"   # s; give-up bound for one recovery poll
: "${SOAK_CHAOS_RECOVERY_POLL:=3}"        # s between recovery polls
: "${SOAK_CHAOS_GPSTART_TRIES:=3}"        # cluster-crash recovery: gpstart attempts,
                                          # each gated on a live coordinator connection
: "${SOAK_CHAOS_WINDOW_GRACE:=5}"         # s added to active_s when stamping the chaos
                                          # exclusion window end, covering post-kill
                                          # transient before queries recover

export SOAK_HOST SOAK_PORT SOAK_USER SOAK_DBS SOAK_TSBS_BIN SOAK_DATA_DIR
export SOAK_SEED_DAYS SOAK_SEED_SCALE SOAK_WORKLOAD_SCALE
export SOAK_CHUNK_INTERVAL SOAK_COMPRESS_AFTER SOAK_COMPRESS_SCHEDULE
export SOAK_WORKLOAD_INTERVAL SOAK_WORKLOAD_LOG_INTERVAL SOAK_WORKLOAD_WORKERS SOAK_WORKLOAD_BATCH_SIZE
export SOAK_SEED_INTERVAL SOAK_SEED_WORKERS SOAK_SEED_BATCH_SIZE SOAK_SEED_SEED
export SOAK_LATE_ARRIVAL_EVERY SOAK_LATE_OFFSET_SEC SOAK_LATE_BATCH_ROWS
export SOAK_LATE_NEAR_OFFSET SOAK_LATE_NEAR_SPAN SOAK_LATE_NEAR_ROWS
export SOAK_LATE_MID_SPAN SOAK_LATE_BULK_OFFSET SOAK_LATE_BULK_SPAN SOAK_LATE_BULK_ROWS
export SOAK_MAX_LATE_DEPTH_SEC
export SOAK_QUERY_LOAD_CONCURRENCY SOAK_QUERY_LOAD_PACE_MS SOAK_QUERY_LOAD_FLUSH SOAK_QUERY_LOAD_SLOW_MS
export SOAK_DDL_CHURN_EVERY
export SOAK_VIEW_CORRECTNESS_EVERY SOAK_VIEW_STATEMENT_TIMEOUT SOAK_PERF_PROBE_EVERY SOAK_METRICS_EVERY
export SOAK_VIEW_RECHECK_SETTLE_SEC
export SOAK_DISK_WARN_PCT SOAK_VIEW_COVERAGE_WARN_HOURLY_PCT
: "${SOAK_BGW_SCHED_HEALTH_EVERY:=60}"
: "${SOAK_BGW_OVERDUE_WARN_SEC:=60}"      # a job overdue by more than this counts as "delayed"
: "${SOAK_BGW_OVERDUE_WARN_PCT:=20}"      # > this % of samples with any delayed job → ⚠ WARN
: "${SOAK_BGW_OVERDUE_FAIL_PCT:=50}"      # ... → fail.perf (scheduler chronically stalled)
export SOAK_BGW_SCHED_HEALTH_EVERY SOAK_BGW_OVERDUE_WARN_SEC SOAK_BGW_OVERDUE_WARN_PCT SOAK_BGW_OVERDUE_FAIL_PCT
export SOAK_WATERMARK_LAG_EVERY
export SOAK_DISK_KILL_PCT SOAK_WATERMARK_FAIL_AFTER
export SOAK_WM_PINNED_FAIL_AFTER SOAK_BGW_FAIL_PCT
export SOAK_RSS_GROWTH_WARN_PCT SOAK_RSS_GROWTH_FAIL_PCT SOAK_RSS_TREND_MIN_SAMPLES
export SOAK_INVALIDATION_LOG_EVERY SOAK_INVAL_NONEMPTY_WARN_PCT SOAK_INVAL_NONEMPTY_FAIL_PCT
export SOAK_INVAL_GROWTH_WARN_PCT SOAK_INVAL_L2_ACCEL_FLOOR SOAK_INVAL_TREND_MIN_SAMPLES
export SOAK_VIEW_INCOMPLETE_FAIL_AFTER SOAK_VIEW_INCOMPLETE_ABORT SOAK_PLAN_DRIFT_MIN_CYCLES
export SOAK_STOP_GRACE_SEC SOAK_HEALTH_GATE_EVERY
export SOAK_CHAOS_LEVEL SOAK_CHAOS_AVG_INTERVAL SOAK_CHAOS_JITTER_DIV
export SOAK_CHAOS_RECOVERY_TIMEOUT SOAK_CHAOS_RECOVERY_POLL SOAK_CHAOS_GPSTART_TRIES
export SOAK_CHAOS_WINDOW_GRACE
export SOAK_WATERMARK_SLACK_MULT SOAK_VIEW_SLOW_DIV SOAK_VIEW_COVERAGE_WARN_PCT
export SOAK_PERF_CRITICAL_RATIO SOAK_PERF_DEGRADED_RATIO SOAK_PERF_SPEEDUP_NOTE_RATIO SOAK_BGW_P95_CRIT_MULT
export SOAK_PERF_SLOWDOWN_FLOOR_MS
export SOAK_DDL_SCRATCH_BUCKET SOAK_DDL_SCRATCH_START_OFFSET SOAK_DDL_SCRATCH_END_OFFSET SOAK_DDL_SCRATCH_SCHEDULE
export SOAK_JOBLOG_SCHEDULE SOAK_JOBLOG_DROP_AFTER
export SOAK_CLUSTER_PORT_SPAN SOAK_VERIFY_TIMEOUT
export SOAK_LOOP_BEAT_STALE_MULT SOAK_LOOP_STAGGER_MAX
export SOAK_JUDGE_ENABLED SOAK_JUDGE_INTERVAL SOAK_JUDGE_MIN_SAMPLES

# ═════════════════════════════════════════════════════════════════════
# DELIBERATELY NOT PARAMETERS (so nothing is undocumented):
#   - setup/03_caggs.sql CAGG definitions (bucket widths, aggregate
#     columns, policy start/end_offset, schedule_interval): these
#     DEFINE the test subject.  Changing them changes WHAT is tested,
#     and three other places are hand-coupled to them (the check
#     windows in view_correctness.sql, the sched[] map in
#     refresh_perf_scrape.sh --report, the lag tolerances read from
#     the policy config).  Change via code review, not env vars.
#   - view_correctness.sql check windows + float tolerances (1e-4 /
#     1e-2): check definitions, coupled to the CAGG layout above.
#   - perf_probe.sh probe queries (1h/24h/7d windows, LIMIT 1000):
#     probe definitions.  Soak does not maintain a shared perf
#     baseline (numbers depend on host/preset/scale).  Run-to-run
#     comparison is done by diffing perf_probe.csv across result dirs.
#   - diagnostic output truncation (LIMIT 30/20 in mismatch snapshots,
#     cut -c200 in chaos detail): display formatting, not behavior.
#   - unit-conversion tables (86400 s/day, the sec/min/hour/day → ms
#     parser in compress_perf_scrape) and gp_inject_fault API constants
#     (start=1, end=-1): definitional, not tunable.
#   - view_correctness mat-check windows: stay OLDER than
#     SOAK_MAX_LATE_DEPTH_SEC by construction, so late writes never dirty
#     a mat-check bucket.  The depth is a param; the coupling (windows <
#     depth) is code.
#   - per-script `${SOAK_X:-fallback}` defaults and SOAK-LOOP header
#     interval=: the layer-1 safety net; values here always win.
#   - run_*.sh capacity values: the launcher layer, by design.
# ═════════════════════════════════════════════════════════════════════
