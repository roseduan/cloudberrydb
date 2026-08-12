# CAGG Soak-Test Framework — Design & Operations Manual

> **中文版本**: [README.zh.md](./README.zh.md)
> **5-minute quickstart**: §1
> **Last updated**: 2026-07-09

This directory is the **continuous-aggregate (CAGG) soak-test framework** for the Apache Cloudberry `time_series` extension. It continuously streams time-series writes into an MPP cluster and periodically verifies that the view is row-for-row equivalent to the source table.

---

## Table of Contents

**Getting started**
- [§0 Motivation](#0-motivation)
- [§1 Prerequisites + Run](#1-prerequisites--run)
- [§2 Capacity planning (picking a preset)](#2-capacity-planning-picking-a-preset)
- [§3 Configuration reference](#3-configuration-reference)

**Active verification**
- [§4 Correctness verification logic](#4-correctness-verification-logic)
- [§5 Performance acceptance](#5-performance-acceptance-view-query-latency)
- [§6 Chaos injection](#6-chaos-injection)

---

## §0 Motivation

The existing regress / isolation2 suites cover **functional correctness + single-fault recovery**, but a few classes of problem only surface under sustained, long-running operation:

| Problem class | regress catches | needs soak |
|---|:---:|:---:|
| Scheduler process memory leak (missed MemoryContextReset) | ✗ | ✓ |
| L1/L2 invalidation-log backlog (row growth after thousands of refreshes) | ✗ | ✓ |
| `bgw_job_stat` accumulation accuracy (total_runs overflow, duration drift) | ✗ | ✓ |
| watermark contention among CAGGs sharing one source table | partial | ✓ |
| resource exhaustion from repeated worker fork/exit (DSM / fd / PID cycling) | ✗ | ✓ |
| scheduling-precision drift (fixed_schedule cumulative error against the real clock) | ✗ | ✓ |
| deadlock / livelock at the 3-way crossing of high-concurrency DML + DDL + refresh | isolation2 partial | ✓ |
| BGW worker dying silently after finishing a refresh (ghost row) | ✗ | ✓ |
| hypertable chunk-routing stability under continuous INSERT (mis-routing, fork overrun) | partial | ✓ |
| CAGG bucket vs chunk alignment drift across chunk boundaries | ✗ | ✓ |
| hypertable per-chunk fork files accumulating over time (fd / inode leak, ts_relfilenode bloat) | ✗ | ✓ |

**Pass/fail watches only two things** — everything else (FATAL / segment-down / deadlock / OOM …) surfaces naturally via system exceptions / error logs and **needs no separate check**:

- **Data correctness (§4)**: **no bucket persistently diverges** (a MISMATCH bucket must **still diverge on a delayed re-check** to escalate to exit 2 — the decidable-region check + targeted delayed re-check filter out the watermark-boundary eventual-consistency transient; a transient that self-heals within the settle delay is recorded, not failed)
- **Query performance (§5)**: for every (db, probe) the hard floor `cv_1hour_24h / source_24h < 1` alarms at any time (view slower than the source → scan-path regression); `plan_sig` unchanged across cycles (plan-drift alarm)
- **Optional chaos (§6)**: chaos mode additionally verifies the fault-recovery path; exit code +8 on unexpected PANIC / unrecovered segment

Other signals such as RSS / disk_pct are recorded by `system_metrics.csv` and are visible to the naked eye when something breaks, so they are **not active verification items**. Intermediate-state signals (mat-table lag, L1/L2 size, watermark monotonicity, BGW counters) are not sampled routinely — they are captured only via an on-trigger dump when view_correctness MISMATCHes (`view_mismatch_*.snapshot.txt`).

---

## §1 Prerequisites + Run

The soak framework is **responsible only for soak logic**, not for how the CBDB cluster is deployed. You must already have a working CBDB cluster (docker / sandbox / bare-metal / Kubernetes — any deployment), then soak runs on top of it.

### 1.1 Prerequisites

Before `bash soak.sh`, your environment must satisfy these 8 conditions:

| # | Check | Note |
|---|---|---|
| 1 | psql can reach the coordinator | `psql -h $SOAK_HOST -p $SOAK_PORT -U $SOAK_USER -d postgres -c 'SELECT 1'` |
| 2 | `shared_preload_libraries` contains `time_series` | `gpconfig -c shared_preload_libraries -v 'time_series' && gpstop -ar` |
| 3 | `time_series` extension installable | time_series.so + .control in `$prefix/lib/postgresql/` and `$prefix/share/postgresql/extension/` |
| 4 | `gp_inject_fault` extension installable | required for chaos; `cd gpcontrib/gp_inject_fault && make USE_PGXS=1 install` |
| 5 | PAX access method registered | required for auto-compression; build CBDB with `--enable-pax` |
| 6 | user has CREATEDB privilege | driver creates/drops `soak_test_a`/`soak_test_b` |
| 7 | all cluster segments status='u' | `SELECT * FROM gp_segment_configuration` |
| 8 | TSBS binary executable | see below |

### 1.2 One-shot verification

```bash
cd contrib/time_series/soak
SOAK_HOST=localhost SOAK_PORT=7000 SOAK_USER=gpadmin \
SOAK_TSBS_BIN=/path/to/tsbs/bin \
  bash tools/verify_env.sh
```

All 8 checks must pass (✓✓✓✓✓✓✓✓) before you can run soak. Any ✗ tells you how to fix it.

### 1.3 Build the TSBS binary

soak uses [TSBS](https://github.com/timescale/tsbs) to seed data and drive the continuous write workload. Build TSBS **for the CPU/OS of the machine running soak**:

```bash
git clone https://github.com/timescale/tsbs                  # or the company fork
cd tsbs
make tsbs_generate_data tsbs_load_timescaledb                # ~1 min
export SOAK_TSBS_BIN=$(pwd)/bin
```

**Note**: the TSBS binary must match the **OS/arch of the machine running soak.sh**. A binary built on Mac cannot run inside a Linux container (Mach-O vs ELF).

### 1.4 Run a soak

```bash
cd contrib/time_series/soak

bash soak.sh                       # default recommended preset: 24h, scale=500, cold-start
bash soak.sh --preset=smoke        # 5-min framework self-check (~1 GB)
bash soak.sh --duration=4h         # recommended but only 4h
bash soak.sh --warm-start          # switch to warm-start (pre-seed 1d of history)
bash soak.sh --chaos=1             # add chaos (binary switch: 0=off, 1=on)

# Full tunables: see §3.1 (CLI flags) + conf/soak_params.sh (86 backstop defaults)
```

Results land in `soak/SOAK-<timestamp>/` — always next to the `soak.sh` script (`SOAK_RESULTS_BASE` defaults to the script's own directory, so it doesn't matter where you `cd`'d to run it). Set `SOAK_RESULTS_BASE=<path>` to pin an explicit location.

### 1.5 Inspect results

```bash
ls -td SOAK-*/ | head -1                     # find the latest run
bash soak.sh --report $(ls -td SOAK-*/ | head -1)  # re-generate the summary report
```

Exit codes:
- `0` = clean
- `2` = correctness MISMATCH (real view-vs-source bug, serious)
- `4` = perf regression (p95 > baseline × 2.0)
- `8` = segment unrecovered after chaos / inject-reset count mismatch
- `6`/`10`/`12`/`14` = combinations

See §3.3.

---

## §2 Capacity planning (picking a preset)

Only two presets:

| Preset | scale | Default duration | Peak disk | compress_after / schedule | Use case |
|---|---|---|---|---|---|
| `smoke`       | 100 | 5 min | ~1 GB             | 2 min / 1 min   | **Framework self-check** — 5-min sanity after a `soak/*` code change |
| `recommended` | 500 | 24 h  | ≤ 100 GB (budget) | 30 min / 10 min | **Product baseline** — find product bugs, gather report data |

Aliases: `recommended` = `default` / `prod` / `bp`.

- **cold-start (default)**: the cpu table starts empty and the workload fills it from T=0; `--warm-start` switches to pre-seeding 1d of history (the preset's `SOAK_SEED_DAYS`).
- **compress_after / schedule**: once a chunk has gone `compress_after` without a write, the BGW compresses it into PAX every `schedule`.
- **Disk**: peak ≈ scale × duration × 500 B/row × #DBs + mat tables + WAL (tighter `compress_after` → smaller). recommended pins scale=500 because 4000 hit ~89 GB (71%) by 8h, and 24h would trip the `SOAK_DISK_KILL_PCT=90` panic gate.

**Which one**: touched `soak/*` code → `smoke`; running the product / gathering data → `recommended`. For shorter/smaller, scale it down on recommended, e.g. `bash soak.sh --duration=4h --scale=200`.

**Override priority (high → low)**: `CLI flag > user env (SOAK_X=…) > preset default > conf/soak_params.sh backstop`. E.g. `SOAK_COMPRESS_AFTER='5 min' bash soak.sh --duration=2h --chaos=1`. Full flag list in §1.4 / §3.1.

---

## §3 Configuration reference

### 3.1 CLI flags (soak.sh)

```
bash soak.sh [options]

  --preset=NAME       smoke | recommended (default: recommended)
                      recommended aliases: default / prod / bp
                      see §2 Capacity planning
  --duration=10m      override the preset's default duration
                      supports s / m / h / d, e.g. 30m / 24h / 7d
  --scale=N           override SOAK_SEED_SCALE + SOAK_WORKLOAD_SCALE together
                      (the most common override, no need to export both vars)
  --cold-start        start from an empty table (default, workload fills from T=0)
  --warm-start        pre-seed the preset's SOAK_SEED_DAYS days of history
  --chaos=N           shortcut for SOAK_CHAOS_LEVEL, N ∈ {0,1} (binary switch), see §6
  --port=7000         coordinator port
  --db=NAME           single DB
  --dbs="a b c"       multiple DBs (default "soak_test_a soak_test_b")
  --skip-setup        skip DROP/CREATE DB + seed, resume on an existing environment
  -h, --help          help
```

### 3.2 Environment variables

Set automatically by the preset (overridable by explicit env):

| Env | Description |
|-----|------|
| `SOAK_SEED_DAYS`              | seed history days |
| `SOAK_SEED_SCALE`             | seed TSBS host count |
| `SOAK_WORKLOAD_SCALE`         | workload TSBS host count = write rate (rows/s/DB) |
| `SOAK_VIEW_CORRECTNESS_EVERY` | view-vs-source auto-fail check period, seconds |
| `SOAK_PERF_PROBE_EVERY`       | perf-probe period, seconds |
| `SOAK_COMPRESS_AFTER`         | auto-compression policy's compress_after (interval string, e.g. '6 hours') |
| `SOAK_COMPRESS_SCHEDULE`      | compression policy's schedule_interval (interval string, e.g. '1 hour') |

Not preset-controlled (defaults suit most cases):

| Env | Default | Description |
|-----|------|------|
| `SOAK_HOST`                 | localhost | coordinator hostname |
| `SOAK_PORT`                 | 7000      | coordinator port |
| `SOAK_USER`                 | gpadmin   | DB user |
| `SOAK_DBS`                  | "soak_test_a soak_test_b" | multi-DB list (space-separated) |
| `SOAK_RESULTS_BASE`         | soak.sh's directory | base of the results directory; each run creates `<base>/SOAK-<timestamp>/` |
| `SOAK_DISK_KILL_PCT`        | 90        | safe cutoff — SIGINT the driver when disk% exceeds this, 0=off |
| `SOAK_DATA_DIR`             | /home/gpadmin/gpdata | df monitoring target |
| `SOAK_METRICS_EVERY`        | 60        | system_metrics.sh sampling period |
| `SOAK_CHAOS_LEVEL`          | 1         | binary switch 0/1, see §6 |
| `SOAK_TSBS_BIN`             | /home/gpadmin/timedb/tsbs/bin | TSBS binary path (inside Docker it's /opt/tsbs/bin) |

### 3.3 Exit code + verdicts directory (verdict decoupled from completion)

**Exit code only signals completion, NOT verdict:**

```
0 = run completed (natural 24h expiry OR panic-driven early stop)
1 = setup failure (cluster down / extension won't install / seed failed)
3 = user interrupt (SIGINT / SIGTERM)
```

**Two-tier verdict only** — signals/panic.\* (environment failure, early
stop) + verdicts/fail.\* (SLO broken, run continues).  No intermediate
warn.\* advisory layer.

**"Did this run meet its SLOs" lives entirely in `$RESULTS/verdicts/`.**
Each trigger appends one row (timestamp + context); files are split
by category:

```
verdicts/fail.perf.refresh                   # refresh p95 > 2× schedule
verdicts/fail.perf.compress                  # compress p95 > 2× schedule
verdicts/fail.perf.bgw_overdue               # policy overdue ratio too high
verdicts/fail.perf.watermark                 # watermark persistently lagging
verdicts/fail.perf.rss_leak                  # RSS grew too much
# (plan drift is diagnostic-only — see report.txt + plan_snapshots/, no fail flag)
verdicts/fail.correctness.view_mismatch      # CAGG mat vs live diverged
verdicts/fail.correctness.view_incomplete    # too many consecutive incomplete checks
verdicts/fail.correctness.invalidation_stuck # L1 non-empty for too long
```

**Deciding whether a run is healthy — two ls commands**:

```
ls $RESULTS/signals/panic.*   non-empty → environment failed mid-run (early stop)
ls $RESULTS/verdicts/fail.*   non-empty → SLO broken (run may still have finished 24h)
both empty                    → clean run
```

Audit workflow:
1. `ls $RESULTS/verdicts/` — see which SLO classes fired
2. `wc -l $RESULTS/verdicts/<file>` — count triggers per class
3. `head/tail $RESULTS/verdicts/<file>` — inspect specific context rows

`signals/panic.*` still trips the driver's **health gate**: the wait
loop polls `signals/panic.*` every minute and terminates early on sight
— a soak whose environment has already failed has no point spinning
idle (2026-06-09 incident: after isolation2 dropped the library it spun
idle for 16 hours; this gate came from that).

**Loop-heartbeat safeguard**: if an oneshot loop's beat file ages out
beyond `SOAK_LOOP_PERMANENTLY_DEAD_MULT × interval` (default 10× —
so 10 min for a 60s loop), the driver raises
`signals/panic.loop_permanently_dead.<name>` and stops the run.
Short blips (age > STALE_MULT × interval but under the permanent-death
threshold) only produce a soak_log note; no flag.

CI / cron use the exit code directly for pass/fail; see `report.txt` for detail.

### 3.4 Output tree (soak_results/YYYYMMDD_HHMMSS_<dur>/)

```
├── report.txt                  # assembled by tools/report.sh (re-runnable against any past dir)
├── manifest.env                # ★ every SOAK_* param in effect this round + git SHA (frozen after 3-layer merge)
├── signals/                    # ★ runtime IPC: chaos_active + panic.* (consumed by soak.sh main loop)
├── verdicts/                   # ★ end-of-run SLO audit: fail.* only (see report.txt)
├── .pids/                      # per-loop process-group ids recorded by the supervisor
├── view_correctness.csv        # ★ one row per CAGG window: ts, db, cagg, window, total_rows, mismatch_count, verdict
├── view_incomplete.csv / view_defer.csv
├── view_correctness.err        # psql errors (expected empty)
├── perf_probe.csv              # ts, db, probe, exec_ms, plan_ms, plan_sig
├── refresh_durations.csv       # ★ BGW refresh durations (passive scrape)
├── compress_durations.csv      # ★ BGW compress durations + chunks/bytes
├── watermark_lag.csv           # ★ per-CAGG materialization progress
├── late_arrival.csv / ddl_churn.csv
├── system_metrics.csv          # ts, disk_pct, rss_total, bgw_workers, ...
├── <loop>.log / <loop>.err     # per-loop stdout / stderr
├── view_mismatch_*.snapshot    # ★ dump L1/L2/watermark immediately on MISMATCH
└── chaos_log.csv               # ★ chaos mode: every inject/reset
```

### 3.5 Architecture

```
══════════════════════════════════ CAGG SOAK ARCHITECTURE ══════════════════════════════════

  Entry                                   Parameter layer (single source of truth)
 ┌─────────────────────────┐    ┌──────────────────────────────────────┐
 │ bash soak.sh            │    │ conf/soak_params.sh                  │
 │   --preset=smoke|       │    │  86 shared defaults, flat pure data  │
 │   recommended           │    │  + "deliberately NOT params" list    │
 │   [--scale=N]           │    └──────────────┬───────────────────────┘
 │   [--duration=X]        │                   │  source (set-if-unset)
 │   [--cold/--warm-start] │                   ▼
 │   [--chaos=N]           │   priority: CLI flag > user env > preset default > conf
 └────────────┬────────────┘
              │
              ▼
 ┌──────────────────────── soak.sh (pure lifecycle) ───────────────────────────┐
 │  1.SETUP          2.MANIFEST     3.RUN           4.WAIT           5.STOP→report │
 │  setup/01..03  →  manifest.env → supervisor  →   health gate:  →  stop_all      │
 │  + record         (freeze all     start_loops     poll panic.*     (per process │
 │    DB OID          params+gitSHA)     │            + loop beats      group, all) │
 └───────────────────────────────────────┼────────────────────────────────────────┘
                                         │ auto-discover by scanning SOAK-LOOP headers
                                         │ each loop = setsid independent process group
                                         │ + staggered start + per-cycle .beat heartbeat
        ┌────────────┬────────────┬────────────┬───────────┬────────────┐
        ▼            ▼            ▼            ▼           ▼            ▼
 ┌───────────┐┌──────────────┐┌────────────┐┌──────────┐┌──────────┐┌────────────┐
 │ workload/ ││ monitor/     ││ chaos/     ││ judge/   ││ tools/   ││ (ext point)│
 │ (writers) ││ (collect)    ││ (sole      ││ verdicts ││ report   ││ drop in a  │
 │           ││              ││  breaker)  ││ (on by   ││ verify_  ││ script     │
 │ tsbs      ││ view_check ◄─┼┤            ││  default)││  env     ││ with a     │
 │  stream   ││ perf_probe   ││ fault_     ││          ││ selftest ││ SOAK-LOOP  │
 │  ×2DB     ││ refresh_scr. ││  catalog   ││ judged   │└──────────┘│ header →   │
 │ late_arriv││ compress_scr.││ (4 faults) ││ (stream+ │            │ auto-wired │
 │ ddl_churn ││ watermark_lag││    ↓       ││  report) │            └────────────┘
 │           ││ invalidation ││ chaos_loop ││ verdicts/│  read CSV → fail.*
 │           ││ system_metr. ││  weighted  ││ 8 verdict┼─ / chaos.*
 │           ││ bgw_sched_hlt││  random    ││  all here)│
 └─────┬─────┘└──────┬───────┘│  →inject→  │└──────────┘
       │ INSERT/DDL  │        │  armed     │
       ▼             ▼ SELECT │  →reset→   │
       │             │        │  quiesce   │
       │             │        └────┬───────┘
       │             │             ▼ gp_inject_fault
 ┌──────────────────────────────────────────────────────────────┐
 │              CloudberryDB cluster (system under test)         │
 │  cpu hypertable ×2DB → trigger→L1 → BGW refresh → 4×CAGG     │
 │  → watermark → compression policy → PAX chunks               │
 └──────────────────────────────────────────────────────────────┘
        │ loops communicate only through files (no IPC variable passing)
        ▼
 ┌─────────────── $RESULTS/ (one dir per run = the only shared state) ──────────────────┐
 │  manifest.env       *.csv (one per loop, append-only)   signals/ ◄─ runtime IPC       │
 │  (param snapshot)   view_correctness / watermark_lag /    ├ chaos_active (judge reads)│
 │  .pids/ + .beat     perf_probe / refresh / compress /     └ panic.* → early-stop      │
 │  (pgroups+beats)    late_arrival / ddl_churn / chaos_log                              │
 │  .db_oid_* (guard)  view_mismatch_*.snapshot (crime scene) verdicts/ ◄─ SLO audit     │
 │                                                            └ fail.*  (SLO broken)    │
 └──────────────────────────────┬────────────────────────────────────────────────────────┘
                                │ after the run (or anytime, against any past dir)
                                ▼
 ┌─────────────── tools/report.sh (stateless glue, ~70 lines) ──────────────────────────┐
 │  walk every SOAK-LOOP script by order=, call `script --report $RESULTS` on each       │
 │  → report.txt (each script writes its own report section) + overall flags verdict     │
 └───────────────────────────────────────────────────────────────────────────────────────┘

 Design principles:
  ① one feature = one file (collect + report in the same file; add monitoring = drop one
     file, zero edits to existing code)
  ② parameters have a single home (soak_params.sh); the entry only sets capacity;
     manifest.env records every deviation
  ③ monitor read-only / chaos holds the sole break-it right / loops pass only files
  ④ checks never assume WHEN the background finishes, only observe WHETHER it finished
     within one snapshot (the decidable region)
  ⑤ panic = early stop, no idle spinning; the framework also monitors itself
     (loop heartbeat → signals/panic.loop_permanently_dead.*)
```

Each loop script is self-describing via a one-line header:

```bash
# SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_FOO_EVERY order=30
```

and must support `script.sh --report <results_dir>` to emit its own report
section. **Adding a monitor/workload = adding one file** — the supervisor
auto-discovers it and the report picks it up, with no edits to any existing
code. To temporarily disable a loop: `SOAK_DISABLE_LOOPS="name1 name2"`;
to re-tune a single loop's cadence use its header-declared `interval_var`
(0 = off).

### 3.6 Judge module (the verdict layer)

**Directory**: `judge/` — a standalone business-verdict engine, separate from monitor (collection).

```
judge/
├── judged.sh                     daemon: streaming verdict loop + --report
└── verdicts/                     one verdict rule per file (8, all migrated)
    ├── view_correctness.sh       MISMATCH aggregation + coverage + incomplete
    ├── refresh_perf.sh           refresh p95 vs schedule
    ├── compress_perf.sh          compress p95 vs schedule
    ├── perf_probe.sh             view latency + speedup + plan drift
    ├── watermark_lag.sh          watermark lag vs tolerance
    ├── invalidation_log.sh       L1/L2 bloat + drain
    ├── system_metrics.sh         disk-peak soft alarm + RSS-leak trend
    └── bgw_scheduler_health.sh   BGW dispatch delay
```

**Three-layer split**:

| Layer | Responsibility |
|---|---|
| **monitor/** | **Pure collection** — sample DB state, append CSV. A migrated dimension's `--report` is a no-op (no longer judges). Exception: `system_metrics` keeps the `panic.disk_full` in-line early stop (a framework-liveness check, not a business verdict). |
| **judge/** | **All business verdicts** — read the monitor CSVs (+ `chaos_log.csv`), apply `verdicts/*.sh`, raise `fail.*` / `chaos.*`. Two verdict modes: **streaming** (the daemon re-evaluates every `SOAK_JUDGE_INTERVAL` seconds, so a threshold breach is caught on the spot rather than at wrap-up) + **report** (`--report` prints the verdict summary). |
| **report.sh** | **Unchanged glue** — it already calls each SOAK-LOOP script's `--report`; judge contributes verdict sections, the (now no-op) monitors no longer contribute sections. |

**Verdict contract** — each `judge/verdicts/<name>.sh` defines:
```
judge_run_<name>    <results_dir> <state_dir>   streaming tick (called every judged cycle)
judge_report_<name> <results_dir>               print the report section (called by judged --report)
```
The verdict logic is factored into a shared `_judge_eval_<name>` function that both entry points call: the streaming one touches the flag (early detection), the report one touches the flag + prints (wrap-up guarantee). Flag-touch is idempotent, so repeated calls are safe.

`SOAK_JUDGE_ENABLED=1` (on by default). **All 8 business verdicts live in the judge layer**; every monitor's `--report` is a no-op — monitors are **pure collectors**. Every verdict section in report.txt comes from `── judge (business verdicts) ──`; monitors emit no verdict section.

**Two things deliberately kept in the collectors (real-time, cannot be judged after the fact)**:
- `monitor/system_metrics.sh`'s `panic.disk_full` early stop at collection time (must fire within seconds to prevent a full disk)
- `monitor/view_correctness_scrape.sh`'s MISMATCH on-the-spot snapshot + `fail.view_mismatch` (the live state is overwritten seconds later, so it must be captured on the spot) + `panic.env_lost` / `panic.view_check_dead`

These are **framework liveness / crime-scene forensics**, not business verdicts that can be recomputed later, so they stay on the collection side. The judge's `view_correctness` verdict only does MISMATCH count aggregation + coverage WARN + incomplete escalation.

---

## §4 Correctness verification logic

Correctness verification has **one auto-fail signal**: `monitor/view_correctness.sql` — an end-to-end check of the `view = source` invariant. If it fails, the driver immediately dumps the live state for post-mortem diagnosis.

| Script | Verifies | Frequency | Auto-fail? | Role |
|---|---|---|---|---|
| `monitor/view_correctness.sql` | **view = source** end-to-end equivalence | every 30 min (preset default) | ✅ any MISMATCH → exit 2 | **The sole auto-fail signal** |
| `view_mismatch_*.snapshot.txt` | dump live L1/L2/watermark when MISMATCH fires | on-trigger | — | diagnostic, read after the fact |

### 4.1 view_correctness — the sole auto-fail signal

#### 4.1.1 Core principle

A CAGG view expands on the catalog like this:

```
cv_1hour  ≡  (SELECT * FROM mat_1hour WHERE bucket <  watermark)
             UNION ALL
             (SELECT * FROM direct_view_1hour WHERE bucket >= watermark)
                                                  ↑
                                       live branch: aggregate cpu in real time
```

At any moment and any refresh progress, the row set the view returns should **equal a full aggregation done directly against the source table**. A slow refresh just means the live branch carries a bit more and the mat branch a bit less; the total is unchanged.

→ `view = source` is a **structural invariant**; a violation of the equivalence must be a real bug:
- the view's UNION ALL filter is wrong (`<` written as `<=`)
- the live branch uses a different aggregation algorithm than mat
- watermark boundary off-by-one
- the mat materialized value is computed wrong

#### 4.1.2 Implementation — single transaction + REPEATABLE READ

On each call to `view_correctness.sql`, the driver runs all 4 CAGG comparisons **inside one REPEATABLE READ transaction**.

```sql
SET optimizer = off;
SET timezone = 'UTC';
SET statement_timeout = '3min';

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- 4 comparisons, sharing one MVCC snapshot
SELECT ... FROM <view query> ...;
SELECT ... FROM <source aggregation> ...;
-- ↑ these two queries see a 100% identical row set of cpu

COMMIT;
```

#### 4.1.3 Comparison method — per-(bucket, tags_id) FULL OUTER JOIN

For each CAGG, we don't just compare the aggregate totals — we **compare every mat column value per (bucket, tags_id)**:

```sql
WITH src AS (
  SELECT time_bucket(W, time) AS bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu
   WHERE time_bucket(W, time) >= w_start AND time_bucket(W, time) < w_end
   GROUP BY 1, 2
),
v AS (
  SELECT bucket, tags_id, cnt, avg_user, max_system
    FROM cv_1min
   WHERE bucket >= w_start AND bucket < w_end
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL OR v.bucket IS NULL    -- a row missing on either side
              OR src.cnt    != v.cnt                       -- count mismatch
              OR abs(src.avg_user - v.avg_user) > 1e-4     -- float tolerance
              OR src.max_system != v.max_system            -- integer exact
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now(), current_database(), 'cv_1min', '30min',
       total_rows, mismatch_count,
       CASE WHEN mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;
```

The FULL OUTER JOIN guarantees any "missing row / extra row / value mismatch" on either side is caught by mismatch_count. **MISMATCH=0 iff view and source are row-for-row identical over that window.**

#### 4.1.4 Driver escalation logic

```
# collector monitor/view_correctness_scrape.sh (pure collection + delayed re-check):
if verdict == 'MISMATCH':
    record differing buckets → view_mismatch_buckets.csv (ts,db,cagg,window,bucket)
    capture a live snapshot (watermark + L1/L2 + differing rows)
# once a differing bucket is >= SOAK_VIEW_RECHECK_SETTLE_SEC seconds past first sighting
# and has no terminal outcome yet, re-query THAT bucket on its own:
for bucket first-seen past settle and not yet terminal:
    re-run the same decidable + comparison for THAT single bucket
    → view_mismatch_recheck.csv (ts,db,cagg,window,bucket,outcome)
       outcome = still_mismatch (decidable and still diverges)
               / resolved       (decidable and now equal)
               / pending         (settle not elapsed or not decidable)

# judge/verdicts/view_correctness.sh (verdict from the re-check outcomes):
CONFIRMED = a bucket with >= 1 still_mismatch  → real divergence → fail.view_mismatch (exit 2)
RESOLVED  = re-query now equal, never still_mismatch → transient, not failed
PENDING   = seen but no terminal outcome yet (settle not elapsed / not decidable) → not judged this run
```

**confirm-before-flag = targeted delayed re-check** (the verdict lives in judge; the collector handles collection + the delayed re-check): the decidable-region check (excludes buckets with pending L1/L2) filters out most eventual-consistency noise, but a late-written row can become visible **a few seconds before** its L1/L2 invalidation entry (a watermark-boundary race; cold-start hit 1/336 on 2026-07-09), so a **single** MISMATCH can still be a transient. The collector therefore only **records the differing bucket + captures the scene**, and **re-queries that one bucket after `SOAK_VIEW_RECHECK_SETTLE_SEC` (default 180s)**: still diverging and decidable → real divergence → `fail.view_mismatch`; self-healed within the settle delay → transient, not failed.
> Why not "recur across cycles": the mat check window slides with the watermark, so at a coarse sampling interval a persistently-diverging bucket may be observed in only **one** cycle (the window then slides past it) — "recurs across ≥N cycles" then never fires. A per-bucket delayed re-check is **decoupled** from whether the window re-covers it: it re-checks **that exact bucket** wherever the window now sits; a late-write transient self-heals within the settle delay → not failed.

### 4.2 On-trigger dump

When `view_correctness` MISMATCH fires, the driver immediately dumps:
- `time_series.cagg_invalidation_log` (L1), full
- `time_series.cagg_materialization_log` (L2), full
- `time_series.cagg_watermark` (per-segment), full

to `view_mismatch_<db>_<cagg>_<HHMMSS>.snapshot.txt`, so you can 100% reconstruct after the fact "why the CAGG view returned a wrong value at that moment". This is the **only** L1/L2/watermark sampling path (no routine periodic snapshot).

### 4.3 Coverage matrix

| Bug type | Caught by view_correctness? | Note |
|---|---|---|
| trigger misses a write / refresh drops a bucket | ✅ | the view's live branch recomputes → exposes the diff |
| refresh leaves stale rows | ✅ | |
| `partial_view` expression mis-wired (a column gets another column's value) | ✅ | |
| refresh SPI writes columns in the wrong order | ✅ | |
| inter-segment motion crosses columns | ✅ | |
| BGW worker materializes with the wrong timezone (bucket-boundary drift) | ✅ | view + source both force UTC |
| the view's UNION ALL filter is wrong (`<` vs `<=`) | ✅ | |
| the live branch's aggregation differs from mat | ✅ | |
| watermark-boundary off-by-one makes the view miss/duplicate rows | ✅ | |
| a compound bug of watermark misplacement + wrong mat value | ✅ | both bugs still stack visibly on the view side |
| invalidation path for source-table UPDATE/DELETE | ❌ | TSBS is append-only so it doesn't fire; covered by regression |
| a bug in `time_bucket()` itself | ❌ | both sides call it = tautology; covered by regression unit tests |
| symmetric errors canceling out (+X / -X row-level cancel) | ❌ | extremely rare, not engineered against |
| NaN / Inf value contamination | ❌ | TSBS doesn't produce it; left as a known limitation |

> **Conclusion**: `view_correctness` is the **sole correctness auto-fail signal for v1 GA**. It covers every bug at the view-construction layer + the mat-materialization layer. Intermediate state (mat lag, L1/L2 size, watermark monotonicity) is captured only via the dump when MISMATCH fires, not sampled routinely.

---

## §5 Performance acceptance (view query latency)

### 5.1 Motivation

`view_correctness` (§4) only checks data equivalence (view = source); it **does not look at response time** — the view's live branch can regress into a full source-table scan + GROUP BY (a user query on `cv_1hour` takes 8–25 s), and yet `view_correctness` and `bgw_job_stat` are all green and **completely blind** to it. §5 periodically queries the view from the user's point of view, records the response-time distribution, and specifically catches this kind of scan-path regression.

### 5.2 Sampling points

Every `SOAK_PERF_PROBE_EVERY` seconds (default 300s), **5 queries** are each timed independently:

| probe label | query | scenario |
|---|---|---|
| `cv_1min_1h`           | `count(*) FROM cv_1min  WHERE bucket >= now()-'1h'`           | BI point query (smallest window) |
| `cv_1hour_24h`         | `count(*) FROM cv_1hour WHERE bucket >= now()-'24h'`          | ops report (scan path) |
| `source_24h`           | live `time_bucket + GROUP BY` on `cpu`, 24h window            | control: real-time source aggregation |
| `cv_1hour_24h_fetch`   | `SELECT bucket, 6 cols FROM cv_1hour ORDER BY ... LIMIT 1000` | **a real BI dashboard fetching data** |

The first 4 are all `count(*)` — mainly to catch **scan-path regression** (the view degenerating into a source-table scan). The 5th, `cv_1hour_24h_fetch`, is closest to real user feel: a BI widget fetching the last 24h to plot 1000 points.

`source_24h` is the **control group**: the same 24h window aggregated equivalently against the source. The ratio of `cv_1hour_24h` to it is the CAGG's measured speedup; when the ratio is **< 1**, the view has degenerated to a source scan and is slower than querying the source directly.

### 5.2.1 Threshold rule

| Metric | Threshold rule | verdict |
|---|---|---|
| `cv_1hour_24h` vs `source_24h` ratio | < 1.0 | 🔴 view slower than source, **always alarms** |
| `cv_1hour_24h` vs `source_24h` ratio | (1, 2.0) | ⚠ CAGG speedup on the low side |
| `plan_sig` (md5 of normalized plan) | identical across cycles | ✓ |
| `plan_sig` changes mid-run and last-Q exec ≥ 1.5× first-Q | self-relative slowdown | ⚠ PLAN DRIFT + EXCESS SLOWDOWN |

**No absolute baseline is maintained**: perf numbers vary with host / preset / scale, so a shared CSV misleads more than it helps. For run-to-run perf comparison, manually diff the `perf_probe.csv` of two results directories.

### 5.3 Implementation: EXPLAIN ANALYZE + plan signature

Run by `monitor/perf_probe.sh`, using `EXPLAIN (ANALYZE, TIMING ON, COSTS OFF, BUFFERS, FORMAT TEXT) <query>` to take the **pure server-side execution time** from PG's `Execution Time:` field (excluding psql startup / network / fetch), and picking up the full plan along the way to compute `plan_sig` for detecting plan regression.

**CSV format**:

```csv
ts,db,probe,exec_ms,plan_ms,plan_sig
2026-05-20 16:05:00,soak_test_a,cv_1min_1h,2.34,0.41,a3f9b2c107e5
2026-05-20 16:05:00,soak_test_a,cv_1hour_24h,12.7,0.52,b1c8e2f4d901
```

- `exec_ms`: server-side execution time
- `plan_ms`: planning time
- `plan_sig`: md5 prefix of the normalized EXPLAIN output (cost/rows/timing all stripped, only the shape kept)

**The first time each (db, probe) appears, the driver dumps the full plan text to `plan_snapshots/${db}_${probe}_init.txt`** as the baseline plan. If plan_sig changes on a later cycle, you can diff the baseline plan against the current plan to see exactly what changed.

### 5.4 Boundary: what §5 does NOT cover

| Not covered | Why not / alternative |
|---|---|
| **Write-path latency** (slow INSERT, chunk-switch jitter) | §5 measures the read path only. A sustained slow INSERT surfaces naturally in the TSBS loader stderr in `workload_*.log` |
| **Instantaneous spikes** (a BGW refresh jittering the view for a few seconds) | a 5-min sampling interval can't catch a few-second spike. For real-time transients check the same-window `system_metrics.csv` (BGW RSS / disk_pct) |
| **Cold-start latency** (mat-table cold cache) | over a 7-day soak the cache is long warmed. Cold-start testing is the domain of a capacity / boot benchmark |
| **Concurrent-view-query scalability** (10 users querying at once) | that's a concurrency benchmark, not stability. Use pgbench for that |

### 5.5 Disable sampling

```bash
SOAK_PERF_PROBE_EVERY=0 bash soak.sh ...
```

With it off, the §5 latency/speedup verdicts can't be applied (perf_probe.csv won't exist). The §5.6 / §5.7 refresh / compress scrapes stop with it too.

### 5.6 Refresh performance (BGW write path, passive scrape)

§5.1–§5.5 all measure the **read path** (a user actively querying the view). But the `refresh_continuous_aggregate` run by the BGW worker is the truly most expensive work:

| Operation | Typical duration | Frequency (large profile, 7d) |
|---|---|---|
| view count(*) | 1-50 ms | 5 min × N (2016 runs) |
| **refresh cv_1min**  | 50-500 ms | 1 min × 3 CAGGs × 2 DBs (10080 runs) |
| **refresh cv_5min**  | 0.1-2 s   | 5 min  (2016 runs) |
| **refresh cv_1hour** | 0.2-5 s   | 1 hour (168 runs) |

#### 5.6.1 Data source — passive scrape

Rather than actively probing (which would add cluster load), we periodically **scrape** `time_series.job_history`:

```sql
SELECT id, job_id, execution_start, duration, succeeded, is_crashed, error_data
  FROM time_series.job_history
 WHERE proc_name = 'policy_refresh' AND id > $cursor
 ORDER BY id;
```

Each scrape records the max(id) seen in a `.refresh_cursor_$DB` file to avoid duplicates. The scrape itself is < 10 ms.

#### 5.6.2 Sampling interval — 5 min

In sync with the view probe. Rationale:
- the scrape is dirt cheap (a single SELECT of a few rows, < 10 KB), so a 5-min interval doesn't spam
- 5 min catches the 1-min-schedule cv_1min refresh (5 samples/cycle, enough for a p95)
- because it's an incremental scrape, nothing is missed — even if the scrape interval is longer than the refresh interval, bgw_job_stat_history doesn't lose history

#### 5.6.3 Threshold rule

Thresholds are **relative to schedule_interval**, not absolute. Key insight: if a refresh duration > schedule_interval, the scheduler must kill the previous still-running worker (scheduler-driven termination).

| Metric | Rule | Verdict |
|---|---|---|
| refresh p95 (per CAGG) | < schedule_interval | ✓ OK |
| refresh p95 (per CAGG) | [schedule_interval, 2 × schedule_interval] | ⚠ WARN (near the kill boundary) |
| refresh p95 (per CAGG) | > 2 × schedule_interval | 🔴 **CRITICAL → exit +4** |

per-CAGG schedule_interval (from setup/03_caggs.sql):

| CAGG | schedule_interval | p95 WARN threshold | p95 CRITICAL threshold |
|---|---|---|---|
| cv_1min  | 60 s    | > 60 s    | > 120 s |
| cv_5min  | 300 s   | > 300 s   | > 600 s |
| cv_1hour | 3600 s  | > 3600 s  | > 7200 s |

#### 5.6.4 Output: refresh_durations.csv

```csv
ts,db,id,job_id,cagg_name,execution_start,duration_ms,succeeded,is_crashed,error
2026-05-25T16:05:00Z,soak_test_a,42,7,cv_1hour,2026-05-25T16:00:00Z,1234,true,false,
2026-05-25T16:05:00Z,soak_test_a,43,5,cv_1min, 2026-05-25T16:04:00Z,87,true,false,
```

#### 5.6.5 report.txt example

```
── refresh perf (from bgw_job_stat_history) ──
  soak_test_a/cv_1min   n=10080 p50=23.0   p95=120.0  p99=380.0  max=950.0  fail=0 crash=0 kills=12  ✓ OK
  soak_test_a/cv_5min   n=2016  p50=45.0   p95=210.0  p99=560.0  max=1200.0 fail=0 crash=0 kills=0   ✓ OK
  soak_test_a/cv_1hour  n=1008  p50=180.0  p95=580.0  p99=1400.0 max=3200.0 fail=0 crash=0 kills=0   ✓ OK
  ...
```

The `kills` column: the number of times a refresh duration > schedule_interval (scheduler-driven termination). A few kills (< 1% of n) is normal; many kills means the BGW is already saturated.

### 5.7 Compression performance (BGW write path)

Same pattern as §5.6, scraping rows with `proc_name='policy_compression'`. The CSV has two extra columns (chunks_compressed, bytes_reclaimed):

```csv
ts,db,id,job_id,table_name,execution_start,duration_ms,succeeded,is_crashed,chunks_compressed,bytes_reclaimed,error
2026-05-25T17:00:00Z,soak_test_a,50,12,cpu,...,5400,true,false,3,12500000,
```

#### 5.7.1 Thresholds

- schedule_interval comes from the preset's SOAK_COMPRESS_SCHEDULE (smoke: 1min, recommended: 10min) — see §2
- p95 < schedule → ✓ OK; p95 in (schedule, 2×): ⚠ WARN; p95 > 2× → 🔴 CRITICAL (+4)

#### 5.7.2 report.txt example

```
── compress perf (from bgw_job_stat_history) ──
  soak_test_a n=28 p50=4500.0 p95=18000.0 p99=42000.0 max=68000.0 fail=0 crash=0 chunks=245 reclaimed=14336.0MB  ✓ OK
  soak_test_b n=28 p50=4400.0 p95=17500.0 p99=41200.0 max=66000.0 fail=0 crash=0 chunks=240 reclaimed=14150.0MB  ✓ OK
```

The `chunks` / `reclaimed` columns accumulate how many chunks were compressed and how much disk was reclaimed over this soak. This is one **functional regression test** for v1 GA: if chunks=0 at the end, the compression policy never fired, which is most likely a bug.

---

## §6 Chaos injection

### 6.1 What chaos is — only real production faults

Chaos injects only **faults that really happen in production and are not artificial** (a process killed by the OS, a cluster crash, resource exhaustion), verifying the system survives and recovers. It does **not** test the precisely-instrumented `gp_inject_fault` races in the code — that's the job of **deterministic regression** (iso2/regress). A fault runs on exactly one track: a code fault point is the language of regression, not of chaos.

### 6.2 Fault catalog (implemented)

`chaos/fault_catalog.csv`, 4 entries (columns: `fault_name,level,target,mechanism,signal,active_s,recovery_budget_s,weight`):

| Fault | Mechanism | Cluster | active_s/budget/weight |
|---|---|---|---|
| refresh_worker_kill | pkill -9 the BGW currently running a refresh (mid-refresh crash recovery) | up | 20 / 120 / 3 |
| compress_worker_kill | pkill -9 the BGW currently running a compress (mid-compress crash) | up | 20 / 120 / 3 |
| scheduler_kill | pg_terminate the BGW scheduler (launcher respawns it) | up | 20 / 60 / 2 |
| cluster_crash | pkill -9 all postgres → gpstart (the crash-consistency showpiece) | **down→recover** | 10 / 180 / 1 |

- All faults share `level=1` — the `level` column is retained for future tiering but no longer filters (see §6.3).
- `active_s`: for worker kills this is the max wait for **poll-then-kill** to catch an in-flight worker (fixing the old "8% lottery" hit rate); it also feeds the exclusion-window computation (§6.4).
- `recovery_budget_s`: the recovery time allowed after reset; exceeding it is reported in the chaos_recovery section of report.txt but **does not raise a flag** (chaos_log.csv already carries every recovery_s value for anyone who wants to trend it); it **also defines the chaos exclusion-window width**.
- The several `gp_inject_fault` error/panic instrumentation entries originally mixed into the catalog have been moved out into iso2 regression (backlog; `recompress_reader_double_count.sql` already exists).

### 6.3 SOAK_CHAOS_LEVEL (binary switch: 0 / 1)

| Value | Selects | Cluster | Use |
|---|---|---|---|
| 0 (default) | none, chaos_loop exits immediately | — | plain soak / baseline |
| 1 | all 4 faults from `fault_catalog.csv` | up during worker kills; briefly down during cluster_crash | regular chaos + crash-consistency in one run |

Frequency is controlled independently by `SOAK_CHAOS_AVG_INTERVAL` (default 900s). For a focused smoke, use `SOAK_CHAOS_ONLY=<name>[,...]` to run only the named faults (e.g. `SOAK_CHAOS_ONLY=cluster_crash` to hammer the recovery path repeatedly, or `SOAK_CHAOS_ONLY=refresh_worker_kill,compress_worker_kill,scheduler_kill` to skip cluster_crash).

### 6.4 Inject + recover cycle

Each round: **weighted pick → inject (log INJECT) → recover → measure_recovery (log RECOVERY_TIME) → pause (to make up AVG_INTERVAL)**.

- **worker kill**: self-heals, the BGW is respawned by the scheduler, no intervention needed.
- **cluster crash → `recover_cluster`**: after a hard kill a bare `gpstart` always fails, so you must do the cleanup "a restart would do automatically but this doesn't" — `pkill -9 postgres` (clear leftover / half-started coordinator) → delete `postmaster.pid` → delete `/tmp/.s.PGSQL.*` (stale socket, else gpstart reports "instance process running") → `ipcrm` this user's leftover SysV shm/sem (else it reports "pre-existing shared memory block still in use") → delete `pgsql_tmp` (leftover spill) → `gpstart`, **judging success by a live `psql SELECT 1` rather than the gpstart exit code**, retrying `SOAK_CHAOS_GPSTART_TRIES` (3) times. Zombies left by a hard kill inside a container not being reaped by PID1 is normal and does not block recovery (they hold no port/memory/IPC); for long runs use `docker run --init`.

### 6.5 Chaos-aware judging (judge layer)

All judging lives in `judge/` (consistent with the §3.6 decoupling). The core is the **exclusion window**: threshold breaches / incomplete samples inside a fault window **do not count as fail** — they measure exactly the injected perturbation; only sustained breaches outside the window become `fail.*`.

- **Exclusion window = [INJECT, INJECT + recovery_budget]**. `chaos_loop` stamps the RECOVERY_TIME row's **timestamp directly as `inject + max(active_s + grace, recovery_budget)`** (not actually waiting that long), and `judge/lib/chaos.sh::chaos_windows_str` produces the windows from it (ISO8601 string comparison, no epoch needed; an unclosed INJECT gets an open window appended, to keep streaming judgment from false-positiving). `SOAK_CHAOS_WINDOW_GRACE` (5s) covers the post-kill transient.
- **Per-verdict fault whitelist** (refactored 2026-07-21). `chaos_windows_str` takes a second arg — a comma-separated list of fault_names that CAN realistically pollute the calling verdict's metric.  Faults outside that list are invisible to the verdict, so an unrelated chaos (e.g. `compress_worker_kill` during a `refresh_perf` sample) doesn't over-shield and let a real bug slide.  Current matrix:

  | Verdict | Whitelisted faults |
  |---|---|
  | `refresh_perf` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `compress_perf` | compress_worker_kill, scheduler_kill, cluster_crash |
  | `bgw_scheduler_health` | scheduler_kill, cluster_crash |
  | `watermark_lag` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `invalidation_log` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `view_correctness` / `framework_health` (view_check_dead) | cluster_crash |
  | `rss_leak` / `perf_probe` | (not chaos-aware — chaos correlation is weak) |

  `chaos_recovery.sh`: recovery_s over budget or timeout is reported (report.txt section) but **not flagged** — a slow-but-successful recovery is not an SLO violation.  A recovery that actually fails (gpstart exit != 0) is caught upstream by chaos_loop → `signals/panic.chaos`.
- **MISMATCH is strictly = 0 at all times** (no chaos-window exemption): the decidable-region check + targeted delayed re-check (§4) filter out the eventual-consistency transient, so whatever still diverges on re-check to a fail is a real divergence — inherently safe, no extra chaos-awareness needed.

Flag namespaces are two-tier: `signals/panic.*` (early stop, framework) / `verdicts/fail.*` (SLO / business).  There is no advisory warn layer.

### 6.6 Validation status

Both chaos=1 and chaos=2 pass validation: worker kills self-heal within seconds, cluster crashes recover via `gpstart` (5–9s), crash-period incompletes are filtered by the exclusion window with no false early stop, 0 MISMATCH, clean run.

### 6.7 Explicitly not done

`gp_inject_fault` error/panic (goes to iso2) · network partition (needs netem) · clock drift (hard to inject reliably) · segment crash + gprecoverseg (mirror recovery is hard, backlog) · disk full (too risky) · manual data-file corruption (belongs to DR).
