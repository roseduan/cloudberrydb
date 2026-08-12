# mock_time — Virtual-Clock-Driven BGW Test Framework

## What It Is

Drives the BGW (Background Worker) scheduler against a **virtual clock**
instead of the real wall clock.  Each tick is advanced explicitly from
SQL; behaviour is fully deterministic, reproducible, and completes in
sub-second wall time.

Ported from TimescaleDB `test/src/bgw/` (Apache 2.0), with
SPI / snapshot / DSM adaptations for CloudberryDB.

## Why It Exists

Without a mocked clock there are only two ways to test the BGW
scheduler — and neither is good:

| Approach | Drawback |
|---|---|
| Real `sleep` + polling | Slow (seconds to minutes); wall-clock dependent so flaky on slow machines; `schedule_interval='5 minutes'` literally needs 5 minutes |
| Direct C unit tests | Misses the real SPI / transaction / locking behaviour we actually care about |

mock_time lets us **verify everything that happens during a virtual
60 seconds within ~1 wall second**, and lets us jump-cut "+1 day" or
"+1 year" to exercise fixed-schedule alignment, retry backoff, and
other time-dependent edges.

## Basic Usage

```sql
-- 1. Create the DSM-handle storage table (test setup phase)
CREATE TABLE public.bgw_dsm_handle_store(handle BIGINT) DISTRIBUTED REPLICATED;
INSERT INTO public.bgw_dsm_handle_store VALUES (0);

-- 2. Allocate a DSM segment and install the mock timer
SELECT time_series.bgw_params_create();

-- 3. Reset the virtual clock to a fixed origin (e.g. 2024-01-01 UTC)
SELECT time_series.bgw_params_reset_time(
    extract(epoch FROM '2024-01-01 00:00:00+00'::timestamptz)::bigint * 1000000,
    false);

-- 4. Configure "wait" to fast-forward the virtual clock immediately
--    rather than really sleeping
SELECT time_series.bgw_params_mock_wait_returns_immediately(1);

-- 5. Run the mock scheduler for 60 virtual seconds, then return
SELECT time_series.bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(60000);

-- 6. Inspect what the scheduler did (bgw_log carries virtual timestamps)
SELECT mock_time, application_name, msg FROM public.sorted_bgw_log;

-- 7. Tear down
SELECT time_series.bgw_params_destroy();
```

In practice this 60-virtual-second test runs in ~1 wall second.

## File Inventory

| File | Role |
|---|---|
| `params.{c,h}` | DSM shared memory: virtual clock, scheduler params, wakeup latch |
| `timer_mock.{c,h}` | Replaces the BGW `Timer` interface — reads the virtual clock and "fakes" wait |
| `scheduler_mock.c` | Runs the scheduler main loop synchronously in the caller's session (does not fork a worker) |
| `log.{c,h}` | `emit_log_hook` writes BGW logs into the `bgw_log` table tagged with the virtual timestamp |

## Implementation

### (1) Where the virtual clock lives

In a DSM (Dynamic Shared Memory) segment:

```c
typedef struct TestParams {
    int64       current_timestamp_us;            // Current virtual time (microseconds)
    int32       mock_wait_returns_immediately;   // Wait-mode policy
    int32       ttl_ms;                          // Scheduler lifetime
    Latch      *scheduler_latch;                 // Wakes the scheduler
} TestParams;
```

DSM is process-shared — both the coordinator session (issuing SQL) and
the BGW process (running the scheduler) attach to the same segment.
The single-row `bgw_dsm_handle_store` table holds the DSM handle so any
session can find the same segment.

### (2) How the scheduler is "fooled"

The BGW scheduler main loop touches time in only two places:

```c
// scheduler.c (production code)
now = timer_get_current_timestamp();   // via Timer abstraction
timer_wait(next_wakeup);                // via Timer abstraction
```

The `Timer` is a function table:

```c
typedef struct Timer {
    TimestampTz (*get_current_timestamp)(void);
    bool        (*wait)(TimestampTz until);
} Timer;
```

In production:
```c
const Timer standard_timer = {
    .get_current_timestamp = GetCurrentTimestamp,    // real wall clock
    .wait                  = wait_using_wait_latch,  // real sleep
};
```

In mock mode:
```c
const Timer mock_timer = {
    .get_current_timestamp = mock_get_current_timestamp,  // reads DSM
    .wait                  = mock_wait_immediate,         // fast-forwards virtual clock
};
```

`scheduler_mock.c` swaps the `current_timer` pointer to `&mock_timer`
before kicking off the scheduler loop.  **The scheduler code has no
idea time is fake** — that is the most elegant property of this design:
zero invasiveness, zero `#ifdef TESTING`.

### (3) How "wait" fast-forwards

`mock_wait_immediate` doesn't call `WaitLatch` — it just advances the
clock:

```c
static bool mock_wait_immediate(TimestampTz until) {
    params->current_timestamp_us = until_in_microseconds;  // jump to target time
    return true;
}
```

Right after the scheduler calls `timer_wait(t)`, it sees
`get_current_timestamp() == t`, decides the wait is over, and continues.
**Sixty seconds of virtual loop time turn into 60 instant fast-forwards;
wall time spent is whatever the real SPI/txn/lock work needs**, usually
a handful of milliseconds.

### (4) Single-step driving from SQL

`bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(ttl_ms)`:

1. Sets `params->ttl_ms = ttl_ms` (virtual milliseconds — the scheduler's
   `quit_time` is computed from this)
2. Calls `bgw_scheduler_process(ttl_ms, NULL)` directly —
   **synchronously, no fork**, in the caller session
3. The scheduler consumes virtual time up to `quit_time`, then returns
4. The SQL function returns; the next SQL statement runs

The caller's transaction and memory contexts are alive throughout, so
log hooks can write to `bgw_log` via SPI without any cross-process
synchronisation.

### (5) How logs align with virtual time

In `log.c`:

```c
void _PG_init_bgw_log() {
    prev_emit_log_hook = emit_log_hook;
    emit_log_hook      = bgw_log_emit;   // intercept every log message
}

static void bgw_log_emit(ErrorData *edata) {
    if (current_timer == &mock_timer) {
        SPI_execute(
            "INSERT INTO public.bgw_log(msg_no, mock_time, application_name, msg) "
            "VALUES ($1, $2, $3, $4)", ...);   // mock_time pulled from DSM
    }
    if (prev_emit_log_hook) prev_emit_log_hook(edata);   // chain through
}
```

The test SQL can then assert:

```sql
SELECT * FROM sorted_bgw_log
 WHERE mock_time = 1704067200000000  -- 2024-01-01 UTC microseconds
   AND application_name = 'DB Scheduler';
```

**Log order = virtual-time order = deterministic output.**  The regress
diff is never flaky because of machine speed.

## CBDB Adaptations

The four places we diverge from the TSDB upstream:

| Concern | TSDB upstream | Our adaptation |
|---|---|---|
| Reading `bgw_dsm_handle_store` | scanner framework (single-node) | **SPI dispatch** — a `DISTRIBUTED REPLICATED` table requires MPP |
| Snapshot before SPI calls | inherited from outer command | **explicit `Push/PopActiveSnapshot`** wrapper (otherwise "cannot execute SQL without an outer snapshot or portal") |
| DSM segment release | `dsm_pin_segment` | same (EXEC_BACKEND parity), `destroy` is a no-op but idempotent |
| Worker fork | postmaster bgworker API | **synchronous direct call** to `bgw_scheduler_process` — no fork in tests |

## SQL Surface (all in the `time_series.` schema)

| Function | Purpose |
|---|---|
| `bgw_params_create()` | Allocate the DSM segment, install the mock timer |
| `bgw_params_destroy()` | Tear-down (no-op but idempotent) |
| `bgw_params_reset_time(us, set_latch)` | Reset the virtual clock to a specific microsecond |
| `bgw_params_mock_wait_returns_immediately(mode)` | Wait-mode policy (1 = immediate-set-until) |
| `bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(ttl_ms)` | Run the scheduler synchronously to completion |
| `bgw_db_scheduler_test_run(ttl_ms)` | Start the scheduler asynchronously |
| `bgw_db_scheduler_test_wait_for_scheduler_finish()` | Wait for the asynchronous scheduler to exit |

## Reference Cases

`test/regress/sql/cagg_bgw_mock.sql` — five complete cases covering all
of the above:

- Scheduler exits cleanly when no jobs exist
- A CAGG refresh policy fires once and the log captures it
- `alter_job` changes `schedule_interval` and the next `next_start` is
  recomputed accordingly
- The `bgw_log` ordering is strictly monotonic in `mock_time`

## Licensing

- Upstream: `timescaledb/test/src/bgw/`, Apache License 2.0
- Our derivative changes (SPI replacing the scanner framework, snapshot
  push/pop, CBDB MPP path) are © HashData and Apache 2.0 compatible
- File-level attribution lives in each source file's header comment
