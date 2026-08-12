/*
 * launcher.c -- per-database scheduler discovery for time_series.
 *
 * Adapted from TimescaleDB src/loader/bgw_launcher.c (Apache 2.0).
 * Simplified for CBDB:
 *   - no message queue (no SQL-level START / STOP / RESTART control plane)
 *   - no pre-allocated worker counter (rely on
 *     RegisterDynamicBackgroundWorker to fail when max_worker_processes
 *     is exhausted; scheduler retries on next poll)
 *   - coordinator-only (segments skip registration entirely; segment-side
 *     scheduler processes have historically been a source of QE crashes)
 *
 * Lifecycle:
 *   shared_preload_libraries = 'time_series'
 *     -> _PG_init registers ONE static bgworker: this launcher
 *   launcher_main()
 *     -> connects to shared catalogs (no DB)
 *     -> every N ms (time_series.bgw_launcher_poll_time) walks
 *       pg_database and spawns a dynamic 'time_series scheduler'
 *       worker for each non-template, datallowconn=true DB
 *     -> scheduler_main itself checks pg_extension and exits cleanly
 *       if time_series is not installed in its target DB.  Launcher
 *       reaps the handle on next poll.
 *
 * Result: a scheduler exists in exactly those databases where a user has
 * run `CREATE EXTENSION time_series`, automatically tracked across
 * CREATE / DROP DATABASE and CREATE / DROP EXTENSION events.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 */
#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* ts_pax_register_per_backend_exit_capture */
#include "../include/compress/ts_compress.h"
#include "bgw_counter.h"
#include "bgw_message_queue.h"

#include "launcher.h"

#include "storage/procarray.h"
#include "tcop/tcopprot.h"

/* PG's GET_VXID_FROM_PGPROC for stamping the SQL caller's vxid into the
 * scheduler bgworker entry; the scheduler uses it to wait for the
 * caller's transaction to commit before doing real work. */
#include "storage/lock.h"


#ifdef GP_VERSION_NUM
#include "cdb/cdbvars.h"		/* Gp_role / GP_ROLE_DISPATCH */
#endif

#include "../include/time_series.h"

#define LAUNCHER_BGW_NAME    "time_series launcher"
#define SCHEDULER_BGW_NAME   "time_series scheduler"
#define SCHEDULER_FUNC_NAME  "bgw_scheduler_main"

/* Default poll interval (seconds) -- exposed via GUC below. */
#define LAUNCHER_DEFAULT_POLL_SEC 60

/* Configured at GUC define time in scheduler.c -- referenced as extern. */
extern int	guc_bgw_launcher_poll_time_ms;
extern bool guc_bgw_enabled;
extern int	guc_bgw_scheduler_restart_time_sec;

/*
 * Per-database scheduler lifecycle state.  Mirrors upstream
 * TimescaleDB src/loader/bgw_launcher.c::SchedulerState.
 *
 *   DISABLED  -- operator-stopped (via stop_background_workers()) or
 *               never enabled.  Launcher does NOT spawn until a START
 *               or RESTART message moves us to ENABLED.
 *   ENABLED   -- should be running, but no slot reserved in the
 *               bgw_counter yet.
 *   ALLOCATED -- bgw_counter slot reserved, RegisterDynamicBackgroundWorker
 *               not yet called.
 *   STARTED   -- scheduler is running (handle is valid).
 */
typedef enum SchedulerState
{
	BGW_DISABLED = 0,
	BGW_ENABLED,
	BGW_ALLOCATED,
	BGW_STARTED
}			SchedulerState;

typedef struct DbLauncherEntry
{
	Oid			dboid;			/* hash key -- first field */
	BackgroundWorkerHandle *handle;
	SchedulerState state;
	VirtualTransactionId vxid;
	int			state_transition_failures;

	/*
	 * Respawn-storm brake.  A scheduler whose database is permanently
	 * broken (half-dropped catalog row with the base subdirectory gone,
	 * corrupt files, ...) dies in InitPostgres within milliseconds of
	 * every start.  Without a brake the STARTED -> ENABLED auto-respawn
	 * relaunches it on every launcher wakeup: observed live as ~80
	 * connection attempts/second and 79k FATALs in one day against one
	 * half-dropped database, with periodic launcher exits as collateral.
	 * Track consecutive short-lived schedulers and delay the next
	 * respawn with exponential backoff (1s doubling to a 60s cap).  A
	 * scheduler that survives past the threshold resets the brake, and
	 * an explicit START/RESTART message also clears it (operator intent
	 * beats the brake).
	 */
	TimestampTz last_start_at;	/* when the current scheduler was spawned */
	TimestampTz next_retry_at;	/* no auto-respawn before this (0 = now) */
	int			short_death_streak; /* consecutive sub-threshold lifetimes */
}			DbLauncherEntry;

/* Lifetime below which a scheduler death counts as "instant" (ms). */
#define SCHEDULER_SHORT_LIFE_MS		5000
/* Backoff cap between respawn attempts of an instant-dying scheduler. */
#define SCHEDULER_BACKOFF_CAP_MS	60000

/* Forward declarations for state-machine transitions used by
 * populate_database_htab() before they are defined later in the file. */
static void scheduler_state_trans_disabled_to_enabled(DbLauncherEntry * entry);
static void scheduler_state_trans_enabled_to_disabled(DbLauncherEntry * entry);
static void
			scheduler_state_trans_allocated_to_disabled(DbLauncherEntry * entry);
static void scheduler_state_trans_started_to_disabled(DbLauncherEntry * entry);
static void scheduler_state_trans_started_to_enabled(DbLauncherEntry * entry);

static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

static void
launcher_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
launcher_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * Build / refresh the database hash table from pg_database.
 *
 * Entries are created for every non-template, datallowconn DB.
 * Entries for DBs no longer present in pg_database (e.g. DROP
 * DATABASE) are removed; their scheduler dies naturally when the
 * postmaster reaps the now-orphaned worker on next polling tick.
 */
static void
populate_database_htab(HTAB *db_htab)
{
	HASH_SEQ_STATUS seq;
	DbLauncherEntry *entry;
	List	   *seen = NIL;
	ListCell   *lc;
	int			ret;
	uint64		i;
	MemoryContext oldcxt;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if ((ret = SPI_connect()) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(ret));

	/*
	 * Skip non-connectable DBs (e.g. template0 via datallowconn=false) AND
	 * skip template databases (template1, plus any user-created `IS_TEMPLATE
	 * true` databases).  Polling templates every cycle just costs a
	 * postmaster fork + extension check + immediate self- exit -- and under
	 * high test churn that postmaster-fork queue contention starves other
	 * DBs' job dispatch by hundreds of ms per poll.
	 *
	 * Special case: CBDB demo clusters mark the user-visible `postgres`
	 * database as datistemplate=true even though it is meant to host
	 * extensions.  Whitelist that name so we still launch a scheduler there.
	 * This mirrors TimescaleDB's upstream behavior of skipping datistemplate
	 * but predates the CBDB demo quirk (vanilla PG's `postgres` is not a
	 * template), so we diverge by one row.
	 */
	ret = SPI_execute("SELECT oid FROM pg_database "
					  "WHERE datallowconn "
					  "  AND (NOT datistemplate OR datname = 'postgres')",
					  true /* read_only */ , 0 /* count */ );
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		bool		isnull;
		Datum		datum = SPI_getbinval(tup, SPI_tuptable->tupdesc, 1, &isnull);
		Oid			dboid;
		bool		found;

		if (isnull)
			continue;
		dboid = DatumGetObjectId(datum);

		entry = (DbLauncherEntry *) hash_search(db_htab, &dboid,
												HASH_ENTER, &found);
		if (!found)
		{
			entry->handle = NULL;
			entry->state = BGW_DISABLED;
			SetInvalidVirtualTransactionId(entry->vxid);
			entry->state_transition_failures = 0;
			entry->last_start_at = 0;
			entry->next_retry_at = 0;
			entry->short_death_streak = 0;
			scheduler_state_trans_disabled_to_enabled(entry);
		}

		/*
		 * Existing entries are left alone here.  Their lifecycle is driven
		 * by scheduler_state_trans_automatic() and the message handlers:
		 *   - a scheduler that died unexpectedly is auto-respawned via
		 *     STARTED -> ENABLED in scheduler_state_trans_automatic();
		 *   - an entry stays DISABLED only after an explicit stop message
		 *     (message_stop_action) and re-enables only on an explicit
		 *     start/restart message.
		 */
		seen = lappend_oid(seen, dboid);
	}
	MemoryContextSwitchTo(oldcxt);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	/*
	 * Reap entries for DBs that disappeared (e.g. DROP DATABASE). If a
	 * scheduler was running for the dropped DB, the postmaster will tear it
	 * down on its own; we just drop our handle so the map stays consistent.
	 */
	hash_seq_init(&seq, db_htab);
	while ((entry = (DbLauncherEntry *) hash_seq_search(&seq)) != NULL)
	{
		bool		still_present = false;

		foreach(lc, seen)
		{
			if (lfirst_oid(lc) == entry->dboid)
			{
				still_present = true;
				break;
			}
		}
		if (!still_present)
		{
			/*
			 * DB was dropped.  Tear the entry down through the state machine
			 * so the counter accounting stays correct, then remove from the
			 * htab.
			 */
			switch (entry->state)
			{
				case BGW_STARTED:
					if (entry->handle != NULL)
						TerminateBackgroundWorker(entry->handle);
					scheduler_state_trans_started_to_disabled(entry);
					break;
				case BGW_ALLOCATED:
					scheduler_state_trans_allocated_to_disabled(entry);
					break;
				case BGW_ENABLED:
					scheduler_state_trans_enabled_to_disabled(entry);
					break;
				case BGW_DISABLED:
					break;
			}
			(void) hash_search(db_htab, &entry->dboid, HASH_REMOVE, NULL);
		}
	}
	list_free(seen);
}

/*
 * Adapted from upstream src/loader/bgw_launcher.c state-machine
 * transition functions.  Each transition is the only path between the
 * two indicated states; scheduler_state_trans_automatic() drives an
 * entry forward through DISABLED -> ENABLED -> ALLOCATED -> STARTED as
 * resources permit, and the message handlers below drive it backwards
 * to DISABLED on operator request.
 */
static void
scheduler_modify_state(DbLauncherEntry * entry, SchedulerState new_state)
{
	Assert(entry->state != new_state);
	entry->state_transition_failures = 0;
	entry->state = new_state;
}

static void
scheduler_state_trans_disabled_to_enabled(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_DISABLED);
	Assert(entry->handle == NULL);
	scheduler_modify_state(entry, BGW_ENABLED);
}

static void
scheduler_state_trans_enabled_to_allocated(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_ENABLED);
	Assert(entry->handle == NULL);

	/*
	 * Respawn-storm brake: an instant-dying scheduler set next_retry_at
	 * in started_to_enabled; stay ENABLED (harmless) until it elapses.
	 * A later poll retries, so no wakeup bookkeeping is needed.
	 */
	if (entry->next_retry_at != 0 &&
		GetCurrentTimestamp() < entry->next_retry_at)
		return;

	if (!ts_bgw_total_workers_increment())
	{
		if (entry->state_transition_failures == 0)
			elog(LOG, "time_series launcher: max_background_workers=%d exceeded",
				 guc_bgw_max_background_workers);
		entry->state_transition_failures++;
		return;
	}
	scheduler_modify_state(entry, BGW_ALLOCATED);
}

static void
scheduler_state_trans_allocated_to_started(DbLauncherEntry * entry)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *new_handle = NULL;
	bool		worker_registered;
	MemoryContext old_cxt;

	Assert(entry->state == BGW_ALLOCATED);
	Assert(entry->handle == NULL);

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = guc_bgw_scheduler_restart_time_sec;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", TS_EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "%s", SCHEDULER_FUNC_NAME);
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s [db %u]",
			 SCHEDULER_BGW_NAME, entry->dboid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "%s", SCHEDULER_BGW_NAME);
	worker.bgw_main_arg = ObjectIdGetDatum(entry->dboid);
	worker.bgw_notify_pid = MyProcPid;
	memcpy(worker.bgw_extra, &entry->vxid, sizeof(VirtualTransactionId));

	/*
	 * RegisterDynamicBackgroundWorker palloc's the handle in the current
	 * memory context.  We need it to outlive the current iteration's
	 * transient context (else next iter's GetBackgroundWorkerPid()
	 * dereferences freed memory), so allocate in TopMemoryContext.
	 */
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	worker_registered = RegisterDynamicBackgroundWorker(&worker, &new_handle);
	MemoryContextSwitchTo(old_cxt);

	if (!worker_registered)
	{
		if (entry->state_transition_failures == 0)
			elog(LOG, "time_series launcher: no postmaster bgworker slot for db %u",
				 entry->dboid);
		entry->state_transition_failures++;
		return;
	}

	entry->handle = new_handle;
	SetInvalidVirtualTransactionId(entry->vxid);
	entry->last_start_at = GetCurrentTimestamp();
	scheduler_modify_state(entry, BGW_STARTED);
	elog(DEBUG1, "time_series launcher: spawned scheduler for db %u",
		 entry->dboid);
}

static void
scheduler_state_trans_started_to_allocated(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_STARTED);
	if (entry->handle != NULL)
	{
		pfree(entry->handle);
		entry->handle = NULL;
	}
	scheduler_modify_state(entry, BGW_ALLOCATED);
}

static void
scheduler_state_trans_enabled_to_disabled(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_ENABLED);
	Assert(entry->handle == NULL);
	scheduler_modify_state(entry, BGW_DISABLED);
}

static void
scheduler_state_trans_allocated_to_disabled(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_ALLOCATED);
	Assert(entry->handle == NULL);
	ts_bgw_total_workers_decrement();
	scheduler_modify_state(entry, BGW_DISABLED);
}

static void
scheduler_state_trans_started_to_disabled(DbLauncherEntry * entry)
{
	Assert(entry->state == BGW_STARTED);
	if (entry->handle != NULL)
	{
		pfree(entry->handle);
		entry->handle = NULL;
	}

	/*
	 * Scheduler's own before_shmem_exit decremented the counter when it
	 * exited; the launcher must NOT double-decrement here.
	 */
	scheduler_modify_state(entry, BGW_DISABLED);
}

/*
 * Called from scheduler_state_trans_automatic() when a STARTED scheduler
 * is discovered dead (GetBackgroundWorkerPid == BGWH_STOPPED) without an
 * explicit stop message.  Transitions to ENABLED so that the next poll
 * of the launcher's main loop re-runs enabled_to_allocated ->
 * allocated_to_started and respawns the scheduler.
 *
 * Explicit stops (SELECT time_series.stop_background_workers()) go through
 * message_stop_action() which lands directly in DISABLED and stays there,
 * so this path is only entered on unexpected death (crash, SIGTERM, OOM).
 */
static void
scheduler_state_trans_started_to_enabled(DbLauncherEntry * entry)
{
	TimestampTz now = GetCurrentTimestamp();

	Assert(entry->state == BGW_STARTED);
	if (entry->handle != NULL)
	{
		pfree(entry->handle);
		entry->handle = NULL;
	}

	/*
	 * Respawn-storm brake (see the DbLauncherEntry comment): a scheduler
	 * that died within SCHEDULER_SHORT_LIFE_MS of starting is presumed
	 * to be failing deterministically (e.g. its database is half-dropped
	 * and every connection attempt FATALs in InitPostgres).  Back the
	 * next respawn off exponentially instead of hammering it on every
	 * launcher wakeup.  A scheduler that lived past the threshold resets
	 * the brake so ordinary crash-recovery keeps its fast respawn.
	 */
	if (entry->last_start_at != 0 &&
		!TimestampDifferenceExceeds(entry->last_start_at, now,
									SCHEDULER_SHORT_LIFE_MS))
	{
		int			backoff_ms;

		entry->short_death_streak++;
		backoff_ms = Min(1000 << Min(entry->short_death_streak - 1, 6),
						 SCHEDULER_BACKOFF_CAP_MS);
		entry->next_retry_at = TimestampTzPlusMilliseconds(now, backoff_ms);

		/*
		 * Milestone logging only: at the backoff cap the streak grows
		 * once a minute, and databases without the extension installed
		 * legitimately live in this state forever (their scheduler
		 * checks and exits) -- unconditional logging would emit one
		 * WARNING per broken/extension-less DB per minute.
		 */
		if (entry->short_death_streak == 3 ||
			entry->short_death_streak % 10 == 0)
			elog(WARNING, "time_series scheduler for database %u died %d"
				 " consecutive times within %d ms of starting;"
				 " delaying next respawn by %d ms",
				 entry->dboid, entry->short_death_streak,
				 SCHEDULER_SHORT_LIFE_MS, backoff_ms);
	}
	else
	{
		entry->short_death_streak = 0;
		entry->next_retry_at = 0;
	}

	/*
	 * Scheduler's own before_shmem_exit decremented the counter when it
	 * exited (mirrors started_to_disabled above); enabled_to_allocated on
	 * the next poll will re-increment.
	 */
	scheduler_modify_state(entry, BGW_ENABLED);
}

/*
 * Drive an entry forward through the normal lifecycle whenever a poll
 * tick or a message bump arrives.  STARTED entries are checked for
 * scheduler death and demoted to ALLOCATED-on-the-way-back-to-STARTED
 * via the message-handler path; here we just notice deaths.
 */
static void
scheduler_state_trans_automatic(DbLauncherEntry * entry)
{
	switch (entry->state)
	{
		case BGW_ENABLED:
			scheduler_state_trans_enabled_to_allocated(entry);
			if (entry->state == BGW_ALLOCATED)
				scheduler_state_trans_allocated_to_started(entry);
			break;
		case BGW_ALLOCATED:
			scheduler_state_trans_allocated_to_started(entry);
			break;
		case BGW_STARTED:
			if (entry->handle != NULL)
			{
				BgwHandleStatus s;
				pid_t		pid;	/* must NOT be NULL: PG dereferences
									 * unconditionally */

				s = GetBackgroundWorkerPid(entry->handle, &pid);
				if (s == BGWH_STOPPED)
				{
					/*
					 * Unexpected scheduler death (crash, SIGTERM, OOM):
					 * transit to ENABLED so the next poll respawns it.
					 * Explicit stops go through message_stop_action() ->
					 * BGW_DISABLED and never reach this branch.
					 */
					scheduler_state_trans_started_to_enabled(entry);
				}
			}
			break;
		case BGW_DISABLED:
			break;
	}
}

static void
scheduler_state_trans_automatic_all(HTAB *db_htab)
{
	HASH_SEQ_STATUS seq;
	DbLauncherEntry *entry;

	hash_seq_init(&seq, db_htab);
	while ((entry = (DbLauncherEntry *) hash_seq_search(&seq)) != NULL)
		scheduler_state_trans_automatic(entry);
}

/*
 * Message-queue handlers -- invoked from the launcher main loop in
 * response to STOP / START / RESTART messages sent by a SQL backend via
 * ts_bgw_message_send_and_wait().  Mirrors the upstream idempotency
 * guarantees: STOP is idempotent (DISABLED stays DISABLED), START is
 * idempotent (STARTED stays STARTED), RESTART always cycles.
 */
static DbLauncherEntry *
db_entry_get_or_create(HTAB *db_htab, Oid db_oid)
{
	DbLauncherEntry *entry;
	bool		found;

	entry = (DbLauncherEntry *) hash_search(db_htab, &db_oid, HASH_ENTER,
											&found);
	if (!found)
	{
		entry->handle = NULL;
		entry->state = BGW_DISABLED;
		SetInvalidVirtualTransactionId(entry->vxid);
		entry->state_transition_failures = 0;
		entry->last_start_at = 0;
		entry->next_retry_at = 0;
		entry->short_death_streak = 0;
	}
	return entry;
}

static bool
message_start_action(HTAB *db_htab, BgwMessage * message)
{
	DbLauncherEntry *entry = db_entry_get_or_create(db_htab, message->db_oid);

	/* Explicit operator START overrides the respawn-storm brake. */
	entry->next_retry_at = 0;
	entry->short_death_streak = 0;

	if (entry->state == BGW_DISABLED)
		scheduler_state_trans_disabled_to_enabled(entry);
	scheduler_state_trans_automatic(entry);
	return entry->state == BGW_STARTED;
}

static bool
message_stop_action(HTAB *db_htab, BgwMessage * message)
{
	DbLauncherEntry *entry = db_entry_get_or_create(db_htab, message->db_oid);

	switch (entry->state)
	{
		case BGW_ENABLED:
			scheduler_state_trans_enabled_to_disabled(entry);
			break;
		case BGW_ALLOCATED:
			scheduler_state_trans_allocated_to_disabled(entry);
			break;
		case BGW_STARTED:
			if (entry->handle != NULL)
			{
				TerminateBackgroundWorker(entry->handle);
				WaitForBackgroundWorkerShutdown(entry->handle);
			}
			scheduler_state_trans_started_to_disabled(entry);
			break;
		case BGW_DISABLED:
			break;
	}
	return entry->state == BGW_DISABLED;
}

static bool
message_restart_action(HTAB *db_htab, BgwMessage * message,
					   VirtualTransactionId vxid)
{
	DbLauncherEntry *entry = db_entry_get_or_create(db_htab, message->db_oid);

	entry->vxid = vxid;

	/*
	 * Explicit operator RESTART overrides the respawn-storm brake, same
	 * reason as START: extension install ends with
	 * restart_background_workers(), so a scheduler that had briefly died
	 * during the pre-install window (no extension yet -> checks and
	 * exits) must be able to come back immediately -- not sit out the
	 * exponential backoff.
	 */
	entry->next_retry_at = 0;
	entry->short_death_streak = 0;

	switch (entry->state)
	{
		case BGW_STARTED:
			if (entry->handle != NULL)
			{
				TerminateBackgroundWorker(entry->handle);
				WaitForBackgroundWorkerShutdown(entry->handle);
			}
			scheduler_state_trans_started_to_allocated(entry);
			break;
		case BGW_DISABLED:
			scheduler_state_trans_disabled_to_enabled(entry);
			break;
		case BGW_ENABLED:
		case BGW_ALLOCATED:
			break;
	}
	scheduler_state_trans_automatic(entry);
	return entry->state == BGW_STARTED;
}

/*
 * Pop and process one message.  Returns true if a message was handled,
 * false if the queue was empty.
 */
static bool
launcher_handle_message(HTAB *db_htab)
{
	BgwMessage *message = ts_bgw_message_receive();
	PGPROC	   *sender;
	VirtualTransactionId vxid;
	bool		ok = false;

	if (message == NULL)
		return false;

	sender = BackendPidGetProc(message->sender_pid);
	if (sender == NULL)
	{
		ereport(LOG,
				(errmsg("time_series launcher: message from non-existent backend pid %d",
						message->sender_pid)));
		ts_bgw_message_send_ack(message, false);
		return true;
	}

	GET_VXID_FROM_PGPROC(vxid, *sender);

	switch (message->message_type)
	{
		case BGW_MSG_START:
			ok = message_start_action(db_htab, message);
			break;
		case BGW_MSG_STOP:
			ok = message_stop_action(db_htab, message);
			break;
		case BGW_MSG_RESTART:
			ok = message_restart_action(db_htab, message, vxid);
			break;
	}

	ts_bgw_message_send_ack(message, ok);
	return true;
}

/*
 * Cleanup hook fired on launcher exit.  Tells postmaster we no longer
 * care about the dynamically-registered schedulers (they keep running
 * on their own; we just release our handles).
 */
static HTAB *cleanup_db_htab = NULL;

static void
launcher_pre_shmem_cleanup(int code, Datum arg)
{
	HASH_SEQ_STATUS seq;
	DbLauncherEntry *entry;

	/*
	 * Release our own bgworker slot.  If postmaster respawns us, the fresh
	 * launcher will reinit the counter anyway, but doing this keeps the
	 * counter sane in the rare case where the launcher exits permanently
	 * (e.g. operator gpstop, or bgw_enabled flipped off).
	 */
	ts_bgw_total_workers_decrement();

	if (cleanup_db_htab == NULL)
		goto done_htab;

	hash_seq_init(&seq, cleanup_db_htab);
	while ((entry = (DbLauncherEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->handle != NULL)
		{
			pfree(entry->handle);
			entry->handle = NULL;
		}
	}
	hash_destroy(cleanup_db_htab);
	cleanup_db_htab = NULL;

done_htab:

	/*
	 * Reset the message-queue reader pid so a fresh launcher (postmaster
	 * respawn) can claim the queue, and so any in-flight sender waiting on
	 * our ack knows we're gone.
	 */
	ts_bgw_message_queue_shmem_cleanup();
}

PG_FUNCTION_INFO_V1(bgw_launcher_main);

Datum
bgw_launcher_main(PG_FUNCTION_ARGS)
{
	HASHCTL		hashinfo;
	HTAB	   *db_htab;

	pqsignal(SIGHUP, launcher_sighup);
	pqsignal(SIGTERM, launcher_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * Forward the proc_exit() code through the PAX atexit hook.  See
	 * ts_compress_pax.cc -- _PG_init's on_proc_exit registration doesn't
	 * reach forked workers, so each bgworker must re-register the capture
	 * callback for postmaster to see the right exit status (e.g. proc_exit(1)
	 * on SIGTERM must surface as exit code 1 so postmaster respawns us,
	 * instead of the hard-coded 0 the old hook forced regardless of the
	 * worker's intent).
	 */
	ts_pax_register_per_backend_exit_capture();

	/*
	 * Connect to `postgres` rather than template1.  CBDB needs an actual DB
	 * context (not just shared catalogs) for transaction / snapshot
	 * machinery; BackgroundWorkerInitializeConnection(NULL, ...) crashes
	 * inside heap_getnext on a shared-catalog scan.  Connecting to template1
	 * works but blocks CREATE DATABASE (template1 must be idle to be cloned).
	 * `postgres` is always present on a healthy cluster and is the
	 * conventional administrative DB.
	 */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
	pgstat_report_appname(LAUNCHER_BGW_NAME);

#ifdef GP_VERSION_NUM

	/*
	 * Run the launcher in UTILITY role so its catalog probes (pg_database,
	 * pg_extension) execute purely on the coordinator without trying to
	 * dispatch to QEs.  A dispatched launcher hangs because no gang
	 * infrastructure exists in a bgworker context.
	 *
	 * The dynamically-spawned per-DB scheduler workers still set Gp_role =
	 * GP_ROLE_DISPATCH in their own entrypoint so policy SQL (CALL
	 * refresh_continuous_aggregate, etc.) runs as a proper QD session.
	 */
	Gp_role = GP_ROLE_UTILITY;
#endif

	MemSet(&hashinfo, 0, sizeof(hashinfo));
	hashinfo.keysize = sizeof(Oid);
	hashinfo.entrysize = sizeof(DbLauncherEntry);
	hashinfo.hcxt = TopMemoryContext;
	db_htab = hash_create("time_series launcher db hash",
						  64, &hashinfo,
						  HASH_BLOBS | HASH_CONTEXT | HASH_ELEM);
	cleanup_db_htab = db_htab;
	before_shmem_exit(launcher_pre_shmem_cleanup, (Datum) 0);

	/*
	 * Reset the bgworker counter and claim slot 1 for ourselves.  The counter
	 * survives postmaster restarts (it's in shared memory) which is exactly
	 * what we don't want on a launcher respawn -- old scheduler decrements
	 * never fired because their parent (us) died with their handles, so the
	 * counter would be stuck reflecting a dead population.
	 * Reinit-then-increment is the upstream pattern.
	 */
	ts_bgw_counter_reinit();
	/* always succeeds: counter just reset */
	(void) ts_bgw_total_workers_increment();

	ts_bgw_message_queue_set_reader();

	ereport(LOG, (errmsg("time_series launcher started")));

	/*
	 * If the launcher was restarted by postmaster, any per-DB schedulers we
	 * spawned in the previous incarnation are still parented to postmaster
	 * and continue to run, but we have no handles for them. On the next poll
	 * cycle we'd spawn duplicate workers and pile up. Terminate any stale
	 * schedulers up front so we start from a clean slate; the next populate /
	 * ensure cycle re-spawns whichever DBs still warrant a scheduler. Mirrors
	 * upstream src/loader/
	 * bgw_launcher.c::terminate_backends_by_backend_type.
	 */
	{
		int			n = pgstat_fetch_stat_numbackends();
		int			curr;

		for (curr = 1; curr <= n; curr++)
		{
			LocalPgBackendStatus *local = pgstat_fetch_stat_local_beentry(curr);
			const PgBackendStatus *be;
			const char *bt;

			if (local == NULL)
				continue;
			be = &local->backendStatus;
			bt = GetBackgroundWorkerTypeByPid(be->st_procpid);
			if (bt != NULL && strcmp(bt, SCHEDULER_BGW_NAME) == 0)
			{
				(void) kill(be->st_procpid, SIGTERM);
				elog(DEBUG1, "time_series launcher: SIGTERM'd stale scheduler pid %d",
					 be->st_procpid);
			}
		}
	}

	while (!got_SIGTERM)
	{
		long		poll_ms;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		/* (Re)discover DBs and drive existing entries forward. */
		populate_database_htab(db_htab);
		scheduler_state_trans_automatic_all(db_htab);

		/*
		 * Drain any pending control-plane messages.  SetLatch from a sender
		 * bumps us out of the WaitLatch below; we handle them here.  After
		 * each message we run the auto-transitions again in case the message
		 * moved an entry to ENABLED.
		 */
		while (launcher_handle_message(db_htab))
			scheduler_state_trans_automatic_all(db_htab);

		poll_ms = (long) guc_bgw_launcher_poll_time_ms;
		if (poll_ms < 100)
			poll_ms = 100;

		/* reset BEFORE wait so prior signals don't short-circuit */
		ResetLatch(MyLatch);
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   poll_ms,
					   PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	ereport(LOG, (errmsg("time_series launcher exiting on SIGTERM")));

	/*
	 * Exit with non-zero status so postmaster restarts us after
	 * `bgw_scheduler_restart_time` seconds.  The launcher is the cluster-wide
	 * registry of per-DB schedulers; if it stayed dead after a SIGTERM (e.g.
	 * a regression test forcibly kills it), no new CAGG policies on any DB
	 * would ever be picked up until the next postmaster restart.  Mirrors
	 * upstream's note in src/loader/bgw_launcher.c: "we decided that on
	 * SIGTERM the launcher should still be restarted".
	 */
	proc_exit(1);
}

/*
 * Register the launcher as a static bgworker during _PG_init.  Called
 * from time_series.c.  Coordinator-only (segments do not run a launcher;
 * see file header).
 */
void
bgw_register_launcher(void)
{
	BackgroundWorker worker;

	if (!guc_bgw_enabled)
		return;

#ifdef GP_VERSION_NUM
	if (Gp_role != GP_ROLE_DISPATCH)
		return;
#endif

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	/*
	 * Launcher gets a hard-coded 5 s postmaster restart interval so that a
	 * launcher crash always self-heals -- this is independent of the
	 * scheduler-level GUC (guc_bgw_scheduler_restart_time_sec, which
	 * defaults to NEVER_RESTART and only controls scheduler respawn).  The
	 * launcher itself is a single cluster-wide singleton and must come
	 * back up regardless of whether the operator wants postmaster to
	 * respawn dying schedulers.  Mirrors TimescaleDB upstream's
	 * BGW_LAUNCHER_RESTART_TIME_S.
	 */
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "%s", TS_EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "bgw_launcher_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", LAUNCHER_BGW_NAME);
	snprintf(worker.bgw_type, BGW_MAXLEN, "%s", LAUNCHER_BGW_NAME);
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
