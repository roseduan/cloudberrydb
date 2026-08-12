/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Adapted from TimescaleDB test/src/bgw/log.c (Apache 2.0).
 *
 * CBDB-specific deviation from 1:1: TSDB upstream uses
 * ts_catalog_insert_values() (a thin wrapper over simple_heap_insert)
 * to write directly to public.bgw_log.  On single-node PG that's
 * trivially correct.  On CBDB with the DISTRIBUTED REPLICATED bgw_log
 * table, heap_insert from a BGW worker on coordinator writes only to
 * the coordinator-local copy; test sessions reading in dispatch mode
 * read from segment copies and see ZERO rows.  We therefore use SPI
 * (which goes through MPP dispatch) so writes propagate to all
 * replicas.
 *
 * The SPI dispatch in turn imposes a contract: emit_log_hook is only
 * safe to install in worker processes that don't already run their
 * own SPI sessions producing arbitrary log lines (because nested SPI
 * during transaction cleanup crashes).  See test_dispatcher_requested
 * in scheduler_mock.c for the opt-in.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <postmaster/bgworker.h>
#include <storage/proc.h>
#include <utils/builtins.h>
#include <utils/snapmgr.h>

#include "log.h"
#include "params.h"

static char *bgw_application_name = "unset";

void
bgw_log_set_application_name(char *name)
{
	bgw_application_name = name;
}

static void
bgw_log_insert(char *msg)
{
	static int32 msg_no = 0;
	Oid argtypes[4] = { INT4OID, INT8OID, TEXTOID, TEXTOID };
	Datum args[4];

	args[0] = Int32GetDatum(msg_no++);
	args[1] = Int64GetDatum((int64) ts_params_get()->current_time);
	args[2] = CStringGetTextDatum(bgw_application_name);
	args[3] = CStringGetTextDatum(msg ? msg : "");

	if (SPI_connect() != SPI_OK_CONNECT)
		return;
	PushActiveSnapshot(GetTransactionSnapshot());

	(void) SPI_execute_with_args(
		"INSERT INTO public.bgw_log VALUES ($1, $2, $3, $4)",
		4, argtypes, args, NULL, false, 0);

	PopActiveSnapshot();
	SPI_finish();
}

static emit_log_hook_type prev_emit_log_hook = NULL;

/*
 * NOTE: using transactions in emit_log_hook functions is not recommended.
 * However we rely on this current functionality for our test verifications,
 * so have to live with it for now.
 */
static void
emit_log_hook_callback(ErrorData *edata)
{
	bool started_txn = false;

	/*
	 * Once proc_exit has started we may no longer be able to start
	 * transactions.
	 */
	if (MyProc == NULL)
		return;

	/*
	 * Skip when we're inside AbortTransaction / AbortSubTransaction
	 * processing.  SPI can't safely run during abort: an inner error
	 * inside SPI (e.g. StartTransactionCommand rejecting blockState =
	 * TBLOCK_STARTED) gets promoted to FATAL (PG_exception_stack is
	 * NULL during outer abort, see elog.c:433), and the promotion's
	 * errstart calls MemoryContextReset(ErrorContext), which wipes
	 * the message buffer of the OUTER elog whose errfinish then
	 * crashes on pfree(edata->message).  We've seen this happen with
	 * BGW workers whose refresh raised an error and the abort path
	 * fires elog(LOG, "session was reset ...") from GpDropTempTables
	 * (cdbgang.c:815); the SIGSEGV terminates the worker and trips
	 * postmaster cluster-wide recovery.  Losing one log line during
	 * abort is strictly better than crashing the worker.
	 */
	if (IsAbortInProgress() || IsAbortedTransactionBlockState())
		return;

	/*
	 * Block signals so we don't lose messages generated during signal
	 * processing if they occur while we are saving this log message
	 * (since emit_log_hook is modified and restored below).
	 */
	BackgroundWorkerBlockSignals();
	PG_TRY();
	{
		/*
		 * If we encounter an error writing to our log hook, remove the
		 * hook to prevent potentially infinite recursion where this
		 * callback keeps encountering an error and is its own logging
		 * callback.  We reinstall the hook when we're successfully done.
		 */
		emit_log_hook = NULL;

		if (!IsTransactionState())
		{
			StartTransactionCommand();
			started_txn = true;
		}

		bgw_log_insert(edata->message);

		if (started_txn)
			CommitTransactionCommand();

		if (prev_emit_log_hook != NULL)
			prev_emit_log_hook(edata);

		emit_log_hook = emit_log_hook_callback;
	}
	PG_CATCH();
	{
		/* If there was an error, rollback what was done before the error */
		if (IsTransactionState())
			AbortCurrentTransaction();

		emit_log_hook = emit_log_hook_callback;
	}
	PG_END_TRY();
	BackgroundWorkerUnblockSignals();
}

void
ts_register_emit_log_hook(void)
{
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = emit_log_hook_callback;
}
