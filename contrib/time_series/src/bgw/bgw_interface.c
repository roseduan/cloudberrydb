/*-------------------------------------------------------------------------
 *
 * bgw_interface.c
 *    SQL-callable C functions that send commands to the launcher via
 *    the shared-memory message queue.  Mirrors TimescaleDB's
 *    _timescaledb_functions.{start,stop,restart}_background_workers()
 *    so callers get a single-statement persistent operation.
 *
 * Adapted from TimescaleDB src/loader/bgw_interface.c (Apache 2.0).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/bgw/bgw_interface.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "bgw_message_queue.h"
#include "bgw_counter.h"

PG_FUNCTION_INFO_V1(ts_bgw_db_workers_start);
PG_FUNCTION_INFO_V1(ts_bgw_db_workers_stop);
PG_FUNCTION_INFO_V1(ts_bgw_db_workers_restart);

Datum
ts_bgw_db_workers_start(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to start background workers")));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(BGW_MSG_START, MyDatabaseId));
}

Datum
ts_bgw_db_workers_stop(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to stop background workers")));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(BGW_MSG_STOP, MyDatabaseId));
}

Datum
ts_bgw_db_workers_restart(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to restart background workers")));

	PG_RETURN_BOOL(ts_bgw_message_send_and_wait(BGW_MSG_RESTART, MyDatabaseId));
}
