/*
 * job_stat_history.c
 *    Per-execution audit log for BGW job runs.
 *
 * V1 always records every job execution to time_series.bgw_job_stat_history:
 *
 *   START   (from mark_start)  INSERT a row, save id on the BgwJob struct
 *   PID     (from worker fork) UPDATE the row's pid
 *   END     (from mark_end)    UPDATE finish/succeeded/data
 *
 * The data JSONB snapshot has the shape
 *   {"job": {<bgw_job snapshot>}, "error_data": <edata>?}
 *
 * Always-on logging (no GUC gate): upstream upstream's track-only-errors
 * path INSERTs from mark_end inside the BGW worker, which hangs on
 * CBDB due to a QD-QE self-deadlock around BIGSERIAL nextval dispatch
 * (BGW workers lack a libpq frontend connection to service the QE's
 * nextval callback).  Splitting INSERT to mark_start (scheduler) and
 * UPDATE to mark_end (worker, WHERE id = $N) avoids it.
 *
 * Apache 2.0 -- adapted from upstream src/bgw/job_stat_history.c.
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#include <postgres.h>

#include <access/xact.h>
#include <executor/spi.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "job_stat_history.h"
#include "../include/bgw/timer.h"

/*
 * Build the data JSONB snapshot for an audit row.  Allocated in the
 * caller's MemoryContext (passed via caller_ctx) so the pointer survives
 * SPI_finish.  The caller must already have an open SPI connection.
 */
static Jsonb *
build_history_data(BgwJob * job, Jsonb *edata, MemoryContext caller_ctx)
{
	StringInfoData jb;
	Datum		d;
	bool		isnull;
	Jsonb	   *out = NULL;
	MemoryContext spi_ctx;
	int			rc;
	const char *config_lit;
	const char *edata_clause;

	if (job->fd.config == NULL)
		config_lit = "'null'";
	else
		config_lit = quote_literal_cstr(JsonbToCString(NULL,
													   &job->fd.config->root, VARSIZE(job->fd.config)));

	if (edata == NULL)
		edata_clause = "";
	else
		edata_clause = psprintf(", 'error_data', %s::jsonb",
								quote_literal_cstr(JsonbToCString(NULL,
																  &edata->root, VARSIZE(edata))));

	initStringInfo(&jb);
	appendStringInfo(&jb,
					 "SELECT jsonb_build_object("
					 "  'job', jsonb_strip_nulls(jsonb_build_object("
					 "     'id',                %d,"
					 "     'application_name',  %s,"
					 "     'proc_schema',       %s,"
					 "     'proc_name',         %s,"
					 "     'config',            %s::jsonb"
					 "  ))"
					 "%s"
					 ")",
					 job->fd.id,
					 quote_literal_cstr(NameStr(job->fd.application_name)),
					 quote_literal_cstr(NameStr(job->fd.proc_schema)),
					 quote_literal_cstr(NameStr(job->fd.proc_name)),
					 config_lit,
					 edata_clause);

	rc = SPI_execute(jb.data, true /* read_only */ , 1);
	pfree(jb.data);
	if (rc != SPI_OK_SELECT || SPI_processed != 1)
	{
		elog(WARNING, "build_history_data: SPI_execute returned %d", rc);
		return NULL;
	}

	spi_ctx = CurrentMemoryContext;
	d = SPI_getbinval(SPI_tuptable->vals[0],
					  SPI_tuptable->tupdesc, 1, &isnull);
	if (!isnull)
	{
		MemoryContextSwitchTo(caller_ctx);
		out = DatumGetJsonbPCopy(d);
		MemoryContextSwitchTo(spi_ctx);
	}
	return out;
}

/*
 * Insert a START row.  Sets job->job_history.id from RETURNING.
 */
static void
history_insert_start(BgwJob * job)
{
	Oid			argtypes[2] = {INT4OID, TIMESTAMPTZOID};
	Datum		argvals[2];
	int			rc;

	argvals[0] = Int32GetDatum(job->fd.id);
	argvals[1] = TimestampTzGetDatum(job->job_history.execution_start);

	rc = SPI_execute_with_args(
							   "INSERT INTO " BGW_JOB_STAT_HISTORY_TABLE_FQ
							   "   (job_id, execution_start) VALUES ($1, $2) RETURNING id",
							   2, argtypes, argvals, NULL, false, 1);

	if (rc == SPI_OK_INSERT_RETURNING && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			job->job_history.id = DatumGetInt64(d);
	}
	else
		elog(WARNING, "history_insert_start: SPI rc=%d processed=%lu",
			 rc, (unsigned long) SPI_processed);
}

/*
 * UPDATE the pid on a previously-inserted START row.
 */
static void
history_update_pid(BgwJob * job)
{
	Oid			argtypes[2] = {INT4OID, INT8OID};
	Datum		argvals[2];
	int			rc;

	argvals[0] = Int32GetDatum(MyProcPid);
	argvals[1] = Int64GetDatum(job->job_history.id);

	rc = SPI_execute_with_args(
							   "UPDATE " BGW_JOB_STAT_HISTORY_TABLE_FQ
							   "   SET pid = $1 WHERE id = $2",
							   2, argtypes, argvals, NULL, false, 0);

	if (rc != SPI_OK_UPDATE)
		elog(WARNING, "history_update_pid: SPI rc=%d", rc);
}

/*
 * UPDATE finish/succeeded/data on a previously-inserted START row.
 */
static void
history_update_finish(BgwJob * job, bool succeeded, Jsonb *data)
{
	Oid			argtypes[5] = {TIMESTAMPTZOID, BOOLOID, JSONBOID, INT4OID, INT8OID};
	Datum		argvals[5];
	char		nulls[5] = {' ', ' ', ' ', ' ', ' '};
	int			rc;

	argvals[0] = TimestampTzGetDatum(timer_get_current_timestamp());
	argvals[1] = BoolGetDatum(succeeded);
	if (data == NULL)
	{
		argvals[2] = (Datum) 0;
		nulls[2] = 'n';
	}
	else
		argvals[2] = JsonbPGetDatum(data);
	argvals[3] = Int32GetDatum(MyProcPid);
	argvals[4] = Int64GetDatum(job->job_history.id);

	rc = SPI_execute_with_args(
							   "UPDATE " BGW_JOB_STAT_HISTORY_TABLE_FQ
							   "   SET execution_finish = $1, succeeded = $2, data = $3, pid = $4"
							   "   WHERE id = $5",
							   5, argtypes, argvals, nulls, false, 0);

	if (rc != SPI_OK_UPDATE)
		elog(WARNING, "history_update_finish: SPI rc=%d", rc);
}

void
bgw_job_stat_history_update(BgwJobStatHistoryUpdateType update_type,
							BgwJob * job, JobResult result, Jsonb *edata)
{
	bool		succeeded;
	Jsonb	   *data = NULL;
	MemoryContext caller_ctx;

	switch (update_type)
	{
		case JOB_STAT_HISTORY_UPDATE_START:
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			history_insert_start(job);
			PopActiveSnapshot();
			SPI_finish();
			elog(DEBUG1, "bgw_job_stat_history: START job_id=%d hist_id=%ld",
				 job->fd.id, (long) job->job_history.id);
			return;

		case JOB_STAT_HISTORY_UPDATE_PID:
			if (job->job_history.id == INVALID_BGW_JOB_STAT_HISTORY_ID)
				return;
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			history_update_pid(job);
			PopActiveSnapshot();
			SPI_finish();
			elog(DEBUG2, "bgw_job_stat_history: PID job_id=%d hist_id=%ld pid=%d",
				 job->fd.id, (long) job->job_history.id, MyProcPid);
			return;

		case JOB_STAT_HISTORY_UPDATE_END:
			succeeded = (result == JOB_SUCCESS);
			if (job->job_history.id == INVALID_BGW_JOB_STAT_HISTORY_ID)
				return;			/* mark_start was skipped (e.g. crash) --
								 * nothing to update */

			caller_ctx = CurrentMemoryContext;
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			data = build_history_data(job, edata, caller_ctx);
			history_update_finish(job, succeeded, data);
			PopActiveSnapshot();
			SPI_finish();
			elog(DEBUG1, "bgw_job_stat_history: END job_id=%d hist_id=%ld "
				 "succeeded=%s%s",
				 job->fd.id, (long) job->job_history.id,
				 succeeded ? "true" : "false",
				 edata != NULL ? " (with error_data)" : "");
			return;

		default:
			elog(WARNING, "bgw_job_stat_history_update: unknown update_type %d",
				 (int) update_type);
			return;
	}
}
