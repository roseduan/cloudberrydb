/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_JOB_H
#define BGW_JOB_H

#include <postgres.h>
#include <postmaster/bgworker.h>
#include <storage/lock.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

#define SCHEDULER_APPNAME "time_series Background Worker Scheduler"

/* ----------------------------------------------------------------
 * Catalog table names used by the BGW framework.  All three live
 * in the time_series schema.
 * ----------------------------------------------------------------
 */
#define BGW_SCHEMA_NAME		"time_series"
#define BGW_JOB_TABLE		"bgw_job"
#define BGW_JOB_TABLE_FQ	BGW_SCHEMA_NAME "." BGW_JOB_TABLE

/* ----------------------------------------------------------------
 * FormData_bgw_job: in-memory mirror of one row from
 * time_series.bgw_job.  Field order matches the SQL CREATE TABLE
 * so SPI tuple unpacking can SPI_getbinval column-by-column.
 * hypertable_id is reserved for cagg policies (= cagg_id);
 * generic jobs leave it 0.
 * ----------------------------------------------------------------
 */
typedef struct FormData_bgw_job
{
	int32		id;
	NameData	application_name;
	Interval	schedule_interval;
	Interval	max_runtime;
	int32		max_retries;
	Interval	retry_period;
	NameData	proc_schema;
	NameData	proc_name;
	Oid			owner;
	bool		scheduled;
	bool		fixed_schedule;
	TimestampTz initial_start;
	int32		hypertable_id;
	Jsonb	   *config;
	NameData	check_schema;
	NameData	check_name;
	text	   *timezone;
} FormData_bgw_job;

typedef struct BgwJobHistory
{
	int64 id;
	TimestampTz execution_start;
} BgwJobHistory;

typedef struct BgwJob
{
	FormData_bgw_job fd;
	BgwJobHistory job_history;
} BgwJob;

/* Positive result numbers reserved for success */
typedef enum JobResult
{
	JOB_FAILURE_TO_START = -1,
	JOB_FAILURE_IN_EXECUTION = 0,
	JOB_SUCCESS = 1,
} JobResult;

typedef bool job_main_func(void);
typedef bool (*scheduler_test_hook_type)(BgwJob *job);

extern BackgroundWorkerHandle *bgw_job_start(BgwJob *job, Oid user_oid);

extern List *bgw_job_get_scheduled(size_t alloc_size, MemoryContext mctx);

extern bool bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx);
extern bool bgw_lock_job_id(int32 job_id, LOCKMODE mode, bool session_lock,
						   LOCKTAG *tag, bool block);
extern BgwJob *bgw_job_find(int job_id, MemoryContext mctx, bool fail_if_not_found);

extern bool bgw_job_has_timeout(BgwJob *job);
extern TimestampTz bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);

extern bool bgw_job_update_by_id(int32 job_id, BgwJob *job);
extern void bgw_job_permission_check(BgwJob *job, const char *cmd);
extern void bgw_job_validate_job_owner(Oid owner);

extern JobResult bgw_job_execute(BgwJob *job);
extern JobResult bgw_job_execute_real(BgwJob *job);
extern void bgw_job_run_config_check(Oid check, int32 job_id, Jsonb *config);

extern Datum bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void bgw_job_set_scheduler_test_hook(scheduler_test_hook_type hook);
extern void bgw_job_set_job_entrypoint_function_name(char *func_name);

extern void bgw_job_validate_schedule_interval(Interval *schedule_interval);
extern char *bgw_job_validate_timezone(Datum timezone);

extern Oid bgw_job_get_funcid(BgwJob *job);

extern bool bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func,
											  int64 initial_runs,
											  Interval *next_interval,
											  bool atomic, bool mark);

#endif /* BGW_JOB_H */
