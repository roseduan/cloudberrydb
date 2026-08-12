/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_JOB_STAT_H
#define BGW_JOB_STAT_H

#include "job.h"

#define LAST_CRASH_REPORTED 1

#define BGW_JOB_STAT_TABLE		"bgw_job_stat"
#define BGW_JOB_STAT_TABLE_FQ	BGW_SCHEMA_NAME "." BGW_JOB_STAT_TABLE

/* ----------------------------------------------------------------
 * FormData_bgw_job_stat: in-memory mirror of one row from
 * time_series.bgw_job_stat.  One row per job; updated in-place by
 * mark_start / mark_end.  Field order matches the SQL CREATE TABLE.
 * ----------------------------------------------------------------
 */
typedef struct FormData_bgw_job_stat
{
	int32		id;					/* job_id */
	TimestampTz last_start;
	TimestampTz last_finish;
	TimestampTz next_start;
	TimestampTz last_successful_finish;
	bool		last_run_success;
	int64		total_runs;
	Interval	total_duration;
	Interval	total_duration_failures;
	int64		total_successes;
	int64		total_failures;
	int64		total_crashes;
	int32		consecutive_failures;
	int32		consecutive_crashes;
	int32		flags;
} FormData_bgw_job_stat;

typedef struct BgwJobStat
{
	FormData_bgw_job_stat fd;
} BgwJobStat;

extern BgwJobStat *bgw_job_stat_find(int job_id);
extern void bgw_job_stat_mark_start(BgwJob *job);
extern void bgw_job_stat_mark_end(BgwJob *job, JobResult result, Jsonb *edata);
extern bool bgw_job_stat_end_was_marked(BgwJobStat *jobstat);

extern void bgw_job_stat_set_next_start(int32 job_id, TimestampTz next_start);
extern bool bgw_job_stat_update_next_start(int32 job_id, TimestampTz next_start,
											  bool allow_unset);

extern TimestampTz bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job,
											  int32 consecutive_failed_launches);
extern void bgw_job_stat_mark_crash_reported(BgwJob *job, JobResult result,
											  TimestampTz next_start);

extern TimestampTz get_next_scheduled_execution_slot(BgwJob *job,
														TimestampTz finish_time);

#endif /* BGW_JOB_STAT_H */
