/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
#ifndef BGW_JOB_STAT_HISTORY_H
#define BGW_JOB_STAT_HISTORY_H

#include "job.h"
#include "job_stat.h"

#define INVALID_BGW_JOB_STAT_HISTORY_ID 0

#define BGW_JOB_STAT_HISTORY_TABLE		"bgw_job_stat_history"
#define BGW_JOB_STAT_HISTORY_TABLE_FQ	BGW_SCHEMA_NAME "." BGW_JOB_STAT_HISTORY_TABLE

typedef enum BgwJobStatHistoryUpdateType
{
	JOB_STAT_HISTORY_UPDATE_START,
	JOB_STAT_HISTORY_UPDATE_END,
	JOB_STAT_HISTORY_UPDATE_PID,
} BgwJobStatHistoryUpdateType;

extern void bgw_job_stat_history_update(BgwJobStatHistoryUpdateType update_type, BgwJob *job,
										   JobResult result, Jsonb *edata);

#endif /* BGW_JOB_STAT_HISTORY_H */
