/*
 * cagg_refresh_policy.h
 *    CAGG automatic refresh policy - the TSL-equivalent layer.
 *
 * This is the policy execution function that the BGW worker calls
 * when a CAGG refresh job is due.  It reads the job's JSONB config
 * (start_offset, end_offset), computes the refresh window as
 * [now - start_offset, now - end_offset], and executes:
 *   CALL time_series.refresh_continuous_aggregate(view_name, start, end)
 *
 * Copyright (c) 2026 HashData Inc.
 */
#ifndef CAGG_REFRESH_POLICY_H
#define CAGG_REFRESH_POLICY_H

#include <postgres.h>
#include <fmgr.h>

/* SQL-callable: policy_refresh_cagg(job_id int, config jsonb) */
extern Datum policy_refresh_cagg(PG_FUNCTION_ARGS);

#endif /* CAGG_REFRESH_POLICY_H */
