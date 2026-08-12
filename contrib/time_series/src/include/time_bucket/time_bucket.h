/*-------------------------------------------------------------------------
 *
 * time_bucket.h
 *    Public API of the time_bucket subsystem.
 *
 *    Each function maps an input value to the start of the bucket it
 *    falls into, given a bucket width (and optional origin/timezone).
 *    Naming pattern: ts_<type>[_<variant>]_bucket.
 *
 *    Consumers so far: gapfill (invokes several bucket functions via
 *    DirectFunctionCall2 to compute synthetic-row bucket boundaries),
 *    plus SQL callers via pg_proc.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/time_bucket/time_bucket.h
 *-------------------------------------------------------------------------
 */
#ifndef TS_TIME_BUCKET_H
#define TS_TIME_BUCKET_H

#include "postgres.h"
#include "fmgr.h"

/* Integer types. */
extern Datum ts_int16_bucket(PG_FUNCTION_ARGS);
extern Datum ts_int32_bucket(PG_FUNCTION_ARGS);
extern Datum ts_int64_bucket(PG_FUNCTION_ARGS);

/* timestamp (without time zone). */
extern Datum ts_timestamp_bucket(PG_FUNCTION_ARGS);
extern Datum ts_timestamp_offset_bucket(PG_FUNCTION_ARGS);

/* timestamptz (with time zone). */
extern Datum ts_timestamptz_bucket(PG_FUNCTION_ARGS);
extern Datum ts_timestamptz_offset_bucket(PG_FUNCTION_ARGS);
extern Datum ts_timestamptz_timezone_bucket(PG_FUNCTION_ARGS);

/* date. */
extern Datum ts_date_bucket(PG_FUNCTION_ARGS);
extern Datum ts_date_offset_bucket(PG_FUNCTION_ARGS);

#endif							/* TS_TIME_BUCKET_H */
