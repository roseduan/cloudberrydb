/*
 * gapfill.c - GapFill marker functions and CustomScan registration
 *
 * Provides:
 *   - ht_gapfill_marker(): generic passthrough for locf()/interpolate()
 *   - 7 gapfill bucket functions (wrappers around ts_*_bucket)
 *   - GapFill CustomScan registration
 *
 * Clean-room implementation for time_series extension.
 *
 * Copyright (c) 2026 HashData Inc.
 * Licensed under Apache License 2.0
 */
#include "../include/time_series.h"
#include "../include/gapfill/gapfill.h"
#include "../include/time_bucket/time_bucket.h"

#include "utils/timestamp.h"
#include "utils/date.h"
#include "nodes/extensible.h"

/* ================================================================
 * Marker Function
 * ================================================================
 *
 * locf(value) and interpolate(value) are declared in SQL to call
 * ht_gapfill_marker. At SQL level, they simply pass through the
 * first argument. The GapFill executor recognizes these function
 * OIDs in the target list and applies LOCF or interpolation logic
 * on gap rows.
 */
PG_FUNCTION_INFO_V1(ht_gapfill_marker);
Datum
ht_gapfill_marker(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/* ================================================================
 * GapFill Bucket Functions
 * ================================================================
 *
 * time_bucket_gapfill(bucket_width, ts, start, finish) is declared
 * to call these C functions. They do the actual bucketing by
 * delegating to the ts_*_bucket functions, but they also accept
 * start/finish arguments (which the executor uses to determine
 * the gap range).
 *
 * At SQL level, arguments 2 (start) and 3 (finish) are optional
 * DEFAULT NULL. The bucket function itself ignores them — the
 * GapFill executor extracts them during planning.
 */

PG_FUNCTION_INFO_V1(ht_gapfill_timestamp_bucket);
Datum
ht_gapfill_timestamp_bucket(PG_FUNCTION_ARGS)
{
	/* time_bucket_gapfill(interval, timestamp, start, finish) */
	return DirectFunctionCall2(ts_timestamp_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_timestamptz_bucket);
Datum
ht_gapfill_timestamptz_bucket(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(ts_timestamptz_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_int16_bucket);
Datum
ht_gapfill_int16_bucket(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(ts_int16_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_int32_bucket);
Datum
ht_gapfill_int32_bucket(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(ts_int32_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_int64_bucket);
Datum
ht_gapfill_int64_bucket(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(ts_int64_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_date_bucket);
Datum
ht_gapfill_date_bucket(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(ts_date_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

PG_FUNCTION_INFO_V1(ht_gapfill_timestamptz_timezone_bucket);
Datum
ht_gapfill_timestamptz_timezone_bucket(PG_FUNCTION_ARGS)
{
	/* time_bucket_gapfill(interval, timestamptz, timezone, start, finish) */
	/* timezone is arg2, start is arg3, finish is arg4 */
	if (!PG_ARGISNULL(2))
	{
		return DirectFunctionCall3(ts_timestamptz_timezone_bucket,
								   PG_GETARG_DATUM(0),
								   PG_GETARG_DATUM(1),
								   PG_GETARG_DATUM(2));
	}
	else
	{
		return DirectFunctionCall2(ts_timestamptz_bucket,
								   PG_GETARG_DATUM(0),
								   PG_GETARG_DATUM(1));
	}
}

/* ================================================================
 * GapFill CustomScan Methods
 * ================================================================ */

const CustomScanMethods ht_gapfill_scan_methods = {
	.CustomName = "GapFill",
	.CreateCustomScanState = ht_gapfill_create_state,
};

/*
 * Register the GapFill custom scan methods.
 * Called from _PG_init().
 */
void
ht_gapfill_scan_init(void)
{
	RegisterCustomScanMethods(&ht_gapfill_scan_methods);
}
