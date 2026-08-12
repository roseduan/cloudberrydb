/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Adapted from TimescaleDB test/src/bgw/params.c (Apache 2.0).
 * Changes vs upstream:
 *   - Drop TSDB-specific includes (ts_catalog/catalog.h, scanner.h,
 *     test_utils.h, utils.h).
 *   - Replace ts_catalog_update() with PG-standard simple_heap_update().
 *   - Replace ts_get_relation_relid() with RangeVarGetRelid().
 *   - Replace TestAssertTrue() with Assert() (we always build with
 *     --enable-cassert in dev, but production builds get a graceful
 *     elog ERROR when invariant is violated).
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <pgstat.h>
#include <storage/dsm.h>
#include <storage/spin.h>
#include <utils/builtins.h>
#include <utils/snapmgr.h>

#include "params.h"

typedef struct TestParamsWrapper
{
	TestParams params;
	slock_t mutex;
} TestParamsWrapper;

/*
 * In CBDB the test harness's bgw_dsm_handle_store is a DISTRIBUTED
 * REPLICATED table.  Use SPI for both UPDATE and SELECT so dispatch
 * routes correctly.  Direct heap_scan + SnapshotSelf doesn't see rows
 * inserted via standard SQL on coordinator.
 */
/*
 * BGW worker contexts have no Portal so SPI_execute fails the
 * EnsurePortalSnapshotExists check in pquery.c.  Push an explicit
 * transaction snapshot around the SPI call (caller already supplied
 * the transaction).
 */
static void
spi_connect_with_snapshot(void)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	PushActiveSnapshot(GetTransactionSnapshot());
}

static void
spi_finish_with_snapshot(void)
{
	PopActiveSnapshot();
	SPI_finish();
}

static void
params_register_dsm_handle(dsm_handle handle)
{
	int		ret;
	Oid		argtypes[1] = { INT8OID };
	Datum	args[1] = { Int64GetDatum((int64) handle) };

	spi_connect_with_snapshot();

	ret = SPI_execute_with_args("UPDATE public.bgw_dsm_handle_store SET handle = $1",
								1, argtypes, args, NULL, false, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "UPDATE bgw_dsm_handle_store failed: %d", ret);
	if (SPI_processed != 1)
		elog(ERROR, "bgw_dsm_handle_store row count is %lu (expected 1)",
			 (unsigned long) SPI_processed);

	spi_finish_with_snapshot();
}

static dsm_handle
params_load_dsm_handle(void)
{
	int		ret;
	dsm_handle handle;
	bool	isnull;

	spi_connect_with_snapshot();

	ret = SPI_execute("SELECT handle FROM public.bgw_dsm_handle_store LIMIT 1",
					  true, 1);
	if (ret != SPI_OK_SELECT || SPI_processed == 0)
		elog(ERROR, "bgw_dsm_handle_store has no row (SPI ret=%d processed=%lu)",
			 ret, (unsigned long) SPI_processed);

	handle = (dsm_handle) DatumGetInt64(
		SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	if (isnull)
		elog(ERROR, "bgw_dsm_handle_store row has NULL handle");

	spi_finish_with_snapshot();
	return handle;
}

static dsm_handle
params_get_dsm_handle(void)
{
	static dsm_handle handle = 0;

	if (handle == 0)
		handle = params_load_dsm_handle();

	return handle;
}

static TestParamsWrapper *
params_open_wrapper(bool *do_close)
{
	dsm_segment *seg;
	dsm_handle handle = params_get_dsm_handle();
	TestParamsWrapper *wrapper;

	/*
	 * If segment is returned via the mapping then there's no need to call
	 * dsm_detach on it in params_close_wrapper.
	 */
	seg = dsm_find_mapping(handle);
	if (seg == NULL)
	{
		seg = dsm_attach(handle);
		if (seg == NULL)
			elog(ERROR, "got NULL segment in params_open_wrapper");
		*do_close = true;
	}
	else
		*do_close = false;

	Assert(seg != NULL);

	wrapper = dsm_segment_address(seg);
	Assert(wrapper != NULL);

	return wrapper;
}

static void
params_close_wrapper(TestParamsWrapper *wrapper)
{
	dsm_segment *seg = dsm_find_mapping(params_get_dsm_handle());

	Assert(seg != NULL);
	dsm_detach(seg);
}

TestParams *
ts_params_get(void)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);
	TestParams *res;

	Assert(wrapper != NULL);

	res = palloc(sizeof(TestParams));

	SpinLockAcquire(&wrapper->mutex);
	memcpy(res, &wrapper->params, sizeof(TestParams));
	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);

	return res;
}

void
ts_params_set_time(int64 new_val, bool set_latch)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	Assert(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);
	wrapper->params.current_time = new_val;
	SpinLockRelease(&wrapper->mutex);

	if (set_latch)
		SetLatch(&wrapper->params.timer_latch);

	if (do_close)
		params_close_wrapper(wrapper);
}

void
ts_initialize_timer_latch(void)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	Assert(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);
	InitLatch(&wrapper->params.timer_latch);
	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);
}

void
ts_reset_and_wait_timer_latch(void)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	Assert(wrapper != NULL);

	ResetLatch(&wrapper->params.timer_latch);
	WaitLatch(&wrapper->params.timer_latch,
			  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
			  10000,
			  PG_WAIT_EXTENSION);

	if (do_close)
		params_close_wrapper(wrapper);
}

static void
params_set_mock_wait_type(MockWaitType new_val)
{
	bool do_close;
	TestParamsWrapper *wrapper = params_open_wrapper(&do_close);

	Assert(wrapper != NULL);

	SpinLockAcquire(&wrapper->mutex);
	wrapper->params.mock_wait_type = new_val;
	SpinLockRelease(&wrapper->mutex);

	if (do_close)
		params_close_wrapper(wrapper);
}

PG_FUNCTION_INFO_V1(bgw_params_reset_time);
Datum
bgw_params_reset_time(PG_FUNCTION_ARGS)
{
	ts_params_set_time(PG_GETARG_INT64(0), PG_GETARG_BOOL(1));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bgw_params_mock_wait_returns_immediately);
Datum
bgw_params_mock_wait_returns_immediately(PG_FUNCTION_ARGS)
{
	params_set_mock_wait_type(PG_GETARG_INT32(0));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bgw_params_create);
Datum
bgw_params_create(PG_FUNCTION_ARGS)
{
	dsm_segment *seg = dsm_create(sizeof(TestParamsWrapper), 0);
	TestParamsWrapper *params;

	Assert(seg != NULL);

	params = dsm_segment_address(seg);
	*params = (TestParamsWrapper) {
		.params = { .current_time = 0 },
	};
	SpinLockInit(&params->mutex);

	params_register_dsm_handle(dsm_segment_handle(seg));

	dsm_pin_mapping(seg);
	dsm_pin_segment(seg);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(bgw_params_destroy);
Datum
bgw_params_destroy(PG_FUNCTION_ARGS)
{
	/*
	 * Removing shared memory segment unpin for now to keep parity with
	 * TSDB upstream — see their comment about EXEC_BACKEND quirks.
	 */
	PG_RETURN_VOID();
}
