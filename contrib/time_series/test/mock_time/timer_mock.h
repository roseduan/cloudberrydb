/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Ported verbatim from TimescaleDB test/src/bgw/timer_mock.h (Apache 2.0).
 */
#pragma once

#include <postgres.h>
#include <postmaster/bgworker.h>

#include "../../src/include/bgw/timer.h"

extern void timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle,
											  MemoryContext scheduler_mctx);

extern const Timer ts_mock_timer;
