/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Ported verbatim from TimescaleDB test/src/bgw/log.h (Apache 2.0).
 */
#pragma once

extern void bgw_log_set_application_name(char *name);
extern void ts_register_emit_log_hook(void);
