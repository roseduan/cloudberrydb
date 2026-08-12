/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 *
 * Portions Copyright (c) 2025-2026, HashData Technology Limited.
 */
/*
 * utils.{c,h} — shared utility functions for the BGW subsystem.
 *
 * Anything in this file is a small cross-cutting helper used by more
 * than one .c in src/bgw/.  Keep it small: if a logical group of
 * helpers grows past ~150 lines, split it out into its own named
 * file (e.g. bgw_jsonb_helpers.c, bgw_locks.c).
 */
#ifndef BGW_UTILS_H
#define BGW_UTILS_H

#include <postgres.h>
#include <utils/elog.h>		/* ErrorData */
#include <utils/jsonb.h>

/* ----------------------------------------------------------------
 * Capture an ErrorData into a JSONB object for storage in
 * bgw_job_stat_history.data.  Captured fields: sqlerrcode, message,
 * detail, hint, filename, lineno, funcname, domain, context_domain,
 * context, schema_name, table_name, column_name, datatype_name,
 * constraint_name, internalquery, detail_log; plus proc_schema /
 * proc_name from the caller (so analytics views don't need to re-join
 * bgw_job).
 *
 * Returns NULL if edata is NULL — callers (build_history_data) treat
 * that as "no error_data for this row" and skip the JSONB key.
 *
 * Implementation: src/bgw/utils.c.
 * ----------------------------------------------------------------
 */
extern Jsonb *errdata_to_jsonb(ErrorData *edata,
								  Name proc_schema, Name proc_name);

#endif /* BGW_UTILS_H */
