/*-------------------------------------------------------------------------
 *
 * ts_smgr.h
 *    Public surface of the time_series smgr sidecar that serves chunk
 *    forks above MAX_FORKNUM.  Per-relation state lives in an
 *    extension-owned HTAB keyed by RelFileNodeBackend, released from
 *    the smgr_close_hook.
 *
 * Copyright (c) 2026 HashData Inc.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/include/storage/ts_smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TS_SMGR_H
#define TS_SMGR_H

#include "postgres.h"
#include "common/relpath.h"
#include "storage/smgr.h"

/* Install smgr_hook, smgr_close_hook, and file_unlink_hook. */
extern void ts_smgr_init(void);

/*
 * Invalidate the cached fork length.  Standard forks write
 * reln->smgr_cached_nblocks[forknum]; chunk forks invalidate the
 * equivalent slot in the extension's sidecar entry.
 */
extern void ts_smgr_invalidate_nblocks(SMgrRelation reln,
									   ForkNumber forknum);

#endif							/* TS_SMGR_H */
