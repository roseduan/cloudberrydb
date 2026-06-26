/*-------------------------------------------------------------------------
 *
 * storage_tablespace_twophase.c
 *
 *	  implement hooks for twophase and tablespace storage
 *
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_tablespace_twophase.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_tablespace.h"
#include "access/twophase_storage_tablespace.h"
#include "crypto/tblspc_kmgr.h"


void
AtTwoPhaseCommit_TablespaceStorage()
{
	/*
	 * Drop the TDE key for the dropped tablespace at the same commit point as
	 * the directory removal (see AtCommit_TablespaceStorage for rationale).
	 * Capture the pending oid before DoPending...() clears it.
	 */
	Oid			dropped = GetPendingTablespaceForDeletionForCommit();

	DoPendingTablespaceDeletionForCommit();
	if (OidIsValid(dropped))
		TblspcKmgrDropKey(dropped);
	UnscheduleTablespaceDirectoryDeletionForAbort();
}


void
AtTwoPhaseAbort_TablespaceStorage()
{
	/*
	 * Drop the DEK left by a rolled-back CREATE TABLESPACE at the same point as
	 * the directory removal (see AtAbort_TablespaceStorage for rationale).
	 */
	Oid			created = GetPendingTablespaceForDeletionForAbort();

	DoPendingTablespaceDeletionForAbort();
	if (OidIsValid(created))
		TblspcKmgrDropKey(created);
	UnscheduleTablespaceDirectoryDeletionForCommit();
}
