/*-------------------------------------------------------------------------
 *
 * storage_tablespace_xact.c
 *
 *	  implement hooks for transactions and tablespace storage
 *
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_tablespace_xact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact_storage_tablespace.h"
#include "catalog/storage_tablespace.h"
#include "crypto/tblspc_kmgr.h"


/*
 * AtCommit_TablespaceStorage:
 *
 * Needs to happen before locks are released to ensure that no
 * concurrent sessions are using the tablespace storage.
 *
 */
void
AtCommit_TablespaceStorage(void)
{
	/*
	 * Capture the tablespace being dropped before DoPending...() clears it, so
	 * we can drop its TDE key at the same commit point as the directory
	 * removal.  Deferring the key deletion to here (rather than doing it
	 * eagerly in DropTableSpace) ensures a rolled-back DROP keeps the key.
	 */
	Oid			dropped = GetPendingTablespaceForDeletionForCommit();

	DoPendingTablespaceDeletionForCommit();
	if (OidIsValid(dropped))
		TblspcKmgrDropKey(dropped);
	UnscheduleTablespaceDirectoryDeletionForAbort();
}


/*
 * AtAbort_TablespaceStorage:
 *
 * Needs to happen before locks are released to ensure that no
 * concurrent sessions are using the tablespace storage.
 *
 */
void
AtAbort_TablespaceStorage(void)
{
	/*
	 * A rolled-back CREATE TABLESPACE leaves behind the DEK that
	 * TblspcKmgrCreateKey() wrote before commit.  Drop it at the same point as
	 * the directory removal (mirrors the commit-time drop above), using the
	 * tablespace oid scheduled for abort-time deletion.
	 */
	Oid			created = GetPendingTablespaceForDeletionForAbort();

	DoPendingTablespaceDeletionForAbort();
	if (OidIsValid(created))
		TblspcKmgrDropKey(created);
	UnscheduleTablespaceDirectoryDeletionForCommit();
}
