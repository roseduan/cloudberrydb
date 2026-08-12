/*-------------------------------------------------------------------------
 *
 * ts_fork_name.c
 *    Relpath hook for time_series extension data forks.
 *
 *    Generates filesystem paths for extension data forks using the
 *    "ts_<N>" naming convention.  Fork number N (where N >=
 *    TS_FIRST_CHUNKNUM) maps to filename suffix
 *    "ts_<N - TS_FIRST_CHUNKNUM>".
 *
 * Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *    contrib/time_series/src/storage/ts_fork_name.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "storage/backendid.h"

#include "../include/storage/ts_fork_name.h"
#include "../include/time_series.h"   /* for TS_FIRST_CHUNKNUM */
#include "../include/access/ts_tableam.h"

/* Saved previous hook for chaining */
static relpath_hook_type prev_relpath_hook = NULL;

/*
 * extension_fork_name - return the "ext<N>" suffix for an extension fork
 */
static const char *
extension_fork_name(ForkNumber forknum)
{
	static char buf[16];

	snprintf(buf, sizeof(buf), "ts_%d",
			 forknum - TS_FIRST_CHUNKNUM);
	return buf;
}

/*
 * ts_relpath_hook - generate relation path for extension data forks
 *
 * For forks >= TS_FIRST_CHUNKNUM, builds the full filesystem path
 * using the "ext<N>" suffix.  For standard forks, returns NULL so the
 * default GetRelationPath logic runs.
 */
static char *
ts_relpath_hook(Oid dbNode, Oid spcNode, Oid relNode,
				int backendId, ForkNumber forkNumber)
{
	const char *forkName;

	if (prev_relpath_hook)
	{
		char	   *path;

		path = prev_relpath_hook(dbNode, spcNode, relNode,
								 backendId, forkNumber);
		if (path)
			return path;
	}

	/* Only handle extension data forks */
	if (forkNumber < TS_FIRST_CHUNKNUM)
		return NULL;

	forkName = extension_fork_name(forkNumber);

	if (spcNode == GLOBALTABLESPACE_OID)
	{
		return psprintf("global/%u_%s", relNode, forkName);
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		if (backendId == InvalidBackendId)
			return psprintf("base/%u/%u_%s",
							dbNode, relNode, forkName);
		else
			return psprintf("base/%u/t_%u_%s",
							dbNode, relNode, forkName);
	}
	else
	{
		if (backendId == InvalidBackendId)
			return psprintf("pg_tblspc/%u/%s/%u/%u_%s",
							spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
							dbNode, relNode, forkName);
		else
			return psprintf("pg_tblspc/%u/%s/%u/t_%u_%s",
							spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
							dbNode, relNode, forkName);
	}
}

/*
 * ts_fork_name_init - register relpath hook
 *
 * Called from _PG_init().  Saves previous hook for chaining.
 */
void
ts_fork_name_init(void)
{
	prev_relpath_hook = relpath_hook;
	relpath_hook = ts_relpath_hook;
}
