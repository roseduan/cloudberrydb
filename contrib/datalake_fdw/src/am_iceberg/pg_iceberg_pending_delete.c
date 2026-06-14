/*-------------------------------------------------------------------------
 *
 * pg_iceberg_pending_delete.c
 *    Abort-time cleanup of iceberg staging files via the PendingRelDelete
 *    framework, mirroring src/backend/catalog/storage_directory_table.c.
 *
 *    INSERT/UPDATE/DELETE on a native ICEBERG table writes data/delete parquet
 *    files to object storage before the transaction commits.  On commit the
 *    files are referenced by the new snapshot and must be kept; on abort they
 *    are orphans and must be removed.  Each QE writer registers one
 *    PendingRelDelete(atCommit=false) per finalized file here; the abort
 *    callback rebuilds a gopher filesystem handle from the storage options
 *    captured at registration and deletes the file (best-effort).
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/am_iceberg/pg_iceberg_pending_delete.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/storage.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "src/datalake_def.h"				/* gopherOptions */
#include "src/common/fileSystemWrapper.h"	/* datalakeDeleteFileByOptions */
#include "include/pg_iceberg_pending_delete.h"

typedef struct PendingRelDeleteIceberg
{
	PendingRelDelete reldelete;		/* base; MUST be first member */
	char		   *path;			/* full staging file path (TopMemoryContext) */
	gopherOptions  *gopher;			/* deep copy in TopMemoryContext */
} PendingRelDeleteIceberg;

/* Deep-copy a gopherOptions into TopMemoryContext (struct + every char* field). */
static gopherOptions *
copy_gopher_options_top(const gopherOptions *src)
{
	gopherOptions *dst = (gopherOptions *)
		MemoryContextAlloc(TopMemoryContext, sizeof(gopherOptions));

	*dst = *src;				/* scalars + bools + raw pointer values */

#define DUP(field) \
	(dst->field = (src->field) ? MemoryContextStrdup(TopMemoryContext, src->field) : NULL)
	DUP(worker_path); DUP(connect_path); DUP(connect_plasma_path);
	DUP(gopherType); DUP(bucket); DUP(protocol);
	DUP(accessKey); DUP(secretKey); DUP(host); DUP(region);
	DUP(hdfs_namenode_host); DUP(hdfs_auth_method);
	DUP(krb_principal); DUP(krb_principal_keytab); DUP(krb5_ccname);
	DUP(hadoop_rpc_protection); DUP(hdfs_user);
	DUP(dfs_name_services); DUP(dfs_ha_namenodes); DUP(dfs_ha_namenode_rpc_addr);
	DUP(dfs_client_failover); DUP(krb_service_principal);
	DUP(ftp_path); DUP(ftp_username); DUP(ftp_password);
	DUP(database_install_dir);
#undef DUP
	return dst;
}

static void
free_gopher_options(gopherOptions *g)
{
#define FREE(field) do { if (g->field) pfree(g->field); } while (0)
	FREE(worker_path); FREE(connect_path); FREE(connect_plasma_path);
	FREE(gopherType); FREE(bucket); FREE(protocol);
	FREE(accessKey); FREE(secretKey); FREE(host); FREE(region);
	FREE(hdfs_namenode_host); FREE(hdfs_auth_method);
	FREE(krb_principal); FREE(krb_principal_keytab); FREE(krb5_ccname);
	FREE(hadoop_rpc_protection); FREE(hdfs_user);
	FREE(dfs_name_services); FREE(dfs_ha_namenodes); FREE(dfs_ha_namenode_rpc_addr);
	FREE(dfs_client_failover); FREE(krb_service_principal);
	FREE(ftp_path); FREE(ftp_username); FREE(ftp_password);
	FREE(database_install_dir);
#undef FREE
	pfree(g);
}

static void
IcebergDestroyPendingRelDelete(PendingRelDelete *reldelete)
{
	PendingRelDeleteIceberg *p = (PendingRelDeleteIceberg *) reldelete;

	if (p->path)
		pfree(p->path);
	if (p->gopher)
		free_gopher_options(p->gopher);
	pfree(p);
}

static void
IcebergDoPendingRelDelete(PendingRelDelete *reldelete)
{
	PendingRelDeleteIceberg *p = (PendingRelDeleteIceberg *) reldelete;

	/*
	 * Runs during AbortTransaction (atCommit=false).  Must never propagate an
	 * error out of the abort path.  datalakeDeleteFileByOptions already
	 * swallows C++ exceptions; wrap in PG_TRY to also absorb any ereport from
	 * the C side.  Leaked files (failure here, crash, or mid-statement error)
	 * are backstopped by Class 2 expire/orphan-listing cleanup.
	 */
	PG_TRY();
	{
		int			rc = datalakeDeleteFileByOptions((void *) p->gopher, p->path);

		if (rc != 0)
			elog(WARNING,
				 "iceberg: could not delete aborted staging file \"%s\"",
				 p->path);
	}
	PG_CATCH();
	{
		FlushErrorState();
		elog(WARNING,
			 "iceberg: error deleting aborted staging file \"%s\"", p->path);
	}
	PG_END_TRY();
}

static struct PendingRelDeleteAction iceberg_pending_rel_deletes_action = {
	.flags = PENDING_REL_DELETE_DEFAULT_FLAG,
	.destroy_pending_rel_delete = IcebergDestroyPendingRelDelete,
	.do_pending_rel_delete = IcebergDoPendingRelDelete
};

void
iceberg_register_staging_pending_delete(Relation rel, const char *path,
										void *gopher_opt)
{
	PendingRelDeleteIceberg *pending;
	MemoryContext old;

	if (path == NULL || gopher_opt == NULL)
		return;

	old = MemoryContextSwitchTo(TopMemoryContext);

	pending = (PendingRelDeleteIceberg *) palloc0(sizeof(PendingRelDeleteIceberg));
	pending->path = pstrdup(path);
	pending->gopher = copy_gopher_options_top((const gopherOptions *) gopher_opt);

	pending->reldelete.atCommit = false;	/* delete if abort */
	pending->reldelete.nestLevel = GetCurrentTransactionNestLevel();
	pending->reldelete.relnode.node = rel->rd_node;
	pending->reldelete.relnode.isTempRelation = false;
	pending->reldelete.relnode.smgr_which = SMGR_INVALID;	/* skip core smgr unlink */
	pending->reldelete.action = &iceberg_pending_rel_deletes_action;

	RegisterPendingDelete(&pending->reldelete);

	MemoryContextSwitchTo(old);
}
