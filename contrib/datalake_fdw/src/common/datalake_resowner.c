/*-------------------------------------------------------------------------
 *
 * datalake_resowner.c
 *    ResourceOwner-based cleanup registry for datalake_fdw contexts.
 *
 *    Registers per-context handles against the current ResourceOwner so
 *    that list-result state is released on transaction/portal cleanup.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/datalake_resowner.c
 *-------------------------------------------------------------------------
 */
#include "datalake_resowner.h"
#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "c.h"
#include "util.h"
#ifdef USE_GOPHER
#include <gopher/gopher.h>
#endif

static datalake_context_handle_t *
datalake_create_context_handle(bool gp_is_writer);

static void
datalake_destroy_context_handle(datalake_context_handle_t *h);

static void
datalake_cleanup_context_handle(datalake_context_handle_t *h);

static void
datalake_context_abort_callback(ResourceReleasePhase phase,
					   bool isCommit,
					   bool isTopLevel,
					   void *arg);

static void
datalake_clear_list_result(int gp_session_id, int cid, bool gp_is_writer);

static datalake_context_handle_t *open_datalake_context_handles;

static bool datalake_context_resowner_callback_registered;

static datalake_context_handle_t *
datalake_create_context_handle(bool gp_is_writer)
{
	datalake_context_handle_t *h;
	h = MemoryContextAlloc(TopMemoryContext, sizeof(datalake_context_handle_t));
	h->cid = gp_command_count;
	h->gp_is_writer = gp_is_writer;
	h->owner = CurrentResourceOwner;
	h->next = open_datalake_context_handles;
	h->prev = NULL;
	if (open_datalake_context_handles)
		open_datalake_context_handles->prev = h;
	open_datalake_context_handles = h;

	return h;
}

/*
 * Close any open handles on abort.
 */
static void
datalake_destroy_context_handle(datalake_context_handle_t *h)
{
	datalake_cleanup_context_handle(h);
}

/*
 * Cleanup open handles.
 */
static void
datalake_cleanup_context_handle(datalake_context_handle_t *h)
{
	/* unlink from linked list first */
	if (h == NULL)
	{
		return;
	}
	if (h->prev)
		h->prev->next = h->next;
	else
		open_datalake_context_handles = h->next;
	if (h->next)
		h->next->prev = h->prev;

	datalake_clear_list_result(gp_session_id, h->cid, h->gp_is_writer);

	pfree(h);
}

/*
 * Close any open handles on abort.
 */
static void
datalake_context_abort_callback(ResourceReleasePhase phase,
					   bool isCommit,
					   bool isTopLevel,
					   void *arg)
{
	datalake_context_handle_t *curr;
	datalake_context_handle_t *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = open_datalake_context_handles;
	while (next)
	{
		curr = next;
		next = curr->next;

		if (curr->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(WARNING, "datalake execute-type external table reference leak: %p still referenced", curr);
			datalake_cleanup_context_handle(curr);
		}
	}
}

datalake_context_handle_t* datalake_register_resource_context(bool gp_is_writer)
{
	if (!datalake_context_resowner_callback_registered)
	{
		RegisterResourceReleaseCallback(datalake_context_abort_callback, NULL);
		datalake_context_resowner_callback_registered = true;
	}
	return datalake_create_context_handle(gp_is_writer);
}

void cleanup_datalake_resource_context(datalake_context_handle_t* h)
{
	datalake_destroy_context_handle(h);
}

static void datalake_clear_list_result(int gp_session_id, int cid, bool gp_is_writer)
{
#ifdef USE_GOPHER
	char hostAddress[MAXPGPATH + 1];
	DatalakeGetGopherSocketPath(hostAddress);
	gopherAdmin admin = gopherCreateAdmin(hostAddress);
	if (admin == NULL)
	{
		elog(LOG, "External table clear list result: failed to create admin handle for '%s' (cid=%d, session=%u) %s",
				 hostAddress, cid, gp_session_id, gopherGetLastError());
		return;
	}
	if (gopherAdminClearListResult(admin, gp_session_id, cid, gp_is_writer) == -1)
	{
		elog(LOG, "External table clear list result failed (cid=%d, session=%u) %s",
			cid, gp_session_id, gopherGetLastError());
	}
	gopherDeleteAdmin(admin);
#else
	/* S3 has no server-side list cache to clear */
	(void)gp_session_id;
	(void)cid;
	(void)gp_is_writer;
#endif
}
