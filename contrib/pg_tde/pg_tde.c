/*-------------------------------------------------------------------------
 *
 * pg_tde.c
 *	  Tablespace TDE management functions.
 *
 *	  Exports:
 *	    pg_tde_status()       – show loaded DEKs from shared memory
 *	    tde_open_tablespace() – unlock a tablespace by name
 *	    tde_sync_keys()       – rescan .wkey directory and load missing keys
 *
 * IDENTIFICATION
 *	  contrib/pg_tde/pg_tde.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "crypto/tblspc_kmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_tde_status);
PG_FUNCTION_INFO_V1(tde_open_tablespace);
PG_FUNCTION_INFO_V1(tde_sync_keys);

/* Map a TBLSPC_ENC_* code to its human-readable name */
static const char *
enc_method_str(uint8 code)
{
	switch (code)
	{
		case TBLSPC_ENC_AES128: return "AES128";
		case TBLSPC_ENC_AES192: return "AES192";
		case TBLSPC_ENC_AES256: return "AES256";
		case TBLSPC_ENC_SM4:    return "SM4";
		default:                return "NONE";
	}
}

/*
 * pg_tde_status()
 *
 * Returns one row per loaded tablespace DEK in shared memory:
 *   spc_oid    oid    – tablespace OID
 *   spc_name   text   – tablespace name (NULL if dropped but key still present)
 *   enc_method text   – e.g. "AES256"
 *   loaded     bool   – always true for shmem entries; false means key present
 *                       on disk but not yet unwrapped (future use)
 */
Datum
pg_tde_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate    *tupstore;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	int					i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to read TDE key status")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build output tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "spc_oid",    OIDOID,  -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "spc_name",   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "enc_method", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "loaded",     BOOLOID, -1, 0);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult  = tupstore;
	rsinfo->setDesc    = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcontext);

	if (!TblspcKeyShmem)
		goto done;

	LWLockAcquire(&TblspcKeyShmem->lock, LW_SHARED);

	for (i = 0; i < TblspcKeyShmem->max_entries; i++)
	{
		SpcEncEntry *e = &TblspcKeyShmem->entries[i];
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};
		HeapTuple	tuple;
		char	   *spc_name;
		Oid			spc_oid;
		uint8		enc_method;
		uint8		present;

		if (e->spc_oid == InvalidOid)
			continue;

		/* Copy fields under the lock before releasing */
		spc_oid    = e->spc_oid;
		enc_method = e->enc_method;
		present    = e->present;

		LWLockRelease(&TblspcKeyShmem->lock);

		spc_name = get_tablespace_name(spc_oid);

		values[0] = ObjectIdGetDatum(spc_oid);
		if (spc_name)
			values[1] = CStringGetTextDatum(spc_name);
		else
		{
			values[1] = (Datum) 0;
			nulls[1]  = true;
		}
		values[2] = CStringGetTextDatum(enc_method_str(enc_method));
		values[3] = BoolGetDatum(present != 0);

		tuple = heap_form_tuple(rsinfo->setDesc, values, nulls);
		tuplestore_puttuple(tupstore, tuple);

		LWLockAcquire(&TblspcKeyShmem->lock, LW_SHARED);
	}

	LWLockRelease(&TblspcKeyShmem->lock);

done:
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

/*
 * tde_open_tablespace(name text) → bool
 *
 * Load the DEK for the named tablespace from its .wkey file into shared
 * memory.  Useful when the KMS was unreachable at server startup and the
 * tablespace is therefore locked.
 *
 * Returns true on success, false if the KMS is still unreachable.
 */
Datum
tde_open_tablespace(PG_FUNCTION_ARGS)
{
	text   *arg  = PG_GETARG_TEXT_PP(0);
	char   *name = text_to_cstring(arg);
	Oid		spcOid;
	bool	ok;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to unlock a tablespace")));

	spcOid = get_tablespace_oid(name, false);	/* errors on missing tablespace */

	ok = TblspcKmgrLoadKey(spcOid);

	PG_RETURN_BOOL(ok);
}

/*
 * tde_sync_keys() → integer
 *
 * Rescan $PGDATA/pg_cryptokeys/tablespaces/ and load any .wkey files whose
 * DEK is not yet present in shared memory.  Returns the count of newly
 * loaded keys.
 *
 * Typical use: after adding a new tablespace on a replica, or after
 * recovering from a KMS outage.
 */
Datum
tde_sync_keys(PG_FUNCTION_ARGS)
{
	int		n;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to sync TDE keys")));

	n = TblspcKmgrSyncKeys();

	PG_RETURN_INT32(n);
}
