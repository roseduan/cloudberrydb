/*-------------------------------------------------------------------------
 *
 * pg_upgrade_pax.c
 *	  PAX state capture and accessors for pg_upgrade.
 *
 * This file owns everything that used to be inline PAX logic in
 * core pg_upgrade files:
 *
 *   - pax_detect_cluster_backend()  (was inline in info.c::get_rel_infos)
 *   - pax_capture_rel_state()       (was a ~150 line block in info.c)
 *   - pax_init_rel_state() / pax_free_rel_state() / pax_rel_has_state()
 *
 * Core info.c now calls these as black boxes; the PaxRelState struct
 * is opaque to pg_upgrade.h (only this file and check_pax.c look
 * inside).
 *
 *	  src/bin/pg_upgrade/greenplum/pg_upgrade_pax.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_class_d.h"

#include "pg_upgrade.h"
#include "pg_upgrade_pax.h"

PaxBackendKind
pax_detect_cluster_backend(PGconn *conn)
{
	PGresult		   *res;
	PaxBackendKind		kind;
	const char		   *installed;
	const char		   *has_pax_tables;

	/*
	 * Single round trip to decide:
	 *   pax not installed in pg_am             -> NONE
	 *   pax installed AND pg_pax_tables exists -> CATALOG
	 *   pax installed AND pg_pax_tables absent -> MANIFEST
	 *
	 * The catalog/manifest choice is fixed at PAX compile time, so
	 * pg_pax_tables existence is authoritative for the whole cluster.
	 */
	res = executeQueryOrDie(conn,
		"SELECT "
		"  EXISTS(SELECT 1 FROM pg_catalog.pg_am "
		"         WHERE amname = 'pax') AS pax_installed, "
		"  EXISTS(SELECT 1 FROM pg_catalog.pg_class c "
		"         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		"         WHERE n.nspname = 'pg_ext_aux' "
		"           AND c.relname = 'pg_pax_tables') AS has_pax_tables");

	installed       = PQgetvalue(res, 0, 0);
	has_pax_tables  = PQgetvalue(res, 0, 1);

	if (installed[0] != 't')
		kind = PAX_BACKEND_NONE;
	else if (has_pax_tables[0] == 't')
		kind = PAX_BACKEND_CATALOG;
	else
		kind = PAX_BACKEND_MANIFEST;

	PQclear(res);
	return kind;
}

void
pax_init_rel_state(RelInfo *rel)
{
	rel->pax = NULL;
}

bool
pax_rel_has_state(const RelInfo *rel)
{
	return rel->pax != NULL && rel->pax->backend != PAX_BACKEND_NONE;
}

void
pax_free_rel_state(PaxRelState *st)
{
	int		i;

	if (st == NULL)
		return;

	for (i = 0; i < st->nblocks; i++)
	{
		if (st->blocks[i].ptstatistics)
			pg_free(st->blocks[i].ptstatistics);
		if (st->blocks[i].ptvisimapname)
			pg_free(st->blocks[i].ptvisimapname);
	}
	if (st->blocks)
		pg_free(st->blocks);
	if (st->manifest.path)
		pg_free(st->manifest.path);
	pg_free(st);
}

void
pax_capture_rel_state(RelInfo *rel,
					  PGconn *conn,
					  PaxBackendKind cluster_kind,
					  PGresult *res,
					  int relnum,
					  int i_amname,
					  char relkind)
{
	PaxRelState	   *st;
	PGresult	   *paxres;
	int				j;
	const char	   *aux_prefix;
	char			aux_buf[NAMEDATALEN];

	/*
	 * Fast skip: caller may have called us on every relation; we
	 * filter PAX leaves here so info.c stays generic.
	 */
	if (cluster_kind == PAX_BACKEND_NONE)
		return;
	if (i_amname < 0)
		return;
	if (PQgetisnull(res, relnum, i_amname))
		return;
	if (strcmp(PQgetvalue(res, relnum, i_amname), "pax") != 0)
		return;
	/*
	 * Accept ordinary tables and materialized views.  PAX matviews ('m')
	 * have a relfilenode and a <relfilenode>_pax/ data directory just like
	 * a regular PAX table; if we skipped them here they would fall through
	 * to the heap transfer path and the _pax/ directory would never be
	 * copied to segments, leaving aux-table rows pointing at non-existent
	 * manifest files.  Partition roots ('p') correctly remain excluded —
	 * they have no storage.
	 */
	if (relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW)
		return;

	/*
	 * Confirm the aux table actually exists for this reloid.  Skips
	 * any PAX-AM relation that for some reason has no aux entry
	 * (defensive — partition roots already filtered above).
	 */
	Assert(cluster_kind == PAX_BACKEND_CATALOG || cluster_kind == PAX_BACKEND_MANIFEST);
	aux_prefix = (cluster_kind == PAX_BACKEND_CATALOG)
				 ? "pg_pax_blocks_" : "pg_manifest_";
	snprintf(aux_buf, sizeof(aux_buf), "%s%u", aux_prefix, rel->reloid);

	paxres = executeQueryOrDie(conn,
				"SELECT 1 FROM pg_catalog.pg_class c "
				"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
				"WHERE  n.nspname = 'pg_ext_aux' "
				"  AND  c.relname = '%s' LIMIT 1",
				aux_buf);
	if (PQntuples(paxres) != 1)
	{
		PQclear(paxres);
		return;
	}
	PQclear(paxres);

	st = (PaxRelState *) pg_malloc0(sizeof(PaxRelState));
	st->backend = cluster_kind;
	rel->pax = st;

	if (cluster_kind == PAX_BACKEND_CATALOG)
	{
		/*
		 * Each micro-partition has a row in pg_pax_blocks_<reloid>.
		 * Capture them all.  ptstatistics round-trips via the
		 * paxauxstats↔bytea WITHOUT FUNCTION cast (design doc §13.2.1).
		 */
		paxres = executeQueryOrDie(conn,
					"SELECT ptblockname, pttupcount, ptblocksize, "
					"       encode(ptstatistics::bytea, 'hex') AS ptstatistics_hex, "
					"       ptvisimapname, "
					"       ptexistexttoast, ptisclustered "
					"FROM   pg_ext_aux.pg_pax_blocks_%u "
					"ORDER BY ptblockname",
					rel->reloid);

		st->nblocks = PQntuples(paxres);
		if (st->nblocks > 0)
			st->blocks = (PaxBlockInfo *) pg_malloc(
				sizeof(PaxBlockInfo) * st->nblocks);

		for (j = 0; j < st->nblocks; j++)
		{
			PaxBlockInfo *b = &st->blocks[j];

			b->ptblockname = atoi(PQgetvalue(paxres, j,
				PQfnumber(paxres, "ptblockname")));
			b->pttupcount = atoi(PQgetvalue(paxres, j,
				PQfnumber(paxres, "pttupcount")));
			b->ptblocksize = atoi(PQgetvalue(paxres, j,
				PQfnumber(paxres, "ptblocksize")));
			b->ptstatistics = pg_strdup(PQgetvalue(paxres, j,
				PQfnumber(paxres, "ptstatistics_hex")));
			if (PQgetisnull(paxres, j, PQfnumber(paxres, "ptvisimapname")))
				b->ptvisimapname = NULL;
			else
				b->ptvisimapname = pg_strdup(PQgetvalue(paxres, j,
					PQfnumber(paxres, "ptvisimapname")));
			b->ptexistexttoast =
				(PQgetvalue(paxres, j,
					PQfnumber(paxres, "ptexistexttoast"))[0] == 't');
			b->ptisclustered =
				(PQgetvalue(paxres, j,
					PQfnumber(paxres, "ptisclustered"))[0] == 't');
		}
		PQclear(paxres);
	}
	else /* MANIFEST */
	{
		/*
		 * pg_manifest_<reloid> holds a single TEXT row whose value is
		 * the filename of the active manifest JSON inside the
		 * relation's <relfilenode>_pax/ directory.  Capture just that
		 * pointer — the JSON file itself rides on the directory
		 * hard-link done by transfer_pax_relfilenode_dir.
		 */
		paxres = executeQueryOrDie(conn,
					"SELECT path FROM pg_ext_aux.pg_manifest_%u LIMIT 1",
					rel->reloid);
		if (PQntuples(paxres) == 1)
			st->manifest.path = pg_strdup(PQgetvalue(paxres, 0, 0));
		PQclear(paxres);
	}

	/*
	 * Both backends share pg_pax_fastsequence.  Capture the high-water
	 * mark; absent row -> 0 + has_fastseq=false.
	 *
	 * max(seq), not a plain projection: the index on
	 * pg_pax_fastsequence(objid) is not unique, so a damaged cluster can
	 * hold several rows for one objid.  A plain "SELECT seq" would then
	 * return more than one row, the PQntuples() == 1 test would fail, and
	 * we would silently capture nothing — leaving restore_pax_aux_table()
	 * to skip the fastsequence write and the upgraded relation sitting at
	 * the seq = 0 that CREATE TABLE installed, i.e. the next INSERT
	 * overwriting an existing micro-partition file.  The aggregate always
	 * returns exactly one row (NULL when there is none), so both nseq = 0
	 * and nseq > 1 degrade to "take the highest value we saw".
	 *
	 * check_for_pax_tables() rejects nseq != 1 up front, but only for
	 * catalog-mode relations: the manifest backend keeps its block list
	 * off-catalog, so that preflight cannot run there and this is the only
	 * line of defence.
	 */
	paxres = executeQueryOrDie(conn,
				"SELECT max(seq) FROM pg_ext_aux.pg_pax_fastsequence "
				"WHERE objid = %u::pg_catalog.oid",
				rel->reloid);
	if (PQntuples(paxres) == 1 && !PQgetisnull(paxres, 0, 0))
	{
		st->fastseq = atoi(PQgetvalue(paxres, 0, 0));
		st->has_fastseq = true;
	}
	PQclear(paxres);

	if (st->backend == PAX_BACKEND_CATALOG)
		pg_log(PG_VERBOSE,
			   "PAX (catalog) state captured for %s.%s (oid %u): "
			   "%d blocks, fastseq %d%s\n",
			   rel->nspname, rel->relname, rel->reloid,
			   st->nblocks, st->fastseq,
			   st->has_fastseq ? "" : " (no fastseq row)");
	else
		pg_log(PG_VERBOSE,
			   "PAX (manifest) state captured for %s.%s (oid %u): "
			   "path %s, fastseq %d%s\n",
			   rel->nspname, rel->relname, rel->reloid,
			   st->manifest.path ? st->manifest.path : "(none)",
			   st->fastseq,
			   st->has_fastseq ? "" : " (no fastseq row)");
}
