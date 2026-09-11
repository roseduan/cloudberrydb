/*-------------------------------------------------------------------------
 *
 * check_pax.c
 *	  pg_upgrade support for PAX (column-optimized) tables.
 *
 * PAX-specific pieces of pg_upgrade live here, keeping check_gp.c focused
 * on the broader Greenplum check pipeline:
 *
 *   check_for_pax_tables()
 *     A compatibility check.  Originally added in Phase 1 as a fatal
 *     guard ("PAX upgrade not supported, use pg_dump / pg_restore");
 *     once the full Phase 2 stack lands it will be softened into an
 *     optional health check on aux-table / fastsequence integrity.
 *
 *   transfer_pax_relfilenode_dir()
 *     Phase 2b file-transfer entry point.  Where transfer_relfile()
 *     copies/links a heap relation's single numbered file series and
 *     transfer_ao() walks the AO segment-file series, PAX physical data
 *     is a directory of micro-partition files (+ visimap files) rooted
 *     at <relfilenode>_pax/.  This function walks the old PAX directory
 *     and link/copy/clones every entry into the new cluster's PAX
 *     directory according to user_opts.transfer_mode.
 *
 *   check_pax_backend_compatibility()
 *     Cross-cluster guard.  PAX picks its metadata backend at build time
 *     (-DUSE_PAX_CATALOG=ON/OFF); a binary upgrade replays the captured
 *     aux rows into whichever aux catalog the new cluster actually has,
 *     so the two clusters must agree.  Runs inside check_new_cluster().
 *
 *   restore_pax_aux_tables()
 *     Phase 2b aux-content restore.  After transfer_pax_relfilenode_dir()
 *     puts the physical micro-partition files in place, this writes back
 *     the captured rows of pg_ext_aux.pg_pax_blocks_<reloid> and resets
 *     pg_ext_aux.pg_pax_fastsequence.seq so that subsequent INSERTs on
 *     the upgraded cluster don't collide with existing block_ids.
 *     Mirrors restore_aosegment_tables() exactly.
 *
 *	  src/bin/pg_upgrade/greenplum/check_pax.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>

#include "common/file_perm.h"

#include "pg_upgrade_greenplum.h"
#include "pg_upgrade_pax.h"

#include "pqexpbuffer.h"

static void restore_pax_aux_table(PGconn *conn, RelInfo *rel);

/*
 * check_for_pax_tables
 *
 * Phase 2b health-checks that run before pg_upgrade commits to the
 * upgrade.  Two checks dispatch on the per-relation backend:
 *
 *   CATALOG-mode preflight:
 *     - paxauxstats ↔ bytea WITHOUT-FUNCTION casts must exist
 *       (info.c uses encode(::bytea,'hex') to capture, restore
 *       uses decode(…,'hex')::paxauxstats — see design doc §13.2.1).
 *       Missing casts → fatal with the exact DDL so the operator can
 *       add them in one step.
 *     - On segments, pg_pax_fastsequence.seq >= max(ptblockname)+1
 *       per catalog-mode PAX leaf.  A smaller value means the next
 *       INSERT after upgrade would collide with an existing block_id.
 *
 *   MANIFEST-mode preflight:
 *     - The cast prereq isn't applicable (manifest mode stores block
 *       metadata in a JSON file, not in a paxauxstats column).
 *     - The block list lives off-catalog in the JSON file; we can't
 *       cheaply max(block_id) via SQL.  Skip the fastseq sanity check
 *       in manifest mode and trust the manifest's internal versioning.
 *       A future enhancement would parse the JSON server-side via a
 *       PAX-provided UDF.
 *
 * Both checks cover every relkind that can own PAX storage — ordinary
 * tables ('r') and materialized views ('m') — so they validate exactly
 * the set of relations pax_capture_rel_state() captures.  A PAX matview
 * has a relfilenode, a <relfilenode>_pax/ directory and its own
 * pg_ext_aux entries just like a PAX table does, so leaving it out here
 * would silently skip the health checks for it.
 *
 * The catalog/manifest choice, on the other hand, is a cluster-wide
 * property rather than a per-relation one: pax_detect_cluster_backend()
 * derives it once from the existence of pg_ext_aux.pg_pax_tables, and
 * check_pax_backend_compatibility() compares a single kind per cluster.
 * A database holding both catalog-mode and manifest-mode PAX tables is
 * not a supported state; the dispatch below is written per relation only
 * so that each check queries the aux catalog that actually exists.
 */
void
check_for_pax_tables(ClusterInfo *cluster)
{
	int			dbnum;
	bool		found = false;

	prep_status("Checking PAX tables for upgrade readiness");

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);
		PGresult   *res;
		int			n_catalog;
		int			ntups;

		/*
		 * Count catalog-mode PAX leaves in this DB.  If zero, the cast
		 * prereq doesn't apply (manifest mode doesn't need it).
		 */
		res = executeQueryOrDie(conn,
								"SELECT count(*) "
								"FROM   pg_catalog.pg_class c "
								"JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
								"WHERE  n.nspname = 'pg_ext_aux' "
	/*
	 * Anchored regex, not LIKE: in a LIKE pattern the underscores of
	 * 'pg_pax_blocks_' are single-character wildcards.  Same predicate
	 * info.c uses to keep these aux tables out of the file map.
	 */
								"  AND  c.relname ~ '^pg_pax_blocks_[0-9]+$' ");
		n_catalog = atoi(PQgetvalue(res, 0, 0));
		PQclear(res);

		/*
		 * Any PAX tables at all (either backend) in this DB?  If not,
		 * nothing to validate.
		 */
		res = executeQueryOrDie(conn,
								"SELECT 1 FROM pg_catalog.pg_class c "
								"JOIN   pg_catalog.pg_am am ON am.oid = c.relam "
								"WHERE  am.amname = 'pax' "
								"  AND  c.relkind IN ('r', 'm') "
								"LIMIT 1");
		if (PQntuples(res) == 0)
		{
			PQclear(res);
			PQfinish(conn);
			continue;
		}
		PQclear(res);

		/*
		 * 1) paxauxstats ↔ bytea casts — only required if at least one
		 * PAX leaf in this DB uses the catalog backend.  Manifest-only
		 * DBs skip this check.
		 */
		if (n_catalog > 0)
		{
			res = executeQueryOrDie(conn,
									"SELECT count(*) "
									"FROM pg_catalog.pg_cast c "
									"JOIN pg_catalog.pg_type src ON src.oid = c.castsource "
									"JOIN pg_catalog.pg_type dst ON dst.oid = c.casttarget "
									"WHERE c.castmethod = 'b' "
									"  AND ((src.typname = 'paxauxstats' AND dst.typname = 'bytea') "
									"    OR (src.typname = 'bytea' AND dst.typname = 'paxauxstats'))");
			ntups = atoi(PQgetvalue(res, 0, 0));
			PQclear(res);

			if (ntups < 2)
			{
				if (!found)
					pg_log(PG_REPORT, "fatal\n");
				found = true;
				gp_fatal_log(
					"| Your installation has PAX (catalog-backend) tables\n"
					"| but database \"%s\" is missing the paxauxstats <-> bytea\n"
					"| casts that pg_upgrade needs to round-trip the\n"
					"| pg_pax_blocks_<oid> ptstatistics column safely.\n"
					"|\n"
					"| Run this DDL on every database that contains PAX tables,\n"
					"| on every node (QD + each segment, utility mode), and then\n"
					"| retry the upgrade:\n"
					"|\n"
					"|   DO $$ BEGIN\n"
					"|     CREATE CAST (pg_ext_aux.paxauxstats AS pg_catalog.bytea)\n"
					"|       WITHOUT FUNCTION;\n"
					"|     CREATE CAST (pg_catalog.bytea AS pg_ext_aux.paxauxstats)\n"
					"|       WITHOUT FUNCTION;\n"
					"|   EXCEPTION WHEN duplicate_object THEN NULL;\n"
					"|   END $$;\n\n",
					active_db->db_name);
			}
		}

		/*
		 * 2) Per-table fastsequence sanity (segment only).
		 *
		 * Find any PAX leaf whose pg_pax_fastsequence.seq is <
		 * max(ptblockname) + 1 in its pg_pax_blocks_<oid>.  A smaller
		 * seq means the next INSERT will allocate a colliding block_id.
		 *
		 * This check is meaningful only on segments — pg_pax_blocks_<oid>
		 * is empty on QD by construction.  We use is_greenplum_dispatcher_mode
		 * to tell which side we're on.
		 */
		if (!is_greenplum_dispatcher_mode())
		{
			/*
			 * pg_pax_blocks_<oid> is one relation per PAX table, so we
			 * can't fold the check into a single set-based query.
			 * Enumerate catalog-mode PAX leaves (we need pg_pax_blocks_*
			 * to exist to do max(ptblockname)) and per-leaf compare
			 * pg_pax_fastsequence.seq against max(ptblockname)+1.
			 *
			 * Manifest-mode leaves are skipped: their block list lives
			 * in a JSON file on disk, not in a queryable catalog table,
			 * so we can't compute max(block_id) cheaply via SQL.  The
			 * manifest's own versioning chain provides equivalent
			 * integrity (each manifest knows its predecessor).
			 */
			res = executeQueryOrDie(conn,
				"SELECT n.nspname, c.relname, c.oid::text "
				"FROM   pg_catalog.pg_class c "
				"JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
				"JOIN   pg_catalog.pg_am am ON am.oid = c.relam "
				"WHERE  am.amname = 'pax' "
				"  AND  c.relkind IN ('r', 'm') "
				"  AND  EXISTS (SELECT 1 FROM pg_class cb "
				"               JOIN pg_namespace nb ON nb.oid = cb.relnamespace "
				"               WHERE nb.nspname = 'pg_ext_aux' "
				"                 AND cb.relname = 'pg_pax_blocks_' || c.oid::text)");

			for (int i = 0; i < PQntuples(res); i++)
			{
				const char *nspname = PQgetvalue(res, i, 0);
				const char *relname = PQgetvalue(res, i, 1);
				const char *oidstr  = PQgetvalue(res, i, 2);
				PGresult   *r2;
				int			nseq;
				int			seq;
				int			need;

				/*
				 * The index on pg_pax_fastsequence(objid) is not unique
				 * (CREATE INDEX, see pax-cdbinit--1.0.sql), so a damaged
				 * cluster can hold several rows for one objid.  Count the
				 * rows and take max(seq) rather than reading the value
				 * through a scalar subquery: a scalar subquery would fail
				 * with "more than one row returned by a subquery used as
				 * an expression" and abort the run with a message that
				 * names neither PAX nor the relation — inside the very
				 * check meant to diagnose broken fastsequence state.
				 */
				r2 = executeQueryOrDie(conn,
					"SELECT (SELECT count(*) "
					"        FROM   pg_ext_aux.pg_pax_fastsequence "
					"        WHERE  objid = %s::oid) AS nseq, "
					"       coalesce("
					"         (SELECT max(seq) "
					"          FROM   pg_ext_aux.pg_pax_fastsequence "
					"          WHERE  objid = %s::oid), -1) AS seq, "
					"       coalesce(max(ptblockname)+1, 0) AS need "
					"FROM pg_ext_aux.pg_pax_blocks_%s",
					oidstr, oidstr, oidstr);

				nseq = atoi(PQgetvalue(r2, 0, 0));
				seq  = atoi(PQgetvalue(r2, 0, 1));
				need = atoi(PQgetvalue(r2, 0, 2));
				PQclear(r2);

				/*
				 * Three broken states, one repair: no fastsequence row,
				 * duplicate rows, or a single row that lags behind the
				 * micro-partitions already on disk.
				 */
				if (nseq != 1 || seq < need)
				{
					char		seqdesc[96];
					const char *consequence;
					int			fixseq;

					/*
					 * Name the state, so the operator can tell which of
					 * the three they are looking at.
					 */
					if (nseq == 0)
						snprintf(seqdesc, sizeof(seqdesc),
								 "has no pg_pax_fastsequence row");
					else if (nseq > 1)
						snprintf(seqdesc, sizeof(seqdesc),
								 "has %d pg_pax_fastsequence rows, "
								 "the largest with seq = %d", nseq, seq);
					else
						snprintf(seqdesc, sizeof(seqdesc),
								 "has pg_pax_fastsequence.seq = %d", seq);

					if (seq < need)
						consequence =
							"| The next INSERT after upgrade would allocate a\n"
							"| block_id that collides with an existing\n"
							"| micro-partition file.\n";
					else
						consequence =
							"| PAX expects exactly one fastsequence row per\n"
							"| relation; with duplicates the block_id handed\n"
							"| out next depends on which row is read first.\n";

					/*
					 * Never hand out a repair value that moves the
					 * counter backwards: with duplicate rows max(seq) can
					 * already exceed max(ptblockname)+1, and resetting to
					 * the smaller number would create the very collision
					 * this check exists to prevent.
					 */
					fixseq = (seq > need) ? seq : need;

					if (!found)
						pg_log(PG_REPORT, "fatal\n");
					found = true;

					/*
					 * The repair is DELETE + INSERT, not UPDATE:
					 * pg_pax_fastsequence has a non-unique index on
					 * objid so ON CONFLICT is unavailable, a plain
					 * UPDATE would silently match zero rows in the
					 * missing-row case, and it would leave duplicates
					 * duplicated.  restore_pax_aux_table() writes the
					 * same pair for the same reason.
					 */
					gp_fatal_log(
						"| PAX relation \"%s\".\"%s\" in database \"%s\"\n"
						"| %s, but pg_pax_blocks_%s has\n"
						"| max(ptblockname)+1 = %d.\n"
						"|\n"
						"%s"
						"|\n"
						"| Fix the source cluster before upgrade by running\n"
						"| on this segment in utility mode:\n"
						"|\n"
						"|   BEGIN;\n"
						"|   DELETE FROM pg_ext_aux.pg_pax_fastsequence\n"
						"|    WHERE objid = %s::pg_catalog.oid;\n"
						"|   INSERT INTO pg_ext_aux.pg_pax_fastsequence\n"
						"|          (objid, seq) VALUES (%s, %d);\n"
						"|   COMMIT;\n\n",
						nspname, relname, active_db->db_name,
						seqdesc, oidstr, need,
						consequence,
						oidstr, oidstr, fixseq);
				}
			}
			PQclear(res);
		}

		PQfinish(conn);
	}

	if (!found)
		check_ok();
}

/*
 * pax_backend_kind_name
 *
 * Human-readable name used in the mismatch message below.
 */
static const char *
pax_backend_kind_name(PaxBackendKind kind)
{
	switch (kind)
	{
		case PAX_BACKEND_CATALOG:
			return "catalog";
		case PAX_BACKEND_MANIFEST:
			return "manifest";
		case PAX_BACKEND_NONE:
			break;
	}
	return "none";
}

/*
 * check_pax_backend_compatibility
 *
 * PAX keeps its per-relation metadata in one of two backends, selected at
 * PAX build time:
 *
 *   CATALOG  (-DUSE_PAX_CATALOG=ON)  — pg_ext_aux.pg_pax_blocks_<reloid>,
 *            one row per micro-partition.
 *   MANIFEST (-DUSE_PAX_CATALOG=OFF) — pg_ext_aux.pg_manifest_<reloid>, a
 *            single row naming a JSON file inside <relfilenode>_pax/.
 *
 * A binary upgrade transfers the physical <relfilenode>_pax/ directory as
 * is and then replays the captured aux rows into the new cluster.  That
 * only works when both clusters agree on which aux catalog exists: the
 * restore in restore_pax_aux_table() issues INSERTs against either
 * pg_pax_blocks_<oid> or pg_manifest_<oid>, and the relation the other
 * backend would expect simply does not exist there.
 *
 * The old cluster's kind is already known: get_rel_infos() stamped every
 * PAX leaf's PaxRelState with the kind returned by
 * pax_detect_cluster_backend(), so we scan the collected RelInfo array
 * rather than opening a second connection to the old cluster.  The new
 * cluster is probed once, directly.
 *
 * Unlike the earlier --skip-pax approach this check is unconditional:
 * there is no bypass option, because skipping PAX tables during a binary
 * upgrade leaves too many catalog inconsistencies (partition hierarchies,
 * sequences, foreign keys) to be a viable upgrade path.
 */
void
check_pax_backend_compatibility(void)
{
	PaxBackendKind old_kind = PAX_BACKEND_NONE;
	PaxBackendKind new_kind;
	PGconn	   *conn;
	int			dbnum;

	prep_status("Checking PAX metadata backend compatibility");

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *olddb = &old_cluster.dbarr.dbs[dbnum];
		int			relnum;

		for (relnum = 0; relnum < olddb->rel_arr.nrels; relnum++)
		{
			RelInfo    *rel = &olddb->rel_arr.rels[relnum];

			if (!pax_rel_has_state(rel))
				continue;

			old_kind = rel->pax->backend;
			break;
		}

		if (old_kind != PAX_BACKEND_NONE)
			break;
	}

	/*
	 * No PAX leaf in the source cluster — there is nothing whose aux rows
	 * we would have to replay, so any new-cluster backend is fine.
	 */
	if (old_kind == PAX_BACKEND_NONE)
	{
		check_ok();
		return;
	}

	conn = connectToServer(&new_cluster, "template1");
	new_kind = pax_detect_cluster_backend(conn);
	PQfinish(conn);

	if (new_kind != old_kind)
		pg_fatal("PAX metadata backend mismatch: old cluster uses %s mode, "
				 "new cluster uses %s mode.\n"
				 "Rebuild the new cluster's PAX with a matching backend "
				 "(-DUSE_PAX_CATALOG=%s) and re-run pg_upgrade.\n",
				 pax_backend_kind_name(old_kind),
				 pax_backend_kind_name(new_kind),
				 old_kind == PAX_BACKEND_CATALOG ? "ON" : "OFF");

	check_ok();
}

/*
 * transfer_pax_relfilenode_dir
 *
 * Move a PAX table's physical data from the old cluster to the new cluster.
 *
 * Unlike heap and AO, PAX stores each table's data as a *directory* named
 * <relfilenode>_pax/ under base/<dboid>/.  Inside that directory are the
 * micro-partition data files (one per block_id) and any visimap files
 * referenced by pg_ext_aux.pg_pax_blocks_<reloid>.ptvisimapname.
 *
 * Since OID is preserved by --binary-upgrade but relfilenode is not, the
 * old and new directory names differ; we drive both paths from the
 * FileNameMap that the standard pg_upgrade transfer machinery built.
 *
 * We do NOT interpret the directory contents — just enumerate every entry
 * and apply user_opts.transfer_mode to each.  Subdirectories aren't
 * expected in the PAX layout; if we hit one we warn and skip it rather
 * than recursing or failing.
 *
 * Empty / missing source directory is not an error: that's the expected
 * state on the QD (PAX data lives on segments) and for empty PAX tables.
 * We still create the matching empty directory on the new cluster so the
 * PAX runtime's mkdir-if-exists logic stays happy.
 */
void
transfer_pax_relfilenode_dir(FileNameMap *map)
{
	char		old_dir[MAXPGPATH * 3];
	char		new_dir[MAXPGPATH * 3];
	DIR		   *dir;
	struct dirent *ent;
	struct stat statbuf;
	int			n_xfer = 0;

	snprintf(old_dir, sizeof(old_dir), "%s%s/%u/%u_pax",
			 map->old_tablespace,
			 map->old_tablespace_suffix,
			 map->old_db_oid,
			 map->old_relfilenode);
	snprintf(new_dir, sizeof(new_dir), "%s%s/%u/%u_pax",
			 map->new_tablespace,
			 map->new_tablespace_suffix,
			 map->new_db_oid,
			 map->new_relfilenode);

	pg_log(PG_STATUS, "%s", old_dir);

	/*
	 * Create the target directory.  EEXIST is OK — the PAX runtime may
	 * have pre-created it during a previous step, or this may be a retry.
	 */
	if (mkdir(new_dir, pg_dir_create_mode) != 0 && errno != EEXIST)
		pg_fatal("could not create PAX directory \"%s\": %s\n",
				 new_dir, strerror(errno));

	/*
	 * Source directory may legitimately be absent (QD, or empty PAX
	 * table that has never been written).  Treat as zero-file transfer.
	 */
	if (stat(old_dir, &statbuf) != 0)
	{
		if (errno == ENOENT)
		{
			pg_log(PG_VERBOSE,
				   "PAX source directory \"%s\" does not exist; "
				   "creating empty target directory (no data to transfer)\n",
				   old_dir);
			return;
		}
		pg_fatal("could not stat PAX directory \"%s\": %s\n",
				 old_dir, strerror(errno));
	}

	if (!S_ISDIR(statbuf.st_mode))
		pg_fatal("expected PAX path \"%s\" to be a directory, got non-directory\n",
				 old_dir);

	dir = opendir(old_dir);
	if (dir == NULL)
		pg_fatal("could not open PAX directory \"%s\": %s\n",
				 old_dir, strerror(errno));

	while ((errno = 0, (ent = readdir(dir))) != NULL)
	{
		char		old_file[MAXPGPATH * 3];
		char		new_file[MAXPGPATH * 3];
		struct stat ent_statbuf;

		if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
			continue;

		snprintf(old_file, sizeof(old_file), "%s/%s", old_dir, ent->d_name);
		snprintf(new_file, sizeof(new_file), "%s/%s", new_dir, ent->d_name);

		if (lstat(old_file, &ent_statbuf) != 0)
			pg_fatal("could not stat PAX entry \"%s\": %s\n",
					 old_file, strerror(errno));

		if (!S_ISREG(ent_statbuf.st_mode))
		{
			pg_log(PG_WARNING,
				   "skipping non-regular PAX entry \"%s\" "
				   "(PAX layout is flat; nested entries not transferred)\n",
				   old_file);
			continue;
		}

		/* If a previous run already linked it, undo before retrying. */
		unlink(new_file);

		switch (user_opts.transfer_mode)
		{
			case TRANSFER_MODE_CLONE:
				pg_log(PG_VERBOSE, "cloning PAX file \"%s\" to \"%s\"\n",
					   old_file, new_file);
				cloneFile(old_file, new_file, map->nspname, map->relname);
				break;
			case TRANSFER_MODE_COPY:
				pg_log(PG_VERBOSE, "copying PAX file \"%s\" to \"%s\"\n",
					   old_file, new_file);
				copyFile(old_file, new_file, map->nspname, map->relname);
				break;
			case TRANSFER_MODE_LINK:
				pg_log(PG_VERBOSE, "linking PAX file \"%s\" to \"%s\"\n",
					   old_file, new_file);
				linkFile(old_file, new_file, map->nspname, map->relname);
				break;
		}
		n_xfer++;
	}
	if (errno != 0)
		pg_fatal("error while reading PAX directory \"%s\": %s\n",
				 old_dir, strerror(errno));

	if (closedir(dir) != 0)
		pg_fatal("error while closing PAX directory \"%s\": %s\n",
				 old_dir, strerror(errno));

	pg_log(PG_VERBOSE,
		   "transferred %d PAX file(s) from \"%s\" to \"%s\"\n",
		   n_xfer, old_dir, new_dir);
}

/*
 * restore_pax_aux_table
 *
 * Repopulate one PAX table's auxiliary catalog state on the new cluster.
 * Dispatches on rel->pax->backend; both backends additionally restore
 * pg_pax_fastsequence which is shared.
 *
 *   CATALOG: TRUNCATE pg_pax_blocks_<reloid>; INSERT every captured row.
 *   MANIFEST: TRUNCATE pg_manifest_<reloid>; INSERT the captured path row.
 *             (The JSON file the path references rides on the directory
 *             hard-link done by transfer_pax_relfilenode_dir.)
 *
 * On QD both are effectively no-ops (no blocks / no path captured —
 * PAX data lives on segments) but the fastsequence row may still need
 * to be normalised.  On segments this is the real workhorse.
 *
 * Mirrors restore_aosegment_table() in aotable.c — same TRUNCATE +
 * row-by-row INSERT model, same PQescapeLiteral usage for text fields.
 */
static void
restore_pax_aux_table(PGconn *conn, RelInfo *rel)
{
	PaxRelState *st = rel->pax;
	PQExpBuffer query;
	int			i;

	if (st == NULL || st->backend == PAX_BACKEND_NONE)
		return;

	query = createPQExpBuffer();

	if (st->backend == PAX_BACKEND_CATALOG)
	{
		/*
		 * TRUNCATE the aux table.  Defensive against anything the QD→segment
		 * data-dir physical copy may have brought along; idempotent across
		 * reruns.
		 */
		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "TRUNCATE pg_ext_aux.pg_pax_blocks_%u",
						  rel->reloid);
		PQclear(executeQueryOrDie(conn, "%s", query->data));

		for (i = 0; i < st->nblocks; i++)
		{
			PaxBlockInfo *b = &st->blocks[i];
			char	   *stats_lit;
			char	   *visimap_lit = NULL;

			/*
			 * ptstatistics was captured as a hex string of the underlying
			 * varlena (paxauxstats::bytea); restore via decode + cast.
			 * Both casts are WITHOUT FUNCTION (design doc §13.2.1).
			 */
			stats_lit = PQescapeLiteral(conn, b->ptstatistics,
										strlen(b->ptstatistics));
			if (stats_lit == NULL)
				pg_log(PG_FATAL,
					   "%s: out of memory escaping ptstatistics hex\n",
					   os_info.progname);

			if (b->ptvisimapname != NULL)
			{
				visimap_lit = PQescapeLiteral(conn, b->ptvisimapname,
											  strlen(b->ptvisimapname));
				if (visimap_lit == NULL)
					pg_log(PG_FATAL,
						   "%s: out of memory escaping ptvisimapname\n",
						   os_info.progname);
			}

			resetPQExpBuffer(query);
			appendPQExpBuffer(query,
							  "INSERT INTO pg_ext_aux.pg_pax_blocks_%u "
							  "(ptblockname, pttupcount, ptblocksize, "
							  " ptstatistics, "
							  " ptvisimapname, ptexistexttoast, ptisclustered) "
							  "VALUES (%d, %d, %d, "
							  "        decode(%s, 'hex')::pg_ext_aux.paxauxstats, "
							  "        %s, %s, %s)",
							  rel->reloid,
							  b->ptblockname,
							  b->pttupcount,
							  b->ptblocksize,
							  stats_lit,
							  visimap_lit ? visimap_lit : "NULL",
							  b->ptexistexttoast ? "true" : "false",
							  b->ptisclustered ? "true" : "false");

			PQclear(executeQueryOrDie(conn, "%s", query->data));

			PQfreemem(stats_lit);
			if (visimap_lit)
				PQfreemem(visimap_lit);
		}
	}
	else if (st->backend == PAX_BACKEND_MANIFEST)
	{
		/*
		 * Manifest backend.  Single-column aux table holds one TEXT row
		 * pointing at the JSON manifest file inside the relation's
		 * <relfilenode>_pax/ directory.  The file itself was copied by
		 * transfer_pax_relfilenode_dir as part of the directory walk —
		 * we just rebuild the catalog row that points to it.
		 *
		 * On QD or for tables that were never populated, paxmanifest.path
		 * may be NULL, in which case we leave the table empty (it stays
		 * exactly as CREATE TABLE left it on the new cluster).
		 */
		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "TRUNCATE pg_ext_aux.pg_manifest_%u",
						  rel->reloid);
		PQclear(executeQueryOrDie(conn, "%s", query->data));

		if (st->manifest.path != NULL)
		{
			char	   *path_lit;

			path_lit = PQescapeLiteral(conn, st->manifest.path,
									   strlen(st->manifest.path));
			if (path_lit == NULL)
				pg_log(PG_FATAL,
					   "%s: out of memory escaping manifest path\n",
					   os_info.progname);

			resetPQExpBuffer(query);
			appendPQExpBuffer(query,
							  "INSERT INTO pg_ext_aux.pg_manifest_%u (path) "
							  "VALUES (%s)",
							  rel->reloid, path_lit);
			PQclear(executeQueryOrDie(conn, "%s", query->data));
			PQfreemem(path_lit);
		}
	}

	/*
	 * Restore fastsequence.  pg_pax_fastsequence has a non-unique index
	 * on (objid) so we can't use ON CONFLICT.  CREATE TABLE on the target
	 * already inserted a row for this objid with seq=0 via
	 * CPaxInitializeFastSequenceEntry, so DELETE + INSERT is the
	 * idempotent pattern shared by both backends.
	 */
	if (st->has_fastseq)
	{
		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "DELETE FROM pg_ext_aux.pg_pax_fastsequence "
						  "WHERE  objid = %u::pg_catalog.oid",
						  rel->reloid);
		PQclear(executeQueryOrDie(conn, "%s", query->data));

		resetPQExpBuffer(query);
		appendPQExpBuffer(query,
						  "INSERT INTO pg_ext_aux.pg_pax_fastsequence "
						  "(objid, seq) "
						  "VALUES (%u, %d)",
						  rel->reloid, st->fastseq);
		PQclear(executeQueryOrDie(conn, "%s", query->data));
	}

	destroyPQExpBuffer(query);

	if (st->backend == PAX_BACKEND_CATALOG)
		pg_log(PG_VERBOSE,
			   "PAX (catalog) aux restored for %s.%s (oid %u): "
			   "%d blocks, fastseq %d%s\n",
			   rel->nspname, rel->relname, rel->reloid,
			   st->nblocks, st->fastseq,
			   st->has_fastseq ? "" : " (skipped)");
	else
		pg_log(PG_VERBOSE,
			   "PAX (manifest) aux restored for %s.%s (oid %u): "
			   "path %s, fastseq %d%s\n",
			   rel->nspname, rel->relname, rel->reloid,
			   st->manifest.path ? st->manifest.path : "(none)",
			   st->fastseq,
			   st->has_fastseq ? "" : " (skipped)");
}

void
restore_pax_aux_tables(void)
{
	int			dbnum;

	prep_status("Restoring PAX auxiliary tables in new cluster");

	report_progress(&new_cluster, FIXUP, "Rebuilding PAX auxiliary tables");
	close_progress();

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *olddb = &old_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, olddb->db_name);
		int			relnum;

		/*
		 * The pg_pax_blocks_<oid> and pg_pax_fastsequence catalogs are in
		 * pg_ext_aux.  Modifying them requires the same allow_system_table_mods
		 * gate that the AO restore uses.
		 */
		PQclear(executeQueryOrDie(conn, "set allow_system_table_mods=true"));

		for (relnum = 0; relnum < olddb->rel_arr.nrels; relnum++)
		{
			RelInfo *rel = &olddb->rel_arr.rels[relnum];

			if (pax_rel_has_state(rel))
				restore_pax_aux_table(conn, rel);
		}

		PQfinish(conn);
	}

	check_ok();
}
