/*
 *	version_gp.c
 *
 *	Greenplum version-specific routines for upgrades
 *
 *	Copyright (c) 2016-Present VMware, Inc. or its affiliates
 *	contrib/pg_upgrade/version_gp.c
 */
#include "postgres_fe.h"

#include "pg_upgrade_greenplum.h"

#include "access/transam.h"

#define NUMERIC_ALLOC 100

/*
 *	check_hash_partition_usage()
 *	8.3 -> 8.4
 *
 *	Hash partitioning was never officially supported in GPDB5 and was removed
 *	in GPDB6, but better check just in case someone has found the hidden GUC
 *	and used them anyway.
 *
 *	The hash algorithm was changed in 8.4, so upgrading is impossible anyway.
 *	This is basically the same problem as with hash indexes in PostgreSQL.
 */
void
check_hash_partition_usage(void)
{
	int				dbnum;
	FILE		   *script = NULL;
	bool			found = false;
	char			output_path[MAXPGPATH];

	/* Merge with PostgreSQL v11 introduced hash partitioning again. */
	if (GET_MAJOR_VERSION(old_cluster.major_version) >= 1100)
		return;

	prep_status("Checking for hash partitioned tables");

	snprintf(output_path, sizeof(output_path), "%s/%s",
			 log_opts.basedir, "hash_partitioned_tables.txt");

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname;
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&old_cluster, active_db->db_name);

		res = executeQueryOrDie(conn,
								"SELECT n.nspname, c.relname "
								"FROM pg_catalog.pg_partition p, pg_catalog.pg_class c, pg_catalog.pg_namespace n "
								"WHERE p.parrelid = c.oid AND c.relnamespace = n.oid "
								"AND parkind = 'h'");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen(output_path, "w")) == NULL)
				pg_log(PG_FATAL, "Could not create necessary file:  %s\n", output_path);
			if (!db_used)
			{
				fprintf(script, "Database:  %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (found)
	{
		fclose(script);
		pg_log(PG_REPORT, "fatal\n");
		gp_fatal_log(
			   "| Your installation contains hash partitioned tables.\n"
			   "| Upgrading hash partitioned tables is not supported,\n"
			   "| so this cluster cannot currently be upgraded.  You\n"
			   "| can remove the problem tables and restart the\n"
			   "| migration.  A list of the problem tables is in the\n"
			   "| file:\n"
			   "| \t%s\n\n", output_path);
	}
	else
		check_ok();
}

/*
 * old_GPDB5_check_for_unsupported_distribution_key_data_types()
 *
 *	abstime, reltime, tinterval, money and anyarray datatypes don't have hash opclasses
 *	in GPDB 6, so they are not supported as distribution keys anymore.
 */
void
old_GPDB5_check_for_unsupported_distribution_key_data_types(void)
{
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for abstime, reltime, tinterval user data types");

	snprintf(output_path, sizeof(output_path), "tables_using_abstime_reltime_tinterval.txt");

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname,
					i_attname;
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&old_cluster, active_db->db_name);

		res = executeQueryOrDie(conn,
								"SELECT nspname, relname, attname "
								"FROM   pg_catalog.pg_class c, "
								"       pg_catalog.pg_namespace n, "
								"       pg_catalog.pg_attribute a, "
								"       gp_distribution_policy p "
								"WHERE  c.oid = a.attrelid AND "
								"       c.oid = p.localoid AND "
								"       a.atttypid in ('pg_catalog.abstime'::regtype, "
								"                      'pg_catalog.reltime'::regtype, "
								"                      'pg_catalog.tinterval'::regtype, "
								"                      'pg_catalog.money'::regtype, "
								"                      'pg_catalog.anyarray'::regtype) AND "
								"       attnum = any (p.attrnums) AND "
								"       c.relnamespace = n.oid AND "
		/* exclude possible orphaned temp tables */
								"  		n.nspname !~ '^pg_temp_'");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		i_attname = PQfnumber(res, "attname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, strerror(errno));
			if (!db_used)
			{
				fprintf(script, "In database: %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname),
					PQgetvalue(res, rowno, i_attname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (script)
		fclose(script);

	if (found)
	{
		pg_log(PG_REPORT, "fatal\n");
		pg_fatal("Your installation contains a user table, that uses a 'abstime',\n"
				 "'reltime', 'tinterval', 'money' or 'anyarray' type as a distribution key column. Using\n"
				 "these datatypes as distribution keys is no longer supported. You can use\n"
				 "ALTER TABLE ... SET DISTRIBUTED RANDOMLY to change the distribution keys,\n"
				 "and restart the upgrade.  A list of the problem columns is in the file:\n"
				 "    %s\n\n", output_path);
	}
	else
		check_ok();
}

/*
 * old_GPDB6_check_for_unsupported_sha256_password_hashes()
 *
 *  Support for password_hash_algorithm='sha-256' was removed in GPDB 7. Check if
 *  any roles have SHA-256 password hashes.
 */
void
old_GPDB6_check_for_unsupported_sha256_password_hashes(void)
{
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for SHA-256 hashed passwords");

	snprintf(output_path, sizeof(output_path), "%s/%s",
			 log_opts.basedir, "roles_using_sha256_passwords.txt");

	/* It's enough to check this in one database, pg_authid is a shared catalog. */
	{
		PGresult   *res;
		int			ntups;
		int			rowno;
		int			i_rolname;
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[0];
		PGconn	   *conn = connectToServer(&old_cluster, active_db->db_name);

		res = executeQueryOrDie(conn,
								"SELECT rolname FROM pg_catalog.pg_authid "
								"WHERE rolpassword LIKE 'sha256%%'");

		ntups = PQntuples(res);
		i_rolname = PQfnumber(res, "rolname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, strerror(errno));
			fprintf(script, "  %s\n",
					PQgetvalue(res, rowno, i_rolname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (script)
		fclose(script);

	if (found)
	{
		pg_log(PG_REPORT, "fatal\n");
		gp_fatal_log(
				 "| Your installation contains roles with SHA-256 hashed passwords. Using\n"
				 "| SHA-256 for password hashes is no longer supported. You can use\n"
				 "| ALTER ROLE <role name> WITH PASSWORD NULL as superuser to clear passwords,\n"
				 "| and restart the upgrade.  A list of the problem roles is in the file:\n"
				 "|    %s\n\n", output_path);
	}
	else
		check_ok();
}

/*
 * new_gpdb_invalidate_bitmap_indexes()
 *
 * pg_upgrade does not migrate the contents of a bitmap index: the index
 * relfilenode is transferred, but the bitmap data lives in the auxiliary
 * pg_bitmapindex.pg_bm_<oid> heap, which is recreated empty by the schema
 * restore.  Mark every bitmap index invalid so nothing can plan against an
 * empty index, then new_reindex_bitmap_indexes() rebuilds them and makes
 * them valid again.  Running in this order means an interrupted upgrade
 * leaves the indexes unusable rather than silently wrong.
 *
 * Note the access method is looked up by name.  It used to be a hardcoded
 * relam = 3013, which is the bitmap AM's OID in Greenplum but not in
 * Cloudberry (7013), so the UPDATE silently matched no rows and left every
 * bitmap index marked valid with an empty auxiliary heap — index scans then
 * returned no rows at all.
 */
void
new_gpdb_invalidate_bitmap_indexes(void)
{
	int			dbnum;

	prep_status("Invalidating bitmap indexes in new cluster");

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *olddb = &new_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, olddb->db_name);

		/*
		 * GPDB doesn't allow hacking the catalogs without setting
		 * allow_system_table_mods first.
		 */
		PQclear(executeQueryOrDie(conn, "set allow_system_table_mods=true"));

		/*
		 * check mode doesn't do much interesting for this but at least
		 * we'll know we are allowed to change allow_system_table_mods
		 * which is required
		 */
		if (!user_opts.check)
		{
			PQclear(executeQueryOrDie(conn,
									  "UPDATE pg_index SET indisvalid = false "
									  "  FROM pg_class c "
									  " WHERE c.oid = indexrelid AND "
									  "       indexrelid >= %u AND "
									  "       c.relam = (SELECT oid FROM pg_am "
									  "                   WHERE amname = 'bitmap');",
									  FirstNormalObjectId));
		}
		PQfinish(conn);
	}

	check_ok();
}

/*
 * BRIN_AOCO_NEW_FORMAT_CATVER is defined in pg_upgrade_greenplum.h.
 * It marks the catalog version at which the on-disk format of BRIN indexes
 * on AO/CO tables changed from pre-allocated UPPER pages to singly-linked
 * revmap chains with AOChainInfo in the metapage.  See commit 0447cca1a73
 * ("Reimplement BRIN internals for AO/CO tables"), which bumped
 * CATALOG_VERSION_NO from 302502071 to 302502091.  Both pg_upgrade.c and
 * this file use the same threshold, so it lives in the shared header.
 */

/*
 * new_reindex_ao_brin_indexes()
 *
 * The on-disk format of BRIN indexes on AO/CO tables changed: the old format
 * used pre-allocated UPPER pages, while the new format uses singly-linked
 * revmap chains with AOChainInfo in the metapage.  Since pg_upgrade transfers
 * index files by copying relfilenodes, the old-format BRIN files are
 * incompatible with the new code.
 *
 * Rebuild these indexes inline so they work immediately after pg_upgrade
 * returns.  Historically pg_upgrade only marked them indisvalid=false and
 * relied on the operator to REINDEX afterwards, but that left a window
 * during which queries silently bypassed the indexes (and risked silent
 * incorrect results if anyone forced indisvalid=true without rebuilding).
 * Doing the REINDEX here makes the AO/CO BRIN upgrade transparent to the
 * user.
 *
 * Only AO/CO table BRIN indexes are affected; heap table BRIN indexes have
 * not changed format.
 *
 * Gate on the source cluster's catalog version: if the source already has
 * the new format, the index files are still valid and there is nothing to
 * do — same-version / minor-version upgrades skip this step entirely.
 */
void
new_reindex_ao_brin_indexes(void)
{
	int			dbnum;

	prep_status("Rebuilding BRIN indexes on AO/CO tables in new cluster");

	if (old_cluster.controldata.cat_ver >= BRIN_AOCO_NEW_FORMAT_CATVER)
	{
		report_status(PG_REPORT, "skipped (source already in new BRIN-on-AO/CO format)");
		return;
	}

	if (user_opts.check)
	{
		check_ok();
		return;
	}

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *newdb = &new_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, newdb->db_name);

		/*
		 * REINDEX every AO/CO BRIN index.  Doing this through a DO block
		 * lets us format() per-index commands using the catalog query
		 * results, which keeps the SELECT and the REINDEX visibility
		 * consistent within one statement.
		 */
		PQclear(executeQueryOrDie(conn,
								  "DO $reindexbrin$ "
								  "DECLARE r record; "
								  "BEGIN "
								  "  FOR r IN "
								  "    SELECT n.nspname, idx.relname "
								  "    FROM   pg_class      idx "
								  "    JOIN   pg_index      i      ON idx.oid = i.indexrelid "
								  "    JOIN   pg_class      tbl    ON tbl.oid = i.indrelid "
								  "    JOIN   pg_namespace  n      ON n.oid = idx.relnamespace "
								  "    JOIN   pg_am         idxam  ON idxam.oid = idx.relam "
								  "    JOIN   pg_am         tblam  ON tblam.oid = tbl.relam "
								  "    WHERE  idxam.amname = 'brin' "
								  "      AND  tblam.amname IN ('ao_row','ao_column') "
								  "      AND  idx.oid >= %u "
								  "  LOOP "
								  "    EXECUTE format('REINDEX INDEX %%I.%%I', r.nspname, r.relname); "
								  "  END LOOP; "
								  "END $reindexbrin$",
								  FirstNormalObjectId));
		PQfinish(conn);
	}

	check_ok();
}

/*
 * new_reindex_bitmap_indexes()
 *
 * Rebuild every bitmap index from the data now present in the new cluster,
 * the same way new_reindex_ao_brin_indexes() rebuilds AO/CO BRIN indexes.
 *
 * A bitmap index cannot survive relfilenode transfer: its payload lives in
 * the auxiliary pg_bitmapindex.pg_bm_<oid> heap and its index, which the
 * schema restore recreates empty.  Historically pg_upgrade only marked the
 * indexes indisvalid=false and left the REINDEX to the operator; that both
 * degraded query plans until someone noticed and — when the invalidation
 * itself did not take effect — produced silently wrong results.
 *
 * Only relkind 'i' is rebuilt.  Partitioned bitmap indexes (relkind 'I')
 * have no storage of their own; they become valid again once their leaves
 * have been rebuilt.
 *
 * Runs unconditionally: unlike the BRIN format change there is no source
 * catalog version at which bitmap contents survive the transfer.  On the
 * coordinator this rebuilds the (empty) coordinator-local indexes; each
 * segment rebuilds its own from local data.
 */
void
new_reindex_bitmap_indexes(void)
{
	int			dbnum;

	prep_status("Rebuilding bitmap indexes in new cluster");

	if (user_opts.check)
	{
		check_ok();
		return;
	}

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *newdb = &new_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, newdb->db_name);

		/*
		 * One DO block per database so the catalog scan and the REINDEXes it
		 * drives see the same snapshot, as in new_reindex_ao_brin_indexes().
		 */
		PQclear(executeQueryOrDie(conn,
								  "DO $reindexbitmap$ "
								  "DECLARE r record; "
								  "BEGIN "
								  "  FOR r IN "
								  "    SELECT n.nspname, idx.relname "
								  "    FROM   pg_class      idx "
								  "    JOIN   pg_index      i     ON idx.oid = i.indexrelid "
								  "    JOIN   pg_namespace  n     ON n.oid = idx.relnamespace "
								  "    JOIN   pg_am         idxam ON idxam.oid = idx.relam "
								  "    WHERE  idxam.amname = 'bitmap' "
								  "      AND  idx.relkind = 'i' "
								  "      AND  idx.oid >= %u "
								  "  LOOP "
								  "    EXECUTE format('REINDEX INDEX %%I.%%I', r.nspname, r.relname); "
								  "  END LOOP; "
								  "END $reindexbitmap$",
								  FirstNormalObjectId));
		PQfinish(conn);
	}

	check_ok();
}

/*
 * cleanup_segment_event_trigger_deps()
 *
 * When gpupgrade rsyncs the coordinator's upgraded data directory to segments,
 * it brings along pg_depend entries that reference pg_event_trigger.  Event
 * triggers exist only on the coordinator, so these rows are stale on segments
 * and can cause errors (e.g. "could not find tuple for event trigger").
 * Remove them.
 */
void
cleanup_segment_event_trigger_deps(void)
{
	int			dbnum;

	prep_status("Cleaning up stale event trigger dependencies on segment");

	if (user_opts.check)
	{
		check_ok();
		return;
	}

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *db = &new_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, db->db_name);

		PQclear(executeQueryOrDie(conn,
								  "set allow_system_table_mods=true"));

		PQclear(executeQueryOrDie(conn,
								  "DELETE FROM pg_catalog.pg_depend "
								  "WHERE classid = "
								  "'pg_catalog.pg_event_trigger'::regclass"));

		PQfinish(conn);
	}

	check_ok();
}
