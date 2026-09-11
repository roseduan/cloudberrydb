/*
 *	frozenxids_gp.c
 *
 *	functions for restoring frozenxid from old cluster
 *
 *	Copyright (c) 2016-Present, Pivotal Software Inc
 */
#include "postgres_fe.h"

#include "pg_upgrade_greenplum.h"

/*
 * Subquery yielding the access methods that keep no transaction information
 * in the tuple header: append-optimized row, append-optimized column and
 * PAX.  Relations using one of them must keep pg_class.relfrozenxid =
 * InvalidTransactionId; the table AM establishes that at creation (see
 * appendonly_relation_set_new_filenode), the relcache asserts it in
 * RelationInitTableAccessMethod() and cluster.c::swap_relation_files()
 * restores it after a rewrite.
 *
 * The set is keyed off the handler rather than off the access method OID,
 * because an access method created with one of the canonical handlers (see
 * create_am_gp.sql, e.g. "create access method ao_row_testam type table
 * handler ao_row_tableam_handler") is dispatched into the AO/PAX code and
 * bound by the same invariant, while its relam is the derived method's own
 * OID.  RelationStorageIsAO() is handler based for the same reason.  PAX is
 * matched by handler name because its pg_am row is created by the extension,
 * not by initdb, so the query also works where PAX is not installed.
 */
#define AMS_WITHOUT_TUPLE_XIDS \
	"(SELECT am.oid FROM pg_catalog.pg_am am "				\
	"JOIN pg_catalog.pg_proc p ON p.oid = am.amhandler "			\
	"WHERE am.amtype = 't' AND p.proname IN "				\
	"('ao_row_tableam_handler', 'ao_column_tableam_handler', "		\
	"'pax_tableam_handler'))"


/*
 * Segment database contains data, and the tuples should not have
 * any entry with a xmin > relfrozenxid for the table in pg_class.
 * Instead of vacuum freezing the entire data, update the relfrozenxid
 * of the relation in pg_class with the datfrozenxid from the corresponding
 * database in the old cluster. This ensures that the xmin is not > relfrozenxid
 * for any of the tuple.
 * Coordinator contains data entries in GPDB catalog tables like
 * gp_fastsequence, pg_aoseg, pg_aocsseg, pg_aoblkdir, and pg_aovisimap that
 * also need relfrozenxid correction. Also, update the new database with the
 * datfrozenxid from the old cluster as that indicates the lowest xid available.
 *
 * In GPDB5 datminmxid does not exist, so use the chkpnt_nxtmulti to update
 * the value in the GPDB6 cluster.
 */
void
update_db_xids(void)
{
	int			dbnum;
	PGconn	   *conn;
	PGconn	   *conn_template1;

	prep_status("Updating xid's in new cluster databases");

	if (user_opts.check)
	{
		check_ok();
		return;
	}

	/*
	 * Used to flip datallowconn for databases that reject connections; see
	 * the loop below.
	 */
	conn_template1 = connectToServer(&new_cluster, "template1");
	PQclear(executeQueryOrDie(conn_template1,
							  "set allow_system_table_mods=true"));

	/*
	 * Iterate over every database, not only the connectable ones.  A
	 * segment's data directory is a copy of the upgraded coordinator's, so
	 * template0 arrives carrying the coordinator's datfrozenxid and
	 * relfrozenxids, which sit far above the segment's own NextXID once
	 * copy_xact_xlog_xid() has reset it.  Every database created from
	 * template0 after the upgrade inherits those values on the segment, and
	 * the first VACUUM of its catalog fails with "found xmin N from before
	 * relfrozenxid M".  get_db_infos() keeps such databases at the tail of
	 * dbarr precisely so that this loop can reach them.
	 */
	for (dbnum = 0; dbnum < old_cluster.dbarr.total_ndbs; dbnum++)
	{
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[dbnum];
		char *escaped_datname = pg_malloc(strlen(active_db->db_name) * 2 + 1);
		uint32 datfrozenxid = active_db->datfrozenxid;
		uint32 datminmxid = active_db->datminmxid;

		/*
		 * Escape against the connection the literal is sent on:
		 * PQescapeString() has no connection context, so it can neither
		 * see the server's encoding - mis-decoding a multibyte datname
		 * whose trailing byte looks like a quote or a backslash - nor its
		 * standard_conforming_strings setting.  freeze_master_data()
		 * escapes the same datallowconn flip this way, see
		 * controldata_gp.c.
		 */
		PQescapeStringConn(conn_template1, escaped_datname,
						   active_db->db_name, strlen(active_db->db_name),
						   NULL);

		/*
		 * template0 and friends have to be opened for the duration, the same
		 * way freeze_master_data() reaches them.
		 */
		if (!active_db->datallowconn)
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET datallowconn = true "
									  "WHERE datname = '%s'",
									  escaped_datname));

		conn = connectToServer(&new_cluster, active_db->db_name);

		PQclear(executeQueryOrDie(conn,
								  "set allow_system_table_mods=true"));

		/*
		 * This one runs on the per-database connection, whose client
		 * encoding is the database's own and need not match template1's.
		 * We are connected to exactly the database being updated, so
		 * address its row by current_database() instead of interpolating
		 * a literal escaped for a different connection.
		 */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE pg_catalog.pg_database "
								  "SET datfrozenxid = '%u', datminmxid = '%u' "
								  "WHERE datname = pg_catalog.current_database()",
								  datfrozenxid,
								  (GET_MAJOR_VERSION(old_cluster.major_version) <= 803) ?
								  old_cluster.controldata.chkpnt_nxtmulti : datminmxid));

		/*
		 * include heap, materialized view, temporary/toast tables and
		 * exclude append-optimized and PAX storage, which has to keep
		 * relfrozenxid Invalid (see AMS_WITHOUT_TUPLE_XIDS above).
		 *
		 * This used to exclude them with "AND NOT relfrozenxid = 0", a
		 * reverse storage-type proxy that holds only for as long as no code
		 * path ever leaves a non-zero value in an AO or PAX pg_class row.
		 * When one does, the UPDATE below preserved the violation and the
		 * next backend to open the relation tripped the relcache assertion.
		 * The proxy also skipped heap relations whose relfrozenxid was
		 * legitimately Invalid.
		 *
		 * Notes: if we ever backport this to Greenplum 5X, remove 'm' first
		 * and then replace 'M' with 'm', because 'm' used to be RELKIND
		 * visimap in 4.3/5X, not matview
		 */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relfrozenxid = '%u' "
								  "WHERE	relkind IN ('r', 'm', 't', 'o', 'b', 'M') "
								  "AND	relam NOT IN " AMS_WITHOUT_TUPLE_XIDS,
								  datfrozenxid));

		/*
		 * Conversely, repair AO and PAX relations that reach us with a stale
		 * non-Invalid relfrozenxid.  A source cluster predating the fix in
		 * cluster.c::swap_relation_files() carries such rows after a VACUUM
		 * FULL / CLUSTER / SET ACCESS METHOD rebuild, and the relcache
		 * assertion fires as soon as the relation is opened - for instance in
		 * pg_upgrade's own post-restore VACUUM FREEZE.
		 */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relfrozenxid = '0' "
								  "WHERE	relfrozenxid <> 0 "
								  "AND	relam IN " AMS_WITHOUT_TUPLE_XIDS));

		/*
		 * update heap, materialized view and TOAST/temporary tables.  Same
		 * relation set as the relfrozenxid update above: append-optimized and
		 * PAX relations keep relminmxid = InvalidMultiXactId too.
		 */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relminmxid = '%u' "
								  "WHERE	relkind IN ('r', 'm', 't', 'o', 'b', 'M') "
								  "AND	relam NOT IN " AMS_WITHOUT_TUPLE_XIDS,
								  (GET_MAJOR_VERSION(old_cluster.major_version) <= 803) ?
								  old_cluster.controldata.chkpnt_nxtmulti : datminmxid));

		/* Repair stale values the same way, see the relfrozenxid reset above. */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relminmxid = '0' "
								  "WHERE	relminmxid <> 0 "
								  "AND	relam IN " AMS_WITHOUT_TUPLE_XIDS));

		PQfinish(conn);

		if (!active_db->datallowconn)
			PQclear(executeQueryOrDie(conn_template1,
									  "UPDATE pg_catalog.pg_database "
									  "SET datallowconn = false "
									  "WHERE datname = '%s'",
									  escaped_datname));

		pg_free(escaped_datname);
	}

	PQfinish(conn_template1);

	check_ok();

}
