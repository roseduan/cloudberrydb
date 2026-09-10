/*
 * xmax_extquery_test.c
 *
 * Third entry point of the "cursor reader gang loses the writer's
 * subtransaction state" bug (see sql/xmax_reader_subxact.sql).
 *
 * A reader gang takes its local snapshot through
 * readSharedLocalSnapshot_forCursor() whenever DtxContextInfo.cursorContext is
 * set, and that flag is just portal->is_extended_query.  Besides DECLARE CURSOR
 * (portalcmds.c) and SPI cursors (spi.c), exec_bind_message() (postgres.c) sets
 * it for *every* extended-protocol Bind -- so a plain parameterized SELECT, the
 * shape JDBC / psycopg2 / ODBC send by default, reaches the same code.  The
 * isolation2 driver only speaks the simple query protocol, hence this helper.
 *
 * It runs the interleaving below and leaves the table behind for the test file
 * to inspect; it prints one fixed line so the expected output stays stable.
 *
 *   A: parameterized SELECT             warm the reader gang up while no
 *                                       subtransaction exists anywhere
 *   B: BEGIN; SAVEPOINT; DELETE id=1;
 *      RELEASE; INSERT id=1;            held open
 *   C: INSERT ...                       commits on every segment, so B's subxid
 *                                       lands inside (snap_xmin, snap_xmax)
 *   A: parameterized SELECT             the trigger
 *   B: COMMIT
 *
 * usage: xmax_extquery_test [conninfo]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libpq-fe.h"

static PGconn *
open_conn(const char *conninfo, const char *who)
{
	PGconn	   *conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "%s: connection failed: %s", who, PQerrorMessage(conn));
		exit(1);
	}
	return conn;
}

/* simple query protocol -- never reaches the cursor snapshot path */
static void
simple_query(PGconn *conn, const char *sql)
{
	PGresult   *res = PQexec(conn, sql);
	ExecStatusType status = PQresultStatus(res);

	if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
		fprintf(stderr, "\"%s\" failed: %s", sql, PQresultErrorMessage(res));
	PQclear(res);
}

/*
 * Extended query protocol: PQexecParams sends Parse/Bind/Describe/Execute, so
 * the segments run this on a reader gang with cursorContext = true.
 */
static void
extended_query(PGconn *conn, const char *sql, const char *param)
{
	const char *values[1];
	PGresult   *res;

	values[0] = param;
	res = PQexecParams(conn, sql, 1, NULL, values, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		fprintf(stderr, "\"%s\" failed: %s", sql, PQresultErrorMessage(res));
	PQclear(res);
}

int
main(int argc, char **argv)
{
	const char *conninfo = (argc > 1) ? argv[1] : "dbname = postgres";
	const char *select_sql = "select id, pad from xmax_extquery where id >= $1";
	PGconn	   *setup,
			   *a,
			   *b,
			   *c;

	setup = open_conn(conninfo, "setup");
	a = open_conn(conninfo, "reader");
	b = open_conn(conninfo, "deleter");
	c = open_conn(conninfo, "bumper");

	/* keep the "does not exist, skipping" notices out of the test output */
	simple_query(setup, "set client_min_messages = warning");
	simple_query(setup, "drop table if exists xmax_extquery");
	simple_query(setup, "drop table if exists xmax_extquery_bump");
	simple_query(setup, "create table xmax_extquery (id int, pad text) distributed by (id)");
	simple_query(setup, "create unique index xmax_extquery_uniq on xmax_extquery (id)");
	simple_query(setup, "create table xmax_extquery_bump (id int) distributed by (id)");
	simple_query(setup, "insert into xmax_extquery select i, 'orig-' || i from generate_series(1, 20) i");
	/* settle the xmin hint bits so the reader goes straight to the xmax path */
	simple_query(setup, "vacuum (freeze, analyze) xmax_extquery");

	/* step 0: warm the reader gang up while no subtransaction exists */
	extended_query(a, select_sql, "1");

	/* step 1: delete in a subtransaction, re-insert the key at the top level */
	simple_query(b, "begin");
	simple_query(b, "savepoint s1");
	simple_query(b, "delete from xmax_extquery where id = 1");
	simple_query(b, "release savepoint s1");
	simple_query(b, "insert into xmax_extquery values (1, 'new-1')");

	/* step 2: push every segment's latestCompletedXid past that subxid */
	simple_query(c, "insert into xmax_extquery_bump select generate_series(1, 300)");

	/* step 3: the trigger */
	extended_query(a, select_sql, "1");

	/* step 4: the delete becomes a fact */
	simple_query(b, "commit");

	printf("xmax_extquery_test: interleaving done\n");

	PQfinish(setup);
	PQfinish(a);
	PQfinish(b);
	PQfinish(c);
	return 0;
}
