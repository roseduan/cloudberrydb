/*-------------------------------------------------------------------------
 *
 * pg_iceberg_extensible.c
 *	  Plugin-side ExtensibleNode subtypes used to dispatch Iceberg-specific
 *	  payloads from QD to QE without adding kernel struct fields.
 *
 *	  This file holds:
 *	    - copy/equal/out/read callbacks for PgIcebergVacuumDispatchNode
 *	    - registration helper invoked from _PG_init
 *	    - QE-side handler invoked from datalake_ProcessUtility
 *	    - back-channel sender that ships per-QE rewrite results to QD via
 *	      a 'y' libpq message tagged PGExtraTypeVacuumPrivate (consumed in
 *	      pg_iceberg_relation_vacuum on the QD).
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_extensible.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/table.h"
#include "cdb/cdbvars.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/libpq-int.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/readfuncs.h"
#include "nodes/value.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "include/pg_iceberg_am.h"
#include "include/pg_iceberg_catalog_helper.h"
#include "include/pg_iceberg_extensible.h"


/* ----------------------------------------------------------------
 *	ExtensibleNode methods for PgIcebergVacuumDispatchNode
 * ----------------------------------------------------------------
 */

static void
copyPgIcebergVacuumDispatch(struct ExtensibleNode *enew,
							const struct ExtensibleNode *eold)
{
	PgIcebergVacuumDispatchNode *newnode = (PgIcebergVacuumDispatchNode *) enew;
	const PgIcebergVacuumDispatchNode *oldnode = (const PgIcebergVacuumDispatchNode *) eold;

	newnode->relId = oldnode->relId;
	newnode->tasks = (List *) copyObject(oldnode->tasks);
}

static bool
equalPgIcebergVacuumDispatch(const struct ExtensibleNode *a,
							 const struct ExtensibleNode *b)
{
	const PgIcebergVacuumDispatchNode *na = (const PgIcebergVacuumDispatchNode *) a;
	const PgIcebergVacuumDispatchNode *nb = (const PgIcebergVacuumDispatchNode *) b;

	return na->relId == nb->relId && equal(na->tasks, nb->tasks);
}

static void
outPgIcebergVacuumDispatch(struct StringInfoData *str,
						   const struct ExtensibleNode *enode)
{
	const PgIcebergVacuumDispatchNode *node = (const PgIcebergVacuumDispatchNode *) enode;
	char	   *tasks_str;

	appendStringInfo(str, " :relId %u", node->relId);

	appendStringInfoString(str, " :tasks ");
	tasks_str = nodeToString(node->tasks);
	appendStringInfoString(str, tasks_str);
	pfree(tasks_str);
}

static void
readPgIcebergVacuumDispatch(struct ExtensibleNode *enode)
{
	PgIcebergVacuumDispatchNode *local_node = (PgIcebergVacuumDispatchNode *) enode;
	const char *token;
	int			length;

	token = pg_strtok(&length);		/* skip :relId */
	token = pg_strtok(&length);
	local_node->relId = atooid(token);

	token = pg_strtok(&length);		/* skip :tasks */
	(void) token;
	local_node->tasks = (List *) nodeRead(NULL, 0);
}

static const ExtensibleNodeMethods pg_iceberg_vacuum_dispatch_methods = {
	.extnodename = PG_ICEBERG_VACUUM_DISPATCH_NODE,
	.node_size = sizeof(PgIcebergVacuumDispatchNode),
	.nodeCopy = copyPgIcebergVacuumDispatch,
	.nodeEqual = equalPgIcebergVacuumDispatch,
	.nodeOut = outPgIcebergVacuumDispatch,
	.nodeRead = readPgIcebergVacuumDispatch,
};


/* ----------------------------------------------------------------
 *	ExtensibleNode methods for PgIcebergAnalyzeDispatchNode
 * ----------------------------------------------------------------
 */

static void
copyPgIcebergAnalyzeDispatch(struct ExtensibleNode *enew,
							 const struct ExtensibleNode *eold)
{
	PgIcebergAnalyzeDispatchNode *newnode = (PgIcebergAnalyzeDispatchNode *) enew;
	const PgIcebergAnalyzeDispatchNode *oldnode = (const PgIcebergAnalyzeDispatchNode *) eold;

	newnode->relids = (List *) copyObject(oldnode->relids);
	newnode->fragments = (List *) copyObject(oldnode->fragments);
}

static bool
equalPgIcebergAnalyzeDispatch(const struct ExtensibleNode *a,
							  const struct ExtensibleNode *b)
{
	const PgIcebergAnalyzeDispatchNode *na = (const PgIcebergAnalyzeDispatchNode *) a;
	const PgIcebergAnalyzeDispatchNode *nb = (const PgIcebergAnalyzeDispatchNode *) b;

	return equal(na->relids, nb->relids) && equal(na->fragments, nb->fragments);
}

static void
outPgIcebergAnalyzeDispatch(struct StringInfoData *str,
							const struct ExtensibleNode *enode)
{
	const PgIcebergAnalyzeDispatchNode *node = (const PgIcebergAnalyzeDispatchNode *) enode;
	char	   *tmp;

	appendStringInfoString(str, " :relids ");
	tmp = nodeToString(node->relids);
	appendStringInfoString(str, tmp);
	pfree(tmp);

	appendStringInfoString(str, " :fragments ");
	tmp = nodeToString(node->fragments);
	appendStringInfoString(str, tmp);
	pfree(tmp);
}

static void
readPgIcebergAnalyzeDispatch(struct ExtensibleNode *enode)
{
	PgIcebergAnalyzeDispatchNode *local_node = (PgIcebergAnalyzeDispatchNode *) enode;
	const char *token;
	int			length;

	token = pg_strtok(&length);		/* skip :relids */
	(void) token;
	local_node->relids = (List *) nodeRead(NULL, 0);

	token = pg_strtok(&length);		/* skip :fragments */
	(void) token;
	local_node->fragments = (List *) nodeRead(NULL, 0);
}

static const ExtensibleNodeMethods pg_iceberg_analyze_dispatch_methods = {
	.extnodename = PG_ICEBERG_ANALYZE_DISPATCH_NODE,
	.node_size = sizeof(PgIcebergAnalyzeDispatchNode),
	.nodeCopy = copyPgIcebergAnalyzeDispatch,
	.nodeEqual = equalPgIcebergAnalyzeDispatch,
	.nodeOut = outPgIcebergAnalyzeDispatch,
	.nodeRead = readPgIcebergAnalyzeDispatch,
};


/* ----------------------------------------------------------------
 *	Registration
 * ----------------------------------------------------------------
 */

void
pg_iceberg_register_extensible_nodes(void)
{
	RegisterExtensibleNodeMethods(&pg_iceberg_vacuum_dispatch_methods);
	RegisterExtensibleNodeMethods(&pg_iceberg_analyze_dispatch_methods);
}


/* ----------------------------------------------------------------
 *	QE-side back-channel: ship per-QE rewrite result to QD
 *
 *	Plugin-side replacement for the (now removed) kernel helper
 *	vac_send_private_to_qd().  The wire format is a 'y' libpq message
 *	carrying a PGExtraTypeVacuumPrivate payload, exactly what the QD
 *	side (pg_iceberg_relation_vacuum) reads from CdbPgResults.
 * ----------------------------------------------------------------
 */

static void
pg_iceberg_send_result_to_qd(List *private_results)
{
	StringInfoData buf;
	char	   *serialized;
	int			data_len;

	if (private_results == NIL)
		return;

	serialized = nodeToString(private_results);
	data_len = strlen(serialized) + 1;

	pq_beginmessage(&buf, 'y');
	pq_sendstring(&buf, "VACUUM");
	pq_sendbyte(&buf, true);		/* mark result ready */
	pq_sendint(&buf, PGExtraTypeVacuumPrivate, sizeof(PGExtraType));
	pq_sendint(&buf, data_len, sizeof(int));
	pq_sendbytes(&buf, serialized, data_len);
	pq_endmessage(&buf);

	pfree(serialized);
}


/* ----------------------------------------------------------------
 *	QE-side dispatcher: invoked from datalake_ProcessUtility when
 *	the utility statement is one of our ExtensibleNode payloads.
 *
 *	Returns true if the node was recognised and handled (caller must
 *	NOT fall through to standard_ProcessUtility), false otherwise.
 * ----------------------------------------------------------------
 */

bool
pg_iceberg_handle_extensible_utility(Node *parsetree)
{
	ExtensibleNode *enode;

	if (parsetree == NULL || !IsA(parsetree, ExtensibleNode))
		return false;

	enode = (ExtensibleNode *) parsetree;

	if (strcmp(enode->extnodename, PG_ICEBERG_VACUUM_DISPATCH_NODE) == 0)
	{
		PgIcebergVacuumDispatchNode *stmt = (PgIcebergVacuumDispatchNode *) enode;
		Relation	rel;
		char	   *result_json;
		List	   *results = NIL;

		Assert(OidIsValid(stmt->relId));

		rel = relation_open(stmt->relId, ShareUpdateExclusiveLock);

		result_json = pg_iceberg_execute_rewrite(rel, stmt->tasks);
		if (result_json != NULL)
			results = lappend(results, makeString(result_json));

		relation_close(rel, NoLock);

		pg_iceberg_send_result_to_qd(results);

		return true;
	}

	if (strcmp(enode->extnodename, PG_ICEBERG_ANALYZE_DISPATCH_NODE) == 0)
	{
		PgIcebergAnalyzeDispatchNode *stmt = (PgIcebergAnalyzeDispatchNode *) enode;
		ListCell   *lcr;
		ListCell   *lcf;

		/*
		 * A fresh dispatch defines the complete relid set of the in-flight
		 * ANALYZE; drop whatever an earlier (possibly aborted) ANALYZE left
		 * behind so a stale fragment list can never be consumed.
		 */
		pg_iceberg_reset_analyze_fragments();

		forboth(lcr, stmt->relids, lcf, stmt->fragments)
			pg_iceberg_stash_analyze_fragments(lfirst_oid(lcr),
											   strVal(lfirst(lcf)));

		return true;
	}

	return false;
}
