/*-------------------------------------------------------------------------
 *
 * pg_iceberg_extensible.h
 *	  Plugin-side ExtensibleNode subtypes used to carry Iceberg AM payloads
 *	  across QD↔QE without touching kernel Node infrastructure.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_extensible.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ICEBERG_EXTENSIBLE_H
#define PG_ICEBERG_EXTENSIBLE_H

#include "postgres.h"
#include "nodes/extensible.h"
#include "nodes/pg_list.h"

/* extnodename used by ExtensibleNodeMethods registration. */
#define PG_ICEBERG_VACUUM_DISPATCH_NODE "PgIcebergVacuumDispatch"
#define PG_ICEBERG_ANALYZE_DISPATCH_NODE "PgIcebergAnalyzeDispatch"

/*
 * PgIcebergVacuumDispatchNode
 *
 * QD packs the Iceberg-specific rewrite task list into this node and ships
 * it to QEs via CdbDispatchUtilityStatement().  On the QE side, the node
 * is intercepted by datalake_ProcessUtility and dispatched to
 * pg_iceberg_handle_extensible_utility(), which opens the relation and
 * runs pg_iceberg_execute_rewrite().
 */
typedef struct PgIcebergVacuumDispatchNode
{
	ExtensibleNode	node;
	Oid				relId;		/* target relation */
	List		   *tasks;		/* AM private task list (formerly vacuum_private) */
} PgIcebergVacuumDispatchNode;

/*
 * PgIcebergAnalyzeDispatchNode
 *
 * QD-side ANALYZE pre-pass (datalake_ProcessUtility) packs every target
 * Iceberg relation's expanded fragment list into this node and ships it to
 * the QEs via CdbDispatchUtilityStatement() before falling through to
 * standard ANALYZE.  On the QE the node is intercepted by
 * pg_iceberg_handle_extensible_utility(), which stashes each relid's
 * fragment JSON into a backend-local cache that
 * pg_iceberg_acquire_sample_rows() later consumes (issue #352).
 *
 * The fragment payload must travel on the normal query channel: shipping it
 * through a synced GUC put it into the gang-connection startup packet, which
 * postmaster rejects beyond MAX_STARTUP_PACKET_LENGTH (64000) -- a large
 * table's fragment list (e.g. TPC-DS store_sales, ~136kB) made every gang
 * creation fail with "invalid length of startup packet".
 *
 * relids and fragments are parallel lists: relids is an OID list, fragments
 * holds one String node (raw fragment-list JSON) per relid.
 */
typedef struct PgIcebergAnalyzeDispatchNode
{
	ExtensibleNode	node;
	List		   *relids;		/* OID list of target relations */
	List		   *fragments;	/* parallel list of String (fragment JSON) */
} PgIcebergAnalyzeDispatchNode;

extern void pg_iceberg_register_extensible_nodes(void);
extern bool pg_iceberg_handle_extensible_utility(Node *parsetree);

#endif							/* PG_ICEBERG_EXTENSIBLE_H */
