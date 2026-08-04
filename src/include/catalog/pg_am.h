/*-------------------------------------------------------------------------
 *
 * pg_am.h
 *	  definition of the "access method" system catalog (pg_am)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_am.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AM_H
#define PG_AM_H

#include "catalog/genbki.h"
#include "catalog/pg_am_d.h"

/* GPDB: convenient macro for checking AO AMs */
#define IsAccessMethodAO(am_oid) \
	((am_oid) == AO_ROW_TABLE_AM_OID || (am_oid) == AO_COLUMN_TABLE_AM_OID)

/*
 * Cloudberry: PAX is a contrib-registered table AM with a fixed OID.  Its
 * pg_am row is created at extension-init time by
 * contrib/pax_storage/tools/gen_sql.c (INSERT INTO pg_am ... 7047), so unlike
 * the core AMs above the OID is not carried in pg_am.dat / pg_am_d.h.
 *
 * The contrib side defines its own macro PAX_TABLE_AM_OID in
 * contrib/pax_storage/src/cpp/comm/pax_rel.h.  Because that header and this
 * one are both pulled into the same translation unit (via the PAX cbdb_api.h
 * umbrella), core deliberately uses a *separate* symbol, EXT_PAX_TABLE_AM_OID,
 * to name the same OID and avoid a duplicate-macro clash.  Keep the two
 * defines and gen_sql.c in sync at 7047.
 *
 * Like AO/AOCS, PAX does not store xmin/xmax per tuple — visibility comes from
 * aux relations (pg_ext_aux.pg_pax_blocks_<oid> or pg_manifest_<oid>) — so
 * its pg_class.relfrozenxid should always be InvalidTransactionId.
 *
 * Core code that decides whether to write a "normal" XID into
 * pg_class.relfrozenxid (e.g. cluster.c::swap_relation_files) checks
 * the IsAccessMethodAO whitelist and historically missed PAX, letting
 * VACUUM FULL / CLUSTER on PAX tables leave non-Invalid stale values
 * in catalog.  IsAccessMethodPAX exists so those sites can be updated
 * symmetrically without depending on contrib headers.
 */
#define EXT_PAX_TABLE_AM_OID 7047
#define IsAccessMethodPAX(am_oid) \
	((am_oid) == EXT_PAX_TABLE_AM_OID)

/* ----------------
 *		pg_am definition.  cpp turns this into
 *		typedef struct FormData_pg_am
 * ----------------
 */
CATALOG(pg_am,2601,AccessMethodRelationId)
{
	Oid			oid;			/* oid */

	/* access method name */
	NameData	amname;

	/* handler function */
	regproc		amhandler BKI_LOOKUP(pg_proc);

	/* see AMTYPE_xxx constants below */
	char		amtype;
} FormData_pg_am;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(amhandler REFERENCES pg_proc(oid));

/* ----------------
 *		Form_pg_am corresponds to a pointer to a tuple with
 *		the format of pg_am relation.
 * ----------------
 */
typedef FormData_pg_am *Form_pg_am;

DECLARE_UNIQUE_INDEX(pg_am_name_index, 2651, on pg_am using btree(amname name_ops));
#define AmNameIndexId  2651
DECLARE_UNIQUE_INDEX_PKEY(pg_am_oid_index, 2652, on pg_am using btree(oid oid_ops));
#define AmOidIndexId  2652

#ifdef EXPOSE_TO_CLIENT_CODE

/*
 * Allowed values for amtype
 */
#define AMTYPE_INDEX					'i' /* index access method */
#define AMTYPE_TABLE					't' /* table access method */

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_AM_H */
