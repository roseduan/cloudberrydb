/*-------------------------------------------------------------------------
 *
 * pg_iceberg_am_handler.h
 * 		Routines for pg_iceberg AM handler.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_am_handler.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_AM_HANDLER_H__
#define __PG_ICEBERG_AM_HANDLER_H__

#include "utils/relcache.h"
#include "pg_iceberg_am.h"

extern bool is_iceberg_rel(Relation rel);

/*
 * Throws ERROR with a HINT pointing at CREATE EXTENSION when the
 * datalake_fdw extension is not installed in the current database.
 *
 * The iceberg access method (OID 8320) and its handler function
 * (OID 8321 -> '$libdir/datalake_fdw') are pre-registered via cdb_init.d
 * on every database, so the AM is reachable even without the extension --
 * but the iceberg schema, pg_iceberg_metadata table, and toolkit
 * functions only appear after CREATE EXTENSION datalake_fdw.  Without
 * this guard, CREATE TABLE ... USING iceberg / CREATE ICEBERG TABLE
 * silently leave a half-baked relation that can be neither read nor
 * dropped.  See issue #337.
 */
extern void pg_iceberg_require_extension_installed(void);

/* DML lifecycle management (called from external DDL/DML hooks) */
extern void pg_iceberg_ext_dml_init(Relation rel, CmdType operation);
extern void pg_iceberg_ext_dml_fini(Relation rel, CmdType operation);

#endif /* __PG_ICEBERG_AM_HANDLER_H__ */
