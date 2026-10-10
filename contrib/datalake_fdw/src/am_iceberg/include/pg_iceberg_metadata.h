/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata.h
 *    Iceberg metadata management functions
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_metadata.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_METADATA_H__
#define __PG_ICEBERG_METADATA_H__

#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"

/*
 * Catalog OIDs for iceberg.pg_iceberg_metadata are pinned at initdb time
 * by iceberg-cdbinit--1.0.sql and exposed through iceberg_oids.h
 * (ICEBERG_METADATA_RELID, ICEBERG_METADATA_PKEY_OID).  C call sites
 * MUST use those constants, not name lookups -- the legacy
 * PG_ICEBERG_SCHEMA_NAME / PG_ICEBERG_METADATA_*_NAME macros are gone.
 */

/*
 * Column numbers for pg_ext_aux.pg_iceberg_metadata.
 *
 * The catalog is created at initdb by iceberg-cdbinit--1.0.sql rather than
 * generated from a BKI header, so there is no genbki-produced _d.h to derive
 * these from.  This header is the single authoritative copy: every C reader
 * of the relation (pg_iceberg_metadata.c, pg_iceberg_metadata_refresh.c,
 * iceberg_toolkit_common.c) addresses columns through these macros, so a
 * schema change is a one-line edit here plus the CREATE TABLE, not a hunt for
 * magic integers.  Keep the two in sync -- attribute numbers are 1-based and
 * follow the column order of that CREATE TABLE.
 */
#define Natts_pg_iceberg_metadata							6

#define Anum_pg_iceberg_metadata_relid						1
#define Anum_pg_iceberg_metadata_metadata_location			2
#define Anum_pg_iceberg_metadata_previous_metadata_location	3
#define Anum_pg_iceberg_metadata_is_internal				4
#define Anum_pg_iceberg_metadata_default_spec_id			5
#define Anum_pg_iceberg_metadata_metadata_cached_at			6

/*
 * IcebergMetadataInfo - stores metadata information for an Iceberg table
 */
typedef struct IcebergMetadataInfo
{
	char *metadata_location;          /* Current metadata file location */
	char *previous_metadata_location; /* Previous metadata file location */
	bool is_internal;                 /* Flag indicating if table is internal */
	int32 default_spec_id;            /* Default partition spec id */
	int64 metadata_cached_at;         /* epoch ms of last external-catalog head
									   * refresh (issue #393); 0 when NULL */
	bool metadata_cached_at_isnull;   /* true when metadata_cached_at is NULL */
} IcebergMetadataInfo;

/* Helper to retrieve iceberg metadata information */
extern IcebergMetadataInfo *pg_iceberg_get_metadata_info(Oid relid);

/* Same, but returns NULL instead of erroring when no entry exists */
extern IcebergMetadataInfo *pg_iceberg_get_metadata_info_missing_ok(Oid relid);

/* Helper to free memory allocated in IcebergMetadataInfo */
extern void pg_iceberg_free_metadata_info(IcebergMetadataInfo *info);

/* Function to insert iceberg metadata */
extern void pg_iceberg_add_metadata(Oid relid, char *metadata_location,
									char *previous_metadata_location,
									bool is_internal, int32 default_spec_id);

/* Function to update iceberg metadata location */
extern void pg_iceberg_update_metadata(Oid relid, const char *metadata_location);

/*
 * Write back the result of an external-catalog head refresh (issue #393):
 * update metadata_location and stamp metadata_cached_at with the agent-supplied
 * epoch-millis timestamp.  Unconditional (last-writer-wins); intended for the
 * read/scan refresh path, never for commit lineage (does not roll previous).
 */
extern void pg_iceberg_update_metadata_cache(Oid relid,
											 const char *metadata_location,
											 int64 cached_at_ms);

/*
 * Update metadata location with Compare-And-Swap (CAS) semantics.
 *
 * If expected_base is not NULL, the update only proceeds when the current
 * catalog metadata_location matches expected_base.  Returns false on
 * mismatch (concurrent modification detected), true on success.
 *
 * When expected_base is NULL, behaves identically to
 * pg_iceberg_update_metadata() (unconditional update, always returns true).
 */
extern bool pg_iceberg_update_metadata_cas(Oid relid,
										   const char *metadata_location,
										   const char *expected_base);

/* Function to delete iceberg metadata */
extern void pg_iceberg_remove_metadata(Oid relid);

#endif /* __PG_ICEBERG_METADATA_H__ */
