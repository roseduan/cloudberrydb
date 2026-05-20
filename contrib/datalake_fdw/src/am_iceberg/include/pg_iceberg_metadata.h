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

/* Schema name for iceberg-related objects */
#define PG_ICEBERG_SCHEMA_NAME "iceberg"

/* Metadata table name */
#define PG_ICEBERG_METADATA_TABLE_NAME "pg_iceberg_metadata"

/*
 * Metadata table primary-key index name.
 *
 * Created implicitly by the PRIMARY KEY constraint on iceberg.pg_iceberg_metadata
 * in datalake_fdw--1.0.sql; PostgreSQL synthesises the name as <table>_pkey.
 */
#define PG_ICEBERG_METADATA_INDEX_NAME "pg_iceberg_metadata_pkey"

/*
 * IcebergMetadataInfo - stores metadata information for an Iceberg table
 */
typedef struct IcebergMetadataInfo
{
	char *metadata_location;          /* Current metadata file location */
	char *previous_metadata_location; /* Previous metadata file location */
	bool is_internal;                 /* Flag indicating if table is internal */
	int32 default_spec_id;            /* Default partition spec id */
} IcebergMetadataInfo;

/* Helper to retrieve iceberg metadata information */
extern IcebergMetadataInfo *pg_iceberg_get_metadata_info(Oid relid);

/* Helper to free memory allocated in IcebergMetadataInfo */
extern void pg_iceberg_free_metadata_info(IcebergMetadataInfo *info);

/* Function to insert iceberg metadata */
extern void pg_iceberg_add_metadata(Oid relid, char *metadata_location,
									char *previous_metadata_location,
									bool is_internal, int32 default_spec_id);

/* Function to update iceberg metadata location */
extern void pg_iceberg_update_metadata(Oid relid, const char *metadata_location);

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
