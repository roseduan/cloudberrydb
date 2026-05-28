/*-------------------------------------------------------------------------
 *
 * iceberg_oids.h
 *    Stable OID constants for iceberg native catalog objects.
 *
 * These values are pinned at initdb time by iceberg-cdbinit--1.0.sql; the
 * cdbinit script does the catalog-rewrite trick (CREATE TABLE, then
 * UPDATE pg_class/pg_type/pg_index/pg_attribute/pg_depend/pg_constraint
 * to the target OIDs) so that on every node these objects are reachable
 * by constant OID -- exactly like PostgreSQL's built-in BKI catalogs
 * (e.g. pg_foreign_data_wrapper / ForeignDataWrapperRelationId) and
 * GPDB's gp_distribution_policy / GpPolicyRelationId.
 *
 * Hot-path lookups must use these constants instead of the legacy
 * get_namespace_oid() + get_relname_relid() pattern, which breaks the
 * moment the catalog table/index is renamed (issue #324 follow-up).
 *
 * Reserved OID block: 8320..8339 (iceberg).
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/iceberg_oids.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ICEBERG_OIDS_H
#define ICEBERG_OIDS_H

/* 8320  pg_am.oid for 'iceberg'                              (ICEBERG_AM_OID)  */
/* 8321  pg_proc.oid for pg_iceberg_tableam_handler                             */

/*
 * Tables live in pg_ext_aux (the PostgreSQL/Cloudberry built-in schema for
 * extension-auxiliary catalogs, oid 7094 in postgres.bki, shared with
 * pax_storage's pg_pax_fastsequence).  No dedicated iceberg namespace oid
 * is reserved.  See iceberg-cdbinit--1.0.sql header for rationale.
 */

/* pg_iceberg_metadata */
#define ICEBERG_METADATA_RELID                  8330
#define ICEBERG_METADATA_PKEY_OID               8331

/* pg_iceberg_deletion_queue */
#define ICEBERG_DELETION_QUEUE_RELID            8334
#define ICEBERG_DELETION_QUEUE_PKEY_OID         8335

#endif /* ICEBERG_OIDS_H */
