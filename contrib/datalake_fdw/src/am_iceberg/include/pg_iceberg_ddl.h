/*-------------------------------------------------------------------------
 *
 * pg_iceberg_ddl.h
 *
 *
 * IDENTIFICATION
 *	  contrib/pg_iceberg/include/pg_iceberg_ddl.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_DDL_H__
#define __PG_ICEBERG_DDL_H__

extern void pg_iceberg_setup_ddl_hooks(void);

/* Truncate a builtin-catalog iceberg table (driven by the TRUNCATE hook). */
extern void pg_iceberg_truncate_table(Oid relid);

/*
 * ALTER TABLE schema evolution for builtin-catalog iceberg tables (#401).
 * build: validate + build the IcebergSchemaOp* list; returns the standard cmd
 * list (intercepted subcommands like ALTER COLUMN TYPE removed) via standardCmds
 * and the intercepted type changes via typeChanges.  apply_type_changes: apply
 * the intercepted ALTER COLUMN TYPE to pg_attribute (QD).  alter_apply: QD-only,
 * fire the UpdateSchema commit + CAS the metadata pointer.
 */
extern List *pg_iceberg_build_alter_ops(Oid relid, List *cmds,
										List **standardCmds, List **typeChanges);
extern void pg_iceberg_apply_type_changes(Oid relid, List *typeChanges);
extern void pg_iceberg_alter_apply(Oid relid, List *ops);

/* RENAME COLUMN on a builtin iceberg table (driven by the T_RenameStmt hook). */
extern void pg_iceberg_rename_column(Oid relid, const char *oldname,
									 const char *newname);

#endif /* __PG_ICEBERG_DDL_H__ */
