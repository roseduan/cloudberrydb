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

#endif /* __PG_ICEBERG_DDL_H__ */
