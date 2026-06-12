/*-------------------------------------------------------------------------
 *
 * laketablecmds.h
 *	  prototypes for laketablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/laketablecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LAKETABLECMDS_H
#define LAKETABLECMDS_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "parser/parse_node.h"
#include "catalog/pg_lake_table.h"
#include "utils/guc.h"

/* GUC variables */
extern char *iceberg_default_catalog;
extern char *iceberg_default_volume;

/* GUC check hooks */
extern bool check_iceberg_default_catalog(char **newval, void **extra, GucSource source);
extern bool check_iceberg_default_volume(char **newval, void **extra, GucSource source);

/* Functions to get default values */
extern const char *GetDefaultIcebergCatalog(void);
extern const char *GetDefaultIcebergVolume(void);

/* Lake table management */
extern void ValidateLakeTableOptions(CreateLakeTableStmt *stmt);
extern void CreateLakeTable(CreateLakeTableStmt *stmt, Oid relId);
extern void RemoveLakeTableEntry(Oid relid);

#endif /* LAKETABLECMDS_H */