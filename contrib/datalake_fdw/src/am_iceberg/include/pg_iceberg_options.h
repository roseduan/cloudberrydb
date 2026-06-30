/*-------------------------------------------------------------------------
 *
 * pg_iceberg_options.h
 *    Generic option parsing utilities for Iceberg catalog tables.
 *
 * This module provides a declarative, table-driven approach for parsing
 * options stored as text[] arrays (in "key=value" format) into C structs.
 *
 * Usage:
 *   1. Define a target struct with the option fields.
 *   2. Define an OptionDef[] array mapping option names to struct offsets.
 *   3. Call parse_options_array() to automatically populate the struct.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/include/pg_iceberg_options.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_OPTIONS_H__
#define __PG_ICEBERG_OPTIONS_H__

#include "postgres.h"
#include "utils/array.h"

/*
 * Supported option value types.
 */
typedef enum OptionType
{
	OPTION_TYPE_STRING,		/* char * field, value is pstrdup'd */
	OPTION_TYPE_BOOL,		/* bool field, parsed from "true"/"false" */
	OPTION_TYPE_INT			/* int field, parsed via atoi/strtol */
} OptionType;

/*
 * OptionDef - descriptor for a single option.
 *
 * Each entry maps an option name to a field within a target struct,
 * identified by the field's byte offset and value type.
 */
typedef struct OptionDef
{
	const char *name;		/* option key name (matched case-insensitively) */
	OptionType  type;		/* expected value type */
	int         offset;		/* byte offset of the field in the target struct */
	union {
		const char *s;		/* string default value */
		bool        b;		/* boolean default value */
		int         i;		/* integer default value */
	} default_val;
} OptionDef;

/*
 * Convenience macro to compute the offset of a field, paired with its type.
 * Usage: OPTION_STRING("catalog_name", IcebergTableOptions, catalog_name, "default")
 */
#define OPTION_STRING(opt_name, struct_type, field, def_val) \
	{ (opt_name), OPTION_TYPE_STRING, offsetof(struct_type, field), {.s = (def_val)} }

#define OPTION_BOOL(opt_name, struct_type, field, def_val) \
	{ (opt_name), OPTION_TYPE_BOOL, offsetof(struct_type, field), {.b = (def_val)} }

#define OPTION_INT(opt_name, struct_type, field, def_val) \
	{ (opt_name), OPTION_TYPE_INT, offsetof(struct_type, field), {.i = (def_val)} }

/*
 * apply_default_options
 *    Populate the target struct with default values from the option definitions.
 */
extern void apply_default_options(const OptionDef *defs,
								  int ndefs,
								  void *target);

/*
 * IcebergTableOptions - parsed Iceberg table options from pg_lake_table.ltoptions
 *
 * Add new Iceberg table options here as fields, and register them
 * in the icebergTableOptionDefs[] array in pg_iceberg_options.c.
 */
typedef struct IcebergTableOptions
{
	char *catalog;		/* Catalog name in external catalog */
	char *namespace;		/* Namespace in external catalog */
	char *table;		/* Table name in external catalog */
	char *location;		/* Optional table location URI override */
	bool autovacuum_enabled;	/* Whether autovacuum is enabled for this table */
	char *compression;		/* Parquet write compression codec (default zstd) */
	int  compression_level;	/* Parquet write compression level; -1 = codec default */
} IcebergTableOptions;

/*
 * parse_options_array
 *    Parse a text[] options array into a target struct using the given
 *    option definitions.
 *
 * The options array contains strings in "key=value" format. For each
 * string, the key is matched (case-insensitively) against the OptionDef
 * entries, and the value is written to the corresponding field in the
 * target struct.
 *
 * Parameters:
 *   options_array  - the PostgreSQL text[] ArrayType to parse
 *   defs           - array of OptionDef descriptors
 *   ndefs          - number of entries in defs[]
 *   target         - pointer to the target struct (must be zeroed by caller)
 */
extern void parse_options_array(ArrayType *options_array,
								const OptionDef *defs,
								int ndefs,
								void *target);

/*
 * parse_iceberg_table_options
 *    Convenience function to parse pg_lake_table.ltoptions into an
 *    IcebergTableOptions struct.
 *
 * Returns a palloc'd IcebergTableOptions. The caller is responsible for
 * freeing it (or relying on memory context cleanup).
 */
extern IcebergTableOptions *parse_iceberg_table_options(ArrayType *options_array);

/*
 * Read and parse pg_lake_table.ltoptions for a relation, and return
 * associated catalog/volume OIDs from the same tuple.
 */
extern IcebergTableOptions *get_iceberg_options(Oid relid,
												Oid *catalog_oid,
												Oid *volume_oid);

/*
 * Upsert "location=<value>" in pg_lake_table.ltoptions for a relation.
 */
extern void pg_iceberg_upsert_location_option(Oid relid, const char *location);

#endif /* __PG_ICEBERG_OPTIONS_H__ */
