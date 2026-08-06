/*-------------------------------------------------------------------------
 *
 * pg_iceberg_options.c
 *    Generic option parsing utilities for Iceberg catalog tables.
 *
 * This module implements a table-driven option parser that can populate
 * C struct fields from PostgreSQL text[] arrays containing "key=value"
 * strings. It replaces ad-hoc option parsing loops with a declarative
 * approach.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/pg_iceberg_options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_lake_table.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "src/common/iceberg_constants.h"
#include "include/pg_iceberg_options.h"

/*
 * Option definitions for Iceberg table options in pg_lake_table.ltoptions.
 *
 * To add a new Iceberg table option:
 *   1. Add a field to the IcebergTableOptions struct in pg_iceberg_options.h.
 *   2. Add a corresponding OPTION_xxx() entry here.
 */
static const OptionDef icebergTableOptionDefs[] = {
	OPTION_STRING("catalog", IcebergTableOptions, catalog, NULL),
	OPTION_STRING("namespace", IcebergTableOptions, namespace, NULL),
	OPTION_STRING("table", IcebergTableOptions, table, NULL),
	OPTION_STRING("location", IcebergTableOptions, location, NULL),
	OPTION_BOOL("autovacuum_enabled", IcebergTableOptions, autovacuum_enabled, true),
	OPTION_STRING("compression", IcebergTableOptions, compression, "zstd"),
	OPTION_INT("compression_level", IcebergTableOptions, compression_level, -1),
	OPTION_STRING("iceberg_partition_by", IcebergTableOptions, partition_by, NULL),
	/* Add new Iceberg table options here */
};

static void pg_iceberg_upsert_location_option_local_internal(Oid relid,
															  const char *location);

PG_FUNCTION_INFO_V1(pg_iceberg_upsert_location_option_local);
Datum
pg_iceberg_upsert_location_option_local(PG_FUNCTION_ARGS)
{
	Oid relid;
	char *location;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("relation OID and location must not be NULL")));

	relid = PG_GETARG_OID(0);
	location = text_to_cstring(PG_GETARG_TEXT_PP(1));

	pg_iceberg_upsert_location_option_local_internal(relid, location);

	PG_RETURN_VOID();
}

/*
 * set_option_value
 *    Write a parsed value into a struct field at the given byte offset.
 *
 * This is the core value-assignment function. It handles type conversion
 * and validation for each supported OptionType.
 */
static void
set_option_value(const char *name, void *target, int offset,
				 const char *value, OptionType type)
{
	char *field_ptr = ((char *) target) + offset;

	switch (type)
	{
		case OPTION_TYPE_STRING:
			*((char **) field_ptr) = pstrdup(value);
			break;

		case OPTION_TYPE_BOOL:
			{
				bool bval;

				if (!parse_bool(value, &bval))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for boolean option \"%s\": \"%s\"",
									name, value)));

				*((bool *) field_ptr) = bval;
			}
			break;

		case OPTION_TYPE_INT:
			{
				char *endptr;
				long lval;

				errno = 0;
				lval = strtol(value, &endptr, 10);
				/* Error if invalid characters, empty input, or overflow (errno != 0) */
				if (*endptr != '\0' || endptr == value || errno != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for integer option \"%s\": \"%s\"",
									name, value)));

				*((int *) field_ptr) = (int) lval;
			}
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unsupported option type %d for option \"%s\"",
							(int) type, name)));
	}
}

/*
 * apply_default_options
 *    Populate the target struct with default values from the option definitions.
 */
void
apply_default_options(const OptionDef *defs, int ndefs, void *target)
{
	int i;

	for (i = 0; i < ndefs; i++)
	{
		void *field_ptr = ((char *) target) + defs[i].offset;

		switch (defs[i].type)
		{
			case OPTION_TYPE_STRING:
				if (defs[i].default_val.s != NULL)
					*((char **) field_ptr) = pstrdup(defs[i].default_val.s);
				break;

			case OPTION_TYPE_BOOL:
				*((bool *) field_ptr) = defs[i].default_val.b;
				break;

			case OPTION_TYPE_INT:
				*((int *) field_ptr) = defs[i].default_val.i;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unsupported option type %d for option \"%s\"",
								(int) defs[i].type, defs[i].name)));
		}
	}
}

/*
 * parse_options_array
 *    Parse a text[] options array into a target struct.
 *
 * Each element of the array is expected to be a "key=value" string.
 * For each element, we search the OptionDef array for a matching key
 * (case-insensitive) and write the value to the corresponding field
 * in the target struct.
 *
 * Unrecognized options are silently ignored (they may be consumed by
 * other subsystems).
 */
void
parse_options_array(ArrayType *options_array,
					const OptionDef *defs,
					int ndefs,
					void *target)
{
	Datum  *options_datums;
	bool   *options_nulls;
	int		noptions;
	int		i;

	Assert(options_array != NULL);
	Assert(defs != NULL);
	Assert(target != NULL);

	deconstruct_array(options_array, TEXTOID, -1, false, 'i',
					  &options_datums, &options_nulls, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char   *option_str;
		char   *eq_pos;
		int		key_len;
		int		j;

		if (options_nulls[i])
			continue;

		option_str = TextDatumGetCString(options_datums[i]);
		eq_pos = strchr(option_str, '=');

		if (eq_pos == NULL)
		{
			elog(DEBUG1, "malformed option \"%s\", expected \"key=value\"", option_str);
			continue;
		}

		key_len = eq_pos - option_str;

		for (j = 0; j < ndefs; j++)
		{
			if (strlen(defs[j].name) == (Size) key_len &&
				pg_strncasecmp(defs[j].name, option_str, key_len) == 0)
			{
				set_option_value(defs[j].name, target, defs[j].offset,
								eq_pos + 1, defs[j].type);
				break;
			}
		}
	}

	pfree(options_datums);
	pfree(options_nulls);
}

void
pg_iceberg_upsert_location_option(Oid relid, const char *location)
{
	Assert(location != NULL && location[0] != '\0');

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		StringInfoData sql_command;
		char *quoted_location;

		quoted_location = quote_literal_cstr(location);
		initStringInfo(&sql_command);
		appendStringInfo(&sql_command,
						 "SELECT pg_catalog.pg_iceberg_upsert_location_option_local(%u, %s)",
						 relid,
						 quoted_location);

		CdbDispatchCommand(sql_command.data, DF_CANCEL_ON_ERROR, NULL);
		pfree(quoted_location);
		pfree(sql_command.data);
	}

	pg_iceberg_upsert_location_option_local_internal(relid, location);
}

static void
pg_iceberg_upsert_location_option_local_internal(Oid relid,
												 const char *location)
{
	Relation		lake_rel;
	ScanKeyData		skey;
	SysScanDesc		scan;
	HeapTuple		lake_tuple;
	Datum			options_datum;
	bool			isnull;
	ArrayType	   *new_options = NULL;
	Datum		   *new_option_datums;
	int				new_option_count = 0;
	bool			updated = false;
	Datum			values[Natts_pg_lake_table];
	bool			nulls[Natts_pg_lake_table];
	bool			replaces[Natts_pg_lake_table];
	HeapTuple		new_tuple;

	Assert(location != NULL && location[0] != '\0');

	lake_rel = table_open(LakeTableRelationId, RowExclusiveLock);

	ScanKeyInit(&skey,
				Anum_pg_lake_table_ltrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(lake_rel, LakeTableOidIndexId, true,
							  NULL, 1, &skey);
	lake_tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(lake_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("lake table entry not found for relation %u", relid)));

	options_datum = heap_getattr(lake_tuple, Anum_pg_lake_table_ltoptions,
								 RelationGetDescr(lake_rel), &isnull);

	if (isnull)
	{
		new_option_count = 1;
		new_option_datums = (Datum *) palloc(sizeof(Datum));
		new_option_datums[0] = CStringGetTextDatum(psprintf("%s=%s",
														 DATALAKEFDW_ICEBERG_OPTION_LOCATION,
														 location));
	}
	else
	{
		ArrayType   *options_array = DatumGetArrayTypeP(options_datum);
		Datum	   *options_datums;
		bool	   *options_nulls;
		int			noptions;
		int			i;

		deconstruct_array(options_array, TEXTOID, -1, false, 'i',
						  &options_datums, &options_nulls, &noptions);
		new_option_datums = (Datum *) palloc(sizeof(Datum) * (noptions + 1));

		for (i = 0; i < noptions; i++)
		{
			char	   *option_str;
			char	   *eq_pos;
			int			key_len;

			if (options_nulls[i])
				continue;

			option_str = TextDatumGetCString(options_datums[i]);
			eq_pos = strchr(option_str, '=');

			if (eq_pos != NULL)
			{
				key_len = eq_pos - option_str;
				if (strlen(DATALAKEFDW_ICEBERG_OPTION_LOCATION) == (Size) key_len &&
					pg_strncasecmp(option_str,
								   DATALAKEFDW_ICEBERG_OPTION_LOCATION,
								   key_len) == 0)
				{
					new_option_datums[new_option_count++] =
						CStringGetTextDatum(psprintf("%s=%s",
												 DATALAKEFDW_ICEBERG_OPTION_LOCATION,
												 location));
					updated = true;
					continue;
				}
			}

			new_option_datums[new_option_count++] = CStringGetTextDatum(option_str);
		}

		if (!updated)
			new_option_datums[new_option_count++] =
				CStringGetTextDatum(psprintf("%s=%s",
										 DATALAKEFDW_ICEBERG_OPTION_LOCATION,
										 location));

		pfree(options_datums);
		pfree(options_nulls);
	}

	new_options = construct_array(new_option_datums,
								  new_option_count,
								  TEXTOID,
								  -1,
								  false,
								  'i');
	pfree(new_option_datums);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	values[Anum_pg_lake_table_ltoptions - 1] = PointerGetDatum(new_options);
	replaces[Anum_pg_lake_table_ltoptions - 1] = true;

	new_tuple = heap_modify_tuple(lake_tuple,
								  RelationGetDescr(lake_rel),
								  values,
								  nulls,
								  replaces);
	CatalogTupleUpdate(lake_rel, &lake_tuple->t_self, new_tuple);
	CommandCounterIncrement();
	heap_freetuple(new_tuple);

	systable_endscan(scan);
	table_close(lake_rel, RowExclusiveLock);
}

/*
 * free_iceberg_table_options
 *		Free an IcebergTableOptions and every pstrdup'd string field it owns.
 *		Safe on NULL.  Callers that stashed a shallow copy of any field must
 *		not use it after this returns.
 */
void
free_iceberg_table_options(IcebergTableOptions *opts)
{
	if (opts == NULL)
		return;

	if (opts->catalog)
		pfree(opts->catalog);
	if (opts->namespace)
		pfree(opts->namespace);
	if (opts->table)
		pfree(opts->table);
	if (opts->location)
		pfree(opts->location);
	if (opts->compression)
		pfree(opts->compression);
	if (opts->partition_by)
		pfree(opts->partition_by);
	pfree(opts);
}

IcebergTableOptions *
get_iceberg_options(Oid relid, Oid *catalog_oid, Oid *volume_oid)
{
	Relation		lake_rel;
	ScanKeyData		skey;
	SysScanDesc		scan;
	HeapTuple		lake_tuple;
	Form_pg_lake_table lake_form;
	bool			isnull;
	Datum			options_datum;
	IcebergTableOptions *opts = NULL;

	/* Open pg_lake_table and search for the relation */
	lake_rel = table_open(LakeTableRelationId, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_pg_lake_table_ltrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(lake_rel, LakeTableOidIndexId, true,
							  NULL, 1, &skey);

	lake_tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(lake_tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("lake table entry not found for relation %u", relid)));

	/* Extract catalog and volume OIDs from pg_lake_table */
	lake_form = (Form_pg_lake_table) GETSTRUCT(lake_tuple);
	*catalog_oid = lake_form->ltforeign_catalog;
	*volume_oid = lake_form->ltforeign_volume;

	/* Extract options using declarative option parser */
	options_datum = heap_getattr(lake_tuple, Anum_pg_lake_table_ltoptions,
								 RelationGetDescr(lake_rel), &isnull);
	if (!isnull)
	{
		ArrayType *options_array = DatumGetArrayTypeP(options_datum);
		opts = parse_iceberg_table_options(options_array);
	}

	systable_endscan(scan);
	table_close(lake_rel, AccessShareLock);

	return opts;
}

/*
 * parse_iceberg_table_options
 *    Parse pg_lake_table.ltoptions into an IcebergTableOptions struct.
 *
 * Returns a palloc0'd IcebergTableOptions with all matched fields populated.
 */
IcebergTableOptions *
parse_iceberg_table_options(ArrayType *options_array)
{
	IcebergTableOptions *opts;

	opts = (IcebergTableOptions *) palloc0(sizeof(IcebergTableOptions));

	/* Apply default values first */
	apply_default_options(icebergTableOptionDefs,
						  lengthof(icebergTableOptionDefs),
						  opts);

	parse_options_array(options_array,
						icebergTableOptionDefs,
						lengthof(icebergTableOptionDefs),
						opts);

	return opts;
}
