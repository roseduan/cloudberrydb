/*-------------------------------------------------------------------------
 *
 * pg_iceberg_metadata_reader.c
 *	  SQL-callable reader for a builtin iceberg table's metadata.json.
 *
 * Exists so the datalake_rest_catalog gateway does not need object storage
 * credentials of its own: the REST spec requires LoadTableResult.metadata to
 * be a complete TableMetadata document, and the database already holds the
 * volume credentials needed to fetch it.
 *
 * COORDINATOR ONLY: it reads the QD-only pg_iceberg_metadata catalog, enforced
 * by the Gp_role check in pg_iceberg_load_metadata_json().  EXECUTE ON
 * COORDINATOR cannot express that here -- the clause is rejected for anything
 * that is not set-returning -- so the runtime check is the only guard; see the
 * note on the CREATE FUNCTION in datalake_fdw--1.0.sql.  Note also the
 * deliberate absence of a "_local" suffix, which in this extension means the
 * opposite thing (the segment-local half of an operation the QD dispatches --
 * see pg_iceberg_upsert_location_option_local).
 *
 * SECURITY: this function performs NO authorization of its own.  It is
 * registered in pg_catalog and must stay revoked from PUBLIC; the only
 * intended caller is the SECURITY DEFINER wrapper
 * pg_ext_aux.iceberg_load_metadata(), which does the RBAC filtering.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_metadata_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"

#include "include/pg_iceberg_catalog_helper.h"

PG_FUNCTION_INFO_V1(pg_iceberg_load_metadata_json_sql);

Datum
pg_iceberg_load_metadata_json_sql(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	char	   *metadata_location;
	char	   *metadata_json;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	BlessTupleDesc(tupdesc);

	pg_iceberg_load_metadata_json(relid, &metadata_location, &metadata_json);

	values[0] = CStringGetTextDatum(metadata_location);
	values[1] = CStringGetTextDatum(metadata_json);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	pfree(metadata_location);
	pfree(metadata_json);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
