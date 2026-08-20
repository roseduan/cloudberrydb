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

PG_FUNCTION_INFO_V1(pg_iceberg_load_metadata_json_local);

Datum
pg_iceberg_load_metadata_json_local(PG_FUNCTION_ARGS)
{
	Oid							relid = PG_GETARG_OID(0);
	IcebergMetadataJsonResult  *res;
	TupleDesc					tupdesc;
	Datum						values[2];
	bool						nulls[2] = {false, false};
	HeapTuple					tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	BlessTupleDesc(tupdesc);

	res = pg_iceberg_load_metadata_json(relid);

	values[0] = CStringGetTextDatum(res->metadata_location);
	values[1] = CStringGetTextDatum(res->metadata_json);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	pg_iceberg_free_metadata_json_result(res);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
