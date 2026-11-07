#include <memory>
#include <stdexcept>
#include "common.h"

void *
gpdbPalloc(Size size)
{
	void *result;
	bool error = false;

	PG_TRY();
	{
		result = palloc(size);
	}
	PG_CATCH();
	{
		error = true;
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error("out of memory");

	return result;
}

char *
gpdbAnyToServer(const char *src, int len, int encoding, int *outLen)
{
	char	   *result = NULL;
	char		errStr[ERROR_STR_LEN];
	bool		error = false;
	MemoryContext mcxt = CurrentMemoryContext;

	/*
	 * SQL_ASCII is the database saying it has no encoding semantics: PG
	 * itself stores and returns the bytes unexamined.  pg_any_to_server
	 * would still validate them against `encoding` and then hand back the
	 * very same bytes, so it could only ever reject data that used to
	 * read, without changing what lands in the datum.  Leave it alone.
	 */
	if (GetDatabaseEncoding() == PG_SQL_ASCII)
	{
		*outLen = len;
		return const_cast<char *>(src);
	}

	PG_TRY();
	{
		result = pg_any_to_server(src, len, encoding);
	}
	PG_CATCH();
	{
		ErrorData  *errdata;

		error = true;

		MemoryContextSwitchTo(mcxt);
		errdata = CopyErrorData();
		FlushErrorState();
		strncpy(errStr, errdata->message, ERROR_STR_LEN - 1);
		errStr[ERROR_STR_LEN - 1] = '\0';
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error(errStr);

	*outLen = (result == src) ? len : (int) strlen(result);
	return result;
}

Datum
gpdbDirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2)
{
	Datum result;
	char errStr[ERROR_STR_LEN];
	bool error = false;
	MemoryContext mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		result = DirectFunctionCall2(func, arg1, arg2);
	}
	PG_CATCH();
	{
		ErrorData *errdata;

		error = true;

		MemoryContextSwitchTo(mcxt);
		errdata = CopyErrorData();
		FlushErrorState();
		strncpy(errStr, errdata->message, ERROR_STR_LEN - 1);
		errStr[ERROR_STR_LEN - 1] = '\0';
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error(errStr);

	return result;
}

Datum
gpdbDirectFunctionCall3(PGFunction func, Datum arg1, Datum arg2, Datum arg3)
{
	Datum result;
	char errStr[ERROR_STR_LEN];
	bool error = false;
	MemoryContext mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		result = DirectFunctionCall3(func, arg1, arg2, arg3);
	}
	PG_CATCH();
	{
		ErrorData *errdata;

		error = true;

		MemoryContextSwitchTo(mcxt);
		errdata = CopyErrorData();
		FlushErrorState();
		strncpy(errStr, errdata->message, ERROR_STR_LEN - 1);
		errStr[ERROR_STR_LEN - 1] = '\0';
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	if (error)
		throw std::runtime_error(errStr);

	return result;
}

Oid
mapParquetDataType(parquet::Type::type type)
{
	switch (type)
	{
		case parquet::Type::BOOLEAN:
			return BOOLOID;
		case parquet::Type::INT32:
			return INT4OID;
		case parquet::Type::INT64:
			return INT8OID;
		case parquet::Type::INT96:
			return NUMERICOID;
		case parquet::Type::FLOAT:
			return FLOAT8OID;
		case parquet::Type::DOUBLE:
			return FLOAT8OID;
		case parquet::Type::BYTE_ARRAY:
			return TEXTOID;
		case parquet::Type::FIXED_LEN_BYTE_ARRAY:
			return NUMERICOID;
		default:
			return InvalidOid;
	}
}

Oid
mapAvroDataType(avro_type_t type)
{
	switch (type)
	{
		case AVRO_STRING:
			return TEXTOID;
		case AVRO_BYTES:
			return BYTEAOID;
		case AVRO_INT32:
			return INT4OID;
		case AVRO_INT64:
			return INT8OID;
		case AVRO_FLOAT:
			return FLOAT4OID;
		case AVRO_DOUBLE:
			return FLOAT8OID;
		case AVRO_BOOLEAN:
			return BOOLOID;
		case AVRO_FIXED:
			return NUMERICOID;
		default:
			return InvalidOid;
	}
}

std::string
convertToGopherPath(std::string &filePath)
{
	const char *pathPos;
	const char *hostPos = filePath.c_str() + 7; /* remove "hdfs://" */

	pathPos = strchr(hostPos, '/'); /* remove host */
	return std::string(pathPos);
}
