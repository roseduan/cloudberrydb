#ifndef DATALAKE_COMMON_H
#define DATALAKE_COMMON_H

#include <cstdarg>
#include <cstddef>
#include <parquet/api/reader.h>
#include <parquet/internal/arrow/io/interfaces.h>
#include <avro.h>

extern "C"
{
#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "mb/pg_wchar.h"
}

#define ERROR_STR_LEN 512

struct Error : std::exception
{
	char text[ERROR_STR_LEN];

	Error(char const* fmt, ...) __attribute__((format(printf,2,3)))
	{
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(text, sizeof text, fmt, ap);
		va_end(ap);
	}

	char const* what() const throw() { return text; }
};

void *gpdbPalloc(Size size);
/*
 * Validate, and convert if the encodings differ, len bytes of text that is in
 * encoding `encoding` into the database encoding.  Returns src itself when no
 * conversion was needed (the bytes are then valid as they stand), otherwise a
 * palloc'd string the caller pfrees; *outLen is the resulting length.  A bad
 * sequence, or a character the database encoding cannot represent, arrives as
 * a C++ exception carrying PostgreSQL's own message.
 */
char *gpdbAnyToServer(const char *src, int len, int encoding, int *outLen);
Datum gpdbDirectFunctionCall2(PGFunction func, Datum arg1, Datum arg2);
Datum gpdbDirectFunctionCall3(PGFunction func, Datum arg1, Datum arg2, Datum arg3);
Oid mapParquetDataType(parquet::Type::type type);
Oid mapAvroDataType(avro_type_t type);
std::string convertToGopherPath(std::string &filePath);

#endif // DATALAKE_COMMON_H
