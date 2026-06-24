#ifndef PARQUET_READER_C_H
#define PARQUET_READER_C_H

#include <gopher/gopher.h>

struct List;
struct DatalakeInternalRecord;

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

typedef struct
{
	gopherFS gopherFilesystem;
	void *buffer;  // dataBufferArray
	List *quals;   // WHERE-clause quals (Expr) for row-group min/max pushdown; NIL if none
} ParquetReadContext;

void *create_parquet_reader(MemoryContext mcxt, void *filePath, void *readContext);
void parquet_open(void *reader,
				  List *columnDesc,
				  bool *attrUsed,
				  int64_t beginOffset,
				  int64_t endOffset);
void parquet_close(void *reader);
bool parquet_next(void *reader, DatalakeInternalRecord *record);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_READER_C_H
