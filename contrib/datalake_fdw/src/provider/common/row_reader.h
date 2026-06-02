#ifndef ROW_READER_H
#define ROW_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils.h"

DatalakeRowReader *datalakeCreateRowReader(MemoryContext mcxt,
						   TupleDesc tupleDesc,
						   int nTblColumn,
						   bool *attrUsed,
						   gopherFS gopherFilesystem,
						   List *combinedScanTasks,
						   DLTblFmt format,
						   ExternalTableMetadata *tableOptions);
bool datalakeRowReaderNext(DatalakeRowReader *reader, DatalakeInternalRecord *record);

/*
 * Deep fast-path inline: call the innermost format reader (e.g. parquet_next)
 * directly, bypassing 3 layers of function-pointer indirection per row:
 *   datalakeRowReaderNext → icebergTaskReaderNext → fileReaderNext
 *
 * Returns true if a row was read; false means the caller must fall back
 * to datalakeRowReaderNext() for file switching.
 *
 * The cached function pointer was set by C code in row_reader.c (same
 * compilation unit as the struct definitions) after traversing:
 *   IcebergTaskReader → FileReader → formatReader->Next + dataReader
 */
static inline bool
datalakeRowReaderFastNext(DatalakeRowReader *reader, DatalakeInternalRecord *record)
{
	if (likely(reader->deepNext != NULL))
	{
		if (reader->deepNext(reader->deepReader, record))
		{
			record->fileId = reader->deepFileId;
			return true;
		}
		/* Current file exhausted, clear cache */
		reader->deepNext = NULL;
		reader->deepReader = NULL;
	}
	return false;
}
void datalakeRowReaderClose(DatalakeRowReader *reader);
void datalakeRowReaderRewind(DatalakeRowReader *reader);

/*
 * The following functions are migrated from datalake_extension.c in hashdata 3X.
 * see https://code.hashdata.xyz/hashdata/hashdata/-/blob/v3.x/gpcontrib/datalake_extension/src/datalake_extension.c?ref_type=heads
 */
DatalakeProtocolContext *datalakeCreateContext(dataLakeOptions *options);
void datalakeCleanupContext(DatalakeProtocolContext *context);
void datalakeProtocolImportStart(dataLakeFdwScanState *scanstate, DatalakeProtocolContext *context, bool *attrUsed);


#endif // ROW_READER_H
