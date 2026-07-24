#ifndef FILE_READER_H
#define FILE_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils.h"

typedef struct DatalakeFormatReader
{
	const char *formatName;
	void *(*Create) (MemoryContext mcxt, void *filenameOrStream, void *extraArg);
	void (*Open) (void *reader, List *columnDesc, bool *attrUsed, int64_t beginOffset, int64_t endOffset);
	bool (*Next) (void *reader, DatalakeInternalRecord *record);
	void (*Close) (void *reader);

	/*
	 * Optional: whether returned Datums for the attribute point into
	 * reader-owned memory (batch slab).  NULL means the caller owns every
	 * returned Datum and may pfree it.
	 */
	bool (*DatumOwned) (void *reader, int attIdx);
} DatalakeFormatReader;

typedef struct FileReader
{
	Reader        base;
	bool          isFileStream;
	FileFragment *dataFile;
	void         *dataReader;
	DatalakeFormatReader *formatReader;
} FileReader;

bool datalakeFileReaderDatumOwned(Reader *reader, int attIdx);

FileReader *
datalakeCreateFileReader(MemoryContext mcxt,
				 List *columnDesc,
				 bool *attrUsed,
				 bool isFileStream,
				 FileFragment *dataFile,
				 void *extraArg,
				 int64_t beginOffset,
				 int64_t endOffset,
				 void *buffer,
				 List *quals);

#endif // FILE_READER_H
