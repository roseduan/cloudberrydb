#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "file_reader.h"
#include "parquet_reader_c.h"
#include "avro_block_reader_c.h"

static bool fileReaderNext(Reader *reader, DatalakeInternalRecord *record);
static void fileReaderClose(Reader *reader);

static Reader methods = {
	NULL,
	fileReaderNext,
	fileReaderClose,
};

static DatalakeFormatReader parquetReader = {
	"parquet",
	create_parquet_reader,
	parquet_open,
	parquet_next,
	parquet_close,
	parquet_datum_owned
};

static DatalakeFormatReader orcReader = {
	"orc",
	NULL,
	NULL,
	NULL,
	NULL
};

static DatalakeFormatReader avroReader = {
	"avro",
	NULL,
	NULL,
	NULL,
	NULL
};

static DatalakeFormatReader avroBlockReader = {
	"avro block",
	create_avro_block_reader,
	avro_block_open,
	avro_block_next,
	avro_block_close
};

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
				 List *quals)
{
	FileReader *reader = palloc0(sizeof(FileReader));
	ParquetReadContext parquetContext;

	reader->base = methods;
	reader->dataFile = dataFile;
	reader->isFileStream = isFileStream;
	elog(LOG, "file path: %s", dataFile->filePath);

	switch (dataFile->format)
	{
		case PARQUET:
			reader->formatReader = &parquetReader;
			parquetContext.fileStream = (ossFileStream) extraArg;
			parquetContext.buffer = buffer;
			parquetContext.quals = quals;
			extraArg = (void *) &parquetContext;
			break;
		case ORC:
			reader->formatReader = &orcReader;
			break;
		case AVRO:
			reader->formatReader = &avroReader;
			break;
		case AVRO_FILE_BLOCK:
			reader->formatReader = &avroBlockReader;
			break;
		default:
			elog(ERROR, "unknown file type: %d", dataFile->format);
	}

	if (reader->formatReader->Create == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("file format \"%s\" is not supported", reader->formatReader->formatName)));

	reader->dataReader = reader->formatReader->Create(mcxt, dataFile->filePath, extraArg);
	reader->formatReader->Open(reader->dataReader, columnDesc, attrUsed, beginOffset, endOffset);

	return reader;
}

static bool
fileReaderNext(Reader *reader, DatalakeInternalRecord *record)
{
	FileReader *fileReader = (FileReader *) reader;
	return fileReader->formatReader->Next(fileReader->dataReader, record);
}

static void
fileReaderClose(Reader *reader)
{
	FileReader *fileReader = (FileReader *) reader;

	fileReader->formatReader->Close(fileReader->dataReader);
	if (fileReader->isFileStream)
		pfree(fileReader->dataFile->filePath);

	pfree(fileReader->dataFile);
	pfree(fileReader);
}

/*
 * Whether Datums returned for the given record attribute are owned by the
 * underlying format reader (point into its batch slab) and therefore must
 * not be pfree'd by the caller.  Format readers without batch support do
 * not set DatumOwned and always hand out caller-owned palloc'd Datums.
 */
bool
datalakeFileReaderDatumOwned(Reader *reader, int attIdx)
{
	FileReader *fileReader = (FileReader *) reader;

	if (fileReader->formatReader->DatumOwned == NULL)
		return false;

	return fileReader->formatReader->DatumOwned(fileReader->dataReader, attIdx);
}
