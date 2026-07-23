#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "src/provider/common/file_reader.h"
#include "hudi_merged_logfile_record_reader.h"
#include "hudi_deltalog_filter.h"

static bool deltaLogFilterNext(Reader *filter, DatalakeInternalRecord *record);
static void deltaLogFilterClose(Reader *filter);
static bool logFilterNext(DeltaLogFilter *filter, DatalakeInternalRecord *record);
static bool mergeFilterNext(DeltaLogFilter *filter, DatalakeInternalRecord *record);

static Reader methods = {
	NULL,
	deltaLogFilterNext,
	deltaLogFilterClose,
};

DeltaLogFilter *
createDeltaLogFilter(MemoryContext mcxt,
					 List *datafileDesc,
					 TupleDesc tupDesc,
					 bool *attrUsed,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 List *deltaLogs,
					 const char *instantTime,
					 ExternalTableMetadata *tableOptions)
{
	DeltaLogFilter *filter = palloc0(sizeof(DeltaLogFilter));
	ossFileStream   logStream = fileStream;

	filter->base = methods;
	filter->dataReader = dataReader;
	filter->tableOptions = tableOptions;
	filter->readLogs = false;
	filter->nColumns = list_length(datafileDesc);
	filter->ownedStream = NULL;

	/*
	 * Merge-on-read (dataReader != NULL): the base data-file reader and the
	 * delta-log reader both drive the same underlying stream, whose backend
	 * (GopherFileSystem) keeps a single open-file handle. If they shared it,
	 * one reader's seek/read would clobber the other's file position and the
	 * parquet scan would fail ("failed to seek file"). Give the delta-log
	 * reader its own cloned stream so the two positions stay independent.
	 * The log-only path (dataReader == NULL) has no such contention and reuses
	 * the caller's stream directly.
	 */
	if (dataReader != NULL)
	{
		logStream = datalakeCloneFileStream(fileStream);
		filter->ownedStream = logStream;
	}

	filter->deltaSet = createMergedLogfileRecordReader(mcxt,
													   datafileDesc,
													   tupDesc,
													   attrUsed,
													   instantTime,
													   logStream,
													   deltaLogs,
													   tableOptions);
	if (dataReader == NULL)
		elog(DEBUG1, "create hudi log only filter");
	else
		elog(DEBUG1, "create hudi merge on read filter");

	return filter;
}

static bool
deltaLogFilterNext(Reader *filter, DatalakeInternalRecord *record)
{
	DeltaLogFilter *deltaLogFilter = (DeltaLogFilter *) filter;

	if (deltaLogFilter->dataReader == NULL)
		return logFilterNext(deltaLogFilter, record);

	return mergeFilterNext(deltaLogFilter, record);
}

static void
deltaLogFilterClose(Reader *filter)
{
	DeltaLogFilter *deltaLogFilter = (DeltaLogFilter *) filter;

	if (deltaLogFilter->dataReader)
		deltaLogFilter->dataReader->Close(deltaLogFilter->dataReader);

	if (deltaLogFilter->deltaSet)
		mergedLogfileRecordReaderClose(deltaLogFilter->deltaSet);

	/*
	 * Release the stream we cloned for the delta-log reader (MOR path only).
	 * mergedLogfileRecordReaderClose() borrows but never owns the stream, so
	 * this is the sole owner and there is no double-free.
	 */
	if (deltaLogFilter->ownedStream != NULL)
		datalakeDestroyFileSystem(deltaLogFilter->ownedStream);

	pfree(deltaLogFilter);

	elog(DEBUG1, "close hudi delta log filter");
}

static bool
logFilterNext(DeltaLogFilter *filter, DatalakeInternalRecord *record)
{
	return mergedLogfileRecordReaderNext(filter->deltaSet, record);
}

static bool
mergeFilterNext(DeltaLogFilter *filter, DatalakeInternalRecord *record)
{
	int  i;
	bool exist;
	bool isDeleted;
	DatalakeInternalRecord *newRecord;

	while(!filter->readLogs && filter->dataReader->Next(filter->dataReader, record))
	{
		exist = mergedLogfileContains(filter->deltaSet, record, &newRecord, &isDeleted);
		if (exist)
		{
			if (isDeleted)
				continue;

			for(i = 0; i < filter->nColumns; i++)
			{
				record->values[i] = newRecord->values[i];
				record->nulls[i] = newRecord->nulls[i];
			}
		}

		return true;
	}

	filter->readLogs = true;
	return logFilterNext(filter, record);
}
