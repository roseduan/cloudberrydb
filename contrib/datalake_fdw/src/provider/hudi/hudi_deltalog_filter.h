#ifndef HUDI_DELTALOG_FILTER_H
#define HUDI_DELTALOG_FILTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"
struct ExternalTableMetadata;

typedef struct DeltaLogFilter
{
	Reader         base;
	Reader        *dataReader;
	bool           readLogs;
	int            nColumns;
	ExternalTableMetadata *tableOptions;
	HudiMergedLogfileRecordReader *deltaSet;
	/*
	 * When this filter runs merge-on-read (dataReader != NULL), the delta-log
	 * reader gets its own cloned stream so it does not share a single file
	 * handle with the base data-file reader. Owned here; released in
	 * deltaLogFilterClose(). NULL for the log-only path (no clone needed).
	 */
	ossFileStream  ownedStream;
} DeltaLogFilter;

DeltaLogFilter *
createDeltaLogFilter(MemoryContext mcxt,
					 List *datafileDesc,
					 TupleDesc tupDesc,
					 bool *attrUsed,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 List *deltaLogs,
					 const char *instantTime,
					 ExternalTableMetadata *tableOptions);

#endif // HUDI_DELTALOG_FILTER_H
