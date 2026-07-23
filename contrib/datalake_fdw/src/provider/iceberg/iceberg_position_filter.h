#ifndef ICEBERG_POSITION_FILTER_H
#define ICEBERG_POSITION_FIlTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/utils.h"

typedef struct DatalakePositionFilter
{
	Reader            base;
	void             *deletesSet;
	bool              isEmptySet;
	bool              borrowedBitmap;	/* true if bitmap is from global index (don't free) */
	Reader           *dataReader;
	void             *sortedMerge;
	int64             nextPosition;
	bool              isEmptyStream;
	MemoryContext	  mcxt;
} DatalakePositionFilter;

DatalakePositionFilter *
datalakeCreatePositionFilter(MemoryContext readerMcxt,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 char *dataFilePath,
					 List *deletes);

/*
 * Create a position filter using a pre-built bitmap from the global delete
 * index.  The bitmap is NOT owned by the filter (do not destroy it on close).
 */
DatalakePositionFilter *
datalakeCreatePositionFilterFromBitmap(Reader *dataReader, void *bitmap);

#endif // ICEBERG_POSITION_FILTER_H
