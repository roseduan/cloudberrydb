#ifndef ICEBERG_EQUALITY_FILTER_H
#define ICEBERG_EQUALITY_FILTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"
typedef struct DatalakeEqualityFilter
{
	Reader         base;
	List          *deletesSets;
	Reader        *dataReader;
	MemoryContext  mcxt;
} DatalakeEqualityFilter;

DatalakeEqualityFilter *
datalakeCreateEqualityFilter(MemoryContext readerMcxt,
					 List *datafileTupleDesc,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 List *deletes);

#endif // ICEBERG_EQUALITY_FIlTER_H
