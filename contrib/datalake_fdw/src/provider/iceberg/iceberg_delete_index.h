#ifndef ICEBERG_DELETE_INDEX_H
#define ICEBERG_DELETE_INDEX_H

#include "postgres.h"
#include "utils/hsearch.h"
#include "src/common/fileSystemWrapper.h"
#include "src/dlproxy/datalake.h"

/*
 * Global position delete index for Iceberg MOR scans.
 *
 * Pre-reads all position delete files once and builds a hash table mapping
 * data file paths to Roaring64Map bitmaps of deleted row positions.
 * This eliminates the need to re-read delete files for every data file task,
 * reducing I/O from O(dataFiles * deleteFiles) to O(deleteFiles) and memory
 * from O(concurrent_readers * reader_size) to O(total_deleted_positions).
 */

typedef struct IcebergDeleteIndexEntry
{
	char	key[MAXPGPATH];		/* data file path (hash key) */
	void   *bitmap;				/* Roaring64Map of deleted positions */
} IcebergDeleteIndexEntry;

typedef struct IcebergDeleteIndex
{
	HTAB		   *htab;
	MemoryContext	mcxt;
} IcebergDeleteIndex;

/*
 * Build a global delete index by reading all unique position delete files.
 * Returns NULL if there are no position delete files.
 */
IcebergDeleteIndex *
icebergBuildDeleteIndex(MemoryContext parentMcxt,
						ossFileStream fileStream,
						List *fileScanTasks);

/*
 * Look up the delete bitmap for a given data file path.
 * Returns the Roaring64Map, or NULL if no deletes exist for this file.
 */
void *
icebergDeleteIndexLookup(IcebergDeleteIndex *index, const char *dataFilePath);

/*
 * Destroy the delete index and free all bitmaps.
 */
void
icebergDeleteIndexDestroy(IcebergDeleteIndex *index);

#endif /* ICEBERG_DELETE_INDEX_H */
