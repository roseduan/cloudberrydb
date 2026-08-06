/*-------------------------------------------------------------------------
 *
 * iceberg_file_index.c
 *	  File index management for Iceberg tables
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "iceberg_file_index.h"
#include "nodes/value.h"
#include "utils/memutils.h"

#define ICEBERG_FILE_INDEX_INITIAL_CAPACITY 16

/*
 * Create a new file index map
 */
IcebergFileIndexMap *
icebergCreateFileIndexMap(void)
{
	IcebergFileIndexMap *map;

	map = (IcebergFileIndexMap *) palloc0(sizeof(IcebergFileIndexMap));
	map->capacity = ICEBERG_FILE_INDEX_INITIAL_CAPACITY;
	map->numFiles = 0;
	map->entries = (IcebergFileIndexEntry *) palloc(
		sizeof(IcebergFileIndexEntry) * map->capacity);
	map->context = CurrentMemoryContext;

	elog(DEBUG1, "Created Iceberg file index map");

	return map;
}

/*
 * Free file index map and all its contents
 */
void
icebergFreeFileIndexMap(IcebergFileIndexMap *map)
{
	uint32 i;

	if (map == NULL)
		return;

	elog(DEBUG1, "Destroying Iceberg file index map with %u files", map->numFiles);

	/* Free all file path strings */
	for (i = 0; i < map->numFiles; i++)
	{
		if (map->entries[i].filePath != NULL)
			pfree(map->entries[i].filePath);
	}

	/* Free the entries array */
	if (map->entries != NULL)
		pfree(map->entries);

	/* Free the map structure itself */
	pfree(map);
}

/*
 * Add a file to the index map
 * Returns the assigned file ID (which is the array index)
 */
uint32
icebergAddFile(IcebergFileIndexMap *map, const char *filePath, int64 recordCount)
{
	MemoryContext oldContext = MemoryContextSwitchTo(map->context);
	uint32 fileId;

	Assert(map != NULL);
	Assert(filePath != NULL);
	Assert(recordCount >= 0);

	/* Check if we've reached the maximum number of files */
	if (map->numFiles >= ICEBERG_MAX_FILE_ID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many files in Iceberg scan"),
				 errdetail("Maximum supported files is %u", ICEBERG_MAX_FILE_ID)));
	}

	/* Expand array if needed */
	if (map->numFiles >= map->capacity)
	{
		uint32 newCapacity = map->capacity * 2;

		/* Make sure we don't exceed the maximum file ID */
		if (newCapacity > ICEBERG_MAX_FILE_ID)
			newCapacity = ICEBERG_MAX_FILE_ID;

		map->entries = (IcebergFileIndexEntry *) repalloc(
			map->entries,
			sizeof(IcebergFileIndexEntry) * newCapacity);
		map->capacity = newCapacity;

		elog(DEBUG2, "Expanded Iceberg file index capacity to %u", newCapacity);
	}

	/* File ID is the current number of files (next index) */
	fileId = map->numFiles;

	/* Add entry to array */
	map->entries[fileId].fileId = fileId;
	map->entries[fileId].filePath = pstrdup(filePath);
	map->entries[fileId].recordCount = recordCount;
	map->entries[fileId].partitionValues = NIL;

	map->numFiles++;

	elog(DEBUG1, "Added file to index: %s (ID=%u, records=%ld)",
		 filePath, fileId, recordCount);
	MemoryContextSwitchTo(oldContext);

	return fileId;
}

/*
 * Get file path from file ID
 * Returns NULL if not found
 */
const char *
icebergGetFilePath(IcebergFileIndexMap *map, uint32 fileId)
{
	if (map == NULL || fileId >= map->numFiles)
		return NULL;

	return map->entries[fileId].filePath;
}

/*
 * Set the identity partition tuple for a file ID.  Deep-copies the list into
 * the map's memory context so it survives the caller's per-fragment cleanup.
 * A NULL list cell denotes a SQL NULL partition value; NIL leaves the file
 * unpartitioned.
 */
void
icebergSetFilePartition(IcebergFileIndexMap *map, uint32 fileId, List *partitionValues)
{
	MemoryContext oldContext;
	ListCell   *lc;
	List	   *copy = NIL;

	if (map == NULL || fileId >= map->numFiles)
		return;

	oldContext = MemoryContextSwitchTo(map->context);
	foreach(lc, partitionValues)
	{
		Value *v = (Value *) lfirst(lc);	/* String node, or NULL for SQL NULL */

		if (v == NULL)
			copy = lappend(copy, NULL);
		else
			copy = lappend(copy, makeString(pstrdup(strVal(v))));
	}
	map->entries[fileId].partitionValues = copy;
	MemoryContextSwitchTo(oldContext);
}

/*
 * Get the identity partition tuple for a file ID.  Returns NIL if the file is
 * unknown or unpartitioned.  The list is owned by the map.
 */
List *
icebergGetFilePartition(IcebergFileIndexMap *map, uint32 fileId)
{
	if (map == NULL || fileId >= map->numFiles)
		return NIL;

	return map->entries[fileId].partitionValues;
}

/*
 * Get file record count from file ID
 * Returns -1 if not found
 */
int64
icebergGetRecordCount(IcebergFileIndexMap *map, uint32 fileId)
{
	if (map == NULL || fileId >= map->numFiles)
		return -1;

	return map->entries[fileId].recordCount;
}

/*
 * Get number of files in the map
 */
uint32
icebergGetNumFiles(IcebergFileIndexMap *map)
{
	if (map == NULL)
		return 0;

	return map->numFiles;
}

/*
 * Clear all entries (but keep the map structure)
 */
void
icebergClearFileIndexMap(IcebergFileIndexMap *map)
{
	uint32 i;

	if (map == NULL)
		return;

	elog(DEBUG2, "Clearing Iceberg file index map (%u files)", map->numFiles);

	/* Free all file path strings */
	for (i = 0; i < map->numFiles; i++)
	{
		if (map->entries[i].filePath != NULL)
		{
			pfree(map->entries[i].filePath);
			map->entries[i].filePath = NULL;
		}
		/* Partition lists live in map->context and are reclaimed with it; just
		 * drop the reference so a reused slot does not alias a stale tuple. */
		map->entries[i].partitionValues = NIL;
	}

	map->numFiles = 0;
}

/* icebergEncodeTID moved to iceberg_file_index.h as static inline */

/* Decode TID into file ID and row offset */
void
icebergDecodeTID(ItemPointer tid, uint32 *fileId, int64 *rowOffset)
{
	BlockNumber blkno;
	OffsetNumber offset;

	Assert(ICEBERG_CTID_IS_VALID(tid));

	blkno = ItemPointerGetBlockNumberNoCheck(tid);
	offset = ItemPointerGetOffsetNumberNoCheck(tid);

	/* Decode: fileId = BlockNumber >> 12 */
	*fileId = blkno >> ICEBERG_ROW_OFFSET_HIGH_BITS;

	/* Decode: rowOffset = ((BlockNumber & 0xFFF) << 16) | OffsetNumber */
	*rowOffset = (int64)((blkno & ICEBERG_ROW_OFFSET_HIGH_MASK) << ICEBERG_ROW_OFFSET_LOW_BITS) | offset;
}