/*-------------------------------------------------------------------------
 *
 * iceberg_file_index.h
 *	  File index management for Iceberg tables to encode file information in TID
 *
 * TID Encoding Scheme:
 *   - 20 bits: File ID (0 to 1,048,575 files)
 *   - 28 bits: Row offset within file (0 to 268,435,455 rows)
 *
 * Layout:
 *   BlockNumber (32 bits):  [File ID: 20 bits][Row Offset High: 12 bits]
 *   OffsetNumber (16 bits): [Row Offset Low: 16 bits]
 *
 *-------------------------------------------------------------------------
 */
#ifndef ICEBERG_FILE_INDEX_H
#define ICEBERG_FILE_INDEX_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "storage/itemptr.h"

/* Bit layout constants */
#define ICEBERG_FILE_ID_BITS		20		/* 20 bits for file ID */
#define ICEBERG_ROW_OFFSET_BITS		28		/* 28 bits for row offset */

/* Maximum values */
#define ICEBERG_MAX_FILE_ID			((1 << ICEBERG_FILE_ID_BITS) - 1)		/* 1,048,575 */
#define ICEBERG_MAX_ROW_OFFSET		((1 << ICEBERG_ROW_OFFSET_BITS) - 1)	/* 268,435,455 */

/* Bit manipulation macros */
#define ICEBERG_ROW_OFFSET_HIGH_BITS	12		/* High 12 bits of row offset go into BlockNumber */
#define ICEBERG_ROW_OFFSET_LOW_BITS		16		/* Low 16 bits of row offset go into OffsetNumber */
#define ICEBERG_ROW_OFFSET_LOW_MASK		0xFFFF	/* Mask for low 16 bits */
#define ICEBERG_ROW_OFFSET_HIGH_MASK	0xFFF	/* Mask for high 12 bits in BlockNumber */

#define ICEBERG_CTID_IS_VALID(ptr)	((bool) (PointerIsValid(ptr)))

/*
 * File index entry
 */
typedef struct IcebergFileIndexEntry
{
	uint32		fileId;				/* File ID (0 to 1,048,575) */
	char	   *filePath;			/* Full path to the data file */
	int64		recordCount;		/* Total records in this file */
	List	   *partitionValues;	/* identity partition tuple (spec order),
									 * String nodes / NULL cells; NIL when the
									 * table is unpartitioned. Used to attribute
									 * position-delete files to a partition on
									 * UPDATE/DELETE. Owned by map->context. */
} IcebergFileIndexEntry;

/*
 * File index map for a scan
 * Maintains file ID -> file path mapping per segment using dynamic array
 */
typedef struct IcebergFileIndexMap
{
	IcebergFileIndexEntry *entries;	/* Dynamic array of file entries */
	uint32		numFiles;			/* Number of files currently in the map */
	uint32		capacity;			/* Allocated capacity of the array */
	MemoryContext context;		/* Memory context for allocations */
} IcebergFileIndexMap;

/* Create a new file index map */
extern IcebergFileIndexMap*
icebergCreateFileIndexMap(void);

/* Free file index map and all its contents */
extern void
icebergFreeFileIndexMap(IcebergFileIndexMap *map);

/* Add a file to the index map, returns the assigned file ID */
extern uint32
icebergAddFile(IcebergFileIndexMap *map, const char *filePath, int64 recordCount);

/* Get file path from file ID, returns NULL if not found */
extern const char *
icebergGetFilePath(IcebergFileIndexMap *map, uint32 fileId);

/* Set the identity partition tuple for a file ID (deep-copied into the map's
 * memory context). partitionValues is a List of String nodes / NULL cells in
 * partition-spec order; NIL for unpartitioned. No-op if fileId is unknown. */
extern void
icebergSetFilePartition(IcebergFileIndexMap *map, uint32 fileId, List *partitionValues);

/* Get the identity partition tuple for a file ID, NIL if unknown/unpartitioned.
 * The returned list is owned by the map; callers must not free it. */
extern List *
icebergGetFilePartition(IcebergFileIndexMap *map, uint32 fileId);

/* Get file record count from file ID, returns -1 if not found */
extern int64
icebergGetRecordCount(IcebergFileIndexMap *map, uint32 fileId);

/* Get number of files in the map */
extern uint32
icebergGetNumFiles(IcebergFileIndexMap *map);

/* Clear all entries (but keep the map structure) */
extern void
icebergClearFileIndexMap(IcebergFileIndexMap *map);

/* Encode file ID and row offset into TID — inlined for hot path */
static inline void
icebergEncodeTID(ItemPointer tid, uint32 fileId, int64 rowOffset)
{
	BlockNumber blkno;
	OffsetNumber offset;

	Assert(fileId <= ICEBERG_MAX_FILE_ID);
	Assert(rowOffset >= 0 && rowOffset <= ICEBERG_MAX_ROW_OFFSET);

	blkno = (fileId << ICEBERG_ROW_OFFSET_HIGH_BITS) |
			((rowOffset >> ICEBERG_ROW_OFFSET_LOW_BITS) & ICEBERG_ROW_OFFSET_HIGH_MASK);
	offset = (OffsetNumber)(rowOffset & ICEBERG_ROW_OFFSET_LOW_MASK);
	ItemPointerSet(tid, blkno, offset);
}

/* Decode TID into file ID and row offset */
extern void
icebergDecodeTID(ItemPointer tid, uint32 *fileId, int64 *rowOffset);

/*
 * Populate the file index map with ALL files from all segments' fragments.
 * This ensures globally consistent file IDs across all segments, which is
 * required for Iceberg UPDATE/DELETE when Redistribute Motion is involved.
 * allFragments is the complete fragment list: [metadata, task0, task1, ...].
 */
extern void
icebergFileIndexMapPopulateFromAllFragments(IcebergFileIndexMap *map,
											List *allFragments);

#endif /* ICEBERG_FILE_INDEX_H */
