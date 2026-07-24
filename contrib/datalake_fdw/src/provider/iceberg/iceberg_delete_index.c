#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/file_reader.h"
#include "src/provider/common/delete_bitmap_c.h"
#include "src/am_iceberg/include/pg_iceberg_guc.h"
#include "iceberg_delete_index.h"

/*
 * Collect all unique position delete files from the task list.
 * Uses a hash set on file path to deduplicate (same delete file appears
 * in many tasks due to the cartesian product in scan planning).
 */
static List *
collectUniquePositionDeletes(List *fileScanTasks)
{
	HASHCTL		ctl;
	HTAB	   *seen;
	List	   *result = NIL;
	ListCell   *lc;
	ListCell   *lcd;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = MAXPGPATH;

	seen = hash_create("DeleteFileDedup", 256, &ctl, HASH_ELEM | HASH_STRINGS);

	foreach(lc, fileScanTasks)
	{
		FileScanTask *task = (FileScanTask *) lfirst(lc);

		foreach(lcd, task->deletes)
		{
			FileFragment *frag = (FileFragment *) lfirst(lcd);
			bool		  found;

			if (frag->content != POSITION_DELETES)
				continue;

			hash_search(seen, frag->filePath, HASH_ENTER, &found);
			if (!found)
				result = lappend(result, frag);
		}
	}

	hash_destroy(seen);
	return result;
}

/*
 * Collect the set of data file paths that this segment will actually read.
 * Used to filter out irrelevant entries when building the delete index.
 */
static HTAB *
collectLocalDataFiles(List *fileScanTasks)
{
	HASHCTL		ctl;
	HTAB	   *localFiles;
	ListCell   *lc;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = MAXPGPATH;

	localFiles = hash_create("LocalDataFiles", 128, &ctl, HASH_ELEM | HASH_STRINGS);

	foreach(lc, fileScanTasks)
	{
		FileScanTask *task = (FileScanTask *) lfirst(lc);
		hash_search(localFiles, task->dataFile->filePath, HASH_ENTER, NULL);
	}

	return localFiles;
}

static List *
createDeleteSchema(void)
{
	DatalakeFieldDescription *filePath = palloc0(sizeof(DatalakeFieldDescription));
	DatalakeFieldDescription *pos = palloc0(sizeof(DatalakeFieldDescription));

	strcpy(filePath->name, "file_path");
	filePath->typeOid = TEXTOID;
	filePath->typeMod = -1;

	strcpy(pos->name, "pos");
	pos->typeOid = INT8OID;
	pos->typeMod = -1;

	return list_make2(filePath, pos);
}

IcebergDeleteIndex *
icebergBuildDeleteIndex(MemoryContext parentMcxt,
						ossFileStream fileStream,
						List *fileScanTasks)
{
	List		   *uniqueDeletes;
	ListCell	   *lc;
	HASHCTL			ctl;
	HTAB		   *localFiles;
	MemoryContext	indexMcxt;
	MemoryContext	oldcxt;
	IcebergDeleteIndex *index;
	List		   *schema;
	int				nfiles;
	int64			totalRecords = 0;
	int64			keptRecords = 0;

	uniqueDeletes = collectUniquePositionDeletes(fileScanTasks);
	nfiles = list_length(uniqueDeletes);
	if (nfiles == 0)
	{
		list_free(uniqueDeletes);
		return NULL;
	}

	/*
	 * Build a set of data file paths that this segment will actually scan.
	 * When reading delete files, we skip entries for data files not in this
	 * set, avoiding bitmap creation and hash table entries for files handled
	 * by other segments.  This reduces memory from O(all_data_files) to
	 * O(local_data_files) — typically a ~48x reduction with 48 segments.
	 */
	localFiles = collectLocalDataFiles(fileScanTasks);

	indexMcxt = AllocSetContextCreate(parentMcxt,
									  "IcebergDeleteIndex",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(indexMcxt);

	index = palloc0(sizeof(IcebergDeleteIndex));
	index->mcxt = indexMcxt;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = sizeof(IcebergDeleteIndexEntry);
	ctl.hcxt = indexMcxt;

	index->htab = hash_create("IcebergDeleteIndex",
							  128,
							  &ctl,
							  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	schema = createDeleteSchema();

	elog(LOG, "iceberg delete index: reading %d unique position delete files, "
		 "%ld local data files",
		 nfiles, hash_get_num_entries(localFiles));

	/*
	 * Read all delete files sequentially, one at a time.
	 * For each (file_path, pos) record, only keep entries where file_path
	 * matches a data file assigned to this segment.
	 */
	foreach(lc, uniqueDeletes)
	{
		FileFragment   *deleteFile = (FileFragment *) lfirst(lc);
		Reader		   *reader;
		bool			attrUsed[2] = {true, true};
		bool			nulls[2] = {false, false};
		Datum			values[2] = {0, 0};
		DatalakeInternalRecord record = {values, nulls, 0};

		/*
		 * datalakeCreateFileReader stores the FileFragment pointer and
		 * fileReaderClose() will pfree() it.  Pass a private copy so the
		 * original fragment in the task's deletes list is not destroyed.
		 */
		{
			FileFragment *deleteCopy = copyObject(deleteFile);
			reader = (Reader *) datalakeCreateFileReader(indexMcxt, schema, attrUsed,
														 true, deleteCopy,
														 fileStream, -1, -1, NULL, NIL);
		}

		while (reader->Next(reader, &record))
		{
			char   *path;
			int		pathLen;
			int64	position;
			char	keybuf[MAXPGPATH];
			int		copyLen;

			pathLen = VARSIZE_ANY_EXHDR(values[0]);
			path = VARDATA_ANY(values[0]);
			position = DatumGetInt64(values[1]);
			totalRecords++;

			copyLen = Min(pathLen, MAXPGPATH - 1);
			memcpy(keybuf, path, copyLen);
			keybuf[copyLen] = '\0';

			/* Batch text Datums point into the Parquet reader's slab. */
			if (!datalakeFileReaderDatumOwned(reader, 0))
				pfree(DatumGetPointer(values[0]));

			/* Skip entries for data files not assigned to this segment */
			if (hash_search(localFiles, keybuf, HASH_FIND, NULL) == NULL)
				continue;

			{
				bool	found;
				IcebergDeleteIndexEntry *entry;

				entry = hash_search(index->htab, keybuf, HASH_ENTER, &found);
				if (!found)
					entry->bitmap = datalakeCreateBitmap();

				datalakeBitmapAdd(entry->bitmap, (uint64) position);
				keptRecords++;
			}
		}

		reader->Close(reader);
	}

	list_free_deep(schema);
	list_free(uniqueDeletes);
	hash_destroy(localFiles);

	MemoryContextSwitchTo(oldcxt);

	elog(LOG, "iceberg delete index: built index with %ld entries "
		 "(%ld/%ld records kept, %.1f%% filtered)",
		 hash_get_num_entries(index->htab),
		 keptRecords, totalRecords,
		 totalRecords > 0 ? 100.0 * (1.0 - (double)keptRecords / totalRecords) : 0.0);

	return index;
}

void *
icebergDeleteIndexLookup(IcebergDeleteIndex *index, const char *dataFilePath)
{
	IcebergDeleteIndexEntry *entry;

	if (index == NULL || index->htab == NULL)
		return NULL;

	entry = hash_search(index->htab, dataFilePath, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	return entry->bitmap;
}

void
icebergDeleteIndexDestroy(IcebergDeleteIndex *index)
{
	HASH_SEQ_STATUS	status;
	IcebergDeleteIndexEntry *entry;

	if (index == NULL)
		return;

	/* Destroy all bitmaps */
	hash_seq_init(&status, index->htab);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->bitmap != NULL)
			datalakeDestroyBitmap(entry->bitmap);
	}

	/* The hash table and all memory are in indexMcxt, just delete it */
	MemoryContextDelete(index->mcxt);
}
