#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "nodes/parsenodes.h"
#include "gopher/gopher.h"
#include "common/hashfn.h"

#include "src/common/fileSystemWrapper.h"
#include "src/provider/iceberg/iceberg_task_reader.h"
#include "src/provider/iceberg/iceberg_file_index.h"
#include "src/provider/iceberg/iceberg_delete_index.h"
#include "src/provider/hudi/hudi_task_reader.h"
#include "row_reader.h"
#include "file_reader.h"
#include "src/common/dataBufferArray_c.h"
#include "src/datalake_def.h"

extern bool disableCacheFile;

static bool resownerCallbackRegistered;
static DatalakeRemoteFileHandle *openRemoteHandles;

static void flatCombinedTasks(List *combinedScanTasks, List **fileScanTasks);
static bool isCacheEnabled(char *cacheEnabled);
static void icebergFileIndexMapInitialize(DatalakeRowReader *reader);

static List *
createFieldDescription(TupleDesc tupleDesc)
{
	int i;
	List *result = NIL;

	for (i = 0; i < tupleDesc->natts; i++)
	{
		DatalakeFieldDescription *fieldDesc = (DatalakeFieldDescription *) palloc0(sizeof(DatalakeFieldDescription));
		Oid typeOid = TupleDescAttr(tupleDesc, i)->atttypid;
		int typeMod = TupleDescAttr(tupleDesc, i)->atttypmod;
		const char *attname = NameStr(TupleDescAttr(tupleDesc, i)->attname);

		strcpy(fieldDesc->name, attname);
		fieldDesc->typeOid = typeOid;
		fieldDesc->typeMod = typeMod;

		result = lappend(result, fieldDesc);
	}

	return result;
}

static DatalakeRemoteFileHandle *
createRemoteFileHandle(gopherFS gopherFilesystem)
{
	DatalakeRemoteFileHandle *result;

	result = MemoryContextAlloc(TopMemoryContext, sizeof(DatalakeRemoteFileHandle));
	result->gopherFilesystem = gopherFilesystem;
	result->reader = NULL;
	result->prev = NULL;
	result->next = openRemoteHandles;
	result->owner = CurrentResourceOwner;

	if (openRemoteHandles)
		openRemoteHandles->prev = result;

	openRemoteHandles = result;

	return result;
}

static void
destroyRemoteFileHandle(DatalakeRemoteFileHandle *handle)
{
	if (handle->prev)
		handle->prev->next = handle->next;
	else
		openRemoteHandles = openRemoteHandles->next;

	if (handle->next)
		handle->next->prev = handle->prev;

	if (handle->gopherFilesystem)
		gopherDisconnect(handle->gopherFilesystem);

	if (handle->reader)
		datalakeRowReaderClose(handle->reader);

	pfree(handle);
}

static void
remoteFileAbortCallback(ResourceReleasePhase phase,
						bool isCommit,
						bool isTopLevel,
						void *arg)
{
	DatalakeRemoteFileHandle *cur;
	DatalakeRemoteFileHandle *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = openRemoteHandles;
	while (next)
	{
		cur = next;
		next = cur->next;

		if (cur->owner == CurrentResourceOwner)
			destroyRemoteFileHandle(cur);
	}
}

static Reader icebergHandler = {
	createIcebergTaskReader,
	icebergTaskReaderNext,
	icebergTaskReaderClose
};

static Reader hudiHandler = {
	createHudiTaskReader,
	hudiTaskReaderNext,
	hudiTaskReaderClose
};

static Reader deltaLakeHandler = {
	NULL,
	NULL,
	NULL
};

typedef struct FileMapEntry {
    char    filename[MAXPGPATH];  /* key: inline string storage */
    int     file_id;
} FileMapEntry;

/*
 * Custom hash func: Dereference char** key and hash the string content.
 * The hash table key is a char* pointer (keysize = sizeof(char*)).
 * hash_search passes a pointer TO that key, so key is actually char**.
 */
static uint32
filename_hash(const void *key, Size keysize)
{
    const char *str = *(const char *const *) key;
    return DatumGetUInt32(hash_any((const unsigned char *) str, strlen(str)));
}

/*
 * Custom hash cmp: Dereference char** keys and compare the string content.
 */
static int
filename_match(const void *key1, const void *key2, Size keysize)
{
    const char *s1 = *(const char *const *) key1;
    const char *s2 = *(const char *const *) key2;

    return strcmp(s1, s2);
}

DatalakeRowReader *
datalakeCreateRowReader(MemoryContext mcxt,
				TupleDesc tupleDesc,
				int nTblColumn,
				bool *attrUsed,
				gopherFS gopherFilesystem,
				List *combinedScanTasks,
				DLTblFmt format,
				ExternalTableMetadata *tableOptions)
{
	MemoryContext oldcxt;
	DatalakeRowReader *reader = MemoryContextAllocZero(TopMemoryContext,
													sizeof(DatalakeRowReader));

	/*
	 * Allocate all reader-owned data in TopMemoryContext so they survive
	 * executor context teardown during abort.  The abort callback
	 * (remoteFileAbortCallback -> datalakeRowReaderClose) runs after
	 * AtAbort_Portals() has already destroyed the executor context.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Keep the task list in its own context (child of TopMemoryContext so it
	 * still survives executor teardown during abort).  Unlike the old design
	 * — where the per-file reader pfree()d each task as it finished — the list
	 * is kept intact for the whole scan so it can be re-read on a rewind
	 * (rescan); it is freed exactly once in datalakeRowReaderClose().
	 */
	reader->taskListMcxt = AllocSetContextCreate(TopMemoryContext,
												 "RowReaderTaskListContext",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(reader->taskListMcxt);
	flatCombinedTasks(combinedScanTasks, &reader->fileScanTasks);
	MemoryContextSwitchTo(TopMemoryContext);
	list_free(combinedScanTasks);

	reader->datafileDesc = createFieldDescription(tupleDesc);
	reader->attrUsed = attrUsed;
	reader->gopherFilesystem = gopherFilesystem;
	reader->mcxt = mcxt;
	reader->tableOptions = tableOptions;
	reader->buffer = datalake_buffer_arr_create(tupleDesc->natts + 8);
	reader->format = format;
	reader->fileIndexMapInitialized = false;  /* Will be lazily initialized on first iteration */

	MemoryContextSwitchTo(oldcxt);

	/* Set handler based on format */
	if (FORMAT_IS_ICEBERG(format))
		reader->handler = &icebergHandler;
	else if (FORMAT_IS_HUDI(format))
		reader->handler = &hudiHandler;
	else
		reader->handler = &deltaLakeHandler;

	reader->taskMcxt = AllocSetContextCreate(TopMemoryContext,
											 "RowReaderContext",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	reader->curMcxt = CurrentMemoryContext;

	return reader;
}

bool
datalakeRowReaderNext(DatalakeRowReader *reader, DatalakeInternalRecord *record)
{
	/*
	 * Save caller's CurrentMemoryContext so we can restore it before we return.
	 * Internally we switch to reader->taskMcxt around per-task setup; if we let
	 * that switch leak out, the caller (executor) ends up running in taskMcxt
	 * and any TupleTableSlot it allocates during ExecForeignUpdate gets stored
	 * in taskMcxt -- which is then reset on the next call here, wiping the
	 * slot memory while the slot is still referenced from estate->es_tupleTable
	 * (use-after-free, manifests as 0x7F7F7F7F NodeTag at ExecResetTupleTable).
	 */
	MemoryContext caller_cxt = CurrentMemoryContext;

	/*
	 * For Iceberg tables, lazily initialize fileIndexMap on first iteration.
	 * This ensures we only build the index when actually needed (i.e., during UPDATE/DELETE),
	 * avoiding unnecessary overhead for plain SELECT queries.
	 */
	if (FORMAT_IS_ICEBERG(reader->format) &&
		datalake_iceberg_file_index_map != NULL &&
		!reader->fileIndexMapInitialized)
	{
		icebergFileIndexMapInitialize(reader);
	}

	/*
	 * For Iceberg tables, lazily build a global position delete index on first
	 * iteration.  This reads all position delete files once and builds a
	 * hash table of (dataFilePath -> bitmap), so each task can look up its
	 * delete set in O(1) instead of re-reading all delete files.
	 */
	if (FORMAT_IS_ICEBERG(reader->format) && !reader->deleteIndexBuilt)
	{
		reader->deleteIndex = icebergBuildDeleteIndex(TopMemoryContext,
													  reader->gopherFilesystem,
													  reader->fileScanTasks);
		reader->deleteIndexBuilt = true;
	}

	while (true)
	{
		if (reader->handler->Next(reader->curReader, record))
		{
			/*
			 * Cache the innermost format reader for deep fast path.
			 * Traverse: curReader (IcebergTaskReader) → dataReader
			 * (FileReader) → formatReader->Next (parquet_next) +
			 * dataReader (BaseFileReader*).
			 *
			 * This is done in C code (same compilation unit as struct
			 * definitions) to avoid C/C++ ABI mismatch issues.
			 */
			if (FORMAT_IS_ICEBERG(reader->format) &&
				reader->deepNext == NULL &&
				reader->deleteIndex == NULL)
			{
				IcebergTaskReader *taskReader = (IcebergTaskReader *) reader->curReader;
				if (taskReader && taskReader->dataReader)
				{
					FileReader *fileReader = (FileReader *) taskReader->dataReader;
					if (fileReader->formatReader &&
						fileReader->formatReader->Next &&
						fileReader->dataReader)
					{
						reader->deepNext = fileReader->formatReader->Next;
						reader->deepReader = fileReader->dataReader;
						reader->deepFileId = taskReader->fileId;
					}
				}
			}
			MemoryContextSwitchTo(caller_cxt);
			return true;
		}
		else if (reader->curTaskIndex < list_length(reader->fileScanTasks))
		{
			DatalakeReaderInitInfo initInfo;
			FileScanTask *curTask;

			/* Clear deep cache — old file's reader is about to be freed */
			reader->deepNext = NULL;
			reader->deepReader = NULL;

			reader->handler->Close(reader->curReader);
			MemoryContextSwitchTo(reader->curMcxt);

			curTask = list_nth(reader->fileScanTasks, reader->curTaskIndex);

			initInfo.taskId = reader->curReaderIndex++;
			initInfo.mcxt = reader->mcxt;
			initInfo.datafileDesc = reader->datafileDesc;
			initInfo.attrUsed = reader->attrUsed;
			initInfo.gopherFilesystem = reader->gopherFilesystem;
			initInfo.fileScanTask = curTask;
			initInfo.tableOptions = reader->tableOptions;
			initInfo.buffer = reader->buffer;
			initInfo.deleteIndex = reader->deleteIndex;

			/* For Iceberg tables, use the file ID stored in the task */
			if (datalake_iceberg_file_index_map != NULL)
			{
				uint32 fileId = curTask->fileId;
				initInfo.fileId = fileId;
				elog(DEBUG2, "Task %d using file ID %u for %s",
					 initInfo.taskId, fileId, curTask->dataFile->filePath);
			}
			else
			{
				initInfo.fileId = 0;  /* Not used for non-Iceberg tables */
			}

			MemoryContextReset(reader->taskMcxt);
			MemoryContextSwitchTo(reader->taskMcxt);

			/*
			 * Hand the per-file reader a private copy of the task: the read
			 * path (task reader, file reader, filters) destructively frees the
			 * task, its dataFile/filePath and deletes when the file is closed.
			 * The master list in taskListMcxt must stay intact so the scan can
			 * be rewound (rescan).  The copy lives in taskMcxt (reset per file).
			 */
			initInfo.fileScanTask = (FileScanTask *) copyObject(curTask);

			reader->curReader = reader->handler->Create(&initInfo);
			/*
			 * Advance the cursor instead of consuming the list, so the task
			 * list stays intact and the scan can be rewound (rescan).  See
			 * datalakeRowReaderRewind().
			 */
			reader->curTaskIndex++;
		}
		else
		{
			reader->handler->Close(reader->curReader);
			MemoryContextSwitchTo(reader->curMcxt);
			reader->curReader = NULL;
			MemoryContextSwitchTo(caller_cxt);
			return false;
		}
	}

	MemoryContextSwitchTo(caller_cxt);
	return false;
}

/*
 * datalakeRowReaderRewind
 *		Reset the reader to re-scan its tasks from the beginning.
 *
 * The scan is restarted in place: we close the file reader that is currently
 * open and rewind the task cursor to 0, so the next datalakeRowReaderNext()
 * re-opens the first task (this mirrors the initial-scan path, where curReader
 * starts NULL and the first Next() falls through to open task 0).
 *
 * State that is identical across passes is deliberately KEPT to avoid
 * re-reading object storage / rebuilding indexes:
 *   - fileScanTasks            (the task list itself; only the cursor resets)
 *   - deleteIndex / deleteIndexBuilt
 *   - the global Iceberg file index map / fileIndexMapInitialized
 *   - datafileDesc, buffer, taskMcxt
 * We must NOT call datalakeRowReaderClose() here: it destroys deleteIndex,
 * datafileDesc and taskMcxt and pfree()s the reader.
 */
void
datalakeRowReaderRewind(DatalakeRowReader *reader)
{
	/* Close() tolerates a NULL curReader (e.g. after a fully drained scan). */
	reader->handler->Close(reader->curReader);
	reader->curReader = NULL;

	/* Restart task iteration from the first file. */
	reader->curTaskIndex = 0;
	reader->curReaderIndex = 0;

	/* Drop the fast-path cache; it points into the now-closed file reader. */
	reader->deepNext = NULL;
	reader->deepReader = NULL;
	reader->deepFileId = 0;
}

void
datalakeRowReaderClose(DatalakeRowReader *reader)
{
	if (reader->curReader)
		reader->handler->Close(reader->curReader);

	if (reader->deleteIndex)
		icebergDeleteIndexDestroy((IcebergDeleteIndex *) reader->deleteIndex);

	list_free_deep(reader->datafileDesc);
	MemoryContextDelete(reader->taskMcxt);
	if (reader->taskListMcxt)
		MemoryContextDelete(reader->taskListMcxt);
	if (reader->buffer)
		datalake_buffer_arr_destroy(reader->buffer);
	pfree(reader);
}

static void
flatCombinedTasks(List *combinedScanTasks, List **fileScanTasks)
{
	ListCell *lco;
	ListCell *lci;
	List *combinedScanTask;

	foreach(lco, combinedScanTasks)
	{
		combinedScanTask = (List *) lfirst(lco);

		foreach(lci, combinedScanTask)
		{
			FileScanTask *task = (FileScanTask *) lfirst(lci);

			/*
			 * Deep-copy the FileScanTask into the current memory context
			 * (TopMemoryContext).  The original nodes were deserialized in
			 * MessageContext on QE, which is reset before the first scan
			 * iteration, leaving dangling pointers.
			 */
			task = copyObject(task);

			*fileScanTasks = lappend(*fileScanTasks, task);
		}

		list_free(combinedScanTask);
	}
}

/*
 * icebergFileIndexMapPopulateFromAllFragments
 *
 * Populate the global file index map using the COMPLETE fragment list that
 * every segment received from the coordinator.  Because all segments iterate
 * the same ordered list, each segment assigns the same file ID to the same
 * file path, making file IDs globally consistent across segments.
 *
 * This is critical for UPDATE/DELETE: the executor may Redistribute Motion
 * rows to a different segment than the one that scanned them.  The receiving
 * segment must be able to decode the ctid (which contains a file ID) and map
 * it back to the correct file path.
 *
 * allFragments layout: [ExternalTableMetadata, combinedTask0, combinedTask1, ...]
 * Each combinedTask is a List of FileScanTask.
 */
void
icebergFileIndexMapPopulateFromAllFragments(IcebergFileIndexMap *map,
											List *allFragments)
{
	ListCell   *lco;
	ListCell   *lci;
	int			i = 0;
	uint32		fileCount = 0;
	HTAB	   *filePathToIdMap = NULL;
	HASHCTL		hashCtl;

	Assert(map != NULL);
	Assert(allFragments != NIL);

	/* Create hash set for file path deduplication */
	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(char *);
	hashCtl.entrysize = sizeof(FileMapEntry);
	hashCtl.hash = filename_hash;
	hashCtl.match = filename_match;

	filePathToIdMap = hash_create("Iceberg global file path to ID map",
								  1024,
								  &hashCtl,
								  HASH_ELEM | HASH_COMPARE | HASH_FUNCTION);

	/* Iterate ALL combined tasks (not just this segment's) */
	foreach_with_count(lco, allFragments, i)
	{
		List	   *combinedScanTask;

		if (i == 0)
			continue;			/* skip ExternalTableMetadata */

		combinedScanTask = (List *) lfirst(lco);

		foreach(lci, combinedScanTask)
		{
			FileScanTask   *task = (FileScanTask *) lfirst(lci);
			const char	   *filePath = task->dataFile->filePath;
			int64			recordCount = task->dataFile->recordCount;
			bool			found;
			FileMapEntry   *entry;

			entry = (FileMapEntry *) hash_search(filePathToIdMap,
												 (void *) &filePath,
												 HASH_FIND, &found);
			if (!found)
			{
				uint32 fileId;

				fileId = icebergAddFile(map, filePath, recordCount);

				entry = (FileMapEntry *) hash_search(filePathToIdMap,
													 (void *) &filePath,
													 HASH_ENTER, &found);
				entry->file_id = fileId;
				fileCount++;
			}
		}
	}

	hash_destroy(filePathToIdMap);

	elog(DEBUG1, "Populated global Iceberg file index with %u unique files from all fragments",
		 fileCount);
}

/*
 * Lazy initialization function for Iceberg file index map on a per-reader basis.
 *
 * When the global map has been pre-populated by
 * icebergFileIndexMapPopulateFromAllFragments (the UPDATE/DELETE path),
 * this function only needs to look up the already-assigned file IDs for
 * this segment's local tasks.
 *
 * When the global map is empty (legacy / fallback path), this function
 * populates it from the local tasks only, which is fine for SELECT or
 * single-segment scenarios.
 */
static void
icebergFileIndexMapInitialize(DatalakeRowReader *reader)
{
	ListCell *lc;
	uint32 fileCount = 0;
	HTAB *filePathToIdMap = NULL;  /* Map: filePath -> fileId */
	HASHCTL hashCtl;

	Assert(!reader->fileIndexMapInitialized);

	/* Create hash set for file path deduplication */
	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(char*);
	hashCtl.entrysize = sizeof(FileMapEntry);
	hashCtl.hash = filename_hash;
	hashCtl.match = filename_match;
	hashCtl.hcxt = reader->mcxt;

	filePathToIdMap = hash_create("Iceberg file path to ID map",
	                              1024,
	                              &hashCtl,
	                              HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	/*
	 * If the map was pre-populated from all fragments, build a reverse
	 * lookup (filePath -> fileId) from the existing map entries so we can
	 * quickly assign file IDs to local tasks.
	 */
	if (datalake_iceberg_file_index_map->numFiles > 0)
	{
		uint32 nfiles = icebergGetNumFiles(datalake_iceberg_file_index_map);

		for (uint32 fid = 0; fid < nfiles; fid++)
		{
			const char	   *fp = icebergGetFilePath(datalake_iceberg_file_index_map, fid);
			bool			found;
			FileMapEntry   *entry;

			entry = (FileMapEntry *) hash_search(filePathToIdMap,
												 (void *) &fp,
												 HASH_ENTER, &found);
			entry->file_id = fid;
		}
		fileCount = nfiles;
	}

	/* Assign file IDs to each local task */
	foreach(lc, reader->fileScanTasks)
	{
		FileScanTask *task = (FileScanTask *) lfirst(lc);
		const char *filePath = task->dataFile->filePath;
		int64 recordCount = task->dataFile->recordCount;
		bool found;
		FileMapEntry *entry;
		uint32 fileId;

		entry = (FileMapEntry *) hash_search(filePathToIdMap,
		                                     (void *) &filePath,
		                                     HASH_FIND, &found);

		if (!found)
		{
			/* File not in map yet (fallback: map was not pre-populated) */
			fileId = icebergAddFile(datalake_iceberg_file_index_map, filePath, recordCount);

			entry = (FileMapEntry *) hash_search(filePathToIdMap,
			                                     (void *) &filePath,
			                                     HASH_ENTER, &found);
			entry->file_id = fileId;
			fileCount++;
		}
		else
		{
			fileId = entry->file_id;
		}

		task->fileId = fileId;
	}

	hash_destroy(filePathToIdMap);

	elog(DEBUG1, "Iceberg file index initialized for reader with %u unique files", fileCount);

	reader->fileIndexMapInitialized = true;
}

static bool
checkInterrupt(void)
{
	if (!InterruptPending)
		return false;

	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return false;

	return true;
}

DatalakeProtocolContext *
datalakeCreateContext(dataLakeOptions *options)
{
	DatalakeProtocolContext *context;
	gopherConfig    *gopherConfig;
	gopherFS        fs;
	bool            saved_disable_cache;

	context = (DatalakeProtocolContext *)palloc0(sizeof(DatalakeProtocolContext));

	/*
	 * Only let an explicit per-table cache_enabled option override the GUC
	 * datalake.disable_cache_file. When the option is unset (e.g. native
	 * iceberg AM tables), keep the GUC value so it globally controls caching
	 * (default OFF = cache on) instead of being forced on every scan.
	 *
	 * disableCacheFile is the live GUC-backed variable, read by other callers
	 * later in the session (config.c, rewrLogical.cpp, gopher_random_file.cpp).
	 * Scope the per-table override to exactly the datalakeCreateGopherConfig
	 * call below -- its only consumer on this path -- and restore the GUC
	 * value afterwards so the override never leaks across scans.
	 */
	saved_disable_cache = disableCacheFile;
	PG_TRY();
	{
		if (options->cache_enabled != NULL)
			disableCacheFile = !isCacheEnabled(options->cache_enabled);
		gopherConfig = datalakeCreateGopherConfig((void*)(options->gopher));
	}
	PG_FINALLY();
	{
		disableCacheFile = saved_disable_cache;
	}
	PG_END_TRY();
	gopherUserCanceledCallBack(&checkInterrupt);

	fs = gopherConnect(*gopherConfig);
	if (fs == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to connect to gopher: %s", gopherGetLastError())));

	datalakeGopherConfigDestroy(gopherConfig);

	if (!resownerCallbackRegistered)
	{
		RegisterResourceReleaseCallback(remoteFileAbortCallback, NULL);
		resownerCallbackRegistered = true;
	}

	context->file = createRemoteFileHandle(fs);

	return context;
}

static bool
isCacheEnabled(char *cacheEnabled)
{
	if (cacheEnabled)
	{
		if (pg_strcasecmp(cacheEnabled, "false") == 0)
			return false;
		else if (pg_strcasecmp(cacheEnabled, "true") == 0)
			return true;
		else
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid value for boolean option \"cache_enabled\": %s", cacheEnabled)));
	}

	return true;
}

void
datalakeCleanupContext(DatalakeProtocolContext *context)
{
	if (context->file)
		destroyRemoteFileHandle(context->file);

	if (context->rowContext)
		MemoryContextDelete(context->rowContext);

	if (context->record)
		pfree(context->record);

	pfree(context);
}

void
datalakeProtocolImportStart(dataLakeFdwScanState *scanstate, DatalakeProtocolContext *context, bool *attrUsed)
{
	int       i = 0;
	ListCell *lc;
	int       numSegments = getgpsegmentCount();
	List     *combinedScanTasks = NIL;

    ExternalTableMetadata *tableOptions = (ExternalTableMetadata *)linitial(scanstate->fragments);

	foreach_with_count(lc, scanstate->fragments, i)
	{
		if (i == 0) continue;
		int idx = i - 1;
		List *combinedScanTask = (List *) lfirst(lc);

		if (GpIdentity.segindex == (idx % numSegments))
		{
			if (list_length(combinedScanTask) > 0)
			{
				combinedScanTasks = lappend(combinedScanTasks, combinedScanTask);
			}
		}
	}

	context->rowContext = AllocSetContextCreate(CurrentMemoryContext,
												"DatalakeExtensionContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	context->file->reader = datalakeCreateRowReader(context->rowContext,
													scanstate->scan_tupdesc,
													scanstate->rel->rd_att->natts,
													attrUsed,
													context->file->gopherFilesystem,
													combinedScanTasks,
													scanstate->options->format,
													tableOptions);
}
