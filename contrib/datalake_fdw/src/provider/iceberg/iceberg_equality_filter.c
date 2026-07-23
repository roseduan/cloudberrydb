#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_equality_filter.h"

typedef struct EqualityHashEntry {
	InternalRecordWrapper *recordKey;
	InternalRecordWrapper *recordValue;
} EqualityHashEntry;

static bool equalityFilterNext(Reader *filter, DatalakeInternalRecord *record);
static void equalityFilterClose(Reader *filter);
static List *readEqualityDeletes(MemoryContext filterMcxt,
								 MemoryContext readerMcxt,
								 List *datafileDesc,
								 ossFileStream fileStream,
								 List *deletes);
static bool deletesSetsContains(DatalakeEqualityFilter *filter, DatalakeInternalRecord *record);

static Reader methods = {
	NULL,
	equalityFilterNext,
	equalityFilterClose,
};

DatalakeEqualityFilter *
datalakeCreateEqualityFilter(MemoryContext readerMcxt,
					 List *datafileDesc,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 List *deletes)
{
	DatalakeEqualityFilter *filter = palloc0(sizeof(DatalakeEqualityFilter));

	elog(DEBUG1, "create iceberg equality filter");

	filter->base = methods;
	filter->dataReader = dataReader;
	filter->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "EqualityFilterContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	filter->deletesSets = readEqualityDeletes(filter->mcxt, readerMcxt, datafileDesc, fileStream, deletes);
	return filter;
}

static bool
equalityFilterNext(Reader *filter, DatalakeInternalRecord *record)
{
	bool isDeleted;
	DatalakeEqualityFilter *equalityFilter = (DatalakeEqualityFilter *) filter;

	while(equalityFilter->dataReader->Next(equalityFilter->dataReader, record))
	{
		isDeleted = deletesSetsContains(equalityFilter, record);
		if (!isDeleted)
			return true;
	}

	return false;
}

static void
equalityFilterClose(Reader *filter)
{
	DatalakeEqualityFilter *equalityFilter = (DatalakeEqualityFilter *) filter;

	equalityFilter->dataReader->Close(equalityFilter->dataReader);
	MemoryContextDelete(equalityFilter->mcxt);
	pfree(equalityFilter);

	elog(DEBUG1, "close iceberg equality filter");
}

static bool
deletesSetsContains(DatalakeEqualityFilter *filter, DatalakeInternalRecord *record)
{
	bool           found = false;
	ListCell      *lc;
	MemoryContext  oldMcxt;
	InternalRecordWrapper  recordWrapper;
	InternalRecordWrapper *pRecordWrapper = &recordWrapper;
	EqualityHashEntry *entry;

	recordWrapper.record = *record;

	oldMcxt = MemoryContextSwitchTo(filter->mcxt);

	foreach(lc, filter->deletesSets)
	{
		DatalakeInternalRecordDesc *deletesSet = (DatalakeInternalRecordDesc *) lfirst(lc);
		recordWrapper.recordDesc = deletesSet;

		entry = (EqualityHashEntry *) hash_search(deletesSet->hashTab, &pRecordWrapper, HASH_FIND, NULL);
		if (entry)
		{
			found = true;
			break;
		}
	}

	MemoryContextSwitchTo(oldMcxt);

	return found;
}

static HTAB *
createHashTable(MemoryContext mcxt, int64 totalRecords)
{
	HTAB    *hashTab;
	HASHCTL  hashCtl;

	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(InternalRecordWrapper *);
	hashCtl.entrysize = sizeof(EqualityHashEntry);
	hashCtl.hash = datalakeRecordHash;
	hashCtl.match = datalakeRecordMatch;
	hashCtl.hcxt = mcxt;
	hashTab = hash_create("EqualityFilterTable", totalRecords, &hashCtl,
						   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return hashTab;
}

static void
createDeletesReaderResources(MemoryContext mcxt,
							 List *datafileDesc,
							 FileFragment **deleteFile,
							 DatalakeInternalRecordDesc **deletesSet,
							 List *deletes)
{
	int i;
	ListCell *lc;

	*deletesSet = palloc(sizeof(DatalakeInternalRecordDesc));

	(*deleteFile) = list_nth(deletes, 0);

	(*deletesSet)->hashTab = createHashTable(mcxt, (*deleteFile)->recordCount);
	(*deletesSet)->nColumns = list_length(datafileDesc);
	(*deletesSet)->nKeys = list_length((*deleteFile)->eqColumnNames);
	(*deletesSet)->keyIndexes = datalakeCreateRecordKeyIndexes((*deleteFile)->eqColumnNames, datafileDesc);
	(*deletesSet)->attrType = palloc0(sizeof(Oid) * list_length(datafileDesc));
	(*deletesSet)->attrUsed = datalakeCreateDeleteProjectionColumns((*deleteFile)->eqColumnNames, datafileDesc);

	foreach_with_count(lc, datafileDesc, i)
	{
		DatalakeFieldDescription *entry = (DatalakeFieldDescription *) lfirst(lc);
		(*deletesSet)->attrType[i] = entry->typeOid;
	}

	list_free_deep((*deleteFile)->eqColumnNames);
}

static void
destroyDeletesReaderResource(List **deletes)
{
	*deletes = list_delete_first(*deletes);
}

static List *
readEqualityDeletes(MemoryContext filterMcxt,
					MemoryContext readerMcxt,
					List *datafileDesc,
					ossFileStream fileStream,
					List *deletes)
{
	Reader         *curReader;
	FileFragment   *deleteFile;
	List           *result = NIL;
	DatalakeInternalRecordDesc *deletesSet;
	MemoryContext   oldMcxt;
	InternalRecordWrapper *recordWrapper;

	oldMcxt = MemoryContextSwitchTo(filterMcxt);

	createDeletesReaderResources(filterMcxt, datafileDesc, &deleteFile, &deletesSet, deletes);
	elog(DEBUG1, "scanning equalityDeletes file %s", deleteFile->filePath);
	curReader = (Reader *) datalakeCreateFileReader(readerMcxt, datafileDesc, deletesSet->attrUsed,
													true, deleteFile, fileStream, -1, -1, NULL, NIL);
	destroyDeletesReaderResource(&deletes);

	result = lappend(result, deletesSet);

	while (true)
	{
		recordWrapper = datalakeCreateInternalRecordWrapper(deletesSet, deletesSet->nColumns);
		if (curReader->Next(curReader, (DatalakeInternalRecord *) recordWrapper))
		{
			bool found;
			EqualityHashEntry *hentry;
			hentry = (EqualityHashEntry *) hash_search(deletesSet->hashTab, &recordWrapper, HASH_ENTER, &found);
			if (!found)
				datalakeDeepCopyRecord(recordWrapper);
		}
		else if (list_length(deletes) > 0)
		{
			datalakeDestroyInternalRecordWrapper(recordWrapper);
			curReader->Close(curReader);

			createDeletesReaderResources(filterMcxt, datafileDesc, &deleteFile, &deletesSet, deletes);

			elog(DEBUG1, "scanning equalityDeletes file %s", deleteFile->filePath);
			curReader = (Reader *) datalakeCreateFileReader(readerMcxt, datafileDesc, deletesSet->attrUsed,
															true, deleteFile, fileStream, -1, -1, NULL, NIL);
			destroyDeletesReaderResource(&deletes);

			result = lappend(result, deletesSet);
		}
		else
		{
			datalakeDestroyInternalRecordWrapper(recordWrapper);
			curReader->Close(curReader);
			break;
		}
	}

	MemoryContextSwitchTo(oldMcxt);
	return result;
}
