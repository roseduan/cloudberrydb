#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_position_filter.h"
#include "src/provider/common/delete_bitmap_c.h"
#include "src/provider/common/sorted_merge_c.h"
#include "src/am_iceberg/include/pg_iceberg_guc.h"

extern int icebergPostionDeletesThreshold;

static bool positionFilterNext(Reader *filter, DatalakeInternalRecord *record);
static void positionFilterClose(Reader *filter);
static void *readPositionDeletes(MemoryContext mcxt,
								 List *schema,
								 ossFileStream fileStream,
								 char *dataFilePath,
								 List *deletes);

static Reader methods = {
	NULL,
	positionFilterNext,
	positionFilterClose,
};

static List *
createPositionDeletesDescription(void)
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

DatalakePositionFilter *
datalakeCreatePositionFilter(MemoryContext readerMcxt,
					 Reader *dataReader,
					 ossFileStream fileStream,
					 char *dataFilePath,
					 List *deletes)
{
	ListCell *lc;
	int64 totalRecords;
	Reader *reader;
	List *readers = NIL;
	List *posDeletesSchema = createPositionDeletesDescription();
	DatalakePositionFilter *filter = palloc0(sizeof(DatalakePositionFilter));

	filter->base = methods;
	filter->dataReader = dataReader;

	totalRecords = datalakeGetFileRecordCount(deletes);
	if (totalRecords < icebergPostionDeletesThreshold)
	{
		elog(DEBUG1, "create in-memory position filter");
		filter->deletesSet = readPositionDeletes(readerMcxt, posDeletesSchema, fileStream, dataFilePath, deletes);
		if (filter->deletesSet == NULL)
			filter->isEmptySet = true;

		list_free_deep(posDeletesSchema);
		return filter;
	}

	elog(DEBUG1, "create iceberg streaming position filter");

	filter->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "PositionFilterContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	/* create streaming filter */
	foreach(lc, deletes)
	{
		bool attrUsed[2] = {true, true};
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);

		elog(DEBUG1, "scanning position file %s", deleteFile->filePath);
		reader = (Reader *) datalakeCreateFileReader(filter->mcxt, posDeletesSchema, attrUsed, true,
													deleteFile, fileStream, -1, -1, NULL, NIL);

		readers = lappend(readers, reader);
	}

	filter->sortedMerge = datalakeCreateSortedMerge(dataFilePath, readers);
	if (!datalakeSortedMergeNext(filter->sortedMerge, &filter->nextPosition))
		filter->isEmptyStream = true;

	list_free_deep(posDeletesSchema);
	list_free(readers);

	return filter;
}

static bool
inMemoryFilterNext(DatalakePositionFilter *filter, DatalakeInternalRecord *record)
{
	bool isDeleted;

	while(filter->dataReader->Next(filter->dataReader, record))
	{
		isDeleted = datalakeBitmapContains(filter->deletesSet, (uint64) record->position);
		if (!isDeleted)
			return true;
	}

	return false;
}

static bool
streamingFilterNext(DatalakePositionFilter *filter, DatalakeInternalRecord *record)
{
	int64 curPosition;

	if (filter->isEmptyStream)
		return filter->dataReader->Next(filter->dataReader, record);

	while(filter->dataReader->Next(filter->dataReader, record))
	{
		curPosition = record->position;

		if (curPosition < filter->nextPosition)
			return true;

		/* consume delete positions until the next is past the current position */
		bool keep = curPosition != filter->nextPosition;
		while (datalakeSortedMergeNext(filter->sortedMerge, &filter->nextPosition))
		{
			/* if any delete position matches the current position, discard */
			if (keep && curPosition == filter->nextPosition)
				keep = false;

			if (filter->nextPosition > curPosition)
				break;
		}

		if (keep)
			return true;
	}

	return false;
}

static bool
positionFilterNext(Reader *filter, DatalakeInternalRecord *record)
{
	DatalakePositionFilter *positionFilter = (DatalakePositionFilter *) filter;

	if (positionFilter->deletesSet != NULL)
		return inMemoryFilterNext(positionFilter, record);
	else if (positionFilter->isEmptySet)
		return positionFilter->dataReader->Next(positionFilter->dataReader, record);
	else
		return streamingFilterNext(positionFilter, record);
}

static void
positionFilterClose(Reader *filter)
{
	DatalakePositionFilter *positionFilter = (DatalakePositionFilter *) filter;

	/* Only destroy bitmap if we own it (not borrowed from global index) */
	if (positionFilter->deletesSet != NULL && !positionFilter->borrowedBitmap)
		datalakeDestroyBitmap(positionFilter->deletesSet);

	if (positionFilter->sortedMerge != NULL)
		datalakeSortedMergeClose(positionFilter->sortedMerge);

	positionFilter->dataReader->Close(positionFilter->dataReader);

	if (positionFilter->mcxt != NULL)
		MemoryContextDelete(positionFilter->mcxt);

	pfree(positionFilter);

	elog(DEBUG1, "close iceberg position filter");
}

DatalakePositionFilter *
datalakeCreatePositionFilterFromBitmap(Reader *dataReader, void *bitmap)
{
	DatalakePositionFilter *filter = palloc0(sizeof(DatalakePositionFilter));

	filter->base = methods;
	filter->dataReader = dataReader;
	filter->deletesSet = bitmap;
	filter->borrowedBitmap = true;

	if (bitmap == NULL)
		filter->isEmptySet = true;

	elog(DEBUG1, "create position filter from global delete index");

	return filter;
}

static void
locationFilter(void **bitmap, char *dataFilePath, int dataFilePathLen, Datum filePathField, Datum positionField, Reader *sourceReader)
{
	int filePathSize = VARSIZE_ANY_EXHDR(filePathField);
	char *filePathName = VARDATA_ANY(filePathField);
	int64 position = DatumGetInt64(positionField);

	if (client_min_messages <= DEBUG1)
	{
		elog(DEBUG1, "locationFilter: dataFilePath='%.*s' (len=%d), deleteFilePath='%.*s' (len=%d), position=%ld",
			 dataFilePathLen, dataFilePath, dataFilePathLen,
			 filePathSize, filePathName, filePathSize, position);
	}

	if (datalakeCharSeqEquals(filePathName, filePathSize, dataFilePath, dataFilePathLen))
	{
		if (*bitmap == NULL)
			*bitmap = datalakeCreateBitmap();

		datalakeBitmapAdd(*bitmap, (uint64) position);
		elog(DEBUG1, "locationFilter: added position %ld to delete bitmap", position);
	}
	else
	{
		elog(DEBUG1, "locationFilter: path mismatch, position %ld NOT added", position);
	}

	/* Batch text Datums point into the Parquet reader's slab. */
	if (!datalakeFileReaderDatumOwned(sourceReader, 0))
		pfree(DatumGetPointer(filePathField));
}

static void *
readPositionDeletes(MemoryContext mcxt,
					List *posDeletesSchema,
					ossFileStream fileStream,
					char *dataFilePath,
					List *deletes)
{
	bool nulls[2] = {false, false};
	Datum values[2] = {0, 0};
	Reader *curReader;
	void *result = NULL;
	bool attrUsed[2] = {true, true};
	int dataFilePathLen = strlen(dataFilePath);
	DatalakeInternalRecord record = {values, nulls, 0};
	FileFragment *positionFile = list_nth(deletes, 0);

	elog(DEBUG1, "scanning position file %s", positionFile->filePath);
	curReader = (Reader *) datalakeCreateFileReader(mcxt, posDeletesSchema, attrUsed, true,
													positionFile, fileStream, -1, -1, NULL, NIL);
	deletes = list_delete_first(deletes);

	while (true)
	{
		if (curReader->Next(curReader, &record))
		{
			locationFilter(&result, dataFilePath, dataFilePathLen, values[0], values[1], curReader);
		}
		else if (list_length(deletes) > 0)
		{
			curReader->Close(curReader);
			positionFile = list_nth(deletes, 0);
			elog(DEBUG1, "scanning position file %s", positionFile->filePath);
			curReader = (Reader *) datalakeCreateFileReader(mcxt, posDeletesSchema, attrUsed, true,
															positionFile, fileStream, -1, -1, NULL, NIL);
			deletes = list_delete_first(deletes);
		}
		else
		{
			curReader->Close(curReader);
			break;
		}
	}

	return result;
}
