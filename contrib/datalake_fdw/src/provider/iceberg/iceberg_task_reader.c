#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_position_filter.h"
#include "iceberg_equality_filter.h"
#include "iceberg_task_reader.h"
#include "iceberg_delete_index.h"

static void printDebugLog(int taskId,
						  const char *filePath,
						  int64 recordCount,
						  int64 startOffset,
						  int64 length,
						  List *posDeletes,
						  List *eqDeletes);
static void reOrganizeDeleteFiles(List *deletes, List **posDeletes, List **eqDeletes);
static void projectRequiredColumns(List *datafileTupleDesc, bool *attrUsed, List *deletes);

Reader *
createIcebergTaskReader(void *args)
{
	Reader *filter;
	List *posDeletes = NIL;
	List *eqDeletes = NIL;
	DatalakeReaderInitInfo *info = (DatalakeReaderInitInfo *) args;

	elog(DEBUG1, "create iceberg reader task [%d]", info->taskId);

	IcebergTaskReader *reader = palloc0(sizeof(IcebergTaskReader));
	reader->fileScanTask = info->fileScanTask;
	reader->taskId = info->taskId;
	reader->fileId = info->fileId;

	reOrganizeDeleteFiles(info->fileScanTask->deletes, &posDeletes, &eqDeletes);

	printDebugLog(info->taskId,
				  info->fileScanTask->dataFile->filePath,
				  info->fileScanTask->dataFile->recordCount,
				  info->fileScanTask->start,
				  info->fileScanTask->length,
				  posDeletes,
				  eqDeletes);

	if (list_length(eqDeletes) > 0)
		projectRequiredColumns(info->datafileDesc, info->attrUsed, eqDeletes);

	filter = (Reader *) datalakeCreateFileReader(info->mcxt, info->datafileDesc, info->attrUsed, true,
												info->fileScanTask->dataFile, info->fileStream,
												info->fileScanTask->start,
												info->fileScanTask->start + info->fileScanTask->length,
												info->buffer, info->filterQuals);

	list_free(info->fileScanTask->deletes);

	if (list_length(posDeletes) > 0)
	{
		if (info->deleteIndex != NULL)
		{
			/* Use pre-built bitmap from global delete index */
			void *bitmap = icebergDeleteIndexLookup(
				(IcebergDeleteIndex *) info->deleteIndex,
				info->fileScanTask->dataFile->filePath);
			filter = (Reader *) datalakeCreatePositionFilterFromBitmap(filter, bitmap);
		}
		else
		{
			filter = (Reader *) datalakeCreatePositionFilter(info->mcxt, filter, info->fileStream,
													 info->fileScanTask->dataFile->filePath, posDeletes);
		}
	}

	if (list_length(eqDeletes) > 0)
		filter = (Reader *) datalakeCreateEqualityFilter(info->mcxt, info->datafileDesc,
												 filter, info->fileStream, eqDeletes);

	reader->dataReader = filter;
	return (Reader *) reader;
}

bool
icebergTaskReaderNext(Reader *reader, DatalakeInternalRecord *record)
{
	IcebergTaskReader *icebergReader = (IcebergTaskReader *) reader;
	bool result;

	if (icebergReader == NULL)
		return false;

	result = icebergReader->dataReader->Next(icebergReader->dataReader, record);

	/* Fill fileId for TID (Tuple ID) if read successfully */
	if (result)
	{
		record->fileId = icebergReader->fileId;
	}

	return result;
}

void
icebergTaskReaderClose(Reader *reader)
{
	IcebergTaskReader *icebergReader = (IcebergTaskReader *) reader;

	if (icebergReader == NULL)
		return;

	if (icebergReader->dataReader)
	{
		icebergReader->dataReader->Close(icebergReader->dataReader);
	}
	if (icebergReader->fileScanTask)
	{
		pfree(icebergReader->fileScanTask);
	}
	elog(DEBUG1, "close iceberg reader task [%d]", icebergReader->taskId);
	pfree(icebergReader);
}

static void
reOrganizeDeleteFiles(List *deletes, List **posDeletes, List **eqDeletes)
{
	ListCell *lc;

	foreach(lc, deletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);

		switch (deleteFile->content)
		{
			case POSITION_DELETES:
				*posDeletes = lappend(*posDeletes, deleteFile);
				break;
			case EQUALITY_DELETES:
				*eqDeletes = lappend(*eqDeletes, deleteFile);
				break;
			default:
				elog(ERROR, "unknown delete file type: %d", deleteFile->content);
		}
	}
}

static void
projectRequiredColumns(List *datafileTupleDesc, bool *attrUsed, List *deletes)
{
	int i = 0;
	ListCell *lco;
	ListCell *lci;
	FileFragment *deleteFile;
	List *eqColumnNames = NIL;

	foreach(lco, deletes)
	{
		deleteFile = (FileFragment *) lfirst(lco);
		eqColumnNames = list_concat_unique(eqColumnNames, deleteFile->eqColumnNames);
	}

	foreach(lco, eqColumnNames)
	{
		char *columnName = strVal(lfirst(lco));

		foreach_with_count(lci, datafileTupleDesc, i)
		{
			DatalakeFieldDescription *fieldDesc = (DatalakeFieldDescription *) lfirst(lci);

			if (attrUsed[i] == true)
				break;

			if (pg_strcasecmp(fieldDesc->name, columnName) == 0)
			{
				attrUsed[i] = true;
				break;
			}
		}
	}

	list_free(eqColumnNames);
}

static void
printDebugLog(int taskId,
			  const char *filePath,
			  int64 recordCount,
			  int64 startOffset,
			  int64 length,
			  List *posDeletes,
			  List *eqDeletes)
{
	ListCell *lc;

	elog(DEBUG1, "[%d] datafile \"%s\" [%ld %ld] records %ld",
			taskId, filePath, startOffset, startOffset + length, recordCount);

	foreach(lc, posDeletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);
		elog(DEBUG1, "[%d] position file \"%s\" records %ld", taskId, deleteFile->filePath, deleteFile->recordCount);
	}

	foreach(lc, eqDeletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);
		elog(DEBUG1, "[%d] equality file \"%s\" records %ld", taskId, deleteFile->filePath, deleteFile->recordCount);
	}
}
