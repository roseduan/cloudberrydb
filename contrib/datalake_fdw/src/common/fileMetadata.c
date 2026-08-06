#include "fileMetadata.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/libpq-int.h"
#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdispatchresult.h"
#include "utils/json.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "nodes/value.h"

/* QD-side: per-Oid metadata hash table (replaces former FDW_ResultMetaList) */
typedef struct FDW_MetaMapEntry
{
	Oid		relid;			/* hash key — must be first field */
	List   *meta_list;		/* List of FileFragment* */
} FDW_MetaMapEntry;

static HTAB *FDW_ResultMetaMap = NULL;
static MemoryContext FDW_MetaMapContext = NULL;
static MemoryContextCallback FDW_MetaMapCb;


/*
 * Compatibility shim for pqAddTuple which may not be exported
 * by older postgres binaries.
 */
static bool
pqAddTuple_compat(PGresult *res, PGresAttValue *tup, const char **errmsgp)
{
	if (res->ntups >= res->tupArrSize)
	{
		int newSize = (res->tupArrSize > 0) ? res->tupArrSize * 2 : 128;
		PGresAttValue **newTuples;

		newTuples = (PGresAttValue **)
			realloc(res->tuples, newSize * sizeof(PGresAttValue *));
		if (!newTuples)
		{
			*errmsgp = "out of memory for query result";
			return false;
		}
		res->tuples = newTuples;
		res->tupArrSize = newSize;
	}
	res->tuples[res->ntups] = tup;
	res->ntups++;
	return true;
}

List *FDW_ResultMetaList = NIL;

typedef struct CdbDispatchCmdAsync
{
	struct CdbDispatchResult **dispatchResultPtrArray;
	int			dispatchCount;
	volatile	DispatchWaitMode waitMode;
	const char *ackMessage;
	char	   *query_text;
	int			query_text_len;
}			CdbDispatchCmdAsync;

static bool
receiveDataMeta_internal(PGconn *conn, PGresult *result,
						 const char **errmsg)
{
	char	   *msg;
	int			msglen;

	result->numAttributes = 1;

	pqGetInt(&msglen, 4, conn);
	Assert(msglen > 0);
	msg = (char *) palloc(msglen);

	if (pqGetnchar(msg, msglen, conn))
	{
		*errmsg = libpq_gettext("insufficient data in \"m\" message");
		return false;
	}

	{
		PGdataValue *columns;
		PGresAttValue *tup;

		tup = (PGresAttValue *)
			pqResultAlloc(result, result->numAttributes * sizeof(PGresAttValue), true);
		if (!tup)
			return false;

		columns = (PGdataValue *)
			pqResultAlloc(result, result->numAttributes * sizeof(PGdataValue), true);
		if (!columns)
			return false;

		columns[0].value = msg;
		columns[0].len = msglen;
		for (int i = 0; i < result->numAttributes; i++)
		{
			char	   *val;

			tup[i].len = columns[i].len;
			val = (char *) pqResultAlloc(result, tup[i].len + 1, false);
			if (val == NULL)
				return false;
			memcpy(val, columns[i].value, tup[i].len);
			val[tup[i].len] = '\0';
			tup[i].value = val;
		}

		/* And add the tuple to the PGresult's tuple array */
		if (!pqAddTuple_compat(result, tup, errmsg))
			return false;

		/* Success! */
		if (!conn->result)
			conn->result = result;

		return true;
	}
}

int
RecvMetaMethod(PGconn *conn, int msgLength)
{
	PGresult   *result;
	const char *errmsg = NULL;

	result = conn->result;
	if (!result)
	{
		result = PQmakeEmptyPGresult(conn, PGRES_TUPLES_OK);
		if (!result)
		{
			/* errmsg == NULL means "out of memory", see below */
			goto advance_and_error;
		}
	}
	result->resultStatus = PGRES_TUPLES_OK;

	if (!receiveDataMeta_internal(conn, result, &errmsg))
		goto advance_and_error;

	return 0;
advance_and_error:
	/* Discard unsaved result, if any */
	if (result && result != conn->result)
		PQclear(result);

	/*
	 * Replace partially constructed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);

	/*
	 * If preceding code didn't provide an error message, assume "out of
	 * memory" was meant.  The advantage of having this special case is that
	 * freeing the old result first greatly improves the odds that gettext()
	 * will succeed in providing a translation.
	 */
	if (!errmsg)
		errmsg = libpq_gettext("out of memory for query result");

	appendPQExpBuffer(&conn->errorMessage, "%s\n", errmsg);
	pqSaveErrorResult(conn);

	/*
	 * Show the message as fully consumed, else pqParseInput3 will overwrite
	 * our error with a complaint about that.
	 */
	conn->inCursor = conn->inStart + 5 + msgLength;

	return 0;
}

int	(*FDWRecvProtocol) (PGconn *conn, int msgLength) = RecvMetaMethod;

/*
 * MemoryContext reset callback: clear global pointers to avoid dangling refs
 * after abort/commit.
 */
static void
metaMapResetCb(void *arg)
{
	FDW_ResultMetaMap = NULL;
	FDW_MetaMapContext = NULL;
}

void
FDW_InitMetaMap(void)
{
	HASHCTL		hash_ctl;

	if (FDW_ResultMetaMap != NULL)
		return;  /* idempotent */

	FDW_MetaMapContext = AllocSetContextCreate(
		TopTransactionContext,
		"FDW Meta Map Context",
		ALLOCSET_SMALL_SIZES);

	FDW_MetaMapCb.func = metaMapResetCb;
	FDW_MetaMapCb.arg = NULL;
	MemoryContextRegisterResetCallback(FDW_MetaMapContext, &FDW_MetaMapCb);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(FDW_MetaMapEntry);
	hash_ctl.hcxt = FDW_MetaMapContext;
	FDW_ResultMetaMap = hash_create("FDW Result Meta Map", 16, &hash_ctl,
									HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
}

List *
FDW_GetMetaList(Oid relid)
{
	FDW_MetaMapEntry *entry;

	if (FDW_ResultMetaMap == NULL)
		return NIL;

	entry = (FDW_MetaMapEntry *) hash_search(FDW_ResultMetaMap,
											  &relid, HASH_FIND, NULL);
	if (entry == NULL)
		return NIL;

	return entry->meta_list;
}

void
FDW_ClearMetaList(Oid relid)
{
	FDW_MetaMapEntry *entry;

	if (FDW_ResultMetaMap == NULL)
		return;

	entry = (FDW_MetaMapEntry *) hash_search(FDW_ResultMetaMap,
											  &relid, HASH_FIND, NULL);
	if (entry != NULL)
	{
		FDW_FreeMetaList(entry->meta_list);
		entry->meta_list = NIL;
		hash_search(FDW_ResultMetaMap, &relid, HASH_REMOVE, NULL);
	}
}

void
FDW_FreeMetaList(List *meta_list)
{
	ListCell   *lc;

	foreach(lc, meta_list)
	{
		FileFragment *meta = (FileFragment *) lfirst(lc);
		ListCell   *pv;

		if (meta == NULL)
			continue;
		if (meta->filePath)
			pfree(meta->filePath);

		/*
		 * Free the partition tuple built in FDW_deserializeMetaToMap: a List
		 * of String nodes (with pstrdup'd/palloc'd strings), NULL cells for
		 * SQL NULL partition values.
		 */
		foreach(pv, meta->partitionValues)
		{
			Value	   *v = (Value *) lfirst(pv);

			if (v)
			{
				if (v->val.str)
					pfree(v->val.str);
				pfree(v);
			}
		}
		list_free(meta->partitionValues);

		pfree(meta);
	}
	list_free(meta_list);
}


void
FDW_SendMeta(bytea *msg)
{
	StringInfoData buf;

	if (msg == NULL)
		return;

	pq_beginmessage(&buf, 'f');
	pq_sendint32(&buf, VARSIZE_ANY_EXHDR(msg));
	pq_sendbytes(&buf, VARDATA_ANY(msg), VARSIZE_ANY_EXHDR(msg));
	pq_endmessage(&buf);
	pq_flush();
}


/*
 * Deserialize all tuples from a PGresult and route them into
 * FDW_ResultMetaMap by the relid embedded in each wire-format tuple.
 */
static void
FDW_deserializeMetaToMap(PGresult *res)
{
	for (int i = 0; i < PQntuples(res); i++)
	{
		char	   *ptr = res->tuples[i][0].value;
		Oid			relid;
		bool		found;
		FDW_MetaMapEntry *map_entry;

		FileFragment *meta = makeNode(FileFragment);

		/* read relid first */
		memcpy(&relid, ptr, sizeof(Oid));
		ptr += sizeof(Oid);

		memcpy(&(meta->fileSize), ptr, sizeof(int64_t));
		ptr += sizeof(int64_t);
		memcpy(&(meta->recordCount), ptr, sizeof(int64_t));
		ptr += sizeof(int64_t);
		memcpy(&(meta->format), ptr, sizeof(FileFormat));
		ptr += sizeof(FileFormat);
		memcpy(&(meta->content), ptr, sizeof(FileContent));
		ptr += sizeof(FileContent);

		size_t		slen;
		memcpy(&slen, ptr, sizeof(size_t));
		ptr += sizeof(size_t);

		meta->filePath = (char *) palloc(slen + 1);
		memcpy(meta->filePath, ptr, slen);
		meta->filePath[slen] = '\0';
		ptr += slen;

		/*
		 * Partition values: [int32 n] then, per value,
		 * [uint8 isNull][int32 len][char bytes[len]].  A NULL element becomes
		 * a NULL list cell (SQL NULL partition value); NIL when n == 0.
		 */
		{
			int32		npv;

			memcpy(&npv, ptr, sizeof(int32));
			ptr += sizeof(int32);

			meta->partitionValues = NIL;
			for (int k = 0; k < npv; k++)
			{
				uint8		isNull;
				int32		len;

				memcpy(&isNull, ptr, sizeof(uint8));
				ptr += sizeof(uint8);
				memcpy(&len, ptr, sizeof(int32));
				ptr += sizeof(int32);

				if (isNull)
				{
					meta->partitionValues =
						lappend(meta->partitionValues, NULL);
				}
				else
				{
					char	   *val = (char *) palloc(len + 1);

					memcpy(val, ptr, len);
					val[len] = '\0';
					ptr += len;
					meta->partitionValues =
						lappend(meta->partitionValues, makeString(val));
				}
			}
		}

		/* store into hash table by relid */
		map_entry = (FDW_MetaMapEntry *) hash_search(FDW_ResultMetaMap,
													  &relid, HASH_ENTER, &found);
		if (!found)
			map_entry->meta_list = NIL;
		map_entry->meta_list = lappend(map_entry->meta_list, meta);
	}
}

void FDW_RecvMeta(CdbDispatcherState * ds)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		goto chain;

	if (IsAbortInProgress())
		goto chain;

	/* hash table not initialized — skip (avoids dangling pointer after abort) */
	if (FDW_ResultMetaMap == NULL)
		goto chain;

	CdbDispatchCmdAsync *pParams = (CdbDispatchCmdAsync *) ds->dispatchParams;

	if (!pParams)
		goto chain;

	/* fetch qe results */
	for (int i = 0; i < pParams->dispatchCount; i++)
	{
		CdbDispatchResult *dispatchResult = pParams->dispatchResultPtrArray[i];

		for (int j = 0; j < cdbdisp_numPGresult(dispatchResult); j++)
		{
			struct pg_result *res = cdbdisp_getPGresult(dispatchResult, j);

			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				FDW_deserializeMetaToMap(res);
			}
		}
	}

chain:
	if (datalake_prev_ProcessDispatchResult)
		datalake_prev_ProcessDispatchResult(ds);
}

/*
 * Wire format:
 *   [Oid relid][int64_t fileSize][int64_t recordCount]
 *   [FileFormat format][FileContent content][size_t slen][char filePath[slen]]
 *   [int32 npv][per partition value: uint8 isNull, int32 len, char bytes[len]]
 *
 * The partition-value trailer carries one entry per partition column (in
 * partition-spec order); isNull marks a SQL NULL value.  npv == 0 for
 * unpartitioned tables.  Encoder and FDW_deserializeMetaToMap must stay
 * byte-for-byte symmetric.
 */
bytea *FDW_serializeMeta(void *msg, Oid relid)
{
	FileFragment *meta = (FileFragment *) msg;
	bytea	   *result;
	ListCell   *lc;

	size_t		slen = strlen(meta->filePath);
	size_t		pv_size = sizeof(int32);	/* npv count */

	foreach(lc, meta->partitionValues)
	{
		Node	   *val = (Node *) lfirst(lc);

		pv_size += sizeof(uint8) + sizeof(int32);	/* isNull + len */
		if (val != NULL)
			pv_size += strlen(strVal(val));
	}

	size_t		size = sizeof(Oid) + 2 * sizeof(int64_t) +
					   sizeof(FileFormat) + sizeof(FileContent) +
					   sizeof(size_t) + slen + pv_size;

	result = (bytea*)palloc0(size + VARHDRSZ);
	SET_VARSIZE(result, size + VARHDRSZ);

	char	   *ptr = VARDATA(result);
	memcpy(ptr, &relid, sizeof(Oid));
	ptr += sizeof(Oid);
	memcpy(ptr, &(meta->fileSize), sizeof(int64_t));
	ptr += sizeof(int64_t);
	memcpy(ptr, &(meta->recordCount), sizeof(int64_t));
	ptr += sizeof(int64_t);
	memcpy(ptr, &(meta->format), sizeof(FileFormat));
	ptr += sizeof(FileFormat);
	memcpy(ptr, &(meta->content), sizeof(FileContent));
	ptr += sizeof(FileContent);
	memcpy(ptr, &slen, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(ptr, meta->filePath, slen);
	ptr += slen;

	int32		npv = list_length(meta->partitionValues);

	memcpy(ptr, &npv, sizeof(int32));
	ptr += sizeof(int32);
	foreach(lc, meta->partitionValues)
	{
		Node	   *val = (Node *) lfirst(lc);
		uint8		isNull = (val == NULL) ? 1 : 0;

		memcpy(ptr, &isNull, sizeof(uint8));
		ptr += sizeof(uint8);

		if (isNull)
		{
			int32		len = 0;

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
		}
		else
		{
			char	   *str = strVal(val);
			int32		len = (int32) strlen(str);

			memcpy(ptr, &len, sizeof(int32));
			ptr += sizeof(int32);
			memcpy(ptr, str, len);
			ptr += len;
		}
	}

	return result;
}

/*
 * FDW_serialize_file_list_to_json
 *     Serialize a list of FileFragment objects to JSON format.
 *     This is used for sending file lists in POST request bodies instead of headers.
 *
 * Parameters:
 *     file_list - List of FileFragment pointers
 *     output - StringInfoData buffer to receive the JSON output
 *
 * The output format is:
 * {
 *   "files": [
 *     {
 *       "filePath": "...",
 *       "format": "PARQUET",
 *       "content": "DATA_FILE",
 *       "fileSize": 12345,
 *       "recordCount": 100
 *     },
 *     ...
 *   ]
 * }
 */
void
FDW_serialize_file_list_to_json(List *file_list, StringInfoData *output)
{
    ListCell   *lc;
    int         file_count = 0;

    if (file_list == NIL)
    {
        initStringInfo(output);
        appendStringInfoString(output, "{\"files\":[]}");
        return;
    }

    initStringInfo(output);
    appendStringInfoChar(output, '{');
    appendStringInfoString(output, "\"files\":[");

    foreach_with_count(lc, file_list, file_count)
    {
        FileFragment *file = (FileFragment *) lfirst(lc);
        const char  *format_str;
        const char  *content_str;

        if (file_count > 0)
            appendStringInfoChar(output, ',');

        appendStringInfoChar(output, '{');

        /* filePath - use escape_json to handle special characters */
        appendStringInfoString(output, "\"filePath\":");
        escape_json(output, file->filePath);

        /* format */
        switch (file->format)
        {
            case ORC:  format_str = "ORC"; break;
            case PARQUET: format_str = "PARQUET"; break;
            case AVRO: format_str = "AVRO"; break;
            default: format_str = "UNKNOWN"; break;
        }
        appendStringInfo(output, ",\"format\":\"%s\"", format_str);

        /* content */
        switch (file->content)
        {
            case DATA: content_str = "DATA_FILE"; break;
            case POSITION_DELETES: content_str = "POSITION_DELETE"; break;
            case EQUALITY_DELETES: content_str = "EQUALITY_DELETE"; break;
            case DELTA_LOG: content_str = "DELTA_LOG"; break;
            default: content_str = "UNKNOWN"; break;
        }
        appendStringInfo(output, ",\"content\":\"%s\"", content_str);

        /* fileSize */
        appendStringInfo(output, ",\"fileSize\":%ld", (long)file->fileSize);

        /* recordCount */
        appendStringInfo(output, ",\"recordCount\":%ld", (long)file->recordCount);

        appendStringInfoChar(output, '}');
    }

    appendStringInfoString(output, "]}");
}
