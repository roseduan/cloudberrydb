#include "fdwFunction.h"
#include "src/datalake_option.h"
#include "src/provider/common/row_reader.h"
#include "src/provider/iceberg/iceberg_file_index.h"
#include "src/common/random_segment.h"
#include "src/datalake_fragment.h"
#include "src/iceberg_volume_fdw/iceberg_volume_fdw.h"
#include "src/common/exttable.h"
#include "src/provider/providerWrapper.h"
#include "access/formatter.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/detoast.h"
#include "nodes/bitmapset.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdisp.h"
#include "commands/copy.h"
#if (PG_VERSION_NUM >= 140000)
#include "commands/copyfrom_internal.h"
#include "commands/copyto_internal.h"
#endif
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#if (PG_VERSION_NUM >= 140000)
#include "utils/backend_progress.h"
#endif
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/sampling.h"
#include "utils/typcache.h"
#include "utils/acl.h"
#include "tcop/utility.h"
#include "optimizer/appendinfo.h"
#include "src/common/fileMetadata.h"
#include "src/provider/iceberg/iceberg_file_index.h"

#define EXEC_FLAG_VECTOR 0x8000

#define FDW_DATALAKE_SEGMENT_ID GpIdentity.segindex

static int CopyRead(void *outbuf, int minlen, int maxlen, void *extra);
static void InitCopyState(ForeignScanState *node, dataLakeFdwScanState *sstate);
static void InitParseStateTo(dataLakeFdwScanState *dataLakesstate, CopyToState cstate);

static void initCopyStateForModify(ResultRelInfo *resultRelInfo);


static void freeFdwPrivateList(List *fdw_private);
static void freeFdwPrivatePartitionList(List *fdw_private);
static void CsvTextErrorCallback(dataLakeFdwScanState *dataLakesstate);
static List* buildAttnameList(dataLakeFdwScanState *sstate);
/* Callback function to clean up global fileIndexMap on memory context reset */
static void fileIndexMapCallback(void *arg);


static void
InitParseStateFrom(CopyFromState cstate, Relation relation,
				   char *uri, int rejectlimit,
				   bool islimitinrows, char logerrors);

#if (PG_VERSION_NUM < 140000)
static CopyState
BeginCopyTo(Relation forrel, List *options);
#else
static CopyToState
BeginCopyToModify(Relation forrel, List *options);
#endif

#if (PG_VERSION_NUM >= 140000)
static void EndCopyScan(CopyFromState cstate);
static void EndCopyModify(CopyToState cstate);
#endif

static void
freePartitionList(List *partitions);

/*
 * Initiates a copy state for datalakeBeginForeignScan() and datalakeReScanForeignScan()
 */
static void
InitCopyState(ForeignScanState *node, dataLakeFdwScanState *sstate)
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyFromState	cstate;
#endif
	/* datalake not support masteronly */
	bool isMasterOnly = false;
	List* copy_options = datalakeGetCopyOptions(RelationGetRelid(sstate->rel));
	int rejectlimit = -1;
	bool islimitinrows = false;
	char logerrors = LOG_ERRORS_DISABLE;
	char* uri = NULL;
	int eflags = 0;
	datalakeGetCopyLogerrorOptions(RelationGetRelid(sstate->rel), &rejectlimit, &islimitinrows, &logerrors);
	datalakeGetUriFromOptions(RelationGetRelid(sstate->rel), &uri);
	List* attnamelist = buildAttnameList(sstate);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(NULL,
						   sstate->rel,
#if (PG_VERSION_NUM >= 140000)
						   NULL,
#endif
						   NULL,
						   false,	/* is_program */
						   &CopyRead,	/* data_source_cb */
						   sstate,	/* data_source_cb_extra */
						   attnamelist, /* attnamelist */
						   (FORMAT_IS_CUSTOM(sstate->options->format) ? NIL : copy_options));	/* copy options */


	InitParseStateFrom(cstate, sstate->rel, uri, rejectlimit, islimitinrows, logerrors);
	sstate->cstate.cstate_scan = cstate;

	if (FORMAT_IS_CUSTOM(sstate->options->format))
	{
		List *uriList = NIL;
		uriList = lappend(uriList, makeString(uri));
		List* customOption = datalakeGetCustomOptions(RelationGetRelid(sstate->rel));
		datalake_to_exttable_BeginForeignScan(node, eflags, (void*)sstate, isMasterOnly, uriList, customOption);
	}
}

static int
CopyRead(void *outbuf, int minlen, int maxlen, void *extra)
{
	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)extra;
	size_t n = 0;
	n = readBufferFromProvider(sstate->provider, outbuf, maxlen);
	return n;
}

void
fdwfunction_initScanStatue(ForeignScanState *node, dataLakeFdwScanState *dataLakesstate)
{
	dataLakesstate->initcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "datalakeFdwMemScanInitCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	createHandler(dataLakesstate->provider, (void*)dataLakesstate);

	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format) ||
		FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		/* csv/text used copy */
		InitCopyState(node, dataLakesstate);
	}
	else
	{
		dataLakesstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "datalakeFdwMemCxt",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	}
}

void
fdwfunction_iterateScanStatus(ForeignScanState *node, dataLakeFdwScanState *dataLakesstate)
{
	bool found = false;
	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		datalake_to_exttable_IterateForeignScan(node);
	}
	else if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		found = NextCopyFrom(dataLakesstate->cstate.cstate_scan,
						NULL,
						node->ss.ss_ScanTupleSlot->tts_values,
						node->ss.ss_ScanTupleSlot->tts_isnull);
	}
	else if (FORMAT_IS_ICEBERG(dataLakesstate->options->format))
	{
		/*
		 * Ultra fast path: call the cached format reader directly from C,
		 * bypassing ALL C++ provider layers (readFromProviderWithTid ->
		 * icebergRead::read -> datalakeRowReaderNext -> icebergTaskReaderNext
		 * -> fileReaderNext).
		 *
		 * The DatalakeRowReader's deepNext/deepReader are set by C code in
		 * row_reader.c, pointing to the innermost format reader (e.g.
		 * parquet_next + BaseFileReader*).
		 */
		DatalakeRowReader *rowReader = (DatalakeRowReader *) dataLakesstate->fastScanReader;
		if (likely(rowReader != NULL && rowReader->deepNext != NULL))
		{
			DatalakeInternalRecord record;
			record.values = (Datum *) node->ss.ss_ScanTupleSlot->tts_values;
			record.nulls = (bool *) node->ss.ss_ScanTupleSlot->tts_isnull;

			if (rowReader->deepNext(rowReader->deepReader, &record))
			{
				record.fileId = rowReader->deepFileId;
				ItemPointer tidPtr = &(node->ss.ss_ScanTupleSlot->tts_tid);
				icebergEncodeTID(tidPtr, record.fileId, record.position);
				found = 1;
			}
			else
			{
				/* File exhausted - clear deep cache and use slow path */
				rowReader->deepNext = NULL;
				rowReader->deepReader = NULL;
				found = readFromProviderWithTid(dataLakesstate->provider,
										(void*)node->ss.ss_ScanTupleSlot->tts_values,
										(void*)node->ss.ss_ScanTupleSlot->tts_isnull,
										(void*)&(node->ss.ss_ScanTupleSlot->tts_tid));
			}
		}
		else
		{
			/* First call or no deep cache - slow path sets up cache */
			found = readFromProviderWithTid(dataLakesstate->provider,
									(void*)node->ss.ss_ScanTupleSlot->tts_values,
									(void*)node->ss.ss_ScanTupleSlot->tts_isnull,
									(void*)&(node->ss.ss_ScanTupleSlot->tts_tid));
		}
	}
	else
	{
		MemoryContextReset(dataLakesstate->rowcontext);
		MemoryContext oldContext = MemoryContextSwitchTo(dataLakesstate->rowcontext);

		found = readFromProvider(dataLakesstate->provider,
								(void*)node->ss.ss_ScanTupleSlot->tts_values,
								(void*)node->ss.ss_ScanTupleSlot->tts_isnull);

		MemoryContextSwitchTo(oldContext);
	}

	if (found)
	{
		if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format))
		{
			setPartitionValue(dataLakesstate->provider, (void*)node->ss.ss_ScanTupleSlot->tts_values, (void*)node->ss.ss_ScanTupleSlot->tts_isnull);
			if (dataLakesstate->cstate.cstate_scan->cdbsreh)
			{
				dataLakesstate->cstate.cstate_scan->cdbsreh->processed++;
			}
		}

		ExecStoreVirtualTuple(node->ss.ss_ScanTupleSlot);
	}
}

void
fdwfunction_iterateRecordBatch(dataLakeFdwScanState *dataLakesstate, VirtualTupleTableSlot *vslot)
{
	MemoryContextReset(dataLakesstate->rowcontext);
	MemoryContext oldContext = MemoryContextSwitchTo(dataLakesstate->rowcontext);
	bool found = readRecordBatch(dataLakesstate->provider, (void**)(&vslot->data));
	if (found)
	{
		ExecStoreVirtualTuple(&vslot->base);
	}
	MemoryContextSwitchTo(oldContext);
}


void
fdwfunction_endScanStatus(dataLakeFdwScanState *dataLakesstate)
{
	if (dataLakesstate->cstate.cstate_scan != NULL && (FORMAT_IS_CSV(dataLakesstate->options->format) ||
			FORMAT_IS_TEXT(dataLakesstate->options->format)))
	{
#if (PG_VERSION_NUM < 140000)
		EndCopyFrom(dataLakesstate->cstate.cstate_scan);
#else
		EndCopyScan(dataLakesstate->cstate.cstate_scan);
#endif
	}

	if (dataLakesstate->provider != NULL && Gp_role != GP_ROLE_DISPATCH)
	{
		destroyHandler(dataLakesstate->provider);
	}
	datalakeFreeDatalakeOptions(dataLakesstate->options);
}

static void
freeFdwPrivateList(List *fdw_private)
{
	pfree(fdw_private);
	return;
}

static void
freePartitionList(List *partitions)
{
	ListCell *lci;
	ListCell *lco;

	foreach(lco, partitions)
	{
		List *values = (List*) lfirst(lco);

		foreach(lci, values)
		{
			char *value = strVal(lfirst(lci));
			pfree(value);
		}

		list_free_deep(values);
	}

	list_free(partitions);
}

static void
freeFdwPrivatePartitionList(List *fdw_private)
{
	List *partitionData = (List*)list_nth(fdw_private, PrivatePartitionData);
	freePartitionList(partitionData);
	pfree(fdw_private);
}

void
fdwfunction_freeFdwPrivate(dataLakeFdwScanState *sstate, ForeignScan *foreignScan)
{
	if (foreignScan->fdw_private)
	{
		if (sstate != NULL)
		{
			if (sstate->options->hiveOption->partitiontable)
			{
				freeFdwPrivatePartitionList(foreignScan->fdw_private);
			}
			else
			{
				freeFdwPrivateList(foreignScan->fdw_private);
				foreignScan->fdw_private = NULL;
			}
		}
		else
		{
			int private_count = list_length(foreignScan->fdw_private);
			if (private_count > PrivatePartitionFragmentLists)
			{
				freeFdwPrivatePartitionList(foreignScan->fdw_private);
			}
			else
			{
				freeFdwPrivateList(foreignScan->fdw_private);
				foreignScan->fdw_private = NULL;
			}
		}
	}
}

static void
InitParseStateTo(dataLakeFdwScanState *dataLakesstate,
				 CopyToState cstate)
{
	/* Initialize 'out_functions', like CopyTo() would. */

	TupleDesc	tupDesc = RelationGetDescr(cstate->rel);
	Form_pg_attribute attr = tupDesc->attrs;
	int			num_phys_attrs = tupDesc->natts;

	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	ListCell   *cur;

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		getTypeOutputInfo(attr[attnum - 1].atttypid,
						  &out_func_oid,
						  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
										"datalakeFdwMemCxt",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	dataLakesstate->cstate.cstate_modify = cstate;
}


void
fdwfunction_initModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo)
{
	dataLakeFdwScanState* dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
	void *sstate = (void*)dataLakesstate;
	if (mtstate->operation == CMD_UPDATE || mtstate->operation == CMD_DELETE)
	{
		dataLakesstate->modify_state->us_provider = initProvider(dataLakesstate->options->format, DL_OP_DELETE, dataLakesstate->options->vectorization);
		createHandler(dataLakesstate->modify_state->us_provider, sstate);
	}
	if (mtstate->operation == CMD_INSERT || mtstate->operation == CMD_UPDATE)
	{
		dataLakesstate->provider = initProvider(dataLakesstate->options->format, DL_OP_WRITE, dataLakesstate->options->vectorization);
		createHandler(dataLakesstate->provider, sstate);
	}

	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format) ||
		FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		initCopyStateForModify(resultRelInfo);
	}
	else
	{
		dataLakesstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											"datalakeFdwMemCxt",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);
	}
}

static void
initCopyStateForModify(ResultRelInfo *resultRelInfo)
{
	List	   *copy_options;
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyToState	cstate;
#endif

	copy_options = datalakeGetCopyOptions(RelationGetRelid(dataLakesstate->rel));

#if (PG_VERSION_NUM < 140000)
	cstate = BeginCopyTo(dataLakesstate->rel,
		(FORMAT_IS_CUSTOM(dataLakesstate->options->format)) ? NIL : copy_options);
#else
	cstate = BeginCopyToModify(dataLakesstate->rel,
		(FORMAT_IS_CUSTOM(dataLakesstate->options->format)) ? NIL : copy_options);
#endif
	InitParseStateTo(dataLakesstate, cstate);
	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		List* customOptions = datalakeGetCustomOptions(RelationGetRelid(dataLakesstate->rel));
		datalake_to_exttable_BeginForeignModify(NULL, resultRelInfo, NIL, customOptions, 0, 0);
	}
}

void
fdwfunction_insertModify(ResultRelInfo *resultRelInfo, TupleTableSlot *slot)
{
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		datalake_to_exttable_ExecForeignInsert(NULL, resultRelInfo, slot, NULL);
	}
	else if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format))
	{

#if (PG_VERSION_NUM < 140000)
		CopyState	cstate = dataLakesstate->cstate.cstate_modify;
#else
		CopyToState	cstate = dataLakesstate->cstate.cstate_modify;
#endif
		/* TEXT or CSV */
		slot_getallattrs(slot);
		CopyOneRowTo(cstate, slot);
		CopySendEndOfRow(cstate);

		StringInfo	fe_msgbuf = cstate->fe_msgbuf;
		writeToProvider(dataLakesstate->provider, fe_msgbuf->data, fe_msgbuf->len);

		/* Reset our buffer to start clean next round */
		cstate->fe_msgbuf->len = 0;
		cstate->fe_msgbuf->data[0] = '\0';
	}
	else
	{
		MemoryContext oldcontext;
		MemoryContextReset(dataLakesstate->rowcontext);
		oldcontext = MemoryContextSwitchTo(dataLakesstate->rowcontext);

		slot_getallattrs(slot);
		writeToProvider(dataLakesstate->provider, slot, 0);

		MemoryContextSwitchTo(oldcontext);
	}
}

void
fdwfunction_endModify(ResultRelInfo *resultRelInfo)
{
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		datalake_to_exttable_EndForeignModify(NULL, resultRelInfo);
	}

	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format) ||
		FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
#if (PG_VERSION_NUM < 140000)
		EndCopyFrom(dataLakesstate->cstate.cstate_modify);
#else
		EndCopyModify(dataLakesstate->cstate.cstate_modify);
#endif
	}

	if (dataLakesstate->provider)
	{
		destroyHandler(dataLakesstate->provider);
	}
	if (dataLakesstate->modify_state->us_provider)
	{
		destroyHandler(dataLakesstate->modify_state->us_provider);
	}
	/* Free global fileIndexMap for Iceberg tables */
	if (datalake_iceberg_file_index_map != NULL)
	{
		icebergFreeFileIndexMap(datalake_iceberg_file_index_map);
		datalake_iceberg_file_index_map = NULL;
	}
}

/*
 * hasZeorSelectedPartition
 * If the hivePartitionConstraints condition is a null then
 * there is no partition key table.
 */
bool fdwfunction_hasZeorSelectedPartition(dataLakeFdwScanState *dataLakesstate)
{
	if (dataLakesstate->options->hiveOption->partitiontable &&
		!dataLakesstate->options->hiveOption->hivePartitionConstraints)
	{
		return true;
	}
	return false;
}

/*
 * error context callback for Datalake
 *
 * The argument for the error context must be CopyFromState.
 */
void CsvTextErrorCallback(dataLakeFdwScanState *dataLakesstate)
{
	CopyFromState cstate = (CopyFromState) dataLakesstate->cstate.cstate_scan;
	char		curlineno_str[32];
	char		filename[1024];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	const char* name = getReadProviderFileName(dataLakesstate->provider);
	if (name != NULL)
	{
		snprintf(filename, sizeof(filename), "file %s,", name);
	}
	else
	{
		filename[0] = '\0';
	}

	if (cstate->opts.binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("Foreign table  %s, %s line %s, column %s",
					   cstate->cur_relname, filename, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("Foreign table %s, %s line %s",
					   cstate->cur_relname, filename, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("Foreign table %s, %s line %s, column %s: \"%s\"",
					   cstate->cur_relname, filename, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("Foreign table %s, %s line %s, column %s: null input",
					   cstate->cur_relname, filename, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, print it.
			 */
			if (cstate->line_buf_valid)
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("Foreign table %s, %s line %s: \"%s\"",
						   cstate->cur_relname, filename, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				errcontext("Foreign table %s, %s line %s",
						   cstate->cur_relname, filename, curlineno_str);
			}
		}
	}
}

void fdwfunction_DatalakeErrorCallback(void *arg)
{
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)arg;
	if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
		FORMAT_IS_TEXT(dataLakesstate->options->format))
	{
		CsvTextErrorCallback(dataLakesstate);
	}
}

List*
buildAttnameList(dataLakeFdwScanState *sstate)
{
	TupleDesc	tupDesc = RelationGetDescr(sstate->rel);
	List	   *attnames = NIL;

	if (sstate->options->hiveOption->hivePartitionKey)
	{
		int attnum;
		int nPartitionKey = list_length(sstate->options->hiveOption->attNums);
		int nattrs = tupDesc->natts - nPartitionKey;
		for (attnum = 1; attnum <= nattrs; attnum++)
		{
			attnames = lappend(attnames,
					makeString(pstrdup(NameStr(*attnumAttName(sstate->rel, attnum)))));
		}
	}
	return attnames;
}

/*
 * Initialize the data parsing state.
 *
 * This includes format descriptions (delimiter, quote...), format type
 * (text, csv), etc...
 */
static void
InitParseStateFrom(CopyFromState cstate, Relation relation,
				   char *uri, int rejectlimit,
				   bool islimitinrows, char logerrors)
{

	if (rejectlimit == -1)
	{
		cstate->cdbsreh = NULL; /* no SREH */
		cstate->errMode = ALL_OR_NOTHING;
	}
	else
	{
		if (logerrors)
		{
			/* errors into file */
			cstate->errMode = SREH_LOG;
		}
		else
		{
			/* no error log */
			cstate->errMode = SREH_IGNORE;
		}
		cstate->cdbsreh = makeCdbSreh(rejectlimit,
									  islimitinrows,
									  uri,
									  (char *) cstate->cur_relname,
									  logerrors);

		cstate->cdbsreh->relid = RelationGetRelid(relation);
	}



	/* and 'fe_mgbuf' */
	cstate->fe_msgbuf = makeStringInfo();
	
	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
												"DatalakeFdwMemCxt",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
}

/*
 * Set up CopyState for writing to an foreign table.
 */
#if (PG_VERSION_NUM < 140000)
static CopyState
BeginCopyTo(Relation forrel, List *options)
#else
static CopyToState
BeginCopyToModify(Relation forrel, List *options)
#endif
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate;
#else
	CopyToState	cstate;
#endif

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

#if (PG_VERSION_NUM <= 90500)
	cstate = BeginCopy(false, forrel, NULL, NULL, NIL, options, NULL);
#elif (PG_VERSION_NUM < 120000)
	cstate = BeginCopy(false, forrel, NULL, NULL, forrel->rd_id, NIL, options, NULL);
#elif (PG_VERSION_NUM < 140000)
	cstate = BeginCopy(NULL, false, forrel, NULL, forrel->rd_id, NIL, options, NULL);
#else
	cstate = BeginCopy(NULL, forrel, NULL, forrel->rd_id, NIL, options, NULL);
#endif
	cstate->dispatch_mode = COPY_DIRECT;

	/*
	 * We use COPY_CALLBACK to mean that the each line should be left in
	 * fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
#if (PG_VERSION_NUM < 140000)
	cstate->null_print_client = cstate->null_print; /* default */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->file_encoding,
														cstate->enc_conversion_proc);
#else
	cstate->opts.null_print_client = cstate->opts.null_print; /* default */
	if (cstate->need_transcoding)
		cstate->opts.null_print_client = pg_server_to_any(cstate->opts.null_print,
														  cstate->opts.null_print_len,
														  cstate->opts.file_encoding);
#endif
	return cstate;
}




#if (PG_VERSION_NUM >= 140000)
/*
 * Clean up storage and release resources for COPY FROM.
 */
static void
EndCopyScan(CopyFromState cstate)
{
	/* No COPY FROM related resources except memory. */
	Assert(!cstate->is_program);
	Assert(cstate->filename == NULL);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (cstate && cstate->cdbsreh)
		{
			CdbSreh	 *cdbsreh = cstate->cdbsreh;
			uint64	total_rejected_from_qd = cdbsreh->rejectcount;
				if (total_rejected_from_qd > 0)
					ReportSrehResults(cdbsreh, total_rejected_from_qd);
		}
	}

	if (cstate != NULL && cstate->errMode != ALL_OR_NOTHING)
	{
		if (Gp_role == GP_ROLE_EXECUTE)
		{
			SendNumRows(cstate->cdbsreh->rejectcount, 0);
		}
		destroyCdbSreh(cstate->cdbsreh);
	}

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Clean up storage and release resources for COPY TO.
 */
static void
EndCopyModify(CopyToState cstate)
{
	/* No COPY FROM related resources except memory. */
	Assert(!cstate->is_program);
	Assert(cstate->filename == NULL);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}
#endif



void
costDataLakeScan(ForeignPath *path, PlannerInfo *root,
				 RelOptInfo *baserel, ParamPathInfo *param_info)
{
	Cost		startup_cost = 0;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/* Mark the path with the correct row estimate */
	if (param_info)
		path->path.rows = param_info->ppi_rows;
	else
		path->path.rows = baserel->rows;

	/*
	 * disk costs
	 */
	run_cost += seq_page_cost * baserel->pages;

	/* CPU costs */
	startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	path->path.startup_cost = startup_cost;
	path->path.total_cost = startup_cost + run_cost;
}

void
fdwfunction_extract_used_attributes(RelOptInfo *baserel)
{
    dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState *) baserel->fdw_private;
    ListCell *lc;

    pull_varattnos((Node *) baserel->reltarget->exprs,
                   baserel->relid,
                   &fdw_private->attrs_used);

    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause,
                       baserel->relid,
                       &fdw_private->attrs_used);
    }

    if (bms_is_empty(fdw_private->attrs_used))
    {
        bms_free(fdw_private->attrs_used);
        fdw_private->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

void
fdwfunction_deparseTargetList(Relation rel, Bitmapset *attrs_used, List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}
}

/*
 * Build a target list (ie, a list of TargetEntry) for the Path's output.
 *
 * This is almost just make_tlist_from_pathtarget(), but we also have to
 * deal with replacing nestloop params.
 */
List *
fdwfunction_build_path_tlist(PlannerInfo *root, Path *path)
{
	List	   *tlist = NIL;
	Index	   *sortgrouprefs = path->pathtarget->sortgrouprefs;
	int			resno = 1;
	ListCell   *v;

	foreach(v, path->pathtarget->exprs)
	{
		Node	   *node = (Node *) lfirst(v);
		TargetEntry *tle;

		/*
		 * If it's a parameterized path, there might be lateral references in
		 * the tlist, which need to be replaced with Params.  There's no need
		 * to remake the TargetEntry nodes, so apply this to each list item
		 * separately.
		 */
		/* Not implemented yet! */
		if (path->param_info)
			Assert(false);

		tle = makeTargetEntry((Expr *) node,
							  resno,
							  NULL,
							  false);
		if (sortgrouprefs)
			tle->ressortgroupref = sortgrouprefs[resno - 1];

		tlist = lappend(tlist, tle);
		resno++;
	}
	return tlist;
}

/*
 * Common FDW function implementations
 */
void datalakefdw_begin_foreign_scan(ForeignScanState *node, int eflags, DatalakeFdwBeginScanConfig *config)
{
    elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignScan starts on segment: %d", GpIdentity.segindex);

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Initialize dataLakeFdwScanState */
    dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));
	dataLakesstate->scan_tupdesc			= CreateTupleDescCopy(node->ss.ps.scandesc);
    /* Get options using the provided function */
    dataLakesstate->options = config->get_options_func(config->context);
    dataLakesstate->rel = node->ss.ss_currentRelation;

    List* fragmentData = NIL;
    ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
    int segmentcount = getgpsegmentCount();
    List *selected_segments = NIL;
    dataLakesstate->provider = NULL;

    if (gp_external_enable_filter_pushdown)
        dataLakesstate->quals = node->ss.ps.plan->qual;

    /* Handle vectorization flag */
    if (eflags & EXEC_FLAG_VECTOR)
    {
        dataLakesstate->options->vectorization = true;
		datalakeCheckValidRecordBatchOpt(dataLakesstate->options);
    }

    List *retrieved_attrs = (List *) list_nth(foreignScan->fdw_private, FdwScanPrivateRetrievedAttrs);

    if (client_min_messages <= DEBUG1)
    {
        elog(DEBUG1, "foreignScan->fdw_private length: %d", list_length(foreignScan->fdw_private));
        if (foreignScan->fdw_private != NIL)
        {
            ListCell *lc;
            int idx = 0;
            foreach(lc, foreignScan->fdw_private)
            {
                elog(DEBUG1, "foreignScan->fdw_private[%d]: %s", idx++, nodeToString(lfirst(lc)));
            }
        }
    }

    /*
     * When queried at all of the nodes. Need to go to object storage
     * do list operation get file list. The file list is sent to the
     * segment for scheduling
     */
    if (Gp_role == GP_ROLE_DISPATCH)
    {
        fragmentData = config->get_fragment_data_func(config->context);
        if (fragmentData != NIL)
        {
            foreignScan->fdw_private = list_concat(foreignScan->fdw_private, fragmentData);
        }
        /* master does not process any fragments */
		List *random_segments = datalakeSelectRandomSegments(segmentcount, external_table_limit_segment_num);
        /* put the random segments into the list */
        foreignScan->fdw_private = list_concat(foreignScan->fdw_private, random_segments);
		/* register resource context for gopher */
		dataLakesstate->gopher_handle_t = gopher_registe_resource_context(/*gp_is_writer*/false);
        /* Set the final state */
		node->fdw_state = (void*)dataLakesstate;
		return;
    }

    /* the last segmentcount elements are the selected random segments */
    int len = list_length(foreignScan->fdw_private);
    for (int i = segmentcount; i >= 1; i--)
    {
        int idx = intVal(list_nth(foreignScan->fdw_private, len - i));
        selected_segments = lappend_int(selected_segments, idx);
    }

    /* remove the last segmentcount elements */
    foreignScan->fdw_private = list_truncate(foreignScan->fdw_private, len - segmentcount);
    fragmentData = datalakeDeserializeExternalFragmentList(dataLakesstate->rel, dataLakesstate->quals,
                                                   dataLakesstate->options, foreignScan->fdw_private);

    dataLakesstate->selected_segments = selected_segments;
    dataLakesstate->provider = initProvider(dataLakesstate->options->format, DL_OP_READ,
                                           dataLakesstate->options->vectorization);
    dataLakesstate->rel = node->ss.ss_currentRelation;
    dataLakesstate->fragments = fragmentData;
    dataLakesstate->retrieved_attrs = retrieved_attrs;

    if (fdwfunction_hasZeorSelectedPartition(dataLakesstate))
    {
        node->fdw_state = (void*)dataLakesstate;
        return;
    }

    fdwfunction_initScanStatue(node, dataLakesstate);

    /* Cache DatalakeRowReader for ultra fast-path scan bypass */
    if (FORMAT_IS_ICEBERG(dataLakesstate->options->format))
        dataLakesstate->fastScanReader = getProviderRowReader(dataLakesstate->provider);

    /* Set the final state */
    node->fdw_state = (void*)dataLakesstate;

    elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignScan ends on segment: %d", GpIdentity.segindex);
}

TupleTableSlot* datalakefdw_iterate_foreign_scan(ForeignScanState *node)
{
    dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;

    if (dataLakesstate->options->hiveOption->partitiontable
        && !dataLakesstate->options->hiveOption->hivePartitionConstraints)
    {
        TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
        ExecClearTuple(slot);
        return slot;
    }

    if (dataLakesstate->options->vectorization)
    {
        VirtualTupleTableSlot *vslot = (VirtualTupleTableSlot*)node->ss.ss_ScanTupleSlot;
        ExecClearTuple(&vslot->base);
        fdwfunction_iterateRecordBatch(dataLakesstate, vslot);
        return (TupleTableSlot*)vslot;
    }
    else
    {
        TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
        ErrorContextCallback errcallback;
        if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
            FORMAT_IS_TEXT(dataLakesstate->options->format) ||
            FORMAT_IS_CUSTOM(dataLakesstate->options->format))
        {
            /* Set up callback to identify error line number. */
            errcallback.callback = fdwfunction_DatalakeErrorCallback;
            errcallback.arg = (void *) dataLakesstate;
            errcallback.previous = error_context_stack;
            error_context_stack = &errcallback;
        }

        ExecClearTuple(slot);
		memset(slot->tts_isnull, 1, slot->tts_tupleDescriptor->natts * sizeof(bool));
        fdwfunction_iterateScanStatus(node, dataLakesstate);

        if (FORMAT_IS_CSV(dataLakesstate->options->format) ||
            FORMAT_IS_TEXT(dataLakesstate->options->format) ||
            FORMAT_IS_CUSTOM(dataLakesstate->options->format))
        {
            /* Remove error callback. */
            error_context_stack = errcallback.previous;
        }
        return slot;
    }
}

void datalakefdw_end_foreign_scan(ForeignScanState *node)
{
    elog(DEBUG5, "datalake_fdw: dataLakeEndForeignScan starts on segment: %d", GpIdentity.segindex);

	/* Do nothing in EXPLAIN (no ANALYZE) case. */
	if (node->fdw_state == NULL)
	{
		return;
	}

	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;

	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)node->fdw_state;

	/* Release resources */
	if (foreignScan->fdw_private)
	{
		elog(DEBUG5, "Freeing fdw_private");
		fdwfunction_freeFdwPrivate(sstate, foreignScan);
		foreignScan->fdw_private = NULL;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* release resource context for gopher */
		cleanup_gopher_resource_context(sstate->gopher_handle_t);
		sstate->gopher_handle_t = NULL;
		return;
	}

	if (FORMAT_IS_CUSTOM(sstate->options->format))
	{
		datalake_to_exttable_EndForeignScan(node);
	}

	fdwfunction_endScanStatus(sstate);

	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignScan ends on segment: %d", GpIdentity.segindex);
}

void datalakefdw_begin_foreign_modify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags,
					  DatalakeFdwBeginScanConfig *config)
{
	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignModify starts on segment: %d", GpIdentity.segindex);

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;
	int i;
	dataLakeFdwScanState *dataLakesstate  = (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));
	dataLakesstate->modify_state = (dataLakeModifyState *) palloc0(sizeof(dataLakeModifyState));
	Relation	relation 			= resultRelInfo->ri_RelationDesc;
	dataLakesstate->options 		= config->get_options_func(config->context);
	dataLakesstate->rel				= relation;
	resultRelInfo->ri_FdwState = dataLakesstate;
	dataLakesstate->modify_state->us_provider = NULL;
	dataLakesstate->modify_state->us_slot = NULL;
	dataLakesstate->cmd = mtstate->operation;
	dataLakesstate->collect_qe_metadata =
		((eflags & DATALAKE_FDW_EFLAG_COLLECT_QE_METADATA) != 0);
	dataLakesstate->local_meta_list = NIL;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		FDW_InitMetaMap();
		FDW_ClearMetaList(RelationGetRelid(relation));
		return;
	}
	if (fdw_private == NULL)
	{
		/* Volume FDW path: when no private data from planner, use filePath from options
		 * This happens when volume FDW constructs the file path directly from volume
		 * configuration rather than getting it from the query planner's private data */
		dataLakesstate->fragments = lappend(dataLakesstate->fragments, pstrdup(dataLakesstate->options->filePath));
	}
	else
	{
		Value *val = lfirst(list_nth_cell(fdw_private, FdwModifyFileDir));
		dataLakesstate->fragments = lappend(dataLakesstate->fragments, pstrdup(val->val.str));
	}
	if (mtstate->operation == CMD_UPDATE || mtstate->operation == CMD_DELETE)
	{
		dataLakesstate->modify_state->us_ctid_no = ExecFindJunkAttributeInTlist(outerPlanState(mtstate)->plan->targetlist, "ctid");
		if (!AttributeNumberIsValid(dataLakesstate->modify_state->us_ctid_no))
			elog(ERROR, "could not find junk ctid column");

		TupleDesc target_tupdesc = CreateTemplateTupleDesc(2);
		for (i = 0; i < DATALAKE_ICEBERG_JUNK_NUM; i++)
		{
			IcebergJunkInfo *info = &datalake_iceberg_junk_info[i];
			TupleDescInitEntry(target_tupdesc, (AttrNumber) (i + 1), info->name, info->type, -1, 0);
		}
		dataLakesstate->modify_state->us_slot = MakeSingleTupleTableSlot(target_tupdesc, &TTSOpsVirtual);

		/*
		 * For Iceberg UPDATE/DELETE operations, create global fileIndexMap.
		 * This will be used to map file IDs to file paths during the modify phase.
		 * It will be freed in EndForeignModify.
		 * We also register a memory context callback to ensure cleanup on error/abort.
		 */
		if (FORMAT_IS_ICEBERG(dataLakesstate->options->format) &&
			datalake_iceberg_file_index_map == NULL)
		{
			MemoryContextCallback *mcb;

			datalake_iceberg_file_index_map = icebergCreateFileIndexMap();
			if (datalake_iceberg_file_index_map == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("failed to create Iceberg file index map")));

			/*
			 * Register a callback to clean up the fileIndexMap when the memory context
			 * is reset or deleted. This ensures cleanup happens even in case of errors
			 * or transaction abort.
			 */
			mcb = MemoryContextAlloc(CurrentMemoryContext,
									  sizeof(MemoryContextCallback));
			mcb->func = fileIndexMapCallback;
			mcb->arg = NULL;
			MemoryContextRegisterResetCallback(CurrentMemoryContext, mcb);

			elog(DEBUG2, "datalake_fdw: Created global Iceberg file index map in BeginForeignModify for %s",
					mtstate->operation == CMD_UPDATE ? "UPDATE" : "DELETE");
		}
		else if (FORMAT_IS_ICEBERG(dataLakesstate->options->format) &&
				 datalake_iceberg_file_index_map != NULL)
		{
			/* Clear existing map for new UPDATE/DELETE operation */
			icebergClearFileIndexMap(datalake_iceberg_file_index_map);
			elog(DEBUG2, "datalake_fdw: Cleared existing Iceberg file index map in BeginForeignModify");
		}

		/*
		 * Issue #333: foreign-table path -- dataLakePlanForeignModify()
		 * computed the complete fragment list at plan time and dispatched
		 * it via fdw_private.  Populate the global file_id -> file_path
		 * map from it so writer-only QEs (whose slice contains no
		 * ForeignScan after a Redistribute Motion) can still decode the
		 * ctid junk column.
		 *
		 * The Iceberg AM path lands here with fdw_private == NULL; its
		 * caller (iceberg_modify_init in pg_iceberg_am_handler.c) populates
		 * the map after this function returns, using AM-specific catalog
		 * helpers.
		 */
		if (FORMAT_IS_ICEBERG(dataLakesstate->options->format) &&
			datalake_iceberg_file_index_map != NULL &&
			fdw_private != NULL &&
			list_length(fdw_private) > FdwModifyAllFragments)
		{
			List *allFragments = (List *) list_nth(fdw_private,
												   FdwModifyAllFragments);

			if (allFragments != NIL)
				icebergFileIndexMapPopulateFromAllFragments(
					datalake_iceberg_file_index_map, allFragments);
		}
	}
	fdwfunction_initModify(mtstate, resultRelInfo);

	elog(DEBUG5, "datalake_fdw: dataLakeBeginForeignModify ends on segment: %d", GpIdentity.segindex);
}

TupleTableSlot *
datalakefdw_exec_foreign_insert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignInsert starts on segment: %d", GpIdentity.segindex);
	fdwfunction_insertModify(resultRelInfo, slot);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignInsert ends on segment: %d", GpIdentity.segindex);
	return slot;
}

void datalakefdw_end_foreign_modify(EState *estate,
					ResultRelInfo *resultRelInfo,
					DatalakeFdwBeginScanConfig *config)
{
	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignModify starts on segment: %d", GpIdentity.segindex);

	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)resultRelInfo->ri_FdwState;
	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (dataLakesstate == NULL)
	{
		return;
	}
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		Oid relid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		List *meta_list = FDW_GetMetaList(relid);

		if (dataLakesstate->options->format == DL_ICEBERG_TABLE && meta_list != NIL)
		{
			config->get_append_metadata_func(resultRelInfo->ri_RelationDesc,
											 dataLakesstate, meta_list,
											 config->context);
		}
		FDW_ClearMetaList(relid);

		if (dataLakesstate->modify_state)
			pfree(dataLakesstate->modify_state);
		datalakeFreeDatalakeOptions(dataLakesstate->options);
		pfree(dataLakesstate);
		return;
	}
	else
	{
		/*
		 * fdwfunction_endModify() tears down provider/copy/writer but no
		 * longer releases dataLakesstate, so we can still read state fields
		 * (options, cmd, local_meta_list) after it returns.
		 *
		 * destroyHandler() now transfers FileFragment ownership directly
		 * into dataLakesstate->local_meta_list when collect_qe_metadata
		 * is true, instead of going through the serialize/deserialize
		 * round-trip via FDW_SendMeta.
		 */
		fdwfunction_endModify(resultRelInfo);

		/* QE vacuum local collection path: harvest and process metadata */
		if (dataLakesstate->options->format == DL_ICEBERG_TABLE &&
			dataLakesstate->collect_qe_metadata &&
			dataLakesstate->local_meta_list != NIL &&
			config != NULL &&
			config->get_append_metadata_func != NULL)
		{
			config->get_append_metadata_func(resultRelInfo->ri_RelationDesc,
											 dataLakesstate,
											 dataLakesstate->local_meta_list,
											 config->context);
		}

		FDW_FreeMetaList(dataLakesstate->local_meta_list);
		dataLakesstate->local_meta_list = NIL;

		/* Release state (moved here from fdwfunction_endModify) */
		if (dataLakesstate->modify_state)
			pfree(dataLakesstate->modify_state);
		datalakeFreeDatalakeOptions(dataLakesstate->options);
		pfree(dataLakesstate);
	}

	elog(DEBUG5, "datalake_fdw: dataLakeEndForeignModify ends on segment: %d", GpIdentity.segindex);

}

TupleTableSlot *datalake_exec_foreign_update(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignUpdate starts on segment: %d", GpIdentity.segindex);

	dataLakeFdwScanState	*sstate			= (dataLakeFdwScanState*)rinfo->ri_FdwState;
	dataLakeModifyState		*mstate			= sstate->modify_state;
	TupleTableSlot			*junk_slot		= mstate->us_slot;

	Assert(mstate);
	ExecClearTuple(junk_slot);

	/* For Iceberg tables, decode tid to get file path and position */
	Datum		datum;
	bool		isNull;
	ItemPointer	ctid;

	datum = ExecGetJunkAttribute(planSlot,
								mstate->us_ctid_no,
								&isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");
	ctid = (ItemPointer) DatumGetPointer(datum);
	uint32 fileId;
	int64 position;
	const char *filePath;

	/* Decode tid to get fileId and position */
	icebergDecodeTID(ctid, &fileId, &position);

	/* Get file path from global fileIndexMap */
	filePath = icebergGetFilePath(datalake_iceberg_file_index_map, fileId);
	if (filePath == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to get file path for file ID %u in Iceberg update", fileId)));
	}

	/* Fill junk slot with file path and position */
	junk_slot->tts_values[0] = CStringGetDatum(filePath);
	junk_slot->tts_isnull[0] = false;
	junk_slot->tts_values[1] = Int64GetDatum((int64) position);
	junk_slot->tts_isnull[1] = false;

	elog(DEBUG2, "datalake_fdw: UPDATE writing delete file=%s, pos="UINT64_FORMAT" for file ID %u",
			filePath, position, fileId);

	MemoryContext oldcontext;
	MemoryContextReset(sstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(sstate->rowcontext);

	slot_getallattrs(slot);
	writeToProvider(sstate->provider, slot, 0);
	writeToProvider(mstate->us_provider, junk_slot, 0);

	MemoryContextSwitchTo(oldcontext);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignUpdate ends on segment: %d", GpIdentity.segindex);
	return slot;
}

void datalake_add_foreign_update_targets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation)
{
	Oid			reloid;
	Oid			vartypeid;
	int32		type_mod;
	Oid			type_coll;
	Var			*var;

	reloid = RelationGetRelid(target_relation);
	get_atttypetypmodcoll(reloid, GpSegmentIdAttributeNumber, &vartypeid, &type_mod, &type_coll);
	var = makeVar(rtindex,
				GpSegmentIdAttributeNumber,
				vartypeid,
				type_mod,
				type_coll,
				0);
	add_row_identity_var(root, var, rtindex, "gp_segment_id");

	get_atttypetypmodcoll(reloid, SelfItemPointerAttributeNumber, &vartypeid, &type_mod, &type_coll);
	var = makeVar(rtindex,
				SelfItemPointerAttributeNumber,
				vartypeid,
				type_mod,
				type_coll,
				0);
	add_row_identity_var(root, var, rtindex, "ctid");
}

TupleTableSlot *datalake_exec_foreign_delete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	dataLakeFdwScanState	*sstate			= (dataLakeFdwScanState*)rinfo->ri_FdwState;
	dataLakeModifyState		*mstate			= sstate->modify_state;
	TupleTableSlot			*junk_slot		= mstate->us_slot;

	Assert(mstate);
	ExecClearTuple(junk_slot);

	/* For Iceberg tables, decode tid to get file path and position */
	Datum		datum;
	bool		isNull;
	ItemPointer	ctid;

	datum = ExecGetJunkAttribute(planSlot,
								mstate->us_ctid_no,
								&isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");
	ctid = (ItemPointer) DatumGetPointer(datum);
	uint32 fileId;
	int64 position;
	const char *filePath;

	/* Decode tid to get fileId and position */
	icebergDecodeTID(ctid, &fileId, &position);

	/* Get file path from global fileIndexMap */
	filePath = icebergGetFilePath(datalake_iceberg_file_index_map, fileId);
	if (filePath == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to get file path for file ID %u in Iceberg update", fileId)));
	}

	/* Fill junk slot with file path and position */
	junk_slot->tts_values[0] = CStringGetDatum(filePath);
	junk_slot->tts_isnull[0] = false;
	junk_slot->tts_values[1] = Int64GetDatum((int64) position);
	junk_slot->tts_isnull[1] = false;

	elog(DEBUG2, "datalake_fdw: UPDATE writing delete file=%s, pos="UINT64_FORMAT" for file ID %u",
			filePath, position, fileId);

	MemoryContext oldcontext;
	MemoryContextReset(sstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(sstate->rowcontext);

	writeToProvider(mstate->us_provider, junk_slot, 0);

	MemoryContextSwitchTo(oldcontext);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignUpdate ends on segment: %d", GpIdentity.segindex);
	return slot;
}

int datalake_is_foreign_rel_updatable(Relation rel)
{
	elog(DEBUG5, "datalake_fdw: dataLakeIsForeignRelUpdatable starts on segment: %d", FDW_DATALAKE_SEGMENT_ID);
	int updatable = 0;
	dataLakeOptions *opts = datalakeGetOptions(RelationGetRelid(rel));
	switch (opts->format)
	{
		case DL_ICEBERG_TABLE:
		{
			updatable = 1u << (int) CMD_INSERT | 1u << (int) CMD_UPDATE | 1u << (int) CMD_DELETE;
			break;
		}
		case DL_HUDI_TABLE:
		{
			updatable = 0u << (int) CMD_INSERT | 0u << (int) CMD_UPDATE | 0u << (int) CMD_DELETE;
			break;
		}
		default:
		{
			updatable = 1u << (int) CMD_INSERT | 0u << (int) CMD_UPDATE | 0u << (int) CMD_DELETE;
		}
	}
	datalakeFreeDatalakeOptions(opts);
	elog(DEBUG5, "datalake_fdw: dataLakeIsForeignRelUpdatable ends on segment: %d", FDW_DATALAKE_SEGMENT_ID);
	return updatable;
}

List *
build_update_tlist_with_junk(Oid foreigntableid, Index varno)
{
	int				i;
	List		   *tlist	= NIL;
	Relation		rel		= heap_open(foreigntableid, NoLock);
	TupleDesc		tupdesc	= RelationGetDescr(rel);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;

		Var *var = makeVar(varno,
							attr->attnum,
							attr->atttypid,
							attr->atttypmod,
							attr->attcollation,
							0);

		TargetEntry *tle = makeTargetEntry((Expr *) var,
											attr->attnum,
											pstrdup(NameStr(attr->attname)),
											false);

		tlist = lappend(tlist, tle);
	}

	for (i = 0; i < sizeof(datalake_iceberg_junk_info) / sizeof(IcebergJunkInfo); i++)
	{
		AttrNumber attno = tupdesc->natts + datalake_iceberg_junk_info[i].attno;
		Var *var = makeVar(varno,
							attno,
							datalake_iceberg_junk_info[i].type,
							-1,
							InvalidOid,
							0);
		TargetEntry *tle = makeTargetEntry((Expr *) var,
											attno,
											datalake_iceberg_junk_info[i].name,
											true);
		tlist = lappend(tlist, tle);
	}

	{
		Var *var = makeVar(varno,
							GpSegmentIdAttributeNumber,
							INT4OID,
							-1,
							0,
							0);
		TargetEntry *tle = makeTargetEntry((Expr *) var,
											GpSegmentIdAttributeNumber,
											"gp_segment_id",
											true);
		tlist = lappend(tlist, tle);

		var = makeVar(varno,
						0,
						RECORDOID,
						-1,
						0,
						0);
		tle = makeTargetEntry((Expr *) var,
								0,
								"wholerow",
								true);
		tlist = lappend(tlist, tle);
	}

	heap_close(rel, NoLock);

	return tlist;
}

#if PG_VERSION_NUM >= 90500
ForeignScan *
datalake_get_foreign_plan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan)
#else
ForeignScan *
datalake_get_foreign_plan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses)
#endif
{
	dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState*)baserel->fdw_private;

	Index		scan_relid = baserel->relid;


	scan_clauses = extract_actual_clauses(scan_clauses, false);

	List* private_lists = list_make2(makeString("scan"), fdw_private->retrieved_attrs);

	return make_foreignscan(
							tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							private_lists
	#if PG_VERSION_NUM >= 90500
								,NIL
								,NIL
								,outer_plan
	#endif
	);
}

void
datalake_get_foreign_paths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
	ForeignPath *path = NULL;
	int			total_cost = 50000;
	Relation	rel;
	dataLakeFdwPlanState *fdw_private = (dataLakeFdwPlanState*)baserel->fdw_private;

	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	rel = table_open(rte->relid, NoLock);

	/* Collect used attributes to reduce number of read columns during scan */
    fdwfunction_extract_used_attributes(baserel);

	fdwfunction_deparseTargetList(rel, fdw_private->attrs_used, &fdw_private->retrieved_attrs);

	path = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
								   NULL,	/* default pathtarget */
#endif
								   baserel->rows,
								   50000,
								   total_cost,
								   NIL, /* no pathkeys */
								   NULL,	/* no outer rel either */
#if PG_VERSION_NUM >= 90500
								   NULL,	/* no extra plan */
#endif
								   NULL);
	heap_close(rel, NoLock);
	/*
	 * Create a ForeignPath node and add it as only possible path.
	 */

	costDataLakeScan(path, root, baserel, path->path.param_info);

	add_path(baserel, (Path *) path, root);
	set_cheapest(baserel);
}

void
datalake_get_foreign_relsize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	dataLakeFdwPlanState* fdw_private = (dataLakeFdwPlanState *) palloc0(sizeof(dataLakeFdwPlanState));
	baserel->fdw_private = fdw_private;

	set_baserel_size_estimates(root, baserel);
}

/*
 * fileIndexMapCallback
 *		Callback function to clean up global fileIndexMap when memory context is reset.
 *		This ensures cleanup happens even in case of errors or transaction abort.
 */
static void
fileIndexMapCallback(void *arg)
{
	/* Free global fileIndexMap for Iceberg tables */
	if (datalake_iceberg_file_index_map != NULL)
	{
		elog(DEBUG2, "datalake_fdw: Cleaning up global Iceberg file index map via callback");
		icebergFreeFileIndexMap(datalake_iceberg_file_index_map);
		datalake_iceberg_file_index_map = NULL;
	}
}

