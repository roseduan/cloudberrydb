#include "postgres.h"

#include "exttable.h"

#include "access/external.h"
#include "access/formatter.h"
#include "access/heapam.h"
#include "access/url.h"
#include "access/valid.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/copy.h"
#include "commands/copyto_internal.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "parser/parse_func.h"
#include "utils/guc.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"

#include "src/datalake_def.h"

/* ----------------------------------------------------------------
 *				   external_ interface functions
 * ----------------------------------------------------------------
 */

#ifdef FILEDEBUGALL
#define FILEDEBUG_1 \
elog(DEBUG2, "external_getnext([%s],dir=%d) called", \
	 RelationGetRelationName(scan->fs_rd), (int) direction)
#define FILEDEBUG_2 \
elog(DEBUG2, "external_getnext returning EOS")
#define FILEDEBUG_3 \
elog(DEBUG2, "external_getnext returning tuple")
#else
#define FILEDEBUG_1
#define FILEDEBUG_2
#define FILEDEBUG_3
#endif   /* !defined(FILEDEBUGALL) */


/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 */
#define FILEAM_HANDLE_ERROR \
if (pstate->errMode == ALL_OR_NOTHING) \
{ \
	/* re-throw error and abort */ \
	PG_RE_THROW(); \
} \
else \
{ \
	/* SREH - release error state */ \
\
	ErrorData	*edata; \
	MemoryContext oldcontext;\
\
	/* SREH must only handle data errors. all other errors must not be caught */\
	if(ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)\
	{\
		PG_RE_THROW(); \
	}\
\
	/* save a copy of the error info */ \
	oldcontext = MemoryContextSwitchTo(pstate->cdbsreh->badrowcontext);\
	edata = CopyErrorData();\
	MemoryContextSwitchTo(oldcontext);\
\
	if (!elog_dismiss(DEBUG5)) \
		PG_RE_THROW(); /* <-- hope to never get here! */ \
\
	truncateEol(&pstate->line_buf, pstate->opts.eol_type); \
	pstate->cdbsreh->rawdata->cursor = 0; \
	pstate->cdbsreh->rawdata->data = pstate->line_buf.data; \
	pstate->cdbsreh->rawdata->len = pstate->line_buf.len; \
	pstate->cdbsreh->linenumber = pstate->cur_lineno; \
	pstate->cdbsreh->processed++; \
\
	/* set the error message. Use original msg and add column name if available */ \
	if (pstate->cur_attname)\
	{\
		pstate->cdbsreh->errmsg = psprintf("%s, column %s", \
				edata->message, \
				pstate->cur_attname); \
	}\
	else\
	{\
		pstate->cdbsreh->errmsg = pstrdup(edata->message); \
	}\
\
	HandleSingleRowError(pstate->cdbsreh); \
	FreeErrorData(edata);\
	if (!IsRejectLimitReached(pstate->cdbsreh)) \
		pfree(pstate->cdbsreh->errmsg); \
}

static DatalakeFileScanDesc external_beginscan(dataLakeFdwScanState *sstate,
											bool isMasterOnly, List *uriList,
											List *extOptions);

static HeapTuple
external_getnext(ForeignScanState *node, dataLakeFdwScanState *sstate);

static void external_rescan(DatalakeFileScanDesc scan);
static void external_endscan(dataLakeFdwScanState *dataLakesstate);
static void external_stopscan(DatalakeFileScanDesc scan);

/* ----------------------------------------------------------------
 *				   external help functions
 * ----------------------------------------------------------------
 */
static Oid
lookupCustomFormatter(List **options, bool iswritable);

static HeapTuple
externalgettup(dataLakeFdwScanState *sstate);

static HeapTuple
externalgettup_custom(dataLakeFdwScanState *sstate);

static ExternalSelectDesc
external_getnext_init(PlanState *state);

static bool
ExternalConstraintCheck(TupleTableSlot *slot, DatalakeFileScanDesc scandesc, EState *estate);

static bool
ExternalPartitionCheck(TupleTableSlot *slot, DatalakeFileScanDesc scandesc, EState *estate);

static void
FunctionCallPrepareFormatter(FunctionCallInfoBaseData *fcinfo,
							 int nArgs,
							 CopyFormatOptions *opts,
							 FmgrInfo *formatter_func,
							 List *formatter_params,
							 FormatterData *formatter,
							 Relation rel,
							 TupleDesc tupDesc,
							 FmgrInfo *convFuncs,
							 Oid *typioparams,
							 bool raw_reached_eof,
							 bool need_transcoding,
							 MemoryContext rowcontext);

static void
justifyDatabuf(StringInfo buf);


static
DatalakeExternalInsertDesc external_insert_init(dataLakeFdwScanState *dataLakesstate, List* extOption);

static void
external_insert(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot);

static void
external_insert_finish(DatalakeExternalInsertDesc extInsertDesc);

/* ----------------
 *		external_beginscan	- begin file scan
 * ----------------
 */
static DatalakeFileScanDesc
external_beginscan(dataLakeFdwScanState *sstate, bool isMasterOnly, List *uriList, List *extOptions)
{
	DatalakeFileScanDesc scan;
	TupleDesc	tupDesc = NULL;
	int			attnum;
	List	   *custom_formatter_params = NIL;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(sstate->rel);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (DatalakeFileScanDesc) palloc0(sizeof(DatalakeFileScanDescData));

	scan->fs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->fs_ctup.t_self);
	scan->fs_rd = sstate->rel;
	scan->fs_scancounter = 0;
	scan->fs_noop = false;
	scan->fs_file = NULL;
	scan->fs_formatter = NULL;
	scan->fs_constraintExprs = NULL;
	if (sstate->rel->rd_att->constr != NULL && sstate->rel->rd_att->constr->num_check > 0)
		scan->fs_hasConstraints = true;
	else
		scan->fs_hasConstraints = false;

	scan->fs_isPartition = sstate->rel->rd_rel->relispartition;

	tupDesc = RelationGetDescr(sstate->rel);
	scan->fs_tupDesc = tupDesc;
	scan->num_phys_attrs = tupDesc->natts;

	scan->values = (Datum *) palloc(scan->num_phys_attrs * sizeof(Datum));
	scan->nulls = (bool *) palloc(scan->num_phys_attrs * sizeof(bool));

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function and the element type (to pass to
	 * the input function).
	 */
	scan->in_functions = (FmgrInfo *) palloc(scan->num_phys_attrs * sizeof(FmgrInfo));
	scan->typioparams = (Oid *) palloc(scan->num_phys_attrs * sizeof(Oid));

	for (attnum = 1; attnum <= scan->num_phys_attrs; attnum++)
	{
		/* We don't need info for dropped attributes */
		Form_pg_attribute attr = TupleDescAttr(scan->fs_tupDesc, attnum - 1);

		if (attr->attisdropped)
			continue;

		getTypeInputInfo(attr->atttypid,
						 &scan->in_func_oid, &scan->typioparams[attnum - 1]);
		fmgr_info(scan->in_func_oid, &scan->in_functions[attnum - 1]);
	}

	custom_formatter_params = list_copy(extOptions);

	if (FORMAT_IS_CUSTOM(sstate->options->format))
	{
		/*
		 * Custom format: get formatter name and find it in the catalog
		 */
		Oid			procOid;

		/* parseFormatString should have seen a formatter name */
		procOid = lookupCustomFormatter(&custom_formatter_params, false);

		/* we found our function. set it up for calling */
		scan->fs_custom_formatter_func = palloc(sizeof(FmgrInfo));
		fmgr_info(procOid, scan->fs_custom_formatter_func);
		scan->fs_custom_formatter_params = custom_formatter_params;

		scan->fs_formatter = (FormatterData *) palloc0(sizeof(FormatterData));
		initStringInfo(&scan->fs_formatter->fmt_databuf);
		scan->fs_formatter->fmt_perrow_ctx = sstate->cstate.cstate_scan->rowcontext;
	}

	/* pgstat_initstats(relation); */

	return scan;
}

/*
 * setCustomFormatter
 *
 * Given a formatter name and a function signature (pre-determined
 * by whether it is readable or writable) find such a function in
 * the catalog and store it to be used later.
 *
 * WET function: 1 record arg, return bytea.
 * RET function: 0 args, returns record.
 */
static Oid
lookupCustomFormatter(List **options, bool iswritable)
{
	ListCell   *cell;
	char	   *formatter_name = NULL;
	List	   *funcname = NIL;
	Oid			procOid = InvalidOid;
	Oid			argList[1];
	Oid			returnOid;

	/*
	 * The formatter is defined as a "formatter=<name>" tuple in the options
	 * array. Extract into a separate list in order to scan the catalog for
	 * the function definition.
	 */
	foreach(cell, *options)
	{
		DefElem *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "formatter") == 0)
		{
			formatter_name = defGetString(defel);
			funcname = list_make1(makeString(formatter_name));
			*options = foreach_delete_current(*options, cell);
			break;
		}
	}
	if (list_length(funcname) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("formatter function not found in table options")));

	if (iswritable)
	{
		argList[0] = RECORDOID;
		returnOid = BYTEAOID;
		procOid = LookupFuncName(funcname, 1, argList, true);
	}
	else
	{
		returnOid = RECORDOID;
		procOid = LookupFuncName(funcname, 0, argList, true);
	}

	if (!OidIsValid(procOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("formatter function \"%s\" of type %s was not found",
						formatter_name,
						(iswritable ? "writable" : "readable")),
				 errhint("Create it with CREATE FUNCTION.")));

	/* check return type matches */
	if (get_func_rettype(procOid) != returnOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("formatter function \"%s\" of type %s has an incorrect return type",
						formatter_name,
						(iswritable ? "writable" : "readable"))));

	/* check allowed volatility */
	if (func_volatile(procOid) != PROVOLATILE_STABLE &&
		func_volatile(procOid) != PROVOLATILE_IMMUTABLE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("formatter function %s is not declared STABLE or IMMUTABLE",
						formatter_name)));

	return procOid;
}

/*
 * Prepare the formatter data to be used inside the formatting UDF.
 * This function should be called every time before invoking the
 * UDF, for both insert and scan operations. Even though there's some
 * redundancy here, it is needed in order to reset some per-call state
 * that should be examined by the caller upon return from the UDF.
 *
 * Also, set up the function call context.
 */
static void
FunctionCallPrepareFormatter(FunctionCallInfoBaseData *fcinfo,
							 int nArgs,
							 CopyFormatOptions *opts,
							 FmgrInfo *formatter_func,
							 List *formatter_params,
							 FormatterData *formatter,
							 Relation rel,
							 TupleDesc tupDesc,
							 FmgrInfo *convFuncs,
							 Oid *typioparams,
							 bool raw_reached_eof,
							 bool need_transcoding,
							 MemoryContext rowcontext)
{
	formatter->type = T_FormatterData;
	formatter->fmt_relation = rel;
	formatter->fmt_tupDesc = tupDesc;
	formatter->fmt_notification = FMT_NONE;
	formatter->fmt_badrow_len = 0;
	formatter->fmt_badrow_num = 0;
	formatter->fmt_args = formatter_params;
	formatter->fmt_conv_funcs = convFuncs;
	formatter->fmt_saw_eof = raw_reached_eof;
	formatter->fmt_typioparams = typioparams;
	formatter->fmt_perrow_ctx = rowcontext;
	formatter->fmt_needs_transcoding = need_transcoding;
	formatter->fmt_external_encoding = opts->file_encoding;

	InitFunctionCallInfoData( /* FunctionCallInfoData */ *fcinfo,
							  /* FmgrInfo */ formatter_func,
							  /* nArgs */ nArgs,
							  /* collation */ InvalidOid,
							  /* Call Context */ (Node *) formatter,
							  /* ResultSetInfo */ NULL);
}

/*
 * justifyDatabuf
 *
 * shift all data remaining in the buffer (anything from cursor to
 * end of buffer) to the beginning of the buffer, and readjust the
 * cursor and length to the new end of buffer position.
 *
 * 3 possible cases:
 *	 1 - cursor at beginning of buffer (whole buffer is a partial row) - nothing to do.
 *	 2 - cursor at end of buffer (last row ended in the last byte of the buffer)
 *	 3 - cursor at middle of buffer (remaining bytes are a partial row)
 */
static void
justifyDatabuf(StringInfo buf)
{
	/* 1 */
	if (buf->cursor == 0)
	{
		/* nothing to do */
	}
	/* 2 */
	else if (buf->cursor == buf->len)
	{
		Assert(buf->data[buf->cursor] == '\0');
		resetStringInfo(buf);
	}
	/* 3 */
	else
	{
		char	   *position = buf->data + buf->cursor;
		int			remaining = buf->len - buf->cursor;

		/* slide data back (data may overlap so use memmove not memcpy) */
		memmove(buf->data, position, remaining);

		buf->len = remaining;
		buf->data[buf->len] = '\0';		/* be consistent with
										 * appendBinaryStringInfo() */
	}

	buf->cursor = 0;
}

/* ----------------
*		externalgettup	form another tuple from the data file.
*		This is the workhorse - make sure it's fast!
*
*		Initialize the scan if not already done.
*		Verify that we are scanning forward only.
*
* ----------------
*/
static HeapTuple
externalgettup(dataLakeFdwScanState *sstate)
{
	HeapTuple	tup = NULL;
	if (sstate->customState.fdw_state == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("datalake unexpect error, invalid exttable custom state.")));
	}
	if (sstate->customState.fdw_state->ess_ScanDesc == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("datalake unexpect error, invalid fileScanDesc.")));
	}
	if (sstate->customState.fdw_state->ess_ScanDesc->fs_custom_formatter_func == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("datalake unexpect error, invalid custom formatter function.")));
	}

	tup = externalgettup_custom(sstate);  /* custom */
	return tup;
}

static HeapTuple
externalgettup_custom(dataLakeFdwScanState *sstate)
{
	HeapTuple	tuple;
	CopyFromState	pstate = sstate->cstate.cstate_scan;
	DatalakeFileScanDesc scan = sstate->customState.fdw_state->ess_ScanDesc;
	FormatterData *formatter = scan->fs_formatter;
	MemoryContext oldctxt = CurrentMemoryContext;

	Assert(formatter);
	Assert(pstate->raw_buf_len >= 0);

	/* while didn't finish processing the entire file */
	/* raw_buf_len was set to 0 in BeginCopyFrom() or external_rescan() */
	while (pstate->raw_buf_len != 0 || !pstate->raw_reached_eof)
	{
		/* need to fill our buffer with data? */
		if (pstate->raw_buf_len == 0)
		{
			int			bytesread = readBufferFromProvider(sstate->provider, pstate->raw_buf, RAW_BUF_SIZE);
			if (bytesread > 0)
			{
				appendBinaryStringInfo(&formatter->fmt_databuf, pstate->raw_buf, bytesread);
				pstate->raw_buf_len = bytesread;
			}
			else
			{
				pstate->raw_reached_eof = true;
			}

			/* HEADER not yet supported ... */
			if (pstate->opts.header_line)
				elog(ERROR, "header line in custom format is not yet supported");
		}

		/* while there is still data in our buffer */
		while (pstate->raw_buf_len > 0)
		{
			bool		error_caught = false;

			/*
			 * Invoke the custom formatter function.
			 */
			PG_TRY();
			{
				LOCAL_FCINFO(fcinfo, 0);

				/* per call formatter prep */
				FunctionCallPrepareFormatter(fcinfo,
						0,
						&pstate->opts,
						scan->fs_custom_formatter_func,
						scan->fs_custom_formatter_params,
						formatter,
						scan->fs_rd,
						scan->fs_tupDesc,
						scan->in_functions,
						scan->typioparams,
						pstate->raw_reached_eof,
						pstate->need_transcoding,
						pstate->rowcontext);
				(void) FunctionCallInvoke(fcinfo);

			}
			PG_CATCH();
			{
				error_caught = true;

				MemoryContextSwitchTo(formatter->fmt_perrow_ctx);

				/*
				 * Save any bad row information that was set by the user
				 * in the formatter UDF (if any). Then handle the error in
				 * FILEAM_HANDLE_ERROR.
				 */
				pstate->cur_lineno = formatter->fmt_badrow_num;
				resetStringInfo(&pstate->line_buf);

				if (formatter->fmt_badrow_len > 0)
				{
					if (formatter->fmt_badrow_data)
						appendBinaryStringInfo(&pstate->line_buf,
								formatter->fmt_badrow_data,
								formatter->fmt_badrow_len);

					formatter->fmt_databuf.cursor += formatter->fmt_badrow_len;
					if (formatter->fmt_databuf.cursor > formatter->fmt_databuf.len ||
							formatter->fmt_databuf.cursor < 0)
					{
						formatter->fmt_databuf.cursor = formatter->fmt_databuf.len;
					}
				}

				FILEAM_HANDLE_ERROR;
				FlushErrorState();

				MemoryContextSwitchTo(oldctxt);
			}
			PG_END_TRY();

			/*
			 * Examine the function results. If an error was caught we
			 * already handled it, so after checking the reject limit,
			 * loop again and call the UDF for the next tuple.
			 */
			if (!error_caught)
			{
				switch (formatter->fmt_notification)
				{
					case FMT_NONE:

						/* got a tuple back */

						tuple = formatter->fmt_tuple;

						if (pstate->cdbsreh)
							pstate->cdbsreh->processed++;

						MemoryContextReset(formatter->fmt_perrow_ctx);

						return tuple;

					case FMT_NEED_MORE_DATA:

						/*
						 * Callee consumed all data in the buffer. Prepare
						 * to read more data into it.
						 */
						pstate->raw_buf_len = 0;
						justifyDatabuf(&formatter->fmt_databuf);
						continue;

					default:
						elog(ERROR, "unsupported formatter notification (%d)",
								formatter->fmt_notification);
						break;
				}
			}
			else
			{
				ErrorIfRejectLimitReached(pstate->cdbsreh);
			}
		}
	}
	if (formatter->fmt_databuf.len > 0)
	{
		/*
		 * The formatter needs more data, but we have reached
		 * EOF. This is an error.
		 */
		ereport(WARNING,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unexpected end of file")));
	}

	/*
	 * if we got here we finished reading all the data.
	 */

	return NULL;
}

/* ----------------------------------------------------------------
*		external_getnext
*
*		Parse a data file and return its rows in heap tuple form
* ----------------------------------------------------------------
*/
static HeapTuple
external_getnext(ForeignScanState *node, dataLakeFdwScanState *sstate)
{
	HeapTuple	tuple;
	DatalakeFileScanDesc scan = sstate->customState.fdw_state->ess_ScanDesc;

	/* Note: no locking manipulations needed */
	FILEDEBUG_1;

	tuple = externalgettup(sstate);


	if (tuple == NULL)
	{
		FILEDEBUG_2;			/* external_getnext returning EOS */

		return NULL;
	}

	/*
	 * if we get here it means we have a new current scan tuple
	 */
	FILEDEBUG_3;				/* external_getnext returning tuple */

	pgstat_count_heap_getnext(scan->fs_rd);

	return tuple;
}

/* ----------------
*		external_rescan  - (re)start a scan of an external file
* ----------------
*/
static void
external_rescan(DatalakeFileScanDesc scan)
{
	/* Close previous scan if it was already open */
	external_stopscan(scan);

	/* The first call to external_getnext will re-open the scan */

	if (!scan->fs_pstate)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("The file parse state of external scan is invalid")));

	/* reset some parse state variables */
	scan->fs_pstate->raw_reached_eof = false;
	scan->fs_pstate->input_reached_eof = false;
	scan->fs_pstate->cur_lineno = 0;
	scan->fs_pstate->cur_attname = NULL;
	scan->fs_pstate->raw_buf_len = 0;
	scan->fs_pstate->input_buf_len = 0;
	scan->fs_pstate->input_buf_index = 0;
}

/* ----------------
*		external_endscan - end a scan
* ----------------
*/
static void
external_endscan(dataLakeFdwScanState *dataLakesstate)
{
	DatalakeFileScanDesc scan = dataLakesstate->customState.fdw_state->ess_ScanDesc;
	char	   *relname = pstrdup(RelationGetRelationName(scan->fs_rd));

	if (dataLakesstate->cstate.cstate_scan != NULL)
	{
		/*
		 * decrement relation reference count and free scan descriptor storage
		 */
		RelationDecrementReferenceCount(scan->fs_rd);
	}

	if (scan->values)
	{
		pfree(scan->values);
		scan->values = NULL;
	}
	if (scan->nulls)
	{
		pfree(scan->nulls);
		scan->nulls = NULL;
	}
	if (scan->in_functions)
	{
		pfree(scan->in_functions);
		scan->in_functions = NULL;
	}
	if (scan->typioparams)
	{
		pfree(scan->typioparams);
		scan->typioparams = NULL;
	}

	if (scan->fs_pstate != NULL && scan->fs_pstate->rowcontext != NULL)
	{
		/*
		 * delete the row context
		 */
		MemoryContextDelete(scan->fs_pstate->rowcontext);
		scan->fs_pstate->rowcontext = NULL;
	}

	/*----
	 * if SREH was active:
	 * 1) QEs: send a libpq message to QD with num of rows rejected in this segment
	 * 2) Free SREH resources
	 *----
	 */
	if (scan->fs_pstate != NULL && scan->fs_pstate->errMode != ALL_OR_NOTHING)
	{
		if (Gp_role == GP_ROLE_EXECUTE)
			SendNumRows(scan->fs_pstate->cdbsreh->rejectcount, 0);

		destroyCdbSreh(scan->fs_pstate->cdbsreh);
	}

	if (scan->fs_formatter)
	{
		/*
		 * TODO: check if this space is automatically freed. if not, then see
		 * what about freeing the user context
		 */
		if (scan->fs_formatter->fmt_databuf.data)
			pfree(scan->fs_formatter->fmt_databuf.data);
		pfree(scan->fs_formatter);
		scan->fs_formatter = NULL;
	}

	/*
	 * free parse state memory
	 */
	if (scan->fs_pstate != NULL)
	{
		if (scan->fs_pstate->attribute_buf.data)
			pfree(scan->fs_pstate->attribute_buf.data);
		if (scan->fs_pstate->line_buf.data)
			pfree(scan->fs_pstate->line_buf.data);
		if (scan->fs_pstate->opts.force_quote_flags)
			pfree(scan->fs_pstate->opts.force_quote_flags);
		if (scan->fs_pstate->opts.force_notnull_flags)
			pfree(scan->fs_pstate->opts.force_notnull_flags);

		pfree(scan->fs_pstate);
		scan->fs_pstate = NULL;
	}

	/*
	 * Close the external file
	 */

	pfree(relname);
}

/* ----------------
*		external_stopscan - closes an external resource without dismantling the scan context
* ----------------
*/
static void
external_stopscan(DatalakeFileScanDesc scan)
{
	/*
	 * Close the external file
	 */
}

/*	----------------
 *		external_getnext_init - prepare ExternalSelectDesc struct before external_getnext
 *	----------------
 */
static ExternalSelectDesc
external_getnext_init(PlanState *state)
{
	ExternalSelectDesc
		desc = (ExternalSelectDesc) palloc0(sizeof(ExternalSelectDescData));
	if (state != NULL)
		desc->projInfo = state->ps_ProjInfo;
	return desc;
}

/*	----------------
 *		external_insert_init
 *	----------------
 */

/*
 * external_insert_init
 *
 * before using external_insert() to insert tuples we need to call
 * this function to initialize our various structures and state..
 */
static DatalakeExternalInsertDesc
external_insert_init(dataLakeFdwScanState *dataLakesstate, List* extOption)
{
	DatalakeExternalInsertDesc extInsertDesc;
	List	   *custom_formatter_params = NIL;
	TupleDesc	tupDesc = NULL;
	/*
	 * allocate and initialize the insert descriptor
	 */
	extInsertDesc = (DatalakeExternalInsertDesc) palloc0(sizeof(DatalakeExternalInsertDescData));

	extInsertDesc->ext_rel = dataLakesstate->rel;
	extInsertDesc->ext_file = NULL;
	extInsertDesc->ext_uri = NULL;
	extInsertDesc->ext_noop = false;
	extInsertDesc->ext_pstate = NULL;

	/*
	 * Allocate and init our structure that keeps track of data parsing state
	 */
	tupDesc = RelationGetDescr(dataLakesstate->rel);
	extInsertDesc->ext_tupDesc = tupDesc;
	/*
	 * Writing to an external table is like COPY TO: we get tuples from the executor,
	 * we format them into the format requested, and write the output to an external
	 * sink.
	 */
	custom_formatter_params = list_copy(extOption);

	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		/*
		 * Custom format: get formatter name and find it in the catalog
		 */
		Oid			procOid;

		procOid = lookupCustomFormatter(&custom_formatter_params, true);

		/* we found our function. set it up for calling  */
		extInsertDesc->ext_custom_formatter_func = palloc(sizeof(FmgrInfo));
		fmgr_info(procOid, extInsertDesc->ext_custom_formatter_func);
		extInsertDesc->ext_custom_formatter_params = custom_formatter_params;

		extInsertDesc->ext_formatter_data = (FormatterData *) palloc0(sizeof(FormatterData));
		extInsertDesc->ext_formatter_data->fmt_perrow_ctx = dataLakesstate->cstate.cstate_modify->rowcontext;
	}
	return extInsertDesc;
}

/*
 * external_insert - format the tuple into text and write to the external source
 *
 * Note the following major differences from heap_insert
 *
 * - wal is always bypassed here.
 * - transaction information is of no interest.
 * - tuples are sent always to the destination (local file or remote target).
 *
 * Like heap_insert(), this function can modify the input tuple!
 */
static void
external_insert(dataLakeFdwScanState *dataLakesstate, TupleTableSlot *slot)
{
	DatalakeExternalInsertDesc extInsertDesc = (DatalakeExternalInsertDesc)dataLakesstate->customState.insert_state;
	TupleDesc	tupDesc = extInsertDesc->ext_tupDesc;
	CopyToStateData *pstate = dataLakesstate->cstate.cstate_modify;
	bool		customFormat = (extInsertDesc->ext_custom_formatter_func != NULL);

	/*
	 * deconstruct the tuple and format it into text
	 */
	if (!customFormat)
	{
		return;
	}
	else
	{
		/* custom format. convert tuple using user formatter */
		Datum		d;
		bytea	   *b;
		LOCAL_FCINFO(fcinfo, 1);
		HeapTuple instup;

		/*
		 * There is some redundancy between FormatterData and
		 * ExternalInsertDesc we may be able to consolidate data structures a
		 * little.
		 */
		FormatterData *formatter = extInsertDesc->ext_formatter_data;

		/* must have been created during insert_init */
		Assert(formatter);

		/* per call formatter prep */
		FunctionCallPrepareFormatter(fcinfo,
									 1,
									 &pstate->opts,
									 extInsertDesc->ext_custom_formatter_func,
									 extInsertDesc->ext_custom_formatter_params,
									 formatter,
									 extInsertDesc->ext_rel,
									 extInsertDesc->ext_tupDesc,
									 pstate->out_functions,
									 NULL,
									 false /* raw_reached_eof */,
									 pstate->need_transcoding,
									 pstate->rowcontext);

		/* Mark the correct record type in the passed tuple */

		/*
		 * get the heap tuple out of the tuple table slot, making sure we have a
		 * writable copy. (the function can scribble on the tuple)
		 */
		instup = ExecCopySlotHeapTuple(slot);

		HeapTupleHeaderSetDatumLength(instup->t_data, instup->t_len);
		HeapTupleHeaderSetTypeId(instup->t_data, tupDesc->tdtypeid);
		HeapTupleHeaderSetTypMod(instup->t_data, tupDesc->tdtypmod);

		fcinfo->args[0].value = HeapTupleGetDatum(instup);
		fcinfo->args[0].isnull = false;

		d = FunctionCallInvoke(fcinfo);
		MemoryContextReset(formatter->fmt_perrow_ctx);

		/* We do not expect a null result */
		if (fcinfo->isnull)
			elog(ERROR, "function %u returned NULL", fcinfo->flinfo->fn_oid);

		b = DatumGetByteaP(d);

		CopyOneCustomRowTo(pstate, b);

		heap_freetuple(instup);
	}

	/* Write the data into the external source */
	writeToProvider(dataLakesstate->provider, pstate->fe_msgbuf->data, pstate->fe_msgbuf->len);

	/* Reset our buffer to start clean next round */
	pstate->fe_msgbuf->len = 0;
	pstate->fe_msgbuf->data[0] = '\0';
}

/*
 * external_insert_finish
 *
 * when done inserting all the data via external_insert() we need to call
 * this function to flush all remaining data in the buffer into the file.
 */
static void
external_insert_finish(DatalakeExternalInsertDesc extInsertDesc)
{
	if (extInsertDesc == NULL)
		return;
	if (extInsertDesc->ext_custom_formatter_func)
	{
		pfree(extInsertDesc->ext_custom_formatter_func);
		extInsertDesc->ext_custom_formatter_func = NULL;
	}

	if (extInsertDesc->ext_formatter_data)
	{
		pfree(extInsertDesc->ext_formatter_data);
		extInsertDesc->ext_formatter_data = NULL;
	}

	if (extInsertDesc->ext_custom_formatter_params)
	{
		list_free_deep(extInsertDesc->ext_custom_formatter_params);
		extInsertDesc->ext_custom_formatter_params = NIL;
	}

	if (extInsertDesc)
	{
		pfree(extInsertDesc);
		extInsertDesc = NULL;
	}
}

/* ----------------------------------------------------------------
 *				   ForeignScan to externalScan interface functions
 * ----------------------------------------------------------------
 */

extern void
datalake_to_exttable_BeginForeignScan(ForeignScanState *node, int eflags,
									  void *datalakeState,
									  bool isMasterOnly,
									  List *uriList,
									  List *extOptions)
{
	DatalakeFileScanDesc currentScanDesc;
	ExternalSelectDesc externalSelectDesc;
	datalake_exttable_fdw_state *fdw_state;
	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)datalakeState;
	if (!sstate->rel)
		elog(ERROR, "external table scan without a current relation");

	currentScanDesc = external_beginscan(sstate, isMasterOnly, uriList, extOptions);
	externalSelectDesc = external_getnext_init(&node->ss.ps);
	if (gp_external_enable_filter_pushdown)
		externalSelectDesc->filter_quals = node->ss.ps.plan->qual;

	fdw_state = palloc(sizeof(datalake_exttable_fdw_state));
	fdw_state->ess_ScanDesc = currentScanDesc;
	fdw_state->externalSelectDesc = externalSelectDesc;

	sstate->customState.fdw_state = fdw_state;
}

static bool
ExternalConstraintCheck(TupleTableSlot *slot, DatalakeFileScanDesc scandesc, EState *estate)
{
	Relation		rel = scandesc->fs_rd;
	TupleConstr		*constr = rel->rd_att->constr;
	ConstrCheck		*check = constr->check;
	uint16			ncheck = constr->num_check;
	ExprContext		*econtext = NULL;
	MemoryContext	oldContext = NULL;

	/* No constraints */
	if (ncheck == 0)
	{
		return true;
	}

	/*
	 * Build expression nodetrees for rel's constraint expressions.
	 * Keep them in the per-query memory context so they'll survive throughout the query.
	 */
	if (scandesc->fs_constraintExprs == NULL)
	{
		oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
		scandesc->fs_constraintExprs =
			(ExprState **) palloc(ncheck * sizeof(ExprState *));
		for (int i = 0; i < ncheck; i++)
		{
			/* ExecQual wants implicit-AND form */
			List	   *qual = make_ands_implicit(stringToNode(check[i].ccbin));

			scandesc->fs_constraintExprs[i] =
				ExecPrepareExpr((Expr *) qual, estate);
		}
		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * We will use the EState's per-tuple context for evaluating constraint
	 * expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* And evaluate the constraints */
	for (int i = 0; i < ncheck; i++)
	{
		ExprState *qual = scandesc->fs_constraintExprs[i];

		if (!ExecCheck(qual, econtext))
			return false;
	}

	return true;
}

/*
 * Check whether a row matches the partition constraint.
 */
static bool
ExternalPartitionCheck(TupleTableSlot *slot, DatalakeFileScanDesc scandesc, EState *estate)
{
	Relation	rel = scandesc->fs_rd;
	ExprContext *econtext;

	/*
	 * Build expression nodetrees for rel's constraint expressions.
	 * Keep them in the per-query memory context so they'll survive throughout the query.
	 */
	if (scandesc->fs_partitionCheckExpr == NULL)
	{
		List	   *partition_check;
		MemoryContext	oldContext;

		oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

		partition_check = RelationGetPartitionQual(rel);

		scandesc->fs_partitionCheckExpr = ExecPrepareCheck(partition_check, estate);

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * We will use the EState's per-tuple context for evaluating constraint
	 * expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	return ExecCheck(scandesc->fs_partitionCheckExpr, econtext);
}

extern TupleTableSlot *
datalake_to_exttable_IterateForeignScan(ForeignScanState *node)
{
	EState	   *estate = node->ss.ps.state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HeapTuple	tuple;
	MemoryContext oldcxt;
	dataLakeFdwScanState *sstate = (dataLakeFdwScanState*)node->fdw_state;
	datalake_exttable_fdw_state *fdw_state = sstate->customState.fdw_state;
	/*
	 * XXX: ForeignNext() calls us in a short-lived memory context, which
	 * seems like a good idea. However external_getnext() allocates some stuff
	 * that needs to live longer. At least on the first call. The old
	 * ExternalScan plan node used to run in a long-lived context, which seems
	 * a bit dangerous to me, but I guess that external_getnext() and all the
	 * external protocols are careful not to leak memory.
	 */
	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

	for (;;)
	{
		tuple = external_getnext(node, sstate);
		if (!tuple)
		{
			ExecClearTuple(slot);
			break;
		}

		ExecStoreHeapTuple(tuple, slot, true);

		/*
		 * If this is a partition in a partitioned table, check each row against
		 * the partition qual, and skip rows that don't belong in this partition.
		 * Foreign tables are not required to enforce that, but that has been
		 * the historical behavior for external tables.
		 */
		if (fdw_state->ess_ScanDesc->fs_isPartition &&
			!ExternalPartitionCheck(slot, fdw_state->ess_ScanDesc, estate))
			continue;

		/*
		 * Similarly, check CHECK constraints and skip rows that don't satisfy
		 * them. Foreign tables are not required required to enforce CHECK
		 * constraints either, they are merely hints to the optimizer, but it
		 * is allowed. (In GPDB 6 and below, partition quals were stored in the
		 * catalogs as CHECK constraints, so this was needed to check the
		 * partition quals.)
		 */
		if (fdw_state->ess_ScanDesc->fs_hasConstraints &&
			!ExternalConstraintCheck(slot, fdw_state->ess_ScanDesc, estate))
			continue;

		break;
	}
	MemoryContextSwitchTo(oldcxt);

	return slot;
}

extern void
datalake_to_exttable_ReScanForeignScan(ForeignScanState *node)
{
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;
	external_rescan(dataLakesstate->customState.fdw_state->ess_ScanDesc);
}

extern void
datalake_to_exttable_EndForeignScan(ForeignScanState *node)
{
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;
	datalake_exttable_fdw_state *fdw_state = dataLakesstate->customState.fdw_state;

	if (node->ss.ps.squelched)
		external_stopscan(fdw_state->ess_ScanDesc);

	/*
	 * report Sreh results if external web table execute on coordinator with reject limit.
	 * if external web table execute on segment, these messages are printed
	 * in cdbdisp_sumRejectedRows()
	*/
	if (Gp_role == GP_ROLE_DISPATCH) {
		CopyFromState cstate = fdw_state->ess_ScanDesc->fs_pstate;
		if (cstate && cstate->cdbsreh)
		{
			CdbSreh	 *cdbsreh = cstate->cdbsreh;
			uint64	total_rejected_from_qd = cdbsreh->rejectcount;
			if (total_rejected_from_qd > 0)
				ReportSrehResults(cdbsreh, total_rejected_from_qd);
		}
	}
	external_endscan(dataLakesstate);
}

/* ModifyTable support */
extern void
datalake_to_exttable_BeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							List* extOption,
							int subplan_index,
							int eflags)
{
	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	dataLakeFdwScanState *dataLakesstate = rinfo->ri_FdwState;
	dataLakesstate->customState.insert_state = external_insert_init(dataLakesstate, extOption);
	rinfo->ri_FdwState = (void*)dataLakesstate;
}

extern TupleTableSlot *
datalake_to_exttable_ExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	dataLakeFdwScanState *dataLakesstate = rinfo->ri_FdwState;
	(void) external_insert(dataLakesstate, slot);
	return slot;
}

extern void
datalake_to_exttable_EndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
	dataLakeFdwScanState *dataLakesstate = rinfo->ri_FdwState;

	if ((dataLakesstate == NULL) ||
		(dataLakesstate->customState.insert_state == NULL))
	{
		return;
	}
	external_insert_finish(dataLakesstate->customState.insert_state);
}

