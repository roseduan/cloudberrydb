/*-------------------------------------------------------------------------
 *
 * pg_iceberg_am_handler.c
 * 		Implementation of Iceberg Table Access Method handler and DML 
 *      state management.
 *
 * This file contains the primary entry point for the Iceberg AM and 
 * manages the lifecycle of DML operations.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_am_handler.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"

#include "include/pg_iceberg_am.h"
#include "include/pg_iceberg_am_handler.h"
#include "include/pg_iceberg_catalog.h"
#include "include/pg_iceberg_metadata.h"
#include "include/pg_iceberg_metadata_tracker.h"

#include "src/datalake_def.h"
#include "src/provider/iceberg/iceberg_file_index.h"
#include "../iceberg_volume_fdw/iceberg_volume_fdw.h"
#include "src/common/fdwFunction.h"

/* Global DML states management */
static IcebergDMLStates icebergDMLStates;

static void reset_state_cb(void *arg);
/* AM Definition */
static const TableAmRoutine iceberg_am_methods;

/* --- DML State Management Functions --- */

static void
init_iceberg_dml_states(void)
{
	HASHCTL hash_ctl;

	if (!icebergDMLStates.state_table)
	{
		Assert(icebergDMLStates.stateCxt == NULL);
		icebergDMLStates.stateCxt = AllocSetContextCreate(
												CurrentMemoryContext,
												"Iceberg DML State Context",
												ALLOCSET_SMALL_SIZES);

		icebergDMLStates.cb.func = reset_state_cb;
		icebergDMLStates.cb.arg = NULL;
		MemoryContextRegisterResetCallback(icebergDMLStates.stateCxt,
										   &icebergDMLStates.cb);

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(IcebergDMLState);
		hash_ctl.hcxt = icebergDMLStates.stateCxt;
		icebergDMLStates.state_table =
			hash_create("Iceberg DML state", 128, &hash_ctl,
						HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
	}
}

static void
init_dml_state(const Oid relationOid)
{
	IcebergDMLState *state;
	bool				found;

	Assert(icebergDMLStates.state_table);

	state = (IcebergDMLState *) hash_search(icebergDMLStates.state_table,
											&relationOid,
											HASH_ENTER,
											&found);
	state->insertDesc = NULL;
	state->updateDesc = NULL;
	state->deleteDesc = NULL;

	Assert(!found);

	icebergDMLStates.last_used_state = state;
}

static IcebergDMLState *
find_dml_state(const Oid relationOid)
{
	IcebergDMLState *state;
	Assert(icebergDMLStates.state_table);

	if (icebergDMLStates.last_used_state &&
			icebergDMLStates.last_used_state->relationOid == relationOid)
		return icebergDMLStates.last_used_state;

	state = (IcebergDMLState *) hash_search(icebergDMLStates.state_table,
											&relationOid,
											HASH_FIND,
											NULL);

	Assert(state);

	icebergDMLStates.last_used_state = state;
	return state;
}

static void
remove_dml_state(const Oid relationOid)
{
	IcebergDMLState *state;
	Assert(icebergDMLStates.state_table);

	state = (IcebergDMLState *) hash_search(icebergDMLStates.state_table,
											&relationOid,
											HASH_REMOVE,
											NULL);

	if (!state)
		return;

	if (icebergDMLStates.last_used_state &&
			icebergDMLStates.last_used_state->relationOid == relationOid)
		icebergDMLStates.last_used_state = NULL;
}

static void
reset_state_cb(void *arg)
{
	icebergDMLStates.state_table = NULL;
	icebergDMLStates.last_used_state = NULL;
	icebergDMLStates.stateCxt = NULL;
}

static FdwRoutine *
get_volume_fdw_routine(void)
{
	ForeignDataWrapper *fdw;

	fdw = GetForeignDataWrapperByName("iceberg_volume_fdw", false);
	return GetFdwRoutine(fdw->fdwhandler);
}

static IcebergModifyDesc *
iceberg_modify_init(Relation rel, IcebergDMLState *state, CmdType operation,
					int begin_eflags)
{
	FdwRoutine *fdwRoutine;
	IcebergModifyDesc *modifyDesc;
	ResultRelInfo *resultRelInfo;
	icebergVolumeScanState *fdwState;
	IcebergTableInfo *table_info;
	ModifyTableState *mtstate = makeNode(ModifyTableState);

	modifyDesc = (IcebergModifyDesc *) palloc0(sizeof(IcebergModifyDesc));

	resultRelInfo = (ResultRelInfo *) palloc0(sizeof(ResultRelInfo));
	resultRelInfo->type = T_ResultRelInfo;
	resultRelInfo->ri_RelationDesc = rel;

	table_info = pg_iceberg_get_table_info(RelationGetRelid(rel));
	fdwState = (icebergVolumeScanState *) palloc0(sizeof(icebergVolumeScanState));

	fdwState->iceTable.volumn_server_name = table_info->volume_server_name;
	fdwState->iceTable.volumn_name = table_info->volume_name;
	fdwState->iceTable.icebergNamespace = get_namespace_name(rel->rd_rel->relnamespace);
	fdwState->iceTable.tableName = pstrdup(RelationGetRelationName(rel));

	{
		char	   *location;

		location = pg_iceberg_resolve_modify_location(rel, operation);
		if (location == NULL || location[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("failed to resolve iceberg table location for relation \"%s\"",
							RelationGetRelationName(rel))));
		fdwState->iceTable.location = location;
	}

	resultRelInfo->ri_FdwState = (void *) fdwState;

	modifyDesc->resultRelInfo = resultRelInfo;
	modifyDesc->tableInfo = table_info;
	modifyDesc->operation = operation;

	fdwRoutine = get_volume_fdw_routine();
	modifyDesc->fdwroutine = fdwRoutine;

	mtstate->operation = operation;

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Plan *plan;
		PlanState *ps;
		TargetEntry *tle;
		TupleDesc planTupDesc;

		plan = makeNode(Plan);
		tle = makeTargetEntry((Expr *) makeVar(1, 1, TIDOID, -1, InvalidOid, 0),
							  1, "ctid", true);
		plan->targetlist = list_make1(tle);

		ps = makeNode(PlanState);
		ps->plan = plan;

		outerPlanState(mtstate) = ps;
		mtstate->mt_nrels = 1;

		planTupDesc = CreateTemplateTupleDesc(1);
		TupleDescInitEntry(planTupDesc, (AttrNumber) 1, "ctid", TIDOID, -1, 0);
		modifyDesc->planSlot = MakeSingleTupleTableSlot(planTupDesc, &TTSOpsVirtual);
	}

	if (Gp_role == GP_ROLE_DISPATCH)
		pg_iceberg_tracker_register_table(RelationGetRelid(rel),
										  rel->rd_rel->relnamespace,
										  rel->rd_rel->relfilenode);

	fdwRoutine->BeginForeignModify(mtstate,
								   modifyDesc->resultRelInfo,
								   NULL,
								   0,
								   begin_eflags);

	/*
	 * Issue #333: populate the global file_id -> file_path map from the
	 * complete fragment list dispatched by the planner_hook
	 * (iceberg_planner_hook -> stash_iceberg_modify_fragments).  Without
	 * this, writer-only QEs whose slice has no Iceberg ForeignScan after a
	 * Redistribute Motion would hit "Failed to get file path for file ID 0
	 * in Iceberg update" because their lazy per-reader populator in
	 * row_reader.c never runs.
	 */
	if ((operation == CMD_UPDATE || operation == CMD_DELETE) &&
		datalake_iceberg_file_index_map != NULL)
	{
		List *allFragments = pg_iceberg_take_modify_fragments(RelationGetRelid(rel));

		if (allFragments != NIL)
			icebergFileIndexMapPopulateFromAllFragments(
				datalake_iceberg_file_index_map, allFragments);
	}

	return modifyDesc;
}

static void
iceberg_modify_cleanup(IcebergModifyDesc *modifyDesc)
{
	if (modifyDesc == NULL)
		return;

	if (modifyDesc->resultRelInfo)
		pfree(modifyDesc->resultRelInfo);

	if (modifyDesc->planSlot)
		ExecDropSingleTupleTableSlot(modifyDesc->planSlot);

	pfree(modifyDesc);
}

static void
iceberg_modify_finish(IcebergModifyDesc *modifyDesc)
{
	const char		   *data_locations;
	TrackedDataFile	   *new_data_files = NULL;
	int					num_data_files = 0;
	TrackedDataFile	   *new_delete_files = NULL;
	int					num_delete_files = 0;
	FdwRoutine		   *fdwRoutine = modifyDesc->fdwroutine;
	Relation			rel = modifyDesc->resultRelInfo->ri_RelationDesc;

	fdwRoutine->EndForeignModify(NULL, modifyDesc->resultRelInfo);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		data_locations = (const char *) modifyDesc->resultRelInfo->ri_FdwState;
		if (data_locations == NULL)
			goto finish;

		pg_iceberg_parse_data_locations(data_locations,
										&new_data_files, &num_data_files,
										&new_delete_files, &num_delete_files);

		/*
		 * Hand the parsed files to the tracker.  Internally this will:
		 *   1. Read the latest global metadata from catalog
		 *   2. Rebase ALL accumulated files (prior stmts + this stmt)
		 *   3. Call the agent to generate a new intermediate metadata file
		 *   4. Update tracker state
		 *
		 * The actual catalog update (CAS) is deferred to PRE_COMMIT.
		 */
		pg_iceberg_tracker_apply_updates_with_rebase(
			RelationGetRelid(rel),
			new_data_files, num_data_files,
			new_delete_files, num_delete_files);
	}

finish:
	iceberg_modify_cleanup(modifyDesc);
}

IcebergModifyDesc *
pg_iceberg_modify_init_for_vacuum(Relation rel, CmdType operation)
{
	return iceberg_modify_init(rel, NULL, operation,
							   DATALAKE_FDW_EFLAG_COLLECT_QE_METADATA);
}

char *
pg_iceberg_modify_finish_for_vacuum(IcebergModifyDesc *modifyDesc)
{
	FdwRoutine *fdwRoutine;
	char *data_locations = NULL;

	if (modifyDesc == NULL)
		return NULL;

	fdwRoutine = modifyDesc->fdwroutine;
	fdwRoutine->EndForeignModify(NULL, modifyDesc->resultRelInfo);

	if (modifyDesc->resultRelInfo &&
		modifyDesc->resultRelInfo->ri_FdwState != NULL)
	{
		data_locations = pstrdup((char *) modifyDesc->resultRelInfo->ri_FdwState);
	}

	iceberg_modify_cleanup(modifyDesc);

	return data_locations;
}

IcebergModifyDesc *
get_or_create_modify_descriptor(Relation rel, CmdType operation)
{
	IcebergDMLState *state;
	IcebergModifyDesc **desc_ptr;

	state = find_dml_state(RelationGetRelid(rel));

	switch (operation)
	{
		case CMD_INSERT:
			desc_ptr = &state->insertDesc;
			break;
		case CMD_UPDATE:
			desc_ptr = &state->updateDesc;
			break;
		case CMD_DELETE:
			desc_ptr = &state->deleteDesc;
			break;
		default:
			elog(ERROR, "unrecognized operation: %d", (int) operation);
			return NULL;
	}

	if (*desc_ptr == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(icebergDMLStates.stateCxt);
		*desc_ptr = iceberg_modify_init(rel, state, operation, 0);
		MemoryContextSwitchTo(oldcxt);
	}

	return *desc_ptr;
}

void
pg_iceberg_dml_init(Relation rel, CmdType operation)
{
	init_iceberg_dml_states();
	init_dml_state(RelationGetRelid(rel));

	if (operation == CMD_INSERT ||
		operation == CMD_UPDATE ||
		operation == CMD_DELETE)
		get_or_create_modify_descriptor(rel, operation);
}

void
pg_iceberg_dml_fini(Relation rel, CmdType operation)
{
	IcebergDMLState *state;
	Oid relationOid = RelationGetRelid(rel);

	Assert(icebergDMLStates.state_table);

	state = (IcebergDMLState *)hash_search(icebergDMLStates.state_table,
										   &relationOid,
										   HASH_FIND,
										   NULL);

	if (!state)
		return;

	if (state->insertDesc != NULL)
	{
		iceberg_modify_finish(state->insertDesc);
		state->insertDesc = NULL;
	}

	if (state->updateDesc != NULL)
	{
		iceberg_modify_finish(state->updateDesc);
		state->updateDesc = NULL;
	}

	if (state->deleteDesc != NULL)
	{
		iceberg_modify_finish(state->deleteDesc);
		state->deleteDesc = NULL;
	}

	remove_dml_state(relationOid);
}

/* --- Public API Implementation --- */

bool
is_iceberg_rel(Relation rel)
{
	Assert(rel != NULL);
	return (rel->rd_tableam == (TableAmRoutine *) &iceberg_am_methods);
}

void
pg_iceberg_ext_dml_init(Relation rel, CmdType operation)
{
	pg_iceberg_dml_init(rel, operation);
}

void
pg_iceberg_ext_dml_fini(Relation rel, CmdType operation)
{
	pg_iceberg_dml_fini(rel, operation);
}

PG_FUNCTION_INFO_V1(pg_iceberg_tableam_handler);
Datum
pg_iceberg_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&iceberg_am_methods);
}

/* --- AM Callback Stubs --- */

static const TupleTableSlotOps *
iceberg_slot_callbacks(Relation rel)
{
	return &TTSOpsVirtual;
}

static TableScanDesc
iceberg_beginscan(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
				  ParallelTableScanDesc pscan, uint32 flags)
{
	return pg_iceberg_beginscan(rel, snapshot, nkeys, key, pscan, flags);
}

static void
iceberg_endscan(TableScanDesc scan)
{
	pg_iceberg_endscan(scan);
}

static void
iceberg_rescan(TableScanDesc scan, struct ScanKeyData *key, bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	pg_iceberg_rescan(scan, key, set_params, allow_strat, allow_sync, allow_pagemode);
}

static bool
iceberg_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	return pg_iceberg_getnextslot(scan, direction, slot);
}

static TableScanDesc
iceberg_scan_begin_extractcolumns(Relation rel, Snapshot snapshot, int nkeys, struct ScanKeyData *key,
								  ParallelTableScanDesc parallel_scan, struct PlanState *ps, uint32 flags)
{
	return pg_iceberg_scan_begin_extractcolumns(rel, snapshot, nkeys, key, parallel_scan, ps, flags);
}

static void
iceberg_scan_set_tidrange(TableScanDesc scan, ItemPointer mintid, ItemPointer maxtid)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not supported")));
}

static bool
iceberg_scan_getnextslot_tidrange(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not supported")));
	return false;
}

static uint32
iceberg_scan_flags(Relation rel)
{
	return 0;
}

static void
iceberg_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
					 struct BulkInsertStateData *bistate)
{
	IcebergModifyDesc *insertDesc = get_or_create_modify_descriptor(rel, CMD_INSERT);
	pg_iceberg_tuple_insert(insertDesc, slot, cid, options, bistate);
}

static TM_Result
iceberg_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, Snapshot snapshot,
					 Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	IcebergModifyDesc *deleteDesc = get_or_create_modify_descriptor(rel, CMD_DELETE);
	return pg_iceberg_tuple_delete(deleteDesc, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart);
}

static TM_Result
iceberg_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
					 Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
					 LockTupleMode *lockmode, bool *update_indexes)
{
	IcebergModifyDesc *updateDesc = get_or_create_modify_descriptor(rel, CMD_UPDATE);
	return pg_iceberg_tuple_update(updateDesc, otid, slot, cid, snapshot, crosscheck, wait, tmfd, lockmode, update_indexes);
}

static Size
iceberg_parallelscan_estimate(Relation rel)
{
	return 0;
}

static Size
iceberg_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	return 0;
}

static void
iceberg_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
}

static struct IndexFetchTableData *
iceberg_index_fetch_begin(Relation rel)
{
	return NULL;
}

static void
iceberg_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
iceberg_index_fetch_end(struct IndexFetchTableData *data)
{
}

static bool
iceberg_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	return false;
}

static bool
iceberg_tuple_fetch_row_version(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	return false;
}

static bool
iceberg_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static void
iceberg_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
}

static bool
iceberg_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	return false;
}

static TransactionId
iceberg_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

static void
iceberg_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, int options, struct BulkInsertStateData *bistate, uint32 specToken)
{
}

static void
iceberg_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded)
{
}

static void
iceberg_multi_insert(Relation rel, TupleTableSlot **slots, int nslots, CommandId cid, int options, struct BulkInsertStateData *bistate)
{
}

static TM_Result
iceberg_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	return TM_Ok;
}

static void
iceberg_relation_set_new_filenode(Relation rel, const RelFileNode *newrnode, char persistence, TransactionId *freezeXid, MultiXactId *minmulti)
{
}

static void
iceberg_relation_nontransactional_truncate(Relation rel)
{
}

static void
iceberg_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
}

static void
iceberg_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap, Relation OldIndex, bool use_sort, TransactionId OldestXmin, TransactionId *xid_cutoff, MultiXactId *multi_cutoff, double *num_tuples, double *tups_vacuumed, double *tups_recently_dead)
{
}

static void
iceberg_relation_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy)
{
	pg_iceberg_relation_vacuum(rel, params, bstrategy);
}

static int
iceberg_relation_acquire_sample_rows(Relation rel, int elevel,
									 HeapTuple *rows, int targrows,
									 double *totalrows, double *totaldeadrows)
{
	return pg_iceberg_acquire_sample_rows(rel, elevel, rows, targrows,
										  totalrows, totaldeadrows);
}

static bool
iceberg_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno, BufferAccessStrategy bstrategy)
{
	/*
	 * Iceberg uses relation_acquire_sample_rows() for ANALYZE.
	 * The block-based analyze API is not applicable.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("API not supported for iceberg relations")));

	return false;
}

static bool
iceberg_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows, double *deadrows, TupleTableSlot *slot)
{
	/*
	 * Iceberg uses relation_acquire_sample_rows() for ANALYZE.
	 * The block-based analyze API is not applicable.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("API not supported for iceberg relations")));

	return false;
}

static double
iceberg_index_build_range_scan(Relation heapRelation, Relation indexRelation, struct IndexInfo *indexInfo, bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno, BlockNumber numblocks, IndexBuildCallback callback, void *callback_state, TableScanDesc scan)
{
	return 0;
}

static void
iceberg_index_validate_scan(Relation heapRelation, Relation indexRelation, struct IndexInfo *indexInfo, Snapshot snapshot, struct ValidateIndexState *state)
{
}

static uint64
iceberg_relation_size(Relation rel, ForkNumber forkNumber)
{
	return pg_iceberg_relation_size(rel, forkNumber);
}

static BlockSequence *
iceberg_relation_get_block_sequences(Relation rel, int *numSequences)
{
	return NULL;
}

static void
iceberg_relation_get_block_sequence(Relation rel, BlockNumber blkNum, BlockSequence *sequence)
{
}

static bool
iceberg_relation_needs_toast_table(Relation rel)
{
	return false;
}

static bool
iceberg_scan_sample_next_block(TableScanDesc scan, struct SampleScanState *scanstate)
{
	return false;
}

static bool
iceberg_scan_sample_next_tuple(TableScanDesc scan, struct SampleScanState *scanstate, TupleTableSlot *slot)
{
	return false;
}

static void
iceberg_estimate_rel_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
						  double *tuples, double *allvisfrac)
{
	pg_iceberg_estimate_rel_size(rel, attr_widths, pages, tuples, allvisfrac);
}

static const TableAmRoutine iceberg_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = iceberg_slot_callbacks,

	.scan_begin = iceberg_beginscan,
	.scan_end = iceberg_endscan,
	.scan_rescan = iceberg_rescan,
	.scan_getnextslot = iceberg_getnextslot,
	.scan_begin_extractcolumns = iceberg_scan_begin_extractcolumns,
	.scan_set_tidrange = iceberg_scan_set_tidrange,
	.scan_getnextslot_tidrange = iceberg_scan_getnextslot_tidrange,
	.scan_flags = iceberg_scan_flags,

	.parallelscan_estimate = iceberg_parallelscan_estimate,
	.parallelscan_initialize = iceberg_parallelscan_initialize,
	.parallelscan_reinitialize = iceberg_parallelscan_reinitialize,

	.index_fetch_begin = iceberg_index_fetch_begin,
	.index_fetch_reset = iceberg_index_fetch_reset,
	.index_fetch_end = iceberg_index_fetch_end,
	.index_fetch_tuple = iceberg_index_fetch_tuple,

	.tuple_fetch_row_version = iceberg_tuple_fetch_row_version,
	.tuple_tid_valid = iceberg_tuple_tid_valid,
	.tuple_get_latest_tid = iceberg_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = iceberg_tuple_satisfies_snapshot,
	.index_delete_tuples = iceberg_index_delete_tuples,

	.tuple_insert = iceberg_tuple_insert,
	.tuple_insert_speculative = iceberg_tuple_insert_speculative,
	.tuple_complete_speculative = iceberg_tuple_complete_speculative,
	.multi_insert = iceberg_multi_insert,
	.tuple_delete = iceberg_tuple_delete,
	.tuple_update = iceberg_tuple_update,
	.tuple_lock = iceberg_tuple_lock,

	.relation_set_new_filenode = iceberg_relation_set_new_filenode,
	.relation_nontransactional_truncate = iceberg_relation_nontransactional_truncate,
	.relation_copy_data = iceberg_relation_copy_data,
	.relation_copy_for_cluster = iceberg_relation_copy_for_cluster,
	.relation_vacuum = iceberg_relation_vacuum,
	.scan_analyze_next_block = iceberg_scan_analyze_next_block,
	.scan_analyze_next_tuple = iceberg_scan_analyze_next_tuple,
	.relation_acquire_sample_rows = iceberg_relation_acquire_sample_rows,
	.index_build_range_scan = iceberg_index_build_range_scan,
	.index_validate_scan = iceberg_index_validate_scan,

	.relation_size = iceberg_relation_size,
	.relation_get_block_sequences = iceberg_relation_get_block_sequences,
	.relation_get_block_sequence = iceberg_relation_get_block_sequence,
	.relation_needs_toast_table = iceberg_relation_needs_toast_table,

	.relation_estimate_size = iceberg_estimate_rel_size,
	.dml_init = pg_iceberg_ext_dml_init,
	.dml_fini = pg_iceberg_ext_dml_fini,

	/* optional, but one callback implies presence of the other */
	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = iceberg_scan_sample_next_block,
	.scan_sample_next_tuple = iceberg_scan_sample_next_tuple,
};
