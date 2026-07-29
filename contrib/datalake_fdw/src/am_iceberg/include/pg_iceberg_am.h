/*-------------------------------------------------------------------------
 *
 * pg_iceberg_am.h
 *
 *
 * IDENTIFICATION
 *	  contrib/pg_iceberg/include/pg_iceberg_am.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PG_ICEBERG_AM_H__
#define __PG_ICEBERG_AM_H__

#include "access/relscan.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "foreign/fdwapi.h"
#include "utils/hsearch.h"
#include "pg_iceberg_catalog.h"

struct VacuumParams;

typedef struct IcebergScanDescData
{
	TableScanDescData rs_base;
	ExprContext		*pushdown_econtext;
	ExprState		*pushdown_qual;

	struct ForeignScanState *scanState;
} IcebergScanDescData;

typedef IcebergScanDescData *IcebergScanDesc;

/* DML related structures */
typedef struct IcebergModifyDesc
{
	ResultRelInfo *resultRelInfo;
	IcebergTableInfo *tableInfo;
	IcebergMetadataInfo *metadataInfo;
	FdwRoutine *fdwroutine;
	TupleTableSlot *planSlot;
	CmdType operation;
} IcebergModifyDesc;

typedef struct IcebergDMLState
{
	Oid relationOid;
	IcebergModifyDesc *insertDesc;
	IcebergModifyDesc *updateDesc;
	IcebergModifyDesc *deleteDesc;
} IcebergDMLState;

typedef struct IcebergDMLStates
{
	IcebergDMLState		   *last_used_state;
	HTAB				   *state_table;

	MemoryContext			stateCxt;
	MemoryContextCallback	cb;
} IcebergDMLStates;

/* Table scan callbacks */
extern TableScanDesc pg_iceberg_beginscan(Relation rel, Snapshot snapshot,
										  int nkeys, struct ScanKeyData *key,
										  ParallelTableScanDesc pscan, uint32 flags);
extern void pg_iceberg_endscan(TableScanDesc scan);
extern void pg_iceberg_rescan(TableScanDesc scan, struct ScanKeyData *key,
							  bool set_params, bool allow_strat,
							  bool allow_sync, bool allow_pagemode);
extern bool pg_iceberg_getnextslot(TableScanDesc scan, ScanDirection direction,
								   TupleTableSlot *slot);
extern TableScanDesc pg_iceberg_scan_begin_extractcolumns(Relation rel,
														  Snapshot snapshot,
														  int nkeys,
														  struct ScanKeyData *key,
														  ParallelTableScanDesc parallel_scan,
														  struct PlanState *ps,
														  uint32 flags);

/* Planner related functions */
extern void pg_iceberg_estimate_rel_size(Relation rel, int32 *attr_widths,
										 BlockNumber *pages,
										 double *tuples, double *allvisfrac);
extern uint64 pg_iceberg_relation_size(Relation rel, ForkNumber forkNumber);

/*
 * Persist Iceberg catalog metadata (recordCount / bytesInDataFile) into
 * pg_class.reltuples / pg_class.relpages.  Required because ORCA reads
 * rel->rd_rel->reltuples directly and does not call the tableam
 * relation_estimate_size callback.  Must run on the QD only.
 */
extern BlockNumber pg_iceberg_refresh_pg_class_stats(Relation rel);

/*
 * Re-apply a metadata-derived relpages after standard ANALYZE sampling
 * overwrote it with the dummy local heap's block count (the sampled
 * reltuples is kept).
 */
extern void pg_iceberg_restore_relpages(Oid relid, BlockNumber pages);

extern void pg_iceberg_snap_high_ndv_stats(Oid relid, int used_target,
											int base_target);

extern List *pg_iceberg_build_scan_am_private(Relation rel, struct PlanState *ps,
											   int random_segment_num,
											   int64 snapshot_id);
extern List *pg_iceberg_materialize_am_private(List *am_private);
extern char *pg_iceberg_list_data_fragments_json(Relation rel, int64 snapshot_id);
extern char *pg_iceberg_resolve_scan_metadata_location(Oid relid, bool *is_internal_out);

/*
 * Time travel: open a scan driven by an explicit tuple descriptor and a
 * fragments JSON payload resolved on the QD (iceberg_snapshot_scan SRF).
 * Drive with the returned state's fdwroutine->IterateForeignScan and close
 * with fdwroutine->EndForeignScan.
 */
extern struct ForeignScanState *pg_iceberg_snapshot_beginscan(
	Relation rel, TupleDesc scan_tupdesc, char *fragments_json,
	const int *snapshot_field_ids, int n_snapshot_field_ids);

/*
 * Planner-hook callback (QD): walk a finished plan tree and inject the
 * QD-resolved fragment list into every iceberg_snapshot_scan FunctionScan.
 */
extern void iceberg_tt_inject_fragments(struct Plan *plan);

/*
 * Planner-hook helper (QD): if `plan` is a FunctionScan on a schema-changed
 * iceberg_snapshot_scan() call, return an equivalent Iceberg CustomScan;
 * otherwise return `plan` unchanged.
 */
extern struct Plan *iceberg_tt_try_make_custom_scan(struct Plan *plan, List *rtable);

/*
 * Main path: install the post_parse_analyze_hook that rewrites
 * iceberg_snapshot_scan() FROM-calls into relation scans, and recover the
 * bound snapshot id from a rewritten relation's alias at execution.
 */
extern void iceberg_tt_install_hooks(void);
extern bool iceberg_tt_parse_alias(const char *aliasname, int64 *snapshot_id);

/*
 * One planner invocation's registry of per-snapshot base-table cardinalities
 * (issue #413).  Stack-allocated by the planner hook and chained through
 * `outer`, so nested planning nests and an error unwinding through the hook
 * cannot leave a stale frame active.  The entries are private to
 * pg_iceberg_time_travel.c.
 */
typedef struct IcebergTTStatsFrame
{
	struct IcebergTTStatsFrame *outer;
	List	   *entries;
} IcebergTTStatsFrame;

extern void iceberg_tt_stats_begin(IcebergTTStatsFrame *frame,
								   struct Query *parse);
extern void iceberg_tt_stats_end(IcebergTTStatsFrame *frame);

/*
 * Guard (QD): if a time-travel snapshot reached the CustomScan through a
 * hand-written relation alias (bypassing the rewrite's schema gate), reject a
 * snapshot whose schema differs from the current schema before it decodes
 * historical files under the wrong tuple descriptor.
 */
extern void iceberg_tt_check_alias_schema(Relation rel, int64 snapshot_id);

/*
 * Process-local cache of modify-time fragment lists, keyed by relid.
 * See pg_iceberg_am.c for the dispatch flow (issue #333).
 */
extern void pg_iceberg_stash_modify_fragments(Oid relid, List *fragments);
extern List *pg_iceberg_take_modify_fragments(Oid relid);

/*
 * Process-local cache of ANALYZE-time fragment lists, keyed by relid.
 * Filled on the QE by the PgIcebergAnalyzeDispatch ExtensibleNode handler,
 * consumed by pg_iceberg_acquire_sample_rows (issue #352).
 */
extern void pg_iceberg_stash_analyze_fragments(Oid relid, const char *fragments);
extern char *pg_iceberg_take_analyze_fragments(Oid relid);
extern void pg_iceberg_reset_analyze_fragments(void);

extern char *pg_iceberg_resolve_modify_location(Relation rel, CmdType operation);
extern int pg_iceberg_acquire_sample_rows(Relation relation, int elevel,
										  HeapTuple *rows, int targrows,
										  double *totalrows, double *totaldeadrows);

/* DML related functions */
extern void pg_iceberg_dml_init(Relation rel, CmdType operation);
extern void pg_iceberg_dml_fini(Relation rel, CmdType operation);

/* Internal DML execution functions (called by handler) */
extern void pg_iceberg_tuple_insert(IcebergModifyDesc *insertDesc, TupleTableSlot *slot, CommandId cid, int options,
									struct BulkInsertStateData *bistate);

extern TM_Result pg_iceberg_tuple_update(IcebergModifyDesc *updateDesc, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
										 Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
										 LockTupleMode *lockmode, bool *update_indexes);

extern TM_Result pg_iceberg_tuple_delete(IcebergModifyDesc *deleteDesc, ItemPointer tid, CommandId cid, Snapshot snapshot,
					 Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart);

/* Vacuum related functions */
extern void pg_iceberg_relation_vacuum(Relation rel, struct VacuumParams *params,
									   BufferAccessStrategy bstrategy);
extern List *pg_iceberg_relation_vacuum_get_dispatch_tasks(Relation rel,
														   struct VacuumParams *params);

/* Helper to get modify descriptor (moved to handler but exposed for internal AM use if needed) */
extern IcebergModifyDesc *get_or_create_modify_descriptor(Relation rel, CmdType operation);
extern IcebergModifyDesc *pg_iceberg_modify_init_for_vacuum(Relation rel, CmdType operation);
extern char *pg_iceberg_modify_finish_for_vacuum(IcebergModifyDesc *modifyDesc);

#endif /* __PG_ICEBERG_AM_H__ */
