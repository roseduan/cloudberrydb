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
extern void pg_iceberg_refresh_pg_class_stats(Relation rel);

extern List *pg_iceberg_build_scan_am_private(Relation rel, struct PlanState *ps,
											   int random_segment_num);
extern List *pg_iceberg_list_data_fragments(Relation rel);

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
