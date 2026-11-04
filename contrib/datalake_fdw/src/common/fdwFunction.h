#ifndef DATALAKE_FDWFUNCTION_H
#define DATALAKE_FDWFUNCTION_H


#include "src/datalake_def.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
/*
 * datalake fdw helper functions
 */



void fdwfunction_initScanStatue(ForeignScanState *node, dataLakeFdwScanState *dataLakesstate);

void fdwfunction_iterateScanStatus(ForeignScanState *node, dataLakeFdwScanState *dataLakesstate);

void fdwfunction_iterateRecordBatch(dataLakeFdwScanState *dataLakesstate, VirtualTupleTableSlot *vslot);

void fdwfunction_endScanStatus(dataLakeFdwScanState *dataLakesstate);

void fdwfunction_rescanScanStatus(ForeignScanState *node,
								  dataLakeFdwScanState *dataLakesstate);


void fdwfunction_freeFdwPrivate(dataLakeFdwScanState *sstate, ForeignScan *foreignScan);

bool fdwfunction_hasZeorSelectedPartition(dataLakeFdwScanState *dataLakesstate);

void fdwfunction_DatalakeErrorCallback(void *arg);

void
fdwfunction_deparseTargetList(Relation rel, Bitmapset *attrs_used, List **retrieved_attrs);

void
fdwfunction_extract_used_attributes(RelOptInfo *baserel);

List *
fdwfunction_build_path_tlist(PlannerInfo *root, Path *path);

void
costDataLakeScan(ForeignPath *path, PlannerInfo *root,
				 RelOptInfo *baserel, ParamPathInfo *param_info);

void fdwfunction_initModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo);

void fdwfunction_insertModify(ResultRelInfo *resultRelInfo, TupleTableSlot *slot);

void fdwfunction_endModify(ResultRelInfo *resultRelInfo);

/* External variables */
extern int external_table_limit_segment_num;

/* External functions */
extern int getgpsegmentCount(void);

/*
 * Configuration structure for BeginForeignScan abstraction
 */
typedef struct DatalakeFdwBeginScanConfig {
    /* Function pointers for different implementations */
    dataLakeOptions* (*get_options_func)(void* context);
    List* (*get_fragment_data_func)(void* context);
	void (*get_append_metadata_func)(Relation relation, dataLakeFdwScanState *sstate, List *file_list, void* context);

	/* Store metadata string */
	char* metadataList;
    /* Context data */
    void* context;

} DatalakeFdwBeginScanConfig;

/*
 * Select/Insert FDW functions
 */
void datalakefdw_begin_foreign_scan(ForeignScanState *node, int eflags, DatalakeFdwBeginScanConfig *config);
TupleTableSlot* datalakefdw_iterate_foreign_scan(ForeignScanState *node);
void datalakefdw_end_foreign_scan(ForeignScanState *node);

void datalakefdw_begin_foreign_modify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags,
					  DatalakeFdwBeginScanConfig *config);

TupleTableSlot *
datalakefdw_exec_foreign_insert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot);

void datalakefdw_end_foreign_modify(EState *estate,
					ResultRelInfo *resultRelInfo,
					DatalakeFdwBeginScanConfig *config);

/*
 * Update/Delete FDW functions
 */

void
datalake_add_foreign_update_targets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation);

TupleTableSlot*
datalake_exec_foreign_update(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

TupleTableSlot *datalake_exec_foreign_delete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);


int datalake_is_foreign_rel_updatable(Relation rel);

List *
build_update_tlist_with_junk(Oid foreigntableid, Index varno);

#if PG_VERSION_NUM >= 90500
ForeignScan *
datalake_get_foreign_plan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan);
#else
ForeignScan *
datalake_get_foreign_plan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses);
#endif

void
datalake_get_foreign_paths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid);

void
datalake_get_foreign_relsize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);



#endif