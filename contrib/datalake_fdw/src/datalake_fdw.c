/*
 * datalake_fdw.c
 *		  Foreign-data wrapper for dataLake (Platform Extension Framework)
 *
 */


#include "datalake_def.h"
#include "datalake_option.h"
#include "datalake_fragment.h"
#include "common/fileSystemWrapper.h"
#include "common/partition_selector.h"
#include "common/random_segment.h"
#include "common/grammar_convert.h"
#include "src/dlproxy/datalake.h"
#include "src/components/agent_cli/c_interface/agent_c_api.h"
#include "src/iceberg_catalog_fdw/iceberg_catalog_fdw.h"
#include "src/provider/iceberg/iceberg_file_index.h"

#include "postgres.h"

#include "access/formatter.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/detoast.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "utils/builtins.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbdisp.h"
#include "commands/copy.h"
#if (PG_VERSION_NUM >= 140000)
#include "commands/copyfrom_internal.h"
#include "commands/copyto_internal.h"
#endif
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
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
#include "optimizer/appendinfo.h"
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
#include "utils/syscache.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "tcop/utility.h"
#include "src/common/fileMetadata.h"
#include "dlproxy/protocol.h"
#include "cdb/cdbdispatchresult.h"

#include "src/common/fdwFunction.h"

#include "am_iceberg/include/pg_iceberg_guc.h"
#include "am_iceberg/include/pg_iceberg_ddl.h"
#include "am_iceberg/include/pg_iceberg_extensible.h"
#include "am_iceberg/include/pg_iceberg_custom_scan.h"
#include "am_iceberg/include/pg_iceberg_am.h"
#include "am_iceberg/include/pg_iceberg_av_consumer.h"


PG_MODULE_MAGIC;

#define DATALAKE_SEGMENT_ID                 GpIdentity.segindex
#define DATALAKE_SEGMENT_COUNT              getgpsegmentCount()

static List *latestFragmentData = NIL;
static double latestIcebergRecordCount = 0;

extern Datum datalake_fdw_handler(PG_FUNCTION_ARGS);

extern Bitmapset **acquire_func_colLargeRowIndexes;
extern double *acquire_func_colNDVBySeg;

void _PG_init(void);

/* Forward declarations for catalog management */
extern void iceberg_catalog_create_catalog(CreateForeignCatalogStmt *createCatalogStmt);
static void handle_create_foreign_catalog(CreateForeignCatalogStmt *createCatalogStmt);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(datalake_fdw_handler);
PG_FUNCTION_INFO_V1(datalake_acquire_sample_rows);

/*
 * FDW functions declarations
 */
static void dataLakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

#if PG_VERSION_NUM >= 90500
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan);
#else
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses);
#endif

static void dataLakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);

static void dataLakeExplainForeignScan(ForeignScanState *node, ExplainState *es);

static void dataLakeBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *dataLakeIterateForeignScan(ForeignScanState *node);

static void dataLakeReScanForeignScan(ForeignScanState *node);

static void dataLakeEndForeignScan(ForeignScanState *node);

static void dataLakeAddForeignUpdateTargets(PlannerInfo *root,
											Index rtindex,
											RangeTblEntry *target_rte,
											Relation target_relation);

/* Foreign updates */
static List *dataLakePlanForeignModify(PlannerInfo *root,
									ModifyTable *plan,
									Index resultRelation,
									int subplan_index);

static void dataLakeBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags);

static TupleTableSlot *dataLakeExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

static TupleTableSlot *dataLakeExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

static TupleTableSlot *dataLakeExecForeignDelete (EState *estate,
												  ResultRelInfo *rinfo,
												  TupleTableSlot *slot,
												  TupleTableSlot *planSlot);

static void dataLakeEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static void commit_iceberg_write(Relation relation, dataLakeFdwScanState *sstate, List *file_list, void* context);

static int dataLakeIsForeignRelUpdatable(Relation rel);

static bool dataLakeIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte);

static int	dataLakeAcquireSampleRowsFunc(Relation relation, int elevel,
										  HeapTuple *rows, int targRows,
										  double *totalRows,
										  double *totalDeadRows);
static bool dataLakeAnalyzeForeignTable(Relation relation,
										AcquireSampleRowsFunc *func,
										BlockNumber *totalPages);
static void prepareIcebergDeleteJunkSlot(ResultRelInfo *rinfo, TupleTableSlot *planSlot,
										 const char *opName);

static void datalake_ProcessUtility(PlannedStmt *pstmt,
									const char *queryString,
									bool readOnlyTree,
									ProcessUtilityContext context,
									ParamListInfo params,
									QueryEnvironment *queryEnv,
									DestReceiver *dest,
									QueryCompletion *qc);

ProcessUtility_hook_type datalake_prev_ProcessUtility = NULL;
ProcessDispatchResult_hook_type datalake_prev_ProcessDispatchResult = NULL;

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct DataLakeAnalyzeState
{
	/* collected sample rows */
	HeapTuple  *rows;			/* array of size targrows */
	int			targrows;		/* target # of sample rows */
	int			numrows;		/* # of sample rows collected */

	/* for random sampling */
	double		samplerows;		/* # of rows fetched */
	double		rowstoskip;		/* # of rows to skip before next sample */
	ReservoirStateData rstate;	/* state for reservoir sampling */

	/* working memory contexts */
	MemoryContext anl_cxt;		/* context for per-analyze lifespan data */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} DataLakeAnalyzeState;

static bool disableFilterPushdown;
bool disableCacheFile;
int icebergPostionDeletesThreshold;
int hudiLogMergerThreshold;
double hudiLogSizeScaleFactor;
int external_table_limit_segment_num;
/*
 * Issue #352: user-facing switch for Iceberg ANALYZE sampling.  The QD->QE
 * fragment transport is a PgIcebergAnalyzeDispatch ExtensibleNode (see
 * pg_iceberg_extensible.c), NOT a synced GUC: a synced GUC travels in the
 * gang-connection startup packet, and a large table's fragment list blows
 * past MAX_STARTUP_PACKET_LENGTH, making every gang creation fail.
 */
bool enable_iceberg_analyze_sampling = true;
bool enable_list_in_master;
char *datalake_agent_server_url = NULL;
bool skip_create_polaris_catalog;
bool enable_get_block_location;
bool enable_iceberg_fragment_cache;

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("datalake.disable_filter_pushdown",
								"Enable passing of query constraints to datalake extension",
								NULL,
								&disableFilterPushdown,
								false,
								PGC_USERSET,
								0,
								NULL,
								NULL,
								NULL);

	DefineCustomBoolVariable("datalake.disable_cache_file",
								"Enable cache files",
								NULL,
								&disableCacheFile,
								false,
								PGC_USERSET,
								0,
								NULL,
								NULL,
								NULL);

	DefineCustomBoolVariable("datalake.external_table_debug",
							"If the value is true, datalake foreign table to turn debug on.",
							NULL,
							&external_table_debug,
							false,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.external_table_new_text",
							"If the value is true, datalake foreign table use the new logic parse text.",
							NULL,
							&external_table_new_text,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("datalake.iceberg_postion_deletes_threshold",
							"default iceberg position delete file threshold",
							NULL,
							&icebergPostionDeletesThreshold,
							100000,
							100000,
							10000000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("datalake.hudi_log_merger_threshold",
							"The log size threshold in MB, for merging log records",
							NULL,
							&hudiLogMergerThreshold,
							512,
							128,
							10240,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomRealVariable("datalake.hudi_log_scale_factor",
							"The calculation factor used for determining the size of temporary file",
							NULL,
							&hudiLogSizeScaleFactor,
							1.3,
							1,
							10,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.enable_iceberg_analyze_sampling",
							"Collect per-column statistics for Iceberg tables by sampling during ANALYZE (issue #352). When off, ANALYZE only refreshes pg_class.reltuples/relpages from metadata.",
							NULL,
							&enable_iceberg_analyze_sampling,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("datalake.external_table_limit_segment_num",
							"Limit the number of segments executed datalake foreign table.",
							NULL,
							&external_table_limit_segment_num,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.external_table_ignore_hidden_file",
							"If the value is true, datalake foreign table ignore read hidden file or directory.",
							NULL,
							&external_table_ignore_hidden_file,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.enable_set_hdfs_user",
							"If the value is true, set the user for writing to HDFS based on the users in the user mapping.",
							NULL,
							&enable_set_hdfs_user,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.enable_list_in_master",
							"If the guc is set to true, the list directory operation will be executed only by the master."
							"Note: This guc is only used for hdfs list directory operation.",
							NULL,
							&enable_list_in_master,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.enable_get_block_location",
							"If the guc is set to true, the list directory operation will get hdfs block location."
							"Note: This guc is only used for hdfs list directory operation.",
							NULL,
							&enable_get_block_location,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("datalake.agent_server_url",
							"Agent server URL for catalog operations",
							NULL,
							&datalake_agent_server_url,
							"http://localhost:3888",
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.skip_create_polaris_catalog",
							"If the value is true, skip creating polaris catalog.",
							NULL,
							&skip_create_polaris_catalog,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("datalake.enable_iceberg_fragment_cache",
							"Cache Iceberg fragment lists using snapshot ID validation.",
							NULL,
							&enable_iceberg_fragment_cache,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	pg_iceberg_init_gucs();
	pg_iceberg_register_extensible_nodes();
	pg_iceberg_install_custom_scan();

	datalake_prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = datalake_ProcessUtility;
	datalake_prev_ProcessDispatchResult = ProcessDispatchResult_hook;
	pg_iceberg_setup_ddl_hooks();
	ProcessDispatchResult_hook = FDW_RecvMeta;
	FDWRecvProtocol = RecvMetaMethod;

	/* Register the autovacuum-driven deletion queue consumer. */
	pg_iceberg_av_consumer_init();
}

/*
 * Collect the OIDs of Iceberg-AM relations targeted by an ANALYZE (issue #352).
 *
 * Handles an explicit or mixed rel list (filtering to the Iceberg ones) as
 * well as a database-wide ANALYZE (rels == NIL), where every Iceberg table in
 * the database is returned by scanning pg_class.  Non-Iceberg rels are ignored;
 * they take the standard ANALYZE path.  Caller frees the returned list.
 */
static List *
collect_iceberg_analyze_relids(List *rels)
{
	List	   *result = NIL;

	if (rels == NIL)
	{
		/* Database-wide ANALYZE: find every Iceberg table via pg_class. */
		Relation		pgclass;
		TableScanDesc	scan;
		HeapTuple		tup;

		pgclass = table_open(RelationRelationId, AccessShareLock);
		scan = table_beginscan_catalog(pgclass, 0, NULL);
		while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			Form_pg_class classform = (Form_pg_class) GETSTRUCT(tup);

			if (classform->relam == ICEBERG_AM_OID &&
				classform->relkind == RELKIND_RELATION)
				result = lappend_oid(result, classform->oid);
		}
		table_endscan(scan);
		table_close(pgclass, AccessShareLock);
	}
	else
	{
		ListCell   *lc;

		foreach(lc, rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
			Oid			relid;
			HeapTuple	tup;
			bool		is_iceberg;

			if (OidIsValid(vrel->oid))
				relid = vrel->oid;
			else if (vrel->relation != NULL)
				relid = RangeVarGetRelid(vrel->relation, NoLock, true);
			else
				continue;
			if (!OidIsValid(relid))
				continue;

			tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
			if (!HeapTupleIsValid(tup))
				continue;
			is_iceberg = (((Form_pg_class) GETSTRUCT(tup))->relam == ICEBERG_AM_OID);
			ReleaseSysCache(tup);

			if (is_iceberg)
				result = lappend_oid(result, relid);
		}
	}
	return result;
}

/*
 * Lightweight check using syscache (no relation_open) so we can probe early
 * in ProcessUtility without taking extra locks beyond what the caller
 * already acquired via AlterTableLookupRelation / RangeVarGetRelid.
 */
static bool
relid_is_iceberg(Oid relid)
{
	HeapTuple	tup;
	bool		is_iceberg;

	if (!OidIsValid(relid))
		return false;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		return false;

	is_iceberg = (((Form_pg_class) GETSTRUCT(tup))->relam == ICEBERG_AM_OID);
	ReleaseSysCache(tup);
	return is_iceberg;
}

/*
 * No ALTER TABLE subcommand is safe on an Iceberg AM table from inside
 * Cloudberry: ALTER COLUMN TYPE drives the standard PG rewrite into empty AM
 * stubs (iceberg_relation_set_new_filenode / iceberg_relation_copy_data) and
 * SIGSEGVs the backend; every other subcommand half-applies (PG catalog
 * updated while the Iceberg manifest stays stale) and silently desyncs PG
 * readers from Spark/Trino/Flink ones.  ADD COLUMN superficially worked via
 * field-id mapping but offered no way to keep the Iceberg-side schema in
 * sync, so it is rejected here too -- schema evolution must go through an
 * Iceberg-aware writer.
 *
 * RENAME COLUMN arrives as T_RenameStmt rather than T_AlterTableStmt; it is
 * rejected in the sibling branch of datalake_ProcessUtility.
 */
static void
reject_unsupported_iceberg_alter(List *cmds)
{
	if (cmds == NIL)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ALTER TABLE is not supported on Iceberg tables"),
			 errhint("Use Spark/Trino/Flink to perform Iceberg schema evolution, or recreate the table.")));
}

static void
datalake_ProcessUtility(PlannedStmt *pstmt,
						const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						QueryCompletion *qc)
{
	switch (nodeTag(pstmt->utilityStmt))
	{
		case T_CreateExternalStmt:
		{
			CreateExternalStmt *createExtStmt;
			CreateForeignTableStmt *foreignStmt = NULL;

			createExtStmt = (CreateExternalStmt *) pstmt->utilityStmt;
			foreignStmt = ConvertExternalTableStmt(createExtStmt);
			if (foreignStmt)
				pstmt->utilityStmt = (Node *) foreignStmt;
			break;
		}
		case T_CreateForeignCatalogStmt:
		{
			CreateForeignCatalogStmt *createCatalogStmt = (CreateForeignCatalogStmt *) pstmt->utilityStmt;
			handle_create_foreign_catalog(createCatalogStmt);
			break;
		}
		case T_ExtensibleNode:
		{
			/*
			 * Iceberg ships AM-private payloads (VACUUM rewrite tasks,
			 * ANALYZE fragment lists) to QEs as ExtensibleNodes through
			 * CdbDispatchUtilityStatement.  Recognise and execute them here;
			 * standard_ProcessUtility has no T_ExtensibleNode case and would
			 * error out.
			 */
			if (pg_iceberg_handle_extensible_utility(pstmt->utilityStmt))
				return;
			break;
		}
		case T_VacuumStmt:
		{
			VacuumStmt *vacstmt = (VacuumStmt *) pstmt->utilityStmt;

			/*
			 * Plain ANALYZE on Iceberg relations: refresh pg_class.reltuples
			 * and pg_class.relpages from the Iceberg catalog metadata (ORCA
			 * reads rel->rd_rel->reltuples directly and never calls the
			 * tableam relation_estimate_size callback; issue #339), and --
			 * when sampling is enabled (issue #352) -- expand every target
			 * rel's fragment list on the QD and ship it to the QEs before
			 * falling through to standard ANALYZE, so the per-row sampling
			 * dispatched by do_analyze_rel can scan without contacting the
			 * QD-only Iceberg catalog service.
			 *
			 * VACUUM — with or without ANALYZE — still goes through the
			 * default path where iceberg_relation_vacuum handles the AM
			 * work via ExtensibleNode dispatch.
			 */
			if (Gp_role == GP_ROLE_DISPATCH && !vacstmt->is_vacuumcmd)
			{
				List	   *ice_relids = collect_iceberg_analyze_relids(vacstmt->rels);
				bool		only_iceberg = (vacstmt->rels != NIL &&
											list_length(ice_relids) == list_length(vacstmt->rels));
				ListCell   *lc;
				List	   *frag_relids = NIL;
				List	   *frag_payloads = NIL;

				/* No Iceberg tables involved: plain standard ANALYZE. */
				if (ice_relids == NIL)
					break;

				/*
				 * Refresh pg_class.reltuples/relpages for every Iceberg rel
				 * from catalog metadata (issue #339), and -- when sampling is
				 * enabled -- collect each rel's expanded fragment list so the
				 * QEs can sample without touching the QD-only Iceberg metadata
				 * catalog.  Covers explicit, mixed, and database-wide
				 * (rels == NIL) ANALYZE.
				 */
				foreach(lc, ice_relids)
				{
					Oid			relid = lfirst_oid(lc);
					Relation	rel;

					/*
					 * try_table_open returns NULL if the relation was dropped
					 * concurrently (possible during a database-wide ANALYZE
					 * that enumerated tables from pg_class) -- skip it instead
					 * of failing the whole statement.
					 */
					rel = try_table_open(relid, ShareUpdateExclusiveLock, false);
					if (rel == NULL)
						continue;

					/*
					 * Honor ANALYZE privileges: never refresh/lock or ship the
					 * metadata of a table the caller may not analyze.  The
					 * kernel skips such tables for a database-wide ANALYZE too,
					 * so this keeps the pre-pass consistent and avoids touching
					 * other owners' tables.
					 */
					if (!pg_class_ownercheck(relid, GetUserId()))
					{
						table_close(rel, ShareUpdateExclusiveLock);
						continue;
					}

					pg_iceberg_refresh_pg_class_stats(rel);

					if (enable_iceberg_analyze_sampling)
					{
						IcebergMetadataInfo *mi = pg_iceberg_get_metadata_info(relid);
						IcebergTableInfo *ti = pg_iceberg_get_table_info(relid);
						char	   *fragments;

						/*
						 * Enumerate the fragment list HERE on the QD: the
						 * datalake agent is QD-only, so a QE cannot expand it
						 * (would fail with CURL code 7).  Ship the expanded
						 * list instead of just the location.
						 */
						fragments = pg_iceberg_get_fragments_with_catalog(
											rel, ti, mi->metadata_location,
											mi->is_internal, NULL);

						frag_relids = lappend_oid(frag_relids, relid);
						frag_payloads = lappend(frag_payloads,
												makeString(fragments));

						pg_iceberg_free_table_info(ti);
						pg_iceberg_free_metadata_info(mi);
					}

					table_close(rel, ShareUpdateExclusiveLock);
				}

				ereport(NOTICE,
						(errmsg("ANALYZE on Iceberg tables refreshed pg_class.reltuples/relpages from Iceberg catalog metadata")));

				if (!enable_iceberg_analyze_sampling)
				{
					/*
					 * Sampling off: metadata-only refresh.  Short-circuit only
					 * when every target is Iceberg (issue #339 behavior); for a
					 * mixed or database-wide list, fall through so non-Iceberg
					 * rels still get analyzed -- the Iceberg rels are skipped by
					 * pg_iceberg_acquire_sample_rows (no payload was shipped).
					 */
					list_free(ice_relids);
					if (only_iceberg)
						return;
					break;
				}

				/*
				 * Issue #352: ship every target rel's fragment list to the QEs
				 * as a PgIcebergAnalyzeDispatch ExtensibleNode, then fall
				 * through to standard ANALYZE.  do_analyze_rel then dispatches
				 * per-row sampling (gp_acquire_sample_rows) to the QEs, whose
				 * pg_iceberg_acquire_sample_rows consumes the stashed lists to
				 * populate pg_statistic.
				 *
				 * The payload must NOT travel as a synced GUC: makeOptions()
				 * embeds synced GUCs into the gang-connection startup packet,
				 * and a single large table's fragment list (e.g. TPC-DS
				 * store_sales, ~136kB) exceeds MAX_STARTUP_PACKET_LENGTH
				 * (64000), so the segment postmasters reject every gang
				 * connection ("invalid length of startup packet") and ANALYZE
				 * dies with "failed to acquire resources".  The utility
				 * dispatch below rides the normal query channel, which has no
				 * such limit, and reaches the same writer-gang QEs that the
				 * sampling query will use.  If the gang is recreated between
				 * the two statements the stash is lost and the QEs degrade to
				 * the no-payload skip path (reltuples-only, no error).
				 */
				if (frag_relids != NIL)
				{
					PgIcebergAnalyzeDispatchNode *dispatch_node;

					dispatch_node = (PgIcebergAnalyzeDispatchNode *)
						newNode(sizeof(PgIcebergAnalyzeDispatchNode),
								T_ExtensibleNode);
					dispatch_node->node.extnodename =
						PG_ICEBERG_ANALYZE_DISPATCH_NODE;
					dispatch_node->relids = frag_relids;
					dispatch_node->fragments = frag_payloads;

					CdbDispatchUtilityStatement((Node *) dispatch_node,
												DF_CANCEL_ON_ERROR,
												NIL,
												NULL);
				}
				list_free(ice_relids);
				/* fall through to standard ANALYZE */
			}
			break;
		}
		case T_AlterTableStmt:
		{
			AlterTableStmt *atstmt = (AlterTableStmt *) pstmt->utilityStmt;
			LOCKMODE	lockmode;
			Oid			relid;

			/*
			 * Use the same lock semantics core uses; this also resolves the
			 * relation OID via the standard AlterTable RangeVar callback.
			 */
			lockmode = AlterTableGetLockLevel(atstmt->cmds);
			relid = AlterTableLookupRelation(atstmt, lockmode);

			if (relid_is_iceberg(relid))
				reject_unsupported_iceberg_alter(atstmt->cmds);
			break;
		}
		case T_RenameStmt:
		{
			RenameStmt *rnstmt = (RenameStmt *) pstmt->utilityStmt;

			/*
			 * RENAME COLUMN goes through RenameStmt (not AlterTableStmt).
			 * Iceberg currently has no schema-evolution hook for column rename;
			 * the rename would only touch pg_attribute while Iceberg metadata
			 * keeps the old column name -- silently desyncs PG vs Spark/Trino
			 * readers.  Reject explicitly.
			 */
			if (rnstmt->renameType == OBJECT_COLUMN && rnstmt->relation != NULL)
			{
				Oid		relid;

				relid = RangeVarGetRelid(rnstmt->relation, AccessShareLock,
										 rnstmt->missing_ok);
				if (relid_is_iceberg(relid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("RENAME COLUMN is not supported on Iceberg tables"),
							 errhint("Recreate the table with the desired column name, or rename the column via Spark/Trino/Flink.")));
			}
			break;
		}
		default:
			break;
	}

	if (datalake_prev_ProcessUtility)
		(*datalake_prev_ProcessUtility) (pstmt, queryString, readOnlyTree,
										 context, params, queryEnv,
										 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
}

/*
 * Foreign-data wrapper handler functions:
 * returns a struct with pointers to the
 * datalake_fdw callback routines.
 */
Datum
datalake_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/*
	 * foreign table scan support
	 */

	/* master - only */
	fdw_routine->GetForeignRelSize = dataLakeGetForeignRelSize;
	fdw_routine->GetForeignPaths = dataLakeGetForeignPaths;
	fdw_routine->GetForeignPlan = dataLakeGetForeignPlan;

	fdw_routine->ExplainForeignScan = dataLakeExplainForeignScan;

	/* segment - only when mpp_execute = segments */
	fdw_routine->BeginForeignScan = dataLakeBeginForeignScan;
	fdw_routine->IterateForeignScan = dataLakeIterateForeignScan;
	fdw_routine->ReScanForeignScan = dataLakeReScanForeignScan;
	fdw_routine->EndForeignScan = dataLakeEndForeignScan;

	/*
	 * foreign table insert support
	 */

	/*
	 * AddForeignUpdateTargets set to NULL, no extra target expressions are
	 * added
	 */
	fdw_routine->AddForeignUpdateTargets = dataLakeAddForeignUpdateTargets;

	/*
	 * PlanForeignModify set to NULL, no additional plan-time actions are
	 * taken
	 */
	fdw_routine->PlanForeignModify = dataLakePlanForeignModify;
	fdw_routine->BeginForeignModify = dataLakeBeginForeignModify;
	fdw_routine->ExecForeignInsert = dataLakeExecForeignInsert;

	/*
	 * ExecForeignUpdate and ExecForeignDelete set to NULL since updates and
	 * deletes are not supported
	 */
	fdw_routine->ExecForeignUpdate = dataLakeExecForeignUpdate;
	fdw_routine->ExecForeignDelete = dataLakeExecForeignDelete;
	fdw_routine->EndForeignModify = dataLakeEndForeignModify;
	fdw_routine->IsForeignRelUpdatable = dataLakeIsForeignRelUpdatable;
	fdw_routine->IsForeignScanParallelSafe = dataLakeIsForeignScanParallelSafe;


	/* Support functions for ANALYZE */
	fdw_routine->AnalyzeForeignTable = dataLakeAnalyzeForeignTable;

	PG_RETURN_POINTER(fdw_routine);
}



/*
 * GetForeignRelSize
 *		set relation size estimates for a foreign table
 */
static void
dataLakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	datalake_get_foreign_relsize(root, baserel, foreigntableid);
}

/*
 * dataLakeGetForeignPaths
 */
static void
dataLakeGetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid)
{
	datalake_get_foreign_paths(root, baserel, foreigntableid);
}

/*
 * GetForeignPlan
 *		create a ForeignScan plan node
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
				  List *scan_clauses,
				  Plan *outer_plan)
#else
static ForeignScan *
dataLakeGetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,	/* target list */
				  List *scan_clauses)
#endif
{
	#if PG_VERSION_NUM >= 90500
		return datalake_get_foreign_plan(root, baserel, foreigntableid, best_path, tlist, scan_clauses, outer_plan);
	#else
		return datalake_get_foreign_plan(root, baserel, foreigntableid, best_path, tlist, scan_clauses);
	#endif
}

/*
 * dataLakeExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void
dataLakeExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	elog(DEBUG5, "datalake_fdw: dataLakeExplainForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);

	/* TODO: make this a meaningful callback */

	elog(DEBUG5, "datalake_fdw: dataLakeExplainForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/* Helper functions for common BeginForeignScan */
static dataLakeOptions* datalake_scan_get_options(void* context)
{
	ForeignScanState *node = (ForeignScanState*)context;
	return datalakeGetOptions(RelationGetRelid(node->ss.ss_currentRelation));
}

static dataLakeOptions* datalake_modify_get_options(void* context)
{
	ResultRelInfo *relinfo = (ResultRelInfo*)context;
	return datalakeGetOptions(RelationGetRelid(relinfo->ri_RelationDesc));
}

static List* datalake_get_fragment_data(void* context)
{
	ForeignScanState *node = (ForeignScanState*)context;
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)palloc0(sizeof(dataLakeFdwScanState));
	Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	dataLakesstate->options = datalakeGetOptions(foreigntableid);
	dataLakesstate->rel = node->ss.ss_currentRelation;
	dataLakesstate->quals = gp_external_enable_filter_pushdown ? node->ss.ps.plan->qual : NULL;

	return datalakeGetExternalFragmentList(dataLakesstate->rel, dataLakesstate->quals, dataLakesstate->options, NULL);
}

/*
 * BeginForeignScan
 *   called during executor startup. perform any initialization
 *   needed, but not start the actual scan.
 */
static void
dataLakeBeginForeignScan(ForeignScanState *node, int eflags)
{
	DatalakeFdwBeginScanConfig config = {
		.get_options_func = datalake_scan_get_options,
		.get_fragment_data_func = datalake_get_fragment_data,
		.context = (void*)node
	};

	datalakefdw_begin_foreign_scan(node, eflags, &config);
}

/*
 * IterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *   Fetch one row from the foreign source, returning it in a tuple table slot
 *    (the node's ScanTupleSlot should be used for this purpose).
 *  Return NULL if no more rows are available.
 */
static TupleTableSlot *
dataLakeIterateForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "datalake_fdw: dataLakeIterateForeignScan Executing on segment: %d", DATALAKE_SEGMENT_ID);
	return datalakefdw_iterate_foreign_scan(node);
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning
 */
static void
dataLakeReScanForeignScan(ForeignScanState *node)
{
	elog(DEBUG5, "datalake_fdw: dataLakeReScanForeignScan starts on segment: %d", DATALAKE_SEGMENT_ID);
	dataLakeFdwScanState *dataLakesstate = (dataLakeFdwScanState*)node->fdw_state;
	if (FORMAT_IS_CUSTOM(dataLakesstate->options->format))
	{
		datalake_to_exttable_ReScanForeignScan(node);
	}
	fdwfunction_endScanStatus(dataLakesstate);
	fdwfunction_initScanStatue(node, dataLakesstate);
	elog(DEBUG5, "datalake_fdw: dataLakeReScanForeignScan ends on segment: %d", DATALAKE_SEGMENT_ID);
}

/*
 * EndForeignScan
 *		End the scan and release resources.
 */
static void
dataLakeEndForeignScan(ForeignScanState *node)
{
	datalakefdw_end_foreign_scan(node);
}

void dataLakeAddForeignUpdateTargets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation)
{
	datalake_add_foreign_update_targets(root, rtindex, target_rte, target_relation);
}

/*
 * dataLakePlanForeignModify
 * 		Generate file prefix, plus (for Iceberg UPDATE/DELETE) the complete
 * 		fragment list that lets every QE populate the global file-ID map.
 */
static List *dataLakePlanForeignModify(PlannerInfo *root,
									   ModifyTable *plan,
									   Index resultRelation,
									   int subplan_index)
{
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	char	   *filePrefix = datalakeGetExternalWriteLocation(rte->relid);
	List	   *allFragments = NIL;

	/*
	 * Issue #333: when Redistribute Motion splits the Iceberg ForeignScan
	 * slice from the ModifyTable slice, the writer QE never runs the scanner
	 * and so its lazy per-reader population of datalake_iceberg_file_index_map
	 * never happens, leaving file_id -> file_path lookups empty.  Compute the
	 * full fragment list here (same helper the scan path uses) and dispatch
	 * it through fdw_private so BeginForeignModify on every QE can call
	 * icebergFileIndexMapPopulateFromAllFragments() up front.
	 */
	if (plan->operation == CMD_UPDATE || plan->operation == CMD_DELETE)
	{
		dataLakeOptions *opts = datalakeGetOptions(rte->relid);

		if (FORMAT_IS_ICEBERG(opts->format))
		{
			Relation	rel = table_open(rte->relid, NoLock);

			allFragments = datalakeGetExternalFragmentList(rel, NIL, opts, NULL);
			table_close(rel, NoLock);
		}
	}

	/*
	 * Keep slot indices in sync with FdwModifyPrivateIndex.  Always append
	 * an entry for FdwModifyAllFragments (NIL when unused) so list_nth at
	 * BeginForeignModify is stable.
	 */
	return list_make2(makeString(filePrefix), allFragments);
}


/*
 * dataLakeBeginForeignModify
 *		Begin an insert operation on a foreign table
 */
static void
dataLakeBeginForeignModify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags)
{
	DatalakeFdwBeginScanConfig config = {
		.get_options_func = datalake_modify_get_options,
		.context = (void*)resultRelInfo
	};
	datalakefdw_begin_foreign_modify(mtstate, resultRelInfo, fdw_private, subplan_index, eflags, &config);
}

/*
 * dataLakeExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
dataLakeExecForeignInsert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	return datalakefdw_exec_foreign_insert(estate, resultRelInfo, slot, planSlot);
}

/*
 * prepareIcebergDeleteJunkSlot
 *		Common helper for UPDATE and DELETE: decode ctid from the plan slot
 *		to obtain the Iceberg file path and row position, then fill the
 *		junk slot that the delete-file writer expects.
 */
static void
prepareIcebergDeleteJunkSlot(ResultRelInfo *rinfo, TupleTableSlot *planSlot,
							 const char *opName)
{
	dataLakeFdwScanState	*sstate		= (dataLakeFdwScanState*)rinfo->ri_FdwState;
	dataLakeModifyState		*mstate		= sstate->modify_state;
	TupleTableSlot			*junk_slot;
	Datum		datum;
	bool		isNull;
	ItemPointer	ctid;
	uint32		fileId;
	int64		position;
	const char *filePath;

	Assert(mstate);
	junk_slot = mstate->us_slot;
	ExecClearTuple(junk_slot);

	datum = ExecGetJunkAttribute(planSlot,
								mstate->us_ctid_no,
								&isNull);
	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "ctid is NULL");
	ctid = (ItemPointer) DatumGetPointer(datum);

	/* Decode tid to get fileId and position */
	icebergDecodeTID(ctid, &fileId, &position);

	/* Get file path from global fileIndexMap */
	filePath = icebergGetFilePath(datalake_iceberg_file_index_map, fileId);
	if (filePath == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Failed to get file path for file ID %u in Iceberg %s", fileId, opName)));
	}

	/* Fill junk slot with file path and position */
	junk_slot->tts_values[0] = CStringGetDatum(filePath);
	junk_slot->tts_isnull[0] = false;
	junk_slot->tts_values[1] = Int64GetDatum((int64) position);
	junk_slot->tts_isnull[1] = false;

	elog(DEBUG2, "datalake_fdw: %s writing delete file=%s, pos="UINT64_FORMAT" for file ID %u",
			opName, filePath, position, fileId);
}

static void commit_iceberg_write(Relation relation, dataLakeFdwScanState *sstate, List *file_list, void* context)
{
	datalakeCommitExternalWrite(relation, sstate, file_list);
}

TupleTableSlot *dataLakeExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	dataLakeFdwScanState	*sstate		= (dataLakeFdwScanState*)rinfo->ri_FdwState;
	dataLakeModifyState		*mstate		= sstate->modify_state;
	MemoryContext			 oldcontext;

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignUpdate starts on segment: %d", DATALAKE_SEGMENT_ID);

	Assert(mstate);

	/*
	 * Iceberg merge-on-read UPDATE is emitted as "INSERT new row + write a
	 * position-delete entry pointing at the old row".  The old row's
	 * (file_path, pos) must come from the ctid junk column, decoded through
	 * the per-operation fileIndexMap built in BeginForeignModify -- the same
	 * path that ExecForeignDelete uses.
	 *
	 * The previous implementation tried to read the junk values from
	 * wholerow attribute positions natts+1 / natts+2, which are past the end
	 * of the wholerow tuple for Iceberg foreign tables; heap_getattr returned
	 * NULL and 0 for every row, causing every position-delete file to be
	 * written as ("", 0).  That silently broke merge-on-read correctness:
	 * the delete entries never matched any data file path, the reader's
	 * delete index was built empty, and queries returned both the pre- and
	 * post-update versions of every updated row.
	 */
	prepareIcebergDeleteJunkSlot(rinfo, planSlot, "UPDATE");

	MemoryContextReset(sstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(sstate->rowcontext);

	/* Write the new row version to the data provider. */
	slot_getallattrs(slot);
	writeToProvider(sstate->provider, slot, 0);

	/* Write the position-delete entry for the old row. */
	writeToProvider(mstate->us_provider, mstate->us_slot, 0);

	MemoryContextSwitchTo(oldcontext);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignUpdate ends on segment: %d", DATALAKE_SEGMENT_ID);
	return slot;
}

/*
 * dataLakeEndForeignModify
 *		Finish an insert operation on a foreign table
 */
static void
dataLakeEndForeignModify(EState *estate,
					ResultRelInfo *resultRelInfo)
{
	DatalakeFdwBeginScanConfig config = {
		.get_append_metadata_func = commit_iceberg_write
	};
	datalakefdw_end_foreign_modify(estate, resultRelInfo, &config);
}

TupleTableSlot *dataLakeExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	dataLakeFdwScanState	*sstate		= (dataLakeFdwScanState*)rinfo->ri_FdwState;
	dataLakeModifyState		*mstate		= sstate->modify_state;
	MemoryContext			 oldcontext;

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignDelete starts on segment: %d", DATALAKE_SEGMENT_ID);

	prepareIcebergDeleteJunkSlot(rinfo, planSlot, "DELETE");

	MemoryContextReset(sstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(sstate->rowcontext);

	writeToProvider(mstate->us_provider, mstate->us_slot, 0);

	MemoryContextSwitchTo(oldcontext);

	elog(DEBUG5, "datalake_fdw: dataLakeExecForeignDelete ends on segment: %d", DATALAKE_SEGMENT_ID);
	return slot;
}

/*
 * dataLakeIsForeignRelUpdatable
 *  Assume table is updatable regardless of settings.
 *		Determine whether a foreign table supports INSERT, UPDATE and/or
 *		DELETE.
 */
static int
dataLakeIsForeignRelUpdatable(Relation rel)
{
	return datalake_is_foreign_rel_updatable(rel);
}

static bool dataLakeIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte)
{
	return false;
}


/*
 * Shuffle a list in-place using the Fisher-Yates algorithm.
 * Returns a new list with elements in random order.
 */
static List *
analyzeShuffleFragments(List *fragments)
{
	int			nfrags = list_length(fragments);
	ListCell   *cell;
	int			i;
	void	  **arr;
	List	   *result = NIL;

	if (nfrags <= 1)
		return fragments;

	arr = palloc(nfrags * sizeof(void *));
	i = 0;
	foreach(cell, fragments)
		arr[i++] = lfirst(cell);

	for (i = nfrags - 1; i > 0; i--)
	{
		int		j = random() % (i + 1);
		void   *tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}

	for (i = 0; i < nfrags; i++)
		result = lappend(result, arr[i]);

	pfree(arr);
	return result;
}

static ForeignScanState *
dataLakeAnalyzeBeginScan(Relation relation, int *total_fragments)
{
	int   i;
	int   segmentcount = DATALAKE_SEGMENT_COUNT;
	List *fragmentData;
	List *retrieved_attrs = NIL;
	List *selected_segments = NIL;
	TupleDesc tupdesc = RelationGetDescr(relation);
	ForeignScanState *node = (ForeignScanState *) palloc0(sizeof(ForeignScanState));
	dataLakeFdwScanState *state = (dataLakeFdwScanState *) palloc0(sizeof(dataLakeFdwScanState));

	state->options = datalakeGetOptions(RelationGetRelid(relation));

	int len = list_length(latestFragmentData);
	for (int i = segmentcount; i >= 1; i--)
	{
		int idx = intVal(list_nth(latestFragmentData, len - i));
		selected_segments = lappend_int(selected_segments, idx);
	}
	/* remove the last segmentcount elements */
	latestFragmentData = list_truncate(latestFragmentData, len - segmentcount);

	fragmentData = datalakeDeserializeExternalFragmentList(relation,
															NIL,
															state->options,
															latestFragmentData);

	/*
	 * For ANALYZE, shuffle fragments so we can scan a random subset
	 * and stop early once we've collected enough sample rows.
	 *
	 * For Iceberg/Hudi, fragmentData[0] is ExternalTableMetadata (not a
	 * CombinedScanTask). It must stay at position 0 because
	 * datalakeProtocolImportStart skips i==0 and casts it to
	 * ExternalTableMetadata*. Shuffling it would cause SIGSEGV when
	 * flatCombinedTasks tries to list_free a non-List pointer.
	 */
	*total_fragments = list_length(fragmentData);
	if (FORMAT_IS_ICEBERG(state->options->format) || FORMAT_IS_HUDI(state->options->format))
	{
		List *metadata = list_make1(linitial(fragmentData));
		List *tasks = list_copy_tail(fragmentData, 1);
		tasks = analyzeShuffleFragments(tasks);
		fragmentData = list_concat(metadata, tasks);
	}
	else
	{
		fragmentData = analyzeShuffleFragments(fragmentData);
	}

	state->provider = initProvider(state->options->format, DL_OP_READ, false);
	state->rel = relation;
	state->fragments = fragmentData;
	state->selected_segments = selected_segments;
	state->scan_tupdesc = CreateTupleDescCopy(tupdesc);

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		if (attr->attisdropped)
			continue;

		retrieved_attrs = lappend_int(retrieved_attrs, i);
	}

	state->retrieved_attrs = retrieved_attrs;

	node->ss.ps.plan = palloc0(sizeof(ForeignScan));
	node->ss.ss_ScanTupleSlot = table_slot_create(relation, NULL);
	node->fdw_state = state;

	if (fdwfunction_hasZeorSelectedPartition(state))
		return node;

	fdwfunction_initScanStatue(node, state);
	return node;
}

static TupleTableSlot *
dataLakeAnalyzeScanNext(ForeignScanState *node)
{
	return dataLakeIterateForeignScan(node);
}

static void
dataLakeAnalyzeEndScan(ForeignScanState *node)
{
	ExecDropSingleTupleTableSlot(node->ss.ss_ScanTupleSlot);
	dataLakeEndForeignScan(node);
}

static const char base64_[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const int8 b64lookup_[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
	-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
	15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
	-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
	41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

static unsigned
pg_base64_encode(const char *src, unsigned len, char *dst)
{
	char	   *p,
			   *lend = dst + 76;
	const char *s,
			   *end = src + len;
	int			pos = 2;
	uint32		buf = 0;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= (unsigned char) *s << (pos << 3);
		pos--;
		s++;

		/* write it out */
		if (pos < 0)
		{
			*p++ = base64_[(buf >> 18) & 0x3f];
			*p++ = base64_[(buf >> 12) & 0x3f];
			*p++ = base64_[(buf >> 6) & 0x3f];
			*p++ = base64_[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
		if (p >= lend)
		{
			*p++ = '\n';
			lend = p + 76;
		}
	}
	if (pos != 2)
	{
		*p++ = base64_[(buf >> 18) & 0x3f];
		*p++ = base64_[(buf >> 12) & 0x3f];
		*p++ = (pos == 0) ? base64_[(buf >> 6) & 0x3f] : '=';
		*p++ = '=';
	}

	return p - dst;
}

static unsigned
pg_base64_decode(const char *src, unsigned len, char *dst)
{
	const char *srcend = src + len,
			   *s = src;
	char	   *p = dst;
	char		c;
	int			b = 0;
	uint32		buf = 0;
	int			pos = 0,
				end = 0;

	while (s < srcend)
	{
		c = *s++;

		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			continue;

		if (c == '=')
		{
			/* end sequence */
			if (!end)
			{
				if (pos == 2)
					end = 1;
				else if (pos == 3)
					end = 2;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("unexpected \"=\"")));
			}
			b = 0;
		}
		else
		{
			b = -1;
			if (c > 0 && c < 127)
				b = b64lookup_[(unsigned char) c];
			if (b < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid symbol")));
		}
		/* add it to buffer */
		buf = (buf << 6) + b;
		pos++;
		if (pos == 4)
		{
			*p++ = (buf >> 16) & 255;
			if (end == 0 || end > 1)
				*p++ = (buf >> 8) & 255;
			if (end == 0 || end > 2)
				*p++ = buf & 255;
			buf = 0;
			pos = 0;
		}
	}

	if (pos != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid end sequence")));

	return p - dst;
}


static unsigned
pg_base64_enc_len(const char *src, unsigned srclen)
{
	/* 3 bytes will be converted to 4, linefeed after 76 chars */
	return (srclen + 2) * 4 / 3 + srclen / (76 * 3 / 4);
}

static unsigned
pg_base64_dec_len(const char *src, unsigned srclen)
{
	return (srclen * 3) >> 2;
}

static char *
encode_string(char *data, unsigned len)
{
	char   *result;
	int		res;
	int		resultlen;

	resultlen = pg_base64_enc_len(data, len);
	result = palloc(resultlen + 1);
	res = pg_base64_encode(data, len, result);
	if (res > resultlen)
		elog(FATAL, "overflow - base64 encode estimate too small");

	result[res] = '\0';
	return result;
}

static char *
decode_string(char *data, unsigned len, int *decodedLen)
{
	char   *result;
	int		res;
	int		resultlen;

	resultlen = pg_base64_dec_len(data, len);
	result = palloc(resultlen);
	res = pg_base64_decode(data, len, result);
	if (res > resultlen)
		elog(FATAL, "overflow - base64 decode estimate too small");

	*decodedLen = res;
	return result;
}

static int
process_sample_rows(Portal portal,
					QueryDesc  *queryDesc,
					Relation onerel,
					HeapTuple *rows,
					int targrows,
					double *totalrows,
					double *totaldeadrows)
{
	/*
	 * 'colLargeRowIndexes' is essentially an argument, but it's passed via a
	 * global variable to avoid changing the AcquireSampleRowsFunc prototype.
	 */
	Bitmapset **colLargeRowIndexes = acquire_func_colLargeRowIndexes;
	/* double     *colLargeRowLength = acquire_func_colLargeRowLength; */
	double     *colNDVBySeg = acquire_func_colNDVBySeg;
	TupleDesc	relDesc = RelationGetDescr(onerel);
	TupleDesc	funcTupleDesc;
	TupleDesc	sampleTupleDesc;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	Datum	   *funcRetValues;
	bool	   *funcRetNulls;
	int			ncolumns;
	AttInMetadata *attinmeta;
	int			numLiveColumns;
	int			i;
	int			index = 0;
	TupleTableSlot *slot;
	Datum	   *dvalues;
	bool	   *dnulls;

	/*
	 * Count the number of columns, excluding dropped columns. We'll need that
	 * later.
	 */
	numLiveColumns = 0;
	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		if (attr->attisdropped)
			continue;

		numLiveColumns++;
	}

	/*
	 * Build a modified tuple descriptor for the table.
	 *
	 * Some datatypes need special treatment, so we cannot use the relation's
	 * original tupledesc.
	 *
	 * Also create tupledesc of return record of function gp_acquire_sample_rows.
	 */
	sampleTupleDesc = CreateTupleDescCopy(relDesc);
	ncolumns = numLiveColumns + NUM_SAMPLE_FIXED_COLS;

	funcTupleDesc = CreateTemplateTupleDesc(ncolumns);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 1, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 2, "", FLOAT8OID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 3, "", FLOAT8ARRAYOID, -1, 0);
	TupleDescInitEntry(funcTupleDesc, (AttrNumber) 4, "", FLOAT8ARRAYOID, -1, 0);

	for (i = 0; i < relDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relDesc, i);

		Oid			typid = gp_acquire_sample_rows_col_type(attr->atttypid);

		TupleDescAttr(sampleTupleDesc, i)->atttypid = typid;

		if (!attr->attisdropped)
		{
			TupleDescInitEntry(funcTupleDesc, (AttrNumber) NUM_SAMPLE_FIXED_COLS + 1 + index, "",
							   typid, attr->atttypmod, attr->attndims);

			index++;
		}
	}

	/* For RECORD results, make sure a typmod has been assigned */
	Assert(funcTupleDesc->tdtypeid == RECORDOID && funcTupleDesc->tdtypmod < 0);
	assign_record_type_typmod(funcTupleDesc);

	attinmeta = TupleDescGetAttInMetadata(sampleTupleDesc);

	/*
	 * Read the result set from each segment. Gather the sample rows *rows,
	 * and sum up the summary rows for grand 'totalrows' and 'totaldeadrows'.
	 */
	funcRetValues = (Datum *) palloc0(funcTupleDesc->natts * sizeof(Datum));
	funcRetNulls = (bool *) palloc0(funcTupleDesc->natts * sizeof(bool));
	dvalues = (Datum *) palloc0(relDesc->natts * sizeof(Datum));
	dnulls = (bool *) palloc0(relDesc->natts * sizeof(bool));
	sampleTuples = 0;
	*totalrows = 0;
	*totaldeadrows = 0;

	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc, &TTSOpsMinimalTuple);

	for (;;)
	{
		bool		ok;
		TupleDesc	typeinfo;
		int			natts;
		Datum		attr;
		bool		isnull;
		double		this_totalrows = 0;
		double		this_totaldeadrows = 0;

		CHECK_FOR_INTERRUPTS();

		ok = tuplestore_gettupleslot(portal->holdStore, true, false, slot);

		if (!ok)
			break;

		typeinfo = slot->tts_tupleDescriptor;
		natts = typeinfo->natts;

		/* There should be only one attribute with OID RECORDOID. */
		if (1 != natts)
		{
			elog(ERROR,
				"wrong number of attributes %d when 1 expected",
				natts);
		}

		if (RECORDOID != typeinfo->attrs[0].atttypid)
		{
			elog(ERROR,
				"wrong attribute OID %d, RECORDOID %d is expected",
				typeinfo->attrs[0].atttypid, RECORDOID);

		}

		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);

		/* There should be only one attribute with OID RECORDOID */
		attr = slot_getattr(slot, 1, &isnull);
		if (isnull)
		{
			elog(ERROR,
				"null value for attribute in tuple");
		}

		/* Get record from attribute and parse it */
		{
			HeapTupleHeader rec = (HeapTupleHeader) PG_DETOAST_DATUM(attr);
			Oid			tupType;
			int32		tupTypmod;
			TupleDesc	tupdesc;
			HeapTupleData tuple;

			/* Extract type info from the tuple itself */
			tupType = HeapTupleHeaderGetTypeId(rec);
			tupTypmod = HeapTupleHeaderGetTypMod(rec);
			tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

			/* Build a temporary HeapTuple control structure */
			tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
			ItemPointerSetInvalid(&(tuple.t_self));
			tuple.t_data = rec;

			/* Break down the tuple into fields */
			heap_deform_tuple(&tuple, tupdesc, funcRetValues, funcRetNulls);

			if (!funcRetNulls[0])
			{
				/* This is a summary row. */
				ArrayType  *arrayVal;
				Datum      *colndv;
				bool       *nulls;
				int        numelems;

				Assert(!funcRetNulls[1] && !funcRetNulls[3]);

				this_totalrows = DatumGetFloat8(funcRetValues[0]);
				this_totaldeadrows = DatumGetFloat8(funcRetValues[1]);
				(*totalrows) += this_totalrows;
				(*totaldeadrows) += this_totaldeadrows;

				arrayVal = DatumGetArrayTypeP(funcRetValues[3]);
				deconstruct_array(arrayVal, FLOAT8OID, 8, true, 'd',
								  &colndv, &nulls, &numelems);
				for (i = 0; i < relDesc->natts; i++)
				{
					double this_colndv = DatumGetFloat8(colndv[i]);
					if (this_colndv < 0) {
						Assert(this_colndv >= -1);
						colNDVBySeg[i] += abs(this_colndv) * this_totalrows;
					} else {
						/* if current segment have any data, then ndv won't be 0.
						 * if current segment have no rows, ndv is 0.
						 */
						colNDVBySeg[i] += DatumGetFloat8(colndv[i]);
					}
				}
			}
			else
			{
				/* This is a sample row. */
				if (sampleTuples >= targrows)
					elog(ERROR, "too many sample rows received from gp_acquire_sample_rows");

				/* Read the 'toolarge' bitmap, if any */
				if (colLargeRowIndexes && !funcRetNulls[2])
				{
					ArrayType  *arrayVal;
					Datum	   *largelength;
					bool	   *nulls;
					int	    numelems;
					arrayVal = DatumGetArrayTypeP(funcRetValues[2]);
					deconstruct_array(arrayVal, FLOAT8OID, 8, true, 'd',
								&largelength, &nulls, &numelems);

					for (i = 0; i < relDesc->natts; i++)
					{
						Form_pg_attribute attr = TupleDescAttr(relDesc, i);

						if (attr->attisdropped)
							continue;

						if (largelength[i] != (Datum) 0)
						{
							colLargeRowIndexes[i] = bms_add_member(colLargeRowIndexes[i], sampleTuples);
							/* colLargeRowLength[i] += DatumGetFloat8(largelength[i]); */
						}
					}
				}

				/* Process the columns */
				index = 0;
				for (i = 0; i < relDesc->natts; i++)
				{
					Form_pg_attribute attr = TupleDescAttr(relDesc, i);

					if (attr->attisdropped)
					{
						dnulls[i] = true;
						continue;
					}

					dnulls[i] = funcRetNulls[NUM_SAMPLE_FIXED_COLS + index];
					dvalues[i] = funcRetValues[NUM_SAMPLE_FIXED_COLS + index];
					index++;	/* Move index to the next result set attribute */
				}

				/*
				* Form a tuple
				*/
				rows[sampleTuples] = heap_form_tuple(attinmeta->tupdesc,
													dvalues,
													dnulls);
				sampleTuples++;

				/*
				 * note: we don't set the OIDs in the sample. ANALYZE doesn't
				 * collect stats for them
				 */
			}
			ReleaseTupleDesc(tupdesc);
		}

		ExecClearTuple(slot);
	}
	ExecDropSingleTupleTableSlot(slot);
	pfree(funcRetValues);
	pfree(funcRetNulls);
	pfree(dvalues);
	pfree(dnulls);

	return sampleTuples;
}

Datum
datalake_acquire_sample_rows(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = NULL;
	gp_acquire_sample_rows_context *ctx;
	MemoryContext oldcontext;
	Oid			relOid = PG_GETARG_OID(0);
	int32		targrows = PG_GETARG_INT32(1);
	bool        inherited = PG_GETARG_BOOL(2);
	text	   *encodedFragment = PG_GETARG_TEXT_PP(3);

	TupleDesc	relDesc;
	TupleDesc	outDesc;
	int			live_natts;

	if (targrows < 1)
		elog(ERROR, "invalid targrows argument");

	if (SRF_IS_FIRSTCALL())
	{
		Relation	onerel;
		int			attno;
		int			outattno;
		VacuumParams	params;
		RangeVar	   *this_rangevar;
		char *decodedFragment;
		int decodedLen;

		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function
		 * calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Construct the context to keep across calls. */
		ctx = (gp_acquire_sample_rows_context *) palloc0(sizeof(gp_acquire_sample_rows_context));
		ctx->targrows = targrows;
		ctx->inherited = inherited;

		if (!pg_class_ownercheck(relOid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
						   get_rel_name(relOid));

		onerel = table_open(relOid, AccessShareLock);
		relDesc = RelationGetDescr(onerel);

		/* will be init in `analyze_rel` */
		ctx->stadistincts = (Datum *) palloc0(relDesc->natts * sizeof(Datum));

		decodedFragment = decode_string(VARDATA_ANY(encodedFragment), VARSIZE_ANY_EXHDR(encodedFragment), &decodedLen);
		latestFragmentData = (List *) deserializeNode(decodedFragment, decodedLen);
		pfree(decodedFragment);

		MemSet(&params, 0, sizeof(VacuumParams));
		params.options |= VACOPT_ANALYZE;
		params.freeze_min_age = -1;
		params.freeze_table_age = -1;
		params.multixact_freeze_min_age = -1;
		params.multixact_freeze_table_age = -1;
		params.is_wraparound = false;
		params.log_min_duration = -1;
		params.index_cleanup = VACOPTVALUE_AUTO;
		params.truncate = VACOPTVALUE_AUTO;

		this_rangevar = makeRangeVar(get_namespace_name(onerel->rd_rel->relnamespace),
									 pstrdup(RelationGetRelationName(onerel)),
									 -1);
		analyze_rel(relOid, this_rangevar, &params, NULL,
					true, GetAccessStrategy(BAS_VACUUM), ctx);

		/* Count the number of non-dropped cols */
		live_natts = 0;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);

			if (relatt->attisdropped)
				continue;
			live_natts++;
		}

		outDesc = CreateTemplateTupleDesc(NUM_SAMPLE_FIXED_COLS + live_natts);

		/* First, some special cols: */

		/* These two are only set in the last, summary row */
		TupleDescInitEntry(outDesc,
						   1,
						   "totalrows",
						   FLOAT8OID,
						   -1,
						   0);
		TupleDescInitEntry(outDesc,
						   2,
						   "totaldeadrows",
						   FLOAT8OID,
						   -1,
						   0);

		/* extra column to indicate oversize cols */
		TupleDescInitEntry(outDesc,
						   3,
						   "oversized_cols_length",
						   FLOAT8ARRAYOID,
						   -1,
						   0);

		/* stadistinct for each live column */
		TupleDescInitEntry(outDesc,
						   4,
						   "stadistinct_array",
						   FLOAT8ARRAYOID,
						   -1,
						   0);

		outattno = NUM_SAMPLE_FIXED_COLS + 1;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
			Oid			typid;

			if (relatt->attisdropped)
				continue;

			typid = gp_acquire_sample_rows_col_type(relatt->atttypid);

			TupleDescInitEntry(outDesc,
							   outattno++,
							   NameStr(relatt->attname),
							   typid,
							   relatt->atttypmod,
							   0);
		}

		BlessTupleDesc(outDesc);
		funcctx->tuple_desc = outDesc;

		ctx->onerel = onerel;
		funcctx->user_fctx = ctx;
		ctx->outDesc = outDesc;

		ctx->index = 0;
		ctx->summary_sent = false;
		/*
		 * we only get sample data from segindex 0 for replicated table
		 */
		if (Gp_role == GP_ROLE_EXECUTE && GpPolicyIsReplicated(onerel->rd_cdbpolicy)
									   && GpIdentity.segindex > 0)
		{
			ctx->index = ctx->num_sample_rows;
			ctx->summary_sent = true;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	ctx = funcctx->user_fctx;
	relDesc = RelationGetDescr(ctx->onerel);
	outDesc = ctx->outDesc;

	Datum	   *outvalues = (Datum *) palloc(outDesc->natts * sizeof(Datum));
	bool	   *outnulls = (bool *) palloc(outDesc->natts * sizeof(bool));
	HeapTuple	res;

	/* First return all the sample rows */
	if (ctx->index < ctx->num_sample_rows)
	{
		HeapTuple	relTuple = ctx->sample_rows[ctx->index];
		int			attno;
		int			outattno;
		bool			has_toolarge = false;
		Datum	   *relvalues = (Datum *) palloc(relDesc->natts * sizeof(Datum));
		bool	   *relnulls = (bool *) palloc(relDesc->natts * sizeof(bool));
		Datum      *oversized_cols_length = (Datum *) palloc0(relDesc->natts * sizeof(Datum));

		heap_deform_tuple(relTuple, relDesc, relvalues, relnulls);

		outattno = NUM_SAMPLE_FIXED_COLS + 1;
		for (attno = 1; attno <= relDesc->natts; attno++)
		{
			Form_pg_attribute relatt = TupleDescAttr(relDesc, attno - 1);
			Datum		relvalue;
			bool		relnull;

			if (relatt->attisdropped)
				continue;
			relvalue = relvalues[attno - 1];
			relnull = relnulls[attno - 1];

			/* Is this attribute "too large" to return? */
			if (relatt->attlen == -1 && !relnull)
			{
				Size		toasted_size = toast_datum_size(relvalue);

				if (toasted_size > WIDTH_THRESHOLD)
				{
					oversized_cols_length[attno - 1] = Float8GetDatum((double)toasted_size);
					has_toolarge = true;
					relvalue = (Datum) 0;
					relnull = true;
				}
			}
			outvalues[outattno - 1] = relvalue;
			outnulls[outattno - 1] = relnull;
			outattno++;
		}

		/*
		 * If any of the attributes were oversized, construct the text datum
		 * to represent the bitmap.
		 */
		if (has_toolarge)
		{
			outvalues[2] = PointerGetDatum(construct_array(oversized_cols_length, relDesc->natts,
														FLOAT8OID, 8, true, 'd'));
			outnulls[2] = false;
		}
		else
		{
			outvalues[2] = (Datum) 0;
			outnulls[2] = true;
		}
		outvalues[0] = (Datum) 0;
		outnulls[0] = true;
		outvalues[1] = (Datum) 0;
		outnulls[1] = true;

		outvalues[3] = (Datum) 0;
		outnulls[3] = true;

		res = heap_form_tuple(outDesc, outvalues, outnulls);

		ctx->index++;

		SIMPLE_FAULT_INJECTOR("returned_sample_row");

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(res));
	}
	else if (!ctx->summary_sent)
	{
		/* Done returning the sample. Return the summary row, and we're done. */
		int			outattno;

		outvalues[0] = Float8GetDatum(ctx->totalrows);
		outnulls[0] = false;
		outvalues[1] = Float8GetDatum(ctx->totaldeadrows);
		outnulls[1] = false;

		outvalues[2] = (Datum) 0;
		outnulls[2] = true;

		outvalues[3] = PointerGetDatum(construct_array(ctx->stadistincts, relDesc->natts,
													   FLOAT8OID, 8, true, 'd'));
		outnulls[3] = false;

		for (outattno = NUM_SAMPLE_FIXED_COLS + 1; outattno <= outDesc->natts; outattno++)
		{
			outvalues[outattno - 1] = (Datum) 0;
			outnulls[outattno - 1] = true;
		}

		res = heap_form_tuple(outDesc, outvalues, outnulls);

		ctx->summary_sent = true;

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(res));
	}

	table_close(ctx->onerel, AccessShareLock);

	pfree(ctx);
	funcctx->user_fctx = NULL;

	SRF_RETURN_DONE(funcctx);

}

/*
 * Build a querydesc for a sql, set "dest" to portal->holdStore
 */
static QueryDesc *build_querydesc(Portal portal, char *sql)
{
	List	   *raw_parsetree_list;
	RawStmt	   *parsetree;
	List	   *querytree_list;
	List	   *plantree_list;
	PlannedStmt *plan_stmt;
	DestReceiver *destReceiver;
	QueryDesc  *queryDesc = NULL;
	destReceiver = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(destReceiver,
									portal->holdStore,
									portal->holdContext,
									false,
									NULL,
									NULL);

	/*
	 * Parse the SQL string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(sql);

	/*
	 * Do parse analysis, rule rewrite, planning, and execution for each raw
	 * parsetree.
	 */

	/* There is only one element in list due to simple select. */
	Assert(list_length(raw_parsetree_list) == 1);
	parsetree = (RawStmt *) linitial(raw_parsetree_list);

	querytree_list = pg_analyze_and_rewrite(parsetree,
											sql,
											NULL,
											0,
											NULL);
	plantree_list = pg_plan_queries(querytree_list, sql, 0, NULL);

	/* There is only one statement in list due to simple select. */
	Assert(list_length(plantree_list) == 1);
	plan_stmt = (PlannedStmt *) linitial(plantree_list);

	queryDesc = CreateQueryDesc(plan_stmt,
								sql,
								GetActiveSnapshot(),
								InvalidSnapshot,
								destReceiver,
								NULL,
								NULL,
								INSTRUMENT_NONE);



	list_free_deep(querytree_list);
	list_free_deep(raw_parsetree_list);

	return queryDesc;
}

static int
acquire_sample_rows_dispatcher(Relation relation, bool inh, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows)
{
	StringInfoData str;
	int			perseg_targrows;
	int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
	char	   *sql;
	Portal		portal;
	dataLakeOptions *options;
	char *sFragment;
	char *encodedFragment;
	int sFragmentLen;
	int sFragmentUnCompressedLen;
	List *sFragmentList;
	QueryDesc  *queryDesc = NULL;

	Assert(targrows > 0.0);

	/*
	 * Step1: Construct SQL command to dispatch to segments.
	 *
	 * Acquire an evenly-sized sample from each segment.
	 *
	 * XXX: If there's a significant bias between the segments, i.e. some
	 * segments have a lot more rows than others, the sample will biased, too.
	 * Would be nice to improve that, but it's not clear how. We could issue
	 * another query to get the table size from each segment first, and use
	 * those to weigh the sample size to get from each segment. But that'd
	 * require an extra round-trip, which is also not good. The caller
	 * actually already did that, to get the total relation size, but it
	 * doesn't pass that down to us, let alone the per-segment sizes.
	 */
	perseg_targrows = targrows / relation->rd_cdbpolicy->numsegments;

	options = datalakeGetOptions(RelationGetRelid(relation));

	sFragmentList = list_make2(makeString("dummy"), makeString("dummy"));
	sFragmentList = list_concat(sFragmentList, latestFragmentData);
	sFragment = serializeNode((Node *) sFragmentList, &sFragmentLen, &sFragmentUnCompressedLen);
	encodedFragment = encode_string(sFragment, sFragmentLen);
	pfree(sFragment);

	/*
	 * Did not use 'select * from pg_catalog.gp_acquire_sample_rows(...) as (..);'
	 * here. Because it requires to specify columns explicitly which leads to
	 * permission check on each columns. This is not consistent with GPDB5 and
	 * may result in different behaviour under different acl configuration.
	 */
	initStringInfo(&str);
	appendStringInfo(&str, "select datalake_acquire_sample_rows(%u::oid, %d, '%s'::boolean, '%s'::text);",
					 RelationGetRelid(relation),
					 perseg_targrows,
					 inh ? "t" : "f",
					 encodedFragment);
	pfree(encodedFragment);

	/*
	 * Step2: Execute the constructed SQL.
	 *
	 * Do not use SPI here, because there might be a large number of wide rows
	 * returned and stored in memory, SPI cannot spill data to disk which may
	 * lead to OOM easily.
	 *
	 * Do not use SPI cusror either, because we should use SPI_cursor_fetch to fetch
	 * results in batches, which may have bad performance.
	 *
	 * Use ExecutorStart|ExecutorRun|ExecutorEnd to execute a plan and store results
	 * into tuplestore could handle this situation well.
	 *
	 * Execute the given query and store the results into portal->holdStore to
	 * avoid memory error.
	 */
	elog(elevel, "Executing SQL: %s", str.data);
	sql = str.data;
	/* Create a new portal to run the query in */
	portal = CreateNewPortal();
	/* Don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;
	/* use a tuplestore to store received tuples to avoid out of memory error */
	PortalCreateHoldStore(portal);
	queryDesc = build_querydesc(portal, sql);

	/* Call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* Run the plan  */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	/* Wait for completion of all qExec processes. */
	if (queryDesc->estate->dispatcherState
		&& queryDesc->estate->dispatcherState->primaryResults)
	{
		cdbdisp_checkDispatchResult(queryDesc->estate->dispatcherState, DISPATCH_WAIT_NONE);
	}

	ExecutorFinish(queryDesc);
	/*
	 * Step3: process results.
	 */
	sampleTuples = process_sample_rows(portal, queryDesc, relation, rows,
									targrows, totalrows, totaldeadrows);

	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);
	PortalDrop(portal, false);

	return sampleTuples;
}

/*
 * Collect sample rows from the result of query.
 *	 - Use all tuples in sample until target # of samples are collected.
 *	 - Subsequently, replace already-sampled tuples randomly.
 */
static void
analyze_row_processor(TupleTableSlot *slot, DataLakeAnalyzeState *astate)
{
	int			targrows = astate->targrows;
	int			pos;			/* array index to store tuple in */
	MemoryContext oldcontext;

	/* Always increment sample row counter. */
	astate->samplerows += 1;

	/*
	 * Determine the slot where this sample row should be stored.  Set pos to
	 * negative value to indicate the row should be skipped.
	 */
	if (astate->numrows < targrows)
	{
		/* First targrows rows are always included into the sample */
		pos = astate->numrows++;
	}
	else
	{
		/*
		 * Now we start replacing tuples in the sample until we reach the end
		 * of the relation.  Same algorithm as in acquire_sample_rows in
		 * analyze.c; see Jeff Vitter's paper.
		 */
		if (astate->rowstoskip < 0)
			astate->rowstoskip = reservoir_get_next_S(&astate->rstate, astate->samplerows, targrows);

		if (astate->rowstoskip <= 0)
		{
			/* Choose a random reservoir element to replace. */
			pos = (int) (targrows * sampler_random_fract(astate->rstate.randstate));
			Assert(pos >= 0 && pos < targrows);
			heap_freetuple(astate->rows[pos]);
		}
		else
		{
			/* Skip this tuple. */
			pos = -1;
		}

		astate->rowstoskip -= 1;
	}

	if (pos >= 0)
	{
		/*
		 * Create sample tuple from current result row, and store it in the
		 * position determined above.  The tuple has to be created in anl_cxt.
		 */
		oldcontext = MemoryContextSwitchTo(astate->anl_cxt);

		astate->rows[pos] = ExecCopySlotHeapTuple(slot);

		MemoryContextSwitchTo(oldcontext);
	}
}


/*
 * Acquire a random sample of rows from foreign table managed by datalake-fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
segmentAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targRows,
							  double *totalRows,
							  double *totalDeadRows)
{
	DataLakeAnalyzeState astate;
	ForeignScanState *state;
	TupleTableSlot *slot;
	int			total_fragments;

	/* Initialize workspace state */
	astate.rows = rows;
	astate.targrows = targRows;
	astate.numrows = 0;
	astate.samplerows = 0;
	astate.rowstoskip = -1;		/* -1 means not set yet */
	reservoir_init_selection_state(&astate.rstate, targRows);

	/* Remember ANALYZE context, and create a per-tuple temp context */
	astate.anl_cxt = CurrentMemoryContext;
	astate.temp_cxt = AllocSetContextCreate(CurrentMemoryContext,
											"datalake_fdw temporary data",
											ALLOCSET_SMALL_SIZES);

	state = dataLakeAnalyzeBeginScan(relation, &total_fragments);

	/*
	 * Scan rows from shuffled fragments using reservoir sampling.
	 *
	 * For accurate n_distinct estimates on high-cardinality columns (e.g.
	 * l_orderkey with millions of distinct values), we need samples that
	 * are spread across the ENTIRE table, not concentrated in the first
	 * few fragments.  Fragments are already shuffled in BeginScan, so
	 * scanning more rows means covering more distinct fragments.
	 *
	 * The scan_limit is proportional to the table size: scan up to 10%
	 * of the segment's rows, which is enough for stadistinct estimation
	 * even on very high cardinality columns.  Reservoir sampling in
	 * analyze_row_processor keeps exactly targRows rows regardless of
	 * how many we scan.
	 *
	 * Bounds:
	 *   - Minimum 100K rows (covers most dimension tables entirely)
	 *   - Maximum 10M rows (bounds ANALYZE time to ~10s per segment)
	 */
	{
		double	scan_limit;

		/*
		 * Scan enough rows to cover all fragments and get accurate
		 * n_distinct estimates.  The key is coverage: with shuffled
		 * fragments, scanning more rows means covering more fragments
		 * and more of the value range for high-cardinality columns.
		 *
		 * Scan up to 1M rows per fragment, capped at 10M total.
		 * Reservoir sampling in analyze_row_processor only keeps
		 * targRows rows in memory regardless of how many we scan,
		 * so memory usage is bounded.  The 10M cap bounds I/O time
		 * to ~10s per segment even for heavily partitioned tables.
		 *
		 * For very large tables, the per-fragment shuffling ensures
		 * we see diverse data even if we stop early.
		 */
		scan_limit = (double) total_fragments * 1000000;
		if (scan_limit > 10000000.0)
			scan_limit = 10000000.0;

		elog(DEBUG5, "datalake_fdw ANALYZE: scan_limit=%.0f, total_fragments=%d, targRows=%d, "
			 "latestIcebergRecordCount=%.0f",
			 scan_limit, total_fragments, targRows, latestIcebergRecordCount);

		for (;;)
		{
			CHECK_FOR_INTERRUPTS();

			slot = dataLakeAnalyzeScanNext(state);
			if (TupIsNull(slot))
				break;

			analyze_row_processor(slot, &astate);

			if (astate.samplerows >= scan_limit)
				break;
		}
	}

	dataLakeAnalyzeEndScan(state);

	/* We assume that we have no dead tuple. */
	*totalDeadRows = 0.0;

	/*
	 * Estimate total row count.  samplerows is the number of rows scanned
	 * (which may be much larger than targRows due to our extended scanning),
	 * while numrows is the number of rows kept in the reservoir (at most
	 * targRows).
	 */
	*totalRows = astate.samplerows;

	ereport(elevel,
			(errmsg("\"%s\": scanned %.0f rows across %d fragments, %d rows in sample",
					RelationGetRelationName(relation),
					astate.samplerows, total_fragments, astate.numrows)));

	return astate.numrows;
}


static int
dataLakeAcquireSampleRowsFunc(Relation relation, int elevel,
							  HeapTuple *rows, int targRows,
							  double *totalRows,
							  double *totalDeadRows)
{
	/*
	 * On the coordinator (DISPATCH or UTILITY mode), dispatch the sample
	 * collection to segments.  GP_ROLE_UTILITY occurs when analyze_rel is
	 * called internally (e.g. from datalake_acquire_sample_rows which runs
	 * in utility mode on the coordinator).
	 */
	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
	{
		int numrows;

		/* Fetch sample from the segments. */
		numrows = acquire_sample_rows_dispatcher(relation, false, elevel,
												 rows, targRows,
												 totalRows, totalDeadRows);

		/*
		 * For Iceberg tables, override totalRows with the precise record
		 * count from Iceberg snapshot metadata. The per-segment sample-based
		 * estimate is wildly inaccurate because each segment only reports
		 * the rows it sampled, not the table total.
		 */
		if (latestIcebergRecordCount > 0)
			*totalRows = latestIcebergRecordCount;

		return numrows;
	}

	return segmentAcquireSampleRowsFunc(relation, elevel, rows,
										targRows, totalRows, totalDeadRows);
}

/*
 * dataLakeAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
dataLakeAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalPages)
{
	int64_t totalSize = 0;
	dataLakeOptions *options;
	IcebergTableStatistics *statistics = NULL;

	/* Return the row-analysis function pointer */
	*func = dataLakeAcquireSampleRowsFunc;
	int segmentcount = DATALAKE_SEGMENT_COUNT;
	options = datalakeGetOptions(RelationGetRelid(relation));

	if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY)
	{
		latestFragmentData = datalakeGetExternalFragmentList(relation, NULL, options, &totalSize);
		/* master does not process any fragments */
		List *random_segments = datalakeSelectRandomSegments(segmentcount, external_table_limit_segment_num);
		/* put the random segments into the list */
		latestFragmentData = list_concat(latestFragmentData, random_segments);
		if (options->format == DL_ICEBERG_TABLE)
		{
			statistics = datalakeGetTableStatistics(RelationGetRelid(relation), options);
			totalSize = statistics->bytesInDataFile;
			latestIcebergRecordCount = (double) statistics->recordCount;
		}
		else
		{
			latestIcebergRecordCount = 0;
		}
		if (statistics) {
			pfree(statistics);
		}
	}

	/*
	 * Convert size to pages.  Must return at least 1 so that we can tell
	 * later on that pg_class.relpages is not default.
	 */
	*totalPages = (totalSize + (BLCKSZ - 1)) / BLCKSZ;
	if (*totalPages < 1)
		*totalPages = 1;

	return true;
}


/*
 * Handle CREATE FOREIGN CATALOG statement
 */
static void
handle_create_foreign_catalog(CreateForeignCatalogStmt *createCatalogStmt)
{
	if (skip_create_polaris_catalog)
		return;

	/* Delegate to iceberg catalog fdw implementation */
	iceberg_catalog_create_catalog(createCatalogStmt);
}
