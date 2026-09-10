/*
 * iceberg_volume_fdw.c
 *		  Foreign-data wrapper for Iceberg volume
 *
 */

#include "postgres.h"
#include <jansson.h>
#include "access/formatter.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/detoast.h"
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
#include "nodes/value.h"
#include "utils/json.h"
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
#include "cdb/cdbdispatchresult.h"
#include "src/common/fileMetadata.h"
#include "src/datalake_def.h"
#include "src/common/fdwFunction.h"
#include "src/common/fdwFunction.h"
#include "iceberg_volume_fdw.h"
#include "iceberg_volume_option.h"
#include "src/datalake_option.h"
#include "src/dlproxy/headers.h"
#include "src/dlproxy/protocol.h"
#include "src/dlproxy/iceberg_common.h"
#include "src/common/random_segment.h"
#include "src/datalake_fragment.h"


#define ICEBERGVOLUME_SEGMENT_ID                 GpIdentity.segindex
#define ICEBERGVOLUME_SEGMENT_COUNT              getgpsegmentCount()
#define ICEBERGVOLUME_EXEC_FLAG_VECTOR 0x8000

typedef struct icebergVolumeFdwPlanState
{
	List	   *retrieved_attrs;
	Bitmapset  *attrs_used;
}icebergVolumeFdwPlanState;

extern int external_table_limit_segment_num;

/*
* SQL functions
*/
extern Datum iceberg_volume_fdw_handler(PG_FUNCTION_ARGS);
extern Datum iceberg_volume_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(iceberg_volume_fdw_handler);
PG_FUNCTION_INFO_V1(iceberg_volume_fdw_validator);


// help function
static char* normalizePath(const char* path);
static void
parseVolumeUri(IcebergVolumeOptions* vopt, dataLakeOptions *opt, icebergTableInfo info);
static void
parseVolumeOption(dataLakeOptions *opt, IcebergVolumeOptions* vopt);
static void
parseCatalogPropertiesOption(dataLakeOptions *opt, const char *catalog_properties,
							 IcebergVolumeOptions *vopt);
static void
setGopherEndpoint(dataLakeOptions *opt, const char *endpoint);
static void
setGopherNamenode(dataLakeOptions *opt, const char *namenodes,
				  const char *port_text);
static const char *
extractCatalogProperties(List *fdw_private);

static List *GetVolumeExternalFragmentList(char* agentClientJsonRespond);

static dataLakeOptions*
getVolumeOptions(icebergTableInfo info);


// fdw function
static void
icebergVolumeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void
icebergVolumeGetForeignPaths(PlannerInfo *root,
                             RelOptInfo *baserel,
                             Oid foreigntableid);
#if PG_VERSION_NUM >= 90500
static ForeignScan *
icebergVolumeGetForeignPlan(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid,
                            ForeignPath *best_path,
                            List *tlist,
                            List *scan_clauses,
                            Plan *outer_plan);
#else
static ForeignScan *
icebergVolumeGetForeignPlan(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid,
                            ForeignPath *best_path,
                            List *tlist,	/* target list */
                            List *scan_clauses);
#endif

static void
icebergVolumeBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *
icebergVolumeIterateForeignScan(ForeignScanState *node);

static void
icebergVolumeEndForeignScan(ForeignScanState *node);

static void
icebergVolumeReScanForeignScan(ForeignScanState *node);

static void
icebergVolumeBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags);

static TupleTableSlot *
icebergVolumeExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot);

static void
icebergVolumeEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static void
icebergVolumeEndForeignModify(EState *estate,
							  ResultRelInfo *resultRelInfo);

static int
icebergVolumeIsForeignRelUpdatable(Relation rel);

static TupleTableSlot*
icebergVolumeExecForeignUpdate(EState *estate,
							   ResultRelInfo *rinfo,
							   TupleTableSlot *slot,
							   TupleTableSlot *planSlot);

static TupleTableSlot*
icebergVolumeExecForeignDelete(EState *estate,
							   ResultRelInfo *rinfo,
							   TupleTableSlot *slot,
							   TupleTableSlot *planSlot);

/*
* iceberg_volume_fdw_handler
*      FDW handler function
*/
Datum
iceberg_volume_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);

    /* master - only */
	fdw_routine->GetForeignRelSize = icebergVolumeGetForeignRelSize;
	fdw_routine->GetForeignPaths = icebergVolumeGetForeignPaths;
	fdw_routine->GetForeignPlan = icebergVolumeGetForeignPlan;

	// scan operate
    fdw_routine->BeginForeignScan = icebergVolumeBeginForeignScan;
	fdw_routine->IterateForeignScan = icebergVolumeIterateForeignScan;
	fdw_routine->ReScanForeignScan = icebergVolumeReScanForeignScan;
	fdw_routine->EndForeignScan = icebergVolumeEndForeignScan;

	// insert operate
	fdw_routine->BeginForeignModify = icebergVolumeBeginForeignModify;
	fdw_routine->ExecForeignInsert = icebergVolumeExecForeignInsert;

	fdw_routine->EndForeignModify = icebergVolumeEndForeignModify;

	// update/delete operate
	fdw_routine->IsForeignRelUpdatable = icebergVolumeIsForeignRelUpdatable;
	fdw_routine->ExecForeignUpdate = icebergVolumeExecForeignUpdate;
	fdw_routine->ExecForeignDelete = icebergVolumeExecForeignDelete;

    PG_RETURN_POINTER(fdw_routine);
}

/*
* iceberg_volume_fdw_validator
*      Validate options given to the FDW
*/
Datum
iceberg_volume_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}

static void
icebergVolumeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    icebergVolumeFdwPlanState* fdw_private = (icebergVolumeFdwPlanState *) palloc0(sizeof(icebergVolumeFdwPlanState));
	baserel->fdw_private = fdw_private;
	set_baserel_size_estimates(root, baserel);
}

static void
icebergVolumeGetForeignPaths(PlannerInfo *root,
                             RelOptInfo *baserel,
                             Oid foreigntableid)
{
	ForeignPath *path = NULL;
	int			total_cost = 50000;
	Relation	rel;
	icebergVolumeFdwPlanState *fdw_private = (icebergVolumeFdwPlanState*)baserel->fdw_private;

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



/*
 * GetForeignPlan
 *		create a ForeignScan plan node
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
icebergVolumeGetForeignPlan(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid,
                            ForeignPath *best_path,
                            List *tlist,
                            List *scan_clauses,
                            Plan *outer_plan)
#else
static ForeignScan *
icebergVolumeGetForeignPlan(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid,
                            ForeignPath *best_path,
                            List *tlist,	/* target list */
                            List *scan_clauses)
#endif
{
	icebergVolumeFdwPlanState *fdw_private = (icebergVolumeFdwPlanState*)baserel->fdw_private;

	Index		scan_relid = baserel->relid;


	scan_clauses = extract_actual_clauses(scan_clauses, false);


	List* private_lists = list_make2(makeString("scan"), fdw_private->retrieved_attrs);
	/* Planner prefers physical tlist (the targetlist containing all Vars in order) for ForiegnScan,
	 * so as to allow self-defined optimization. So we need build tlist by path to extract the Vars we
	 * actually need.
	 */
	List *fdwtlist = fdwfunction_build_path_tlist(root, &best_path->path);

	return make_foreignscan(
							fdwtlist,
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

/* Helper functions for common BeginForeignScan */
static dataLakeOptions* iceberg_volume_get_options(void* context)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)context;
	dataLakeOptions* opts = getVolumeOptions(vState->iceTable);

	/* Time travel: hand the snapshot's field-ids down to the reader. */
	opts->iceberg_snapshot_field_ids = vState->snapshot_field_ids;
	opts->n_iceberg_snapshot_field_ids = vState->n_snapshot_field_ids;
	return opts;
}

static List* iceberg_volume_get_fragment_data(void* context)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)context;
	return GetVolumeExternalFragmentList((char*)vState->iceTable.agentCliRespond);
}

/*
 * rejectStandaloneVolumeScan
 *		Refuse to run when the Iceberg access method has not prepared our state.
 *
 * iceberg_volume_fdw is internal to the Iceberg AM: the scan state is built by
 * iceberg_create_foreign_scan_state() before the AM invokes our callbacks.  A
 * NULL state therefore means the plain executor reached us, which only happens
 * when a foreign or external table was bound to an iceberg_volume_fdw server.
 * That can be a user writing CREATE FOREIGN TABLE ... SERVER <volume server>
 * directly -- the validator accepts anything -- or, before the wrapper filter
 * in find_foreign_server(), a gphdfs:// external table whose namenode happened
 * to match a volume server.
 *
 * This has to be an ERROR rather than a quiet return: returning would let the
 * table be read through the wrong wrapper, turning a crash into wrong results.
 * (The early return in icebergVolumeReScanForeignScan() is a different case --
 * there a NULL state legitimately means the QD, or a segment that received no
 * fragments.)
 */
static void
rejectStandaloneVolumeScan(const void *vState)
{
	if (unlikely(vState == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg_volume_fdw cannot back a foreign or external table"),
				 errdetail("This foreign data wrapper is internal to the Iceberg access "
						   "method and requires scan state prepared by it."),
				 errhint("Use a datalake_fdw server for external tables, or an ICEBERG "
						 "TABLE for Iceberg data.")));
}

static void
icebergVolumeBeginForeignScan(ForeignScanState *node, int eflags)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)node->fdw_state;

	rejectStandaloneVolumeScan(vState);

	/*
	 * Always try to extract catalog_properties from fdw_private.
	 * Even when a volume is configured, we need table-location from
	 * catalog_properties to determine the correct data path (e.g.,
	 * Polaris manages its own table location which may differ from
	 * the volume-derived path).
	 */
	{
		ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
		vState->iceTable.catalog_properties =
			extractCatalogProperties(foreignScan->fdw_private);
	}

	DatalakeFdwBeginScanConfig config = {
		.get_options_func = iceberg_volume_get_options,
		.get_fragment_data_func = iceberg_volume_get_fragment_data,
		.context = (void*)vState
	};

	datalakefdw_begin_foreign_scan(node, eflags, &config);
}

static TupleTableSlot *
icebergVolumeIterateForeignScan(ForeignScanState *node)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		return NULL;
	}
	return datalakefdw_iterate_foreign_scan(node);
}

static void
icebergVolumeEndForeignScan(ForeignScanState *node)
{
	datalakefdw_end_foreign_scan(node);
}

/*
 * ReScanForeignScan
 *		Restart the scan from the beginning (e.g. iceberg scan as a NestLoop
 *		inner).  The Iceberg native CustomScan delegates here via
 *		pg_iceberg_rescan(); before this callback existed the slot was NULL and
 *		the indirect call crashed the segment.
 *
 *	icebergVolumeBeginForeignScan() (via datalakefdw_begin_foreign_scan) has
 *	already built a dataLakeFdwScanState with the deserialized fragments and an
 *	initialised provider.  We just rewind that provider's row reader (it keeps
 *	its own task list / delete index), so no re-listing or re-dispatch occurs.
 *
 *	NB: do NOT reuse datalake_fdw's end+init rescan path here — its
 *	fdwfunction_endScanStatus() pfree()s dataLakesstate->options, which the
 *	subsequent re-init would then read (use-after-free).
 */
static void
icebergVolumeReScanForeignScan(ForeignScanState *node)
{
	dataLakeFdwScanState *dataLakesstate;

	if (node->fdw_state == NULL)
		return;

	dataLakesstate = (dataLakeFdwScanState *) node->fdw_state;

	/*
	 * The provider is only initialised on segments that received fragments;
	 * on the dispatcher (or a segment with no selected partition) there is no
	 * reader to rewind.
	 */
	if (Gp_role == GP_ROLE_DISPATCH || dataLakesstate->provider == NULL)
		return;

	reScanProvider(dataLakesstate->provider);
}

List *GetVolumeExternalFragmentList(char* agentClientJsonRespond)
{
    List *result = parseIcebergFragmentResponse(agentClientJsonRespond, strlen(agentClientJsonRespond));
    return result;
}

static dataLakeOptions*
getVolumeOptions(icebergTableInfo info)
{
	DLProt protocol;
	dataLakeOptions *opt = (dataLakeOptions*)palloc0(sizeof(dataLakeOptions));
	opt->gopher = (gopherOptions*)palloc0(sizeof(gopherOptions));
	opt->hiveOption = (hiveOptions*)palloc0(sizeof(hiveOptions));

	char connect_path[1024] = {0};
	DatalakeGetGopherSocketPath(connect_path);
	opt->gopher->connect_path = pstrdup(connect_path);

	char connect_plasma_path[1024] = {0};
	DatalakeGetGopherPlasmaSocketPath(connect_plasma_path);
	opt->gopher->connect_plasma_path = pstrdup(connect_plasma_path);

	char connect_worker_path[1024] = {0};
	DatalakeGetGopherMetaPath(connect_worker_path);
	opt->gopher->worker_path = pstrdup(connect_worker_path);

	opt->fileSizeLimit = 128 * 1024 * 1024;

	protocol = DL_OSS_PROTOCOL_S3A;
	opt->protocol = protocol;
	opt->format = DL_ICEBERG_TABLE;

	/*
	 * Native iceberg AM parquet write compression. The AM layer supplies the
	 * per-table codec (default "zstd") and an optional level via icebergTableInfo;
	 * the non-AM volume path leaves info.compression NULL and keeps the palloc0
	 * default (UNCOMPRESS). Validate at this point (write begin) and fail with an
	 * ERROR rather than silently writing uncompressed data.
	 */
	opt->compressLevel = info.compression_level;

	/*
	 * Iceberg PARTITION BY columns.  Copy into the write options so the fanout
	 * writer (iceberg_write.cpp) can resolve the partition attnos from the
	 * relation tuple descriptor and route rows per partition.  NULL/empty is
	 * the unpartitioned case and leaves the writer on its single-stream path.
	 */
	if (info.partition_by != NULL && info.partition_by[0] != '\0')
		opt->partition_by = pstrdup(info.partition_by);

	if (info.compression != NULL && info.compression[0] != '\0')
	{
		CompressType ct = datalakeGetCompression(info.compression);

		if (ct == UNSUPPORTCOMPRESS || !PARQUET_SUPPORT_COMPRESS(ct))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid iceberg write compression \"%s\"", info.compression),
					 errhint("supported parquet codecs are uncompress, snappy, gzip, zstd, lz4")));

		opt->compress = ct;

		/* A compression level only applies to codecs that support one. */
		if (info.compression_level >= 0)
		{
			if (ct == ZSTD && (info.compression_level < 1 || info.compression_level > 22))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("zstd compression level %d out of range", info.compression_level),
						 errhint("valid zstd levels are 1..22")));
			else if (ct == GZIP && (info.compression_level < 1 || info.compression_level > 9))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("gzip compression level %d out of range", info.compression_level),
						 errhint("valid gzip levels are 1..9")));
			else if (ct != ZSTD && ct != GZIP)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("compression codec \"%s\" does not support a compression level",
								info.compression)));
		}
	}

	/*
	 * When catalog_properties contains table-location, use it as the
	 * authoritative data path. This ensures the write/read path matches
	 * the catalog's table location (e.g., Polaris manages its own location
	 * which may differ from the volume-derived path).
	 */
	if (info.location == NULL && info.catalog_properties != NULL)
	{
		json_t *root = json_loads(info.catalog_properties, 0, NULL);
		if (root)
		{
			json_t *loc = json_object_get(root, "table-location");
			if (loc && json_is_string(loc))
				info.location = pstrdup(json_string_value(loc));
			json_decref(root);
		}
	}

	if (info.volumn_server_name != NULL && info.volumn_name != NULL)
	{
		/* Volume specified — use volume for S3 connection options */
		IcebergVolumeOptions* vopt = getIcebergVolumeOptions(info.volumn_server_name, info.volumn_name);

		if (protocol == DL_HDFS_PROTOCOL)
		{
			//TODO:
		}
		else if (protocol == DL_FTP_PROTOCOL)
		{
			//TODO:
		}
		else
		{
			parseVolumeOption(opt, vopt);

			/*
			 * A volume may carry only `server_name`, leaving endpoint /
			 * credentials / bucket in the s3.conf section that section names.
			 * Only the agent reads conf files, so those keys reach us solely
			 * through the load-table response that the AM stashed in
			 * fdw_private.  Fill whatever the SQL OPTIONS left NULL from
			 * there; SQL still wins per key, matching the merge the agent
			 * performs on the metadata plane.  Before this, the volume branch
			 * ignored catalog_properties outright and a conf-only volume could
			 * be created but never read or written (issue #410).
			 */
			if (info.catalog_properties != NULL)
				parseCatalogPropertiesOption(opt, info.catalog_properties, vopt);

			parseVolumeUri(vopt, opt, info);

			/*
			 * Refuse to hand an incomplete connection to gopher.  A gopher
			 * config with no address does not merely fail this query: the
			 * segment's gopher store exits at startup and its socket then
			 * refuses every later connection until the cluster is restarted.
			 * Fail here instead, naming the volume.
			 *
			 * Which field carries the address depends on the volume type, and
			 * checking the wrong one rejects a working volume.  An HDFS volume
			 * puts it in hdfs_namenode_host, derived from `hdfs_namenodes`,
			 * and leaves gopher->host NULL -- gopher->host is written only
			 * from the `endpoint` option, which HDFS volumes need not carry.
			 */
			if (opt->gopher->gopherType != NULL &&
				pg_strcasecmp(opt->gopher->gopherType, "hdfs") == 0)
			{
				if (opt->gopher->hdfs_namenode_host == NULL ||
					opt->gopher->hdfs_namenode_host[0] == '\0')
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("iceberg volume \"%s.%s\" resolved no namenode",
									info.volumn_server_name, info.volumn_name),
							 errhint("Set hdfs_namenodes in the volume SERVER OPTIONS, or make sure the \"%s\" section of gphdfs.conf is readable by the datalake agent.",
									 vopt->volume_server.server_name != NULL
									 ? vopt->volume_server.server_name : "server_name")));
			}
			else if (opt->gopher->host == NULL || opt->gopher->host[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("iceberg volume \"%s.%s\" resolved no endpoint",
								info.volumn_server_name, info.volumn_name),
						 errhint("Set the endpoint in the volume SERVER OPTIONS, or make sure the \"%s\" section of s3.conf is readable by the datalake agent.",
								 vopt->volume_server.server_name != NULL
								 ? vopt->volume_server.server_name : "server_name")));
		}
	}
	else if (info.catalog_properties != NULL)
	{
		/* No volume — construct S3 options from catalog_properties */
		if (info.location == NULL)
			elog(ERROR, "iceberg_volume_fdw: catalog_properties missing table-location");
		/* No volume behind this call, so no SQL OPTION can be overridden. */
		parseCatalogPropertiesOption(opt, info.catalog_properties, NULL);
		parseVolumeUri(NULL, opt, info);
	}
	else
	{
		elog(ERROR, "iceberg_volume_fdw: neither volume nor catalog_properties provided");
	}

	return opt;
}

/*
 * Parse and validate an HDFS namenode port from a user-supplied option
 * string.  Returns the port on success and raises an error for an empty,
 * non-numeric, or out-of-range value.  The FDW validator
 * (iceberg_volume_fdw_validator) is a no-op, so without this a typo such as
 * "hdfs_port '8020x'" or a trailing ':' in hdfs_namenodes would silently
 * become port 0 and surface later as a cryptic gopher connection error
 * instead of an actionable message at option-parse time.
 */
static int
parseHdfsNamenodePort(const char *port_str)
{
	char	   *endptr;
	long		port;

	if (port_str == NULL || *port_str == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hdfs namenode port: missing port number")));

	errno = 0;
	port = strtol(port_str, &endptr, 10);
	if (errno != 0 || *endptr != '\0' || port <= 0 || port > 65535)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hdfs namenode port: \"%s\"", port_str)));

	return (int) port;
}

/*
 * setGopherNamenode
 *
 * Split an `hdfs_namenodes` value into the host and port gopher wants.  A
 * colon means host:port; in HA mode the value is a bare nameservice name and
 * the port comes from `hdfs_port`, defaulting to the HDFS RPC port.
 *
 * Shared by the volume options and the catalog properties so an HDFS volume
 * resolves the same namenode whichever source described it.
 */
static void
setGopherNamenode(dataLakeOptions *opt, const char *namenodes,
				  const char *port_text)
{
	char *dup;
	char *colon;

	if (namenodes == NULL || *namenodes == '\0')
		return;

	dup = pstrdup(namenodes);
	colon = strrchr(dup, ':');

	if (colon != NULL)
	{
		*colon = '\0';
		opt->gopher->hdfs_namenode_host = pstrdup(dup);
		opt->gopher->hdfs_namenode_port = parseHdfsNamenodePort(colon + 1);
	}
	else
	{
		opt->gopher->hdfs_namenode_host = pstrdup(dup);
		opt->gopher->hdfs_namenode_port = (port_text != NULL)
			? parseHdfsNamenodePort(port_text) : 8020;
	}

	pfree(dup);
}

/*
 * setGopherEndpoint
 *
 * Record an endpoint the way gopher wants it: host with no scheme, TLS use
 * carried separately in useHttps.  The native S3 backend rebuilds
 * scheme://host inside createHandle() and the gopher backend strips a scheme
 * itself, so the bare host suits both.
 *
 * Both sources of an endpoint go through here.  While the catalog path stored
 * its value verbatim, an "http://host:port" coming from the catalog reached
 * gopher as a hostname, which resolves nowhere -- and being a connection
 * failure rather than a configuration one, it surfaced as minutes inside the
 * liboss2 retry loop rather than as an error.
 */
static void
setGopherEndpoint(dataLakeOptions *opt, const char *endpoint)
{
	if (endpoint == NULL)
		return;

	if (pg_strncasecmp(endpoint, "https://", 8) == 0)
	{
		opt->gopher->useHttps = true;
		endpoint += 8;
	}
	else if (pg_strncasecmp(endpoint, "http://", 7) == 0)
	{
		opt->gopher->useHttps = false;
		endpoint += 7;
	}

	opt->gopher->host = pstrdup(endpoint);
}

static void
parseVolumeOption(dataLakeOptions *opt, IcebergVolumeOptions* vopt)
{
	if (vopt->volume_server.server_type)
	{
		opt->gopher->gopherType = pstrdup(vopt->volume_server.server_type);
		/*
		 * Native (non-Gopher) backends dispatch by protocol string via
		 * BackendRegistry; the volume's server_type ("s3"/"hdfs"/...) is
		 * the registered protocol name. Ignored by the Gopher backend,
		 * which keys off gopherType instead.
		 */
		opt->gopher->protocol = pstrdup(vopt->volume_server.server_type);
	}

	if (vopt->volume_server.region)
	{
		opt->gopher->region = pstrdup(vopt->volume_server.region);
	}

	if (vopt->volume_server.endpoint)
		setGopherEndpoint(opt, vopt->volume_server.endpoint);

	if (vopt->volume_server.bucket_name)
	{
		opt->gopher->bucket = pstrdup(vopt->volume_server.bucket_name);
	}

	/*
	 * An unset path_style_access must not be read as "virtual host": the
	 * other storage paths in this repo (datalake_option.c isvirtual,
	 * dfs-tablespace-ext) all leave useVirtualHost false when the user says
	 * nothing, and the private object stores this FDW targets resolve only
	 * path-style hosts -- a virtual-host default turns a missing option into
	 * CURLE_COULDNT_RESOLVE_HOST on "<bucket>.<endpoint-host>".  Only an
	 * explicit path_style_access 'false' asks for virtual-host addressing.
	 */
	if (vopt->volume_server.path_style_access_set)
	{
		opt->gopher->useVirtualHost = !vopt->volume_server.path_style_access;
	}
	else
	{
		opt->gopher->useVirtualHost = false;
	}

	if (vopt->volume_user.aws_access_key_id)
	{
		opt->gopher->accessKey = pstrdup(vopt->volume_user.aws_access_key_id);
	}

	if (vopt->volume_user.aws_secret_access_key)
	{
		opt->gopher->secretKey = pstrdup(vopt->volume_user.aws_secret_access_key);
	}

	/*
	 * HDFS volumes: the segment (QE) gopher writer reads the namenode from
	 * gopher->hdfs_namenode_host / gopher->hdfs_namenode_port.  Populate them
	 * from the FDW-style `hdfs_namenodes` option (host, host:port, or — in HA
	 * mode — the nameservice name), falling back to the legacy `endpoint`
	 * ("hdfs://host:port" or "host:port") for backward compatibility.  Without
	 * these set, datalakeCreateGopherConfig reports "Unrecognized hdfs name
	 * node" on the first INSERT.  The auth / HA knobs mirror the regular
	 * datalake_fdw HDFS server path (see datalake_option.c).
	 */
	if (vopt->volume_server.server_type != NULL &&
		pg_strcasecmp(vopt->volume_server.server_type, "hdfs") == 0)
	{
		const char *namenodes = vopt->volume_server.hdfs_namenodes;
		const char *endpoint = vopt->volume_server.endpoint;

		if (namenodes != NULL && *namenodes != '\0')
		{
			setGopherNamenode(opt, namenodes, vopt->volume_server.hdfs_port);
		}
		else if (endpoint != NULL && *endpoint != '\0')
		{
			const char *host_port = endpoint;
			const char *scheme_sep = strstr(endpoint, "://");
			char	   *dup;
			char	   *colon;

			if (scheme_sep != NULL)
				host_port = scheme_sep + 3;

			dup = pstrdup(host_port);
			colon = strchr(dup, ':');
			if (colon != NULL)
			{
				*colon = '\0';
				opt->gopher->hdfs_namenode_host = pstrdup(dup);
				opt->gopher->hdfs_namenode_port = parseHdfsNamenodePort(colon + 1);
			}
			else
			{
				opt->gopher->hdfs_namenode_host = pstrdup(dup);
				/* default HDFS RPC port */
				opt->gopher->hdfs_namenode_port = 8020;
			}
			pfree(dup);
		}

		/* auth / rpc knobs (all optional; default simple auth) */
		if (vopt->volume_server.hdfs_auth_method != NULL)
			opt->gopher->hdfs_auth_method =
				pstrdup(vopt->volume_server.hdfs_auth_method);
		else
			opt->gopher->hdfs_auth_method = pstrdup("simple");

		if (vopt->volume_server.hadoop_rpc_protection != NULL)
			opt->gopher->hadoop_rpc_protection =
				pstrdup(vopt->volume_server.hadoop_rpc_protection);
		if (vopt->volume_server.krb_principal != NULL)
			opt->gopher->krb_principal =
				pstrdup(vopt->volume_server.krb_principal);
		if (vopt->volume_server.krb_principal_keytab != NULL)
			opt->gopher->krb_principal_keytab =
				pstrdup(vopt->volume_server.krb_principal_keytab);
		if (vopt->volume_server.data_transfer_protocol != NULL)
			opt->gopher->data_transfer_protocol =
				(pg_strcasecmp(vopt->volume_server.data_transfer_protocol,
							   "true") == 0);

		/*
		 * Reach the datanodes by hostname rather than by the address the
		 * namenode reports, which is what a containerised or NATed cluster
		 * needs.  The option was accepted and parsed all along, and then went
		 * nowhere: gopherOptions had no field for it.
		 */
		if (vopt->volume_server.dfs_client_use_datanode_hostname != NULL)
			opt->gopher->dfs_client_use_datanode_hostname =
				(pg_strcasecmp(vopt->volume_server.dfs_client_use_datanode_hostname,
							   "true") == 0);

		/* HA volumes: wire the nameservice / failover config for gopher. */
		if (vopt->volume_server.is_ha_supported != NULL &&
			pg_strcasecmp(vopt->volume_server.is_ha_supported, "true") == 0)
		{
			opt->gopher->is_ha_supported = true;
			if (vopt->volume_server.dfs_nameservices != NULL)
				opt->gopher->dfs_name_services =
					pstrdup(vopt->volume_server.dfs_nameservices);
			if (vopt->volume_server.dfs_ha_namenodes != NULL)
				opt->gopher->dfs_ha_namenodes =
					pstrdup(vopt->volume_server.dfs_ha_namenodes);
			if (vopt->volume_server.dfs_namenode_rpc_address != NULL)
				opt->gopher->dfs_ha_namenode_rpc_addr =
					pstrdup(vopt->volume_server.dfs_namenode_rpc_address);
			if (vopt->volume_server.dfs_client_failover_proxy_provider != NULL)
				opt->gopher->dfs_client_failover =
					pstrdup(vopt->volume_server.dfs_client_failover_proxy_provider);
			if (vopt->volume_server.krb_service_principal != NULL)
				opt->gopher->krb_service_principal =
					pstrdup(vopt->volume_server.krb_service_principal);
		}
	}
}

/*
 * parseCatalogPropertiesOption
 *     Parse Polaris catalog_properties JSON to fill dataLakeOptions.
 *
 * Expected JSON format:
 * {
 *   "config": {
 *     "s3.endpoint": "http://host:port",
 *     "s3.path-style-access": "true",
 *     "client.region": "us-east-1"
 *   },
 *   "storage-credentials": [{
 *     "prefix": "s3://bucket/warehouse/...",
 *     "config": {
 *       "s3.access-key-id": "***",
 *       "s3.secret-access-key": "***"
 *     }
 *   }]
 * }
 *
 * vopt distinguishes the two callers:
 *
 *   NULL     -- no volume is configured, so this JSON is the sole source of
 *               the connection settings.  Every field is written and the
 *               required ones are validated here.
 *   non-NULL -- a volume supplied whatever the user wrote in SQL OPTIONS, and
 *               the remainder is expected to come from the `server_name` conf
 *               section that only the agent can read.  Fill gaps only, so SQL
 *               OPTIONS keep winning per key exactly as they do on the agent
 *               side, and leave validation to the caller, which can name the
 *               volume in the error.
 *
 * The volume is passed rather than a "gap-fill" flag because the flag is not
 * enough: for the boolean settings, whether SQL wrote anything can only be
 * answered from the volume's own options, and by the time this runs
 * opt->gopher holds a bool that cannot say "unset".  See FILL_BOOL_GAP.
 */
static void
parseCatalogPropertiesOption(dataLakeOptions *opt, const char *catalog_properties,
							 IcebergVolumeOptions *vopt)
{
/*
 * Fill one gopher field from the catalog's config object, honouring the
 * gap-fill rule: in gap-fill mode the SQL OPTIONS already spoke for any field
 * that is set, so only a NULL one may be written.
 */
#define FILL_STRING_GAP(json_key, field) \
	do { \
		json_t *v_ = json_object_get(config, (json_key)); \
		if (v_ && json_is_string(v_) && \
			(!fill_gaps_only || opt->gopher->field == NULL)) \
			opt->gopher->field = pstrdup(json_string_value(v_)); \
	} while (0)

/*
 * The bool counterpart, which cannot decide the question the same way: false
 * is both the palloc0 default and a legitimate SQL answer, so "is this field
 * still a gap?" has nothing to read off opt->gopher.  `written` carries the
 * answer instead -- true when the volume's SQL OPTIONS spelled the option out
 * -- which restores the same rule the strings follow: SQL wins, the conf
 * section fills the rest.
 *
 * This used to be a plain no-op in gap-fill mode, which dropped the conf
 * file's value outright.  For is_ha_supported that was worse than losing a
 * knob: dfs_nameservices and friends arrive through FILL_STRING_GAP either
 * way, so gopher was handed a nameservice with HA switched off, and dialled
 * the nameservice name as a hostname on setGopherNamenode's default port --
 * an address that resolves nowhere.  data_transfer_protocol was silently lost
 * the same way, which a cluster requiring SASL on the data path refuses.
 */
#define FILL_BOOL_GAP(json_key, field, written) \
	do { \
		json_t *v_ = json_object_get(config, (json_key)); \
		if (v_ && json_is_string(v_) && !(fill_gaps_only && (written))) \
			opt->gopher->field = \
				(pg_strcasecmp(json_string_value(v_), "true") == 0); \
	} while (0)

	json_t		   *root;
	json_error_t	error;
	json_t		   *config;
	json_t		   *credentials;
	json_t		   *val;
	bool			fill_gaps_only = (vopt != NULL);

	/*
	 * Which booleans the volume's SQL OPTIONS wrote explicitly.  Recorded
	 * here because this is the last place that still knows: the volume spells
	 * them as strings (NULL when absent) plus path_style_access_set, while
	 * opt->gopher has only the bools parseVolumeOption() already resolved.
	 */
	bool			sql_wrote_path_style =
		(vopt != NULL && vopt->volume_server.path_style_access_set);
	bool			sql_wrote_is_ha =
		(vopt != NULL && vopt->volume_server.is_ha_supported != NULL);
	bool			sql_wrote_transfer_protocol =
		(vopt != NULL && vopt->volume_server.data_transfer_protocol != NULL);
	bool			sql_wrote_datanode_hostname =
		(vopt != NULL &&
		 vopt->volume_server.dfs_client_use_datanode_hostname != NULL);

	root = json_loads(catalog_properties, 0, &error);
	if (!root)
	{
		/*
		 * In gap-fill mode this JSON is a secondary source: the volume already
		 * supplied whatever the user wrote, and a catalog whose properties are
		 * not in the S3 shape this function reads -- or are absent altogether
		 * -- is a normal configuration, not an error.  Skip, and let the
		 * caller's completeness check name the volume if something is really
		 * missing.  With no volume at all (fill_gaps_only false) this is the
		 * sole source of the connection settings, so it must still raise.
		 */
		if (fill_gaps_only)
			return;
		elog(ERROR, "parseCatalogPropertiesOption: failed to parse JSON: %s", error.text);
	}

	PG_TRY();
	{
		/*
		 * S3 is the only object-storage backend reachable via Iceberg REST
		 * catalog responses today. Pass "s3" — iceberg-gopher accepts it as
		 * a first-class UFS type (alongside legacy "s3a").
		 */
		if (opt->gopher->gopherType == NULL)
			opt->gopher->gopherType = pstrdup("s3");

		/* Parse "config" object */
		config = json_object_get(root, "config");
		if (config && json_is_object(config))
		{
			/*
			 * HDFS keys keep their SQL OPTION spelling on the wire, so a
			 * value can be traced from the conf file through the agent's
			 * response to gopher without a rename in between.  Only the
			 * namenode is load-bearing -- without it
			 * datalakeCreateGopherConfig reports "Unrecognized hdfs name
			 * node" on the first write -- but the auth and HA knobs travel
			 * with it, because a kerberised or HA cluster is unreachable
			 * without them and they are equally absent from the SQL OPTIONS
			 * of a volume that names only a conf section.
			 */
			if (opt->gopher->hdfs_namenode_host == NULL ||
				!fill_gaps_only)
			{
				json_t *nn = json_object_get(config, "hdfs_namenodes");
				json_t *port = json_object_get(config, "hdfs_port");

				if (nn && json_is_string(nn))
					setGopherNamenode(opt, json_string_value(nn),
									  (port && json_is_string(port))
									  ? json_string_value(port) : NULL);
			}

			FILL_STRING_GAP("hdfs_auth_method", hdfs_auth_method);
			FILL_STRING_GAP("krb_principal", krb_principal);
			FILL_STRING_GAP("krb_principal_keytab", krb_principal_keytab);
			FILL_STRING_GAP("hadoop_rpc_protection", hadoop_rpc_protection);
			FILL_STRING_GAP("data_transfer_protection", data_transfer_protection);
			FILL_STRING_GAP("dfs_nameservices", dfs_name_services);
			FILL_STRING_GAP("dfs_ha_namenodes", dfs_ha_namenodes);
			FILL_STRING_GAP("dfs_namenode_rpc_address", dfs_ha_namenode_rpc_addr);
			FILL_STRING_GAP("dfs_client_failover_proxy_provider",
							dfs_client_failover);
			FILL_STRING_GAP("username", hdfs_user);
			FILL_BOOL_GAP("is_ha_supported", is_ha_supported,
						  sql_wrote_is_ha);
			FILL_BOOL_GAP("data_transfer_protocol", data_transfer_protocol,
						  sql_wrote_transfer_protocol);
			FILL_BOOL_GAP("dfs_client_use_datanode_hostname",
						  dfs_client_use_datanode_hostname,
						  sql_wrote_datanode_hostname);

			val = json_object_get(config, "s3.endpoint");
			if (val && json_is_string(val) &&
				(!fill_gaps_only || opt->gopher->host == NULL))
				setGopherEndpoint(opt, json_string_value(val));

			val = json_object_get(config, "client.region");
			if (val && json_is_string(val) &&
				(!fill_gaps_only || opt->gopher->region == NULL))
				opt->gopher->region = pstrdup(json_string_value(val));

			/*
			 * useVirtualHost is a plain bool, so by the time it reaches this
			 * function an unwritten volume option is indistinguishable from an
			 * explicit "false" -- parseVolumeOption() writes it either way.  A
			 * NULL test cannot decide this one, so it is decided by the
			 * volume's own path_style_access_set flag, like the HDFS booleans
			 * above.
			 *
			 * Gap-fill must not reverse a choice the user actually wrote: a
			 * volume with path_style_access = true against a path-style-only
			 * endpoint such as MinIO would start signing virtual-host requests
			 * the moment the catalog answered "s3.path-style-access": "false",
			 * and the connection would fail.  So the catalog speaks here only
			 * when SQL stayed silent.
			 *
			 * When the key is absent there is nothing to write: useVirtualHost
			 * already holds the right default -- false (path style) from the
			 * palloc0 in authoritative mode, matching parseVolumeOption's
			 * unset-option default, or whatever parseVolumeOption resolved in
			 * gap-fill mode.
			 */
			val = json_object_get(config, "s3.path-style-access");
			if (val && json_is_string(val) &&
				!(fill_gaps_only && sql_wrote_path_style))
				opt->gopher->useVirtualHost =
					(pg_strcasecmp(json_string_value(val), "true") != 0);
		}

		/* Parse "storage-credentials" array */
		credentials = json_object_get(root, "storage-credentials");
		if (credentials && json_is_array(credentials) && json_array_size(credentials) > 0)
		{
			json_t *first_cred = json_array_get(credentials, 0);

			/* Extract bucket from prefix URI, e.g. "s3://bucket/..." */
			val = json_object_get(first_cred, "prefix");
			if (val && json_is_string(val) &&
				(!fill_gaps_only || opt->gopher->bucket == NULL))
			{
				const char *prefix = json_string_value(val);
				const char *scheme_end = strstr(prefix, "://");
				if (scheme_end)
				{
					const char *after_scheme = scheme_end + 3;
					const char *slash = strchr(after_scheme, '/');
					if (slash)
						opt->gopher->bucket = pnstrdup(after_scheme, slash - after_scheme);
					else
						opt->gopher->bucket = pstrdup(after_scheme);
				}
			}

			/* Extract credentials from nested config */
			json_t *cred_config = json_object_get(first_cred, "config");
			if (cred_config && json_is_object(cred_config))
			{
				val = json_object_get(cred_config, "s3.access-key-id");
				if (val && json_is_string(val) &&
					(!fill_gaps_only || opt->gopher->accessKey == NULL))
					opt->gopher->accessKey = pstrdup(json_string_value(val));

				val = json_object_get(cred_config, "s3.secret-access-key");
				if (val && json_is_string(val) &&
					(!fill_gaps_only || opt->gopher->secretKey == NULL))
					opt->gopher->secretKey = pstrdup(json_string_value(val));
			}
		}

		/* Fallback: extract bucket from table-location if not set by storage-credentials */
		if (opt->gopher->bucket == NULL)
		{
			json_t *table_loc = json_object_get(root, "table-location");
			if (table_loc && json_is_string(table_loc))
			{
				const char *loc_uri = json_string_value(table_loc);
				const char *scheme_end = strstr(loc_uri, "://");
				if (scheme_end)
				{
					const char *after_scheme = scheme_end + 3;
					const char *slash = strchr(after_scheme, '/');
					if (slash)
						opt->gopher->bucket = pnstrdup(after_scheme, slash - after_scheme);
					else
						opt->gopher->bucket = pstrdup(after_scheme);
				}
			}
		}

		/*
		 * Validate required fields.  Skipped in gap-fill mode: there the
		 * caller knows the volume name and can say something more useful than
		 * "catalog properties".
		 */
		if (!fill_gaps_only)
		{
			if (opt->gopher->host == NULL)
				elog(ERROR, "parseCatalogPropertiesOption: missing s3.endpoint in catalog properties");
			if (opt->gopher->bucket == NULL)
				elog(ERROR, "parseCatalogPropertiesOption: could not determine bucket from catalog properties");
		}
	}
	PG_CATCH();
	{
		json_decref(root);
		PG_RE_THROW();
	}
	PG_END_TRY();

	json_decref(root);

#undef FILL_STRING_GAP
#undef FILL_BOOL_GAP
}

/*
 * extractCatalogProperties
 *     Extract catalog_properties JSON string from fdw_private list.
 *
 * The catalog_properties is a String node appended by the AM layer
 * after fragment data and before random segment Integer nodes.
 * We find it by scanning backwards from the end, skipping Integer
 * nodes (random segments), and checking if the first String node
 * looks like a catalog_properties JSON.
 *
 * The JSON always contains a "config" key and optionally
 * "storage-credentials".  We check for either marker.
 */
static const char *
extractCatalogProperties(List *fdw_private)
{
	int			len;
	int			i;

	if (fdw_private == NIL)
		return NULL;

	len = list_length(fdw_private);

	/*
	 * Walk backwards: skip trailing Integer nodes (random segments), then
	 * check the node just before them.
	 *
	 */
	for (i = len - 1; i >= 2; i--)
	{
		Node *node = (Node *) list_nth(fdw_private, i);

		if (!IsA(node, Integer))
		{
			/* First non-Integer node from the end */
			if (IsA(node, String))
			{
				const char *str = strVal(node);

				/*
				 * catalog_properties always has "config" and may have
				 * "storage-credentials" and "table-location".
				 * Check for any of these markers.
				 */
				if (str &&
					(strstr(str, "storage-credentials") != NULL ||
					 strstr(str, "\"config\"") != NULL ||
					 strstr(str, "table-location") != NULL))
					return str;
			}
			break;
		}
	}

	return NULL;
}

static char*
normalizePath(const char* path)
{
	if (!path)
		return NULL;

	char* normalized = pstrdup(path);
	char* src = normalized;
	char* dst = normalized;

	while (*src)
	{
		if (*src == '/')
		{
			*dst++ = '/';
			while (*src == '/')
				src++;
		}
		else if (*src == ' ' || *src == '\t' || *src == '\n' || *src == '\r')
		{
			src++;
		}
		else
		{
			*dst++ = *src++;
		}
	}
	*dst = '\0';

	return normalized;
}

static void
parseVolumeUri(IcebergVolumeOptions* vopt, dataLakeOptions *opt, icebergTableInfo info)
{
	StringInfoData path;

	initStringInfo(&path);

	if (info.location != NULL)
	{
		/*
		 * info.location is an Iceberg table location URI, e.g.
		 *   "s3a://bucket/warehouse/ns/table"
		 *
		 * filePath must be path-only (no protocol/bucket) because the
		 * protocol, bucket, and endpoint are provided separately via
		 * gopher options.  Extract the path component after "://bucket".
		 */
		const char *loc = info.location;
		const char *scheme_end = strstr(loc, "://");
		if (scheme_end != NULL)
		{
			const char *after_scheme = scheme_end + 3; /* skip "://" */
			const char *path_start = strchr(after_scheme, '/');
			if (path_start != NULL)
				appendStringInfo(&path, "%s/data", path_start);
			else
				appendStringInfo(&path, "/data");
		}
		else
		{
			/* No scheme found — treat the whole value as a path */
			appendStringInfo(&path, "/%s/data", loc);
		}
	}
	else
	{
		/*
		 * Legacy fallback: assemble path from volume base_path + namespace + tableName.
		 * Requires a valid vopt (local volume must be specified).
		 */
		if (vopt == NULL)
			elog(ERROR, "parseVolumeUri: location is NULL and no volume options provided");

		elog(DEBUG1, "parseVolumeUri: location not provided, using legacy fallback");

		if (vopt->foreign_volume.base_path)
			appendStringInfo(&path, "/%s", vopt->foreign_volume.base_path);

		if (info.basePath)
			appendStringInfo(&path, "/%s", info.basePath);

		if (info.icebergNamespace)
			appendStringInfo(&path, "/%s", info.icebergNamespace);
		else
			appendStringInfo(&path, "/public");

		if (info.tableName)
			appendStringInfo(&path, "/%s", info.tableName);

		appendStringInfo(&path, "/data");
	}

	elog(DEBUG1, "parseVolumeUri: raw path = %s", path.data);

	char *normalized = normalizePath(path.data);
	opt->filePath = normalized;

	elog(DEBUG1, "parseVolumeUri: normalized path = %s", normalized);

	pfree(path.data);
}

static void
icebergVolumeBeginForeignModify(ModifyTableState *mtstate,
					  ResultRelInfo *resultRelInfo,
					  List *fdw_private,
					  int subplan_index,
					  int eflags)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)resultRelInfo->ri_FdwState;

	/* Same reasoning as in icebergVolumeBeginForeignScan(). */
	rejectStandaloneVolumeScan(vState);

	/*
	 * Take catalog_properties from fdw_private when the planner put them
	 * there.  Only overwrite on a hit: on the modify path the iceberg AM has
	 * already placed the dispatched properties on this state, and fdw_private
	 * carries none, so assigning unconditionally would erase them.
	 */
	{
		const char *props = extractCatalogProperties(fdw_private);

		if (props != NULL)
			vState->iceTable.catalog_properties = props;
	}

	DatalakeFdwBeginScanConfig config = {
		.get_options_func = iceberg_volume_get_options,
		.context = (void*)vState
	};
	datalakefdw_begin_foreign_modify(mtstate, resultRelInfo, fdw_private, subplan_index, eflags, &config);
}

static TupleTableSlot *
icebergVolumeExecForeignInsert(EState *estate,
					 ResultRelInfo *resultRelInfo,
					 TupleTableSlot *slot,
					 TupleTableSlot *planSlot)
{
	return datalakefdw_exec_foreign_insert(estate, resultRelInfo, slot, planSlot);
}

static char*
get_file_foramt_name(FileFormat format)
{
	switch (format)
	{
		case PARQUET:
			return "parquet";
		case ORC:
			return "orc";
		case AVRO:
			return "avro";
		default:
			return "unknown";
	}
}

static char*
get_position_on_delete_string(FileContent type)
{
	switch (type)
	{
		case DATA:
			return "DATA_FILE";
		case POSITION_DELETES:
			return "POSITION_DELETE";
		case EQUALITY_DELETES:
			return "EQUALITY_DELETE";
		case DELTA_LOG:
			return "DELTA_LOG";
		default:
			return "unknown";
	}
}

static void
iceberg_volume_get_metadata(Relation relation, dataLakeFdwScanState *sstate, List *file_list, void* context)
{
	if (!file_list || list_length(file_list) == 0)
		return;

	StringInfoData json_buf;
	ListCell *lc;
	bool first = true;
	const char *key = (sstate->cmd == CMD_UPDATE || sstate->cmd == CMD_DELETE) ? "updateFragments" : "fragments";

	initStringInfo(&json_buf);

	// Build fragments JSON structure
	appendStringInfo(&json_buf, "{\"%s\":[", key);

	foreach(lc, file_list)
	{
		FileFragment *fragment = (FileFragment *) lfirst(lc);

		if (!first)
			appendStringInfo(&json_buf, ",");

		appendStringInfo(&json_buf, "{");
		appendStringInfo(&json_buf, "\"path\":\"%s\",", fragment->filePath);
		appendStringInfo(&json_buf, "\"format\":\"%s\",", get_file_foramt_name(fragment->format));
		appendStringInfo(&json_buf, "\"record_count\":%ld,", fragment->recordCount);
		appendStringInfo(&json_buf, "\"file_size_in_bytes\":%ld,", fragment->fileSize);
		appendStringInfo(&json_buf, "\"position_on_delete\":\"%s\"", get_position_on_delete_string(fragment->content));

		/*
		 * Partition tuple for this data file (identity values in spec order).
		 * A NULL list element serializes as JSON null; an empty/NIL list (an
		 * unpartitioned table) yields "partition_values":[].  The tracker
		 * parses this back and the agent uses it for withPartition().
		 */
		appendStringInfo(&json_buf, ",\"partition_values\":[");
		{
			ListCell   *pvlc;
			bool		pvfirst = true;

			foreach(pvlc, fragment->partitionValues)
			{
				Node	   *pv = (Node *) lfirst(pvlc);

				if (!pvfirst)
					appendStringInfoChar(&json_buf, ',');
				pvfirst = false;

				if (pv == NULL)
					appendStringInfoString(&json_buf, "null");
				else
					escape_json(&json_buf, strVal(pv));
			}
		}
		appendStringInfo(&json_buf, "]}");

		first = false;
	}

	appendStringInfo(&json_buf, "]}");

	// Store JSON in config via context parameter
	DatalakeFdwBeginScanConfig *config = (DatalakeFdwBeginScanConfig*)context;
	if (config)
		config->metadataList = pstrdup(json_buf.data);

	pfree(json_buf.data);
}

static void
icebergVolumeEndForeignModify(EState *estate,
							  ResultRelInfo *resultRelInfo)
{
	DatalakeFdwBeginScanConfig config = {
		.get_append_metadata_func = iceberg_volume_get_metadata,
		.context = &config
	};

	elog(LOG, "icebergVolumeEndForeignModify: Gp_role=%d, seg=%d",
		 Gp_role, GpIdentity.segindex);

	datalakefdw_end_foreign_modify(estate, resultRelInfo, &config);

	elog(LOG, "icebergVolumeEndForeignModify: after end_foreign_modify, metadataList %s NULL",
		 config.metadataList ? "is NOT" : "IS");

	if (config.metadataList == NULL)
	{
		resultRelInfo->ri_FdwState = NULL;
	}
	else
	{
		resultRelInfo->ri_FdwState = (void*)pstrdup(config.metadataList);
		elog(LOG, "icebergVolumeEndForeignModify: metadataList = %.200s", config.metadataList);
	}
}

static int
icebergVolumeIsForeignRelUpdatable(Relation rel)
{
	return datalake_is_foreign_rel_updatable(rel);
}

static TupleTableSlot*
icebergVolumeExecForeignUpdate(EState *estate,
							   ResultRelInfo *rinfo,
							   TupleTableSlot *slot,
							   TupleTableSlot *planSlot)
{
	return datalake_exec_foreign_update(estate, rinfo, slot, planSlot);
}

static TupleTableSlot*
icebergVolumeExecForeignDelete(EState *estate,
							   ResultRelInfo *rinfo,
							   TupleTableSlot *slot,
							   TupleTableSlot *planSlot)
{
	return datalake_exec_foreign_delete(estate, rinfo, slot, planSlot);
}