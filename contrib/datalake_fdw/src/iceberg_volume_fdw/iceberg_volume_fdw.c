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
parseCatalogPropertiesOption(dataLakeOptions *opt, const char *catalog_properties);
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
	return getVolumeOptions(vState->iceTable);
}

static List* iceberg_volume_get_fragment_data(void* context)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)context;
	return GetVolumeExternalFragmentList((char*)vState->iceTable.agentCliRespond);
}

static void
icebergVolumeBeginForeignScan(ForeignScanState *node, int eflags)
{
	icebergVolumeScanState* vState = (icebergVolumeScanState*)node->fdw_state;

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
			parseVolumeUri(vopt, opt, info);
		}
	}
	else if (info.catalog_properties != NULL)
	{
		/* No volume — construct S3 options from catalog_properties */
		if (info.location == NULL)
			elog(ERROR, "iceberg_volume_fdw: catalog_properties missing table-location");
		parseCatalogPropertiesOption(opt, info.catalog_properties);
		parseVolumeUri(NULL, opt, info);
	}
	else
	{
		elog(ERROR, "iceberg_volume_fdw: neither volume nor catalog_properties provided");
	}

	return opt;
}

static void
parseVolumeOption(dataLakeOptions *opt, IcebergVolumeOptions* vopt)
{
	if (vopt->volume_server.server_type)
	{
		opt->gopher->gopherType = pstrdup(vopt->volume_server.server_type);
	}

	if (vopt->volume_server.region)
	{
		opt->gopher->region = pstrdup(vopt->volume_server.region);
	}

	if (vopt->volume_server.endpoint)
	{
		opt->gopher->host = pstrdup(vopt->volume_server.endpoint);
	}

	if (vopt->volume_server.bucket_name)
	{
		opt->gopher->bucket = pstrdup(vopt->volume_server.bucket_name);
	}

	if (vopt->volume_server.path_style_access)
	{
		opt->gopher->useVirtualHost = false;
	}
	else
	{
		opt->gopher->useVirtualHost = true;
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
	 * HDFS volumes carry the namenode URL in the `endpoint` option.  The
	 * gopher / hdfs writer reads it from gopher->hdfs_namenode_host and
	 * gopher->hdfs_namenode_port; without these set, datalakeCreateGopherConfig
	 * dereferences NULL and crashes on the first INSERT.  Default
	 * hdfs_auth_method to "simple" so simple-auth HDFS clusters work without
	 * extra OPTIONS.  Endpoint may be either "hdfs://host:port" (with scheme)
	 * or just "host:port".
	 */
	if (vopt->volume_server.server_type != NULL &&
		pg_strcasecmp(vopt->volume_server.server_type, "hdfs") == 0)
	{
		const char *endpoint = vopt->volume_server.endpoint;
		if (endpoint != NULL && *endpoint != '\0')
		{
			const char *host_port = endpoint;
			const char *scheme_sep = strstr(endpoint, "://");
			if (scheme_sep != NULL)
				host_port = scheme_sep + 3;

			char *dup = pstrdup(host_port);
			char *colon = strchr(dup, ':');
			if (colon != NULL)
			{
				*colon = '\0';
				opt->gopher->hdfs_namenode_host = pstrdup(dup);
				opt->gopher->hdfs_namenode_port = atoi(colon + 1);
			}
			else
			{
				opt->gopher->hdfs_namenode_host = pstrdup(dup);
				/* default HDFS RPC port */
				opt->gopher->hdfs_namenode_port = 8020;
			}
			pfree(dup);
		}

		opt->gopher->hdfs_auth_method = pstrdup("simple");
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
 */
static void
parseCatalogPropertiesOption(dataLakeOptions *opt, const char *catalog_properties)
{
	json_t		   *root;
	json_error_t	error;
	json_t		   *config;
	json_t		   *credentials;
	json_t		   *val;

	root = json_loads(catalog_properties, 0, &error);
	if (!root)
		elog(ERROR, "parseCatalogPropertiesOption: failed to parse JSON: %s", error.text);

	PG_TRY();
	{
		/*
		 * S3 is the only object-storage backend reachable via Iceberg REST
		 * catalog responses today. Pass "s3" — iceberg-gopher accepts it as
		 * a first-class UFS type (alongside legacy "s3a").
		 */
		opt->gopher->gopherType = pstrdup("s3");

		/* Parse "config" object */
		config = json_object_get(root, "config");
		if (config && json_is_object(config))
		{
			val = json_object_get(config, "s3.endpoint");
			if (val && json_is_string(val))
				opt->gopher->host = pstrdup(json_string_value(val));

			val = json_object_get(config, "client.region");
			if (val && json_is_string(val))
				opt->gopher->region = pstrdup(json_string_value(val));

			val = json_object_get(config, "s3.path-style-access");
			if (val && json_is_string(val))
				opt->gopher->useVirtualHost =
					(pg_strcasecmp(json_string_value(val), "true") != 0);
			else
				opt->gopher->useVirtualHost = true;
		}

		/* Parse "storage-credentials" array */
		credentials = json_object_get(root, "storage-credentials");
		if (credentials && json_is_array(credentials) && json_array_size(credentials) > 0)
		{
			json_t *first_cred = json_array_get(credentials, 0);

			/* Extract bucket from prefix URI, e.g. "s3://bucket/..." */
			val = json_object_get(first_cred, "prefix");
			if (val && json_is_string(val))
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
				if (val && json_is_string(val))
					opt->gopher->accessKey = pstrdup(json_string_value(val));

				val = json_object_get(cred_config, "s3.secret-access-key");
				if (val && json_is_string(val))
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

		/* Validate required fields */
		if (opt->gopher->host == NULL)
			elog(ERROR, "parseCatalogPropertiesOption: missing s3.endpoint in catalog properties");
		if (opt->gopher->bucket == NULL)
			elog(ERROR, "parseCatalogPropertiesOption: could not determine bucket from catalog properties");
	}
	PG_CATCH();
	{
		json_decref(root);
		PG_RE_THROW();
	}
	PG_END_TRY();

	json_decref(root);
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
	 * Walk backwards: skip trailing Integer nodes (random segments),
	 * then check the node just before them.
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

	/*
	 * Always try to extract catalog_properties from fdw_private.
	 * Even with a volume, we need table-location for the correct data path.
	 */
	vState->iceTable.catalog_properties =
		extractCatalogProperties(fdw_private);

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
		appendStringInfo(&json_buf, "}");

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