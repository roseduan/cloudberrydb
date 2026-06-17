/*-------------------------------------------------------------------------
 *
 * pg_iceberg_av_consumer.c
 *    Autovacuum-driven consumer for pg_ext_aux.pg_iceberg_deletion_queue.
 *
 * Design summary
 * --------------
 *
 *   AutoVacWorkerMain
 *     ├─ do_autovacuum()
 *     └─ AutoVacWorkerPostHook(MyDatabaseId)    -- our entry point
 *
 *   datalake_av_consume(datid)
 *     ├─ guards: GUC enabled, role==DISPATCH, min_interval, extension installed
 *     ├─ fetch a bounded batch via pg_iceberg_deletion_queue_get_batch
 *     └─ for each entry:
 *         ├─ resolve server + volume + user_mapping (fallback PUBLIC)
 *         ├─ build fileIOConfig JSON (s3 / hdfs / abfss dispatch)
 *         ├─ POST /v1/files/cleanup-from-metadata via libcurl wrapper
 *         ├─ success: _remove(path)
 *         ├─ retry_count+1 <  max_retry: _record_failure(path, errmsg)
 *         └─ retry_count+1 >= max_retry: _move_to_failed(path, errmsg)
 *
 * The DROP TABLE side (pg_iceberg_ddl.c OAT_DROP) only enqueues -- it
 * never tries to call dlagent synchronously.  Once enqueued, this hook is
 * the sole code path that actually deletes physical S3/HDFS objects.
 *
 * IDENTIFICATION
 *    contrib/datalake_fdw/src/am_iceberg/pg_iceberg_av_consumer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

#include "../components/agent_cli/c_interface/agent_cjson_builder.hpp"
#include "../common/agent_cli_wrapper.h"

#include "include/pg_iceberg_av_consumer.h"
#include "include/pg_iceberg_deletion_queue.h"

/* GUC state */
static bool deletion_queue_enabled = true;
static int  deletion_queue_batch_size = 100;
static int  deletion_queue_max_retry = 5;
static int  deletion_queue_min_interval = 60;

/* Hook-chaining state */
static AutoVacWorkerPostHook_type prev_av_hook = NULL;

/* Per-DB throttle (process-local; resets when worker process forks fresh) */
static TimestampTz last_run_ts = 0;

/* External GUC defined by datalake_fdw.c */
extern char *datalake_agent_server_url;

/* Forward declarations */
static void datalake_av_consume(Oid datid);
static void do_delete_for_entry(DeletionQueueEntry *e);
static char *build_fileio_config_json(ForeignServer *server,
                                      Oid volume_oid,
                                      UserMapping *um);
static UserMapping *resolve_user_mapping_for(Oid server_oid,
                                             const char *owner_username);
static char *extract_option(List *options, const char *name);
static void  put_string_if_present(agentcli_cJSON *cfg, List *opts,
                                   const char *opt_name,
                                   const char *json_key);


/* ================================================================
 * Init: GUCs + hook registration
 * ================================================================
 */
void
pg_iceberg_av_consumer_init(void)
{
    DefineCustomBoolVariable(
        "datalake_fdw.deletion_queue_enabled",
        "Enable the autovacuum-driven iceberg deletion queue consumer.",
        NULL,
        &deletion_queue_enabled,
        true,
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "datalake_fdw.deletion_queue_batch_size",
        "Maximum number of queue entries processed per autovacuum cycle.",
        NULL,
        &deletion_queue_batch_size,
        100,
        1, 10000,
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "datalake_fdw.deletion_queue_max_retry",
        "Move an entry to pg_iceberg_deletion_failed after this many failures.",
        NULL,
        &deletion_queue_max_retry,
        5,
        0, 100,
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL);

    DefineCustomIntVariable(
        "datalake_fdw.deletion_queue_min_interval",
        "Minimum seconds between consumer runs per autovacuum worker process.",
        NULL,
        &deletion_queue_min_interval,
        60,
        1, 86400,
        PGC_SIGHUP,
        GUC_UNIT_S,
        NULL, NULL, NULL);

    /* Chain ourselves into the hook (preserve any prior callback). */
    prev_av_hook = AutoVacWorkerPostHook;
    AutoVacWorkerPostHook = datalake_av_consume;
}


/* ================================================================
 * Hook body
 * ================================================================
 */
static void
datalake_av_consume(Oid datid)
{
    TimestampTz now;
    List       *batch = NIL;
    ListCell   *lc;

    /* Cheap gates first (avoid any catalog work when disabled). */
    if (!deletion_queue_enabled)
        goto chain;
    if (Gp_role != GP_ROLE_DISPATCH)
        goto chain;

    now = GetCurrentTimestamp();
    if (last_run_ts != 0 &&
        !TimestampDifferenceExceeds(last_run_ts, now,
                                    (int64) deletion_queue_min_interval * 1000))
        goto chain;
    last_run_ts = now;

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    /* If datalake_fdw is not installed in this DB, there is nothing to do. */
    if (get_extension_oid("datalake_fdw", /*missing_ok*/ true) == InvalidOid)
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        goto chain;
    }

    /* SPI-backed batch fetch.  May raise; let it propagate. */
    batch = pg_iceberg_deletion_queue_get_batch(deletion_queue_batch_size,
                                                deletion_queue_max_retry);

    foreach(lc, batch)
    {
        DeletionQueueEntry *e = (DeletionQueueEntry *) lfirst(lc);
        volatile int32 attempt_after = e->retry_count + 1;

        /*
         * Isolate each entry in its own PG_TRY so one malformed row cannot
         * abort the whole batch.  On success, remove from the queue; on
         * failure, either bump retry_count or push to the DLQ depending on
         * whether attempt_after has reached max_retry.
         */
        PG_TRY();
        {
            do_delete_for_entry(e);
            pg_iceberg_deletion_queue_remove(e->path);
        }
        PG_CATCH();
        {
            ErrorData *edata;

            MemoryContextSwitchTo(TopTransactionContext);
            edata = CopyErrorData();
            FlushErrorState();

            if (attempt_after >= deletion_queue_max_retry)
                pg_iceberg_deletion_queue_move_to_failed(e->path,
                                                         edata->message);
            else
                pg_iceberg_deletion_queue_record_failure(e->path,
                                                         edata->message);

            FreeErrorData(edata);
        }
        PG_END_TRY();
    }

    PopActiveSnapshot();
    CommitTransactionCommand();

chain:
    if (prev_av_hook)
        prev_av_hook(datid);
}


/* ================================================================
 * do_delete_for_entry: resolve credentials, build fileIOConfig, call dlagent
 * ================================================================
 */
static void
do_delete_for_entry(DeletionQueueEntry *e)
{
    Oid              server_oid;
    ForeignServer   *server;
    Oid              volume_oid;
    UserMapping     *um;
    char            *fileio_config;
    AgentCliHandle  *h;

    if (e->server_name == NULL || e->server_name[0] == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("deletion_queue entry \"%s\" has empty server_name",
                        e->path ? e->path : "(null)")));

    if (e->volume_name == NULL || e->volume_name[0] == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("deletion_queue entry \"%s\" has empty volume_name",
                        e->path)));

    /* 1. Foreign server */
    server_oid = get_foreign_server_oid(e->server_name, /*missing_ok*/ true);
    if (!OidIsValid(server_oid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("foreign server \"%s\" no longer exists "
                        "for deletion_queue entry \"%s\"",
                        e->server_name, e->path)));
    server = GetForeignServer(server_oid);

    /* 2. Foreign volume (sanity: it must still belong to this server) */
    volume_oid = get_foreign_volume_oid(e->volume_name, e->server_name,
                                        /*missing_ok*/ true);
    if (!OidIsValid(volume_oid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("foreign volume \"%s\" on server \"%s\" no longer exists "
                        "for deletion_queue entry \"%s\"",
                        e->volume_name, e->server_name, e->path)));

    /* 3. User mapping (DROP-time owner, fallback to PUBLIC) */
    um = resolve_user_mapping_for(server_oid, e->owner_username);
    if (um == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("no usable user mapping found for user \"%s\" "
                        "on server \"%s\" (deletion_queue entry \"%s\")",
                        e->owner_username ? e->owner_username : "(null)",
                        e->server_name, e->path)));

    /* 4. Build fileIOConfig JSON */
    fileio_config = build_fileio_config_json(server, volume_oid, um);

    /* 5. POST to dlagent's /v1/files/cleanup-from-metadata.
     *
     * agent_cli_wrapper_create signature requires non-NULL prefix /
     * namespace, but neither is used by the cleanup endpoint -- pass
     * stable sentinels so callers can grep for them in agent logs.
     */
    h = agent_cli_wrapper_create(datalake_agent_server_url,
                                 "iceberg",
                                 "cleanup-from-metadata");

    PG_TRY();
    {
        agentcli_cJSON *parsed;
        agentcli_cJSON *failed_arr;
        const char     *resp_body;

        if (e->deletion_type == DELETION_TYPE_FILE)
        {
            /* Delete this single object directly (e.g. VACUUM's old files). */
            agent_cli_wrapper_delete_file(h, e->path, fileio_config);
            agent_cli_wrapper_check_exec_error_json(h, "files/delete failed");
        }
        else
        {
            /* Parse the path as a metadata.json tree and delete all of it. */
            agent_cli_wrapper_cleanup_metadata(h, e->path, fileio_config);
            agent_cli_wrapper_check_exec_error_json(h,
                                                    "cleanup-from-metadata failed");
        }

        /*
         * dlagent returns 200 even on partial failure (per-file errors are
         * reported in the "failed" array).  Treat any non-empty failed
         * array as an ERROR so the outer PG_CATCH does record_failure or
         * move_to_failed.
         */
        resp_body = agent_cli_wrapper_get_response(h);
        if (resp_body && resp_body[0] != '\0')
        {
            parsed = agentcli_cJSON_Parse(resp_body);
            if (parsed != NULL)
            {
                failed_arr = agentcli_cJSON_GetObjectItem(parsed, "failed");
                if (failed_arr != NULL &&
                    agentcli_cJSON_GetArraySize(failed_arr) > 0)
                {
                    char *details = agentcli_cJSON_PrintUnformatted(failed_arr);

                    agentcli_cJSON_Delete(parsed);
                    /* details is allocated by cJSON; copy into PG context */
                    {
                        char *details_pg = pstrdup(details ? details : "(no details)");

                        if (details)
                            free(details);
                        ereport(ERROR,
                                (errcode(ERRCODE_FDW_ERROR),
                                 errmsg("dlagent partial cleanup failure for \"%s\": %s",
                                        e->path, details_pg)));
                    }
                }
                agentcli_cJSON_Delete(parsed);
            }
        }
    }
    PG_FINALLY();
    {
        agent_cli_wrapper_destroy(h);
    }
    PG_END_TRY();

    pfree(fileio_config);
}


/* ================================================================
 * resolve_user_mapping_for: GetUserMapping with PUBLIC fallback (NoError)
 * ================================================================
 */
static UserMapping *
resolve_user_mapping_for(Oid server_oid, const char *owner_username)
{
    HeapTuple   tp = NULL;
    Oid         lookup_user = InvalidOid;
    UserMapping *um;
    Datum       umoptions_datum;
    bool        umoptions_isnull;
    Form_pg_user_mapping um_form;

    /* Try the DROP-time owner first. */
    if (owner_username && owner_username[0] != '\0')
    {
        Oid user_oid = get_role_oid(owner_username, /*missing_ok*/ true);

        if (OidIsValid(user_oid))
        {
            tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                                 ObjectIdGetDatum(user_oid),
                                 ObjectIdGetDatum(server_oid));
            if (HeapTupleIsValid(tp))
                lookup_user = user_oid;
        }
    }

    /* Fall back to PUBLIC mapping (umuser = InvalidOid). */
    if (!HeapTupleIsValid(tp))
    {
        tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                             ObjectIdGetDatum(InvalidOid),
                             ObjectIdGetDatum(server_oid));
        if (HeapTupleIsValid(tp))
            lookup_user = InvalidOid;
    }

    if (!HeapTupleIsValid(tp))
        return NULL;

    um = (UserMapping *) palloc0(sizeof(UserMapping));
    um_form = (Form_pg_user_mapping) GETSTRUCT(tp);
    um->umid     = um_form->oid;
    um->userid   = lookup_user;
    um->serverid = server_oid;

    umoptions_datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
                                      Anum_pg_user_mapping_umoptions,
                                      &umoptions_isnull);
    if (!umoptions_isnull)
        um->options = untransformRelOptions(umoptions_datum);
    else
        um->options = NIL;

    ReleaseSysCache(tp);
    return um;
}


/* ================================================================
 * build_fileio_config_json: serialize server.options + um.options into
 * the JSON shape dlagent expects (dispatch by server "type" field).
 * ================================================================
 */
static char *
extract_option(List *options, const char *name)
{
    ListCell *lc;

    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, name) == 0)
            return defGetString(def);
    }
    return NULL;
}

static void
put_string_if_present(agentcli_cJSON *cfg, List *opts,
                      const char *opt_name, const char *json_key)
{
    char *v = extract_option(opts, opt_name);
    if (v != NULL && v[0] != '\0')
        agentcli_cJSON_AddStringToObject(cfg, json_key, v);
}

static char *
build_fileio_config_json(ForeignServer *server,
                         Oid volume_oid,
                         UserMapping *um)
{
    agentcli_cJSON *cfg = agentcli_cJSON_CreateObject();
    char           *type;
    char           *json;

    type = extract_option(server->options, "type");
    if (type == NULL)
        type = "s3";    /* historical default */

    agentcli_cJSON_AddStringToObject(cfg, "type", type);

    if (strcmp(type, "s3") == 0)
    {
        /* Server-level S3 options */
        put_string_if_present(cfg, server->options, "endpoint",          "endpoint");
        put_string_if_present(cfg, server->options, "region",            "region");
        put_string_if_present(cfg, server->options, "bucket_name",       "bucket_name");
        put_string_if_present(cfg, server->options, "path_style_access", "path_style_access");
        /* User-mapping credentials */
        put_string_if_present(cfg, um->options, "access_key_id",     "access_key_id");
        put_string_if_present(cfg, um->options, "secret_access_key", "secret_access_key");
    }
    else if (strcmp(type, "hdfs") == 0)
    {
        put_string_if_present(cfg, server->options, "hdfs_namenodes",         "namenodes");
        put_string_if_present(cfg, server->options, "hdfs_auth_method",       "auth_method");
        put_string_if_present(cfg, server->options, "hadoop_rpc_protection",  "rpc_protection");
        put_string_if_present(cfg, server->options, "is_ha_supported",        "is_ha");
        put_string_if_present(cfg, server->options, "dfs_nameservices",       "nameservices");
        put_string_if_present(cfg, server->options, "dfs_ha_namenodes",       "ha_namenodes");
        put_string_if_present(cfg, server->options, "dfs_namenode_rpc_address",
                              "namenode_rpc_address");
        put_string_if_present(cfg, server->options, "dfs_client_failover_proxy_provider",
                              "failover_proxy_provider");

        put_string_if_present(cfg, um->options, "username",             "username");
        put_string_if_present(cfg, um->options, "krb_principal",        "krb_principal");
        put_string_if_present(cfg, um->options, "krb_principal_keytab", "krb_principal_keytab");
    }
    else if (strcmp(type, "abfss") == 0)
    {
        put_string_if_present(cfg, server->options, "tenant_id",            "tenant_id");
        put_string_if_present(cfg, server->options, "multi_tenant_app_name",
                              "multi_tenant_app_name");
        put_string_if_present(cfg, server->options, "consent_url",          "consent_url");
        put_string_if_present(cfg, server->options, "hierarchical",         "hierarchical");

        put_string_if_present(cfg, um->options, "username", "username");
    }
    else
    {
        agentcli_cJSON_Delete(cfg);
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("unsupported foreign server type \"%s\" "
                        "for deletion-queue consumer",
                        type)));
    }

    /* volume_oid is currently informational; future volume-level options
     * (base_path, enable_caching, allow_writes) could be projected here if
     * the agent needs them.  Suppress unused-warning. */
    (void) volume_oid;

    json = agentcli_cJSON_PrintUnformatted(cfg);
    agentcli_cJSON_Delete(cfg);

    if (json == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("failed to serialize fileIOConfig JSON")));

    /* Re-allocate in PG memory so we own the string. */
    {
        char *json_pg = pstrdup(json);
        free(json);
        return json_pg;
    }
}
