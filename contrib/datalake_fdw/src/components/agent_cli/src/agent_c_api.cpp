/*
 * agent_c_api.cpp
 *
 * Unified C API for the agent_cli component.
 * Combines the low-level C++ bridging (formerly agent_c_api.cpp) with the
 * PostgreSQL-integrated wrapper layer (formerly agent_cli_wrapper.c).
 *
 * The static functions (agent_cli_init_internal, etc.) bridge C++ AgentClient.
 * The public agent_cli_wrapper_* functions add PostgreSQL memory management
 * (palloc/pfree/elog) and are called directly by iceberg_catalog_fdw.c.
 */
/*
 * Include PostgreSQL headers first, then save and undef the macros that
 * collide with C++ identifiers (INFO, LOG, WARNING, ERROR, DEBUG1-5, etc.)
 * before including the agent_cli C++ headers.  We restore them at the end
 * of the C++ section so the wrapper code below can use elog().
 */
extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "utils/memutils.h"
#include "agent_cjson_builder.hpp"
#include "src/am_iceberg/include/pg_iceberg_guc.h"

/*
 * The transport accessor, defined in datalake_fdw.c.  Declared here rather
 * than by including datalake_def.h, which pulls in the whole FDW surface and
 * its PG macros into a translation unit that has to undef half of them again.
 */
extern int	datalake_agent_transport_get(void);
/* "host:port" for the gRPC agent; datalake_fdw.c owns the two GUCs behind it. */
extern const char *datalake_agent_grpc_target(void);
/* datalake.rpc_max_message_size in bytes; same ownership, same reason. */
extern int	datalake_agent_rpc_max_message_bytes_get(void);
#define DATALAKE_AGENT_TRANSPORT_REST	0
#define DATALAKE_AGENT_TRANSPORT_GRPC	1
}

/* Save PG log-level macros and undef so they don't break C++ enum definitions */
#pragma push_macro("LOG")
#pragma push_macro("INFO")
#pragma push_macro("WARNING")
#pragma push_macro("ERROR")
#pragma push_macro("DEBUG1")
#pragma push_macro("DEBUG2")
#pragma push_macro("DEBUG3")
#pragma push_macro("DEBUG4")
#pragma push_macro("DEBUG5")

/*
 * c.h also defines Max, Min and the gettext family as macros, and real
 * functions by those names are declared in headers grpcpp.h drags in.  A
 * two-argument macro meeting a one-argument declaration is a hard error:
 *
 *   absl/container/internal/layout.h:287:30: error: macro "Max" requires 2
 *                                            arguments, but only 1 given
 *   libintl.h:61:14: error: expected ')' before 'unsigned'   [ngettext]
 *
 * The reach is what makes this version-dependent.  grpc 1.59.5 has
 * credentials.h -> tls_crl_provider.h -> grpc/grpc_crl_provider.h ->
 * absl/status/statusor.h -> cord.h -> cord_rep_ring.h -> layout.h; 1.59.2
 * has no crl_provider header at all and never reaches abseil's containers.
 * So this file compiles on an image carrying 1.59.2 and fails on one
 * carrying 1.59.5, which is exactly how it reached CI: the image the build
 * job actually compiles in (centos-7:devel-cbdb-20260903) is ahead of the
 * development ones, and every image to hand locally still had 1.59.2.
 *
 * contrib/pax_storage/src/cpp/comm/cbdb_api.h undefs the same set for the
 * same reason -- it just does it permanently, where this file has to put
 * them back for the wrapper section below.
 */
#pragma push_macro("Max")
#pragma push_macro("Min")
#pragma push_macro("gettext")
#pragma push_macro("dgettext")
#pragma push_macro("ngettext")
#pragma push_macro("dngettext")
#undef LOG
#undef INFO
#undef WARNING
#undef ERROR
#undef DEBUG1
#undef DEBUG2
#undef DEBUG3
#undef DEBUG4
#undef DEBUG5
#undef Max
#undef Min
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext

#include <agent_c_api.h>
#include <agent_client.hpp>
#ifdef HAVE_GRPCXX
#include "grpc_agent_client.hpp"
#endif
#include <agent_types.hpp>

/* libc for the wrapper layer below; libcurl is no longer needed here, the
 * cleanup endpoints go through AgentClient like every other operation. */
#include <string.h>
#include <logger.hpp>
#include <memory>
#include <cstring>
#include <cstdlib>

using namespace agent_cli;

/* ----------------------------------------------------------------
 * Internal helpers (C++ → C bridge)
 * ----------------------------------------------------------------
 */
namespace {
    thread_local std::string last_error_message;

    char* duplicate_string(const char* str) {
        if (!str) return nullptr;
        size_t len = strlen(str);
        char* copy = static_cast<char*>(malloc(len + 1));
        if (copy) {
            strcpy(copy, str);
        }
        return copy;
    }

    /*
     * Free the strings the previous call left in a response.  A handle reuses
     * one response for every call it makes, so overwriting the pointers
     * leaked two malloc'd strings per agent call for the life of the handle.
     *
     * Safe because every string in a response is malloc'd here:
     * convert_response() and set_error_response() are its only writers (the
     * cleanup-from-metadata and delete wrappers that used to pstrdup into it
     * now go through the client), and callers copy what they keep -- they
     * pstrdup the body or parse it before the next call on the handle.
     */
    void release_response_strings(agent_cli_response_t* c_response) {
        free(c_response->response_body);
        c_response->response_body = nullptr;
        free(c_response->error_message);
        c_response->error_message = nullptr;
    }

    void convert_response(const Response& cpp_response, agent_cli_response_t* c_response) {
        if (!c_response) return;

        release_response_strings(c_response);
        c_response->http_status = cpp_response.http_status;
        c_response->curl_code = cpp_response.curl_code;
        c_response->response_body = duplicate_string(cpp_response.response_body.c_str());
        c_response->response_size = cpp_response.response_body.size();
        c_response->error_message = duplicate_string(cpp_response.error_message.c_str());
        c_response->total_time = cpp_response.total_time;
        c_response->retry_count = cpp_response.retry_count;
    }

    /*
     * The request never produced an HTTP response (cancelled, or the client
     * threw).  Clear whatever the previous call left in the response and
     * carry the reason in error_message, so callers that report from the
     * response do not print a stale or empty "HTTP Status: 0" error.
     */
    void set_error_response(agent_cli_response_t* c_response) {
        if (!c_response) return;

        release_response_strings(c_response);
        c_response->http_status = 0;
        c_response->curl_code = 0;
        c_response->response_size = 0;
        c_response->error_message = duplicate_string(last_error_message.c_str());
        c_response->total_time = 0;
        c_response->retry_count = 0;
    }

    agent_cli_status_t validate_params(const void* param, const char* name) {
        if (!param) {
            last_error_message = std::string(name) + " is null";
            return AGENT_CLI_ERROR_INVALID_PARAM;
        }
        return AGENT_CLI_SUCCESS;
    }

    static agent_cli_status_t errorCodeToCliStatus(ErrorCode code) {
        switch (code) {
            case ErrorCode::SUCCESS:        return AGENT_CLI_SUCCESS;
            case ErrorCode::INVALID_PARAM:  return AGENT_CLI_ERROR_INVALID_PARAM;
            case ErrorCode::NETWORK:        return AGENT_CLI_ERROR_NETWORK;
            case ErrorCode::JSON_PARSE:     return AGENT_CLI_ERROR_JSON_PARSE;
            case ErrorCode::HTTP:           return AGENT_CLI_ERROR_HTTP;
            case ErrorCode::MEMORY:         return AGENT_CLI_ERROR_MEMORY;
            case ErrorCode::CURL:           return AGENT_CLI_ERROR_CURL;
            case ErrorCode::TIMEOUT:        return AGENT_CLI_ERROR_TIMEOUT;
            case ErrorCode::THREAD:         return AGENT_CLI_ERROR_THREAD;
            case ErrorCode::AUTH:           return AGENT_CLI_ERROR_AUTH;
            case ErrorCode::NOT_FOUND:      return AGENT_CLI_ERROR_NOT_FOUND;
            case ErrorCode::CONFLICT:       return AGENT_CLI_ERROR_CONFLICT;
            case ErrorCode::BAD_REQUEST:    return AGENT_CLI_ERROR_BAD_REQUEST;
            default:                        return AGENT_CLI_ERROR_HTTP;
        }
    }
}

/* ----------------------------------------------------------------
 * Internal low-level C API (static, not exported)
 * ----------------------------------------------------------------
 */
extern "C" {

static agent_cli_status_t agent_cli_init_internal(const agent_cli_config_t* config, agent_cli_handle_t* handle) {
    if (auto status = validate_params(config, "config")) return status;
    if (auto status = validate_params(handle, "handle")) return status;
    if (auto status = validate_params(config->server_url, "server_url")) return status;

    if (strlen(config->server_url) == 0) {
        last_error_message = "server_url is empty";
        return AGENT_CLI_ERROR_INVALID_PARAM;
    }

    try {
        LogLevel log_level = LogLevel::INFO;
        if (config->log_level >= AGENT_CLI_LOG_OFF && config->log_level <= AGENT_CLI_LOG_DEBUG) {
            log_level = static_cast<LogLevel>(config->log_level);
        }
        Logger::instance().init("", log_level);

        LOG_INFO("Agent CLI initializing with server: " + std::string(config->server_url));

        Config cpp_config;
        cpp_config.server_url = config->server_url;
        cpp_config.grpc_target = config->grpc_target ? config->grpc_target : "";
        cpp_config.auth_token = config->auth_token ? config->auth_token : "";
        cpp_config.request_timeout_seconds = config->request_timeout_seconds > 0 ? config->request_timeout_seconds : 30;
        cpp_config.connect_timeout_seconds = config->connect_timeout_seconds > 0 ? config->connect_timeout_seconds : 10;
        cpp_config.max_retries = config->max_retries >= 0 ? config->max_retries : 1;
        cpp_config.retry_delay_ms = config->retry_delay_ms > 0 ? config->retry_delay_ms : 1000;

        AgentContext* context = new AgentContext();
        /*
         * THE TRANSPORT DISPATCH FOR THE CONTROL PLANE.
         *
         * Chosen once, here, where the handle is built.  Because AgentClient
         * is polymorphic, the operation macros below and every
         * agent_cli_wrapper_* signature -- and the 101 call sites above them
         * -- are untouched by the addition of gRPC.
         *
         * datalake.agent_transport is PGC_POSTMASTER, so it cannot change
         * under a live handle.
         */
#ifdef HAVE_GRPCXX
        if (datalake_agent_transport_get() == DATALAKE_AGENT_TRANSPORT_GRPC) {
            /*
             * Before the first channel exists, because channels are cached by
             * target and carry the args they were built with.  The agent gets
             * the same number on its command line from datalake_proxy, so the
             * sender's ceiling and the receiver's are one setting; see
             * dlagent_transport.h.
             */
            agent_cli::grpcx::set_max_message_size(
                datalake_agent_rpc_max_message_bytes_get());
            context->client.reset(new GrpcAgentClient(cpp_config));
            LOG_INFO("Agent CLI using the gRPC transport, target " + cpp_config.grpc_target);
        } else {
            context->client.reset(new AgentClient(cpp_config));
        }
#else
        /*
         * Without gRPC support a "grpc" setting has already been rejected,
         * by datalake_proxy's check hook or by agent_cli_wrapper_create.
         */
        context->client.reset(new AgentClient(cpp_config));
#endif

        *handle = context;
        LOG_INFO("Agent CLI initialized successfully");
        return AGENT_CLI_SUCCESS;
    } catch (const std::exception& e) {
        last_error_message = e.what();
        return AGENT_CLI_ERROR_MEMORY;
    } catch (...) {
        last_error_message = "Unknown error during initialization";
        return AGENT_CLI_ERROR_MEMORY;
    }
}

static void agent_cli_cleanup_internal(agent_cli_handle_t handle) {
    if (handle) {
        try {
            delete static_cast<AgentContext*>(handle);
        } catch (...) {}
    }
}

static void agent_cli_set_interrupt_callback_internal(agent_cli_handle_t handle, agent_cli_interrupt_callback_t callback) {
    if (!handle) return;
    try {
        AgentContext* ctx = static_cast<AgentContext*>(handle);
        if (callback) {
            ctx->client->set_interrupt_callback([callback]() -> bool { return callback(); });
        } else {
            ctx->client->set_interrupt_callback(nullptr);
        }
    } catch (...) {}
}

/*
 * Macro for the repetitive table/catalog operation pattern.
 * Each function: validate → call C++ client method → convert response → return status.
 */
#define AGENT_CLI_OP_WITH_TABLE(func_name, client_method) \
static agent_cli_status_t func_name(agent_cli_handle_t handle, \
                                    const char* table_name, \
                                    const char* request_json, \
                                    agent_cli_response_t* response) { \
    last_error_message.clear(); \
    if (auto status = validate_params(handle, "handle")) return status; \
    if (auto status = validate_params(response, "response")) return status; \
    if (auto status = validate_params(table_name, "table_name")) return status; \
    try { \
        auto* context = static_cast<AgentContext*>(handle); \
        if (!context->client) { \
            last_error_message = "Client not initialized"; \
            return AGENT_CLI_ERROR_INVALID_PARAM; \
        } \
        Response cpp_response = context->client->client_method(table_name, request_json); \
        convert_response(cpp_response, response); \
        return errorCodeToCliStatus(cpp_response.respond_last_status); \
    } catch (const CancelledException& e) { \
        last_error_message = e.what(); \
        set_error_response(response); \
        return AGENT_CLI_ERROR_CANCELLED; \
    } catch (const std::exception& e) { \
        last_error_message = e.what(); \
        set_error_response(response); \
        return AGENT_CLI_ERROR_NETWORK; \
    } catch (...) { \
        last_error_message = "Unknown error in " #client_method; \
        set_error_response(response); \
        return AGENT_CLI_ERROR_NETWORK; \
    } \
}

#define AGENT_CLI_OP_NO_TABLE(func_name, client_method) \
static agent_cli_status_t func_name(agent_cli_handle_t handle, \
                                    const char* request_json, \
                                    agent_cli_response_t* response) { \
    last_error_message.clear(); \
    if (auto status = validate_params(handle, "handle")) return status; \
    if (auto status = validate_params(request_json, "request_json")) return status; \
    if (auto status = validate_params(response, "response")) return status; \
    try { \
        auto* context = static_cast<AgentContext*>(handle); \
        if (!context->client) { \
            last_error_message = "Client not initialized"; \
            return AGENT_CLI_ERROR_INVALID_PARAM; \
        } \
        Response cpp_response = context->client->client_method(request_json); \
        convert_response(cpp_response, response); \
        return errorCodeToCliStatus(cpp_response.respond_last_status); \
    } catch (const CancelledException& e) { \
        last_error_message = e.what(); \
        set_error_response(response); \
        return AGENT_CLI_ERROR_CANCELLED; \
    } catch (const std::exception& e) { \
        last_error_message = e.what(); \
        set_error_response(response); \
        return AGENT_CLI_ERROR_NETWORK; \
    } catch (...) { \
        last_error_message = "Unknown error in " #client_method; \
        set_error_response(response); \
        return AGENT_CLI_ERROR_NETWORK; \
    } \
}

/* Table operations (internal) */
AGENT_CLI_OP_NO_TABLE(agent_cli_create_table_internal, create_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_load_table_internal, load_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_load_metadata_json_internal, load_metadata_json)
AGENT_CLI_OP_WITH_TABLE(agent_cli_table_exists_internal, table_exists)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_fragment_internal, get_fragment)
AGENT_CLI_OP_WITH_TABLE(agent_cli_plan_file_groups_internal, plan_file_groups)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_file_groups_internal, commit_file_groups)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_append_internal, commit_append)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_update_internal, commit_update)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_rewrite_internal, commit_rewrite)
AGENT_CLI_OP_WITH_TABLE(agent_cli_submit_maintenance_internal, submit_maintenance)
AGENT_CLI_OP_WITH_TABLE(agent_cli_append_table_internal, append_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_update_table_internal, update_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_truncate_table_internal, truncate_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_drop_table_internal, drop_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_statistics_internal, get_statistics)
AGENT_CLI_OP_WITH_TABLE(agent_cli_update_schema_internal, update_schema)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_snapshot_schema_internal, get_snapshot_schema)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_snapshots_internal, get_snapshots)

/* Catalog operations (internal) */
AGENT_CLI_OP_NO_TABLE(agent_cli_create_catalog_internal, create_catalog)
AGENT_CLI_OP_NO_TABLE(agent_cli_create_namespace_internal, create_namespace)
AGENT_CLI_OP_NO_TABLE(agent_cli_list_catalogs_internal, list_catalogs)
AGENT_CLI_OP_NO_TABLE(agent_cli_list_namespaces_internal, list_namespaces)

/* Restore PG log-level macros for the wrapper section (elog, etc.) */
#pragma pop_macro("LOG")
#pragma pop_macro("INFO")
#pragma pop_macro("WARNING")
#pragma pop_macro("ERROR")
#pragma pop_macro("DEBUG1")
#pragma pop_macro("DEBUG2")
#pragma pop_macro("DEBUG3")
#pragma pop_macro("DEBUG4")
#pragma pop_macro("DEBUG5")
#pragma pop_macro("Max")
#pragma pop_macro("Min")
#pragma pop_macro("gettext")
#pragma pop_macro("dgettext")
#pragma pop_macro("ngettext")
#pragma pop_macro("dngettext")

/* ----------------------------------------------------------------
 * Public PostgreSQL-integrated API (formerly agent_cli_wrapper.c)
 *
 * These functions use palloc/pfree for memory management and elog
 * for error reporting.  They delegate to the static *_internal
 * functions above.
 * ----------------------------------------------------------------
 */

static void
agent_cli_wrapper_init_config(const char* server_url, const char* prefix,
                              const char* namespace_name, agent_cli_config_t *config)
{
    config->server_url = pstrdup(server_url);
    config->namespace_name = pstrdup(namespace_name);
    config->auth_token = NULL;
    config->request_timeout_seconds = pg_iceberg_agent_request_timeout;
    config->connect_timeout_seconds = 60;
    config->max_retries = 3;
    config->retry_delay_ms = 1000;

    /* Map PostgreSQL log level to agent CLI log level */
    if (client_min_messages <= DEBUG1)
        config->log_level = AGENT_CLI_LOG_DEBUG;
    else if (client_min_messages <= INFO)
        config->log_level = AGENT_CLI_LOG_INFO;
    else if (client_min_messages <= WARNING)
        config->log_level = AGENT_CLI_LOG_WARN;
    else if (client_min_messages <= ERROR)
        config->log_level = AGENT_CLI_LOG_ERROR;
    else
        config->log_level = AGENT_CLI_LOG_OFF;
}

/*
 * The interrupt check every handle gets by default: pending, and safe to act
 * on right now.  Mirrors what CHECK_FOR_INTERRUPTS would do, without raising
 * from inside the C++ client.
 */
static bool
agent_cli_default_interrupt_check(void)
{
    if (!InterruptPending)
        return false;
    if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
        return false;
    /*
     * Only interrupts that ProcessInterrupts() turns into an error, the same
     * rule as iceberg_catalog_interrupt_callback: memory-context logging,
     * procsignal barriers and several timeouts also set InterruptPending, and
     * abandoning the request for those fails a statement nothing cancelled.
     */
    if (ProcDiePending)
        return true;
    return QueryCancelPending && QueryCancelHoldoffCount == 0;
}

AgentCliHandle*
agent_cli_wrapper_create(const char* server_url, const char* prefix, const char* namespace_name)
{
#ifndef HAVE_GRPCXX
    /*
     * Rejects datalake.agent_transport=grpc on a build without gRPC, which
     * datalake_proxy's check hook covers only where it is preloaded.  Here,
     * before any C++ object exists, because it raises an ERROR.
     */
    (void) datalake_agent_transport_get();
#endif
    AgentCliHandle *handle = (AgentCliHandle *)palloc0(sizeof(AgentCliHandle));
    handle->agentConfig = (agent_cli_config_t *)palloc0(sizeof(agent_cli_config_t));
    handle->currentResponse = (agent_cli_response_t *)palloc0(sizeof(agent_cli_response_t));

    agent_cli_wrapper_init_config(server_url, prefix, namespace_name, handle->agentConfig);

    /*
     * The gRPC target, resolved here so the C++ side never has to reinterpret
     * a REST URL.  Always set: the transport is chosen inside
     * agent_cli_init_internal, and computing this unconditionally keeps that
     * decision in one place.
     *
     * Leaving it null is not harmless: GrpcAgentClient falls back to
     * target_from_url(server_url), and that URL is the REST endpoint, so every
     * catalog operation under agent_transport=grpc dials the REST agent's port
     * and fails with "Connection refused" against a perfectly healthy agent.
     */
    handle->agentConfig->grpc_target = pstrdup(datalake_agent_grpc_target());

    agent_cli_status_t status = agent_cli_init_internal(handle->agentConfig, &handle->handle);
    if (status != AGENT_CLI_SUCCESS)
    {
        handle->lastStatus = status;
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(status));
        handle->isValid = false;
        elog(ERROR, "Failed to initialize agent CLI: status %d %s", status, agent_cli_get_error_string(status));
    }

    /*
     * Every handle is cancellable, not just the one the catalog FDW remembered
     * to arm.
     *
     * agent_cli_wrapper_set_interrupt_callback had exactly ONE call site
     * (initAgentCliHandle in iceberg_catalog_fdw.c) against six agent_cli_wrapper_create() call
     * sites, so the other five produced handles that could not be interrupted
     * at all: a Ctrl-C during one of those operations waited out the full
     * retry schedule.  Registering the default here fixes all of them at once,
     * and the callback is generic -- it only asks whether an interrupt is
     * pending and safe to service.  A caller that wants different behaviour
     * still overrides it.
     *
     * isValid MUST be set first.  agent_cli_wrapper_set_interrupt_callback
     * starts with `if (!handle || !handle->isValid) return;`, so arming the
     * callback on a handle that is not yet marked valid is a silent no-op,
     * leaving every handle except the one iceberg_catalog_fdw.c arms by hand
     * exactly as uninterruptible as before.  Measured on a live cluster: with
     * CREATE FOREIGN CATALOG stuck on an unresponsive catalog server,
     * InterruptPending=1, QueryCancelPending=1, InterruptHoldoffCount=0 and
     * the backend sitting in select() in http_client.cpp -- inside the
     * loop whose first statement is check_for_interrupts() -- because
     * HttpClient::interrupt_callback_ was still an empty std::function.
     * statement_timeout=300ms and pg_cancel_backend were both ignored for the
     * full 17 s the retry schedule takes.
     */
    handle->isValid = true;
    agent_cli_wrapper_set_interrupt_callback(handle, agent_cli_default_interrupt_check);

    handle->responseValid = false;
    return handle;
}

void
agent_cli_wrapper_destroy(AgentCliHandle* handle)
{
    if (!handle)
        return;
    if (handle->handle)
        agent_cli_cleanup_internal(handle->handle);
    /* The response's strings are malloc'd; the response itself is palloc'd. */
    if (handle->currentResponse)
        agent_cli_free_response(handle->currentResponse);
    pfree(handle);
}

void
agent_cli_wrapper_set_interrupt_callback(AgentCliHandle* handle, agent_cli_interrupt_callback_t callback)
{
    if (!handle || !handle->isValid)
        return;
    agent_cli_set_interrupt_callback_internal(handle->handle, callback);
}

/*
 * The message a failed operation should report.
 *
 * agent_cli_get_error_string() only names the status code ("Network error"),
 * which is useless when the failure happened before any request went out --
 * a null argument, or an exception thrown while composing the URL.  Those
 * paths do record what actually went wrong in last_error_message, and the
 * caller has no other way to see it, so prefer it whenever it is set.
 */
static const char *
agent_cli_failure_detail(agent_cli_status_t status)
{
    if (!last_error_message.empty())
        return last_error_message.c_str();
    return agent_cli_get_error_string(status);
}

/*
 * Macro for the repetitive wrapper pattern:
 *   validate handle → call internal → store status/error
 */
#define WRAPPER_TABLE_OP(wrapper_name, internal_name) \
void wrapper_name(AgentCliHandle* handle, const char* table_name, const char* json) \
{ \
    if (!handle || !handle->isValid) \
        elog(ERROR, "Invalid agent CLI handle"); \
    handle->lastStatus = internal_name(handle->handle, table_name, json, handle->currentResponse); \
    handle->responseValid = true; \
    if (handle->lastStatus == AGENT_CLI_ERROR_CANCELLED) \
        CHECK_FOR_INTERRUPTS(); \
    if (handle->lastStatus != AGENT_CLI_SUCCESS) \
        handle->lastErrorMessage = pstrdup(agent_cli_failure_detail(handle->lastStatus)); \
}

#define WRAPPER_NO_TABLE_OP(wrapper_name, internal_name) \
void wrapper_name(AgentCliHandle* handle, const char* json) \
{ \
    if (!handle || !handle->isValid) \
        elog(ERROR, "Invalid agent CLI handle"); \
    handle->lastStatus = internal_name(handle->handle, json, handle->currentResponse); \
    handle->responseValid = true; \
    if (handle->lastStatus == AGENT_CLI_ERROR_CANCELLED) \
        CHECK_FOR_INTERRUPTS(); \
    if (handle->lastStatus != AGENT_CLI_SUCCESS) \
        handle->lastErrorMessage = pstrdup(agent_cli_failure_detail(handle->lastStatus)); \
}

/* Table operations */
/* create_table is special: the internal function doesn't take table_name */
void agent_cli_wrapper_create_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");
    handle->lastStatus = agent_cli_create_table_internal(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;
    if (handle->lastStatus == AGENT_CLI_ERROR_CANCELLED)
        CHECK_FOR_INTERRUPTS();
    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

WRAPPER_TABLE_OP(agent_cli_wrapper_load_table, agent_cli_load_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_load_metadata_json, agent_cli_load_metadata_json_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_table_exists, agent_cli_table_exists_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_fragment, agent_cli_get_fragment_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_plan_file_groups, agent_cli_plan_file_groups_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_file_groups, agent_cli_commit_file_groups_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_append, agent_cli_commit_append_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_update, agent_cli_commit_update_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_rewrite, agent_cli_commit_rewrite_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_submit_maintenance, agent_cli_submit_maintenance_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_append_table, agent_cli_append_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_update_table, agent_cli_update_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_truncate_table, agent_cli_truncate_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_drop_table, agent_cli_drop_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_statistics, agent_cli_get_statistics_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_update_schema, agent_cli_update_schema_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_snapshot_schema, agent_cli_get_snapshot_schema_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_snapshots, agent_cli_get_snapshots_internal)

/* Catalog operations */
WRAPPER_NO_TABLE_OP(agent_cli_wrapper_create_catalog, agent_cli_create_catalog_internal)
WRAPPER_NO_TABLE_OP(agent_cli_wrapper_list_catalogs, agent_cli_list_catalogs_internal)
WRAPPER_NO_TABLE_OP(agent_cli_wrapper_list_namespaces, agent_cli_list_namespaces_internal)

/* Utility functions */
const char*
agent_cli_wrapper_get_response(AgentCliHandle* handle)
{
    if (!handle || !handle->responseValid)
        return NULL;
    return handle->currentResponse->response_body;
}

bool
agent_cli_wrapper_is_success(AgentCliHandle* handle)
{
    if (!handle)
        return false;
    return handle->lastStatus == AGENT_CLI_SUCCESS;
}

void
agent_cli_wrapper_check_error(AgentCliHandle* handle)
{
    if (!handle)
        elog(ERROR, "Invalid agent CLI handle");
    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        elog(ERROR, "Agent CLI operation failed: %s",
             handle->lastErrorMessage ? handle->lastErrorMessage : agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_check_exec_error(AgentCliHandle* handle, const char *error_prefix)
{
    StringInfoData errorBuf;

    if (!handle)
        elog(ERROR, "%s: Invalid agent CLI handle", error_prefix);

    /*
     * A cancelled operation is not an agent failure, and reporting it as one
     * loses what actually happened.  The interrupt that stopped the call is
     * still pending, so hand it to PostgreSQL and let it raise its own
     * "canceling statement due to statement timeout" / "due to user request"
     * -- the same message every other cancelled statement in the system
     * produces, with the right SQLSTATE (57014) for a client to recognise.
     *
     * Without this the user sees "Failed to create catalog: HTTP Status: 0,
     * CURL Code: 0, Error: Operation cancelled", which reads like the catalog
     * server did something, and carries XX000.
     */
    if (handle->lastStatus == AGENT_CLI_ERROR_CANCELLED)
        CHECK_FOR_INTERRUPTS();

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
    {
        initStringInfo(&errorBuf);
        appendStringInfo(&errorBuf, "%s: HTTP Status: %d, CURL Code: %ld",
                         error_prefix,
                         handle->currentResponse ? handle->currentResponse->http_status : 0,
                         handle->currentResponse ? handle->currentResponse->curl_code : 0);
        if (handle->lastErrorMessage)
            appendStringInfo(&errorBuf, ", Error: %s", handle->lastErrorMessage);
        if (handle->currentResponse && handle->currentResponse->response_body)
            appendStringInfo(&errorBuf, ", Response: %s", handle->currentResponse->response_body);
        elog(ERROR, "%s", errorBuf.data);
    }

    if (handle->currentResponse &&
        (handle->currentResponse->response_body == NULL || handle->currentResponse->response_body[0] == '\0'))
        elog(ERROR, "%s: No response body", error_prefix);
}

void
agent_cli_wrapper_check_exec_error_json(AgentCliHandle* handle, const char *error_prefix)
{
    StringInfoData errorBuf;
    agentcli_cJSON *json = NULL;
    agentcli_cJSON *error_obj = NULL;
    agentcli_cJSON *field = NULL;
    bool has_stack_trace = false;

    if (!handle)
        elog(ERROR, "%s: Invalid agent CLI handle", error_prefix);

    /*
     * A cancelled operation is not an agent failure, and reporting it as one
     * loses what actually happened.  The interrupt that stopped the call is
     * still pending, so hand it to PostgreSQL and let it raise its own
     * "canceling statement due to statement timeout" / "due to user request"
     * -- the same message every other cancelled statement in the system
     * produces, with the right SQLSTATE (57014) for a client to recognise.
     *
     * Without this the user sees "Failed to create catalog: HTTP Status: 0,
     * CURL Code: 0, Error: Operation cancelled", which reads like the catalog
     * server did something, and carries XX000.
     */
    if (handle->lastStatus == AGENT_CLI_ERROR_CANCELLED)
        CHECK_FOR_INTERRUPTS();

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
    {
        initStringInfo(&errorBuf);
        appendStringInfo(&errorBuf, "%s: HTTP Status: %d, CURL Code: %ld",
                         error_prefix,
                         handle->currentResponse ? handle->currentResponse->http_status : 0,
                         handle->currentResponse ? handle->currentResponse->curl_code : 0);
        if (handle->lastErrorMessage)
            appendStringInfo(&errorBuf, ", Error: %s", handle->lastErrorMessage);
        if (handle->currentResponse && handle->currentResponse->error_message)
            appendStringInfo(&errorBuf, ", Response Error: %s", handle->currentResponse->error_message);

        if (handle->currentResponse && handle->currentResponse->response_body)
        {
            json = agentcli_cJSON_Parse(handle->currentResponse->response_body);
            if (json)
            {
                error_obj = agentcli_cJSON_GetObjectItem(json, "error");
                if (error_obj)
                {
                    field = agentcli_cJSON_GetObjectItem(error_obj, "code");
                    if (agentcli_cJSON_IsNumber(field))
                        appendStringInfo(&errorBuf, "\nError Code: %d", field->valueint);
                    field = agentcli_cJSON_GetObjectItem(error_obj, "type");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nError Type: %s", field->valuestring);
                    field = agentcli_cJSON_GetObjectItem(error_obj, "message");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nMessage: %s", field->valuestring);
                    field = agentcli_cJSON_GetObjectItem(error_obj, "stack");
                    if (agentcli_cJSON_IsString(field) && field->valuestring && strlen(field->valuestring) > 0)
                    {
                        has_stack_trace = true;
                        if (client_min_messages <= LOG)
                            appendStringInfo(&errorBuf, "\n\nStack Trace:\n%s", field->valuestring);
                    }
                }
                else
                {
                    field = agentcli_cJSON_GetObjectItem(json, "message");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nMessage: %s", field->valuestring);
                }
                agentcli_cJSON_Delete(json);
            }
            else
            {
                appendStringInfo(&errorBuf, "\nRaw Response: %s", handle->currentResponse->response_body);
            }

            if (handle->currentResponse->total_time > 0)
                appendStringInfo(&errorBuf, "\nRequest Time: %.3f seconds", handle->currentResponse->total_time);
            if (handle->currentResponse->retry_count > 0)
                appendStringInfo(&errorBuf, "\nRetry Count: %d", handle->currentResponse->retry_count);
        }

        if (has_stack_trace && client_min_messages > LOG)
            appendStringInfo(&errorBuf, "\n\nHint: Set client_min_messages to 'log' or lower for detailed info");

        elog(ERROR, "%s", errorBuf.data);
    }

    if (handle->currentResponse &&
        (handle->currentResponse->response_body == NULL || handle->currentResponse->response_body[0] == '\0'))
        elog(ERROR, "%s: No response body", error_prefix);
}

void agent_cli_free_response(agent_cli_response_t* response) {
    if (response) {
        if (response->response_body) {
            free(response->response_body);
            response->response_body = nullptr;
        }
        if (response->error_message) {
            free(response->error_message);
            response->error_message = nullptr;
        }
        memset(response, 0, sizeof(agent_cli_response_t));
    }
}

const char* agent_cli_get_error_string(agent_cli_status_t status) {
    switch (status) {
        case AGENT_CLI_SUCCESS: return "Success";
        case AGENT_CLI_ERROR_CANCELLED: return "Operation cancelled";
        case AGENT_CLI_ERROR_INVALID_PARAM: return "Invalid parameter";
        case AGENT_CLI_ERROR_NETWORK: return "Network error";
        case AGENT_CLI_ERROR_JSON_PARSE: return "JSON parse error";
        case AGENT_CLI_ERROR_HTTP: return "HTTP error";
        case AGENT_CLI_ERROR_MEMORY: return "Memory error";
        case AGENT_CLI_ERROR_CURL: return "CURL error";
        case AGENT_CLI_ERROR_TIMEOUT: return "Timeout error";
        case AGENT_CLI_ERROR_THREAD: return "Thread error";
        case AGENT_CLI_ERROR_AUTH: return "Authentication error";
        case AGENT_CLI_ERROR_NOT_FOUND: return "Not found";
        case AGENT_CLI_ERROR_CONFLICT: return "Conflict";
        case AGENT_CLI_ERROR_BAD_REQUEST: return "Bad request";
        default: return "Unknown error";
    }
}

/* ----------------------------------------------------------------
 * File-level cleanup: agent_cli_wrapper_cleanup_metadata and
 * agent_cli_wrapper_delete_file.
 *
 * These two are ordinary operations: the same JSON bodies, dispatched
 * through the same AgentClient the other 24 use, so the transport decides --
 * even though the cleanup endpoints are not part of the prefix-based method
 * registry.  As raw libcurl POSTs to /api/v1/files/... they would ignore
 * datalake.agent_transport: under grpc the REST agent is not running, so
 * every cleanup would fail "Connection refused" and leave the files it was
 * meant to remove for the orphan backstop.  In the subtransaction-abort path
 * that failure is worse than a leak -- see the abort-path PANIC guard in
 * pg_iceberg_metadata_tracker.c.
 * ----------------------------------------------------------------
 */

AGENT_CLI_OP_NO_TABLE(agent_cli_cleanup_metadata_internal, cleanup_metadata)
AGENT_CLI_OP_NO_TABLE(agent_cli_delete_file_internal, delete_file)

/*
 * Append a JSON string body, escaping exactly what the curl versions escaped:
 * a backslash and a double quote.  Kept identical rather than widened to full
 * JSON escaping, because these bodies have to stay byte-for-byte what dlagent
 * already accepts: the REST path's observable behaviour must not change.
 */
static void
append_json_escaped(StringInfo out, const char *value)
{
    for (const char *p = value; *p; p++)
    {
        if (*p == '\\' || *p == '"')
            appendStringInfoChar(out, '\\');
        appendStringInfoChar(out, *p);
    }
}

/*
 * The failure detail the cleanup callers log: the agent's status and body for
 * an HTTP-level failure, otherwise the transport's own reason.
 */
static char *
cleanup_failure_detail(AgentCliHandle* handle)
{
    agent_cli_response_t *r = handle->currentResponse;

    if (handle->lastStatus == AGENT_CLI_ERROR_HTTP && r != NULL && r->http_status != 0)
        return psprintf("dlagent returned HTTP %d: %s", r->http_status,
                        (r->response_body && r->response_size > 0)
                        ? r->response_body : "(empty)");
    if (r != NULL && r->error_message != NULL && r->error_message[0] != '\0')
        return psprintf("%s: %s", agent_cli_get_error_string(handle->lastStatus),
                        r->error_message);
    return pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

static void
append_fileio_config(StringInfo out, const char *fileio_config_json)
{
    if (fileio_config_json && fileio_config_json[0] != '\0')
        appendStringInfoString(out, fileio_config_json);
    else
        appendStringInfoString(out, "{}");
}

void
agent_cli_wrapper_cleanup_metadata(AgentCliHandle* handle,
                                   const char* metadata_path,
                                   const char* fileio_config_json)
{
    StringInfoData body;

    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    if (metadata_path == NULL || metadata_path[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("metadata_path is empty");
        handle->responseValid    = false;
        return;
    }

    /* {"metadataPath":"<path>","fileIOConfig":<cfg>} */
    initStringInfo(&body);
    appendStringInfoString(&body, "{\"metadataPath\":\"");
    append_json_escaped(&body, metadata_path);
    appendStringInfoString(&body, "\",\"fileIOConfig\":");
    append_fileio_config(&body, fileio_config_json);
    appendStringInfoChar(&body, '}');

    handle->lastStatus = agent_cli_cleanup_metadata_internal(handle->handle, body.data,
                                                             handle->currentResponse);
    handle->responseValid = true;
    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = cleanup_failure_detail(handle);

    pfree(body.data);
}

void
agent_cli_wrapper_delete_file(AgentCliHandle* handle,
                              const char* path,
                              const char* fileio_config_json)
{
    StringInfoData body;

    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    if (path == NULL || path[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("path is empty");
        handle->responseValid    = false;
        return;
    }

    /* {"paths":["<path>"],"fileIOConfig":<cfg>} */
    initStringInfo(&body);
    appendStringInfoString(&body, "{\"paths\":[\"");
    append_json_escaped(&body, path);
    appendStringInfoString(&body, "\"],\"fileIOConfig\":");
    append_fileio_config(&body, fileio_config_json);
    appendStringInfoChar(&body, '}');

    handle->lastStatus = agent_cli_delete_file_internal(handle->handle, body.data,
                                                        handle->currentResponse);
    handle->responseValid = true;
    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = cleanup_failure_detail(handle);

    pfree(body.data);
}

} // extern "C"
