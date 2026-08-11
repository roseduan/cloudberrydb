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
#undef LOG
#undef INFO
#undef WARNING
#undef ERROR
#undef DEBUG1
#undef DEBUG2
#undef DEBUG3
#undef DEBUG4
#undef DEBUG5

#include <agent_c_api.h>
#include <agent_client.hpp>
#include <agent_types.hpp>

/* libcurl + libc for agent_cli_wrapper_cleanup_metadata (defined below) */
#include <curl/curl.h>
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

    void convert_response(const Response& cpp_response, agent_cli_response_t* c_response) {
        if (!c_response) return;

        c_response->http_status = cpp_response.http_status;
        c_response->curl_code = cpp_response.curl_code;
        c_response->response_body = duplicate_string(cpp_response.response_body.c_str());
        c_response->response_size = cpp_response.response_body.size();
        c_response->error_message = duplicate_string(cpp_response.error_message.c_str());
        c_response->total_time = cpp_response.total_time;
        c_response->retry_count = cpp_response.retry_count;
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
        cpp_config.auth_token = config->auth_token ? config->auth_token : "";
        cpp_config.request_timeout_seconds = config->request_timeout_seconds > 0 ? config->request_timeout_seconds : 30;
        cpp_config.connect_timeout_seconds = config->connect_timeout_seconds > 0 ? config->connect_timeout_seconds : 10;
        cpp_config.max_retries = config->max_retries >= 0 ? config->max_retries : 1;
        cpp_config.retry_delay_ms = config->retry_delay_ms > 0 ? config->retry_delay_ms : 1000;

        AgentContext* context = new AgentContext();
        context->client.reset(new AgentClient(cpp_config));

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
    if (auto status = validate_params(handle, "handle")) return status; \
    if (auto status = validate_params(response, "response")) return status; \
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
        return AGENT_CLI_ERROR_CANCELLED; \
    } catch (const std::exception& e) { \
        last_error_message = e.what(); \
        return AGENT_CLI_ERROR_NETWORK; \
    } catch (...) { \
        last_error_message = "Unknown error in " #client_method; \
        return AGENT_CLI_ERROR_NETWORK; \
    } \
}

#define AGENT_CLI_OP_NO_TABLE(func_name, client_method) \
static agent_cli_status_t func_name(agent_cli_handle_t handle, \
                                    const char* request_json, \
                                    agent_cli_response_t* response) { \
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
        return AGENT_CLI_ERROR_CANCELLED; \
    } catch (const std::exception& e) { \
        last_error_message = e.what(); \
        return AGENT_CLI_ERROR_NETWORK; \
    } catch (...) { \
        last_error_message = "Unknown error in " #client_method; \
        return AGENT_CLI_ERROR_NETWORK; \
    } \
}

/* Table operations (internal) */
AGENT_CLI_OP_NO_TABLE(agent_cli_create_table_internal, create_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_load_table_internal, load_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_table_exists_internal, table_exists)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_fragment_internal, get_fragment)
AGENT_CLI_OP_WITH_TABLE(agent_cli_plan_file_groups_internal, plan_file_groups)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_file_groups_internal, commit_file_groups)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_append_internal, commit_append)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_update_internal, commit_update)
AGENT_CLI_OP_WITH_TABLE(agent_cli_commit_rewrite_internal, commit_rewrite)
AGENT_CLI_OP_WITH_TABLE(agent_cli_append_table_internal, append_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_update_table_internal, update_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_truncate_table_internal, truncate_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_drop_table_internal, drop_table)
AGENT_CLI_OP_WITH_TABLE(agent_cli_get_statistics_internal, get_statistics)
AGENT_CLI_OP_WITH_TABLE(agent_cli_update_schema_internal, update_schema)

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
    config->request_timeout_seconds = 300;
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

AgentCliHandle*
agent_cli_wrapper_create(const char* server_url, const char* prefix, const char* namespace_name)
{
    AgentCliHandle *handle = (AgentCliHandle *)palloc0(sizeof(AgentCliHandle));
    handle->agentConfig = (agent_cli_config_t *)palloc0(sizeof(agent_cli_config_t));
    handle->currentResponse = (agent_cli_response_t *)palloc0(sizeof(agent_cli_response_t));

    agent_cli_wrapper_init_config(server_url, prefix, namespace_name, handle->agentConfig);

    agent_cli_status_t status = agent_cli_init_internal(handle->agentConfig, &handle->handle);
    if (status != AGENT_CLI_SUCCESS)
    {
        handle->lastStatus = status;
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(status));
        handle->isValid = false;
        elog(ERROR, "Failed to initialize agent CLI: status %d %s", status, agent_cli_get_error_string(status));
    }

    handle->isValid = true;
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
    if (handle->lastStatus != AGENT_CLI_SUCCESS) \
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus)); \
}

#define WRAPPER_NO_TABLE_OP(wrapper_name, internal_name) \
void wrapper_name(AgentCliHandle* handle, const char* json) \
{ \
    if (!handle || !handle->isValid) \
        elog(ERROR, "Invalid agent CLI handle"); \
    handle->lastStatus = internal_name(handle->handle, json, handle->currentResponse); \
    handle->responseValid = true; \
    if (handle->lastStatus != AGENT_CLI_SUCCESS) \
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus)); \
}

/* Table operations */
/* create_table is special: the internal function doesn't take table_name */
void agent_cli_wrapper_create_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");
    handle->lastStatus = agent_cli_create_table_internal(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;
    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

WRAPPER_TABLE_OP(agent_cli_wrapper_load_table, agent_cli_load_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_table_exists, agent_cli_table_exists_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_fragment, agent_cli_get_fragment_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_plan_file_groups, agent_cli_plan_file_groups_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_file_groups, agent_cli_commit_file_groups_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_append, agent_cli_commit_append_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_update, agent_cli_commit_update_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_commit_rewrite, agent_cli_commit_rewrite_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_append_table, agent_cli_append_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_update_table, agent_cli_update_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_truncate_table, agent_cli_truncate_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_drop_table, agent_cli_drop_table_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_get_statistics, agent_cli_get_statistics_internal)
WRAPPER_TABLE_OP(agent_cli_wrapper_update_schema, agent_cli_update_schema_internal)

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
 * agent_cli_wrapper_cleanup_metadata
 *
 * libcurl-based POST to /api/v1/files/cleanup-from-metadata.  Bypasses the
 * agent_cli_* C++ method routing because the cleanup endpoint is not part of
 * the prefix-based method registry.  Called by the deletion-queue consumer.
 * ----------------------------------------------------------------
 */
static size_t
cleanup_metadata_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    StringInfo body = (StringInfo) userdata;
    size_t total = size * nmemb;

    appendBinaryStringInfo(body, ptr, (int) total);
    return total;
}

void
agent_cli_wrapper_cleanup_metadata(AgentCliHandle* handle,
                                   const char* metadata_path,
                                   const char* fileio_config_json)
{
    CURL              *curl;
    CURLcode           res;
    long               http_code = 0;
    struct curl_slist *headers = NULL;
    StringInfoData     url;
    StringInfoData     body;
    StringInfoData     response;
    const char        *base_url;

    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    if (handle->agentConfig == NULL ||
        handle->agentConfig->server_url == NULL ||
        handle->agentConfig->server_url[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("agent server_url is empty");
        handle->responseValid    = false;
        return;
    }
    base_url = handle->agentConfig->server_url;

    if (metadata_path == NULL || metadata_path[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("metadata_path is empty");
        handle->responseValid    = false;
        return;
    }

    /* Build URL: <base>/api/v1/files/cleanup-from-metadata (trim trailing /). */
    initStringInfo(&url);
    {
        size_t base_len = strlen(base_url);
        if (base_len > 0 && base_url[base_len - 1] == '/')
            appendBinaryStringInfo(&url, base_url, (int)(base_len - 1));
        else
            appendStringInfoString(&url, base_url);
        appendStringInfoString(&url, "/api/v1/files/cleanup-from-metadata");
    }

    /* Build JSON body.  metadata_path is hand-escaped; fileio_config_json is
     * assumed to be a valid JSON object and inlined verbatim. */
    initStringInfo(&body);
    appendStringInfoString(&body, "{\"metadataPath\":\"");
    for (const char *p = metadata_path; *p; p++)
    {
        if (*p == '\\' || *p == '"')
            appendStringInfoChar(&body, '\\');
        appendStringInfoChar(&body, *p);
    }
    appendStringInfoString(&body, "\",\"fileIOConfig\":");
    if (fileio_config_json && fileio_config_json[0] != '\0')
        appendStringInfoString(&body, fileio_config_json);
    else
        appendStringInfoString(&body, "{}");
    appendStringInfoChar(&body, '}');

    initStringInfo(&response);

    curl = curl_easy_init();
    if (!curl)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_CURL;
        handle->lastErrorMessage = pstrdup("curl_easy_init() failed");
        handle->responseValid    = false;

        pfree(url.data);
        pfree(body.data);
        pfree(response.data);
        return;
    }

    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL,          url.data);
    curl_easy_setopt(curl, CURLOPT_POST,         1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS,   body.data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) body.len);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER,   headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cleanup_metadata_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA,    (void *) &response);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL,     1L);   /* avoid SIGPIPE in PG backends */

    if (handle->agentConfig->request_timeout_seconds > 0)
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                         (long) handle->agentConfig->request_timeout_seconds);
    if (handle->agentConfig->connect_timeout_seconds > 0)
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                         (long) handle->agentConfig->connect_timeout_seconds);

    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    /* Populate handle->currentResponse so callers can read the body. */
    if (handle->currentResponse->response_body)
    {
        pfree(handle->currentResponse->response_body);
        handle->currentResponse->response_body = NULL;
    }
    handle->currentResponse->response_body = pstrdup(response.data);
    handle->currentResponse->response_size = response.len;
    handle->currentResponse->http_status   = (int) http_code;
    handle->currentResponse->curl_code     = (long) res;
    handle->responseValid                  = true;

    if (res != CURLE_OK)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_NETWORK;
        handle->lastErrorMessage = psprintf("curl_easy_perform failed: %s",
                                            curl_easy_strerror(res));
    }
    else if (http_code < 200 || http_code >= 300)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_HTTP;
        handle->lastErrorMessage = psprintf("dlagent returned HTTP %ld: %s",
                                            http_code,
                                            response.len > 0 ? response.data : "(empty)");
    }
    else
    {
        handle->lastStatus       = AGENT_CLI_SUCCESS;
        handle->lastErrorMessage = NULL;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    pfree(url.data);
    pfree(body.data);
    pfree(response.data);
}

/*
 * ----------------------------------------------------------------
 * agent_cli_wrapper_delete_file
 *
 * libcurl-based POST to /api/v1/files/delete, deleting a single object by
 * path (DELETION_TYPE_FILE entries, e.g. VACUUM's rewritten old files).
 * Mirrors agent_cli_wrapper_cleanup_metadata but sends
 * {"paths":["<path>"],"fileIOConfig":...} and does NOT parse the path as a
 * metadata tree.
 * ----------------------------------------------------------------
 */
void
agent_cli_wrapper_delete_file(AgentCliHandle* handle,
                              const char* path,
                              const char* fileio_config_json)
{
    CURL              *curl;
    CURLcode           res;
    long               http_code = 0;
    struct curl_slist *headers = NULL;
    StringInfoData     url;
    StringInfoData     body;
    StringInfoData     response;
    const char        *base_url;

    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    if (handle->agentConfig == NULL ||
        handle->agentConfig->server_url == NULL ||
        handle->agentConfig->server_url[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("agent server_url is empty");
        handle->responseValid    = false;
        return;
    }
    base_url = handle->agentConfig->server_url;

    if (path == NULL || path[0] == '\0')
    {
        handle->lastStatus       = AGENT_CLI_ERROR_INVALID_PARAM;
        handle->lastErrorMessage = pstrdup("path is empty");
        handle->responseValid    = false;
        return;
    }

    /* Build URL: <base>/api/v1/files/delete (trim trailing /). */
    initStringInfo(&url);
    {
        size_t base_len = strlen(base_url);
        if (base_len > 0 && base_url[base_len - 1] == '/')
            appendBinaryStringInfo(&url, base_url, (int)(base_len - 1));
        else
            appendStringInfoString(&url, base_url);
        appendStringInfoString(&url, "/api/v1/files/delete");
    }

    /* Build JSON body: {"paths":["<path>"],"fileIOConfig":<cfg>} */
    initStringInfo(&body);
    appendStringInfoString(&body, "{\"paths\":[\"");
    for (const char *p = path; *p; p++)
    {
        if (*p == '\\' || *p == '"')
            appendStringInfoChar(&body, '\\');
        appendStringInfoChar(&body, *p);
    }
    appendStringInfoString(&body, "\"],\"fileIOConfig\":");
    if (fileio_config_json && fileio_config_json[0] != '\0')
        appendStringInfoString(&body, fileio_config_json);
    else
        appendStringInfoString(&body, "{}");
    appendStringInfoChar(&body, '}');

    initStringInfo(&response);

    curl = curl_easy_init();
    if (!curl)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_CURL;
        handle->lastErrorMessage = pstrdup("curl_easy_init() failed");
        handle->responseValid    = false;

        pfree(url.data);
        pfree(body.data);
        pfree(response.data);
        return;
    }

    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    curl_easy_setopt(curl, CURLOPT_URL,          url.data);
    curl_easy_setopt(curl, CURLOPT_POST,         1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS,   body.data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) body.len);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER,   headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cleanup_metadata_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA,    (void *) &response);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL,     1L);

    if (handle->agentConfig->request_timeout_seconds > 0)
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,
                         (long) handle->agentConfig->request_timeout_seconds);
    if (handle->agentConfig->connect_timeout_seconds > 0)
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,
                         (long) handle->agentConfig->connect_timeout_seconds);

    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (handle->currentResponse->response_body)
    {
        pfree(handle->currentResponse->response_body);
        handle->currentResponse->response_body = NULL;
    }
    handle->currentResponse->response_body = pstrdup(response.data);
    handle->currentResponse->response_size = response.len;
    handle->currentResponse->http_status   = (int) http_code;
    handle->currentResponse->curl_code     = (long) res;
    handle->responseValid                  = true;

    if (res != CURLE_OK)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_NETWORK;
        handle->lastErrorMessage = psprintf("curl_easy_perform failed: %s",
                                            curl_easy_strerror(res));
    }
    else if (http_code < 200 || http_code >= 300)
    {
        handle->lastStatus       = AGENT_CLI_ERROR_HTTP;
        handle->lastErrorMessage = psprintf("dlagent returned HTTP %ld: %s",
                                            http_code,
                                            response.len > 0 ? response.data : "(empty)");
    }
    else
    {
        handle->lastStatus       = AGENT_CLI_SUCCESS;
        handle->lastErrorMessage = NULL;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    pfree(url.data);
    pfree(body.data);
    pfree(response.data);
}

} // extern "C"
