#include "agent_cli_wrapper.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "../components/agent_cli/c_interface/agent_cjson_builder.hpp"

#include <curl/curl.h>
#include <string.h>

/* Global resource tracking */
bool agent_cli_wrapper_resowner_callback_registered = false;
AgentCliResource *agent_cli_wrapper_open_resources = NULL;

/* Forward declarations */
static void agent_cli_wrapper_init_config(const char* server_url, const char* prefix, const char* namespace_name, agent_cli_config_t *config);
/*
 * Create and initialize agent CLI handle with PostgreSQL integration
 */
AgentCliHandle*
agent_cli_wrapper_create(const char* server_url, const char* prefix, const char* namespace_name)
{
    AgentCliHandle *handle = (AgentCliHandle *)palloc0(sizeof(AgentCliHandle));
    handle->agentConfig = (agent_cli_config_t *)palloc0(sizeof(agent_cli_config_t));
    handle->currentResponse = (agent_cli_response_t *)palloc0(sizeof(agent_cli_response_t));

    /* Initialize agent config */
    agent_cli_wrapper_init_config(server_url, prefix, namespace_name, handle->agentConfig);

    /* Initialize agent CLI */
    agent_cli_status_t status = agent_cli_init(handle->agentConfig, &handle->handle);
    if (status != AGENT_CLI_SUCCESS)
    {
        handle->lastStatus = status;
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(status));
        handle->isValid = false;
        elog(ERROR, "Failed to initialize agent CLI: status %d %s", status, agent_cli_get_error_string(status));
    }

    handle->isValid = true;
    handle->responseValid = false;

    /* Register resource for cleanup */
    // agent_cli_wrapper_register_resource(handle);

    return handle;
}

/*
 * Destroy agent CLI handle and cleanup resources
 */
void
agent_cli_wrapper_destroy(AgentCliHandle* handle)
{
    if (!handle)
        return;

    if (handle->handle)
        agent_cli_cleanup(handle->handle);


    pfree(handle);
    handle = NULL;
}

/*
 * Table operations
 */
void
agent_cli_wrapper_create_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_create_table(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_load_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_load_table(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_table_exists(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_table_exists(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_get_fragment(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_get_fragment(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_plan_file_groups(AgentCliHandle* handle, const char* table_name, const char* json)
{
	if (!handle || !handle->isValid)
		elog(ERROR, "Invalid agent CLI handle");

	handle->lastStatus = agent_cli_plan_file_groups(handle->handle, table_name, json, handle->currentResponse);
	handle->responseValid = true;

	if (handle->lastStatus != AGENT_CLI_SUCCESS)
		handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_commit_file_groups(AgentCliHandle* handle, const char* table_name, const char* json)
{
	if (!handle || !handle->isValid)
		elog(ERROR, "Invalid agent CLI handle");

	handle->lastStatus = agent_cli_commit_file_groups(handle->handle, table_name, json, handle->currentResponse);
	handle->responseValid = true;

	if (handle->lastStatus != AGENT_CLI_SUCCESS)
		handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_commit_append(AgentCliHandle* handle, const char* table_name, const char* json)
{
	if (!handle || !handle->isValid)
		elog(ERROR, "Invalid agent CLI handle");

	handle->lastStatus = agent_cli_commit_append(handle->handle, table_name, json, handle->currentResponse);
	handle->responseValid = true;

	if (handle->lastStatus != AGENT_CLI_SUCCESS)
		handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_commit_update(AgentCliHandle* handle, const char* table_name, const char* json)
{
	if (!handle || !handle->isValid)
		elog(ERROR, "Invalid agent CLI handle");

	handle->lastStatus = agent_cli_commit_update(handle->handle, table_name, json, handle->currentResponse);
	handle->responseValid = true;

	if (handle->lastStatus != AGENT_CLI_SUCCESS)
		handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_commit_rewrite(AgentCliHandle* handle, const char* table_name, const char* json)
{
	if (!handle || !handle->isValid)
		elog(ERROR, "Invalid agent CLI handle");

	handle->lastStatus = agent_cli_commit_rewrite(handle->handle, table_name, json, handle->currentResponse);
	handle->responseValid = true;

	if (handle->lastStatus != AGENT_CLI_SUCCESS)
		handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_append_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_append_table(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_update_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_update_table(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_drop_table(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_drop_table(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_get_statistics(AgentCliHandle* handle, const char* table_name, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_get_statistics(handle->handle, table_name, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

/* ----------------------------------------------------------------
 * agent_cli_wrapper_cleanup_metadata
 *
 * libcurl-based POST to /v1/files/cleanup-from-metadata. See header for
 * rationale of bypassing agent_cli_* C++ method routing.
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

    /* Build URL: <base>/v1/files/cleanup-from-metadata
     *
     * Trim a trailing slash if any so we don't end up with "..//v1/...".
     */
    initStringInfo(&url);
    {
        size_t base_len = strlen(base_url);
        if (base_len > 0 && base_url[base_len - 1] == '/')
            appendBinaryStringInfo(&url, base_url, (int)(base_len - 1));
        else
            appendStringInfoString(&url, base_url);
        appendStringInfoString(&url, "/api/v1/files/cleanup-from-metadata");
    }

    /* Build JSON body.
     *
     * metadata_path is hand-escaped here (only backslashes and quotes are
     * possible in object-storage URIs).  fileio_config_json is assumed to be
     * a syntactically valid JSON object and is inlined verbatim.
     */
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
 * Catalog management operations
 */
void
agent_cli_wrapper_create_catalog(AgentCliHandle* handle, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_create_catalog(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_list_catalogs(AgentCliHandle* handle, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_list_catalogs(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

void
agent_cli_wrapper_list_namespaces(AgentCliHandle* handle, const char* json)
{
    if (!handle || !handle->isValid)
        elog(ERROR, "Invalid agent CLI handle");

    handle->lastStatus = agent_cli_list_namespaces(handle->handle, json, handle->currentResponse);
    handle->responseValid = true;

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
        handle->lastErrorMessage = pstrdup(agent_cli_get_error_string(handle->lastStatus));
}

/*
 * Utility functions
 */
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
    {
        elog(ERROR, "Agent CLI operation failed: %s",
             handle->lastErrorMessage ? handle->lastErrorMessage : agent_cli_get_error_string(handle->lastStatus));
    }
}

/*
 * Check for agent CLI execution errors and provide detailed error information
 * Similar to check_catalog_fdw_exec_error but for AgentCliHandle
 */
void
agent_cli_wrapper_check_exec_error(AgentCliHandle* handle, const char *error_prefix)
{
    StringInfoData errorBuf;

    if (!handle)
        elog(ERROR, "%s: Invalid agent CLI handle", error_prefix);

    if (handle->lastStatus != AGENT_CLI_SUCCESS)
    {
        initStringInfo(&errorBuf);

        appendStringInfo(&errorBuf,
                         "%s: HTTP Status: %d, CURL Code: %ld",
                         error_prefix,
                         handle->currentResponse ? handle->currentResponse->http_status : 0,
                         handle->currentResponse ? handle->currentResponse->curl_code : 0);

        if (handle->lastErrorMessage)
            appendStringInfo(&errorBuf, ", Error: %s", handle->lastErrorMessage);

        if (handle->currentResponse && handle->currentResponse->error_message)
            appendStringInfo(&errorBuf, ", Response Error: %s", handle->currentResponse->error_message);

        if (handle->currentResponse && handle->currentResponse->response_body)
        {
            /* Extract error details from JSON response */
            char *codeStart = strstr(handle->currentResponse->response_body, "\"code\":");
            if (codeStart)
            {
                codeStart += 7;
                while (*codeStart == ' ') codeStart++;
                char *codeEnd = strchr(codeStart, ',');
                if (codeEnd)
                    appendStringInfo(&errorBuf, "\nError Code: %.*s", (int)(codeEnd - codeStart), codeStart);
            }

            char *typeStart = strstr(handle->currentResponse->response_body, "\"type\":\"");
            if (typeStart)
            {
                typeStart += 8;
                char *typeEnd = strchr(typeStart, '"');
                if (typeEnd)
                    appendStringInfo(&errorBuf, "\nError Type: %.*s", (int)(typeEnd - typeStart), typeStart);
            }

            char *msgStart = strstr(handle->currentResponse->response_body, "\"message\":\"");
            if (msgStart)
            {
                msgStart += 11;
                char *msgEnd = strstr(msgStart, "\",\"");
                if (!msgEnd) msgEnd = strstr(msgStart, "\"}");
                if (msgEnd)
                    appendStringInfo(&errorBuf, "\nMessage: %.*s", (int)(msgEnd - msgStart), msgStart);
            }

            /* Extract and format stack trace */
            char *stackStart = strstr(handle->currentResponse->response_body, "\"stack\":\"");
            if (stackStart && client_min_messages <= LOG)
            {
                stackStart += 9;
                char *stackEnd = strstr(stackStart, "\",\"");
                if (!stackEnd) stackEnd = strstr(stackStart, "\"}");
                if (stackEnd)
                {
                    appendStringInfo(&errorBuf, "\n\nStack Trace:\n");
                    char *src = stackStart;
                    while (src < stackEnd)
                    {
                        if (src[0] == '\\' && src[1] == 'n')
                        {
                            appendStringInfoChar(&errorBuf, '\n');
                            src += 2;
                        }
                        else if (src[0] == '\\' && src[1] == 't')
                        {
                            appendStringInfoChar(&errorBuf, '\t');
                            src += 2;
                        }
                        else if (src[0] == '\\' && src[1] == '"')
                        {
                            appendStringInfoChar(&errorBuf, '"');
                            src += 2;
                        }
                        else
                        {
                            appendStringInfoChar(&errorBuf, *src);
                            src++;
                        }
                    }
                }
            }

            /* Add timing information if available */
            if (handle->currentResponse->total_time > 0)
                appendStringInfo(&errorBuf, "\nRequest Time: %.3f seconds", handle->currentResponse->total_time);

            if (handle->currentResponse->retry_count > 0)
                appendStringInfo(&errorBuf, "\nRetry Count: %d", handle->currentResponse->retry_count);
        }

        elog(ERROR, "%s", errorBuf.data);
    }

    if (handle->currentResponse && 
        (handle->currentResponse->response_body == NULL || handle->currentResponse->response_body[0] == '\0'))
        elog(ERROR, "%s: No response body", error_prefix);
}

void
agent_cli_wrapper_set_interrupt_callback(AgentCliHandle* handle, agent_cli_interrupt_callback_t callback)
{
    if (!handle || !handle->isValid)
        return;
    
    agent_cli_set_interrupt_callback(handle->handle, callback);
}

/*
 * Resource management functions - COMMENTED OUT
 */

// AgentCliResource*
// agent_cli_wrapper_register_resource(AgentCliHandle* handle)
// {
//     AgentCliResource *resource = MemoryContextAlloc(TopMemoryContext, sizeof(AgentCliResource));

//     resource->agentClihandle = handle;
//     resource->resowner = CurrentResourceOwner;

//     /* Add to linked list */
//     resource->next = agent_cli_wrapper_open_resources;
//     if (agent_cli_wrapper_open_resources)
//         agent_cli_wrapper_open_resources->prev = resource;
//     agent_cli_wrapper_open_resources = resource;

//     /* Register resource owner callback if not already done */
//     agent_cli_wrapper_register_resowner_callback();

//     return resource;
// }

// void
// agent_cli_wrapper_unregister_resource(AgentCliResource* resource)
// {
//     if (!resource)
//         return;

//     /* Remove from linked list */
//     if (resource->prev)
//         resource->prev->next = resource->next;
//     else
//         agent_cli_wrapper_open_resources = resource->next;

//     if (resource->next)
//         resource->next->prev = resource->prev;

//     /* Cleanup handle */
//     if (resource->agentClihandle)
//         agent_cli_wrapper_destroy(resource->agentClihandle);

//     pfree(resource);
// }

// void
// agent_cli_wrapper_resource_cleanup(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
// {
//     AgentCliResource *resource, *next;

//     if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
//         return;

//     for (resource = agent_cli_wrapper_open_resources; resource; resource = next)
//     {
//         next = resource->next;

//         if (resource->resowner == CurrentResourceOwner)
//         {
//             elog(WARNING, "Agent CLI resource leak detected, cleaning up");
//             agent_cli_wrapper_unregister_resource(resource);
//         }
//     }
// }


/*
 * Initialize agent config with PostgreSQL log level mapping
 */
static void
agent_cli_wrapper_init_config(const char* server_url, const char* prefix, const char* namespace_name, agent_cli_config_t *config)
{
    config->server_url = server_url ? pstrdup(server_url) : NULL;
    config->namespace_name = namespace_name ? pstrdup(namespace_name) : NULL;
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

/*
 * Register resource owner callback (once) - COMMENTED OUT
 */
/*
static void
agent_cli_wrapper_register_resowner_callback(void)
{
    if (!agent_cli_wrapper_resowner_callback_registered)
    {
        RegisterResourceReleaseCallback(agent_cli_wrapper_resource_cleanup, NULL);
        agent_cli_wrapper_resowner_callback_registered = true;
    }
}
*/
/*
 * Parse JSON error response using agentcli_cJSON library - safer alternative
 */
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

        appendStringInfo(&errorBuf,
                         "%s: HTTP Status: %d, CURL Code: %ld",
                         error_prefix,
                         handle->currentResponse ? handle->currentResponse->http_status : 0,
                         handle->currentResponse ? handle->currentResponse->curl_code : 0);

        if (handle->lastErrorMessage)
            appendStringInfo(&errorBuf, ", Error: %s", handle->lastErrorMessage);

        if (handle->currentResponse && handle->currentResponse->error_message)
            appendStringInfo(&errorBuf, ", Response Error: %s", handle->currentResponse->error_message);

        if (handle->currentResponse && handle->currentResponse->response_body)
        {
            /* Parse JSON response */
            json = agentcli_cJSON_Parse(handle->currentResponse->response_body);
            if (json)
            {
                /* Look for error object */
                error_obj = agentcli_cJSON_GetObjectItem(json, "error");
                if (error_obj)
                {
                    /* Extract error code */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "code");
                    if (agentcli_cJSON_IsNumber(field))
                        appendStringInfo(&errorBuf, "\nError Code: %d", field->valueint);

                    /* Extract error type */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "type");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nError Type: %s", field->valuestring);

                    /* Extract error message */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "message");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nMessage: %s", field->valuestring);

                    /* Extract operation */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "operation");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nOperation: %s", field->valuestring);

                    /* Extract responseBody */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "responseBody");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nResponse Body: %s", field->valuestring);

                    /* Extract and format stack trace */
                    field = agentcli_cJSON_GetObjectItem(error_obj, "stack");
                    if (agentcli_cJSON_IsString(field) && field->valuestring && strlen(field->valuestring) > 0)
                    {
                        has_stack_trace = true;
                        if (client_min_messages <= LOG)
                        {
                            appendStringInfo(&errorBuf, "\n\nStack Trace:\n%s", field->valuestring);
                        }
                    }
                }
                else
                {
                    /* No error object, try direct fields */
                    field = agentcli_cJSON_GetObjectItem(json, "message");
                    if (agentcli_cJSON_IsString(field) && field->valuestring)
                        appendStringInfo(&errorBuf, "\nMessage: %s", field->valuestring);
                }

                agentcli_cJSON_Delete(json);
            }
            else
            {
                /* JSON parsing failed, include raw response */
                appendStringInfo(&errorBuf, "\nRaw Response: %s", handle->currentResponse->response_body);
            }

            /* Add timing information if available */
            if (handle->currentResponse->total_time > 0)
                appendStringInfo(&errorBuf, "\nRequest Time: %.3f seconds", handle->currentResponse->total_time);

            if (handle->currentResponse->retry_count > 0)
                appendStringInfo(&errorBuf, "\nRetry Count: %d", handle->currentResponse->retry_count);
        }

        /* Add hint if stack trace is available but not shown */
        if (has_stack_trace && client_min_messages > LOG)
        {
            appendStringInfo(&errorBuf, "\n\nHint: Set client_min_messages to 'log' or lower for detailed info");
        }

        elog(ERROR, "%s", errorBuf.data);
    }

    if (handle->currentResponse && 
        (handle->currentResponse->response_body == NULL || handle->currentResponse->response_body[0] == '\0'))
        elog(ERROR, "%s: No response body", error_prefix);
}
