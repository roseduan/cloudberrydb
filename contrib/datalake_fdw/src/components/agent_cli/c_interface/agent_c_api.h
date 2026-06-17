#ifndef AGENT_C_API_H
#define AGENT_C_API_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef bool (*agent_cli_interrupt_callback_t)(void);

typedef enum {
    AGENT_CLI_CREATETABLE_ALREADY_EXSITS = 1,
    AGENT_CLI_SUCCESS = 0,
    AGENT_CLI_ERROR_CANCELLED = -1,
    AGENT_CLI_ERROR_INVALID_PARAM = -2,
    AGENT_CLI_ERROR_NETWORK = -3,
    AGENT_CLI_ERROR_JSON_PARSE = -4,
    AGENT_CLI_ERROR_HTTP = -5,
    AGENT_CLI_ERROR_MEMORY = -6,
    AGENT_CLI_ERROR_CURL = -7,
    AGENT_CLI_ERROR_TIMEOUT = -8,
    AGENT_CLI_ERROR_THREAD = -9,
    AGENT_CLI_ERROR_AUTH = -10,
    AGENT_CLI_ERROR_NOT_FOUND = -11,
    AGENT_CLI_ERROR_CONFLICT = -12,
    AGENT_CLI_ERROR_BAD_REQUEST = 13
} agent_cli_status_t;

typedef enum {
    AGENT_CLI_LOG_OFF = 0,
    AGENT_CLI_LOG_ERROR = 1,
    AGENT_CLI_LOG_WARN = 2,
    AGENT_CLI_LOG_INFO = 3,
    AGENT_CLI_LOG_DEBUG = 4
} agent_cli_log_level_t;

typedef void* agent_cli_handle_t;

typedef struct {
    const char* server_url;
    const char* namespace_name;
    const char* auth_token;
    int request_timeout_seconds;
    int connect_timeout_seconds;
    int max_retries;
    int retry_delay_ms;
    agent_cli_log_level_t log_level;
} agent_cli_config_t;

typedef struct {
    int http_status;
    long curl_code;
    char* response_body;
    size_t response_size;
    char* error_message;
    double total_time;
    int retry_count;
} agent_cli_response_t;

/* ----------------------------------------------------------------
 * PostgreSQL-integrated handle (replaces agent_cli_wrapper.h)
 *
 * The AgentCliHandle bundles the underlying C++ client handle with
 * PostgreSQL-compatible memory management and error tracking.
 * ----------------------------------------------------------------
 */
typedef struct AgentCliHandle
{
    agent_cli_handle_t handle;
    agent_cli_status_t lastStatus;
    const char *lastErrorMessage;
    agent_cli_config_t *agentConfig;
    agent_cli_response_t *currentResponse;
    bool responseValid;
    bool isValid;
} AgentCliHandle;

/* Initialization and cleanup */
AgentCliHandle* agent_cli_wrapper_create(const char* server_url, const char* prefix, const char* namespace_name);
void agent_cli_wrapper_destroy(AgentCliHandle* handle);

/* Table operations */
void agent_cli_wrapper_create_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_load_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_table_exists(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_get_fragment(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_plan_file_groups(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_commit_file_groups(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_commit_append(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_commit_update(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_commit_rewrite(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_append_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_update_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_truncate_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_drop_table(AgentCliHandle* handle, const char* table_name, const char* json);
void agent_cli_wrapper_get_statistics(AgentCliHandle* handle, const char* table_name, const char* json);

/*
 * File-level cleanup invoked by the deletion-queue consumer.  Issues an HTTP
 * POST to <server_url>/api/v1/files/cleanup-from-metadata so dlagent walks the
 * snapshot tree referenced by metadata_path (using fileio_config_json) and
 * deletes every referenced object.  On failure populates handle->lastStatus /
 * lastErrorMessage; callers MUST check via agent_cli_wrapper_check_exec_error_json.
 */
void agent_cli_wrapper_cleanup_metadata(AgentCliHandle* handle,
										const char* metadata_path,
										const char* fileio_config_json);

/* Catalog management operations */
void agent_cli_wrapper_create_catalog(AgentCliHandle* handle, const char* json);
void agent_cli_wrapper_list_catalogs(AgentCliHandle* handle, const char* json);
void agent_cli_wrapper_list_namespaces(AgentCliHandle* handle, const char* json);

/* Utility functions */
const char* agent_cli_wrapper_get_response(AgentCliHandle* handle);
bool agent_cli_wrapper_is_success(AgentCliHandle* handle);
void agent_cli_wrapper_check_error(AgentCliHandle* handle);
void agent_cli_wrapper_check_exec_error(AgentCliHandle* handle, const char *error_prefix);
void agent_cli_wrapper_check_exec_error_json(AgentCliHandle* handle, const char *error_prefix);
void agent_cli_wrapper_set_interrupt_callback(AgentCliHandle* handle, agent_cli_interrupt_callback_t callback);

/* Utility (internal) */
const char* agent_cli_get_error_string(agent_cli_status_t status);
void agent_cli_free_response(agent_cli_response_t* response);

#ifdef __cplusplus
}
#endif

#endif // AGENT_C_API_H
