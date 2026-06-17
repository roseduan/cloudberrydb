#ifndef AGENT_CLI_WRAPPER_H
#define AGENT_CLI_WRAPPER_H

#include "postgres.h"
#include "utils/resowner.h"
#include "utils/memutils.h"
#include "src/components/agent_cli/c_interface/agent_c_api.h"

/*
 * AgentCliHandle is defined canonically in agent_c_api.h (included above),
 * which explicitly supersedes the definition that used to live here.
 * Re-declaring the struct breaks any translation unit that includes both
 * headers (e.g. pg_iceberg_av_consumer.c), so rely solely on agent_c_api.h.
 */

/* Resource management - same as existing structure */
typedef struct AgentCliResource
{
    AgentCliHandle *agentClihandle;
    ResourceOwner resowner;
    struct AgentCliResource *next;
    struct AgentCliResource *prev;
} AgentCliResource;

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
void agent_cli_wrapper_drop_table(AgentCliHandle* handle, const char* table_name, const char* json);

void agent_cli_wrapper_get_statistics(AgentCliHandle* handle, const char* table_name, const char* json);

/*
 * File-level cleanup invoked by the deletion-queue consumer.
 *
 * POSTs to ${handle->agentConfig->server_url}/api/v1/files/cleanup-from-metadata
 * with body  { "metadataPath": <metadata_path>,
 *              "fileIOConfig": <fileio_config_json> }.
 *
 * dlagent reads the metadata.json with the supplied fileIOConfig, walks the
 * full snapshot tree (manifest lists -> manifests -> data/delete files), and
 * deletes every referenced object.  This is called after DROP TABLE has
 * already torn down the catalog identity of the table, so the path is the
 * only handle the queue still has on the dataset.
 *
 * Unlike the other wrappers this one issues an HTTP POST directly via
 * libcurl rather than going through agent_cli_* C++ method routing, because
 * the cleanup endpoint is not part of the prefix-based method registry.
 *
 * On HTTP failure the function populates handle->lastStatus / lastErrorMessage
 * and returns; callers MUST check via agent_cli_wrapper_check_exec_error_json.
 *
 * fileio_config_json must already be a JSON object literal (e.g. produced by
 * agentcli_cJSON_PrintUnformatted).  It is inlined verbatim into the request
 * body; the caller owns its lifetime.
 */
void agent_cli_wrapper_cleanup_metadata(AgentCliHandle* handle,
                                        const char* metadata_path,
                                        const char* fileio_config_json);

/*
 * Delete a single object by path (DELETION_TYPE_FILE).  POSTs
 * {"paths":["<path>"], "fileIOConfig":...} to /api/v1/files/delete.
 */
void agent_cli_wrapper_delete_file(AgentCliHandle* handle,
                                   const char* path,
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

/* Resource management functions */
AgentCliResource* agent_cli_wrapper_register_resource(AgentCliHandle* handle);
void agent_cli_wrapper_unregister_resource(AgentCliResource* resource);
void agent_cli_wrapper_resource_cleanup(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);

/* Global resource tracking - moved from FDW */
extern bool agent_cli_wrapper_resowner_callback_registered;
extern AgentCliResource *agent_cli_wrapper_open_resources;

#endif /* AGENT_CLI_WRAPPER_H */
