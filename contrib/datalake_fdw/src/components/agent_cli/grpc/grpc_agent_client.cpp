/*-------------------------------------------------------------------------
 *
 * grpc_agent_client.cpp
 *    The AgentClient implementation that speaks gRPC.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_agent_client.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "grpc_agent_client.hpp"

#include "agent_types.hpp"

namespace agent_cli {

namespace {

const char* const SERVICE_CATALOG = "/dlagent.v1.IcebergCatalog/";
const char* const SERVICE_MANAGEMENT = "/dlagent.v1.CatalogManagement/";

/*
 * gRPC status -> the ErrorCode the C ABI already understands.
 *
 * The mirror of dlagent-core's ErrorMapper, which turns the same HTTP-shaped
 * status into a gRPC code on the way out.  Keeping the two tables aligned is
 * what makes the SQLSTATE a user sees identical under both transports.
 *
 * CANCELLED is deliberately absent: it is raised as CancelledException so the
 * operation macros in agent_c_api.cpp produce AGENT_CLI_ERROR_CANCELLED
 * through the path the REST transport already uses.
 */
ErrorCode to_error_code(const grpc::Status& status)
{
    switch (status.error_code())
    {
        case grpc::StatusCode::OK:                 return ErrorCode::SUCCESS;
        case grpc::StatusCode::INVALID_ARGUMENT:   return ErrorCode::BAD_REQUEST;
        case grpc::StatusCode::UNAUTHENTICATED:
        case grpc::StatusCode::PERMISSION_DENIED:  return ErrorCode::AUTH;
        case grpc::StatusCode::NOT_FOUND:          return ErrorCode::NOT_FOUND;
        case grpc::StatusCode::ALREADY_EXISTS:     return ErrorCode::CONFLICT;
        case grpc::StatusCode::UNAVAILABLE:        return ErrorCode::NETWORK;
        case grpc::StatusCode::DEADLINE_EXCEEDED:  return ErrorCode::TIMEOUT;
        case grpc::StatusCode::RESOURCE_EXHAUSTED: return ErrorCode::MEMORY;
        default:                                   return ErrorCode::HTTP;
    }
}

/*
 * The HTTP status the C side still reports, so error messages and any code
 * that switches on it read the same under both transports.
 */
int to_http_status(const grpc::Status& status)
{
    switch (status.error_code())
    {
        case grpc::StatusCode::OK:                 return 200;
        case grpc::StatusCode::INVALID_ARGUMENT:   return 400;
        case grpc::StatusCode::UNAUTHENTICATED:    return 401;
        case grpc::StatusCode::PERMISSION_DENIED:  return 403;
        case grpc::StatusCode::NOT_FOUND:          return 404;
        case grpc::StatusCode::ALREADY_EXISTS:     return 409;
        case grpc::StatusCode::UNAVAILABLE:        return 503;
        case grpc::StatusCode::DEADLINE_EXCEEDED:  return 504;
        default:                                   return 500;
    }
}

/*
 * The operation's HTTP-shaped status -> ErrorCode.
 *
 * Deliberately the same shape as the REST client's setErrorCode(): both
 * transports deliver an HTTP status and a JSON body, so both must classify
 * them the same way or the same failure would produce two different SQLSTATEs.
 */
ErrorCode http_status_to_error_code(int http_status)
{
    if (http_status >= 200 && http_status < 300)
        return ErrorCode::SUCCESS;
    switch (http_status)
    {
        case 400: return ErrorCode::BAD_REQUEST;
        case 401:
        case 403: return ErrorCode::AUTH;
        case 404: return ErrorCode::NOT_FOUND;
        case 409: return ErrorCode::CONFLICT;
        case 502:
        case 503: return ErrorCode::NETWORK;
        case 504: return ErrorCode::TIMEOUT;
        default:  return ErrorCode::HTTP;
    }
}

/*
 * DlAgentServer.NOT_READY_DESCRIPTION: what the agent's readiness gate says
 * when it refuses a call it has not run.  Change both or neither.
 */
const char* const NOT_READY_DESCRIPTION = "dlagent is not ready";
const int NOT_READY_RETRY_MS = 200;

/*
 * How long a catalog call waits for the agent to accept a connection.  Covers
 * a restart (datalake_proxy.restart_time is 5 s for gRPC, and the agent
 * listens about a second after it starts) without turning an agent that is
 * not coming back into a 300 s wait.
 */
const int CONNECT_TIMEOUT_MS = 10000;

}  // namespace

GrpcAgentClient::GrpcAgentClient(const Config& config)
    : AgentClient(config),
      target_(config.grpc_target.empty() ? target_from_url(config.server_url)
                                         : config.grpc_target)
{
}

GrpcAgentClient::~GrpcAgentClient() = default;

std::string GrpcAgentClient::target_from_url(const std::string& server_url)
{
    /*
     * Only a fallback, for a configuration that set the REST URL and nothing
     * else.  Strips the scheme and any path; the port is left as-is, which
     * will be the REST port and will therefore fail fast with UNAVAILABLE
     * rather than half-working.
     */
    std::string target = server_url;
    size_t scheme = target.find("://");
    if (scheme != std::string::npos)
        target = target.substr(scheme + 3);
    size_t slash = target.find('/');
    if (slash != std::string::npos)
        target = target.substr(0, slash);
    return target;
}

grpcx::CallOptions GrpcAgentClient::options_for(const RequestConfig* request_config) const
{
    grpcx::CallOptions options;
    int seconds = config_.request_timeout_seconds;
    if (request_config != nullptr && request_config->request_timeout_seconds > 0)
        seconds = request_config->request_timeout_seconds;
    if (seconds > 0)
        options.deadline_ms = seconds * 1000;
    options.wait_for_ready = true;
    options.connect_timeout_ms = CONNECT_TIMEOUT_MS;
    return options;
}

Response GrpcAgentClient::unary(const std::string& method,
                                const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config,
                                bool wait_for_ready)
{
    Response response;

    std::map<std::string, std::string> metadata;
    /*
     * Encoded exactly as the REST client encodes it into the URL path, and
     * decoded by the agent as Spring decodes a path variable.  A raw
     * non-ASCII name is not allowed in gRPC metadata at all.
     */
    if (!table_name.empty())
        metadata["dlagent-table"] = encode_path_segment(table_name);
    if (request_config != nullptr)
    {
        for (std::map<std::string, std::string>::const_iterator it =
                 request_config->headers.begin();
             it != request_config->headers.end(); ++it)
        {
            metadata[it->first] = it->second;
        }
    }

    int app_status = -1;
    /*
     * Timed for the same reason the REST client reports curl's total_time: the
     * C side prints "Request Time: N seconds" in an error message when it is
     * set (agent_cli_wrapper_check_exec_error_json), so leaving it at zero
     * would silently drop a line that the REST transport prints.
     */
    gpr_timespec call_start = gpr_now(GPR_CLOCK_MONOTONIC);
    grpcx::CallOptions options = options_for(request_config);
    options.wait_for_ready = wait_for_ready;
    grpc::Status status = grpcx::call_unary(
        target_, method, request_json, metadata, options,
        interrupt_check(), &response.response_body, &app_status);

    /*
     * The one UNAVAILABLE that is safe to retry: the agent's readiness gate
     * refusing a call between binding its port and being ready
     * (DlAgentServer.NOT_READY_DESCRIPTION).  That call never ran.  Retried
     * within the same deadline, with the interrupt check between attempts.
     */
    int not_ready_retries = 0;
    while (wait_for_ready &&
           status.error_code() == grpc::StatusCode::UNAVAILABLE &&
           status.error_message() == NOT_READY_DESCRIPTION)
    {
        gpr_timespec spent = gpr_time_sub(gpr_now(GPR_CLOCK_MONOTONIC), call_start);
        int remaining_ms = options.deadline_ms -
            (int) (spent.tv_sec * 1000 + spent.tv_nsec / 1000000);
        if (remaining_ms <= NOT_READY_RETRY_MS)
            break;

        const InterruptCheckCallback& check = interrupt_check();
        if (check && check())
            throw CancelledException();
        gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                                     gpr_time_from_millis(NOT_READY_RETRY_MS, GPR_TIMESPAN)));

        options.deadline_ms = remaining_ms - NOT_READY_RETRY_MS;
        app_status = -1;
        response.response_body.clear();
        status = grpcx::call_unary(
            target_, method, request_json, metadata, options,
            interrupt_check(), &response.response_body, &app_status);
        not_ready_retries++;
    }

    /*
     * NO REPLAY.  datalake_proxy starts the agent lazily, so the first call
     * after a restart can arrive before the agent listens; the REST client
     * covers that window by retrying CURLE_COULDNT_CONNECT.  Here the call is
     * made wait_for_ready (options_for), so it waits for the connection
     * within its one deadline and is then sent exactly once.  Retrying on
     * UNAVAILABLE instead would be wrong: gRPC also reports UNAVAILABLE when
     * the connection breaks after the agent received the call, and replaying
     * a commit then appends its files twice.  Agent-produced 5xx are not
     * retried either (the REST client retries those for idempotent calls
     * only).
     */
    response.retry_count = not_ready_retries;

    gpr_timespec spent = gpr_time_sub(gpr_now(GPR_CLOCK_MONOTONIC), call_start);
    response.total_time = (double) spent.tv_sec + (double) spent.tv_nsec / 1e9;

    if (status.error_code() == grpc::StatusCode::CANCELLED)
    {
        /*
         * Reuse the REST path's cancellation reporting rather than inventing a
         * second one: the operation macros already turn this into
         * AGENT_CLI_ERROR_CANCELLED.
         */
        throw CancelledException();
    }

    if (!status.ok())
    {
        /*
         * A TRANSPORT failure: the agent could not be reached, the deadline
         * expired, the request was malformed.  There is no body to report.
         */
        response.http_status = to_http_status(status);
        response.respond_last_status = to_error_code(status);
        response.error_message = status.error_message();
        return response;
    }

    /*
     * The call succeeded at the transport level, so the operation's own
     * verdict is in the dlagent-status trailer and the body is whatever the
     * REST path would have returned -- byte for byte.  Mapping the trailer
     * through the same table the REST client uses for an HTTP status is what
     * makes the errcode the user finally sees identical under both
     * transports.
     */
    response.http_status = app_status > 0 ? app_status : 200;
    response.respond_last_status = http_status_to_error_code(response.http_status);
    return response;
}

#define GRPC_OP_WITH_TABLE(cpp_name, rpc_name, service)                          \
Response GrpcAgentClient::cpp_name(const std::string& table_name,                \
                                   const std::string& request_json,              \
                                   const RequestConfig* request_config) {        \
    return unary(std::string(service) + rpc_name, table_name, request_json,       \
                 request_config);                                                 \
}

#define GRPC_OP_NO_TABLE(cpp_name, rpc_name, service)                            \
Response GrpcAgentClient::cpp_name(const std::string& request_json,              \
                                   const RequestConfig* request_config) {        \
    return unary(std::string(service) + rpc_name, std::string(), request_json,    \
                 request_config);                                                 \
}

GRPC_OP_NO_TABLE(create_table, "CreateTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(load_table, "LoadTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(load_metadata_json, "LoadMetadataJson", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(table_exists, "TableExists", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(get_fragment, "GetFragment", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(plan_file_groups, "PlanFileGroups", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(commit_file_groups, "CommitFileGroups", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(commit_append, "CommitAppend", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(commit_update, "CommitUpdate", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(commit_rewrite, "CommitRewrite", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(append_table, "AppendTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(update_table, "UpdateTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(truncate_table, "TruncateTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(drop_table, "DropTable", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(submit_maintenance, "SubmitMaintenance", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(update_schema, "UpdateSchema", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(get_statistics, "GetStatistics", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(get_snapshot_schema, "GetSnapshotSchema", SERVICE_CATALOG)
GRPC_OP_WITH_TABLE(get_snapshots, "GetSnapshots", SERVICE_CATALOG)

GRPC_OP_NO_TABLE(create_catalog, "CreateCatalog", SERVICE_MANAGEMENT)
GRPC_OP_NO_TABLE(list_catalogs, "ListCatalogs", SERVICE_MANAGEMENT)
GRPC_OP_NO_TABLE(list_namespaces, "ListNamespaces", SERVICE_MANAGEMENT)
GRPC_OP_NO_TABLE(create_namespace, "CreateNamespace", SERVICE_MANAGEMENT)

/*
 * The file-cleanup pair lives on IcebergCatalog, matching dlagent.proto; they
 * take no table name because they address objects by path.  Both run on
 * error-cleanup paths with interrupts held off, so they are one attempt that
 * fails at once if the agent is down, as under REST.
 */
Response GrpcAgentClient::cleanup_metadata(const std::string& request_json,
                                           const RequestConfig* request_config)
{
    return unary(std::string(SERVICE_CATALOG) + "CleanupMetadata", std::string(),
                 request_json, request_config, /* wait_for_ready */ false);
}

Response GrpcAgentClient::delete_file(const std::string& request_json,
                                      const RequestConfig* request_config)
{
    return unary(std::string(SERVICE_CATALOG) + "DeleteFile", std::string(),
                 request_json, request_config, /* wait_for_ready */ false);
}

#undef GRPC_OP_WITH_TABLE
#undef GRPC_OP_NO_TABLE

}  // namespace agent_cli
