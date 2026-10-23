/*-------------------------------------------------------------------------
 *
 * grpc_agent_client.hpp
 *    The AgentClient implementation that speaks gRPC.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_agent_client.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef AGENT_CLI_GRPC_AGENT_CLIENT_HPP
#define AGENT_CLI_GRPC_AGENT_CLIENT_HPP

#include "agent_client.hpp"
#include "grpc_transport.hpp"

namespace agent_cli {

/**
 * The gRPC transport, as a drop-in for {@link AgentClient}.
 *
 * <p>Subclassing rather than branching inside each method is what keeps the C
 * ABI untouched: {@code agent_c_api.cpp}'s operation macros already call
 * {@code context->client->some_method(...)}, so choosing the implementation at
 * handle-creation time makes every {@code agent_cli_wrapper_*} signature, and
 * the 101 call sites above them, unchanged.
 *
 * <p>Cancellation reuses the existing path exactly: on
 * {@code StatusCode::CANCELLED} this throws {@link CancelledException}, which
 * the operation macros already catch and turn into
 * {@code AGENT_CLI_ERROR_CANCELLED}. No macro changes, and the REST and gRPC
 * paths cannot drift apart on how a cancel is reported.
 */
class GrpcAgentClient : public AgentClient {
public:
    explicit GrpcAgentClient(const Config& config);
    ~GrpcAgentClient();

    Response create_table(const std::string& request_json,
                          const RequestConfig* request_config = nullptr) override;
    Response load_table(const std::string& table_name, const std::string& request_json,
                        const RequestConfig* request_config = nullptr) override;
    Response load_metadata_json(const std::string& table_name, const std::string& request_json,
                                const RequestConfig* request_config = nullptr) override;
    Response table_exists(const std::string& table_name, const std::string& request_json,
                          const RequestConfig* request_config = nullptr) override;
    Response get_fragment(const std::string& table_name, const std::string& request_json,
                          const RequestConfig* request_config = nullptr) override;
    Response plan_file_groups(const std::string& table_name, const std::string& request_json,
                              const RequestConfig* request_config = nullptr) override;
    Response commit_file_groups(const std::string& table_name, const std::string& request_json,
                                const RequestConfig* request_config = nullptr) override;
    Response commit_append(const std::string& table_name, const std::string& request_json,
                           const RequestConfig* request_config = nullptr) override;
    Response commit_update(const std::string& table_name, const std::string& request_json,
                           const RequestConfig* request_config = nullptr) override;
    Response commit_rewrite(const std::string& table_name, const std::string& request_json,
                            const RequestConfig* request_config = nullptr) override;
    Response append_table(const std::string& table_name, const std::string& request_json,
                          const RequestConfig* request_config = nullptr) override;
    Response update_table(const std::string& table_name, const std::string& request_json,
                          const RequestConfig* request_config = nullptr) override;
    Response truncate_table(const std::string& table_name, const std::string& request_json,
                            const RequestConfig* request_config = nullptr) override;
    Response drop_table(const std::string& table_name, const std::string& request_json,
                        const RequestConfig* request_config = nullptr) override;
    Response submit_maintenance(const std::string& table_name, const std::string& request_json,
                                const RequestConfig* request_config = nullptr) override;
    Response update_schema(const std::string& table_name, const std::string& request_json,
                           const RequestConfig* request_config = nullptr) override;
    Response get_statistics(const std::string& table_name, const std::string& request_json,
                            const RequestConfig* request_config = nullptr) override;
    Response get_snapshot_schema(const std::string& table_name, const std::string& request_json,
                                 const RequestConfig* request_config = nullptr) override;
    Response get_snapshots(const std::string& table_name, const std::string& request_json,
                           const RequestConfig* request_config = nullptr) override;
    Response create_catalog(const std::string& request_json,
                            const RequestConfig* request_config = nullptr) override;
    Response list_catalogs(const std::string& request_json,
                           const RequestConfig* request_config = nullptr) override;
    Response list_namespaces(const std::string& request_json,
                             const RequestConfig* request_config = nullptr) override;
    Response create_namespace(const std::string& request_json,
                              const RequestConfig* request_config = nullptr) override;
    Response cleanup_metadata(const std::string& request_json,
                              const RequestConfig* request_config = nullptr) override;
    Response delete_file(const std::string& request_json,
                         const RequestConfig* request_config = nullptr) override;

    /** The gRPC target ("host:port") derived from a datalake.agent_server_url. */
    static std::string target_from_url(const std::string& server_url);

private:
    /**
     * wait_for_ready=false fails at once when the agent is not listening;
     * used for best-effort cleanup calls, as the REST client makes them a
     * single attempt.
     */
    Response unary(const std::string& method, const std::string& table_name,
                   const std::string& request_json, const RequestConfig* request_config,
                   bool wait_for_ready = true);
    /** One message per element, concatenated into a JSON array for the caller. */
    Response streaming(const std::string& method, const std::string& table_name,
                       const std::string& request_json, const RequestConfig* request_config);

    grpcx::CallOptions options_for(const RequestConfig* request_config) const;

    std::string target_;
};

}  // namespace agent_cli

#endif  /* AGENT_CLI_GRPC_AGENT_CLIENT_HPP */
