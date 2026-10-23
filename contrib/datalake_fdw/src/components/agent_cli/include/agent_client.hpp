#ifndef AGENT_CLIENT_HPP
#define AGENT_CLIENT_HPP

#include <string>
#include <memory>
#include <mutex>
#include <atomic>
#include <functional>
#include <map>
#include "agent_types.hpp"

namespace agent_cli {

typedef std::function<bool()> InterruptCheckCallback;

struct Config {
    std::string server_url;
    //std::string prefix;
    std::string auth_token;

    // Timeout settings
    int request_timeout_seconds = 300;   // Total request timeout
    int connect_timeout_seconds = 60;   // TCP connection timeout

    // Retry configuration
    int max_retries = 3;
    int retry_delay_ms = 1000;

    /*
     * The gRPC target ("host:port"), set only when
     * datalake.agent_transport = grpc.  It is a separate field rather than a
     * reinterpretation of server_url because the two transports listen on
     * different ports by design: deriving one from the other
     * would mean guessing, and guessing wrong points the client at the other
     * transport's agent, which fails like a network fault.
     */
    std::string grpc_target;
};

struct RequestConfig {
    std::string gopher_config;
    std::string volume_config;
    std::string iceberg_config;
    std::map<std::string, std::string> headers;
    int request_timeout_seconds = 0;    // Override client default (0 = use client default)
};

struct Response {
    int http_status = 0;
    long curl_code = 0;
    std::string response_body;
    std::string error_message;
    std::string curl_error_message;
    double total_time = 0.0;
    int retry_count = 0;
    ErrorCode respond_last_status = ErrorCode::SUCCESS;
};

class HttpClient;

class AgentClient {
public:
    explicit AgentClient(const Config& config);
    /*
     * Virtual: GrpcAgentClient derives from this so the transport is chosen
     * once, when the handle is created, and every agent_cli_wrapper_* signature
     * and all 101 call sites above them stay unchanged.
     */
    virtual ~AgentClient();

    // Set interrupt check callback for cancellation support
    /*
     * Virtual so the gRPC transport can observe cancellation too.  The base
     * keeps its own copy in addition to handing it to the HTTP client: the
     * gRPC path has no HttpClient to delegate to, and reading the callback
     * back is how its poll loop learns the caller wants out.
     */
    virtual void set_interrupt_callback(InterruptCheckCallback callback);

    // All methods are inherently thread-safe
    virtual Response create_table(const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    virtual Response load_table(const std::string& table_name,
                       const std::string& request_json,
                       const RequestConfig* request_config = nullptr);

    virtual Response table_exists(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    virtual Response get_fragment(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    // REST catalog gateway support (#382/#935): fetch a builtin table's
    // metadata.json verbatim so the gateway need not hold object storage
    // credentials of its own.
    virtual Response load_metadata_json(const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config = nullptr);

    virtual Response plan_file_groups(const std::string& table_name,
                              const std::string& request_json,
                              const RequestConfig* request_config = nullptr);

    virtual Response commit_file_groups(const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config = nullptr);

    virtual Response commit_append(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    virtual Response commit_update(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    virtual Response commit_rewrite(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    virtual Response submit_maintenance(const std::string& table_name,
                                        const std::string& request_json,
                                        const RequestConfig* request_config = nullptr);

    virtual Response append_table(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);
    
    virtual Response update_table(const std::string& table_name,
                          const std::string& request_json,
                          const RequestConfig* request_config = nullptr);

    virtual Response truncate_table(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    virtual Response drop_table(const std::string& table_name,
                        const std::string& request_json,
                        const RequestConfig* request_config = nullptr);

    virtual Response update_schema(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    virtual Response get_statistics(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    virtual Response get_snapshot_schema(const std::string& table_name,
                                 const std::string& request_json,
                                 const RequestConfig* request_config = nullptr);

    virtual Response get_snapshots(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    // Catalog management methods
    virtual Response create_catalog(const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    virtual Response list_catalogs(const std::string& request_json,
                          const RequestConfig* request_config = nullptr);

    virtual Response list_namespaces(const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    virtual Response create_namespace(const std::string& request_json,
                             const RequestConfig* request_config = nullptr);

    // File-level cleanup.
    //
    // Ordinary operations like any other, so the transport picks the
    // implementation.  As raw libcurl POSTs in agent_c_api.cpp they would keep
    // talking HTTP to the REST agent even under datalake.agent_transport=grpc
    // -- where nothing listens on the REST port, so every metadata cleanup
    // would fail and orphaned files would pile up.
    virtual Response cleanup_metadata(const std::string& request_json,
                             const RequestConfig* request_config = nullptr);

    virtual Response delete_file(const std::string& request_json,
                             const RequestConfig* request_config = nullptr);

    // Statistics (thread-safe)
    void get_stats(long& total_requests, long& failed_requests) const;

protected:
    Config config_;
    /** The callback last given to set_interrupt_callback; may be empty. */
    InterruptCheckCallback interrupt_check_;
    const InterruptCheckCallback& interrupt_check() const { return interrupt_check_; }
    // Percent-encode a single URL path segment (e.g. a table name) so that
    // reserved characters are not interpreted as URL syntax. (Issue #369.)
    // The gRPC client sends the table name encoded the same way, which also
    // keeps it within the printable ASCII that gRPC metadata allows.
    static std::string encode_path_segment(const std::string& segment);

private:
    std::unique_ptr<HttpClient> http_client_;
    
    // Thread-safe statistics
    mutable std::mutex stats_mutex_;
    std::atomic<long> total_requests_{0};
    std::atomic<long> failed_requests_{0};

    std::string build_url(const std::string& endpoint) const;
    // Build "api/v1/tables/<encoded table_name>/<op>".
    std::string build_table_endpoint(const std::string& table_name,
                                     const std::string& op) const;
    Response execute_request(const std::string& method,
                           const std::string& url,
                           const std::string& json_data,
                           const RequestConfig* request_config,
                           bool idempotent = true,
                           bool single_attempt = false);
    ErrorCode setErrorCode(int http_status, long curl_code);
};

} // namespace agent_cli

#endif // AGENT_CLIENT_HPP
