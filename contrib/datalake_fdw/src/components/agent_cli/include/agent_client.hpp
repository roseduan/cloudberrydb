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
    ~AgentClient();

    // Set interrupt check callback for cancellation support
    void set_interrupt_callback(InterruptCheckCallback callback);

    // All methods are inherently thread-safe
    Response create_table(const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    Response load_table(const std::string& table_name,
                       const std::string& request_json,
                       const RequestConfig* request_config = nullptr);

    Response table_exists(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    Response get_fragment(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);

    // REST catalog gateway support (#382/#935): fetch a builtin table's
    // metadata.json verbatim so the gateway need not hold object storage
    // credentials of its own.
    Response load_metadata_json(const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config = nullptr);

    Response plan_file_groups(const std::string& table_name,
                              const std::string& request_json,
                              const RequestConfig* request_config = nullptr);

    Response commit_file_groups(const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config = nullptr);

    Response commit_append(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    Response commit_update(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    Response commit_rewrite(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    Response append_table(const std::string& table_name,
                         const std::string& request_json,
                         const RequestConfig* request_config = nullptr);
    
    Response update_table(const std::string& table_name,
                          const std::string& request_json,
                          const RequestConfig* request_config = nullptr);

    Response truncate_table(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    Response drop_table(const std::string& table_name,
                        const std::string& request_json,
                        const RequestConfig* request_config = nullptr);

    Response update_schema(const std::string& table_name,
                           const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    Response get_statistics(const std::string& table_name,
                            const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    // Catalog management methods
    Response create_catalog(const std::string& request_json,
                           const RequestConfig* request_config = nullptr);

    Response list_catalogs(const std::string& request_json,
                          const RequestConfig* request_config = nullptr);

    Response list_namespaces(const std::string& request_json,
                            const RequestConfig* request_config = nullptr);

    Response create_namespace(const std::string& request_json,
                             const RequestConfig* request_config = nullptr);

    // Statistics (thread-safe)
    void get_stats(long& total_requests, long& failed_requests) const;

private:
    Config config_;
    std::unique_ptr<HttpClient> http_client_;
    
    // Thread-safe statistics
    mutable std::mutex stats_mutex_;
    std::atomic<long> total_requests_{0};
    std::atomic<long> failed_requests_{0};

    std::string build_url(const std::string& endpoint) const;
    // Percent-encode a single URL path segment (e.g. a table name) so that
    // reserved characters are not interpreted as URL syntax. (Issue #369.)
    static std::string encode_path_segment(const std::string& segment);
    // Build "api/v1/tables/<encoded table_name>/<op>".
    std::string build_table_endpoint(const std::string& table_name,
                                     const std::string& op) const;
    Response execute_request(const std::string& method,
                           const std::string& url,
                           const std::string& json_data,
                           const RequestConfig* request_config);
    ErrorCode setErrorCode(int http_status, long curl_code);
};

} // namespace agent_cli

#endif // AGENT_CLIENT_HPP
