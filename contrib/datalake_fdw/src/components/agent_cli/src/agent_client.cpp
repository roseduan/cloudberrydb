#include <agent_client.hpp>
#include <http_client.hpp>
#include <logger.hpp>
#include <sstream>
#include <stdexcept>

namespace agent_cli {

AgentClient::AgentClient(const Config& config)
    : config_(config), http_client_(new HttpClient(config)) {
}

AgentClient::~AgentClient() = default;

void AgentClient::set_interrupt_callback(InterruptCheckCallback callback) {
    if (http_client_) {
        http_client_->set_interrupt_callback(callback);
    }
}

std::string AgentClient::build_url(const std::string& endpoint) const {
    std::ostringstream oss;
    oss << config_.server_url;
    if (!config_.server_url.empty() && config_.server_url.back() != '/') {
        oss << '/';
    }
    oss << endpoint;
    return oss.str();
}

Response AgentClient::create_table(const std::string& request_json,
                                  const RequestConfig* request_config) {
    std::string url = build_url("api/v1/tables/create");
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::load_table(const std::string& table_name,
                                const std::string& request_json,
                                const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/load";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::table_exists(const std::string& table_name,
                                  const std::string& request_json,
                                  const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/exists";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::get_fragment(const std::string& table_name,
                                  const std::string& request_json,
                                  const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/getFragment";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::plan_file_groups(const std::string& table_name,
                                       const std::string& request_json,
                                       const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/planFileGroups";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::commit_file_groups(const std::string& table_name,
                                          const std::string& request_json,
                                          const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/commitFileGroups";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::commit_append(const std::string& table_name,
                                    const std::string& request_json,
                                    const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/commitAppend";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::commit_update(const std::string& table_name,
                                    const std::string& request_json,
                                    const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/commitUpdate";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::commit_rewrite(const std::string& table_name,
                                     const std::string& request_json,
                                     const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/commitRewrite";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::append_table(const std::string& table_name,
                                  const std::string& request_json,
                                  const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/append";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::update_table(const std::string& table_name,
                                  const std::string& request_json,
                                  const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/update";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::truncate_table(const std::string& table_name,
                                    const std::string& request_json,
                                    const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/truncate";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::drop_table(const std::string& table_name,
                                 const std::string& request_json,
                                 const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/drop";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::get_statistics(const std::string& table_name,
                                     const std::string& request_json,
                                     const RequestConfig* request_config) {
    std::ostringstream endpoint;
    endpoint << "api/v1/tables/" << table_name << "/getStatistics";
    std::string url = build_url(endpoint.str());
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::execute_request(const std::string& method,
                                     const std::string& url,
                                     const std::string& json_data,
                                     const RequestConfig* request_config) {
    total_requests_++;

    LOG_INFO("HTTP " + method + " " + url);

    HttpRequest request;
    request.method = method;
    request.url = url;
    request.body = json_data;
    request.timeout = config_.request_timeout_seconds;
    request.connect_timeout = config_.connect_timeout_seconds;
    if (request_config) {
        if (request_config->request_timeout_seconds > 0) {
            request.timeout = request_config->request_timeout_seconds;
        }

        for (const auto& header : request_config->headers) {
            request.headers[header.first] = header.second;
        }
    }

    HttpResponse http_response = http_client_->execute(request);

    Response response;
    response.http_status = http_response.status_code;
    response.curl_code = http_response.curl_code;
    response.response_body = std::move(http_response.body);
    response.error_message = std::move(http_response.error_message);
    response.total_time = http_response.total_time;

    if (response.http_status >= 400 || response.curl_code != 0) {
        failed_requests_++;
        LOG_ERROR("HTTP request failed: " + method + " " + url + 
                  " -> status=" + std::to_string(response.http_status) + 
                  " curl_code=" + std::to_string(response.curl_code) +
                  (response.error_message.empty() ? "" : " error=" + response.error_message));
    }
    response.respond_last_status = setErrorCode(http_response.status_code, http_response.curl_code);

    LOG_INFO("HTTP " + method + " " + url + " -> " + 
             std::to_string(response.http_status) + 
             " (" + std::to_string(response.total_time) + "s)");

    return response;
}

ErrorCode AgentClient::setErrorCode(int http_status, long curl_code) {
    if (curl_code != 0) {
        switch (curl_code) {
            case 28: // CURLE_OPERATION_TIMEDOUT
                return ErrorCode::TIMEOUT;
            case 6:  // CURLE_COULDNT_RESOLVE_HOST
            case 7:  // CURLE_COULDNT_CONNECT
                return ErrorCode::NETWORK;
            default:
                return ErrorCode::CURL;
        }
    }

    if (http_status >= 200 && http_status < 300) {
        return ErrorCode::SUCCESS;
    }

    switch (http_status) {
        case 400:
            return ErrorCode::BAD_REQUEST;
        case 401:
        case 403:
            return ErrorCode::AUTH;
        case 404:
            return ErrorCode::NOT_FOUND;
        case 409:
            return ErrorCode::CONFLICT;
        case 408:
        case 504:
            return ErrorCode::TIMEOUT;
        default:
            return ErrorCode::HTTP;
    }
    // should not reach here
    return ErrorCode::SUCCESS;
}

void AgentClient::get_stats(long& total_requests, long& failed_requests) const {
    total_requests = total_requests_.load();
    failed_requests = failed_requests_.load();
}

Response AgentClient::create_catalog(const std::string& request_json,
                                    const RequestConfig* request_config) {
    std::string url = build_url("api/v1/catalogs/create");
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::list_catalogs(const std::string& request_json,
                                   const RequestConfig* request_config) {
    std::string url = build_url("api/v1/catalogs/list");
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::list_namespaces(const std::string& request_json,
                                     const RequestConfig* request_config) {
    std::string url = build_url("api/v1/namespaces/list");
    return execute_request("POST", url, request_json, request_config);
}

Response AgentClient::create_namespace(const std::string& request_json,
                                      const RequestConfig* request_config) {
    std::string url = build_url("api/v1/namespaces");
    return execute_request("POST", url, request_json, request_config);
}

} // namespace agent_cli
