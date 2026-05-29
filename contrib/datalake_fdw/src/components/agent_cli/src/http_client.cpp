#include <http_client.hpp>
#include <agent_types.hpp>
#include <logger.hpp>
#include <sstream>
#include <thread>
#include <chrono>
#include <sys/select.h>
#include <errno.h>

namespace agent_cli {

HttpClient::HttpClient(const Config& config) : config_(config), multi_handle_(nullptr), curl_still_running_(0) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl_handle_ = curl_easy_init();
    if (!curl_handle_) {
        throw std::runtime_error("Failed to initialize CURL");
    }

    multi_handle_ = curl_multi_init();
    if (!multi_handle_) {
        curl_easy_cleanup(curl_handle_);
        throw std::runtime_error("Failed to initialize CURL multi handle");
    }

    setup_curl_options();
}

void HttpClient::set_interrupt_callback(InterruptCheckCallback callback) {
    interrupt_callback_ = callback;
}

void HttpClient::check_for_interrupts() {
    if (interrupt_callback_) {
        if (interrupt_callback_()) {
            LOG_WARN("Request cancelled by interrupt callback");
            throw CancelledException();
        }
    }
}

HttpClient::~HttpClient() {
    if (multi_handle_) {
        curl_multi_cleanup(multi_handle_);
    }
    if (curl_handle_) {
        curl_easy_cleanup(curl_handle_);
    }
    curl_global_cleanup();
}

void HttpClient::setup_curl_options() {
    curl_easy_setopt(curl_handle_, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl_handle_, CURLOPT_MAXREDIRS, 5L);
    curl_easy_setopt(curl_handle_, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl_handle_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION, write_callback);

    if (Logger::instance().is_debug_enabled()) {
        curl_easy_setopt(curl_handle_, CURLOPT_VERBOSE, 1L);
        curl_easy_setopt(curl_handle_, CURLOPT_DEBUGFUNCTION, curl_debug_callback);
        curl_easy_setopt(curl_handle_, CURLOPT_DEBUGDATA, nullptr);
    }
}

int HttpClient::curl_debug_callback(CURL* /*handle*/, curl_infotype type,
                                    char* data, size_t size, void* /*userptr*/) {
    std::string msg(data, size);
    while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r')) {
        msg.pop_back();
    }
    if (msg.empty()) return 0;

    // Sanitize sensitive headers in output
    if (type == CURLINFO_HEADER_OUT || type == CURLINFO_HEADER_IN) {
        std::string lower_msg = msg;
        for (size_t i = 0; i < lower_msg.size(); ++i) {
            lower_msg[i] = std::tolower(lower_msg[i]);
        }
        if (lower_msg.find("authorization:") != std::string::npos ||
            lower_msg.find("x-auth-token:") != std::string::npos ||
            lower_msg.find("x-api-key:") != std::string::npos ||
            lower_msg.find("cookie:") != std::string::npos) {
            size_t colon_pos = msg.find(':');
            if (colon_pos != std::string::npos) {
                msg = msg.substr(0, colon_pos + 1) + " ***REDACTED***";
            }
        }
    }

    switch(type) {
        case CURLINFO_TEXT:
            LOG_DEBUG("[CURL] " + msg);
            break;
        case CURLINFO_HEADER_OUT:
            LOG_DEBUG("[CURL =>] " + msg);
            break;
        case CURLINFO_DATA_OUT:
            if (size > 512) {
                LOG_DEBUG("[CURL => DATA] " + msg.substr(0, 512) + "... (truncated)");
            } else {
                LOG_DEBUG("[CURL => DATA] " + msg);
            }
            break;
        case CURLINFO_HEADER_IN:
            LOG_DEBUG("[CURL <=] " + msg);
            break;
        case CURLINFO_DATA_IN:
            if (size > 512) {
                LOG_DEBUG("[CURL <= DATA] " + msg.substr(0, 512) + "... (truncated)");
            } else {
                LOG_DEBUG("[CURL <= DATA] " + msg);
            }
            break;
        default:
            break;
    }
    return 0;
}

size_t HttpClient::write_callback(void* contents, size_t size, size_t nmemb, std::string* data) {
    size_t total_size = size * nmemb;
    data->append(static_cast<char*>(contents), total_size);
    return total_size;
}

HttpResponse HttpClient::execute(const HttpRequest& request) {
    return perform_request_with_retry(request);
}

HttpResponse HttpClient::perform_request_with_retry(const HttpRequest& request) {
    HttpResponse response;
    int attempts = 0;
    int max_attempts = 1;
    if (config_.max_retries > 1) {
        max_attempts = config_.max_retries;
    }

    /*
     * The dlagent JVM is fork+exec'd lazily by the datalake_proxy bgworker as
     * a child of each PG backend; Spring Boot needs ~7-10s to bind the
     * Tomcat port (3888). Under workloads that churn backends quickly
     * (pg_regress in particular, where short-lived connections rapid-fire
     * SQL and each backend death takes its child agent with it), a single
     * iceberg op can land in a window where the previous agent has just
     * died and the new one isn't bound yet. The default
     * max_retries=3 * retry_delay_ms=1000 = 3s budget is way too short, so
     * the call surfaces a CURLE_COULDNT_CONNECT (libcurl code 7) to the
     * SQL caller as a hard failure.
     *
     * Detect that condition specifically and stretch the budget to ~60s, so
     * back-to-back agent reboots have time to settle. Other errors (4xx/5xx
     * body responses, network mid-flight failures) keep the original
     * max_attempts since they're not "service starting up" conditions.
     */
    constexpr int CONN_REFUSED_EXTRA_ATTEMPTS = 60;

    while (attempts < max_attempts) {
        response = perform_single_request(request);

        if (response.curl_code == CURLE_OK && response.status_code < 500) {
            break;
        }

        if (response.curl_code == CURLE_COULDNT_CONNECT &&
            max_attempts < CONN_REFUSED_EXTRA_ATTEMPTS) {
            max_attempts = CONN_REFUSED_EXTRA_ATTEMPTS;
        }

        attempts++;
        if (attempts < max_attempts) {
            LOG_WARN("Request failed (attempt " + std::to_string(attempts) + "/" +
                     std::to_string(max_attempts) + "), retrying in " +
                     std::to_string(config_.retry_delay_ms) + "ms. Status: " +
                     std::to_string(response.status_code) + ", curl_code: " +
                     std::to_string(response.curl_code));
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.retry_delay_ms));
        } else {
            LOG_ERROR("Request failed after " + std::to_string(attempts) + " attempts");
        }
    }

    return response;
}

HttpResponse HttpClient::perform_single_request(const HttpRequest& request) {
    std::lock_guard<std::mutex> lock(curl_mutex_);

    if (Logger::instance().is_debug_enabled()) {
        LOG_DEBUG("==> HTTP Request");
        LOG_DEBUG("URL: " + request.url);
        LOG_DEBUG("Method: " + request.method);
        if (!request.body.empty()) {
            if (request.body.size() > 1024) {
                LOG_DEBUG("Body: " + request.body.substr(0, 1024) + "... (truncated)");
            } else {
                LOG_DEBUG("Body: " + request.body);
            }
        }
    }

    HttpResponse response;
    std::string response_body;

    curl_easy_reset(curl_handle_);
    setup_curl_options();

    curl_easy_setopt(curl_handle_, CURLOPT_URL, request.url.c_str());
    curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA, &response_body);

    if (request.method == "POST") {
        curl_easy_setopt(curl_handle_, CURLOPT_POST, 1L);
        if (!request.body.empty()) {
            curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, request.body.c_str());
            curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDSIZE, request.body.length());
            if (Logger::instance().is_debug_enabled()) {
                LOG_DEBUG(request.body);
            }
        }
    } else if (request.method == "GET") {
        curl_easy_setopt(curl_handle_, CURLOPT_HTTPGET, 1L);
    }

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    if (Logger::instance().is_debug_enabled()) {
        LOG_DEBUG("Headers: Content-Type: application/json; Accept: application/json");
    }

    if (!config_.auth_token.empty()) {
        std::string auth_header = "Authorization: Bearer " + config_.auth_token;
        headers = curl_slist_append(headers, auth_header.c_str());
        if (Logger::instance().is_debug_enabled()) {
            LOG_DEBUG("Headers: Authorization: Bearer ***REDACTED***");
        }
    }

    for (std::map<std::string, std::string>::const_iterator it = request.headers.begin();
         it != request.headers.end(); ++it) {
        std::string header_str = it->first + ": " + it->second;
        headers = curl_slist_append(headers, header_str.c_str());
        if (Logger::instance().is_debug_enabled()) {
            // Sanitize sensitive headers
            if (it->first == "Authorization" || it->first == "X-Auth-Token" || 
                it->first == "X-API-Key" || it->first == "Cookie") {
                LOG_DEBUG("Headers: " + it->first + ": ***REDACTED***");
            } else {
                LOG_DEBUG("Headers: " + header_str);
            }
        }
    }

    curl_easy_setopt(curl_handle_, CURLOPT_HTTPHEADER, headers);

    int timeout = request.timeout > 0 ? request.timeout : config_.request_timeout_seconds;
    int connect_timeout = request.connect_timeout > 0 ? request.connect_timeout : config_.connect_timeout_seconds;

    curl_easy_setopt(curl_handle_, CURLOPT_TIMEOUT, timeout);
    curl_easy_setopt(curl_handle_, CURLOPT_CONNECTTIMEOUT, connect_timeout);

    std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();

    // Use multi-curl for better performance
    CURLMcode multi_code = curl_multi_add_handle(multi_handle_, curl_handle_);
    if (multi_code != CURLM_OK) {
        curl_slist_free_all(headers);
        response.curl_code = CURLE_FAILED_INIT;
        response.error_message = "Failed to add handle to multi";
        return response;
    }

    // Perform multi-curl operations
    multi_perform();

    // Wait for completion with timeout handling
    while (curl_still_running_ > 0) {
        check_for_interrupts();

        fd_set fdread, fdwrite, fdexcep;
        int maxfd = -1;
        long curl_timeo = -1;

        FD_ZERO(&fdread);
        FD_ZERO(&fdwrite);
        FD_ZERO(&fdexcep);

        struct timeval timeout_tv;
        timeout_tv.tv_sec = 1;
        timeout_tv.tv_usec = 0;

        curl_multi_timeout(multi_handle_, &curl_timeo);
        if (curl_timeo >= 0) {
            timeout_tv.tv_sec = curl_timeo / 1000;
            if (timeout_tv.tv_sec > 1) {
                timeout_tv.tv_sec = 1;
            } else {
                timeout_tv.tv_usec = (curl_timeo % 1000) * 1000;
            }
        }

        curl_multi_fdset(multi_handle_, &fdread, &fdwrite, &fdexcep, &maxfd);

        if (maxfd == -1) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        } else {
            int rc = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout_tv);
            if (rc == -1 && errno != EINTR && errno != EAGAIN) {
                break;
            }
        }

        multi_perform();
    }

    std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    response.total_time = duration.count() / 1000.0;

    // Check for transfer completion and errors
    check_multi_info(&response);

    long http_code = 0;
    curl_easy_getinfo(curl_handle_, CURLINFO_RESPONSE_CODE, &http_code);

    response.status_code = static_cast<int>(http_code);
    response.body = response_body;
    response.response_size = response_body.size();

    // Remove handle from multi
    curl_multi_remove_handle(multi_handle_, curl_handle_);
    curl_slist_free_all(headers);

    if (Logger::instance().is_debug_enabled()) {
        LOG_DEBUG("<== HTTP Response");
        LOG_DEBUG("Status: " + std::to_string(response.status_code));
        LOG_DEBUG("Time: " + std::to_string(response.total_time) + "s");
        if (!response.body.empty()) {
            if (response.body.size() > 1024) {
                LOG_DEBUG("Body: " + response.body.substr(0, 1024) + "... (truncated)");
            } else {
                LOG_DEBUG("Body: " + response.body);
            }
        }
        if (!response.error_message.empty()) {
            LOG_DEBUG("Error: " + response.error_message);
        }
    }

    return response;
}

void HttpClient::multi_perform() {
    CURLMcode mc;
    do {
        mc = curl_multi_perform(multi_handle_, &curl_still_running_);
    } while (mc == CURLM_CALL_MULTI_PERFORM);

    if (mc != CURLM_OK) {
        Logger::instance().error("curl_multi_perform failed: " + std::string(curl_multi_strerror(mc)));
    }
}

void HttpClient::check_multi_info(HttpResponse* response) {
    CURLMsg* msg;
    int msgs_left;

    while ((msg = curl_multi_info_read(multi_handle_, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            response->curl_code = static_cast<long>(msg->data.result);
            if (msg->data.result != CURLE_OK) {
                response->error_message = curl_easy_strerror(msg->data.result);
            }
            break;
        }
    }
}

} // namespace agent_cli
