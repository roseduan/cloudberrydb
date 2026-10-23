#ifndef HTTP_CLIENT_HPP
#define HTTP_CLIENT_HPP

#include "agent_client.hpp"
#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <functional>
#include <curl/curl.h>

namespace agent_cli {

typedef std::function<bool()> InterruptCheckCallback;

struct HttpRequest {
    std::string method;
    std::string url;
    std::string body;
    std::map<std::string, std::string> headers;
    int timeout = 30;
    int connect_timeout = 10;
    /*
     * False for requests that must not be replayed blindly (Iceberg commits:
     * a retried commit that already applied duplicates data).  Such requests
     * are retried only when the connection could not be established at all,
     * i.e. the server provably never saw them.
     */
    bool idempotent = true;
    /*
     * Never retried, whatever the failure.  For best-effort calls made on
     * error-cleanup paths, where interrupts are held off: waiting out the
     * agent-restart window there would stall a ROLLBACK uncancellably.
     */
    bool single_attempt = false;
};

struct HttpResponse {
    int status_code = 0;
    long curl_code = 0;
    std::string body;
    std::string error_message;
    double total_time = 0.0;
    size_t response_size = 0;
};

class HttpClient {
public:
    explicit HttpClient(const Config& config);
    ~HttpClient();

    HttpResponse execute(const HttpRequest& request);
    void set_interrupt_callback(InterruptCheckCallback callback);

private:
    Config config_;
    CURL* curl_handle_;
    CURLM* multi_handle_;
    int curl_still_running_;
    mutable std::mutex curl_mutex_;
    InterruptCheckCallback interrupt_callback_;
    
    void setup_curl_options();
    static size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* data);
    static int curl_debug_callback(CURL* handle, curl_infotype type, char* data, size_t size, void* userptr);
    
    HttpResponse perform_request_with_retry(const HttpRequest& request);
    HttpResponse perform_single_request(const HttpRequest& request);
    void multi_perform();
    void check_multi_info(HttpResponse* response);
    void check_for_interrupts();
};

} // namespace agent_cli

#endif // HTTP_CLIENT_HPP
