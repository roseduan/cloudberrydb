/*-------------------------------------------------------------------------
 *
 * grpc_stream.cpp
 *    The data plane over gRPC, shaped like libchurl.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_stream.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "grpc_stream.hpp"
#include "grpc_transport.hpp"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <new>
#include <string>

namespace {

using agent_cli::grpcx::CallOptions;
using agent_cli::grpcx::call_server_streaming;

const char* const METHOD_READ = "/dlagent.v1.DataProxy/Read";
const char* const METHOD_WRITE = "/dlagent.v1.DataProxy/Write";

struct Headers {
    std::map<std::string, std::string> values;
};

struct Stream {
    std::string target;
    bool is_write = false;
    std::map<std::string, std::string> metadata;
    bool (*interrupt)(void) = nullptr;

    /* Request body accumulated by dl_grpc_stream_write(). */
    std::string request;

    /*
     * Response bytes not yet handed to the caller.
     *
     * The call runs to completion inside the first read, and the chunks are
     * queued here.  That is a deliberate simplification of what libchurl does
     * (which interleaves), and it is sound for this boundary because the two
     * POST paths in protocol.c already send a bounded request and then read
     * the reply -- there is no interleaving to preserve.  It also keeps the
     * whole RPC inside one call_server_streaming(), so the cancellation and
     * deadline behaviour is the one grpc_transport implements rather than a
     * second, subtly different one here.
     */
    std::deque<std::string> chunks;
    size_t chunk_offset = 0;
    bool executed = false;

    bool failed = false;
    std::string error;

    /* The agent's trailing metadata, for the error document it may carry. */
    std::map<std::string, std::string> trailers;
};

/*
 * HTTP/1 connection-level headers.  protocol.c sets some of them for libcurl
 * (Connection: close on every request); in HTTP/2 they are malformed, and
 * the agent resets any stream that carries one.  They mean nothing to the
 * agent, so they are dropped here.
 */
bool is_hop_by_hop(const std::string& lower)
{
    static const char* const names[] = {
        "connection", "keep-alive", "proxy-connection", "transfer-encoding",
        "te", "upgrade", "host"
    };
    for (const char* name : names)
        if (lower == name)
            return true;
    return false;
}

Stream* as_stream(DL_GRPC_STREAM handle)
{
    return static_cast<Stream*>(handle);
}

void mark_failed(Stream* stream, const char* what)
{
    stream->failed = true;
    stream->error = what;
    /* Whatever arrived before the failure must not be read as a short reply. */
    stream->chunks.clear();
    stream->chunk_offset = 0;
}

void execute_call(Stream* stream);

/*
 * Runs the call once.  No C++ exception may leave this file: the callers are
 * C (libchurl.c), and an exception unwinding through C frames terminates the
 * backend -- and with it the cluster -- instead of failing one statement.  A
 * std::bad_alloc while buffering a large reply is the realistic one.
 */
void execute(Stream* stream)
{
    if (stream->executed)
        return;
    stream->executed = true;
    try
    {
        execute_call(stream);
    }
    catch (const std::bad_alloc&)
    {
        mark_failed(stream, "out of memory while receiving the agent's reply");
    }
    catch (const std::exception& e)
    {
        mark_failed(stream, e.what());
    }
    catch (...)
    {
        mark_failed(stream, "unexpected error in the gRPC data plane");
    }
}

void execute_call(Stream* stream)
{

    /*
     * No deadline and no idle limit, as under REST, where libchurl sets no
     * timeout on the data plane: the agent may plan for minutes before its
     * first byte.  A cancel still ends the call through the interrupt check.
     */
    CallOptions options;
    options.deadline_ms = 0;
    options.stream_idle_timeout_ms = 0;
    agent_cli::grpcx::InterruptCheck interrupted;
    if (stream->interrupt != nullptr)
    {
        bool (*fn)(void) = stream->interrupt;
        interrupted = [fn]() -> bool { return fn(); };
    }

    grpc::Status status = call_server_streaming(
        stream->target,
        stream->is_write ? METHOD_WRITE : METHOD_READ,
        stream->request,
        stream->metadata,
        options,
        interrupted,
        [stream](const std::string& message) -> bool {
            stream->chunks.push_back(message);
            return true;
        },
        &stream->trailers);

    if (!status.ok())
        mark_failed(stream, status.error_message().empty()
                    ? "gRPC data-plane call failed"
                    : status.error_message().c_str());
}

}  // namespace

extern "C" {

DL_GRPC_HEADERS dl_grpc_headers_init(void)
{
    return new (std::nothrow) Headers();
}

void dl_grpc_headers_append(DL_GRPC_HEADERS headers, const char *key, const char *value)
{
    if (headers == nullptr || key == nullptr)
        return;
    /*
     * gRPC metadata keys must be lower-case; the HTTP header names protocol.c
     * builds are mixed case.  Normalising here rather than at the 31 append
     * call sites keeps those untouched.
     */
    try
    {
        std::string name(key);
        for (size_t i = 0; i < name.size(); i++)
            name[i] = (char) tolower((unsigned char) name[i]);
        if (is_hop_by_hop(name))
            return;
        static_cast<Headers*>(headers)->values[name] = value ? value : "";
    }
    catch (...)
    {
        /* Only bad_alloc is possible; the call then fails on the agent side
         * for a missing parameter, which is an error, not a crash. */
    }
}

void dl_grpc_headers_override(DL_GRPC_HEADERS headers, const char *key, const char *value)
{
    dl_grpc_headers_append(headers, key, value);
}

void dl_grpc_headers_remove(DL_GRPC_HEADERS headers, const char *key)
{
    if (headers == nullptr || key == nullptr)
        return;
    try
    {
        std::string name(key);
        for (size_t i = 0; i < name.size(); i++)
            name[i] = (char) tolower((unsigned char) name[i]);
        static_cast<Headers*>(headers)->values.erase(name);
    }
    catch (...)
    {
    }
}

void dl_grpc_headers_cleanup(DL_GRPC_HEADERS headers)
{
    delete static_cast<Headers*>(headers);
}

void dl_grpc_set_max_message_size(int bytes)
{
    agent_cli::grpcx::set_max_message_size(bytes);
}

DL_GRPC_STREAM dl_grpc_stream_start(const char *target, bool is_write,
                                    DL_GRPC_HEADERS headers,
                                    bool (*interrupt)(void))
{
    Stream* stream = new (std::nothrow) Stream();
    if (stream == nullptr)
        return nullptr;
    try
    {
        stream->target = target ? target : "";
        stream->is_write = is_write;
        stream->interrupt = interrupt;
        if (headers != nullptr)
            stream->metadata = static_cast<Headers*>(headers)->values;
    }
    catch (...)
    {
        stream->executed = true;
        mark_failed(stream, "out of memory while preparing the gRPC call");
    }
    return stream;
}

size_t dl_grpc_stream_write(DL_GRPC_STREAM handle, const char *buf, size_t len)
{
    Stream* stream = as_stream(handle);
    if (stream == nullptr)
        return 0;
    if (buf == nullptr || len == 0)
    {
        /* libchurl's "end of upload" signal; nothing to do here. */
        return 0;
    }
    if (stream->failed)
        return 0;
    try
    {
        stream->request.append(buf, len);
    }
    catch (...)
    {
        stream->executed = true;
        mark_failed(stream, "out of memory while buffering the request");
        return 0;
    }
    return len;
}

size_t dl_grpc_stream_read(DL_GRPC_STREAM handle, char *buf, size_t max_size)
{
    Stream* stream = as_stream(handle);
    if (stream == nullptr || buf == nullptr || max_size == 0)
        return 0;

    execute(stream);

    size_t written = 0;
    while (written < max_size && !stream->chunks.empty())
    {
        const std::string& chunk = stream->chunks.front();
        size_t available = chunk.size() - stream->chunk_offset;
        size_t take = available < (max_size - written) ? available : (max_size - written);
        memcpy(buf + written, chunk.data() + stream->chunk_offset, take);
        written += take;
        stream->chunk_offset += take;
        if (stream->chunk_offset == chunk.size())
        {
            stream->chunks.pop_front();
            stream->chunk_offset = 0;
        }
    }
    return written;
}

bool dl_grpc_stream_failed(DL_GRPC_STREAM handle, const char **msg)
{
    Stream* stream = as_stream(handle);
    if (stream == nullptr)
    {
        if (msg != nullptr)
            *msg = "could not allocate the gRPC stream";
        return true;
    }
    /*
     * The call runs on first use.  Asking whether it failed is a use: the GET
     * path checks before it reads, exactly as REST checks the HTTP status
     * before handing out the body, and must see the outcome.
     */
    execute(stream);
    if (stream->failed && msg != nullptr)
        *msg = stream->error.c_str();
    return stream->failed;
}

bool dl_grpc_stream_error_document(DL_GRPC_STREAM handle, long *http_status,
                                   const char **body, size_t *body_len)
{
    Stream* stream = as_stream(handle);
    if (stream == nullptr || !stream->failed)
        return false;
    std::map<std::string, std::string>::const_iterator st =
        stream->trailers.find(DL_GRPC_TRAILER_STATUS);
    if (st == stream->trailers.end())
        return false;
    *http_status = strtol(st->second.c_str(), nullptr, 10);
    std::map<std::string, std::string>::const_iterator doc =
        stream->trailers.find(DL_GRPC_TRAILER_ERROR_DOCUMENT);
    if (doc != stream->trailers.end())
    {
        *body = doc->second.c_str();
        *body_len = doc->second.size();
    }
    else
    {
        *body = "";
        *body_len = 0;
    }
    return *http_status > 0;
}

void dl_grpc_stream_cleanup(DL_GRPC_STREAM handle, bool after_error)
{
    (void) after_error;
    delete as_stream(handle);
}

}  // extern "C"
