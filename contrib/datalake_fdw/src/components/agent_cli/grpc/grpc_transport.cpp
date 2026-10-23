/*-------------------------------------------------------------------------
 *
 * grpc_transport.cpp
 *    Channel cache and interruptible async calls for the datalake agent's
 *    gRPC transport.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_transport.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "grpc_transport.hpp"

#include <unistd.h>

#include <mutex>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <cstdlib>

#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>

namespace agent_cli {
namespace grpcx {

namespace {

struct CachedChannel {
    std::shared_ptr<grpc::Channel> channel;
    pid_t owner_pid;
};

std::mutex g_cache_mutex;
std::unordered_map<std::string, CachedChannel> g_cache;

/*
 * The fallback ceiling, used only if nothing ever calls
 * set_max_message_size() -- the unit tests in this directory, which build a
 * channel without a PG backend around them.
 *
 * It repeats DATALAKE_AGENT_DEFAULT_RPC_MAX_MESSAGE_SIZE_MB from
 * dlagent_transport.h rather than including it, because that header reaches
 * pg_strcasecmp and this translation unit must not pull in postgres.h (see
 * the macro collision documented in agent_c_api.cpp).  The two are checked
 * against each other by the regression suite, not by the compiler.
 */
const int DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

/*
 * Set from the GUC before any channel exists, by whichever plane calls
 * first; see set_max_message_size() in the header.  Plain int rather than
 * atomic: it is written on the backend's thread, always with the same value,
 * and read on the same thread thereafter.
 */
int g_max_message_size = DEFAULT_MAX_MESSAGE_SIZE;

/* What we accept in received metadata; see default_channel_args(). */
const int MAX_METADATA_SIZE = 4 * 1024 * 1024;

int max_message_size()
{
    return g_max_message_size > 0 ? g_max_message_size : DEFAULT_MAX_MESSAGE_SIZE;
}

grpc::ChannelArguments default_channel_args()
{
    grpc::ChannelArguments args;

    /*
     * Never route agent traffic through an HTTP proxy.  libchurl.c sets
     * CURLOPT_PROXY to "" for exactly this reason on the REST path, and the
     * gRPC client has to match: the agent is a loopback process, and with
     * http_proxy set in the backend's environment every RPC would be hijacked
     * to an external proxy.  This was measured, not imagined -- it broke an
     * early probe run.
     */
    args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);

    /*
     * The agent is a local process that datalake_proxy restarts within
     * seconds, so reconnect on a short, flat schedule.  gRPC's default
     * backoff grows to 120 s, which would keep a call waiting (see
     * CallOptions::wait_for_ready) long after the agent is back.
     */
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1000);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);

    /*
     * A bounded message size, configurable on both sides.  The default 4 MiB
     * is too small for a large Iceberg file list, and silently truncating one
     * would be worse than failing.
     *
     * "On both sides" is the part that was missing while this was a literal
     * here: grpc enforces the limit on whichever end RECEIVES, so a client
     * raised to 64 MB against a grpc-java server left at its 4 MiB default
     * failed every request above 4 MiB with RESOURCE_EXHAUSTED -- under grpc
     * only, for a request rest accepts.  The value now comes from
     * datalake.rpc_max_message_size, which datalake_proxy also puts on the
     * agent's command line, so the two cannot be moved apart.
     */
    int limit = max_message_size();

    /*
     * Only what we SEND is bounded by it, because the agent receives with the
     * same limit.  What we receive is not: REST puts no ceiling on a reply,
     * and a fragment list or metadata document for a very large table can
     * exceed any fixed number; it is bounded by memory, as under REST.
     */
    args.SetMaxReceiveMessageSize(-1);
    args.SetMaxSendMessageSize(limit);

    /*
     * A failed data-plane call brings its error document back as a trailer
     * (dlagent-error-bin), and gRPC's default metadata ceiling is 8 KiB soft,
     * 16 KiB hard.  An Iceberg or S3 stack trace alone is often 10-50 KiB,
     * so at the default the document -- the real error, its HTTP status and
     * the 404/409 classification -- was dropped and the user saw only
     * "received metadata size exceeds hard limit".  The agent caps the
     * document at 1 MiB (base64 makes that about 1.4 MiB on the wire); this
     * leaves room above it.  Soft and hard are equal so nothing between them
     * is rejected at random.
     */
    args.SetInt(GRPC_ARG_MAX_METADATA_SIZE, MAX_METADATA_SIZE);
    args.SetInt(GRPC_ARG_ABSOLUTE_MAX_METADATA_SIZE, MAX_METADATA_SIZE);

    return args;
}

/*
 * THE MESSAGE FRAMING.
 *
 * A grpc::ByteBuffer given to GenericStub is the message body EXACTLY as it
 * goes on the wire, so the payload has to be wrapped in the one-field message
 * that proto/dlagent.proto declares:
 *
 *     Request/Response { bytes payload = 1; }
 *     -> 0x0A, varint(length), bytes           (and nothing at all when empty)
 *
 * Getting this wrong is not detectable on either side alone.  The Java
 * marshaller's golden tests prove ITS framing matches protoc, and the
 * in-process Java tests use that same marshaller at both ends, so both pass
 * while the C++ side sends a bare JSON document -- which the Java side then
 * reads as a message whose first byte, '{' = 0x7B, is tag 15 wire type 3.
 * That is precisely what a first real cross-language call reported, and why
 * one exists.
 */
std::string frame_payload(const std::string& payload)
{
    if (payload.empty())
    {
        /* proto3 omits a field holding its default value. */
        return std::string();
    }
    std::string out;
    out.push_back(static_cast<char>(0x0A));
    size_t len = payload.size();
    while (len >= 0x80)
    {
        out.push_back(static_cast<char>((len & 0x7F) | 0x80));
        len >>= 7;
    }
    out.push_back(static_cast<char>(len));
    out.append(payload);
    return out;
}

/* The inverse, tolerating fields this build does not know about. */
bool unframe_payload(const std::string& message, std::string* out)
{
    out->clear();
    size_t pos = 0;
    while (pos < message.size())
    {
        uint64_t tag = 0;
        int shift = 0;
        for (;;)
        {
            if (pos >= message.size())
                return false;
            unsigned char b = static_cast<unsigned char>(message[pos++]);
            tag |= static_cast<uint64_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
            if (shift > 63)
                return false;
        }

        int field = static_cast<int>(tag >> 3);
        int wire = static_cast<int>(tag & 0x07);

        if (wire == 2)
        {
            uint64_t len = 0;
            shift = 0;
            for (;;)
            {
                if (pos >= message.size())
                    return false;
                unsigned char b = static_cast<unsigned char>(message[pos++]);
                len |= static_cast<uint64_t>(b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                    break;
                shift += 7;
                if (shift > 63)
                    return false;
            }
            /*
             * Compared as a remaining-bytes budget, not as pos + len: the
             * length is attacker-controlled in principle and a 64-bit sum can
             * wrap, which would turn a bad message into an endless loop
             * instead of a rejected one.
             */
            if (len > message.size() - pos)
                return false;
            if (field == 1)
                out->assign(message, pos, static_cast<size_t>(len));
            pos += static_cast<size_t>(len);
        }
        else if (wire == 0)
        {
            for (;;)
            {
                if (pos >= message.size())
                    return false;
                if ((static_cast<unsigned char>(message[pos++]) & 0x80) == 0)
                    break;
            }
        }
        else if (wire == 1)
        {
            if (pos + 8 > message.size())
                return false;
            pos += 8;
        }
        else if (wire == 5)
        {
            if (pos + 4 > message.size())
                return false;
            pos += 4;
        }
        else
        {
            /* Groups (3, 4) and the undefined 6, 7: not our message. */
            return false;
        }
    }
    return true;
}

/* Verbatim both ways: framing is the caller's business, see call_unary. */
grpc::ByteBuffer to_buffer(const std::string& message)
{
    grpc::Slice slice(message.data(), message.size());
    return grpc::ByteBuffer(&slice, 1);
}

bool from_buffer(const grpc::ByteBuffer& buffer, std::string* out)
{
    std::vector<grpc::Slice> slices;
    if (!buffer.Dump(&slices).ok())
        return false;
    out->clear();
    for (const grpc::Slice& slice : slices)
        out->append(reinterpret_cast<const char*>(slice.begin()), slice.size());
    return true;
}

/*
 * The first metadata entry gRPC would refuse, described, or "" if all are
 * valid.
 *
 * grpc++ does not return an error for an invalid key or value: it fails an
 * assertion and aborts the process, and the postmaster then restarts every
 * session.  Keys reach here from user input -- a foreign table option "a:b"
 * becomes x-gp-options-a:b -- so they are checked before the call is built,
 * and a bad one fails that call like any other error.  The rules are the
 * gRPC wire spec's: keys are [0-9a-z_.-] (callers lower-case them), values
 * of non-"-bin" keys are printable ASCII.
 */
std::string invalid_metadata(const std::map<std::string, std::string>& metadata)
{
    for (std::map<std::string, std::string>::const_iterator it = metadata.begin();
         it != metadata.end(); ++it)
    {
        const std::string& key = it->first;
        bool key_ok = !key.empty();
        for (size_t i = 0; key_ok && i < key.size(); i++)
        {
            unsigned char c = (unsigned char) key[i];
            key_ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                     c == '-' || c == '_' || c == '.';
        }
        if (!key_ok)
            return "invalid name \"" + key + "\" for the gRPC transport: only "
                   "letters, digits, '-', '_' and '.' are allowed";

        if (key.size() >= 4 && key.compare(key.size() - 4, 4, "-bin") == 0)
            continue;
        for (size_t i = 0; i < it->second.size(); i++)
        {
            unsigned char c = (unsigned char) it->second[i];
            if (c < 0x20 || c > 0x7e)
                return "invalid value for \"" + key + "\" for the gRPC "
                       "transport: only printable ASCII is allowed";
        }
    }
    return std::string();
}

void apply_metadata(grpc::ClientContext* context,
                    const std::map<std::string, std::string>& metadata)
{
    for (std::map<std::string, std::string>::const_iterator it = metadata.begin();
         it != metadata.end(); ++it)
    {
        if (!it->second.empty())
            context->AddMetadata(it->first, it->second);
    }
}

gpr_timespec from_now_ms(int ms)
{
    return gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                        gpr_time_from_millis(ms, GPR_TIMESPAN));
}

/*
 * Bound the wait for a connection to options.connect_timeout_ms (and the
 * deadline, if shorter), polling the interrupt callback; see CallOptions.
 * OK once the channel is READY, or when there is no bound to apply.
 *
 * Nothing has been sent yet, so giving up here is always safe to report as
 * "could not reach the agent".
 */
grpc::Status await_connection(const std::shared_ptr<grpc::Channel>& channel,
                              const std::string& target,
                              const CallOptions& options,
                              const InterruptCheck& interrupted)
{
    if (!options.wait_for_ready || options.connect_timeout_ms <= 0)
        return grpc::Status::OK;

    int limit_ms = options.connect_timeout_ms;
    if (options.deadline_ms > 0 && options.deadline_ms < limit_ms)
        limit_ms = options.deadline_ms;
    gpr_timespec give_up = from_now_ms(limit_ms);

    for (;;)
    {
        grpc_connectivity_state state = channel->GetState(true);
        if (state == GRPC_CHANNEL_READY)
            return grpc::Status::OK;
        if (interrupted && interrupted())
            return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled by the caller");
        if (gpr_time_cmp(gpr_now(GPR_CLOCK_MONOTONIC), give_up) >= 0)
            return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                "could not connect to the datalake agent at " + target +
                                " within " + std::to_string(limit_ms / 1000) + " s");
        channel->WaitForStateChange(state, from_now_ms(options.poll_interval_ms));
    }
}

/*
 * Wait for one completion-queue event, giving the interrupt callback a look
 * every poll_interval_ms and giving up after limit_ms.
 *
 * Returns true when the event arrived.  On cancellation it calls TryCancel and
 * KEEPS WAITING: the operation still owns the response buffer and the context,
 * so returning before its event is drained would destroy them underneath gRPC.
 * That is the difference between a cancelled query and a crashed backend.
 */
bool await_event(grpc::CompletionQueue* cq, void* expected_tag, bool* ok,
                 grpc::ClientContext* context, const CallOptions& options,
                 const InterruptCheck& interrupted, int limit_ms,
                 bool* was_cancelled)
{
    int waited_ms = 0;
    bool cancel_requested = false;

    for (;;)
    {
        void* got_tag = nullptr;
        bool got_ok = false;
        grpc::CompletionQueue::NextStatus next =
            cq->AsyncNext(&got_tag, &got_ok, from_now_ms(options.poll_interval_ms));

        if (next == grpc::CompletionQueue::GOT_EVENT)
        {
            *ok = got_ok;
            /*
             * Tags are per-call here (one operation in flight at a time), so a
             * tag we did not expect means the queue is being used in a way
             * this code does not model.  Treat it as not-ok rather than
             * pretending the expected event arrived.
             */
            return got_tag == expected_tag;
        }

        if (next == grpc::CompletionQueue::SHUTDOWN)
        {
            *ok = false;
            return false;
        }

        /* TIMEOUT: this is the only place the caller regains control. */
        waited_ms += options.poll_interval_ms;

        if (!cancel_requested && interrupted && interrupted())
        {
            cancel_requested = true;
            if (was_cancelled)
                *was_cancelled = true;
            context->TryCancel();
            /* Keep looping; the cancelled op still has to complete. */
        }

        if (!cancel_requested && limit_ms > 0 && waited_ms >= limit_ms)
        {
            cancel_requested = true;
            context->TryCancel();
        }
    }
}

void copy_trailers(const grpc::ClientContext& context,
                   std::map<std::string, std::string>* out)
{
    out->clear();
    const std::multimap<grpc::string_ref, grpc::string_ref>& md =
        context.GetServerTrailingMetadata();
    for (std::multimap<grpc::string_ref, grpc::string_ref>::const_iterator it = md.begin();
         it != md.end(); ++it)
    {
        (*out)[std::string(it->first.data(), it->first.size())] =
            std::string(it->second.data(), it->second.size());
    }
}

void configure_context(grpc::ClientContext* context,
                       const std::map<std::string, std::string>& metadata,
                       int deadline_ms, bool wait_for_ready)
{
    apply_metadata(context, metadata);
    context->set_wait_for_ready(wait_for_ready);
    if (deadline_ms > 0)
    {
        context->set_deadline(
            gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                         gpr_time_from_millis(deadline_ms, GPR_TIMESPAN)));
    }
}

/* Drain a queue we are done with, so nothing is destroyed under an operation. */
void drain(grpc::CompletionQueue* cq)
{
    cq->Shutdown();
    void* tag = nullptr;
    bool ok = false;
    while (cq->Next(&tag, &ok))
    {
        /* discard */
    }
}

}  // namespace

void set_max_message_size(int bytes)
{
    if (bytes > 0)
    {
        g_max_message_size = bytes;
    }
}

std::shared_ptr<grpc::Channel> ChannelCache::acquire(const std::string& target)
{
    std::lock_guard<std::mutex> guard(g_cache_mutex);

    pid_t self = getpid();
    std::unordered_map<std::string, CachedChannel>::iterator it = g_cache.find(target);
    if (it != g_cache.end())
    {
        if (it->second.owner_pid == self)
            return it->second.channel;

        /*
         * Inherited across a fork.  Everything in the map belongs to the
         * parent, not just this entry, so drop the lot rather than repairing
         * one key at a time.
         */
        g_cache.clear();
    }

    CachedChannel entry;
    entry.channel = grpc::CreateCustomChannel(
        target, grpc::InsecureChannelCredentials(), default_channel_args());
    entry.owner_pid = self;
    g_cache[target] = entry;
    return entry.channel;
}

void ChannelCache::evict(const std::string& target)
{
    std::lock_guard<std::mutex> guard(g_cache_mutex);
    g_cache.erase(target);
}

void ChannelCache::clear()
{
    std::lock_guard<std::mutex> guard(g_cache_mutex);
    g_cache.clear();
}

size_t ChannelCache::size()
{
    std::lock_guard<std::mutex> guard(g_cache_mutex);
    return g_cache.size();
}

bool is_connect_error(const grpc::Status& status)
{
    /*
     * By status code, never by matching "Connection refused" in the message.
     * Measured: what an agent that is listening but not yet SERVING returns is
     * a clean UNAVAILABLE.  Text matching would miss it and fail the first
     * query of every cold start.
     */
    return status.error_code() == grpc::StatusCode::UNAVAILABLE;
}

static void maybe_evict(const std::string& target, const grpc::Status& status)
{
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
    {
        ChannelCache::evict(target);
    }
}

grpc::Status call_unary(const std::string& target,
                        const std::string& method,
                        const std::string& request,
                        const std::map<std::string, std::string>& metadata,
                        const CallOptions& options,
                        const InterruptCheck& interrupted,
                        std::string* response,
                        int* app_status)
{
    std::string response_message;
    std::string status_trailer;
    grpc::Status status = call_unary_raw(target, method, frame_payload(request), metadata,
                                         options, interrupted, &response_message,
                                         &status_trailer);
    if (app_status != nullptr)
    {
        *app_status = status_trailer.empty() ? -1 : atoi(status_trailer.c_str());
    }
    if (status.ok() && response != nullptr)
    {
        if (!unframe_payload(response_message, response))
        {
            return grpc::Status(grpc::StatusCode::INTERNAL,
                                "malformed dlagent gRPC message from " + method);
        }
    }
    return status;
}

grpc::Status call_unary_raw(const std::string& target,
                            const std::string& method,
                            const std::string& request,
                            const std::map<std::string, std::string>& metadata,
                            const CallOptions& options,
                            const InterruptCheck& interrupted,
                            std::string* response,
                            std::string* status_trailer)
{
    std::string bad = invalid_metadata(metadata);
    if (!bad.empty())
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, bad);

    std::shared_ptr<grpc::Channel> channel = ChannelCache::acquire(target);

    grpc::Status not_connected = await_connection(channel, target, options, interrupted);
    if (!not_connected.ok())
        return not_connected;

    grpc::TemplatedGenericStub<grpc::ByteBuffer, grpc::ByteBuffer> stub(channel);

    grpc::CompletionQueue cq;
    grpc::ClientContext context;
    configure_context(&context, metadata, options.deadline_ms, options.wait_for_ready);

    grpc::ByteBuffer request_buffer = to_buffer(request);
    std::unique_ptr<grpc::ClientAsyncResponseReader<grpc::ByteBuffer> > reader =
        stub.PrepareUnaryCall(&context, method, request_buffer, &cq);
    if (!reader)
    {
        drain(&cq);
        return grpc::Status(grpc::StatusCode::INTERNAL,
                            "failed to prepare the gRPC call for " + method);
    }
    reader->StartCall();

    grpc::ByteBuffer response_buffer;
    grpc::Status status;
    void* tag = reinterpret_cast<void*>(1);
    reader->Finish(&response_buffer, &status, tag);

    bool ok = false;
    bool cancelled = false;
    bool matched = await_event(&cq, tag, &ok, &context, options, interrupted,
                               /* the deadline already bounds this */ 0, &cancelled);
    drain(&cq);

    if (!matched || !ok)
    {
        grpc::Status failure = cancelled
            ? grpc::Status(grpc::StatusCode::CANCELLED, "cancelled by the caller")
            : grpc::Status(grpc::StatusCode::INTERNAL,
                           "the gRPC call for " + method + " did not complete");
        maybe_evict(target, failure);
        return failure;
    }

    if (status_trailer != nullptr)
    {
        const std::multimap<grpc::string_ref, grpc::string_ref>& trailers =
            context.GetServerTrailingMetadata();
        std::multimap<grpc::string_ref, grpc::string_ref>::const_iterator it =
            trailers.find("dlagent-status");
        if (it != trailers.end())
            status_trailer->assign(it->second.data(), it->second.size());
    }

    if (status.ok() && response != nullptr && !from_buffer(response_buffer, response))
    {
        return grpc::Status(grpc::StatusCode::INTERNAL,
                            "failed to read the response of " + method);
    }
    maybe_evict(target, status);
    return status;
}

grpc::Status call_server_streaming(const std::string& target,
                                   const std::string& method,
                                   const std::string& request,
                                   const std::map<std::string, std::string>& metadata,
                                   const CallOptions& options,
                                   const InterruptCheck& interrupted,
                                   const MessageSink& sink,
                                   std::map<std::string, std::string>* trailers)
{
    std::string bad = invalid_metadata(metadata);
    if (!bad.empty())
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, bad);

    std::shared_ptr<grpc::Channel> channel = ChannelCache::acquire(target);
    grpc::TemplatedGenericStub<grpc::ByteBuffer, grpc::ByteBuffer> stub(channel);

    grpc::CompletionQueue cq;
    grpc::ClientContext context;
    /*
     * No overall deadline: a scan can legitimately stream for minutes.  The
     * bound that matters is per-read, applied in the loop below, so a producer
     * that stops producing is detected without cutting off one that is simply
     * slow.
     */
    configure_context(&context, metadata, 0, options.wait_for_ready);

    std::unique_ptr<grpc::ClientAsyncReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer> > stream =
        stub.PrepareCall(&context, method, &cq);
    if (!stream)
    {
        drain(&cq);
        return grpc::Status(grpc::StatusCode::INTERNAL,
                            "failed to prepare the gRPC stream for " + method);
    }

    void* tag = reinterpret_cast<void*>(1);
    bool ok = false;
    bool cancelled = false;

    /*
     * Ends a call that failed before the response phase.  Finish() is what
     * reports WHY -- connection refused, the agent's readiness gate, an
     * over-limit request -- and callers decide on that (datalakeDoRPC retries
     * a refused connection), so a generic "could not start" would hide it.
     */
    auto finish_early = [&](const char* fallback) -> grpc::Status {
        grpc::Status status;
        if (!cancelled)
        {
            stream->Finish(&status, tag);
            await_event(&cq, tag, &ok, &context, options, interrupted,
                        options.deadline_ms, &cancelled);
        }
        drain(&cq);
        if (cancelled)
            return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled by the caller");
        if (status.ok())
            status = grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                  std::string(fallback) + method);
        if (trailers != nullptr)
            copy_trailers(context, trailers);
        maybe_evict(target, status);
        return status;
    };

    stream->StartCall(tag);
    if (!await_event(&cq, tag, &ok, &context, options, interrupted,
                     options.deadline_ms, &cancelled) || !ok)
        return finish_early("could not start the gRPC stream for ");

    grpc::ByteBuffer request_buffer = to_buffer(frame_payload(request));
    stream->Write(request_buffer, tag);
    if (!await_event(&cq, tag, &ok, &context, options, interrupted,
                     options.deadline_ms, &cancelled) || !ok)
        return finish_early("failed to send the request for ");

    stream->WritesDone(tag);
    await_event(&cq, tag, &ok, &context, options, interrupted,
                options.deadline_ms, &cancelled);

    bool sink_stopped = false;
    for (;;)
    {
        grpc::ByteBuffer message;
        stream->Read(&message, tag);
        if (!await_event(&cq, tag, &ok, &context, options, interrupted,
                         options.stream_idle_timeout_ms, &cancelled))
        {
            break;
        }
        if (!ok)
        {
            /* End of stream, or the call failed; Finish() below says which. */
            break;
        }

        std::string raw;
        std::string payload;
        if (!from_buffer(message, &raw) || !unframe_payload(raw, &payload))
        {
            context.TryCancel();
            break;
        }
        if (sink && !sink(payload))
        {
            sink_stopped = true;
            context.TryCancel();
            break;
        }
    }

    grpc::Status status;
    stream->Finish(&status, tag);
    await_event(&cq, tag, &ok, &context, options, interrupted,
                options.deadline_ms, &cancelled);
    drain(&cq);
    if (trailers != nullptr)
        copy_trailers(context, trailers);

    if (cancelled)
        return grpc::Status(grpc::StatusCode::CANCELLED, "cancelled by the caller");
    if (sink_stopped)
    {
        /*
         * The caller stopped reading on purpose, so the CANCELLED that
         * TryCancel produced is expected rather than a failure.
         */
        return grpc::Status::OK;
    }
    maybe_evict(target, status);
    return status;
}

}  // namespace grpcx
}  // namespace agent_cli
