/*-------------------------------------------------------------------------
 *
 * grpc_transport.hpp
 *    Channel cache and interruptible async calls for the datalake agent's
 *    gRPC transport.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_transport.hpp
 *
 * C++ ERROR BOUNDARY: nothing here calls a PostgreSQL API.  Cancellation is
 * observed through the caller-supplied callback, never by touching
 * InterruptPending, because these calls can run on a gRPC thread.
 *-------------------------------------------------------------------------
 */
#ifndef AGENT_CLI_GRPC_TRANSPORT_HPP
#define AGENT_CLI_GRPC_TRANSPORT_HPP

#include <functional>
#include <map>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

namespace agent_cli {
namespace grpcx {

/** Observed between polls; true means "the caller wants out". */
typedef std::function<bool()> InterruptCheck;

/** Called once per streamed message; return false to stop reading. */
typedef std::function<bool(const std::string&)> MessageSink;

struct CallOptions {
    /*
     * EVERY call has a time limit.  Five keepalive configurations were
     * measured against a blackholed route and none of them recovered a stuck
     * stream inside 60 s, while a deadline returned within 14 ms of its
     * expiry.  So keepalive is not a backstop here and a deadline is not
     * optional.
     */
    int deadline_ms = 30000;

    /*
     * How often the wait loop wakes to look at the interrupt callback.  This
     * is what makes a blocked RPC cancellable at all: a synchronous grpc++
     * read cannot be interrupted by a signal (20 SIGUSR1 delivered to a
     * thread blocked in Read(); the handler ran every time and Read() never
     * returned), so the only way a single-threaded backend regains control is
     * to never block indefinitely in the first place.
     */
    int poll_interval_ms = 100;

    /*
     * Per-message idle limit for streaming calls.  A stream may legitimately
     * run for minutes, so it gets no overall deadline; what it must not do is
     * wait forever for the NEXT message.
     */
    int stream_idle_timeout_ms = 60000;

    /*
     * Wait, within the deadline, for the channel to connect instead of
     * failing at once while the agent is still starting.  A call made this
     * way is sent at most once, so it is safe for non-idempotent operations
     * -- unlike replaying a call that failed UNAVAILABLE, which can also mean
     * the agent received it and the connection broke before it answered.
     */
    bool wait_for_ready = false;

    /*
     * With wait_for_ready: how long to wait for the CONNECTION, separately
     * from the call's deadline (0 = the deadline alone).  Without it a call
     * to an agent that is not coming back waits out the whole request
     * timeout -- 300 s by default -- where REST fails in about 3 s on a
     * refused connection.  The bound only has to cover a restart, which
     * datalake_proxy does within seconds.
     */
    int connect_timeout_ms = 0;
};

/**
 * Shared channels, keyed by target.
 *
 * Two behaviours that exist because of measurements rather than taste:
 *
 *  - Every entry records the pid that created it and is discarded if the pid
 *    has changed.  gRPC state does not survive fork(): a test created a
 *    channel, forked, and the child's first RPC hung permanently AND its
 *    deadline never fired, because the timer thread does not exist in the
 *    child.
 *    GRPC_ENABLE_FORK_SUPPORT did not help.  A PostgreSQL backend IS a fork of
 *    the postmaster, so without this check the very first query in a new
 *    backend can wedge -- and a smoke test would not catch it, because the
 *    first RPC after the fork happens to succeed.
 *
 *  - A channel that produced UNAVAILABLE or DEADLINE_EXCEEDED is evicted.  A
 *    blackholed peer otherwise poisons the cache and every later RPC pays a
 *    full deadline before failing.
 */
class ChannelCache {
public:
    static std::shared_ptr<grpc::Channel> acquire(const std::string& target);
    static void evict(const std::string& target);
    /** Drop everything; used when the pid changed and by tests. */
    static void clear();
    /** Number of live entries, for tests. */
    static size_t size();
};

/**
 * One unary RPC, interruptible.
 *
 * Returns the gRPC status; on OK the response bytes are in *response.  The
 * status is ALWAYS the one Finish() produced, including on the normal path:
 * dropping it would leave the C side unable to tell a cancelled call from an
 * empty answer.
 */
/**
 * One unary RPC whose request and response bodies are passed through
 * VERBATIM, with no payload framing applied.
 *
 * For messages that are not {@code dlagent.v1.Request/Response} -- today that
 * means the health protocol, whose response field is a varint rather than a
 * length-delimited payload, so unwrapping it as one would silently yield
 * nothing.  Callers build and read the message themselves.
 */
grpc::Status call_unary_raw(const std::string& target,
                            const std::string& method,
                            const std::string& request_message,
                            const std::map<std::string, std::string>& metadata,
                            const CallOptions& options,
                            const InterruptCheck& interrupted,
                            std::string* response_message,
                            std::string* status_trailer = nullptr);

grpc::Status call_unary(const std::string& target,
                        const std::string& method,
                        const std::string& request,
                        const std::map<std::string, std::string>& metadata,
                        const CallOptions& options,
                        const InterruptCheck& interrupted,
                        std::string* response,
                        /**
                         * The application's HTTP-shaped status, from the
                         * dlagent-status trailer; -1 when the server sent
                         * none.  See DlagentMethods.STATUS_KEY on the Java
                         * side for why it is not the gRPC status.
                         */
                        int* app_status = nullptr);

/**
 * One server-streaming RPC, interruptible, one callback per message.
 *
 * The sink returning false stops the read loop and cancels the call, which is
 * how a caller that has seen enough avoids waiting for a producer that has
 * not finished.
 */
/**
 * trailers, if given, receives the server's trailing metadata (binary
 * "-bin" entries included, as raw bytes) whenever the call got that far.
 */
grpc::Status call_server_streaming(const std::string& target,
                                   const std::string& method,
                                   const std::string& request,
                                   const std::map<std::string, std::string>& metadata,
                                   const CallOptions& options,
                                   const InterruptCheck& interrupted,
                                   const MessageSink& sink,
                                   std::map<std::string, std::string>* trailers = nullptr);

/** True when the status means "could not reach a serving agent". */
bool is_connect_error(const grpc::Status& status);

/*
 * The largest single message a channel may send or receive, in bytes.
 *
 * Pushed in rather than read here: this translation unit is deliberately free
 * of postgres.h (only agent_c_api.cpp includes both, and it pays for that with
 * a page of macro push/undef/pop), so it cannot call GetConfigOption itself.
 * Both planes set it from datalake.rpc_max_message_size before their first
 * call: agent_c_api.cpp when it builds a gRPC handle, libchurl (through
 * dl_grpc_set_max_message_size) before it starts a stream.
 *
 * Takes effect for channels created afterwards.  That is enough because the
 * GUC is PGC_POSTMASTER and every first call sets it, so no channel can
 * predate the value it should have been built with -- whichever plane a
 * backend uses first.
 */
void set_max_message_size(int bytes);

}  // namespace grpcx
}  // namespace agent_cli

#endif  /* AGENT_CLI_GRPC_TRANSPORT_HPP */
