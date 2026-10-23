/*-------------------------------------------------------------------------
 *
 * grpc_health.cpp
 *    A readiness probe for the gRPC datalake agent.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_health.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "grpc_health.hpp"
#include "grpc_transport.hpp"

#include <string>

namespace {

DlHealthStatus health_check(const char *target, const char *service,
                            int timeout_ms);

}  // namespace

/*
 * The C entry point.  No C++ exception may leave it: the caller is
 * datalake_proxy's C code, through which an exception cannot unwind.
 */
extern "C"
DlHealthStatus dl_grpc_health_check(const char *target, const char *service,
                                    int timeout_ms)
{
    try
    {
        return health_check(target, service, timeout_ms);
    }
    catch (...)
    {
        return DL_HEALTH_UNREACHABLE;
    }
}

namespace {

DlHealthStatus health_check(const char *target, const char *service,
                            int timeout_ms)
{
    if (target == nullptr)
        return DL_HEALTH_UNREACHABLE;

    /*
     * HealthCheckRequest{ string service = 1 }, hand-encoded for the same
     * reason the rest of this transport is: the health protocol's messages are
     * one field each, and generating code for them would require protoc plus
     * the grpc C++ plugin at build time, which three of the five product build
     * images do not have.
     *
     * service="" is the default value, which proto3 omits entirely -- so an
     * empty request is an empty message, not a zero-length field.
     */
    std::string request;
    std::string name = service ? service : "";
    if (!name.empty())
    {
        request.push_back(static_cast<char>(0x0A));
        size_t len = name.size();
        while (len >= 0x80)
        {
            request.push_back(static_cast<char>((len & 0x7F) | 0x80));
            len >>= 7;
        }
        request.push_back(static_cast<char>(len));
        request.append(name);
    }

    agent_cli::grpcx::CallOptions options;
    options.deadline_ms = timeout_ms > 0 ? timeout_ms : 2000;
    /*
     * Poll faster than a normal RPC: this runs in a startup loop where the
     * answer is expected within a second, and the resolution of "how long did
     * the agent take to become ready" is the poll interval.
     */
    options.poll_interval_ms = 20;

    std::string response;
    grpc::Status status = agent_cli::grpcx::call_unary_raw(
        target, "/grpc.health.v1.Health/Check", request,
        std::map<std::string, std::string>(), options,
        agent_cli::grpcx::InterruptCheck(), &response);

    if (!status.ok())
    {
        if (status.error_code() == grpc::StatusCode::NOT_FOUND)
        {
            /* The server has no such slot registered. */
            return DL_HEALTH_SERVICE_UNKNOWN;
        }
        return DL_HEALTH_UNREACHABLE;
    }

    /* HealthCheckResponse{ ServingStatus status = 1 }: 08 <varint>, or empty. */
    if (response.size() == 0)
        return DL_HEALTH_UNKNOWN;
    if (response.size() != 2 || static_cast<unsigned char>(response[0]) != 0x08)
        return DL_HEALTH_UNKNOWN;

    switch (static_cast<unsigned char>(response[1]))
    {
        case 1: return DL_HEALTH_SERVING;
        case 2: return DL_HEALTH_NOT_SERVING;
        case 3: return DL_HEALTH_SERVICE_UNKNOWN;
        default: return DL_HEALTH_UNKNOWN;
    }
}

}  // namespace
