/*-------------------------------------------------------------------------
 *
 * grpc_health.hpp
 *    A readiness probe for the gRPC datalake agent.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_health.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef AGENT_CLI_GRPC_HEALTH_HPP
#define AGENT_CLI_GRPC_HEALTH_HPP

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Mirrors grpc.health.v1.HealthCheckResponse.ServingStatus. */
typedef enum
{
	DL_HEALTH_UNKNOWN = 0,
	DL_HEALTH_SERVING = 1,
	DL_HEALTH_NOT_SERVING = 2,
	DL_HEALTH_SERVICE_UNKNOWN = 3,
	/** Could not reach the agent at all. */
	DL_HEALTH_UNREACHABLE = -1
} DlHealthStatus;

/**
 * One grpc.health.v1.Health/Check.
 *
 * @param target   "host:port"
 * @param service  the slot to ask about; use "dlagent", NOT "" -- the
 *                 top-level slot is SERVING as soon as the server binds its
 *                 port, which is exactly the not-yet-ready window the
 *                 two-phase startup exists to expose.
 */
DlHealthStatus dl_grpc_health_check(const char *target, const char *service,
									int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif  /* AGENT_CLI_GRPC_HEALTH_HPP */
