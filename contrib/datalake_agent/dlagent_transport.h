/*-------------------------------------------------------------------------
 *
 * dlagent_transport.h
 *	  The agent transport setting, shared by the two modules that read it.
 *
 * WHO OWNS THE GUC, AND WHY IT MATTERS.  datalake_proxy defines
 * datalake.agent_transport; datalake_fdw only reads it.  datalake_proxy is the
 * module in shared_preload_libraries -- it is the one that launches an agent,
 * and it needs the value in the postmaster, before any backend exists --
 * whereas datalake_fdw is loaded on demand when a foreign table is first
 * touched.
 *
 * AND WHY NEITHER SIDE MAY USE AN extern.  The two modules are two .so files
 * that are NOT linked against each other.  PostgreSQL loads a library with
 * dlopen(RTLD_NOW | RTLD_GLOBAL) (dfmgr.c:240), so an unresolved *data* symbol
 * is not a lazy binding that might never happen -- it fails the load.  In a
 * shared_preload_libraries entry that is FATAL in PostmasterMain and the
 * cluster does not start at all:
 *
 *     FATAL: could not load library ".../datalake_proxy.so":
 *            undefined symbol: datalake_agent_transport
 *
 * which is what happens if the GUC lives in datalake_fdw and datalake_proxy
 * reaches for it with an extern.  Adding datalake_fdw to shared_preload_libraries would paper over
 * it, but only in that direction and only for installations that do so; the
 * reverse extern (fdw reaching into proxy) would then make datalake_fdw
 * unloadable wherever datalake_proxy is not preloaded, which is every
 * installation that uses the FDW without the agent.
 *
 * So the rule is: the owner defines the GUC, everyone else reads it BY NAME
 * with GetConfigOption().  That works whether or not the owner is loaded --
 * an unclaimed "datalake.*" setting from postgresql.conf is kept by the GUC
 * machinery as a placeholder string, which is exactly what GetConfigOption
 * hands back.
 *
 * Portions Copyright (c) 2023-2025, HashData Technology Limited.
 *
 * IDENTIFICATION
 *		contrib/datalake_agent/dlagent_transport.h
 *-------------------------------------------------------------------------
 */
#ifndef DLAGENT_TRANSPORT_H
#define DLAGENT_TRANSPORT_H

/*
 * Which transport talks to the datalake agent.  An enum rather than a bool so
 * a third transport can be added without changing every call site's shape, and
 * so the GUC prints as a name in pg_settings.
 */
typedef enum DatalakeAgentTransport
{
	DATALAKE_AGENT_TRANSPORT_REST = 0,
	DATALAKE_AGENT_TRANSPORT_GRPC = 1
} DatalakeAgentTransport;

/*
 * The transport a cluster uses when nothing chooses one: grpc, except on a
 * build without gRPC support (contrib/datalake_agent/grpcxx.mk did not find
 * grpc++), which can only speak REST.  A build-time default rather than a
 * plain grpc, because a default this build refuses would stop every such
 * cluster at startup (see datalake_proxy's _PG_init).  REST stays available
 * on every build, as the fallback: set datalake.agent_transport = rest and
 * restart the cluster.
 */
#ifdef HAVE_GRPCXX
#define DATALAKE_AGENT_DEFAULT_TRANSPORT	DATALAKE_AGENT_TRANSPORT_GRPC
#else
#define DATALAKE_AGENT_DEFAULT_TRANSPORT	DATALAKE_AGENT_TRANSPORT_REST
#endif

/* The settings both modules name.  Spelled once so they cannot drift. */
#define DLAGENT_GUC_TRANSPORT		"datalake.agent_transport"
#define DLAGENT_GUC_GRPC_PORT		"datalake.agent_grpc_port"
#define DLAGENT_GUC_SERVER_URL		"datalake.agent_server_url"
#define DLAGENT_GUC_RPC_MAX_MESSAGE_SIZE	"datalake.rpc_max_message_size"

/* The REST agent's historical endpoint, and still the default. */
#define DATALAKE_AGENT_DEFAULT_URL	"http://localhost:3888"

/*
 * The gRPC agent's port.  Deliberately not 3888: the two transports are two
 * processes, and sharing a port would let a leftover agent of the other kind
 * answer.
 */
#define DATALAKE_AGENT_DEFAULT_GRPC_PORT	3889

/*
 * THE ONE MESSAGE-SIZE CEILING, IN MEGABYTES, THAT BOTH SIDES TAKE.
 *
 * gRPC bounds a single message on the RECEIVING side, and the two receivers
 * are in different languages: grpc++ on the client, grpc-java on the server.
 * Both default to 4 MiB, and a request carries the whole file list as one
 * message -- FDW_serialize_file_list_to_json() emits about 200 bytes per
 * file, so 4 MiB runs out at roughly 21,000 files, while
 * datalake.iceberg_max_file_removals_per_vacuum alone defaults to 100,000.
 *
 * Raising it on one side only is worse than leaving both at the default,
 * because it fails ASYMMETRICALLY: a client that may send 64 MB talking to a
 * server that may receive 4 MiB turns a large commit into RESOURCE_EXHAUSTED
 * under grpc while the identical request succeeds under rest.  That is what
 * this build did before the setting existed -- grpc_transport.cpp raised both
 * of its own limits and nothing raised the server's.
 *
 * So the value travels: datalake_proxy owns the GUC and hands it to the agent
 * on the command line (--dlagent.rpc_max_message_size), exactly as it does
 * with the port, and datalake_fdw reads the same GUC by name for the client.
 * One setting, three readers, no way to move one without the others.
 */
#define DATALAKE_AGENT_DEFAULT_RPC_MAX_MESSAGE_SIZE_MB	64

/*
 * The upper bound the GUC accepts, in megabytes.  grpc++ and grpc-java both
 * take the limit as a 32-bit byte count, so the ceiling has to stay well
 * inside INT_MAX bytes; 2047 MB is the largest whole megabyte that does.
 */
#define DATALAKE_AGENT_MAX_RPC_MAX_MESSAGE_SIZE_MB	2047

/*
 * Read a transport setting that arrived as text.
 *
 * Both the enum GUC's own value and an unclaimed placeholder come back from
 * GetConfigOption() as a string, so one parser serves both cases.  Anything
 * unrecognised -- including NULL, meaning the setting was never given -- is
 * DATALAKE_AGENT_DEFAULT_TRANSPORT, the same value datalake_proxy defines the
 * GUC with, so the FDW and the agent it talks to agree even when nothing set
 * it.
 */
static inline int
dlagent_transport_from_string(const char *value)
{
	if (value != NULL && pg_strcasecmp(value, "grpc") == 0)
		return DATALAKE_AGENT_TRANSPORT_GRPC;
	if (value != NULL && pg_strcasecmp(value, "rest") == 0)
		return DATALAKE_AGENT_TRANSPORT_REST;
	return DATALAKE_AGENT_DEFAULT_TRANSPORT;
}

/*
 * Build the gRPC target "host:port" from the agent URL and the gRPC port.
 *
 * The host comes from datalake.agent_server_url (so one setting still moves
 * both transports to another machine) and the port from
 * datalake.agent_grpc_port, because the two agents listen on different ports
 * by design.  Deriving the port from the URL instead would point the gRPC
 * client at the REST agent, which fails like a network fault rather than like
 * a misconfiguration.
 *
 * Shared rather than written twice because both modules connect to the same
 * agent, and a target the proxy probes but the FDW does not use would make a
 * health check that passes while every query fails.
 */
static inline void
dlagent_format_grpc_target(char *buf, size_t buflen,
						   const char *host, size_t len, int port)
{
	/* An IPv6 literal needs its brackets back once a port follows it. */
	bool		bracket = memchr(host, ':', len) != NULL && host[0] != '[';
	size_t		room = buflen - 8 - (bracket ? 2 : 0);	/* ":65535", NUL */

	if (len > room)
		len = room;
	snprintf(buf, buflen, "%s%.*s%s:%d", bracket ? "[" : "", (int) len, host,
			 bracket ? "]" : "", port);
}

static inline void
dlagent_build_grpc_target(char *buf, size_t buflen, const char *url, int port)
{
	const char *host;
	const char *end;

	if (url == NULL || url[0] == '\0')
		url = DATALAKE_AGENT_DEFAULT_URL;

	host = strstr(url, "://");
	host = host ? host + 3 : url;

	if (host[0] == '[')
	{
		/* "[::1]:3888": the host is the bracketed literal, brackets included. */
		end = strchr(host, ']');
		end = end ? end + 1 : NULL;
	}
	else
	{
		end = strchr(host, ':');
		if (end == NULL)
			end = strchr(host, '/');
	}
	dlagent_format_grpc_target(buf, buflen, host,
							   end ? (size_t) (end - host) : strlen(host), port);
}

#endif							/* DLAGENT_TRANSPORT_H */
