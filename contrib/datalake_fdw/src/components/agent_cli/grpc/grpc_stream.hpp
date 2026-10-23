/*-------------------------------------------------------------------------
 *
 * grpc_stream.hpp
 *    The data plane over gRPC, shaped like libchurl.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/components/agent_cli/grpc/grpc_stream.hpp
 *
 * A C-callable surface deliberately mirroring libchurl.h, so the dispatch in
 * libchurl.c is a one-line delegation per function and protocol.c's 52 call
 * sites are untouched.
 *
 * REWRITTEN, NOT PORTED.  The old branch's grpc_stream.cpp used the
 * synchronous grpc++ streaming API, which was measured to be uninterruptible
 * by signals: a backend blocked in Read() never reaches CHECK_FOR_INTERRUPTS, so
 * the query cannot be cancelled and pg_terminate_backend does not help either.
 * Everything here goes through the async CompletionQueue in grpc_transport.
 *-------------------------------------------------------------------------
 */
#ifndef AGENT_CLI_GRPC_STREAM_HPP
#define AGENT_CLI_GRPC_STREAM_HPP

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque, like CHURL_HANDLE. */
typedef void *DL_GRPC_STREAM;

/** Opaque header/metadata collection, like CHURL_HEADERS. */
typedef void *DL_GRPC_HEADERS;

DL_GRPC_HEADERS dl_grpc_headers_init(void);
void dl_grpc_headers_append(DL_GRPC_HEADERS headers, const char *key, const char *value);
void dl_grpc_headers_override(DL_GRPC_HEADERS headers, const char *key, const char *value);
void dl_grpc_headers_remove(DL_GRPC_HEADERS headers, const char *key);
void dl_grpc_headers_cleanup(DL_GRPC_HEADERS headers);

/**
 * The largest message a data-plane call may send, in bytes:
 * datalake.rpc_max_message_size.  Call before dl_grpc_stream_start: channels
 * are cached per target and keep the limit they were created with, so a
 * backend whose first call is on the data plane would otherwise build its
 * channel with the default and keep it.
 */
void dl_grpc_set_max_message_size(int bytes);

/**
 * Start a Read (download) or Write (upload) call.
 *
 * @param target     "host:port"
 * @param is_write   true for DataProxy/Write, false for DataProxy/Read
 * @param interrupt  polled between waits; the caller's CHECK_FOR_INTERRUPTS
 *                   equivalent, which must NOT raise from here
 */
DL_GRPC_STREAM dl_grpc_stream_start(const char *target, bool is_write,
                                    DL_GRPC_HEADERS headers,
                                    bool (*interrupt)(void));

/** Buffer request bytes; sent when the first read happens. */
size_t dl_grpc_stream_write(DL_GRPC_STREAM stream, const char *buf, size_t len);

/** Read up to max_size response bytes; 0 means end of stream. */
size_t dl_grpc_stream_read(DL_GRPC_STREAM stream, char *buf, size_t max_size);

/** True if the stream ended in an error; fills msg when it did. */
bool dl_grpc_stream_failed(DL_GRPC_STREAM stream, const char **msg);

/*
 * The trailers a failed data-plane call carries when the agent itself
 * answered with an error: the HTTP status REST would have returned and the
 * same JSON error document (DlagentMethods in dlagent-grpc).
 */
#define DL_GRPC_TRAILER_STATUS          "dlagent-status"
#define DL_GRPC_TRAILER_ERROR_DOCUMENT  "dlagent-error-bin"

/*
 * For a failed stream that the agent answered with an error document: the
 * HTTP status and the document, valid until dl_grpc_stream_cleanup.  False
 * for a transport failure (nothing reached the agent, or it vanished).
 */
bool dl_grpc_stream_error_document(DL_GRPC_STREAM stream, long *http_status,
                                   const char **body, size_t *body_len);

void dl_grpc_stream_cleanup(DL_GRPC_STREAM stream, bool after_error);

#ifdef __cplusplus
}
#endif

#endif  /* AGENT_CLI_GRPC_STREAM_HPP */
