/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

/**
 * The transport-neutral classification of a failed operation.
 *
 * <p>There has to be a single source of truth for the mapping
 * between a business error, an HTTP status and a gRPC status code, so that the
 * errcode / SQLSTATE the FDW finally reports is the same under both transports.
 * This enum is that middle term: core decides the category, and each transport
 * renders it in its own vocabulary.
 *
 * <p>It is deliberately NOT a replacement for {@code ServiceResult.statusCode}.
 * That field stays HTTP-shaped (200/404/409/500) because the REST path puts it
 * straight on the wire, and changing it would make the rollback path's
 * responses differ. The category is derived
 * from it instead.
 */
public enum ErrorCategory {

    /** Not an error. */
    OK,

    /** The request was malformed or missing something; HTTP 400. */
    INVALID_ARGUMENT,

    /** Authentication or authorization failed; HTTP 401 / 403. */
    UNAUTHENTICATED,

    /** The catalog, namespace or table does not exist; HTTP 404. */
    NOT_FOUND,

    /** The object already exists, or a commit lost a race; HTTP 409. */
    ALREADY_EXISTS,

    /** The agent could not reach its storage or catalog backend; HTTP 502/503. */
    UNAVAILABLE,

    /** The operation ran out of time; HTTP 504. */
    DEADLINE_EXCEEDED,

    /** Anything else, including an unexpected exception; HTTP 500. */
    INTERNAL
}
