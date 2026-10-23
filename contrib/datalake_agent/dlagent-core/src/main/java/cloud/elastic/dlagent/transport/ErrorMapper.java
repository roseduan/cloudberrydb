/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import cloud.elastic.dlagent.service.ServiceResult;

/**
 * The single source of truth for classifying a failed operation, shared by both
 * transports.
 *
 * <p>Saying "JSON in, JSON out" is not enough to make the two transports
 * equivalent: the error BODY can be byte-identical while the C side still
 * classifies the failure differently, because the classification also depends
 * on the transport-level status -- an HTTP status for REST, a
 * {@code grpc::StatusCode} for gRPC. The payload contract guarantees the
 * body; this guarantees the status alongside it.
 *
 * <p>The input is {@code ServiceResult.getStatusCode()}, which is HTTP-shaped
 * on purpose (see {@link ErrorCategory}). The gRPC service turns the category
 * into a Status; the REST controller needs no translation because the status
 * code is already what it puts on the wire.
 */
public final class ErrorMapper {

    private ErrorMapper() {
    }

    /** The category for an HTTP-shaped status code. */
    public static ErrorCategory categorize(int httpStatus) {
        if (httpStatus >= 200 && httpStatus < 300) {
            return ErrorCategory.OK;
        }
        switch (httpStatus) {
            case 400:
                return ErrorCategory.INVALID_ARGUMENT;
            case 401:
            case 403:
                return ErrorCategory.UNAUTHENTICATED;
            case 404:
                return ErrorCategory.NOT_FOUND;
            case 409:
                return ErrorCategory.ALREADY_EXISTS;
            case 502:
            case 503:
                return ErrorCategory.UNAVAILABLE;
            case 504:
                return ErrorCategory.DEADLINE_EXCEEDED;
            default:
                // 4xx we have not enumerated is still the caller's fault, and
                // reporting it as INTERNAL would send the FDW down the "retry
                // the agent" path for a request that can never succeed.
                return httpStatus >= 400 && httpStatus < 500
                        ? ErrorCategory.INVALID_ARGUMENT
                        : ErrorCategory.INTERNAL;
        }
    }

    /**
     * The category for a result, from its status code alone.
     *
     * <p>Deliberately not keyed on {@code isSuccess()}: two endpoints answer
     * {@code 201 or 409} with a data body rather than an error body, so a
     * result can carry data and still not be a success. The status code is the
     * thing both transports put on the wire, so it is the thing to classify.
     */
    public static ErrorCategory categorize(ServiceResult<?> result) {
        return categorize(result.getStatusCode());
    }
}
