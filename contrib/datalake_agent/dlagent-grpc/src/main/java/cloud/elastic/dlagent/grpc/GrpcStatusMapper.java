/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.transport.ErrorCategory;
import io.grpc.Status;

/**
 * Renders core's {@link ErrorCategory} as a gRPC status.
 *
 * <p>The other half of the error mapping contract: the
 * body bytes are identical between transports because both come from the one
 * serializer, and the CLASSIFICATION is identical because both start from the
 * same {@code ServiceResult.statusCode} and are rendered by a single mapping
 * -- {@code ErrorMapper} in core for the category, this for the gRPC wording.
 * Without that the C side could receive the same error JSON under two
 * different transport verdicts and report two different SQLSTATEs.
 */
public final class GrpcStatusMapper {

    private GrpcStatusMapper() {
    }

    public static Status toStatus(ErrorCategory category) {
        switch (category) {
            case OK:
                return Status.OK;
            case INVALID_ARGUMENT:
                return Status.INVALID_ARGUMENT;
            case UNAUTHENTICATED:
                return Status.UNAUTHENTICATED;
            case NOT_FOUND:
                return Status.NOT_FOUND;
            case ALREADY_EXISTS:
                return Status.ALREADY_EXISTS;
            case UNAVAILABLE:
                return Status.UNAVAILABLE;
            case DEADLINE_EXCEEDED:
                return Status.DEADLINE_EXCEEDED;
            case INTERNAL:
            default:
                return Status.INTERNAL;
        }
    }
}
