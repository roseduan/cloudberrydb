/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import io.grpc.Status;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Recognizes "the client went away" on the gRPC transport.
 *
 * <p>The Spring implementation tests for Tomcat's {@code ClientAbortException};
 * there is no equivalent type here, and the shape of the answer was measured
 * rather than guessed (a real C++ client against a real grpc-java server): a C++ {@code TryCancel}, a client deadline expiry and a
 * killed client all surface on the handler as
 * {@code io.grpc.InternalStatusRuntimeException} with status CANCELLED or
 * DEADLINE_EXCEEDED -- never as an IOException.
 *
 * <p>Hence the test is on the STATUS CODE, via {@link Status#fromThrowable},
 * and NOT {@code t instanceof StatusRuntimeException}: the concrete class is
 * the package-private InternalStatusRuntimeException, so an instanceof against
 * the public type is a coin flip across grpc versions.
 *
 * <p>Getting this wrong is not cosmetic. A disconnect misread as a server
 * fault is logged as an error with a stack trace on every cancelled query,
 * which is exactly the regression the old branch introduced on the REST side
 * by widening its catch to IOException.
 */
@Named
@Singleton
public class GrpcClientDisconnectPredicate implements ClientDisconnectPredicate {

    @Override
    public boolean isClientDisconnect(Throwable t) {
        if (t == null) {
            return false;
        }
        Status.Code code = Status.fromThrowable(t).getCode();
        return code == Status.Code.CANCELLED || code == Status.Code.DEADLINE_EXCEEDED;
    }
}
