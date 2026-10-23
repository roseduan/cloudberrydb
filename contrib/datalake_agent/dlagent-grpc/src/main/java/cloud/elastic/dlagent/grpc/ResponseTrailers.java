/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Lets a handler add trailing metadata to a call that completes successfully.
 *
 * <p>grpc-java has no API for this on the {@code StreamObserver} a handler is
 * given: trailers can be attached to an ERROR (through
 * {@code StatusRuntimeException}) but not to a normal completion. The
 * operations here need exactly that -- the application's HTTP-shaped status
 * accompanies a perfectly ordinary response body, because a non-OK gRPC status
 * would discard the body and the C side needs both (see
 * {@code DlagentMethods.STATUS_KEY}).
 *
 * <p>So the handler puts trailers in a call-scoped holder and this interceptor
 * merges them in when the call closes. A Context key rather than a ThreadLocal:
 * handlers run on pooled gRPC threads, where a ThreadLocal would leak one
 * call's status onto the next.
 */
public final class ResponseTrailers {

    private static final Context.Key<AtomicReference<Metadata>> PENDING =
            Context.key("dlagent-response-trailers");

    private ResponseTrailers() {
    }

    /** Record trailers for the call in progress; later calls replace earlier ones. */
    public static void set(Metadata trailers) {
        AtomicReference<Metadata> holder = PENDING.get();
        if (holder != null) {
            holder.set(trailers);
        }
    }

    public static ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                AtomicReference<Metadata> holder = new AtomicReference<>();

                ServerCall<ReqT, RespT> merging =
                        new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                            @Override
                            public void close(Status status, Metadata trailers) {
                                Metadata pending = holder.get();
                                if (pending != null) {
                                    trailers.merge(pending);
                                }
                                super.close(status, trailers);
                            }
                        };

                return Contexts.interceptCall(
                        Context.current().withValue(PENDING, holder), merging, headers, next);
            }
        };
    }
}
