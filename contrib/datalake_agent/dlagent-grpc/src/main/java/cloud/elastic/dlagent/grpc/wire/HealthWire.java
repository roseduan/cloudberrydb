/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc.wire;

import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The standard {@code grpc.health.v1.Health} service, on the wire, without
 * protobuf-java.
 *
 * <p>dlagent-grpc cannot use grpc-services' {@code HealthStatusManager}: that
 * class speaks the health protocol through generated message classes and so
 * drags in grpc-protobuf and protobuf-java 3.25.8, which is the dependency
 * this module exists to avoid (see its pom). The health messages are as small
 * as the payload ones, so they get the same treatment -- implemented exactly
 * and pinned by golden bytes from protoc run on the vendored
 * {@code proto/health.proto}.
 *
 * <pre>
 *   HealthCheckRequest{service=""}        -> (nothing)
 *   HealthCheckRequest{service="dlagent"} -> 0a 07 64 6c 61 67 65 6e 74
 *   HealthCheckResponse{UNKNOWN}          -> (nothing)
 *   HealthCheckResponse{SERVING}          -> 08 01
 *   HealthCheckResponse{NOT_SERVING}      -> 08 02
 *   HealthCheckResponse{SERVICE_UNKNOWN}  -> 08 03
 * </pre>
 *
 * <p>The request is field 1, length-delimited -- structurally the same message
 * as {@code dlagent.v1.Request} -- so its framing is reused rather than
 * written twice. The response is field 1, varint.
 */
public final class HealthWire {

    public static final String SERVICE = "grpc.health.v1.Health";

    /** The status values of {@code HealthCheckResponse.ServingStatus}. */
    public enum ServingStatus {
        UNKNOWN(0),
        SERVING(1),
        NOT_SERVING(2),
        SERVICE_UNKNOWN(3);

        private final int wireValue;

        ServingStatus(int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }
    }

    /** {@code HealthCheckRequest}: the service name, "" meaning the server. */
    public static final MethodDescriptor.Marshaller<String> REQUEST =
            new MethodDescriptor.Marshaller<String>() {
                @Override
                public InputStream stream(String value) {
                    byte[] name = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
                    return new ByteArrayInputStream(PayloadMarshaller.encode(name));
                }

                @Override
                public String parse(InputStream stream) {
                    return new String(PayloadMarshaller.decode(readFully(stream)),
                            StandardCharsets.UTF_8);
                }
            };

    /** {@code HealthCheckResponse}: one varint field. */
    public static final MethodDescriptor.Marshaller<ServingStatus> RESPONSE =
            new MethodDescriptor.Marshaller<ServingStatus>() {
                @Override
                public InputStream stream(ServingStatus value) {
                    return new ByteArrayInputStream(encodeStatus(value));
                }

                @Override
                public ServingStatus parse(InputStream stream) {
                    return decodeStatus(readFully(stream));
                }
            };

    public static final MethodDescriptor<String, ServingStatus> CHECK =
            MethodDescriptor.<String, ServingStatus>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, "Check"))
                    .setRequestMarshaller(REQUEST)
                    .setResponseMarshaller(RESPONSE)
                    .build();

    public static final MethodDescriptor<String, ServingStatus> WATCH =
            MethodDescriptor.<String, ServingStatus>newBuilder()
                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, "Watch"))
                    .setRequestMarshaller(REQUEST)
                    .setResponseMarshaller(RESPONSE)
                    .build();

    /** Visible for the golden test. */
    static byte[] encodeStatus(ServingStatus status) {
        if (status == null || status == ServingStatus.UNKNOWN) {
            // Field 1 holding its default value is not serialized.
            return new byte[0];
        }
        return new byte[] {0x08, (byte) status.wireValue()};
    }

    /** Visible for the golden test. */
    static ServingStatus decodeStatus(byte[] message) {
        if (message.length == 0) {
            return ServingStatus.UNKNOWN;
        }
        if (message.length != 2 || message[0] != 0x08) {
            throw Status.INTERNAL
                    .withDescription("malformed HealthCheckResponse")
                    .asRuntimeException();
        }
        for (ServingStatus s : ServingStatus.values()) {
            if (s.wireValue() == message[1]) {
                return s;
            }
        }
        return ServingStatus.UNKNOWN;
    }

    private static byte[] readFully(InputStream in) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(32);
            byte[] buf = new byte[1024];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw Status.INTERNAL.withCause(e)
                    .withDescription("failed to read a health message").asRuntimeException();
        }
    }

    private HealthWire() {
    }
}
