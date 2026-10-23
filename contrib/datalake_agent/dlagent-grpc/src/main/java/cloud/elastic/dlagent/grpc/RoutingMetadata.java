/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.grpc.wire.DlagentMethods;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Carries the two routing values REST takes from the URL path -- the catalog
 * {@code prefix} and the {@code table} name -- from gRPC metadata to the
 * handler.
 *
 * <p>They travel in metadata rather than in the payload so that the request
 * body stays byte-identical to the REST one (see {@link DlagentMethods}), which
 * leaves the handler needing a way to read them. A {@link Context} key is that
 * way: it is scoped to the call and inherited by whatever the handler runs on,
 * unlike a ThreadLocal, which would leak across calls on a pooled gRPC thread
 * and attribute one query's table name to another's.
 *
 * <p>PERCENT-DECODED. The C client escapes the table name with
 * curl_easy_escape for both transports (encode_path_segment): into the URL
 * path for REST, into this metadata for gRPC, where a raw non-ASCII value is
 * not allowed at all. Spring decodes a path variable as UTF-8 with path
 * semantics, so the values are decoded the same way here: {@code %XX} is a
 * byte, a {@code +} stays a plus. {@code URLDecoder} would be wrong -- it is
 * form decoding and turns {@code +} into a space.
 */
public final class RoutingMetadata {

    private static final Context.Key<String> PREFIX = Context.key("dlagent-prefix");
    private static final Context.Key<String> TABLE = Context.key("dlagent-table");

    private RoutingMetadata() {
    }

    /** The catalog prefix for the call in progress, or null. */
    public static String prefix() {
        return PREFIX.get();
    }

    /** The table name for the call in progress, or null. */
    public static String table() {
        return TABLE.get();
    }

    /** Installs the values from a call's metadata into its context. */
    public static ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                Context context = Context.current()
                        .withValue(PREFIX, decode(headers.get(DlagentMethods.PREFIX_KEY)))
                        .withValue(TABLE, decode(headers.get(DlagentMethods.TABLE_KEY)));
                return Contexts.interceptCall(context, call, headers, next);
            }
        };
    }

    /**
     * Percent-decodes a path segment the way Spring decodes a path variable.
     * A {@code %} not followed by two hex digits is kept as it is; the client
     * never sends one, since curl_easy_escape escapes {@code %} itself.
     */
    static String decode(String value) {
        if (value == null || value.indexOf('%') < 0) {
            return value;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(value.length());
        int i = 0;
        while (i < value.length()) {
            char c = value.charAt(i);
            if (c == '%' && i + 2 < value.length()
                    && Character.digit(value.charAt(i + 1), 16) >= 0
                    && Character.digit(value.charAt(i + 2), 16) >= 0) {
                bytes.write((Character.digit(value.charAt(i + 1), 16) << 4)
                        | Character.digit(value.charAt(i + 2), 16));
                i += 3;
            } else {
                byte[] utf8 = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                bytes.write(utf8, 0, utf8.length);
                i++;
            }
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }
}
