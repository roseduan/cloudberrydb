package org.apache.cloudberry.iceberg.gateway.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@code GET /v1/config} — the client handshake. The {@code endpoints} field explicitly advertises
 * only the read-only endpoint set so clients (Spark/Trino/PyIceberg) adapt and never attempt writes
 * (design §4.1).
 *
 * <p>The advertised strings use the Iceberg REST OpenAPI spec's canonical {@code {prefix}}-templated
 * form (e.g. {@code "GET /v1/{prefix}/namespaces"}), matching the literal {@code Capability}
 * constants that strict clients (e.g. PyIceberg &ge;0.11) check the {@code endpoints} array against.
 * iceberg-core 1.6.1 has no {@code org.apache.iceberg.rest.Endpoint} constants to source these from,
 * so they are hardcoded here for the same read-only operation set.
 *
 * <p>We do <b>not</b> set a {@code prefix} property in {@code defaults}/{@code overrides}: Phase 1
 * serves a single PG database per gateway instance, and the gateway's actual routes are prefix-less
 * (the stock iceberg-core REST routes with no {@code {prefix}} segment resolved). Clients resolve
 * an empty prefix and call the prefix-less routes we actually serve — the advertised {@code
 * endpoints} strings are spec-shaped capability declarations only, not the literal paths to call.
 * Multi-database prefixes are Phase 3 and would require custom routing beyond the stock adapter
 * (see implementation-plan §Phase 3).
 *
 * <p><b>{@code s3.endpoint} is intentionally not advertised</b> (issue #382 C2 Task 4): this
 * endpoint stays unauthenticated per design decision C1 (clients call it pre-auth to discover the
 * read-only route set), so it must not leak the internal object-storage URL to unauthenticated
 * callers. Clients are expected to carry their own storage configuration; the gateway does not
 * vend it. {@code s3.path-style-access} is kept because it is a non-sensitive boolean hint.
 */
public final class ConfigEndpoint extends HttpServlet {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The read-only endpoint set advertised to clients, in the Iceberg REST OpenAPI spec's
     * canonical {@code {prefix}}-templated form. The gateway itself serves these prefix-less
     * (no {@code prefix} property is set in the config response), but the {@code endpoints}
     * array is a capability declaration matched verbatim against strict clients' spec-literal
     * constants, so it must use the templated form regardless of how routes are actually served.
     */
    private static final List<String> READ_ONLY_ENDPOINTS = List.of(
            "GET /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}",
            "HEAD /v1/{prefix}/namespaces/{namespace}",
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
    );

    private final GatewayConfig config;

    public ConfigEndpoint(GatewayConfig config) {
        this.config = config;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, Object> defaults = Map.of(
                "s3.path-style-access", config.get("s3.path-style-access", "true"));
        Map<String, Object> body = Map.of(
                "defaults", defaults,
                "overrides", Map.of(),
                "endpoints", READ_ONLY_ENDPOINTS
        );
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        MAPPER.writeValue(resp.getOutputStream(), body);
    }
}
