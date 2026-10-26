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
 * <p>{@code prefix}: in single-database mode none is set, so clients resolve an empty prefix and
 * call the prefix-less routes the stock iceberg-core adapter serves. In multi-database mode
 * ({@code pg.jdbcUrlTemplate}, issue #935) a client's {@code warehouse} names the database and is
 * returned as {@code overrides.prefix}; {@link GatewayFilter} strips that segment again before the
 * stock adapter routes the request. Either way the advertised {@code endpoints} strings are
 * spec-shaped capability declarations only, not the literal paths to call.
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
    private final boolean multiDb;

    public ConfigEndpoint(GatewayConfig config) {
        this(config, false);
    }

    public ConfigEndpoint(GatewayConfig config, boolean multiDb) {
        this.config = config;
        this.multiDb = multiDb;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String, Object> defaults = Map.of(
                "s3.path-style-access", config.get("s3.path-style-access", "true"));
        // In multi-database mode the client's warehouse names the database, handed back as the
        // spec's prefix so every later call is routed /v1/{prefix}/... . Nothing is validated
        // here: this endpoint is unauthenticated and must not answer "does database X exist".
        // Existence and CONNECT are enforced by GatewayFilter on the first authenticated call.
        String warehouse = req.getParameter("warehouse");
        if (multiDb && warehouse != null && GatewayFilter.RESERVED_SEGMENTS.contains(warehouse)) {
            // A prefix equal to a spec route segment cannot be told apart from that route, so
            // GatewayFilter would not strip it and the request would be served from the default
            // database. Refuse it here instead. This reveals nothing about which databases exist.
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.setContentType("application/json");
            MAPPER.writeValue(resp.getOutputStream(), Map.of("error", Map.of(
                    "message", "Database name '" + warehouse + "' collides with an Iceberg REST route"
                            + " and cannot be served by this gateway",
                    "type", "BadRequestException",
                    "code", 400)));
            return;
        }
        Map<String, Object> overrides = (multiDb && warehouse != null && !warehouse.isBlank())
                ? Map.of("prefix", GatewayFilter.encodePrefix(warehouse))
                : Map.of();
        Map<String, Object> body = Map.of(
                "defaults", defaults,
                "overrides", overrides,
                "endpoints", READ_ONLY_ENDPOINTS
        );
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        MAPPER.writeValue(resp.getOutputStream(), body);
    }
}
