package org.apache.cloudberry.iceberg.gateway.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single gateway gate in front of the iceberg-core REST servlet. For every {@code /v1/*} request
 * except the public handshake/token endpoints it:
 *
 * <ol>
 *   <li>rejects write verbs (POST/PUT/DELETE/PATCH) with HTTP 405 + ErrorModel (design §4.2/§7) —
 *       so writes never reach the underlying catalog;</li>
 *   <li>validates the bearer JWT and binds the PG role to the request thread ({@link CurrentUser}),
 *       returning 401 on a missing/invalid/expired token (design §6.1/§7).</li>
 * </ol>
 */
public final class GatewayFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayFilter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Public paths handled by their own servlets; skipped by this gate. */
    private static final Set<String> PUBLIC_PATHS = Set.of(
            "/v1/config", "/v1/oauth/tokens", "/v1/oauth2/tokens");

    private static final Set<String> WRITE_METHODS = Set.of("POST", "PUT", "DELETE", "PATCH");

    private final JwtService jwt;

    public GatewayFilter(JwtService jwt) {
        this.jwt = jwt;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;

        String path = req.getRequestURI();
        if (PUBLIC_PATHS.contains(path)) {
            chain.doFilter(request, response);
            return;
        }

        // (1) Read-only enforcement — block writes before they reach the catalog.
        if (WRITE_METHODS.contains(req.getMethod().toUpperCase())) {
            errorModel(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED, 405, "MethodNotAllowedException",
                    "This catalog is read-only; write operations are not supported.");
            return;
        }

        // (2) Authentication — validate bearer JWT, resolve PG role.
        String auth = req.getHeader("Authorization");
        if (auth == null || !auth.regionMatches(true, 0, "Bearer ", 0, 7)) {
            // Security event (issue #382 C4 observability): unauthenticated request reached a
            // protected route. WARN (not DEBUG) so it survives default production log levels.
            LOG.warn("401 missing bearer token: {} {} from {}", req.getMethod(), path, req.getRemoteAddr());
            errorModel(resp, HttpServletResponse.SC_UNAUTHORIZED, 401, "NotAuthorizedException",
                    "Missing bearer token");
            return;
        }
        String token = auth.substring(7).trim();
        String pgRole;
        try {
            pgRole = jwt.verifyAndGetUser(token);
        } catch (RuntimeException e) {
            // Security event (issue #382 C4 observability): invalid/expired/tampered JWT. WARN
            // (not DEBUG) so it survives default production log levels; message only, no stack
            // trace, to keep this from being noisy against routine token expiry.
            LOG.warn("401 JWT validation failed: {} {} from {}: {}",
                    req.getMethod(), path, req.getRemoteAddr(), e.getMessage());
            errorModel(resp, HttpServletResponse.SC_UNAUTHORIZED, 401, "NotAuthorizedException",
                    "Invalid or expired token");
            return;
        }

        CurrentUser.set(pgRole);
        try {
            // (3) HEAD support — iceberg 1.6.1's test-fixture RESTCatalogAdapter registers no HEAD
            // routes at all, so every HEAD request the servlet sees returns 400. We synthesize the
            // spec's HEAD semantics (namespaceExists / tableExists) ourselves by delegating to the
            // already-correct GET handler and mapping its outcome onto a bodyless HEAD response.
            if ("HEAD".equals(req.getMethod().toUpperCase()) && isExistsRoute(path)) {
                handleHeadAsGet(req, resp, chain);
            } else {
                chain.doFilter(request, response);
            }
        } finally {
            CurrentUser.clear();
        }
    }

    /**
     * True for the two "exists" routes defined by the Iceberg REST OpenAPI spec:
     * {@code /v1/namespaces/{ns}} (namespaceExists) and {@code /v1/namespaces/{ns}/tables/{table}}
     * (tableExists). Deliberately excludes the list routes {@code /v1/namespaces} and
     * {@code /v1/namespaces/{ns}/tables}, which take no HEAD semantics under the spec.
     */
    private static boolean isExistsRoute(String path) {
        String[] seg = path.split("/");
        // "/v1/namespaces/{ns}"                -> ["", "v1", "namespaces", "{ns}"]                (len 4)
        // "/v1/namespaces/{ns}/tables/{table}"  -> ["", "v1", "namespaces", "{ns}", "tables", "{table}"] (len 6)
        if (seg.length < 4 || !"namespaces".equals(seg[2])) {
            return false;
        }
        if (seg.length == 4) {
            return true;
        }
        return seg.length == 6 && "tables".equals(seg[4]);
    }

    /**
     * Delegates a HEAD request to the GET handler chain (loadNamespace / loadTable), suppressing the
     * body, and maps the captured GET status onto a spec-compliant, bodyless HEAD response: any 2xx
     * (200 from the GET handler) becomes 204 No Content; 404 stays 404; anything else (401/403/5xx)
     * is propagated as-is. Note: tableExists therefore triggers a full loadTable (metadata.json
     * fetch) rather than a cheap existence probe — heavier, but correct; acceptable for now.
     */
    private static void handleHeadAsGet(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws IOException, ServletException {
        MethodOverrideRequest getReq = new MethodOverrideRequest(req);
        CapturingResponse capture = new CapturingResponse(resp);
        chain.doFilter(getReq, capture);

        int captured = capture.capturedStatus();
        int finalStatus = (captured >= 200 && captured < 300) ? HttpServletResponse.SC_NO_CONTENT : captured;
        resp.setStatus(finalStatus);
        // HEAD never carries a body — nothing else to write.
    }

    private static void errorModel(HttpServletResponse resp, int status, int code, String type, String msg)
            throws IOException {
        resp.setStatus(status);
        resp.setContentType("application/json");
        Map<String, Object> body = Map.of("error",
                Map.of("message", msg, "type", type, "code", code));
        MAPPER.writeValue(resp.getOutputStream(), body);
    }

    /** Presents an incoming HEAD request as GET to downstream handlers. */
    private static final class MethodOverrideRequest extends HttpServletRequestWrapper {
        MethodOverrideRequest(HttpServletRequest request) {
            super(request);
        }

        @Override
        public String getMethod() {
            return "GET";
        }
    }

    /**
     * Records the status the wrapped GET handling chain would have sent, while discarding any body
     * it writes — the real {@link HttpServletResponse} is left untouched until the caller applies
     * the mapped HEAD status.
     *
     * <p>iceberg 1.6.1's test-fixture {@code RESTCatalogAdapter.execute()} always rethrows a
     * wrapping {@code RESTException} after invoking its error consumer — even though that consumer
     * already set the correct status (e.g. 404) and wrote the error body. {@code RESTCatalogServlet}
     * then catches that {@code RESTException} and unconditionally calls {@code setStatus(500)}. On a
     * real servlet-container response this second call is a documented no-op because the response
     * was already committed by the first body write; we must emulate that "status locks once the
     * body starts" behavior ourselves, or the correct 404 gets clobbered by the spurious 500.
     */
    private static final class CapturingResponse extends HttpServletResponseWrapper {
        private int status = HttpServletResponse.SC_OK;
        private boolean committed;
        private ServletOutputStream sink;
        private PrintWriter writer;

        CapturingResponse(HttpServletResponse response) {
            super(response);
        }

        int capturedStatus() {
            return status;
        }

        @Override
        public void setStatus(int sc) {
            if (!committed) {
                this.status = sc;
            }
        }

        @Override
        @SuppressWarnings("deprecation")
        public void setStatus(int sc, String sm) {
            if (!committed) {
                this.status = sc;
            }
        }

        @Override
        public void sendError(int sc) {
            if (!committed) {
                this.status = sc;
            }
        }

        @Override
        public void sendError(int sc, String msg) {
            if (!committed) {
                this.status = sc;
            }
        }

        @Override
        public ServletOutputStream getOutputStream() {
            committed = true;
            if (sink == null) {
                sink = new ServletOutputStream() {
                    @Override
                    public void write(int b) {
                        // discard — HEAD responses never carry a body.
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setWriteListener(WriteListener writeListener) {
                        // no-op: never blocks, so no listener callbacks are needed.
                    }
                };
            }
            return sink;
        }

        @Override
        public PrintWriter getWriter() {
            committed = true;
            if (writer == null) {
                writer = new PrintWriter(new OutputStream() {
                    @Override
                    public void write(int b) {
                        // discard — HEAD responses never carry a body.
                    }
                });
            }
            return writer;
        }
    }
}
