package org.apache.cloudberry.iceberg.gateway.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.auth.PgCredentialVerifier;
import org.apache.cloudberry.iceberg.gateway.auth.RateLimiter;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code POST /v1/oauth2/tokens} — OAuth2 client-credentials grant, reinterpreted against PG
 * (design §6.1). {@code client_id:client_secret} is treated as PG {@code username:password},
 * verified by opening a real PG connection; on success we return a short-lived bearer JWT.
 *
 * <p>Accepts either {@code client_id}/{@code client_secret} form fields or a combined
 * {@code credential=client_id:client_secret} field (the PyIceberg/Spark convention).
 *
 * <p>Rate-limited (issue #382 C2 Task 3): once {@code client_id} extraction succeeds, and before
 * {@link PgCredentialVerifier#verify}, a {@link RateLimiter} check is made against {@code
 * client_id + "|" + remoteAddr}. Failed attempts count toward the budget too — that's the whole
 * point, since the limiter exists to slow password brute-forcing, not just to throttle chatty
 * clients.
 */
public final class OAuthTokenEndpoint extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_CLIENT_ID_LENGTH = 256;

    private final PgCredentialVerifier verifier;
    private final JwtService jwt;
    private final RateLimiter rateLimiter;

    public OAuthTokenEndpoint(PgCredentialVerifier verifier, JwtService jwt, RateLimiter rateLimiter) {
        this.verifier = verifier;
        this.jwt = jwt;
        this.rateLimiter = rateLimiter;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String clientId = req.getParameter("client_id");
        String clientSecret = req.getParameter("client_secret");

        String credential = req.getParameter("credential");
        if ((clientId == null || clientSecret == null) && credential != null) {
            int sep = credential.indexOf(':');
            if (sep > 0) {
                clientId = credential.substring(0, sep);
                clientSecret = credential.substring(sep + 1);
            }
        }

        // RFC 6749 §2.3.1: client credentials may also arrive via HTTP Basic auth
        // (base64 of client_id:client_secret). That scheme is the RFC's MUST-support
        // method; the body-parameter forms above are its NOT RECOMMENDED alternative.
        // DuckDB's iceberg extension uses Basic auth, sending only grant_type/scope in
        // the body, so we fall back to the Authorization header here.
        if (clientId == null || clientSecret == null) {
            String authz = req.getHeader("Authorization");
            if (authz != null && authz.regionMatches(true, 0, "Basic ", 0, 6)) {
                try {
                    String decoded = new String(
                            java.util.Base64.getDecoder().decode(authz.substring(6).trim()),
                            java.nio.charset.StandardCharsets.UTF_8);
                    int sep = decoded.indexOf(':');
                    if (sep > 0) {
                        clientId = decoded.substring(0, sep);
                        clientSecret = decoded.substring(sep + 1);
                    }
                } catch (IllegalArgumentException ignored) {
                    // malformed base64 -> fall through to the missing-credential error below
                }
            }
        }

        if (clientId == null || clientSecret == null) {
            error(resp, HttpServletResponse.SC_BAD_REQUEST, "invalid_request",
                    "Missing client_id/client_secret (or credential)");
            return;
        }

        // client_id is attacker-supplied and unvalidated, and it becomes (part of) the rate
        // limiter's map key (issue #382 C2 review finding). Reject oversized values up front so
        // an attacker can't inflate per-key memory by sending huge client_ids; this is on top of
        // (not instead of) the rate limiter's own bounded key count.
        if (clientId.length() > MAX_CLIENT_ID_LENGTH) {
            error(resp, HttpServletResponse.SC_BAD_REQUEST, "invalid_request",
                    "client_id too long");
            return;
        }

        String rateLimitKey = clientId + "|" + req.getRemoteAddr();
        if (!rateLimiter.tryAcquire(rateLimitKey)) {
            // Security event (issue #382 C4 observability): budget exhaustion is the signal a
            // brute-force/credential-stuffing run is in progress against this client_id+IP pair.
            LOG.warn("OAuth token request rate-limited: client_id={} remoteAddr={}",
                    clientId, req.getRemoteAddr());
            resp.setHeader("Retry-After", String.valueOf(rateLimiter.getWindowSeconds()));
            error(resp, 429, "rate_limited",
                    "Too many token requests for this client; try again later");
            return;
        }

        if (!verifier.verify(clientId, clientSecret)) {
            // Security event (issue #382 C4 observability): a failed PG auth attempt through the
            // gateway. Never log clientSecret (it's the PG password).
            LOG.warn("OAuth token request denied (invalid_client): client_id={} remoteAddr={}",
                    clientId, req.getRemoteAddr());
            error(resp, HttpServletResponse.SC_UNAUTHORIZED, "invalid_client",
                    "PostgreSQL authentication failed");
            return;
        }

        String token = jwt.issue(clientId);
        // expires_in must reflect the configured jwt.ttlSeconds (JwtService), not a literal, so
        // clients that refresh on this value stay in step with the token's actual exp claim.
        Map<String, Object> body = Map.of(
                "access_token", token,
                "token_type", "bearer",
                "expires_in", jwt.getTtlSeconds()
        );
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        MAPPER.writeValue(resp.getOutputStream(), body);
    }

    private static void error(HttpServletResponse resp, int status, String code, String desc)
            throws IOException {
        resp.setStatus(status);
        resp.setContentType("application/json");
        MAPPER.writeValue(resp.getOutputStream(), Map.of("error", code, "error_description", desc));
    }
}
