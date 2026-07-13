package org.apache.cloudberry.iceberg.gateway.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.auth.PgCredentialVerifier;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Offline integration test: proves the OAuth token endpoint rate-limits repeated auth attempts
 * from the same {@code client_id + remote address} (issue #382 C2 Task 3). The verifier always
 * fails (bogus, unreachable JDBC URL, same trick as {@code PgCredentialVerifierTest}), so every
 * attempt within budget is a 401; once the budget is exhausted the endpoint must short-circuit
 * to 429 WITHOUT even attempting verification.
 */
class OAuthRateLimitTest {

    private static final int MAX_ATTEMPTS = 3;

    private Server server;
    private HttpClient http;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());

        // This test drives plain HTTP end-to-end (rate-limiter/filter); pin HTTPS off since the
        // gateway defaults it on (issue #382 production readiness).
        GatewayConfig config = GatewayConfig.forTest(Map.of(
                "oauth.rateLimit.maxAttempts", String.valueOf(MAX_ATTEMPTS),
                "oauth.rateLimit.windowSeconds", "60",
                "server.https.enabled", "false"));
        JwtService jwt = new JwtService(config);
        PgCredentialVerifier verifier = new PgCredentialVerifier(
                "jdbc:postgresql://127.0.0.1:1/nope?connectTimeout=1&loginTimeout=1");

        CatalogResolver resolver = pgRole -> catalog;
        server = GatewayServer.buildServer(0, resolver, jwt, verifier, config);
        server.start();

        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        baseUrl = "http://localhost:" + port;
        http = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    private HttpResponse<String> postToken() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/oauth2/tokens"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(BodyPublishers.ofString("credential=someuser:pw"))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void exceedingBudgetReturns429WithRetryAfter() throws Exception {
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            HttpResponse<String> resp = postToken();
            assertThat(resp.statusCode())
                    .as("attempt %d should be a plain auth failure, not rate-limited", i + 1)
                    .isEqualTo(401);
            assertThat(resp.body()).contains("invalid_client");
        }

        HttpResponse<String> limited = postToken();
        assertThat(limited.statusCode()).isEqualTo(429);
        assertThat(limited.body()).contains("rate_limited");
        assertThat(limited.headers().firstValue("Retry-After")).isPresent();
        assertThat(limited.headers().firstValue("Retry-After").get()).isEqualTo("60");
    }
}
