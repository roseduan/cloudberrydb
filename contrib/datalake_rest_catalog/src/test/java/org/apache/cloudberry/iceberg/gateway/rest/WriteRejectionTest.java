package org.apache.cloudberry.iceberg.gateway.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Offline proof of issue #382's error contract for write requests: the gateway is read-only and
 * must reject any write verb with a 4xx status (per {@link GatewayFilter}), and must never leak a
 * 500 for a request that is rejected purely because it's a write. Mirrors the server/JWT setup
 * from {@link GatewayReadPathTest}, using an {@link InMemoryCatalog} so no PG/MinIO is required.
 */
class WriteRejectionTest {

    private Server server;
    private HttpClient http;
    private String baseUrl;
    private String bearer;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", java.util.Map.of());

        // This test drives plain HTTP end-to-end (GatewayFilter write rejection); pin HTTPS off
        // since the gateway defaults it on (issue #382 production readiness).
        GatewayConfig config = GatewayConfig.forTest(java.util.Map.of("server.https.enabled", "false"));
        JwtService jwt = new JwtService(config);
        bearer = jwt.issue("alice");

        CatalogResolver resolver = pgRole -> catalog; // every user -> same in-memory catalog
        server = GatewayServer.buildServer(0, resolver, jwt, /*verifier*/ null, config);
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

    @Test
    void createTableIsRejectedNotServerError() throws Exception {
        // POST /v1/namespaces/sales/tables is a write endpoint -> GatewayFilter should reject
        // it (405), and must never surface a 500.
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/namespaces/sales/tables"))
                .header("Authorization", "Bearer " + bearer)
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isBetween(400, 499);
        assertThat(resp.statusCode()).isNotEqualTo(500);
    }
}
