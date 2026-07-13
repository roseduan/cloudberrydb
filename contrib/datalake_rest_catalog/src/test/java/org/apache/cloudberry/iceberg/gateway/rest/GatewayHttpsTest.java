package org.apache.cloudberry.iceberg.gateway.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GatewayHttpsTest {
    private Server server;

    @AfterEach void tearDown() throws Exception { if (server != null) server.stop(); }

    @Test
    void servesConfigOverHttpsWhenEnabled() throws Exception {
        // A GatewayConfig with https enabled pointing at the test keystore.
        GatewayConfig cfg = GatewayConfig.forTest(java.util.Map.of(
                "server.https.enabled", "true",
                "server.https.port", "0",
                "server.https.keystore.path", getClass().getResource("/test-keystore.p12").getPath(),
                "server.https.keystore.password", "changeit"));
        InMemoryCatalog cat = new InMemoryCatalog(); cat.initialize("t", java.util.Map.of());
        CatalogResolver resolver = r -> cat;
        server = GatewayServer.buildServer(0, resolver, new org.apache.cloudberry.iceberg.gateway.auth.JwtService(cfg), null, cfg);
        server.start();
        int httpsPort = ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getLocalPort();

        HttpClient http = HttpClient.newBuilder().sslContext(trustAll()).build();
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create("https://localhost:" + httpsPort + "/v1/config")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    void refusesToStartWhenHttpsRequiredButKeystoreMissing() throws Exception {
        // required=true with a blank keystore path must fail closed rather than silently
        // falling back to plaintext HTTP (issue #382 C2 Task 1).
        GatewayConfig cfg = GatewayConfig.forTest(java.util.Map.of(
                "server.https.enabled", "true",
                "server.https.required", "true",
                "server.https.keystore.path", ""));
        InMemoryCatalog cat = new InMemoryCatalog(); cat.initialize("t", java.util.Map.of());
        CatalogResolver resolver = r -> cat;
        JwtService jwt = new JwtService(cfg);
        assertThrows(IllegalStateException.class,
                () -> GatewayServer.buildServer(0, resolver, jwt, null, cfg));
    }

    @Test
    void refusesToStartWhenHttpsRequiredButDisabled() throws Exception {
        // required=true but enabled=false must fail closed rather than silently starting a
        // plaintext listener under a config key that promises HTTPS (issue #382 C2 final review).
        GatewayConfig cfg = GatewayConfig.forTest(java.util.Map.of(
                "server.https.required", "true",
                "server.https.enabled", "false"));
        InMemoryCatalog cat = new InMemoryCatalog(); cat.initialize("t", java.util.Map.of());
        CatalogResolver resolver = r -> cat;
        JwtService jwt = new JwtService(cfg);
        assertThrows(IllegalStateException.class,
                () -> GatewayServer.buildServer(0, resolver, jwt, null, cfg));
    }

    @Test
    void defaultOnGeneratesSelfSignedAndServesHttps() throws Exception {
        // issue #382 production readiness: HTTPS defaults on (server.https.enabled=true in
        // gateway.properties) with no keystore configured and required=false. Rather than fail
        // closed (a footgun for the new default) or fall back to plaintext (defeating the point
        // of defaulting HTTPS on), the gateway lazily mints a throwaway self-signed PKCS12 and
        // serves HTTPS with it. forTest(Map.of()) applies no overrides, so this exercises the
        // bundled default exactly as a fresh install would see it (except port=0 for an
        // ephemeral test port).
        GatewayConfig cfg = GatewayConfig.forTest(java.util.Map.of("server.https.port", "0"));
        InMemoryCatalog cat = new InMemoryCatalog(); cat.initialize("t", java.util.Map.of());
        CatalogResolver resolver = r -> cat;
        server = GatewayServer.buildServer(0, resolver, new JwtService(cfg), null, cfg);
        server.start();
        int httpsPort = ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getLocalPort();

        HttpClient http = HttpClient.newBuilder().sslContext(trustAll()).build();
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create("https://localhost:" + httpsPort + "/v1/config")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    private static SSLContext trustAll() throws Exception {
        SSLContext c = SSLContext.getInstance("TLS");
        c.init(null, new TrustManager[]{ new X509TrustManager() {
            public void checkClientTrusted(java.security.cert.X509Certificate[] a, String b) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] a, String b) {}
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
        }}, new java.security.SecureRandom());
        return c;
    }
}
