package org.apache.cloudberry.iceberg.gateway.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end proof that the read chain works: HTTP -> {@link GatewayFilter} -> iceberg-core
 * RESTCatalogServlet/Adapter -> {@link org.apache.cloudberry.iceberg.gateway.catalog.UserScopedCatalog} ->
 * catalog. The PG-backed catalog is swapped for an {@link InMemoryCatalog} so the transport +
 * auth + routing are exercised independently of the still-stubbed system-table reader.
 */
class GatewayReadPathTest {

    private Server server;
    private HttpClient http;
    private String baseUrl;
    private String bearer;

    @BeforeEach
    void setUp() throws Exception {
        // Seed an in-memory catalog with one namespace + one table.
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", java.util.Map.of());
        catalog.createNamespace(Namespace.of("db"));
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        catalog.createTable(TableIdentifier.of("db", "events"), schema);

        // This test drives plain HTTP end-to-end (servlet/filter/routing); pin HTTPS off since
        // the gateway defaults it on (issue #382 production readiness).
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

    private HttpResponse<String> get(String path, boolean auth) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path)).GET();
        if (auth) {
            b.header("Authorization", "Bearer " + bearer);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void configAdvertisesReadOnlyEndpointsWithoutAuth() throws Exception {
        HttpResponse<String> resp = get("/v1/config", false);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("endpoints").contains("/v1/{prefix}/namespaces");
    }

    @Test
    void configAdvertisesCanonicalPrefixTemplatedEndpoints() throws Exception {
        // PyIceberg >=0.11 does a strict endpoint-capability check against the Iceberg REST
        // OpenAPI spec's canonical {prefix}-templated Capability constants (issue #382). The
        // gateway must advertise exactly those literal strings for the read-only operation set,
        // even though it actually serves the routes prefix-less (no `prefix` property is set).
        HttpResponse<String> resp = get("/v1/config", false);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body())
                .contains("\"GET /v1/{prefix}/namespaces\"")
                .contains("\"GET /v1/{prefix}/namespaces/{namespace}\"")
                .contains("\"HEAD /v1/{prefix}/namespaces/{namespace}\"")
                .contains("\"GET /v1/{prefix}/namespaces/{namespace}/tables\"")
                .contains("\"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}\"")
                .contains("\"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}\"");
        // Config must NOT set a `prefix` property (defaults/overrides) — clients resolve an
        // empty prefix and call our actual prefix-less routes.
        assertThat(resp.body()).doesNotContain("\"prefix\"");
    }

    @Test
    void configDoesNotLeakS3Endpoint() throws Exception {
        // Issue #382 C2 Task 4: /v1/config is unauthenticated (design decision C1), so it must
        // never advertise the internal object-storage URL. Non-sensitive hints (e.g. path-style
        // access) and the read-only endpoint set stay.
        HttpResponse<String> resp = get("/v1/config", false);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).doesNotContain("s3.endpoint");
        assertThat(resp.body()).contains("s3.path-style-access");
        assertThat(resp.body()).contains("endpoints").contains("/v1/{prefix}/namespaces");
    }

    @Test
    void listNamespaces() throws Exception {
        HttpResponse<String> resp = get("/v1/namespaces", true);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("db");
    }

    @Test
    void listTables() throws Exception {
        HttpResponse<String> resp = get("/v1/namespaces/db/tables", true);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("events");
    }

    @Test
    void loadTableReturnsMetadata() throws Exception {
        HttpResponse<String> resp = get("/v1/namespaces/db/tables/events", true);
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).contains("metadata").contains("schema").contains("\"id\"");
    }

    @Test
    void missingTokenIsRejected() throws Exception {
        HttpResponse<String> resp = get("/v1/namespaces", false);
        assertThat(resp.statusCode()).isEqualTo(401);
    }

    @Test
    void writeVerbIsRejectedWith405() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/namespaces"))
                .header("Authorization", "Bearer " + bearer)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"namespace\":[\"x\"]}"))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(405);
        assertThat(resp.body()).contains("read-only");
    }

    // --- HEAD (namespaceExists / tableExists) -------------------------------------------------
    //
    // iceberg 1.6.1's test-fixture RESTCatalogAdapter has no HEAD routes at all, so before the
    // GatewayFilter HEAD->GET adapter these all returned 400. Assert the spec-compliant mapping:
    // exists -> 204 No Content, missing -> 404, and in both cases an empty body.

    private HttpResponse<Void> head(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Authorization", "Bearer " + bearer)
                .method("HEAD", BodyPublishers.noBody())
                .build();
        return http.send(req, HttpResponse.BodyHandlers.discarding());
    }

    @Test
    void headNamespaceExistsReturns204() throws Exception {
        HttpResponse<Void> resp = head("/v1/namespaces/db");
        assertThat(resp.statusCode()).isEqualTo(204);
        assertThat(resp.body()).isNull();
    }

    @Test
    void headNamespaceMissingReturns404() throws Exception {
        HttpResponse<Void> resp = head("/v1/namespaces/nope");
        assertThat(resp.statusCode()).isEqualTo(404);
        assertThat(resp.body()).isNull();
    }

    @Test
    void headTableExistsReturns204() throws Exception {
        HttpResponse<Void> resp = head("/v1/namespaces/db/tables/events");
        assertThat(resp.statusCode()).isEqualTo(204);
        assertThat(resp.body()).isNull();
    }

    @Test
    void headTableMissingReturns404() throws Exception {
        HttpResponse<Void> resp = head("/v1/namespaces/db/tables/nope");
        assertThat(resp.statusCode()).isEqualTo(404);
        assertThat(resp.body()).isNull();
    }
}
