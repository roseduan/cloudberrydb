package org.apache.cloudberry.iceberg.gateway.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cloudberry.iceberg.gateway.auth.DatabaseRouter;
import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Multi-database mode (issue #935): the Iceberg REST {@code warehouse} names a database, handed
 * back as {@code prefix}; {@link GatewayFilter} strips {@code /v1/{prefix}/}, checks the end
 * user's CONNECT privilege and binds {@link CurrentDatabase} for the catalog resolver. Each
 * database is an in-memory catalog holding a differently named table, so a request that reaches
 * the wrong database is visible in the response.
 */
class MultiDatabaseRoutingTest {

    /** Router over a fixed database set; "alice" may connect to everything but db_locked. */
    private static final class FakeRouter implements DatabaseRouter {
        private final boolean multiDb;

        FakeRouter(boolean multiDb) {
            this.multiDb = multiDb;
        }

        @Override
        public boolean multiDb() {
            return multiDb;
        }

        @Override
        public boolean canConnect(String pgRole, String db) {
            if ("db_flaky".equals(db)) {
                // As canConnect does when the default database cannot answer.
                throw new IllegalStateException("CONNECT privilege check failed");
            }
            return Set.of("postgres", "db2", "my db", "a+b", "template1", "db_noauthz").contains(db);
        }

        @Override
        public void requireServable(String db) {
            if ("template1".equals(db)) {
                throw new UnservableDatabaseException(404, "NoSuchWarehouseException", "template: " + db);
            }
            if ("db_noauthz".equals(db)) {
                throw new UnservableDatabaseException(400, "BadRequestException", "not enabled: " + db);
            }
        }
    }

    private Server server;
    private HttpClient http;
    private String baseUrl;
    private String bearer;

    private static InMemoryCatalog catalogWithTable(String table) {
        InMemoryCatalog c = new InMemoryCatalog();
        c.initialize(table, Map.of());
        c.createNamespace(Namespace.of("sales"));
        c.createTable(TableIdentifier.of("sales", table),
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        return c;
    }

    private void start(boolean multiDb) throws Exception {
        Map<String, InMemoryCatalog> byDb = Map.of(
                "postgres", catalogWithTable("t_default"),
                "db2", catalogWithTable("t_db2"),
                "my db", catalogWithTable("t_mydb"),
                "a+b", catalogWithTable("t_aplusb"));
        // null CurrentDatabase = no prefix = default database, as GatewayServer.main wires it.
        CatalogResolver resolver = pgRole -> {
            String db = CurrentDatabase.get();
            return byDb.get(db == null ? "postgres" : db);
        };
        GatewayConfig config = GatewayConfig.forTest(Map.of("server.https.enabled", "false"));
        JwtService jwt = new JwtService(config);
        bearer = jwt.issue("alice");
        server = GatewayServer.buildServer(0, resolver, jwt, null, config, new FakeRouter(multiDb));
        server.start();
        baseUrl = "http://localhost:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        http = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    private HttpResponse<String> send(String method, String path) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Authorization", "Bearer " + bearer);
        b = "GET".equals(method) ? b.GET() : b.method(method, BodyPublishers.noBody());
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void configReturnsWarehouseAsPrefix() throws Exception {
        start(true);
        assertThat(send("GET", "/v1/config?warehouse=db2").body()).contains("\"prefix\":\"db2\"");
        assertThat(send("GET", "/v1/config?warehouse=my%20db").body()).contains("\"prefix\":\"my%20db\"");
        assertThat(send("GET", "/v1/config").body()).doesNotContain("\"prefix\"");
        // Unauthenticated and unvalidated: an unknown database still just gets its prefix back.
        assertThat(send("GET", "/v1/config?warehouse=nosuchdb").statusCode()).isEqualTo(200);
    }

    @Test
    void prefixSelectsTheDatabase() throws Exception {
        start(true);
        assertThat(send("GET", "/v1/db2/namespaces/sales/tables").body()).contains("t_db2").doesNotContain("t_default");
        assertThat(send("GET", "/v1/namespaces/sales/tables").body()).contains("t_default");
        // CurrentDatabase must not leak into the next request on a reused worker thread.
        assertThat(send("GET", "/v1/db2/namespaces/sales/tables").body()).contains("t_db2");
        assertThat(send("GET", "/v1/namespaces/sales/tables").body()).contains("t_default");
        assertThat(send("GET", "/v1/db2/namespaces/sales/tables/t_db2").statusCode()).isEqualTo(200);
    }

    @Test
    void encodedPrefixIsDecoded() throws Exception {
        start(true);
        assertThat(send("GET", "/v1/my%20db/namespaces/sales/tables").body()).contains("t_mydb");
        // In a path segment '+' is a plus sign (RFC 3986), not the form encoding's space.
        assertThat(send("GET", "/v1/a+b/namespaces/sales/tables").body()).contains("t_aplusb");
        assertThat(send("GET", "/v1/a%2Bb/namespaces/sales/tables").body()).contains("t_aplusb");
        assertThat(send("GET", "/v1/my+db/namespaces/sales/tables").statusCode()).isEqualTo(403);
    }

    @Test
    void prefixRoundTripsThroughConfigForSpacesAndPlusSigns() throws Exception {
        start(true);
        // What a client puts in the path is the prefix exactly as /v1/config returned it.
        assertThat(send("GET", "/v1/config?warehouse=my%20db").body()).contains("\"prefix\":\"my%20db\"");
        assertThat(send("GET", "/v1/config?warehouse=a%2Bb").body()).contains("\"prefix\":\"a%2Bb\"");
        assertThat(GatewayFilter.decodePrefix(GatewayFilter.encodePrefix("my db"))).isEqualTo("my db");
        assertThat(GatewayFilter.decodePrefix(GatewayFilter.encodePrefix("a+b"))).isEqualTo("a+b");
        assertThat(GatewayFilter.decodePrefix(GatewayFilter.encodePrefix("x?y&z#w/v"))).isEqualTo("x?y&z#w/v");
    }

    @Test
    void headExistsWorksUnderAPrefix() throws Exception {
        start(true);
        assertThat(send("HEAD", "/v1/db2/namespaces/sales/tables/t_db2").statusCode()).isEqualTo(204);
        assertThat(send("HEAD", "/v1/db2/namespaces/sales/tables/t_default").statusCode()).isEqualTo(404);
        assertThat(send("HEAD", "/v1/db2/namespaces/sales").statusCode()).isEqualTo(204);
    }

    @Test
    void noConnectPrivilegeIs403AndUnknownDatabaseLooksTheSame() throws Exception {
        start(true);
        HttpResponse<String> locked = send("GET", "/v1/db_locked/namespaces");
        assertThat(locked.statusCode()).isEqualTo(403);
        assertThat(locked.body()).contains("ForbiddenException");
        assertThat(send("GET", "/v1/nosuchdb/namespaces").statusCode()).isEqualTo(403);
    }

    @Test
    void unservableDatabaseUsesTheRoutersStatus() throws Exception {
        start(true);
        assertThat(send("GET", "/v1/template1/namespaces").statusCode()).isEqualTo(404);
        HttpResponse<String> noAuthz = send("GET", "/v1/db_noauthz/namespaces");
        assertThat(noAuthz.statusCode()).isEqualTo(400);
        assertThat(noAuthz.body()).contains("not enabled");
    }

    @Test
    void writesUnderAPrefixAreStillRejected() throws Exception {
        start(true);
        assertThat(send("POST", "/v1/db2/namespaces").statusCode()).isEqualTo(405);
        assertThat(send("DELETE", "/v1/db2/namespaces/sales/tables/t_db2").statusCode()).isEqualTo(405);
    }

    @Test
    void reservedFirstSegmentsAreNotPrefixes() throws Exception {
        start(true);
        // "namespaces" is a spec route, never a database name.
        assertThat(send("GET", "/v1/namespaces/sales").statusCode()).isEqualTo(200);
        assertThat(send("GET", "/v1/namespaces").body()).contains("sales");
    }

    @Test
    void percentEncodedReservedSegmentsAreNotPrefixes() throws Exception {
        start(true);
        // %6eamespaces decodes to "namespaces": it must not be taken as a database name.
        // Taken as a prefix it would be checked as a database ("No CONNECT privilege on database
        // namespaces", 403); left alone it reaches the spec routes, which do not know it.
        HttpResponse<String> resp = send("GET", "/v1/%6eamespaces/namespaces/sales/tables");
        assertThat(resp.statusCode()).isNotEqualTo(200).isNotEqualTo(403);
        assertThat(resp.body()).doesNotContain("database namespaces");
    }

    @Test
    void configRefusesADatabaseNamedLikeASpecRoute() throws Exception {
        start(true);
        // Handed back as a prefix, /v1/namespaces/... could not be told apart from the spec
        // route and would be served from the default database.
        for (String reserved : new String[]{"namespaces", "tables", "config", "oauth"}) {
            HttpResponse<String> resp = send("GET", "/v1/config?warehouse=" + reserved);
            assertThat(resp.statusCode()).as(reserved).isEqualTo(400);
            assertThat(resp.body()).contains("BadRequestException").doesNotContain("\"prefix\"");
        }
        // Only an exact match is reserved.
        assertThat(send("GET", "/v1/config?warehouse=namespaces_db").body()).contains("\"prefix\":\"namespaces_db\"");
    }

    @Test
    void singleDatabaseModeIgnoresAReservedWarehouse() throws Exception {
        start(false);
        HttpResponse<String> resp = send("GET", "/v1/config?warehouse=namespaces");
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).doesNotContain("\"prefix\"");
    }

    @Test
    void anUnexpectedRouterFailureIsA503ErrorModelNotAnHtmlPage() throws Exception {
        start(true);
        HttpResponse<String> resp = send("GET", "/v1/db_flaky/namespaces");
        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("ServiceUnavailableException").doesNotContain("<html");
        // The gateway keeps serving other databases afterwards.
        assertThat(send("GET", "/v1/db2/namespaces/sales/tables").body()).contains("t_db2");
    }

    @Test
    void singleDatabaseModeIgnoresWarehouseAndDoesNotRoutePrefixes() throws Exception {
        start(false);
        assertThat(send("GET", "/v1/config?warehouse=db2").body()).doesNotContain("\"prefix\"");
        assertThat(send("GET", "/v1/namespaces/sales/tables").body()).contains("t_default");
        HttpResponse<String> prefixed = send("GET", "/v1/db2/namespaces/sales/tables");
        assertThat(prefixed.statusCode()).isNotEqualTo(200);
        assertThat(prefixed.body()).doesNotContain("t_db2");
    }
}
