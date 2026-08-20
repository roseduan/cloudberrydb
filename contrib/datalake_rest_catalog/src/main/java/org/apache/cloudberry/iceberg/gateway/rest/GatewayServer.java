package org.apache.cloudberry.iceberg.gateway.rest;

import org.apache.cloudberry.iceberg.gateway.auth.AuthPreflight;
import org.apache.cloudberry.iceberg.gateway.auth.JwtService;
import org.apache.cloudberry.iceberg.gateway.auth.PgCredentialVerifier;
import org.apache.cloudberry.iceberg.gateway.auth.RateLimiter;
import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogResolver;
import org.apache.cloudberry.iceberg.gateway.catalog.CatalogStore;
import org.apache.cloudberry.iceberg.gateway.catalog.PgIcebergCatalog;
import org.apache.cloudberry.iceberg.gateway.catalog.PgLakeCatalogStore;
import org.apache.cloudberry.iceberg.gateway.catalog.UserScopedCatalog;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import javax.servlet.DispatcherType;
import org.apache.iceberg.catalog.Catalog;
import org.apache.cloudberry.iceberg.gateway.catalog.NoStorageFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application entry point. Boots a Jetty (javax.servlet) server exposing the read-only Iceberg REST
 * Catalog:
 *
 * <ul>
 *   <li>{@code GET /v1/config} — {@link ConfigEndpoint} (advertises the read-only endpoint set);</li>
 *   <li>{@code POST /v1/oauth/tokens} + {@code /v1/oauth2/tokens} — {@link OAuthTokenEndpoint}
 *       (PG credential exchange -> JWT);</li>
 *   <li>{@code /v1/*} — iceberg-core's {@link RESTCatalogServlet} wrapping a
 *       {@link RESTCatalogAdapter} over a {@link UserScopedCatalog}, fronted by {@link GatewayFilter}
 *       (405 for writes, JWT auth for reads).</li>
 * </ul>
 *
 * <p>Routing / serialization / ErrorModel come from iceberg-core (design §3.1). The adapter routes
 * are prefix-less, matching the Phase 1 single-database-per-instance decision.
 */
public final class GatewayServer {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayServer.class);

    /**
     * Assemble (but do not start) the Jetty server. Extracted so tests can drive the exact same
     * wiring against an in-memory catalog. {@code verifier} may be null when the OAuth endpoint is
     * not needed (e.g. tests that mint JWTs directly).
     */
    public static Server buildServer(int port, CatalogResolver resolver, JwtService jwt,
                                     PgCredentialVerifier verifier, GatewayConfig config) {
        boolean httpsEnabled = config.getBool("server.https.enabled", true);
        boolean httpsRequired = config.getBool("server.https.required", false);
        if (httpsRequired && !httpsEnabled) {
            // A downgrade footgun (issue #382 C2 final review): required=true is a promise that
            // the gateway never serves plaintext. Without this check, required=true+enabled=false
            // silently falls through to the plaintext branch below instead of refusing to start.
            throw new IllegalStateException(
                    "server.https.required=true but server.https.enabled=false — refusing to start a plaintext listener");
        }

        Server server = new Server();
        ServerConnector connector;
        if (httpsEnabled) {
            String ksPath = config.get("server.https.keystore.path", "");
            String ksPassword = config.get("server.https.keystore.password", "");
            if (ksPath.isBlank()) {
                if (httpsRequired) {
                    // Fail-closed (issue #382 C2 review finding C2T1-m2, extended for the
                    // default-on rollout): a deployment that PROMISES HTTPS via `required=true`
                    // must never silently substitute a throwaway self-signed cert.
                    throw new IllegalStateException(
                            "server.https.enabled=true, server.https.required=true, but "
                                    + "server.https.keystore.path is empty — refusing to "
                                    + "auto-generate a self-signed certificate for a deployment "
                                    + "that requires HTTPS; configure a real keystore");
                }
                // HTTPS defaults on (issue #382 production readiness), but most dev/local
                // bring-ups won't have a real certificate handy. Rather than fail closed (which
                // would make the new default a footgun) or fall back to plaintext (which would
                // defeat the point of defaulting HTTPS on), lazily mint a self-signed PKCS12
                // once per process and serve HTTPS with it. Loudly logged since it is NOT
                // suitable for production.
                ksPath = ensureSelfSignedKeystore();
                ksPassword = GENERATED_KEYSTORE_PASSWORD;
                LOG.warn("server.https.enabled=true but server.https.keystore.path is empty — "
                        + "serving HTTPS with an auto-generated, ephemeral, self-signed "
                        + "certificate (CN=localhost) at {}. THIS IS NOT SUITABLE FOR "
                        + "PRODUCTION: configure server.https.keystore.path with a real "
                        + "certificate, or set server.https.enabled=false to opt out. "
                        + "(issue #382)", ksPath);
            }
            SslContextFactory.Server ssl = new SslContextFactory.Server();
            ssl.setKeyStorePath(ksPath);
            ssl.setKeyStorePassword(ksPassword);
            HttpConfiguration httpsCfg = new HttpConfiguration();
            httpsCfg.addCustomizer(new SecureRequestCustomizer(false)); // no SNI host check (self-signed/local)
            connector = new ServerConnector(server,
                    new SslConnectionFactory(ssl, "http/1.1"),
                    new HttpConnectionFactory(httpsCfg));
            connector.setPort(config.getInt("server.https.port", 8443));
        } else {
            connector = new ServerConnector(server);
            connector.setPort(port);
        }
        server.addConnector(connector);

        ServletContextHandler ctx = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        ctx.setContextPath("/");

        // One shared adapter over a user-scoped catalog; per-request role comes from CurrentUser.
        Catalog catalog = new UserScopedCatalog(resolver, CurrentUser::get);
        RESTCatalogAdapter adapter = new RESTCatalogAdapter(catalog);
        RESTCatalogServlet restServlet = new RESTCatalogServlet(adapter);

        // The gate: 405 for writes + JWT auth for reads (skips config/token paths).
        ctx.addFilter(new FilterHolder(new GatewayFilter(jwt)), "/v1/*",
                EnumSet.of(DispatcherType.REQUEST));

        // Exact-path servlets win over the "/v1/*" catalog servlet in the servlet spec.
        ctx.addServlet(new ServletHolder(new ConfigEndpoint(config)), "/v1/config");
        if (verifier != null) {
            // Brute-force guard (issue #382 C2 Task 3): trailing-window limiter keyed by
            // client_id + remote address, counting failed attempts too.
            RateLimiter oauthRateLimiter = new RateLimiter(
                    config.getInt("oauth.rateLimit.maxAttempts", 20),
                    config.getInt("oauth.rateLimit.windowSeconds", 60),
                    config.getInt("oauth.rateLimit.maxKeys", 100_000),
                    System::currentTimeMillis);
            OAuthTokenEndpoint tokens = new OAuthTokenEndpoint(verifier, jwt, oauthRateLimiter);
            ctx.addServlet(new ServletHolder(tokens), "/v1/oauth/tokens");   // Iceberg client default
            ctx.addServlet(new ServletHolder(tokens), "/v1/oauth2/tokens");  // design §6.1 alias
        }
        ctx.addServlet(new ServletHolder(restServlet), "/v1/*");

        server.setHandler(ctx);
        // HTTPS defaults ON (server.https.enabled, issue #382 production readiness) since
        // client_secret == the PG password (issue #382 C2 Task 1). Set server.https.enabled=false
        // to opt back into plaintext (e.g. behind a TLS-terminating proxy).
        return server;
    }

    /** PKCS12 password for the lazily auto-generated self-signed dev keystore. Not a secret worth
     * protecting beyond obscurity: the private key it guards is regenerated per-process and never
     * intended for anything but a same-box, single-run, dev/local HTTPS listener. */
    private static final String GENERATED_KEYSTORE_PASSWORD = "changeit";

    private static volatile String generatedKeystorePath;
    private static final Object GENERATED_KEYSTORE_LOCK = new Object();

    /**
     * Lazily mint (once per process) a throwaway self-signed PKCS12 keystore via the JDK's
     * {@code keytool}, for the {@code server.https.enabled=true} + blank-keystore +
     * {@code server.https.required=false} case (issue #382 production readiness: HTTPS
     * defaults on, but most dev/local bring-ups have no real certificate configured).
     */
    private static String ensureSelfSignedKeystore() {
        String existing = generatedKeystorePath;
        if (existing != null) {
            return existing;
        }
        synchronized (GENERATED_KEYSTORE_LOCK) {
            if (generatedKeystorePath != null) {
                return generatedKeystorePath;
            }
            try {
                File tmp = File.createTempFile("drc-selfsigned-", ".p12");
                if (!tmp.delete()) {
                    // keytool refuses to overwrite; the temp file must not exist beforehand.
                    throw new IOException("could not remove placeholder temp file " + tmp);
                }
                tmp.deleteOnExit();
                String keytool = System.getProperty("java.home") + File.separator + "bin"
                        + File.separator + "keytool";
                ProcessBuilder pb = new ProcessBuilder(
                        keytool,
                        "-genkeypair",
                        "-alias", "drc",
                        "-keyalg", "RSA",
                        "-keysize", "2048",
                        "-validity", "3650",
                        "-storetype", "PKCS12",
                        "-keystore", tmp.getAbsolutePath(),
                        "-storepass", GENERATED_KEYSTORE_PASSWORD,
                        "-dname", "CN=localhost",
                        "-ext", "SAN=dns:localhost,ip:127.0.0.1");
                pb.redirectErrorStream(true);
                Process proc = pb.start();
                String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                int rc = proc.waitFor();
                if (rc != 0) {
                    throw new IllegalStateException(
                            "keytool exited " + rc + " while generating a self-signed HTTPS "
                                    + "keystore: " + output);
                }
                generatedKeystorePath = tmp.getAbsolutePath();
                return generatedKeystorePath;
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException(
                        "Failed to auto-generate a self-signed HTTPS keystore via keytool; "
                                + "configure server.https.keystore.path explicitly or set "
                                + "server.https.enabled=false", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.load();

        AuthPreflight.verifyOrExit(config);

        RoleScopedConnection pool = new RoleScopedConnection(config);
        PgCredentialVerifier verifier = new PgCredentialVerifier(config);
        JwtService jwt = new JwtService(config);

        // No object storage credentials here by design: metadata.json arrives through the
        // database (pg_ext_aux.iceberg_load_metadata) and data files are read by the client
        // against its own credentials. TableOperations.io() still has to return something.
        FileIO sharedIO = new NoStorageFileIO();
        CatalogStore store = new PgLakeCatalogStore();

        // Production resolver: bind each request to a PG-role-scoped read-only catalog.
        CatalogResolver resolver = pgRole ->
                new PgIcebergCatalog(pool, store, sharedIO, pgRole);

        int port = config.getInt("server.port", 8181);
        Server server = buildServer(port, resolver, jwt, verifier, config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping server", e);
            }
            pool.close();
        }));

        boolean httpsOn = config.getBool("server.https.enabled", true);
        int effectivePort = httpsOn ? config.getInt("server.https.port", 8443) : port;
        LOG.info("Starting PG Iceberg REST gateway on {}://*:{} (single database, prefix-less)",
                httpsOn ? "https" : "http", effectivePort);
        server.start();
        server.join();
    }

    private GatewayServer() {
    }
}
