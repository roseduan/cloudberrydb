package org.apache.cloudberry.iceberg.gateway.auth;

import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fail-closed startup checks (sub-project C). Refuses to boot when the deployment cannot actually
 * authenticate — the exact condition PoC #382 exploited (trust HBA + superuser authenticator).
 */
public final class AuthPreflight {

    private static final Logger LOG = LoggerFactory.getLogger(AuthPreflight.class);

    /** Structured result so tests can assert without catching System.exit. */
    public static final class Result {
        public final boolean ok;
        public final String reason; // null when ok
        Result(boolean ok, String reason) { this.ok = ok; this.reason = reason; }
    }

    public static void verifyOrExit(GatewayConfig config) {
        Result r = check(config.get("pg.jdbcUrl"),
                config.get("pg.authenticator.user"),
                config.get("pg.authenticator.password", ""));
        if (!r.ok) {
            LOG.error("STARTUP ABORTED — auth preflight failed: {}", r.reason);
            System.exit(1);
        }
        LOG.info("auth preflight OK (authenticator is password-checked, non-superuser)");

        Result jwtResult = checkJwt(config);
        if (!jwtResult.ok) {
            LOG.error("STARTUP ABORTED — JWT preflight failed: {}", jwtResult.reason);
            System.exit(1);
        }
        String b64 = config.get("jwt.hmacSecretBase64", "");
        if (b64 == null || b64.isBlank()) {
            LOG.warn("jwt.hmacSecretBase64 is empty — using an EPHEMERAL JWT key — "
                    + "tokens die on restart; NOT for production; set jwt.hmacSecretBase64 "
                    + "(>=32 bytes base64) for a persistent HS256 secret.");
        } else {
            LOG.info("JWT preflight OK (configured HMAC secret meets HS256 length requirement)");
        }
    }

    /**
     * Config-only JWT signing-secret check (issue #382 C2 decision J1). No DB connection needed.
     *
     * <ul>
     *   <li>blank secret + {@code jwt.requireConfiguredSecret=true} -&gt; fail-closed.
     *   <li>blank secret + require=false -&gt; ok (caller must warn: ephemeral key).
     *   <li>non-blank secret decoding to &lt;32 bytes -&gt; fail (HS256 needs &gt;=256 bit).
     *   <li>non-blank secret &gt;=32 bytes -&gt; ok.
     * </ul>
     */
    public static Result checkJwt(GatewayConfig config) {
        String b64 = config.get("jwt.hmacSecretBase64", "");
        boolean requireConfigured = config.getBool("jwt.requireConfiguredSecret", false);
        if (b64 == null || b64.isBlank()) {
            if (requireConfigured) {
                return new Result(false, "jwt.hmacSecretBase64 is empty but "
                        + "jwt.requireConfiguredSecret=true — configure a persistent HS256 secret "
                        + "(>=32 bytes base64)");
            }
            return new Result(true, null);
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException e) {
            return new Result(false, "jwt.hmacSecretBase64 is not valid base64: " + e.getMessage());
        }
        if (decoded.length < 32) {
            return new Result(false, "jwt.hmacSecretBase64 decodes to " + decoded.length
                    + " bytes; HS256 requires >=32");
        }
        return new Result(true, null);
    }

    /** Package-visible for tests. */
    static Result check(String jdbcUrl, String authUser, String authPass) {
        // Missing credentials must surface as a structured preflight failure, not as the
        // NullPointerException Properties.setProperty(..., null) would throw inside canConnect.
        if (authUser == null || authUser.isBlank()) {
            return new Result(false, "pg.authenticator.user is not configured");
        }
        if (authPass == null) {
            return new Result(false, "pg.authenticator.password is not configured");
        }
        // 1) A WRONG password must be REJECTED. If it connects, HBA is trust/peer/ident -> auth void.
        if (canConnect(jdbcUrl, authUser, authPass + "__wrong_canary__")) {
            return new Result(false, "authenticator '" + authUser
                    + "' connects with a WRONG password — pg_hba is trust/passwordless. "
                    + "Harden pg_hba to scram-sha-256 for the gateway's connections.");
        }
        // 2) The real credentials must connect (else the pool is dead on arrival).
        if (!canConnect(jdbcUrl, authUser, authPass)) {
            return new Result(false, "authenticator '" + authUser
                    + "' cannot connect with the configured password.");
        }
        // 3) Authenticator must NOT be superuser/bypassrls (SET ROLE model requires low privilege).
        Boolean isSuper = querySuper(jdbcUrl, authUser, authPass);
        if (isSuper == null) {
            return new Result(false, "could not determine authenticator privilege level.");
        }
        if (isSuper) {
            return new Result(false, "authenticator '" + authUser
                    + "' is superuser/bypassrls; use a dedicated NOINHERIT NOSUPERUSER role "
                    + "(see security/create_authenticator.sql).");
        }
        return new Result(true, null);
    }

    private static boolean canConnect(String url, String user, String pass) {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", pass == null ? "" : pass);
        p.setProperty("loginTimeout", "5");
        try (Connection ignored = DriverManager.getConnection(url, p)) {
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private static Boolean querySuper(String url, String user, String pass) {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", pass == null ? "" : pass);
        p.setProperty("loginTimeout", "5");
        try (Connection c = DriverManager.getConnection(url, p);
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user")) {
            return rs.next() ? rs.getBoolean(1) : Boolean.FALSE;
        } catch (SQLException e) {
            return null;
        }
    }

    private AuthPreflight() {}
}
