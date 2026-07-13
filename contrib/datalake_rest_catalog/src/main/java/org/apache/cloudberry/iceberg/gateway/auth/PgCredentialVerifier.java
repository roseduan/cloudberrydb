package org.apache.cloudberry.iceberg.gateway.auth;

import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Verifies {@code client_id:client_secret} (== PG {@code username:password}) by actually opening a
 * PostgreSQL connection with those credentials (SCRAM handshake done by the driver). A successful
 * connection == authenticated (design §6.1). No second user store.
 */
public final class PgCredentialVerifier {

    private final String jdbcUrl;

    public PgCredentialVerifier(GatewayConfig config) {
        this(config.get("pg.jdbcUrl"));
    }

    /** Explicit-URL constructor, convenient for tests. */
    public PgCredentialVerifier(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * @return true iff: password is non-empty AND the credentials authenticate AND the role is
     *         NOT superuser/bypassrls. Superusers bypass all RBAC, so the gateway must never
     *         front them (sub-project C, decision D2). Empty password is rejected up-front so a
     *         trust/peer HBA path cannot mint tokens for a passwordless "login" (P0-1).
     */
    public boolean verify(String pgUser, String pgPassword) {
        if (pgUser == null || pgUser.isEmpty()) return false;
        if (pgPassword == null || pgPassword.isEmpty()) return false;

        Properties props = new Properties();
        props.setProperty("user", pgUser);
        props.setProperty("password", pgPassword);
        props.setProperty("loginTimeout", "5");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                java.sql.Statement st = conn.createStatement();
                java.sql.ResultSet rs = st.executeQuery(
                        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user")) {
            // No SET ROLE here: current_user == the just-authenticated login role.
            if (rs.next() && rs.getBoolean(1)) {
                return false; // superuser / bypassrls -> refuse (decision D2)
            }
            return true;
        } catch (SQLException e) {
            // 28P01 invalid_password / 28000 invalid_authorization_specification -> auth failure.
            return false;
        }
    }
}
