package org.apache.cloudberry.iceberg.gateway.auth;

import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies {@code client_id:client_secret} (== PG {@code username:password}) by actually opening a
 * PostgreSQL connection with those credentials (SCRAM handshake done by the driver). A successful
 * connection == authenticated (design §6.1). No second user store.
 *
 * <p>A successful connection only proves the password when pg_hba checks it on this path. A
 * sidecar gateway reaches the coordinator over loopback, which cloud clusters initialise with
 * {@code host all all 127.0.0.1/24 trust}, and the deployment fragment only hardens the
 * authenticator's own role. On such a path any password logs any user in. So every successful
 * login is followed by one with a password that cannot be right; if that also gets in, the path
 * is not password-checked and no token is issued. {@link AuthPreflight} applies the same canary
 * to the authenticator at startup.
 */
public final class PgCredentialVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(PgCredentialVerifier.class);

    /** One login attempt: whether the role bypasses RBAC; throws if the login is refused. */
    @FunctionalInterface
    interface Login {
        boolean bypassesRbac(String pgUser, String pgPassword) throws SQLException;
    }

    private final Login login;

    public PgCredentialVerifier(GatewayConfig config) {
        this(config.get("pg.jdbcUrl"));
    }

    /** Explicit-URL constructor, convenient for tests. */
    public PgCredentialVerifier(String jdbcUrl) {
        this((user, password) -> jdbcLogin(jdbcUrl, user, password));
    }

    PgCredentialVerifier(Login login) {
        this.login = login;
    }

    /**
     * @return true iff: password is non-empty AND the credentials authenticate AND a wrong
     *         password for the same role does NOT authenticate AND the role is NOT
     *         superuser/bypassrls. Superusers bypass all RBAC, so the gateway must never front
     *         them (sub-project C, decision D2). Empty password is rejected up-front so a
     *         trust/peer HBA path cannot mint tokens for a passwordless "login" (P0-1).
     */
    public boolean verify(String pgUser, String pgPassword) {
        if (pgUser == null || pgUser.isEmpty()) return false;
        if (pgPassword == null || pgPassword.isEmpty()) return false;

        boolean bypassesRbac;
        try {
            bypassesRbac = login.bypassesRbac(pgUser, pgPassword);
        } catch (SQLException e) {
            // 28P01 invalid_password / 28000 invalid_authorization_specification -> auth failure.
            return false;
        }
        if (bypassesRbac) {
            return false; // superuser / bypassrls -> refuse (decision D2)
        }
        // Only reached with the right password, so a client guessing passwords costs one login.
        // The canary shares nothing with the real password: a backend that truncates passwords
        // (LDAP DES/bcrypt) must not see it as a match, and the real one is not sent twice.
        String canary = "__wrong_canary__" + UUID.randomUUID();
        try {
            login.bypassesRbac(pgUser, canary);
        } catch (SQLException e) {
            if (isAuthenticationFailure(e)) {
                return true; // the wrong password was refused: this path really checks passwords
            }
            // Fail closed. On a trust path a client can make the canary fail for other reasons
            // (connection limit, too many clients, login timeout) by flooding token requests;
            // reading that as "passwords are checked" would hand out tokens with no password.
            LOG.warn("Refusing a token for role '{}': could not confirm that its password is checked"
                    + " (canary login failed with SQLState {}, not an authentication failure)",
                    pgUser, e.getSQLState());
            return false;
        }
        LOG.error("Refusing a token for role '{}': it also logs in with a WRONG password, so pg_hba "
                + "does not check passwords on the gateway's path to the default database "
                + "(trust/peer/ident). Add a password rule for gateway users from the gateway's "
                + "address above the trust rule (issue #935, deployment 2.4).", pgUser);
        return false;
    }

    /** 28P01 invalid_password, 28000 invalid_authorization_specification (e.g. pg_hba reject). */
    static boolean isAuthenticationFailure(SQLException e) {
        String state = e.getSQLState();
        return "28P01".equals(state) || "28000".equals(state);
    }

    private static boolean jdbcLogin(String jdbcUrl, String pgUser, String pgPassword) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", pgUser);
        props.setProperty("password", pgPassword);
        props.setProperty("loginTimeout", "5");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                java.sql.Statement st = conn.createStatement();
                java.sql.ResultSet rs = st.executeQuery(
                        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user")) {
            // No SET ROLE here: current_user == the just-authenticated login role.
            return rs.next() && rs.getBoolean(1);
        }
    }
}
