package org.apache.cloudberry.iceberg.gateway.auth;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Owns the PostgreSQL connection pool under a single {@code NOINHERIT} authenticator role
 * (PostgREST model, design §6.2) and hands out connections scoped to an end user.
 *
 * <p>Since Task 3 (issue #382 item B), {@link #openAsAuthenticator()} — no {@code SET ROLE} — is
 * the primary production read path: the DB session identity stays {@code iceberg_authenticator},
 * and RBAC filtering happens inside the SECURITY DEFINER {@code pg_ext_aux.iceberg_visible_tables}
 * accessor keyed off a caller-supplied {@code p_role} argument rather than session identity.
 * {@link #openAs(String)} (real {@code SET ROLE}) remains for scenarios that need the session to
 * actually assume the end user's role, notably the direct-DB-attacker simulation test
 * ({@code directDbAccessToMetadataIsDeniedForUnprivilegedRole}).
 *
 * <p><b>Isolation invariant (open question §10.2):</b> before a connection returns to the pool it
 * MUST {@code RESET ROLE} and {@code DISCARD ALL} so no session state (role, temp tables, GUCs)
 * leaks across users. This is enforced in {@link ScopedSession#close()}.
 */
public final class RoleScopedConnection implements AutoCloseable {

    private final HikariDataSource dataSource;

    public RoleScopedConnection(GatewayConfig config) {
        this(config.get("pg.jdbcUrl"),
                config.get("pg.authenticator.user"),
                config.get("pg.authenticator.password"),
                config.getInt("pg.pool.maxSize", 10));
    }

    /** Explicit-args constructor, convenient for tests (e.g. a Testcontainers PostgreSQL). */
    public RoleScopedConnection(String jdbcUrl, String user, String password, int maxPoolSize) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbcUrl);
        hc.setUsername(user);
        hc.setPassword(password);
        hc.setMaximumPoolSize(maxPoolSize);
        hc.setPoolName("pg-iceberg-authenticator");
        this.dataSource = new HikariDataSource(hc);
    }

    /**
     * Borrow a connection and {@code SET ROLE} to the given end user for the duration of the
     * returned session. Closing the session restores the authenticator role and scrubs state.
     */
    public ScopedSession openAs(String pgRole) throws SQLException {
        Connection conn = dataSource.getConnection();
        try {
            // Use a REPEATABLE READ tx so a whole TableMetadata is synthesized from one MVCC
            // snapshot (design §5.1 consistency). Caller commits/rolls back via the session.
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                // Quote the role to avoid injection; validate upstream too.
                st.execute("SET ROLE " + quoteIdent(pgRole));
            }
            return new ScopedSession(conn);
        } catch (SQLException e) {
            conn.close();
            throw e;
        }
    }

    /**
     * Borrow a connection as the authenticator itself — <b>no {@code SET ROLE}</b>. Used for reads
     * that are gated by a SECURITY DEFINER accessor taking a trusted {@code p_role} argument
     * (design D3, issue #382 item B): the DB session identity stays {@code iceberg_authenticator}
     * (the only role granted EXECUTE on {@code pg_ext_aux.iceberg_visible_tables}), while RBAC
     * filtering happens inside the function keyed off the caller-supplied role, not off
     * {@code current_user}/session identity.
     */
    public ScopedSession openAsAuthenticator() throws SQLException {
        Connection conn = dataSource.getConnection();
        try {
            // Same REPEATABLE READ consistency guarantee as openAs(); deliberately no SET ROLE.
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            }
            return new ScopedSession(conn);
        } catch (SQLException e) {
            conn.close();
            throw e;
        }
    }

    private static String quoteIdent(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void close() {
        dataSource.close();
    }

    /** A per-request, role-scoped view over a pooled connection. */
    public static final class ScopedSession implements AutoCloseable {
        private final Connection conn;

        ScopedSession(Connection conn) {
            this.conn = conn;
        }

        public Connection connection() {
            return conn;
        }

        @Override
        public void close() throws SQLException {
            try {
                conn.rollback();          // read-only work; never commit
                conn.setAutoCommit(true); // DISCARD ALL must run OUTSIDE a transaction block
                try (Statement st = conn.createStatement()) {
                    // DISCARD ALL also resets the role, temp tables, and session GUCs
                    // (open question §10.2 — no role/state leakage across pooled users).
                    st.execute("DISCARD ALL");
                }
            } finally {
                conn.close(); // returns to the pool
            }
        }
    }
}
