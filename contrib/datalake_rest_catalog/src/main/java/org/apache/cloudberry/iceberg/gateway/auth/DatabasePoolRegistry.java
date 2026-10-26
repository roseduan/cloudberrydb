package org.apache.cloudberry.iceberg.gateway.auth;

import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection.ScopedSession;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps a database name (the Iceberg REST {@code prefix}, chosen by the client through
 * {@code warehouse}) to the authenticator pool for that database. Without
 * {@code pg.jdbcUrlTemplate} the gateway stays single-database and every request uses the
 * default pool, exactly as before.
 *
 * <p>A database is only ever connected to after it has been resolved in {@code pg_database}
 * through the default pool, so a client-supplied name is never put into a JDBC URL unchecked.
 * Template databases are refused outright: a live connection to template1 makes every
 * {@code CREATE DATABASE} in the cluster fail ("source database is being accessed by other
 * users"). Per-database pools drain to zero once idle for the same reason with respect to
 * {@code DROP DATABASE}.
 *
 * <p>Pools are never evicted: a dropped or renamed database keeps its slot under
 * {@code pg.pool.maxDatabases} until the gateway restarts.
 */
public final class DatabasePoolRegistry implements DatabaseRouter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DatabasePoolRegistry.class);

    /** Hikari's floor for idleTimeout; it treats 0 as "never retire" and raises 1..9 s to this. */
    private static final int MIN_IDLE_TIMEOUT_SECONDS = 10;

    private final RoleScopedConnection defaultPool;
    private final String jdbcUrlTemplate;
    private final String user;
    private final String password;
    private final int maxSizePerDb;
    private final long idleTimeoutMs;
    private final KeyedPools<RoleScopedConnection> pools;

    public DatabasePoolRegistry(GatewayConfig config, RoleScopedConnection defaultPool) {
        this.defaultPool = defaultPool;
        this.jdbcUrlTemplate = config.get("pg.jdbcUrlTemplate", "");
        this.user = config.get("pg.authenticator.user");
        this.password = config.get("pg.authenticator.password");
        this.maxSizePerDb = config.getInt("pg.pool.maxSizePerDb", 4);
        this.idleTimeoutMs = idleTimeoutMs(config.getInt("pg.pool.idleTimeoutSeconds", 30));
        this.pools = new KeyedPools<>(config.getInt("pg.pool.maxDatabases", 32));
    }

    @Override
    public boolean multiDb() {
        return jdbcUrlTemplate != null && !jdbcUrlTemplate.isBlank();
    }

    /** The pool for {@code db}; {@code null} (no prefix on the request) means the default database. */
    public RoleScopedConnection forDatabase(String db) {
        if (db == null || !multiDb()) {
            return defaultPool;
        }
        return pools.get(db, this::createPool);
    }

    private RoleScopedConnection createPool(String db) {
        requireListed(db);
        RoleScopedConnection pool;
        try {
            pool = new RoleScopedConnection(
                    jdbcUrlFor(jdbcUrlTemplate, db), user, password, maxSizePerDb,
                    0, idleTimeoutMs, "pg-iceberg-authenticator-" + db);
        } catch (RuntimeException e) {
            // Hikari fails fast on the first connection, e.g. when the authenticator itself
            // lacks CONNECT on this database (REVOKE CONNECT ... FROM PUBLIC), or, on cloud,
            // USAGE on a warehouse the database sets as its default (ALTER DATABASE ... SET
            // warehouse), which is checked at login.
            throw connectionFailure(db, e);
        }
        try {
            requireAuthzInstalled(pool, db);
        } catch (RuntimeException e) {
            pool.close();
            throw e;
        }
        return pool;
    }

    /**
     * The JDBC URL for {@code db}. A database name may legally contain {@code ? & / #}, which
     * would otherwise be parsed as URL syntax, e.g. a database named {@code x?ssl=true} adds a
     * connection parameter. pgjdbc URL-decodes the database segment, so encoding it round-trips.
     */
    static String jdbcUrlFor(String template, String db) {
        return template.replace("{db}", URLEncoder.encode(db, StandardCharsets.UTF_8));
    }

    /** Hikari idleTimeout for a configured number of seconds, never below Hikari's own floor. */
    static long idleTimeoutMs(int seconds) {
        return Math.max(seconds, MIN_IDLE_TIMEOUT_SECONDS) * 1000L;
    }

    /**
     * Maps a failure to reach {@code db} onto what the client sees. The server's own text names
     * internal hosts and roles (pg_hba.conf entries, the authenticator), so it goes to the log
     * only. Privilege and authentication failures are configuration the operator must fix (400);
     * anything else may be transient, so the client should retry (503).
     */
    static UnservableDatabaseException connectionFailure(String db, Throwable e) {
        SQLException sql = firstSqlException(e);
        String state = sql == null ? null : sql.getSQLState();
        LOG.warn("Gateway cannot reach database {} (SQLState {}): {}", db, state,
                sql != null ? sql.getMessage() : e.toString());
        if (state != null && (state.startsWith("28") || state.startsWith("42"))) {
            return new UnservableDatabaseException(400, "BadRequestException",
                    "Gateway cannot connect to database " + db + ": the authenticator needs CONNECT"
                            + " on the database, and on cloud USAGE on the database's default"
                            + " warehouse if one is set (details in the gateway log)");
        }
        return new UnservableDatabaseException(503, "ServiceUnavailableException",
                "Database " + db + " is temporarily unreachable (details in the gateway log)");
    }

    private static SQLException firstSqlException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException) {
                return (SQLException) t;
            }
        }
        return null;
    }

    @Override
    public void requireServable(String db) {
        forDatabase(db);
    }

    /**
     * The end user's CONNECT privilege on {@code db}. The gateway must check it itself: the
     * session that actually connects is the authenticator's, so PG never sees the end user here.
     */
    @Override
    public boolean canConnect(String pgRole, String db) {
        try (ScopedSession s = defaultPool.openAsAuthenticator();
                PreparedStatement ps = s.connection().prepareStatement(
                        "SELECT has_database_privilege(r.oid, d.oid, 'CONNECT')"
                                + " FROM pg_roles r, pg_database d WHERE r.rolname = ? AND d.datname = ?")) {
            ps.setString(1, pgRole);
            ps.setString(2, db);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("CONNECT privilege check failed", e);
        }
    }

    private void requireListed(String db) {
        try (ScopedSession s = defaultPool.openAsAuthenticator();
                PreparedStatement ps = s.connection().prepareStatement(
                        "SELECT datistemplate, datallowconn FROM pg_database WHERE datname = ?")) {
            ps.setString(1, db);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new UnservableDatabaseException(404, "NoSuchWarehouseException",
                            "Database does not exist: " + db);
                }
                if (rs.getBoolean(1) || !rs.getBoolean(2)) {
                    throw new UnservableDatabaseException(404, "NoSuchWarehouseException",
                            "Database is a template or does not allow connections: " + db);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Database lookup failed", e);
        }
    }

    private static void requireAuthzInstalled(RoleScopedConnection pool, String db) {
        // Looked up in pg_proc rather than via to_regprocedure(): the latter needs USAGE on
        // pg_ext_aux, which is exactly what is missing in a database without the authz layer.
        try (ScopedSession s = pool.openAsAuthenticator();
                Statement st = s.connection().createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT has_function_privilege(p.oid, 'EXECUTE') FROM pg_proc p"
                                + " JOIN pg_namespace n ON n.oid = p.pronamespace"
                                + " WHERE n.nspname = 'pg_ext_aux' AND p.proname = 'iceberg_load_metadata'")) {
            if (!rs.next() || !rs.getBoolean(1)) {
                throw new UnservableDatabaseException(400, "BadRequestException",
                        "Database " + db + " is not enabled for the REST catalog: "
                                + "run security/rest_catalog_authz.sql in it");
            }
        } catch (SQLException e) {
            throw connectionFailure(db, e);
        }
    }

    @Override
    public void close() {
        pools.all().forEach(RoleScopedConnection::close);
        defaultPool.close();
    }
}
