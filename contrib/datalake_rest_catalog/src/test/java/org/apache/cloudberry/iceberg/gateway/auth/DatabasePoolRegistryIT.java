package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection.ScopedSession;
import org.apache.cloudberry.iceberg.gateway.config.GatewayConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Multi-database registry against a live cluster (issue #935): the pg_database, CONNECT and
 * authz-layer checks that the offline tests cannot reach. Creates and drops its own databases.
 *
 * <p>Precondition: the deployment's template1 step is done (datalake_fdw and
 * security/rest_catalog_authz.sql installed in template1, iceberg_authenticator exists and
 * pg_hba lets it log in from here with its password). Run with:
 * <pre>
 *   PG_TEST_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/postgres   superuser connection, for setup
 *   DRC_IT_JDBC_URL_TEMPLATE=jdbc:postgresql://127.0.0.1:5432/{db}?ssl=false
 *   DRC_IT_DEFAULT_DB=postgres                                    must not need a warehouse grant
 *   DRC_IT_AUTH_PASSWORD=...                                      iceberg_authenticator's password
 *   mvn test -Dtest=DatabasePoolRegistryIT
 * </pre>
 */
class DatabasePoolRegistryIT {

    private static final String OK = "it935_ok";
    private static final String ODD = "it935 x?ssl=true&y#z";
    private static final String NO_AUTHZ = "it935_noauthz";
    private static final String NO_CONNECT = "it935_noconnect";
    private static final String USER = "it935_user";

    private static String adminUrl;
    private static Properties adminProps;
    private static DatabasePoolRegistry registry;

    @BeforeAll
    static void setUp() throws Exception {
        adminUrl = System.getenv("PG_TEST_JDBC_URL");
        String template = System.getenv("DRC_IT_JDBC_URL_TEMPLATE");
        assumeTrue(adminUrl != null && !adminUrl.isBlank() && template != null && !template.isBlank(),
                "PG_TEST_JDBC_URL / DRC_IT_JDBC_URL_TEMPLATE unset — skipping (needs live cluster)");
        adminProps = new Properties();
        adminProps.setProperty("user", System.getenv().getOrDefault("PG_TEST_USER", "gpadmin"));
        adminProps.setProperty("password", System.getenv().getOrDefault("PG_TEST_PASSWORD", ""));

        dropAll();
        admin("CREATE DATABASE " + OK);
        admin("CREATE DATABASE \"" + ODD + "\"");
        admin("CREATE DATABASE " + NO_AUTHZ + " TEMPLATE template0");
        admin("CREATE DATABASE " + NO_CONNECT);
        admin("REVOKE CONNECT ON DATABASE " + NO_CONNECT + " FROM PUBLIC");
        admin("CREATE ROLE " + USER + " LOGIN");

        String password = System.getenv().getOrDefault("DRC_IT_AUTH_PASSWORD", "");
        String defaultUrl = template.replace("{db}", System.getenv().getOrDefault("DRC_IT_DEFAULT_DB", "postgres"));
        GatewayConfig config = GatewayConfig.forTest(Map.of(
                "pg.jdbcUrl", defaultUrl,
                "pg.jdbcUrlTemplate", template,
                "pg.authenticator.user", "iceberg_authenticator",
                "pg.authenticator.password", password));
        registry = new DatabasePoolRegistry(config,
                new RoleScopedConnection(defaultUrl, "iceberg_authenticator", password, 2));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (registry != null) {
            registry.close();
        }
        if (adminUrl != null && !adminUrl.isBlank()) {
            dropAll();
        }
    }

    @Test
    void servesADatabaseCreatedFromTemplate1() throws Exception {
        assertThat(currentDatabase(registry.forDatabase(OK))).isEqualTo(OK);
    }

    @Test
    void servesADatabaseWhoseNameIsUrlSyntax() throws Exception {
        assertThat(currentDatabase(registry.forDatabase(ODD))).isEqualTo(ODD);
    }

    @Test
    void refusesTemplatesAndMissingDatabasesWithoutConnecting() {
        assertStatus(() -> registry.forDatabase("template1"), 404);
        assertStatus(() -> registry.forDatabase("it935_does_not_exist"), 404);
    }

    @Test
    void namesTheFixForADatabaseWithoutTheAuthzLayer() {
        assertThatThrownBy(() -> registry.forDatabase(NO_AUTHZ))
                .isInstanceOfSatisfying(DatabaseRouter.UnservableDatabaseException.class, e -> {
                    assertThat(e.status()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("rest_catalog_authz.sql");
                });
    }

    @Test
    void authenticatorWithoutConnectIs400WithoutServerText() {
        assertThatThrownBy(() -> registry.forDatabase(NO_CONNECT))
                .isInstanceOfSatisfying(DatabaseRouter.UnservableDatabaseException.class, e -> {
                    assertThat(e.status()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("CONNECT").doesNotContain("FATAL");
                });
    }

    @Test
    void canConnectFollowsTheEndUsersPrivilege() {
        assertThat(registry.canConnect(USER, OK)).isTrue();
        assertThat(registry.canConnect(USER, NO_CONNECT)).isFalse();
        assertThat(registry.canConnect(USER, "it935_does_not_exist")).isFalse();
    }

    private static String currentDatabase(RoleScopedConnection pool) throws SQLException {
        try (ScopedSession s = pool.openAsAuthenticator();
                Statement st = s.connection().createStatement();
                ResultSet rs = st.executeQuery("SELECT current_database()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static void assertStatus(Runnable call, int status) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(DatabaseRouter.UnservableDatabaseException.class,
                        e -> assertThat(e.status()).isEqualTo(status));
    }

    private static void dropAll() throws SQLException {
        for (String db : new String[]{OK, ODD, NO_AUTHZ, NO_CONNECT}) {
            admin("DROP DATABASE IF EXISTS \"" + db + "\" WITH (FORCE)");
        }
        admin("DROP ROLE IF EXISTS " + USER);
    }

    private static void admin(String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(adminUrl, adminProps);
                Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
