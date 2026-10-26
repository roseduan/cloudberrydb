package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/** The database name is a client-chosen path segment and must not be parsed as JDBC URL syntax. */
class DatabasePoolRegistryTest {

    private static final String TEMPLATE = "jdbc:postgresql://coord:5432/{db}?ssl=false";

    @Test
    void plainNameIsUnchanged() {
        assertThat(DatabasePoolRegistry.jdbcUrlFor(TEMPLATE, "sales_db"))
                .isEqualTo("jdbc:postgresql://coord:5432/sales_db?ssl=false");
    }

    @Test
    void urlSyntaxInTheNameCannotAddParametersOrChangeTheDatabase() {
        String url = DatabasePoolRegistry.jdbcUrlFor(TEMPLATE, "x?ssl=true&y#z/w");
        assertThat(url).isEqualTo("jdbc:postgresql://coord:5432/x%3Fssl%3Dtrue%26y%23z%2Fw?ssl=false");
        // Exactly the template's own query string survives.
        assertThat(url.indexOf('?')).isEqualTo(url.lastIndexOf('?'));
        assertThat(url).doesNotContain("&").doesNotContain("#");
    }

    @Test
    void spaceIsEncoded() {
        assertThat(DatabasePoolRegistry.jdbcUrlFor(TEMPLATE, "my db"))
                .isEqualTo("jdbc:postgresql://coord:5432/my+db?ssl=false");
    }

    @Test
    void idleTimeoutNeverDropsToZeroWhichHikariReadsAsNeverRetire() {
        assertThat(DatabasePoolRegistry.idleTimeoutMs(0)).isEqualTo(10_000L);
        assertThat(DatabasePoolRegistry.idleTimeoutMs(5)).isEqualTo(10_000L);
        assertThat(DatabasePoolRegistry.idleTimeoutMs(30)).isEqualTo(30_000L);
    }

    @Test
    void privilegeAndAuthFailuresAre400WithoutServerText() {
        // What Hikari throws: a RuntimeException wrapping the driver's SQLException.
        String serverText = "FATAL: no pg_hba.conf entry for host \"10.1.2.3\", user \"iceberg_authenticator\"";
        for (String state : new String[]{"28000", "28P01", "42501"}) {
            DatabaseRouter.UnservableDatabaseException e = DatabasePoolRegistry.connectionFailure("db1",
                    new RuntimeException("pool init", new SQLException(serverText, state)));
            assertThat(e.status()).as(state).isEqualTo(400);
            assertThat(e.getMessage()).contains("db1").contains("CONNECT").contains("warehouse")
                    .doesNotContain("pg_hba").doesNotContain("10.1.2.3");
        }
    }

    @Test
    void otherFailuresAre503SoClientsRetry() {
        DatabaseRouter.UnservableDatabaseException network = DatabasePoolRegistry.connectionFailure("db1",
                new RuntimeException(new SQLException("Connection refused", "08001")));
        assertThat(network.status()).isEqualTo(503);
        assertThat(network.getMessage()).doesNotContain("refused");

        DatabaseRouter.UnservableDatabaseException noSql = DatabasePoolRegistry.connectionFailure("db1",
                new IllegalStateException("something else"));
        assertThat(noSql.status()).isEqualTo(503);
    }
}
