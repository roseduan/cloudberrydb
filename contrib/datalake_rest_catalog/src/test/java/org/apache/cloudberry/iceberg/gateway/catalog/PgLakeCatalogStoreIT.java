package org.apache.cloudberry.iceberg.gateway.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection;
import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection.ScopedSession;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test against the real lightning-382 cluster (builtin iceberg sales.orders seeded by
 * e2e/seed_builtin.sql). Skips when PG_TEST_JDBC_URL is unset (no cluster).
 *
 * <p>Since Task 3 (issue #382 item B), {@link CatalogStore} reads go through
 * {@code pool.openAsAuthenticator()} (no {@code SET ROLE}) with the end-user role passed as the
 * {@code pgRole} argument, mirroring how the gateway itself calls it: a single trusted DB identity
 * (here, the pool's configured test user — {@code gpadmin} by default) plus a caller-supplied
 * {@code p_role} that the SECURITY DEFINER {@code pg_ext_aux.iceberg_visible_tables} function uses
 * for {@code has_schema_privilege}/{@code has_table_privilege} filtering. {@code pool.openAs(...)}
 * (real {@code SET ROLE}) is still used directly below for {@code directDbAccessToMetadataIsDeniedForUnprivilegedRole},
 * which simulates an attacker connecting to Postgres directly (bypassing the gateway entirely) —
 * exactly the scenario {@code SET ROLE} still models correctly.
 */
class PgLakeCatalogStoreIT {

    private static final TableIdentifier ORDERS = TableIdentifier.of("sales", "orders");
    private static final Namespace SALES = Namespace.of("sales");

    private static RoleScopedConnection pool;
    private static final PgLakeCatalogStore STORE = new PgLakeCatalogStore(null);

    @BeforeAll
    static void setUp() {
        String url = System.getenv("PG_TEST_JDBC_URL");
        assumeTrue(url != null && !url.isBlank(), "PG_TEST_JDBC_URL unset — skipping (needs live cluster)");
        String user = System.getenv().getOrDefault("PG_TEST_USER", "gpadmin");
        String pass = System.getenv().getOrDefault("PG_TEST_PASSWORD", "");
        pool = new RoleScopedConnection(url, user, pass, 4);
    }

    @AfterAll
    static void tearDown() { if (pool != null) pool.close(); }

    private static S3FileIO minioIO() {
        S3FileIO io = new S3FileIO();
        io.initialize(Map.of(
                "s3.endpoint", "http://localhost:9000",
                "s3.access-key-id", "minioadmin",
                "s3.secret-access-key", "minioadmin",
                "s3.path-style-access", "true",
                "client.region", "us-east-1"));
        return io;
    }

    @Test
    void loadTableReadsRealMetadataJson() throws Exception {
        PgLakeCatalogStore store = new PgLakeCatalogStore(minioIO());
        try (ScopedSession s = pool.openAsAuthenticator()) {
            TableMetadata md = store.loadTableMetadata(s.connection(), "iceberg_reader", ORDERS);
            assertThat(md.location()).isNotBlank();
            assertThat(md.schema().findField("id")).isNotNull();
            assertThat(md.schema().findField("name")).isNotNull();
            assertThat(md.currentSnapshot()).isNotNull();
        }
    }

    @Test
    void readerSeesNamespaceAndTable() throws Exception {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            assertThat(STORE.listNamespaces(s.connection(), "iceberg_reader")).contains(SALES);
            assertThat(STORE.listTables(s.connection(), "iceberg_reader", SALES)).contains(ORDERS);
            assertThat(STORE.namespaceExists(s.connection(), "iceberg_reader", SALES)).isTrue();
            assertThat(STORE.canSelect(s.connection(), "iceberg_reader", ORDERS)).isTrue();
        }
    }

    @Test
    void noAccessSeesNothing() throws Exception {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            assertThat(STORE.listNamespaces(s.connection(), "no_access")).doesNotContain(SALES);
            assertThat(STORE.listTables(s.connection(), "no_access", SALES)).isEmpty();
            assertThat(STORE.canSelect(s.connection(), "no_access", ORDERS)).isFalse();
        }
    }

    /**
     * Regression test for the loadNamespace 500 NPE: iceberg-core's
     * GetNamespaceResponse.Builder#setProperties calls properties.containsKey(null) as internal
     * validation. Map.of() (a JDK immutable map) throws NPE on a null-key lookup; a plain
     * HashMap does not. This test would have caught the original Map.of() bug.
     */
    @Test
    void loadNamespaceMetadataIsNullKeyTolerant() throws Exception {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            Map<String, String> props = STORE.loadNamespaceMetadata(s.connection(), "iceberg_reader", SALES);
            assertThat(props).isNotNull();
            assertThat(props.containsKey(null)).isFalse();
        }
    }

    @Test
    void loadNamespaceMetadataReturnsNullForMissingNamespace() throws Exception {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            Map<String, String> props =
                    STORE.loadNamespaceMetadata(s.connection(), "iceberg_reader", Namespace.of("does_not_exist"));
            assertThat(props).isNull();
        }
    }

    /**
     * Defense-in-depth regression for review item I-2: {@code pg_ext_aux.iceberg_visible_tables}
     * must reject a superuser/bypassrls {@code p_role} outright, not just rely on
     * {@code has_*_privilege} (which is always TRUE for a superuser). This models an authenticator
     * credential compromise combined with a forged/superuser {@code p_role} claim — aligns with
     * decision D2's mint-time superuser rejection, applied here as belt-and-suspenders inside the
     * definer function itself. Uses {@code gpadmin} (superuser, bypassrls) as the pool identity —
     * the test cluster's only superuser — passed as {@code p_role} via {@code openAsAuthenticator()},
     * exactly like the gateway would pass an (attacker-controlled) JWT-derived role name.
     */
    @Test
    void superuserPRoleSeesNothingThroughDefinerFunction() throws Exception {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            assertThat(STORE.listTables(s.connection(), "gpadmin", SALES)).isEmpty();
            assertThat(STORE.canSelect(s.connection(), "gpadmin", ORDERS)).isFalse();
        }
    }

    @Test
    void directDbAccessToMetadataIsDeniedForUnprivilegedRole() throws Exception {
        // no_access must NOT be able to read pg_ext_aux.pg_iceberg_metadata directly (bypassing
        // the gateway). Before Task 3 the seed granted SELECT TO PUBLIC -> this leaked.
        try (ScopedSession s = pool.openAs("no_access")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
                try (var st = s.connection().createStatement();
                        var rs = st.executeQuery(
                                "SELECT count(*) FROM pg_ext_aux.pg_iceberg_metadata")) {
                    rs.next();
                }
            }).isInstanceOf(java.sql.SQLException.class)
              .hasMessageContaining("permission denied");
        }
    }
}
