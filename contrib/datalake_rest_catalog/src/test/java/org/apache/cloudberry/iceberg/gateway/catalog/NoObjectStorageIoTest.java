package org.apache.cloudberry.iceberg.gateway.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection;
import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection.ScopedSession;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Test;

/**
 * Proves the loadTable path touches no object storage, now that metadata.json arrives through the
 * database. Reading the code cannot establish this: iceberg-core is free to reach for
 * TableOperations.io() lazily anywhere between BaseTable construction and LoadTableResponse
 * serialization. So the whole path is driven with a FileIO that throws on contact -- if this test
 * passes, the path really is IO-free, and if it ever stops being IO-free the stack trace names the
 * exact caller.
 *
 * <p>The path driven here is the production one: PgIcebergCatalog.loadTable ->
 * CatalogHandlers.loadTable (what RESTCatalogAdapter itself calls) -> JSON serialization with the
 * REST serializers.
 */
class NoObjectStorageIoTest {

    /** Minimal CatalogStore returning the shared fixture document, no database involved. */
    private static final class StubStore implements CatalogStore {
        @Override
        public List<Namespace> listNamespaces(Connection c, String pgRole) {
            return List.of(Namespace.of("db"));
        }

        @Override
        public boolean namespaceExists(Connection c, String pgRole, Namespace ns) {
            return true;
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(Connection c, String pgRole, Namespace ns) {
            return Map.of();
        }

        @Override
        public List<TableIdentifier> listTables(Connection c, String pgRole, Namespace ns) {
            return List.of(TableIdentifier.of(Namespace.of("db"), "t"));
        }

        @Override
        public boolean canSelect(Connection c, String pgRole, TableIdentifier id) {
            return true;
        }

        @Override
        public TableMetadata loadTableMetadata(Connection c, String pgRole, TableIdentifier id) {
            return TableMetadataParser.fromJson(TestMetadataDoc.LOC, TestMetadataDoc.DOC);
        }
    }

    private static PgIcebergCatalog catalogWith(org.apache.iceberg.io.FileIO io) throws Exception {
        RoleScopedConnection pool = mock(RoleScopedConnection.class);
        ScopedSession session = mock(ScopedSession.class);
        when(pool.openAsAuthenticator()).thenReturn(session);
        when(session.connection()).thenReturn(mock(Connection.class));

        PgIcebergCatalog catalog = new PgIcebergCatalog(pool, new StubStore(), io, "alice");
        catalog.initialize("test", Map.of());
        return catalog;
    }

    @Test
    void loadTablePerformsNoObjectStorageIo() throws Exception {
        PgIcebergCatalog catalog = catalogWith(new ThrowingFileIO());
        TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "t");

        // Exactly what RESTCatalogAdapter does for GET /v1/namespaces/{ns}/tables/{table}.
        LoadTableResponse resp = CatalogHandlers.loadTable(catalog, id);

        // Same configuration iceberg-core's own (package-private) RESTObjectMapper applies, so
        // this serializes through exactly the production code path rather than a bean-shaped
        // approximation of it.
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        RESTSerializers.registerAll(mapper);
        String json = mapper.writeValueAsString(resp);

        assertThat(resp.tableMetadata().location()).isEqualTo("s3://warehouse/db/t");
        assertThat(resp.metadataLocation()).isEqualTo(TestMetadataDoc.LOC);
        assertThat(json).contains("\"id\"").contains("metadata-location");
    }

    @Test
    void theTableStillExposesTheInjectedIo() throws Exception {
        // Guards against the proof above going vacuous: if some refactor stopped threading the
        // FileIO through to the table, ThrowingFileIO would never be reachable and the IO-free
        // assertion would pass for the wrong reason.
        PgIcebergCatalog catalog = catalogWith(new ThrowingFileIO());
        Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("db"), "t"));
        assertThat(((BaseTable) table).io()).isInstanceOf(ThrowingFileIO.class);
    }
}
