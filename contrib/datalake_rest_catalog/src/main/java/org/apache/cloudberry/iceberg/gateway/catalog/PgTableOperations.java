package org.apache.cloudberry.iceberg.gateway.catalog;

import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection;
import java.sql.SQLException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * Read-only {@link TableOperations}. {@link #refresh()} synthesizes {@link TableMetadata} from PG
 * within a single role-scoped, REPEATABLE READ transaction. {@link #commit} is unsupported — this
 * catalog never writes (design §2.2).
 */
public final class PgTableOperations implements TableOperations {

    private final RoleScopedConnection pool;
    private final String pgRole;
    private final TableIdentifier identifier;
    private final CatalogStore reader;
    private final FileIO fileIO;

    private TableMetadata current;

    public PgTableOperations(RoleScopedConnection pool, String pgRole, TableIdentifier identifier,
                             CatalogStore reader, FileIO fileIO) {
        this.pool = pool;
        this.pgRole = pgRole;
        this.identifier = identifier;
        this.reader = reader;
        this.fileIO = fileIO;
    }

    @Override
    public TableMetadata current() {
        if (current == null) {
            return refresh();
        }
        return current;
    }

    @Override
    public TableMetadata refresh() {
        try (RoleScopedConnection.ScopedSession session = pool.openAsAuthenticator()) {
            this.current = reader.loadTableMetadata(session.connection(), pgRole, identifier);
            return current;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load metadata for " + identifier, e);
        }
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Read-only catalog: commit is not supported");
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        // metadata.json is virtualized (never persisted); return a synthetic location.
        return current().location() + "/metadata/" + fileName;
    }

    @Override
    public org.apache.iceberg.io.LocationProvider locationProvider() {
        return org.apache.iceberg.LocationProviders.locationsFor(current().location(), current().properties());
    }
}
