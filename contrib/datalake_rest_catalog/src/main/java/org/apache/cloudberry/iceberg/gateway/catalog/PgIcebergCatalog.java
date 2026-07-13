package org.apache.cloudberry.iceberg.gateway.catalog;

import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection;
import org.apache.cloudberry.iceberg.gateway.auth.RoleScopedConnection.ScopedSession;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only {@link Catalog} backed by PostgreSQL system tables. All write paths throw
 * {@link UnsupportedOperationException} (design §2.2); the REST layer turns those into HTTP 405.
 *
 * <p>The current end-user (PG role) is resolved per-request from the JWT subject. In this skeleton
 * it is injected at construction; wire a per-request instance (or a ThreadLocal) in the REST layer.
 */
public final class PgIcebergCatalog implements Catalog, SupportsNamespaces {

    private static final Logger LOG = LoggerFactory.getLogger(PgIcebergCatalog.class);

    private String name;
    private final RoleScopedConnection pool;
    private final CatalogStore reader;
    private final FileIO fileIO;
    private final String pgRole;

    public PgIcebergCatalog(RoleScopedConnection pool, CatalogStore reader, FileIO fileIO, String pgRole) {
        this.pool = pool;
        this.reader = reader;
        this.fileIO = fileIO;
        this.pgRole = pgRole;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    // ------------------------------------------------------------------ read paths

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            return reader.listTables(s.connection(), pgRole, namespace);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        // Authorize first; unauthorized -> NoSuchTableException so we don't leak existence (§6.2).
        try (ScopedSession s = pool.openAsAuthenticator()) {
            if (!reader.canSelect(s.connection(), pgRole, identifier)) {
                // Security event (issue #382 C4 observability): canSelect() is deliberately
                // indistinguishable to the *client* between "doesn't exist" and "exists but no
                // grant" (§6.2 anti-enumeration), but operators auditing access still want a
                // record of the denial. INFO (not WARN): this fires on every not-found lookup too,
                // not just genuine RBAC violations, so it is not by itself an alarm signal.
                LOG.info("loadTable denied or not found: role={} table={}", pgRole, identifier);
                throw new NoSuchTableException("Table does not exist: %s", identifier);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        PgTableOperations ops = new PgTableOperations(pool, pgRole, identifier, reader, fileIO);
        return new BaseTable(ops, identifier.toString());
    }

    // ------------------------------------------------------------------ namespaces (read)

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            return reader.listNamespaces(s.connection(), pgRole);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        try (ScopedSession s = pool.openAsAuthenticator()) {
            Map<String, String> props = reader.loadNamespaceMetadata(s.connection(), pgRole, namespace);
            if (props == null) {
                // Reader returns null for a missing namespace; convert to the standard
                // Iceberg REST 404 instead of letting it flow through as an NPE.
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            return props;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------ write paths -> 405

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        throw new UnsupportedOperationException("Read-only catalog");
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException("Read-only catalog");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException("Read-only catalog");
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        throw new UnsupportedOperationException("Read-only catalog");
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        throw new UnsupportedOperationException("Read-only catalog");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        throw new UnsupportedOperationException("Read-only catalog");
    }
}
