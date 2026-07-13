package org.apache.cloudberry.iceberg.gateway.catalog;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A single {@link Catalog} instance handed to the shared REST adapter. Every call is delegated to a
 * per-user catalog resolved from the current request's PG role, so one adapter safely serves all
 * users while PostgreSQL authorization (GRANT/RLS) stays per-request (design §6.2).
 */
public final class UserScopedCatalog implements Catalog, SupportsNamespaces {

    private final CatalogResolver resolver;
    private final Supplier<String> currentRole;

    public UserScopedCatalog(CatalogResolver resolver, Supplier<String> currentRole) {
        this.resolver = resolver;
        this.currentRole = currentRole;
    }

    private Catalog delegate() {
        return resolver.forUser(currentRole.get());
    }

    private SupportsNamespaces nsDelegate() {
        Catalog c = delegate();
        if (!(c instanceof SupportsNamespaces)) {
            throw new UnsupportedOperationException("Catalog does not support namespaces");
        }
        return (SupportsNamespaces) c;
    }

    @Override
    public String name() {
        return "pg";
    }

    // ------------------------------------------------------------------ Catalog

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return delegate().listTables(namespace);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        return delegate().loadTable(identifier);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier) {
        return delegate().tableExists(identifier);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                             String location, Map<String, String> properties) {
        return delegate().createTable(identifier, schema, spec, location, properties);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return delegate().dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        delegate().renameTable(from, to);
    }

    // ------------------------------------------------------------------ SupportsNamespaces

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        nsDelegate().createNamespace(namespace, metadata);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
        return nsDelegate().listNamespaces(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        return nsDelegate().loadNamespaceMetadata(namespace);
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
        return nsDelegate().namespaceExists(namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) {
        return nsDelegate().dropNamespace(namespace);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties) {
        return nsDelegate().setProperties(namespace, properties);
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties) {
        return nsDelegate().removeProperties(namespace, properties);
    }
}
