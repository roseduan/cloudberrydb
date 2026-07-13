package org.apache.cloudberry.iceberg.gateway.catalog;

import org.apache.iceberg.catalog.Catalog;

/**
 * Resolves the {@link Catalog} to use for a given authenticated PG role. In production this returns
 * a {@link PgIcebergCatalog} bound to that role (so PG's GRANT/RLS applies); in tests it can return
 * a shared in-memory catalog. Lets the single, shared REST adapter serve every user correctly.
 */
@FunctionalInterface
public interface CatalogResolver {
    Catalog forUser(String pgRole);
}
