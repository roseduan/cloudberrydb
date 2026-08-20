package org.apache.cloudberry.iceberg.gateway.catalog;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Reads a catalog backend and answers the read-only Iceberg catalog queries. Each method runs on a
 * {@link Connection} already opened by {@code RoleScopedConnection} inside a REPEATABLE READ
 * transaction. Since Task 3 the connection is the shared {@code iceberg_authenticator} identity
 * (no SET ROLE); {@code pgRole} is the JWT-authenticated end-user role, passed through to the
 * SECURITY DEFINER {@code pg_ext_aux.iceberg_visible_tables(p_role)} accessor so RBAC filtering
 * happens inside the database, keyed off the trusted caller-supplied role rather than
 * {@code current_user} (which would be the function owner). Implemented by
 * {@code PgLakeCatalogStore}, which reads the real pg_lake_table catalog. Since Task 4,
 * {@code loadTableMetadata} reads the {@code metadata.json} document through
 * {@code pg_ext_aux.iceberg_load_metadata} instead of a still-stubbed system-table reader that
 * fetched only a pointer and left this process to read object storage itself.
 */
public interface CatalogStore {
    List<Namespace> listNamespaces(Connection c, String pgRole);
    boolean namespaceExists(Connection c, String pgRole, Namespace ns);
    Map<String, String> loadNamespaceMetadata(Connection c, String pgRole, Namespace ns);
    List<TableIdentifier> listTables(Connection c, String pgRole, Namespace ns);
    boolean canSelect(Connection c, String pgRole, TableIdentifier id);
    TableMetadata loadTableMetadata(Connection c, String pgRole, TableIdentifier id);
}
