package org.apache.cloudberry.iceberg.gateway.catalog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/**
 * CatalogStore backed by the real builtin iceberg catalog: pg_lake_table + pg_ext_aux.pg_iceberg_metadata.
 *
 * <p>Since Task 3 (issue #382 item B) every query here goes through the SECURITY DEFINER
 * {@code pg_ext_aux.iceberg_visible_tables(p_role)} accessor instead of joining
 * {@code pg_ext_aux.pg_iceberg_metadata} directly with {@code current_user}. Two reasons: (1) the
 * base table's PUBLIC grant was removed, so a direct join as an arbitrary role would fail with
 * "permission denied for schema pg_ext_aux"; (2) the caller no longer SET ROLEs into the
 * end-user's identity (the connection stays as {@code iceberg_authenticator}), so
 * {@code current_user} is no longer meaningful for RBAC filtering. {@code pgRole} — the
 * JWT-authenticated end-user role — is bound as the function's {@code p_role} argument instead,
 * and the function does the {@code has_schema_privilege}/{@code has_table_privilege} filtering
 * server-side under its owner's elevated read access to {@code pg_ext_aux}.
 */
public final class PgLakeCatalogStore implements CatalogStore {

    private final FileIO fileIO;   // used by loadTableMetadata (Task 4); null OK for browse-only.

    public PgLakeCatalogStore(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    @Override
    public List<Namespace> listNamespaces(Connection c, String pgRole) {
        String sql = "SELECT DISTINCT nspname FROM pg_ext_aux.iceberg_visible_tables(?) ORDER BY 1";
        java.util.List<Namespace> out = new java.util.ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(Namespace.of(rs.getString(1)));
            }
        } catch (SQLException e) { throw new RuntimeException("listNamespaces failed", e); }
        return out;
    }

    @Override
    public boolean namespaceExists(Connection c, String pgRole, Namespace ns) {
        String sql = "SELECT 1 FROM pg_ext_aux.iceberg_visible_tables(?) WHERE nspname = ? LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(ns));
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { throw new RuntimeException("namespaceExists failed", e); }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Connection c, String pgRole, Namespace ns) {
        if (!namespaceExists(c, pgRole, ns)) return null;   // caller -> NoSuchNamespaceException
        // NOTE: must be null-tolerant. iceberg-core's GetNamespaceResponse.Builder#setProperties
        // internally does properties.containsKey(null) as validation; Map.of() (a JDK immutable
        // map) throws NPE on a null-key lookup, whereas HashMap.containsKey(null) safely returns
        // false. Using Map.of() here caused a 500 NPE on every loadNamespace call.
        return new java.util.HashMap<>();           // single-level ns: no schema-level props
    }

    @Override
    public List<TableIdentifier> listTables(Connection c, String pgRole, Namespace ns) {
        String sql = "SELECT relname FROM pg_ext_aux.iceberg_visible_tables(?) "
                   + "WHERE nspname = ? ORDER BY 1";
        java.util.List<TableIdentifier> out = new java.util.ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(ns));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(TableIdentifier.of(ns, rs.getString(1)));
            }
        } catch (SQLException e) { throw new RuntimeException("listTables failed", e); }
        return out;
    }

    @Override
    public boolean canSelect(Connection c, String pgRole, TableIdentifier id) {
        String sql = "SELECT 1 FROM pg_ext_aux.iceberg_visible_tables(?) "
                   + "WHERE nspname = ? AND relname = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(id.namespace()));
            ps.setString(3, id.name());
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { throw new RuntimeException("canSelect failed", e); }
    }

    @Override
    public TableMetadata loadTableMetadata(Connection c, String pgRole, TableIdentifier id) {
        String loc = queryMetadataLocation(c, pgRole, id);
        if (loc == null) throw new NoSuchTableException("Table does not exist: %s", id);
        String s3 = loc.replaceFirst("^s3a://", "s3://");
        return TableMetadataParser.read(fileIO, s3);
    }

    private String queryMetadataLocation(Connection c, String pgRole, TableIdentifier id) {
        // Sourced from the same authz-gated function as canSelect: a row is only returned when
        // p_role has both USAGE on the namespace and SELECT on the table, so a non-null result
        // here already implies canSelect() would be true.
        String sql = "SELECT metadata_location FROM pg_ext_aux.iceberg_visible_tables(?) "
                   + "WHERE nspname = ? AND relname = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(id.namespace()));
            ps.setString(3, id.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) { throw new RuntimeException("queryMetadataLocation failed", e); }
    }

    private static String single(Namespace ns) {
        if (ns.length() != 1) throw new IllegalArgumentException("Only single-level namespaces: " + ns);
        return ns.level(0);
    }
}
