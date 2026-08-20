package org.apache.cloudberry.iceberg.gateway.catalog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

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
 *
 * <p>Since Task 4 (issue #382 / #935, "kernel-side metadata load") this class does no object
 * storage IO of its own: {@link #loadTableMetadata} reads the {@code metadata.json} document
 * through {@code pg_ext_aux.iceberg_load_metadata}, which the database fetches using the
 * volume's own credentials. There is no {@code FileIO} field here any more -- that absence is
 * the point.
 *
 * <p>Documents are cached by metadata location. That is safe in a way that caching by table name
 * would not be: an Iceberg commit never rewrites a metadata file, it writes a new one and moves
 * the pointer, so a given location resolves to one immutable document forever. A commit
 * therefore invalidates the cache implicitly -- the next pointer read simply misses. The cache
 * is process-wide and NOT keyed by role, which is only sound because every request still
 * performs the per-role pointer lookup through {@code iceberg_visible_tables} before it can
 * reach the cache; authorization is never cached, only content that has already been authorized
 * for the asking role.
 */
public final class PgLakeCatalogStore implements CatalogStore {

    /** Entries retained. Deliberately modest: this is a latency cache, not a store. */
    private static final int DEFAULT_MAX_ENTRIES = 128;

    /**
     * Documents above this size are served but not retained. Metadata docs are normally a few
     * KB; the outliers are tables with runaway snapshot history, and those are exactly the ones
     * you do not want N copies of resident.
     */
    private static final int DEFAULT_MAX_DOC_BYTES = 1024 * 1024;

    private final int maxDocBytes;
    private final Map<String, TableMetadata> byLocation;

    public PgLakeCatalogStore() {
        this(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_DOC_BYTES);
    }

    public PgLakeCatalogStore(int maxEntries, int maxDocBytes) {
        this.maxDocBytes = maxDocBytes;
        this.byLocation = Collections.synchronizedMap(
                new LinkedHashMap<String, TableMetadata>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, TableMetadata> eldest) {
                        return size() > maxEntries;
                    }
                });
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
        // Two steps on purpose. Step one is a pure catalog read: it costs no object storage IO
        // and it is the authorization gate, so it must happen on every request. Step two -- the
        // one that makes the coordinator fetch a document through dlagent -- only runs when this
        // process has not already seen that exact location.
        String loc = queryVisibleMetadataLocation(c, pgRole, id);
        if (loc == null) {
            // Zero rows covers both "no such table" and "no SELECT privilege": the accessor is
            // anti-enumeration by contract, and so is this 404.
            throw new NoSuchTableException("Table does not exist: %s", id);
        }

        TableMetadata cached = byLocation.get(loc);
        if (cached != null) {
            return cached;
        }
        return fetchAndCache(c, pgRole, id, loc);
    }

    /**
     * The table's current metadata pointer, or null if {@code pgRole} cannot see the table.
     * Catalog read only -- same authz-gated accessor as {@link #canSelect}, so a non-null result
     * already implies canSelect() would be true.
     */
    private String queryVisibleMetadataLocation(Connection c, String pgRole, TableIdentifier id) {
        String sql = "SELECT metadata_location FROM pg_ext_aux.iceberg_visible_tables(?) "
                   + "WHERE nspname = ? AND relname = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(id.namespace()));
            ps.setString(3, id.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("queryVisibleMetadataLocation failed", e);
        }
    }

    /**
     * One call, one document. The database reads metadata.json with the volume's own
     * credentials, so this process needs no object storage access at all -- see the
     * "kernel-side metadata load" design note. The location comes back alongside the document
     * because TableMetadata carries it and clients compare it.
     *
     * <p>The document is cached under the location the accessor RETURNED, not under
     * {@code pointerLoc}. Those agree for the gateway (its sessions never hold uncommitted DML,
     * so the kernel's tracker resolves to the same committed pointer). If they ever diverged,
     * every lookup would simply miss -- degraded latency, never a stale document.
     */
    private TableMetadata fetchAndCache(Connection c, String pgRole, TableIdentifier id, String pointerLoc) {
        String sql = "SELECT metadata_location, metadata_json "
                   + "FROM pg_ext_aux.iceberg_load_metadata(?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, pgRole);
            ps.setString(2, single(id.namespace()));
            ps.setString(3, id.name());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    // Raced with a DROP between the pointer read and here, or -- the case worth
                    // naming -- the two accessors disagree about which tables are readable.
                    // iceberg_visible_tables filters to builtin-catalog tables precisely so this
                    // cannot happen; if it does, it is a 404, never a 500.
                    throw new NoSuchTableException("Table does not exist: %s", id);
                }
                String loc = rs.getString(1);
                String doc = rs.getString(2);
                TableMetadata md = TableMetadataParser.fromJson(loc, doc);
                if (doc.length() <= maxDocBytes) {
                    byLocation.put(loc, md);
                }
                return md;
            }
        } catch (SQLException e) {
            throw new RuntimeException("loadTableMetadata failed", e);
        }
    }

    private static String single(Namespace ns) {
        if (ns.length() != 1) throw new IllegalArgumentException("Only single-level namespaces: " + ns);
        return ns.level(0);
    }
}
