package org.apache.cloudberry.iceberg.gateway.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Test;

/**
 * loadTableMetadata must build TableMetadata from the document the database returns, and must NOT
 * touch object storage. There is deliberately no FileIO in sight here -- that absence is the
 * behaviour under test.
 *
 * <p>It must also do so in two steps: a per-role pointer lookup that always runs (it is the
 * authorization gate), and a document fetch that runs only on a cache miss. The cache tests below
 * assert both halves of that, because "we added a cache" and "the cache is actually consulted"
 * are different claims, and so are "the cache is consulted" and "the cache can go stale".
 */
class PgLakeCatalogStoreMetadataTest {

    // The document + location fixture lives in TestMetadataDoc so this test and the
    // no-object-storage-IO proof cannot drift apart.
    private static final String DOC = TestMetadataDoc.DOC;

    private static final String LOC = TestMetadataDoc.LOC;

    private static final TableIdentifier ID = TableIdentifier.of(Namespace.of("db"), "t");

    /**
     * Wires the two statements separately, keyed on the accessor each one calls, so a test can
     * count them independently. A single shared mock would make "how many times was the document
     * fetched" unanswerable -- which is the whole question here.
     */
    private static final class Harness {
        final Connection c = mock(Connection.class);
        final PreparedStatement pointerPs = mock(PreparedStatement.class);
        final PreparedStatement docPs = mock(PreparedStatement.class);
        final ResultSet pointerRs = mock(ResultSet.class);
        final ResultSet docRs = mock(ResultSet.class);

        Harness() throws Exception {
            when(c.prepareStatement(contains("iceberg_visible_tables"))).thenReturn(pointerPs);
            when(c.prepareStatement(contains("iceberg_load_metadata"))).thenReturn(docPs);
            when(pointerPs.executeQuery()).thenReturn(pointerRs);
            when(docPs.executeQuery()).thenReturn(docRs);
            when(docRs.next()).thenReturn(true);
            when(docRs.getString(1)).thenReturn(LOC);
            when(docRs.getString(2)).thenReturn(DOC);
        }

        /** Pointer lookup resolves to {@code loc}; pass null for "role cannot see this table". */
        void pointsAt(String loc) throws Exception {
            when(pointerRs.next()).thenReturn(loc != null);
            if (loc != null) {
                when(pointerRs.getString(1)).thenReturn(loc);
            }
        }
    }

    @Test
    void buildsMetadataFromDatabaseDocument() throws Exception {
        Harness h = new Harness();
        h.pointsAt(LOC);

        TableMetadata md = new PgLakeCatalogStore().loadTableMetadata(h.c, "alice", ID);

        assertThat(md.metadataFileLocation()).isEqualTo(LOC);
        assertThat(md.location()).isEqualTo("s3://warehouse/db/t");
        assertThat(md.schema().findField("id")).isNotNull();
        // The caller-supplied role reaches BOTH accessors: the pointer lookup is the authz gate
        // and the document fetch re-checks it. Neither may silently run as the connection's role.
        verify(h.pointerPs).setString(1, "alice");
        verify(h.docPs).setString(1, "alice");
        verify(h.docPs).setString(2, "db");
        verify(h.docPs).setString(3, "t");
    }

    @Test
    void zeroRowsMeansNoSuchTable() throws Exception {
        Harness h = new Harness();
        h.pointsAt(null);

        assertThatThrownBy(() -> new PgLakeCatalogStore().loadTableMetadata(h.c, "alice", ID))
                .isInstanceOf(NoSuchTableException.class);

        // And it must reach that verdict without asking the coordinator to fetch anything: an
        // unauthorized or missing table costs zero object storage IO.
        verify(h.c, never()).prepareStatement(contains("iceberg_load_metadata"));
    }

    @Test
    void repeatLoadOfTheSameLocationDoesNotRefetchTheDocument() throws Exception {
        Harness h = new Harness();
        h.pointsAt(LOC);
        PgLakeCatalogStore store = new PgLakeCatalogStore();

        TableMetadata first = store.loadTableMetadata(h.c, "alice", ID);
        TableMetadata second = store.loadTableMetadata(h.c, "alice", ID);

        assertThat(second).isSameAs(first);
        // The pointer lookup still runs every time -- it is the authorization gate, and skipping
        // it would let a revoked role keep reading from cache.
        verify(h.pointerPs, times(2)).executeQuery();
        verify(h.docPs, times(1)).executeQuery();
    }

    @Test
    void aNewLocationRefetches() throws Exception {
        Harness h = new Harness();
        h.pointsAt(LOC);
        PgLakeCatalogStore store = new PgLakeCatalogStore();
        store.loadTableMetadata(h.c, "alice", ID);

        // A commit wrote a new metadata file and moved the pointer. Nothing invalidates the cache
        // explicitly; the new pointer simply misses. That is the entire invalidation story, and it
        // only works because Iceberg never rewrites a metadata file in place.
        String newLoc = "s3://warehouse/db/t/metadata/00002-def.metadata.json";
        h.pointsAt(newLoc);
        when(h.docRs.getString(1)).thenReturn(newLoc);

        TableMetadata after = store.loadTableMetadata(h.c, "alice", ID);

        assertThat(after.metadataFileLocation()).isEqualTo(newLoc);
        verify(h.docPs, times(2)).executeQuery();
    }

    @Test
    void oversizedDocumentsAreServedButNotRetained() throws Exception {
        Harness h = new Harness();
        h.pointsAt(LOC);
        // maxDocBytes below the fixture size: the document must still be returned, just not kept.
        PgLakeCatalogStore store = new PgLakeCatalogStore(128, DOC.length() - 1);

        assertThat(store.loadTableMetadata(h.c, "alice", ID).metadataFileLocation()).isEqualTo(LOC);
        store.loadTableMetadata(h.c, "alice", ID);

        verify(h.docPs, times(2)).executeQuery();
    }

    @Test
    void aStatementWeDidNotWireWouldFailLoudly() throws Exception {
        // Guards the harness itself: if loadTableMetadata starts issuing a third, unrecognised
        // statement, prepareStatement returns null for it and the test suite notices, rather than
        // the new query silently sharing another statement's mock.
        Connection c = mock(Connection.class);
        when(c.prepareStatement(anyString())).thenReturn(null);
        assertThatThrownBy(() -> new PgLakeCatalogStore().loadTableMetadata(c, "alice", ID))
                .isInstanceOf(RuntimeException.class);
    }
}
