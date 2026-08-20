package org.apache.cloudberry.iceberg.gateway.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
 * loadTableMetadata must build TableMetadata from the document the database returns, and must
 * NOT touch object storage. There is deliberately no FileIO in sight here -- that absence is
 * the behaviour under test.
 */
class PgLakeCatalogStoreMetadataTest {

    // The document + location fixture lives in TestMetadataDoc so this test and the
    // no-object-storage-IO proof cannot drift apart.
    private static final String DOC = TestMetadataDoc.DOC;

    private static final String LOC = TestMetadataDoc.LOC;

    @Test
    void buildsMetadataFromDatabaseDocument() throws Exception {
        Connection c = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(c.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn(LOC);
        when(rs.getString(2)).thenReturn(DOC);

        TableMetadata md = new PgLakeCatalogStore()
                .loadTableMetadata(c, "alice", TableIdentifier.of(Namespace.of("db"), "t"));

        assertThat(md.metadataFileLocation()).isEqualTo(LOC);
        assertThat(md.location()).isEqualTo("s3://warehouse/db/t");
        assertThat(md.schema().findField("id")).isNotNull();
        verify(ps).setString(1, "alice");
        verify(ps).setString(2, "db");
        verify(ps).setString(3, "t");
    }

    @Test
    void zeroRowsMeansNoSuchTable() throws Exception {
        Connection c = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(c.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        assertThatThrownBy(() -> new PgLakeCatalogStore()
                .loadTableMetadata(c, "alice", TableIdentifier.of(Namespace.of("db"), "t")))
                .isInstanceOf(NoSuchTableException.class);
    }
}
