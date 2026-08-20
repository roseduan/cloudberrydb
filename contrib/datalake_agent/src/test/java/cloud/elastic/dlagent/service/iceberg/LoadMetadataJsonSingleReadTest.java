package cloud.elastic.dlagent.service.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cloud.elastic.dlagent.api.model.RequestContext;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalog;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * loadMetadataJson must perform EXACTLY ONE object storage read.
 *
 * <p>The obvious implementation -- load the table, then read its metadata pointer -- fetches the
 * same object twice, because a table load has to GET metadata.json in order to build
 * TableMetadata at all (DlIcebergBuildInTableOperations.doRefresh). That duplication is invisible
 * in the response, so it can only be pinned down by counting reads. Hence a counting FileIO and
 * a catalog whose loadTable must never be called.
 */
@ExtendWith(MockitoExtension.class)
class LoadMetadataJsonSingleReadTest {

    private static final String DOC =
            "{\"format-version\":2,\"table-uuid\":\"c8f0e0a2-0000-4000-8000-000000000001\","
            + "\"location\":\"file:/warehouse/db/t\",\"unknown-future-field\":{\"kept\":true}}";

    private static final String POINTER_PROP = "buildInCatalog.metadata_location";

    /** Delegating FileIO that records how many objects were opened. */
    private static final class CountingFileIO implements FileIO {
        private final FileIO delegate;
        private int reads;

        CountingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        int reads() {
            return reads;
        }

        @Override
        public InputFile newInputFile(String path) {
            reads++;
            return delegate.newInputFile(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException("read-only: " + path);
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException("read-only: " + path);
        }
    }

    @Mock
    private IcebergCatalogWrapper wrapper;

    @InjectMocks
    private IcebergServiceImpl service;

    @Test
    void readsTheDocumentOnceAndNeverLoadsTheTable(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("00001-abc.metadata.json");
        Files.write(f, DOC.getBytes(StandardCharsets.UTF_8));

        CountingFileIO io = new CountingFileIO(new HadoopFileIO(new Configuration()));
        IcebergCatalog catalog = mock(IcebergCatalog.class);
        when(catalog.io()).thenReturn(io);
        when(wrapper.getIcebergCatalog(any())).thenReturn(catalog);

        byte[] out = service.loadMetadataJson(
                "db", "t",
                Collections.singletonMap(POINTER_PROP, f.toUri().toString()),
                mock(RequestContext.class));

        assertEquals(DOC, new String(out, StandardCharsets.UTF_8));
        assertEquals(1, io.reads(), "metadata.json must be fetched exactly once");
        verify(catalog, never()).loadTable(any(TableIdentifier.class), any(), any());
    }
}
