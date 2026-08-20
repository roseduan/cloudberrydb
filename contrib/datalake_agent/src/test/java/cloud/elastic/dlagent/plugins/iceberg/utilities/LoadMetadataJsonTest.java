package cloud.elastic.dlagent.plugins.iceberg.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The endpoint must return the metadata.json bytes VERBATIM. Asserting on the exact byte
 * array (not on a parsed field) is the point: a parse -> re-serialize implementation would
 * still produce valid-looking JSON but would silently drop fields iceberg 1.3.0 does not
 * model, and the gateway (1.6.1) would then serve a lossy document.
 */
class LoadMetadataJsonTest {

    /** A metadata doc containing a field iceberg 1.3.0's TableMetadata does not model. */
    private static final String DOC =
            "{\"format-version\":2,\"table-uuid\":\"c8f0e0a2-0000-4000-8000-000000000001\","
            + "\"location\":\"file:/warehouse/db/t\",\"last-sequence-number\":1,"
            + "\"unknown-future-field\":{\"kept\":true},\"schemas\":[],\"snapshots\":[]}";

    @Test
    void returnsBytesVerbatim(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("00001-abc.metadata.json");
        Files.write(f, DOC.getBytes(StandardCharsets.UTF_8));

        HadoopFileIO io = new HadoopFileIO(new Configuration());
        byte[] out = MetadataJsonReader.readMetadataBytes(io, f.toUri().toString());

        assertEquals(DOC, new String(out, StandardCharsets.UTF_8));
        assertTrue(new String(out, StandardCharsets.UTF_8).contains("unknown-future-field"),
                "verbatim passthrough must preserve fields the 1.3.0 model drops");
    }

    @Test
    void rejectsBlankLocation() {
        HadoopFileIO io = new HadoopFileIO(new Configuration());
        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> MetadataJsonReader.readMetadataBytes(io, "  "));
        assertTrue(e.getMessage().contains("metadataLocation"));
    }

    /**
     * Endpoint-level guard: a table whose resolved metadata pointer is blank must surface as
     * NoSuchTableException (-> 404 + ErrorModel via IcebergExceptionHandler), not the
     * IllegalArgumentException (-> 400) that readMetadataBytes' own null-argument guard throws.
     * Table-not-found and pointer-empty are spec'd to look the same to the caller.
     */
    @Test
    void requireMetadataLocationPresentThrowsNoSuchTableExceptionWhenBlank() {
        assertThrows(NoSuchTableException.class,
                () -> MetadataJsonReader.requireMetadataLocationPresent("  ", "ns", "t"));
        assertThrows(NoSuchTableException.class,
                () -> MetadataJsonReader.requireMetadataLocationPresent(null, "ns", "t"));
    }

    @Test
    void requireMetadataLocationPresentAllowsNonBlank() {
        MetadataJsonReader.requireMetadataLocationPresent("file:/warehouse/db/t/m.json", "ns", "t");
    }
}
