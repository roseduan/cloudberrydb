package cloud.elastic.dlagent.plugins.iceberg.utilities;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IcebergUtilities#isInternalConfigKey} and
 * {@link IcebergUtilities#stripInternalProperties}, which enforce the
 * open-source Iceberg convention that runtime catalog/volume/gopher
 * configuration must never be persisted into TableMetadata.properties.
 */
public class IcebergUtilitiesTest {

    @Test
    public void isInternalConfigKey_returnsTrue_forKnownInternalPrefixes() {
        assertTrue(IcebergUtilities.isInternalConfigKey("IcebergCatalogConfig.server_type"));
        assertTrue(IcebergUtilities.isInternalConfigKey("IcebergVolumeConfig.secret_access_key"));
        assertTrue(IcebergUtilities.isInternalConfigKey("IcebergVolumeConfig.access_key_id"));
        assertTrue(IcebergUtilities.isInternalConfigKey("IcebergAdditionalConfig.splitSize"));
        assertTrue(IcebergUtilities.isInternalConfigKey("FileIOConfig.impl_class"));
        assertTrue(IcebergUtilities.isInternalConfigKey("gopherFileIOConfig.worker_path"));
        assertTrue(IcebergUtilities.isInternalConfigKey("gopher.connect_path"));
        assertTrue(IcebergUtilities.isInternalConfigKey("buildInCatalog.table_exists"));
        assertTrue(IcebergUtilities.isInternalConfigKey("buildInCatalog.metadata_location"));
    }

    @Test
    public void isInternalConfigKey_returnsTrue_forExactInternalKeys() {
        assertTrue(IcebergUtilities.isInternalConfigKey("config_files"));
    }

    @Test
    public void isInternalConfigKey_returnsFalse_forIcebergTableProperties() {
        assertFalse(IcebergUtilities.isInternalConfigKey("format-version"));
        assertFalse(IcebergUtilities.isInternalConfigKey("write.format.default"));
        assertFalse(IcebergUtilities.isInternalConfigKey("write.parquet.compression-codec"));
        assertFalse(IcebergUtilities.isInternalConfigKey("write.update.mode"));
        assertFalse(IcebergUtilities.isInternalConfigKey("history.expire.max-snapshot-age-ms"));
        assertFalse(IcebergUtilities.isInternalConfigKey("commit.retry.num-retries"));
        assertFalse(IcebergUtilities.isInternalConfigKey("gc.enabled"));
    }

    @Test
    public void isInternalConfigKey_returnsFalse_forArbitraryUserKeys() {
        // OSS Iceberg allows users to set any table property; we only block the
        // specific prefixes used by our internal runtime-config protocol.
        assertFalse(IcebergUtilities.isInternalConfigKey("my.company.team"));
        assertFalse(IcebergUtilities.isInternalConfigKey("custom_property"));
        assertFalse(IcebergUtilities.isInternalConfigKey("owner"));
    }

    @Test
    public void isInternalConfigKey_returnsFalse_forEmptyOrNull() {
        assertFalse(IcebergUtilities.isInternalConfigKey(null));
        assertFalse(IcebergUtilities.isInternalConfigKey(""));
    }

    @Test
    public void isInternalConfigKey_returnsFalse_forKeysThatDoNotStartWithPrefix() {
        // "gopher" without a trailing dot is NOT a prefix match — avoid false
        // positives on user keys that happen to contain the substring.
        assertFalse(IcebergUtilities.isInternalConfigKey("gopher"));
        assertFalse(IcebergUtilities.isInternalConfigKey("IcebergCatalogConfig"));
        assertFalse(IcebergUtilities.isInternalConfigKey("write.gopher.something"));
    }

    @Test
    public void stripInternalProperties_returnsEmptyMap_forNullOrEmptyInput() {
        assertTrue(IcebergUtilities.stripInternalProperties(null).isEmpty());
        assertTrue(IcebergUtilities.stripInternalProperties(new HashMap<>()).isEmpty());
    }

    @Test
    public void stripInternalProperties_keepsUserKeys_dropsInternalKeys() {
        Map<String, String> input = new HashMap<>();
        input.put("format-version", "2");
        input.put("write.format.default", "parquet");
        input.put("my.company.team", "data-platform");
        input.put("IcebergVolumeConfig.secret_access_key", "secretpw");
        input.put("IcebergVolumeConfig.access_key_id", "admin");
        input.put("gopher.connect_path", "/tmp/.s.gopher.7000");
        input.put("buildInCatalog.table_exists", "false");
        input.put("config_files", "s3.conf");

        Map<String, String> out = IcebergUtilities.stripInternalProperties(input);

        assertEquals(3, out.size());
        assertEquals("2", out.get("format-version"));
        assertEquals("parquet", out.get("write.format.default"));
        assertEquals("data-platform", out.get("my.company.team"));
        assertFalse(out.containsKey("IcebergVolumeConfig.secret_access_key"));
        assertFalse(out.containsKey("IcebergVolumeConfig.access_key_id"));
        assertFalse(out.containsKey("gopher.connect_path"));
        assertFalse(out.containsKey("buildInCatalog.table_exists"));
        assertFalse(out.containsKey("config_files"));
    }

    @Test
    public void stripInternalProperties_doesNotMutateInput() {
        Map<String, String> input = new HashMap<>();
        input.put("write.format.default", "parquet");
        input.put("gopher.connect_path", "/tmp/foo");
        int originalSize = input.size();

        IcebergUtilities.stripInternalProperties(input);

        assertEquals(originalSize, input.size());
        assertTrue(input.containsKey("gopher.connect_path"));
    }
}
