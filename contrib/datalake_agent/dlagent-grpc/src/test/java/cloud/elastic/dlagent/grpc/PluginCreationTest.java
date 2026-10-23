/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.utilities.BeanLocator;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.plugins.hive.HiveMetadataFetcher;
import cloud.elastic.dlagent.plugins.hudi.HudiMetadataFetcher;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The data plane creates its metadata fetcher by class name:
 * {@code BasePluginFactory.getPlugin} calls the no-arg constructor
 * reflectively, and those constructors resolve their collaborators through
 * {@link BeanLocator}. Under REST the Spring context installs itself there;
 * the gRPC agent has to install Guice in the composition {@link Main} really
 * uses, or every Hive and Hudi read through the gRPC agent fails.
 */
class PluginCreationTest {

    @AfterEach
    void clearLocator() {
        BeanLocator.clearResolver();
    }

    @Test
    void theAgentsCompositionCanCreateEveryMetadataFetcher() throws Exception {
        DlConfig config = DlConfig.load(
                new ByteArrayInputStream("server.address=localhost\n".getBytes(StandardCharsets.UTF_8)),
                Collections.emptyMap(), new String[0]);
        Main.compose(config);

        for (Class<?> fetcher : new Class<?>[] {IcebergMetadataFetcher.class,
                HiveMetadataFetcher.class, HudiMetadataFetcher.class}) {
            // Exactly what BasePluginFactory.getPlugin does.
            assertNotNull(fetcher.getConstructor().newInstance(), fetcher.getSimpleName());
        }
    }
}
