package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Spring configuration for Iceberg plugin beans.
 *
 * IcebergMetadataFetcher extends BasePlugin which holds a mutable
 * RequestContext as instance state, set via setRequestContext() before
 * each operation. Tomcat dispatches concurrent HTTP requests to multiple
 * threads simultaneously; if the fetcher were a singleton, two threads
 * would race on setRequestContext() / fragment access and one writer's
 * fragments would overwrite the other's, manifesting as one writer's
 * data appearing twice in the Iceberg manifest while the other writer's
 * data is silently dropped (concurrent INSERT data loss).
 *
 * Use prototype scope so each Spring injection (in IcebergServiceImpl)
 * yields a fresh fetcher instance and the per-request context cannot
 * cross-contaminate.
 */
@Configuration
public class IcebergPluginConfig {

    @Autowired
    private IcebergUtilities icebergUtilities;

    @Autowired
    private IcebergCatalogWrapper icebergCatalogWrapper;

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public IcebergMetadataFetcher icebergMetadataFetcher() {
        return new IcebergMetadataFetcher(icebergUtilities, icebergCatalogWrapper);
    }
}
