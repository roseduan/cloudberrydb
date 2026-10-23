package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.api.configuration.IcebergRestProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * MVC configuration for Iceberg REST API
 */
@Configuration
@EnableWebMvc
@ConditionalOnProperty(name = "iceberg.rest.enabled", havingValue = "true")
@Slf4j
public class IcebergRestMvcConfig implements WebMvcConfigurer {

    @Autowired
    private IcebergRestProperties icebergRestConfig;

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        log.info("Configuring Iceberg REST API at base path: {}", icebergRestConfig.getBasePath());
    }
}
