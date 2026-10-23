/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.api.configuration.DlEnvironment;
import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.configuration.IcebergRestProperties;
import cloud.elastic.dlagent.api.configuration.PropertiesDlEnvironment;
import cloud.elastic.dlagent.service.ServiceApplication;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.catalina.connector.ClientAbortException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

import java.util.Properties;

/**
 * Boots the dlagent-core beans in a plain Spring container, the way
 * {@code ServiceApplication}'s component scan does, but without the servlet
 * layer.
 * <p>
 * Not {@code @SpringBootTest}: the full application context would also start
 * {@code DlProcessMonitor}, whose {@code @Scheduled} watchdog calls
 * {@code System.exit(0)} within a second when {@code /proc/<parent.pid>} does
 * not look like a datalake proxy - it would take the surefire JVM with it.
 * <p>
 * {@code service.rest} and {@code service.spring} are filtered out for the
 * same reason: the controllers pull in the MVC infrastructure.
 * {@code ServiceApplication} has to be filtered out as well, and not because
 * of what it contains: it is {@code @SpringBootApplication}, so scanning it
 * would register a nested {@code @ComponentScan} with no filters at all plus
 * {@code @EnableAutoConfiguration}, and the exclusions above would quietly
 * stop applying.
 * <p>
 * What is left is exactly the set of core beans a transport has to be able
 * to assemble, plus the things a composition root is required to supply
 * ({@link DlEnvironment}, {@link MeterRegistry}, the two configuration
 * objects and {@link ClientDisconnectPredicate}), plus
 * {@link IcebergPluginConfig}, which owns the prototype scope.
 */
@Configuration
@ComponentScan(
        basePackages = {"cloud.elastic.dlagent", "org.apache.hadoop.security"},
        excludeFilters = {
                @ComponentScan.Filter(
                        type = FilterType.REGEX,
                        pattern = "cloud\\.elastic\\.dlagent\\.service\\.(rest|spring)\\..*"),
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        classes = ServiceApplication.class)})
@Import(IcebergPluginConfig.class)
public class CoreBeansTestConfiguration {

    /**
     * @return the property source, empty so every core default applies
     */
    @Bean
    public DlEnvironment dlEnvironment() {
        return new PropertiesDlEnvironment(new Properties());
    }

    /**
     * @return dlagent.* with compiled-in defaults
     */
    @Bean
    public DlServerProperties dlServerProperties() {
        return new DlServerProperties();
    }

    /**
     * @return gopher.* with compiled-in defaults
     */
    /**
     * Defaults only, as with the gopher properties below: the real binding is
     * DlConfiguration's {@code @ConfigurationProperties} bean, which this
     * test context deliberately does not import.
     */
    @Bean
    public IcebergRestProperties icebergRestProperties() {
        return new IcebergRestProperties();
    }

    @Bean
    public GopherConfigurationProperties gopherConfigurationProperties() {
        return new GopherConfigurationProperties();
    }

    /**
     * @return a registry standing in for actuator's auto-configured one
     */
    @Bean
    public MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }

    /**
     * The production REST binding, repeated here rather than component-scanned
     * because {@code service.spring} is excluded above.
     *
     * @return the Tomcat-based disconnect test
     */
    @Bean
    public ClientDisconnectPredicate clientDisconnectPredicate() {
        return t -> t instanceof ClientAbortException;
    }
}
