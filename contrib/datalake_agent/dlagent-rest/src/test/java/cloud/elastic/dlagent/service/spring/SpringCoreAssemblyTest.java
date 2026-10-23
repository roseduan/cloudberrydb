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

import cloud.elastic.dlagent.api.model.ConfigurationFactory;
import cloud.elastic.dlagent.api.model.PluginConf;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.api.utilities.CharsetUtils;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import cloud.elastic.dlagent.plugins.iceberg.IcebergPolarisCatalogManager;
import cloud.elastic.dlagent.service.HttpHeaderDecoder;
import cloud.elastic.dlagent.service.MetricsReporter;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.bridge.BridgeFactory;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.profile.ProfilesConf;
import cloud.elastic.dlagent.service.security.SecurityService;
import cloud.elastic.dlagent.service.utilities.BasePluginFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.ResolvableType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JSR-330 core assembles under Spring.
 * <p>
 * The other half, the same classes under Guice, is
 * {@code cloud.elastic.dlagent.grpc.GuiceCoreAssemblyTest}. Together they are
 * what makes "one copy of the business logic, two transports" a checked
 * property rather than an intention.
 * <p>
 * Swapping {@code @Component} for {@code @Named} and {@code @Autowired} for
 * {@code @Inject} is only equivalent as long as Spring keeps treating the
 * JSR-330 annotations as its own; this test is what says it does.
 */
class SpringCoreAssemblyTest {

    private static AnnotationConfigApplicationContext context;

    @BeforeAll
    static void startContainer() {
        context = new AnnotationConfigApplicationContext(CoreBeansTestConfiguration.class);
    }

    @AfterAll
    static void stopContainer() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void theServiceLayerResolves() {
        assertNotNull(context.getBean(ReadService.class));
        assertNotNull(context.getBean(IcebergService.class));
        assertNotNull(context.getBean(SecurityService.class));
        assertNotNull(context.getBean(BridgeFactory.class));
        assertNotNull(context.getBean(ConfigurationFactory.class));
        assertNotNull(context.getBean(MetricsReporter.class));
    }

    @Test
    void thePluginLayerResolves() {
        assertNotNull(context.getBean(BasePluginFactory.class));
        assertNotNull(context.getBean(IcebergCatalogWrapper.class));
        assertNotNull(context.getBean(SecureLogin.class));
        assertNotNull(context.getBean(CharsetUtils.class));
        assertNotNull(context.getBean(HttpHeaderDecoder.class));
    }

    /**
     * {@code ProfilesConf} keeps a private no-arg constructor and now carries
     * {@code @Inject} on it for Guice's benefit; Spring must still be able to
     * build it, and it must actually have parsed dlagent-profiles.xml out of
     * the core jar.
     */
    @Test
    void theProfileConfigurationLoads() {
        PluginConf pluginConf = context.getBean(PluginConf.class);

        assertTrue(pluginConf instanceof ProfilesConf);
        Map<String, String> icebergPlugins = pluginConf.getPlugins("iceberg");
        assertNotNull(icebergPlugins, "dlagent-profiles.xml must ship with dlagent-core");
        assertTrue(icebergPlugins.containsKey("METADATA"),
                "iceberg profile should name its metadata fetcher, got " + icebergPlugins);
    }

    /**
     * {@code HttpRequestParser} now implements
     * {@code RequestParser<Map<String, List<String>>>} instead of
     * {@code RequestParser<MultiValueMap<String, String>>}; the generic
     * lookup has to keep working because {@code DlBaseResource} injects it
     * that way.
     */
    @Test
    void theRequestParserResolvesByItsGenericType() {
        ResolvableType type = ResolvableType.forClassWithGenerics(
                RequestParser.class,
                ResolvableType.forClassWithGenerics(Map.class,
                        ResolvableType.forClass(String.class),
                        ResolvableType.forClassWithGenerics(List.class, String.class)));

        String[] names = context.getBeanNamesForType(type);
        assertEquals(1, names.length, "expected exactly one request parser, got " + names.length);
    }

    /**
     * The {@code @Named("polarisIcebergCatalogManager")} qualifier replaced a
     * {@code @Qualifier} of the same name; the bean name must be unchanged.
     */
    @Test
    void theQualifiedPolarisManagerKeepsItsBeanName() {
        Object byName = context.getBean("polarisIcebergCatalogManager");
        assertTrue(byName instanceof IcebergPolarisCatalogManager);
        assertSame(byName, context.getBean(IcebergPolarisCatalogManager.class));
    }

    /**
     * IcebergServiceImpl injects {@code javax.inject.Provider<IcebergMetadataFetcher>}
     * where it used to inject Spring's {@code ObjectProvider}. If Spring could
     * not satisfy that, the context above would already have failed - this
     * pins down that the provider hands out a usable fetcher.
     */
    @Test
    void theFetcherProviderProduces() {
        assertNotNull(context.getBean(IcebergMetadataFetcher.class));
    }
}
