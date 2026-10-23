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

package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.api.configuration.DlEnvironment;
import cloud.elastic.dlagent.api.model.ConfigurationFactory;
import cloud.elastic.dlagent.api.model.PluginConf;
import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.api.utilities.BeanLocator;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergPolarisCatalogManager;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.MetricsReporter;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.bridge.BridgeFactory;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.profile.ProfilesConf;
import cloud.elastic.dlagent.service.security.SecurityService;
import cloud.elastic.dlagent.service.utilities.BasePluginFactory;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The same JSR-330 core assembles under Guice.
 * <p>
 * The Spring half is
 * {@code cloud.elastic.dlagent.service.spring.SpringCoreAssemblyTest}. If
 * both pass, {@code @Named} / {@code @Inject} / {@code @Singleton} really do
 * carry the whole wiring and neither transport needs its own copy of the
 * business logic.
 */
class GuiceCoreAssemblyTest {

    private static Injector injector() {
        return Guice.createInjector(new DiModule());
    }

    @AfterEach
    void clearLocator() {
        BeanLocator.clearResolver();
    }

    @Test
    void theServiceLayerResolves() {
        Injector injector = injector();

        assertNotNull(injector.getInstance(ReadService.class));
        assertNotNull(injector.getInstance(IcebergService.class));
        assertNotNull(injector.getInstance(SecurityService.class));
        assertNotNull(injector.getInstance(BridgeFactory.class));
        assertNotNull(injector.getInstance(ConfigurationFactory.class));
        assertNotNull(injector.getInstance(MetricsReporter.class));
        assertNotNull(injector.getInstance(BasePluginFactory.class));
        assertNotNull(injector.getInstance(SecureLogin.class));
    }

    @Test
    void theCompositionRootSuppliesWhatCoreCannotProvideItself() {
        Injector injector = injector();

        assertNotNull(injector.getInstance(DlEnvironment.class));
        assertNotNull(injector.getInstance(MeterRegistry.class));
    }

    /**
     * {@code ProfilesConf}'s no-arg constructor is private, which Guice will
     * not use unless it is annotated {@code @Inject} - and the profiles file
     * has to be reachable from the core jar.
     */
    @Test
    void theProfileConfigurationLoads() {
        PluginConf pluginConf = injector().getInstance(PluginConf.class);

        assertTrue(pluginConf instanceof ProfilesConf);
        Map<String, String> icebergPlugins = pluginConf.getPlugins("iceberg");
        assertNotNull(icebergPlugins, "dlagent-profiles.xml must ship with dlagent-core");
        assertTrue(icebergPlugins.containsKey("METADATA"),
                "iceberg profile should name its metadata fetcher, got " + icebergPlugins);
    }

    @Test
    void theRequestParserResolvesByItsGenericType() {
        RequestParser<Map<String, List<String>>> parser = injector().getInstance(
                Key.get(new TypeLiteral<RequestParser<Map<String, List<String>>>>() { }));

        assertNotNull(parser);
    }

    @Test
    void theQualifiedPolarisManagerResolves() {
        Injector injector = injector();

        assertNotNull(injector.getInstance(
                Key.get(IcebergPolarisCatalogManager.class, Names.named("polarisIcebergCatalogManager"))));
    }

    @Test
    void singletonsAreShared() {
        Injector injector = injector();

        assertSame(injector.getInstance(IcebergCatalogWrapper.class),
                injector.getInstance(IcebergCatalogWrapper.class));
        assertSame(injector.getInstance(IcebergUtilities.class),
                injector.getInstance(IcebergUtilities.class));
        assertSame(injector.getInstance(ReadService.class),
                injector.getInstance(ReadService.class));
    }

    /**
     * What matters about {@link ClientDisconnectPredicate} is WHICH
     * implementation is bound: the predicate decides whether a client
     * going away is logged as a routine disconnect or as a server fault with a
     * stack trace on every cancelled query, which is the regression the old
     * branch introduced on the REST side.
     */
    @Test
    void theGrpcDisconnectPredicateIsBound() {
        ClientDisconnectPredicate predicate =
                injector().getInstance(ClientDisconnectPredicate.class);
        assertTrue(predicate instanceof GrpcClientDisconnectPredicate,
                "expected the gRPC implementation, got " + predicate.getClass().getName());

        // And that it judges by status code.
        assertTrue(predicate.isClientDisconnect(
                io.grpc.Status.CANCELLED.asRuntimeException()));
        assertTrue(predicate.isClientDisconnect(
                io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException()));
        assertTrue(!predicate.isClientDisconnect(
                io.grpc.Status.INTERNAL.asRuntimeException()),
                "a real server fault must not be mistaken for a disconnect");
        assertTrue(!predicate.isClientDisconnect(new java.io.IOException("disk")),
                "an IOException is not how gRPC reports a disconnect");
    }

    /**
     * The three {@code *MetadataFetcher} classes are created reflectively by
     * BasePluginFactory and reach back through BeanLocator; the gRPC root has
     * to install Guice behind it, exactly as the Spring root installs the
     * application context.
     */
    @Test
    void beanLocatorResolvesThroughGuiceOnceInstalled() {
        Injector injector = injector();
        DiModule.installBeanLocator(injector);

        assertSame(injector.getInstance(IcebergUtilities.class),
                BeanLocator.getBean(IcebergUtilities.class));
    }
}
