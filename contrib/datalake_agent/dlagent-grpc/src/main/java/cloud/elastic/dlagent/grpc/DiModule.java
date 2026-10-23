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
import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.configuration.IcebergRestProperties;
import cloud.elastic.dlagent.api.configuration.PropertiesDlEnvironment;
import cloud.elastic.dlagent.api.model.ConfigurationFactory;
import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
import cloud.elastic.dlagent.api.model.PluginConf;
import cloud.elastic.dlagent.api.utilities.BeanLocator;
import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import cloud.elastic.dlagent.plugins.iceberg.IcebergPolarisCatalogManager;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import cloud.elastic.dlagent.service.HttpRequestParser;
import cloud.elastic.dlagent.service.RequestParser;
import cloud.elastic.dlagent.service.bridge.BridgeFactory;
import cloud.elastic.dlagent.service.bridge.SimpleBridgeFactory;
import cloud.elastic.dlagent.service.controller.ReadService;
import cloud.elastic.dlagent.service.controller.ReadServiceImpl;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.iceberg.IcebergServiceImpl;
import cloud.elastic.dlagent.service.profile.ProfilesConf;
import cloud.elastic.dlagent.service.security.BaseSecurityService;
import cloud.elastic.dlagent.service.security.SecurityService;
import cloud.elastic.dlagent.grpc.config.ConfigurationBinder;
import cloud.elastic.dlagent.grpc.config.DlConfig;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Guice composition root for the gRPC transport: the counterpart of
 * dlagent-rest's component scan plus {@code DlConfiguration} /
 * {@code IcebergPluginConfig}.
 * <p>
 * Only bindings that Guice cannot infer are listed. Concrete
 * {@code @Singleton}-annotated classes with an {@code @Inject} or non-private
 * no-arg constructor are just-in-time bound by Guice with the right scope, so
 * this module carries the interface-to-implementation bindings, the
 * qualifier binding, the configuration objects, and the one scope that must
 * <em>not</em> be a singleton.
 * <p>
 * {@link ClientDisconnectPredicate} is bound explicitly to the gRPC
 * implementation: without a binding the predicate would be a silently wrong
 * default rather than a loud error.
 */
public class DiModule extends AbstractModule {

    private final Properties properties;
    private final DlConfig config;

    /**
     * Creates the module with an empty configuration; the resulting
     * {@link DlServerProperties} and {@link GopherConfigurationProperties}
     * carry their compiled-in defaults. For tests that do not care about
     * configuration -- production goes through {@link #DiModule(DlConfig)},
     * because the compiled-in defaults are NOT the shipped ones.
     */
    public DiModule() {
        this(null);
    }

    /**
     * Creates the module over the resolved agent configuration.
     *
     * <p>The configuration is carried rather than only its {@link Properties}
     * rendering because the property objects below have to be BOUND from it,
     * not merely handed a map: their field initialisers disagree with
     * application.properties (gopher.enabled false vs true, block-size 128MB vs
     * 16MB), so a provider that returns {@code new GopherConfigurationProperties()}
     * gives the gRPC agent a different FileIO from the REST agent's.
     *
     * @param config resolved agent configuration, or null for none
     */
    public DiModule(DlConfig config) {
        this.config = config;
        this.properties = new Properties();
        if (config != null) {
            this.properties.putAll(config.effectiveConfiguration());
        }
    }

    @Override
    protected void configure() {
        /*
         * The transport-specific half of the disconnect predicate:
         *   Status.fromThrowable(t).getCode() in {CANCELLED, DEADLINE_EXCEEDED}
         * (and not `t instanceof StatusRuntimeException`,
         * the concrete type is io.grpc.InternalStatusRuntimeException).
         */
        bind(ClientDisconnectPredicate.class).to(GrpcClientDisconnectPredicate.class)
                .in(Scopes.SINGLETON);

        bind(PluginConf.class).to(ProfilesConf.class).in(Scopes.SINGLETON);
        bind(ConfigurationFactory.class).to(BaseConfigurationFactory.class).in(Scopes.SINGLETON);
        bind(BridgeFactory.class).to(SimpleBridgeFactory.class).in(Scopes.SINGLETON);
        bind(SecurityService.class).to(BaseSecurityService.class).in(Scopes.SINGLETON);
        bind(ReadService.class).to(ReadServiceImpl.class).in(Scopes.SINGLETON);
        bind(IcebergService.class).to(IcebergServiceImpl.class).in(Scopes.SINGLETON);
        bind(new TypeLiteral<RequestParser<Map<String, List<String>>>>() { })
                .to(HttpRequestParser.class).in(Scopes.SINGLETON);

        /*
         * IcebergServiceImpl injects this with @Named, mirroring the
         * @Component("polarisIcebergCatalogManager") / @Qualifier pair the
         * REST side has always used.
         */
        bind(IcebergPolarisCatalogManager.class)
                .annotatedWith(Names.named("polarisIcebergCatalogManager"))
                .to(IcebergPolarisCatalogManager.class)
                .in(Scopes.SINGLETON);

    }

    /**
     * IcebergMetadataFetcher, one fresh instance per lookup.
     * <p>
     * MUST stay unscoped: the fetcher holds a mutable RequestContext, so a
     * singleton lets two concurrent INSERTs race on
     * {@code setRequestContext()} - one writer's parquet ends up in the
     * manifest twice and the other's is silently dropped. This is the Guice
     * half of that rule; the REST half is
     * {@code IcebergPluginConfig}'s {@code @Bean @Scope(SCOPE_PROTOTYPE)},
     * and {@code IcebergMetadataFetcherScopeTest} asserts both. Adding
     * {@code @Singleton} to this method is the mistake it guards against.
     * <p>
     * Constructing explicitly, mirroring IcebergPluginConfig, rather than
     * leaving it to a just-in-time binding: the class also has a public
     * no-arg constructor that resolves its collaborators through
     * {@link BeanLocator} (it exists for {@code BasePluginFactory}'s
     * reflection), and that is the one Guice would pick on its own - which
     * then fails whenever the injector is built before the locator is
     * installed.
     *
     * @param icebergUtilities     iceberg helpers
     * @param icebergCatalogWrapper catalog cache
     * @return a fresh fetcher
     */
    @Provides
    public IcebergMetadataFetcher icebergMetadataFetcher(IcebergUtilities icebergUtilities,
                                                         IcebergCatalogWrapper icebergCatalogWrapper) {
        return new IcebergMetadataFetcher(icebergUtilities, icebergCatalogWrapper);
    }

    /**
     * The resolved agent configuration, transport-neutrally.
     *
     * @return the environment core reads properties from
     */
    @Provides
    @Singleton
    public DlEnvironment dlEnvironment() {
        return new PropertiesDlEnvironment(properties);
    }

    /**
     * dlagent.* properties. On the REST side spring-boot binds this class from
     * the same keys; here it is handed over as an instance.
     *
     * @return the dlagent properties
     */
    @Provides
    @Singleton
    public DlServerProperties dlServerProperties() {
        return bound(new DlServerProperties(), DlServerProperties.PROPERTY_PREFIX);
    }

    /**
     * gopher.* properties; same reasoning as {@link #dlServerProperties()}.
     *
     * @return the gopher properties
     */
    @Provides
    @Singleton
    public GopherConfigurationProperties gopherConfigurationProperties() {
        return bound(new GopherConfigurationProperties(),
                     GopherConfigurationProperties.PROPERTY_PREFIX);
    }

    /**
     * The {@code iceberg.rest} defaults. dlagent-rest binds these with
     * {@code @ConfigurationProperties}; here the configuration parser fills
     * them in. Defaults keep the two jars agreeing when the key is unset.
     */
    @Provides
    @Singleton
    public IcebergRestProperties icebergRestProperties() {
        return bound(new IcebergRestProperties(), IcebergRestProperties.PROPERTY_PREFIX);
    }

    /**
     * Applies the resolved configuration to a property object, exactly as
     * spring-boot's {@code @ConfigurationProperties} does on the REST side.
     *
     * @param target the property object
     * @param prefix its property prefix
     * @param <T>    the property object's type
     * @return the same object, bound
     */
    private <T> T bound(T target, String prefix) {
        if (config != null) {
            ConfigurationBinder.bind(target, prefix, config);
        }
        return target;
    }

    /**
     * A registry for {@code MetricsReporter}. The REST side gets one from
     * spring-boot-actuator's auto-configuration, which the gRPC transport
     * deliberately does not carry, so bind a plain in-memory registry: the
     * metric calls stay identical, only the (unused) export endpoint is
     * absent.
     *
     * @return the meter registry
     */
    @Provides
    @Singleton
    public MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }

    /**
     * Installs Guice as the resolver behind {@link BeanLocator}, the way
     * dlagent-rest installs the Spring context.
     * <p>
     * Must run before the first request: the {@code *MetadataFetcher} classes
     * are created reflectively and call {@code BeanLocator.getBean} from their
     * no-arg constructors. {@link Main#compose} calls it before the server
     * starts listening.
     *
     * @param injector the injector to resolve through
     */
    public static void installBeanLocator(Injector injector) {
        BeanLocator.setResolver(injector::getInstance);
    }
}
