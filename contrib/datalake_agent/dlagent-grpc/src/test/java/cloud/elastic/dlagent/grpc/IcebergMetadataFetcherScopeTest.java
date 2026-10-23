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

import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.jupiter.api.Test;

import javax.inject.Provider;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * IcebergMetadataFetcher must not be a singleton -- the Guice half.
 * <p>
 * Same hazard as the REST half
 * ({@code cloud.elastic.dlagent.service.spring.IcebergMetadataFetcherScopeTest}):
 * the fetcher holds a mutable RequestContext, so sharing it lets two
 * concurrent INSERTs race on {@code setRequestContext()} and drop one
 * writer's data files from the manifest. JSR-330 cannot express "new
 * instance every time", so {@code DiModule} has to say
 * {@code Scopes.NO_SCOPE} explicitly and this test has to check that it
 * still does.
 */
class IcebergMetadataFetcherScopeTest {

    private static Injector injector() {
        return Guice.createInjector(new DiModule());
    }

    @Test
    void everyLookupYieldsAFreshFetcher() {
        Injector injector = injector();

        assertNotSame(injector.getInstance(IcebergMetadataFetcher.class),
                injector.getInstance(IcebergMetadataFetcher.class),
                "IcebergMetadataFetcher is shared between requests; concurrent INSERTs will "
                        + "race on setRequestContext() and lose one writer's data files");
    }

    /**
     * IcebergServiceImpl does not call the injector directly, it holds a
     * {@code javax.inject.Provider}; the scope has to hold through that too.
     */
    @Test
    void theInjectedProviderAlsoYieldsFreshFetchers() {
        Provider<IcebergMetadataFetcher> provider =
                injector().getProvider(IcebergMetadataFetcher.class);

        assertNotSame(provider.get(), provider.get());
    }

    /**
     * Control: the collaborators stay shared, so the assertions above are
     * about this one binding and not about a misconfigured injector.
     */
    @Test
    void collaboratorsAreStillSingletons() {
        Injector injector = injector();

        assertSame(injector.getInstance(IcebergCatalogWrapper.class),
                injector.getInstance(IcebergCatalogWrapper.class));
    }
}
