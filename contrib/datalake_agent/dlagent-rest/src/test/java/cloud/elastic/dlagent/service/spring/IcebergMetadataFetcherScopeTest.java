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

import cloud.elastic.dlagent.plugins.iceberg.IcebergCatalogWrapper;
import cloud.elastic.dlagent.plugins.iceberg.IcebergMetadataFetcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IcebergMetadataFetcher must not be a singleton -- the REST half.
 * <p>
 * The fetcher extends BasePlugin and holds a mutable RequestContext that
 * {@code IcebergServiceImpl.newFetcher} sets per operation. As a singleton,
 * two concurrent INSERTs into the same iceberg table race on
 * {@code setRequestContext()}: one writer's parquet ends up in the manifest
 * twice and the other writer's is silently dropped. Both
 * {@code IcebergPluginConfig} and {@code IcebergServiceImpl} carry that
 * warning in a comment.
 * <p>
 * JSR-330 has no portable annotation for "new instance every time", so the
 * scope has to be declared separately in each composition root, and getting
 * it wrong neither fails the build nor fails startup -- it only loses data, under concurrency, in production.
 * Hence this test, and its Guice counterpart in dlagent-grpc.
 */
class IcebergMetadataFetcherScopeTest {

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
    void everyLookupYieldsAFreshFetcher() {
        IcebergMetadataFetcher first = context.getBean(IcebergMetadataFetcher.class);
        IcebergMetadataFetcher second = context.getBean(IcebergMetadataFetcher.class);

        assertNotSame(first, second,
                "IcebergMetadataFetcher is shared between requests; concurrent INSERTs will "
                        + "race on setRequestContext() and lose one writer's data files");
    }

    @Test
    void theBeanDefinitionSaysPrototype() {
        BeanDefinition definition =
                context.getBeanFactory().getBeanDefinition("icebergMetadataFetcher");

        assertTrue(definition.isPrototype(), "expected prototype scope, got " + definition.getScope());
        assertFalse(definition.isSingleton());
    }

    /**
     * Control: the collaborators the fetcher is built from are still shared,
     * so the previous assertions are about scope and not about a container
     * that hands out fresh copies of everything.
     */
    @Test
    void collaboratorsAreStillSingletons() {
        assertSame(context.getBean(IcebergCatalogWrapper.class),
                context.getBean(IcebergCatalogWrapper.class));
    }
}
