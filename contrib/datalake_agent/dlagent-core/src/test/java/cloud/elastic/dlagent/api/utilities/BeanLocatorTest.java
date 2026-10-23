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

package cloud.elastic.dlagent.api.utilities;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The container-neutral static locator that replaced {@code SpringContext}.
 * Both composition roots install a resolver into it during startup; the three
 * {@code *MetadataFetcher} no-arg constructors read from it.
 */
class BeanLocatorTest {

    @AfterEach
    void clear() {
        BeanLocator.clearResolver();
    }

    @Test
    void resolvesThroughTheInstalledResolver() {
        Map<Class<?>, Object> container = new HashMap<>();
        String bean = "a bean";
        container.put(String.class, bean);
        BeanLocator.setResolver(container::get);

        assertSame(bean, BeanLocator.getBean(String.class));
    }

    /**
     * Without a resolver the failure must be immediate and say what is wrong.
     * The alternative - a null resolver producing a null bean - would surface
     * as an NPE deep inside a plugin constructor on the first request.
     */
    @Test
    void failsLoudlyWhenNoResolverIsInstalled() {
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> BeanLocator.getBean(String.class));
        assertTrue(thrown.getMessage().contains("BeanLocator.setResolver"),
                "the message should name the fix, was: " + thrown.getMessage());
    }

    @Test
    void rejectsANullResolver() {
        assertThrows(IllegalArgumentException.class, () -> BeanLocator.setResolver(null));
    }

    @Test
    void theLastInstalledResolverWins() {
        BeanLocator.setResolver(type -> "first");
        BeanLocator.setResolver(type -> "second");

        assertEquals("second", BeanLocator.getBean(String.class));
    }
}
