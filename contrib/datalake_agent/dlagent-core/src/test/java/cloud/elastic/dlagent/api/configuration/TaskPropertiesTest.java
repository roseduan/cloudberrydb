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

package cloud.elastic.dlagent.api.configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TaskProperties} replaces spring-boot-autoconfigure's
 * {@code TaskExecutionProperties}, and {@code DlConfiguration} feeds it
 * straight into the {@code TaskExecutorBuilder} that sizes the thread pool
 * streaming every read response.
 * <p>
 * application.properties only sets thread-name-prefix and four pool keys, so
 * the remaining values come from these defaults; a "tidier" default here
 * would silently resize a production thread pool. The expected values were
 * read off a live {@code new TaskExecutionProperties()} on spring-boot
 * 2.7.18.
 */
class TaskPropertiesTest {

    @Test
    void defaultsMatchSpringBoot2718() {
        TaskProperties properties = new TaskProperties();

        assertEquals("task-", properties.getThreadNamePrefix());
        assertEquals(Integer.MAX_VALUE, properties.getPool().getQueueCapacity());
        assertEquals(8, properties.getPool().getCoreSize());
        assertEquals(Integer.MAX_VALUE, properties.getPool().getMaxSize());
        assertTrue(properties.getPool().isAllowCoreThreadTimeout());
        assertEquals(Duration.ofSeconds(60), properties.getPool().getKeepAlive());
        assertFalse(properties.getShutdown().isAwaitTermination());
        assertNull(properties.getShutdown().getAwaitTerminationPeriod());
    }

    /**
     * The nested objects must be settable in place: spring-boot's relaxed
     * binder walks {@code getPool()} and calls setters on the returned
     * instance rather than replacing it.
     */
    @Test
    void nestedValuesAreMutable() {
        TaskProperties properties = new TaskProperties();

        properties.setThreadNamePrefix("dlagent-response-");
        properties.getPool().setCoreSize(8);
        properties.getPool().setMaxSize(200);
        properties.getPool().setQueueCapacity(0);
        properties.getPool().setAllowCoreThreadTimeout(false);

        assertEquals("dlagent-response-", properties.getThreadNamePrefix());
        assertEquals(200, properties.getPool().getMaxSize());
        assertEquals(0, properties.getPool().getQueueCapacity());
        assertFalse(properties.getPool().isAllowCoreThreadTimeout());
    }
}
