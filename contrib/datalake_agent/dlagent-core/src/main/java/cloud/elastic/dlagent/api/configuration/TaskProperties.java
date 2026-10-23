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

import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

/**
 * Own replacement for {@code org.springframework.boot.autoconfigure.task
 * .TaskExecutionProperties}, which dlagent-core cannot reference once it stops
 * depending on spring-boot-autoconfigure.
 * <p>
 * Property names and defaults are copied verbatim from Spring Boot 2.7.18 so
 * that binding {@code dlagent.task.*} keeps producing the same values -- the
 * defaults below were read off a live {@code new TaskExecutionProperties()}:
 * {@code thread-name-prefix=task-}, {@code pool.queue-capacity=2147483647},
 * {@code pool.core-size=8}, {@code pool.max-size=2147483647},
 * {@code pool.allow-core-thread-timeout=true}, {@code pool.keep-alive=PT1M},
 * {@code shutdown.await-termination=false},
 * {@code shutdown.await-termination-period=null}.
 * {@code src/main/resources/application.properties} sets thread-name-prefix,
 * pool.allow-core-thread-timeout, pool.core-size, pool.max-size and
 * pool.queue-capacity; the rest come from these defaults.
 */
@Getter
@Setter
public class TaskProperties {

    private final Pool pool = new Pool();

    private final Shutdown shutdown = new Shutdown();

    /**
     * Prefix to use for the names of newly created threads.
     */
    private String threadNamePrefix = "task-";

    @Getter
    @Setter
    public static class Pool {

        /**
         * Queue capacity. An unbounded capacity does not increase the pool and
         * therefore ignores the "max-size" property.
         */
        private int queueCapacity = Integer.MAX_VALUE;

        /**
         * Core number of threads.
         */
        private int coreSize = 8;

        /**
         * Maximum allowed number of threads. If tasks are filling up the queue,
         * the pool can expand up to that size to accommodate the load. Ignored
         * if the queue is unbounded.
         */
        private int maxSize = Integer.MAX_VALUE;

        /**
         * Whether core threads are allowed to time out. This enables dynamic
         * growing and shrinking of the pool.
         */
        private boolean allowCoreThreadTimeout = true;

        /**
         * Time limit for which threads may remain idle before being terminated.
         */
        private Duration keepAlive = Duration.ofSeconds(60);
    }

    @Getter
    @Setter
    public static class Shutdown {

        /**
         * Whether the executor should wait for scheduled tasks to complete on
         * shutdown.
         */
        private boolean awaitTermination;

        /**
         * Maximum time the executor should wait for remaining tasks to complete.
         */
        private Duration awaitTerminationPeriod;
    }
}
