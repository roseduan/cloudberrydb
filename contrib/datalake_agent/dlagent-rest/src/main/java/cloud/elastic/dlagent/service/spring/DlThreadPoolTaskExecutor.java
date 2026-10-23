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

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * A {@link ThreadPoolTaskExecutor} that enhances error reporting when a
 * {@link TaskRejectedException} occurs. The error messages provide hints on
 * how to overcome {@link TaskRejectedException} errors, by suggesting tuning
 * parameters for dlagent.
 */
public class DlThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

    private static final String DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE = "dlagent Server processing capacity exceeded.";
    private static final String DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT = "Consider increasing the values of 'dlagent.task.pool.max-size' and/or 'dlagent.task.pool.queue-capacity' in 'dlagent-application.properties'";

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Runnable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public Future<?> submit(Runnable task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            DlRuntimeException exception = new DlRuntimeException(
                    DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Callable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            DlRuntimeException exception = new DlRuntimeException(
                    DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(DLAGENT_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }
}
