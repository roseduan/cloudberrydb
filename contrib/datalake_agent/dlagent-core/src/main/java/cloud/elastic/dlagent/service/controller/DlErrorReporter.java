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

package cloud.elastic.dlagent.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.service.utilities.ThrowingSupplier;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;

/**
 * Base class that allows executing an action that returns a result or throws an exception. The exception
 * is logged into a log file and wrapped into a DlRuntimeException that can be later handled
 * by the DlExceptionHandler. This is the only place where any exception thrown within dlagent should be logged.
 * <p>
 * In the future, this logic should be implemented by an Spring AOP annotation-driven aspect and the corresponding
 * annotation can be specified for the endpoint methods that must have any exceptions they throw be reported before
 * propagating them to the container.
 *
 * @param <T> type of data the action returns
 */
@Slf4j
public abstract class DlErrorReporter<T> {

    /**
     * Tells apart "the client went away" from a genuine failure. Supplied by
     * the transport, because only the transport knows what a disconnect looks
     * like -- see {@link ClientDisconnectPredicate}. This used to be a
     * hard-coded {@code catch (org.apache.catalina.connector
     * .ClientAbortException)}, which dlagent-core may no longer reference.
     */
    private final ClientDisconnectPredicate clientDisconnectPredicate;

    /**
     * Creates the reporter.
     *
     * @param clientDisconnectPredicate transport-specific disconnect test
     */
    protected DlErrorReporter(ClientDisconnectPredicate clientDisconnectPredicate) {
        this.clientDisconnectPredicate = clientDisconnectPredicate;
    }

    protected T invokeWithErrorHandling(ThrowingSupplier<T, Exception> action) {
        try {
            // call the action and return the value if there are no errors
            return action.get();
        } catch (Error e) {
            // let Error propagate itself
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            if (clientDisconnectPredicate.isClientDisconnect(e)) {
                // a client (GPDB) decided to end the connection, which is common for
                // LIMIT queries (ex: SELECT * FROM table LIMIT 1), so we want to log
                // just a warning message, not an error with the full stacktrace
                // (unless in debug mode)
                if (log.isDebugEnabled()) {
                    // Stacktrace in debug
                    log.warn("Remote connection closed by the client.", e);
                } else {
                    log.warn("Remote connection closed by the client (enable debug for the stacktrace).");
                }
                // wrap into DlAgentRuntimeException so that it can be handled by the DlAgentExceptionHandler
                throw new DlRuntimeException(e);
            }
            if (e instanceof DlRuntimeException) {
                // let DlRuntimeException propagate itself
                log.error(e.getMessage(), e);
                throw (DlRuntimeException) e;
            }
            log.error(StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getName()), e);
            // wrap into DlRuntimeException so that it can be handled by the DlExceptionHandler
            throw new DlRuntimeException(e);
        }
    }
}

