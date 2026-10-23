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

import cloud.elastic.dlagent.api.error.DlRuntimeException;
import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The error classification that used to be a hard-coded
 * {@code catch (ClientAbortException)}.
 * <p>
 * Restructuring that chain is the one edit in the de-Spring pass that could
 * silently change what an operator sees in the log, so each of the four
 * outcomes is pinned here: client disconnect, DlRuntimeException, any other
 * exception, and Error.
 */
class DlErrorReporterTest {

    /** Minimal concrete subclass; DlErrorReporter is abstract. */
    private static final class Reporter extends DlErrorReporter<String> {
        Reporter(ClientDisconnectPredicate predicate) {
            super(predicate);
        }

        String run(java.util.concurrent.Callable<String> action) {
            return invokeWithErrorHandling(action::call);
        }
    }

    private static final ClientDisconnectPredicate EVERYTHING_IS_A_DISCONNECT = t -> true;
    private static final ClientDisconnectPredicate NOTHING_IS_A_DISCONNECT = t -> false;

    @Test
    void returnsTheValueWhenNothingFails() {
        Reporter reporter = new Reporter(NOTHING_IS_A_DISCONNECT);
        assertEquals("ok", reporter.run(() -> "ok"));
    }

    @Test
    void wrapsAClientDisconnect() {
        Reporter reporter = new Reporter(EVERYTHING_IS_A_DISCONNECT);
        IOException cause = new IOException("client went away");

        DlRuntimeException thrown = assertThrows(DlRuntimeException.class,
                () -> reporter.run(() -> {
                    throw cause;
                }));

        assertSame(cause, thrown.getCause());
    }

    @Test
    void wrapsAnOrdinaryFailure() {
        Reporter reporter = new Reporter(NOTHING_IS_A_DISCONNECT);
        IOException cause = new IOException("s3: connection reset");

        DlRuntimeException thrown = assertThrows(DlRuntimeException.class,
                () -> reporter.run(() -> {
                    throw cause;
                }));

        assertSame(cause, thrown.getCause());
    }

    /**
     * The predicate decides, not the exception type. An IOException that the
     * transport does not call a disconnect must stay a failure - widening the
     * old catch to IOException is exactly the regression found on the
     * abandoned branch, where every S3 / HDFS read error would have been
     * demoted to "Aborted" at WARN without a stacktrace.
     */
    @Test
    void ioExceptionAloneIsNotADisconnect() {
        ClientDisconnectPredicate onlyMarked = t -> t instanceof MarkerException;

        assertTrue(onlyMarked.isClientDisconnect(new MarkerException()));
        org.junit.jupiter.api.Assertions.assertFalse(
                onlyMarked.isClientDisconnect(new IOException("s3 read failed")));
    }

    @Test
    void letsDlRuntimeExceptionThrough() {
        Reporter reporter = new Reporter(NOTHING_IS_A_DISCONNECT);
        DlRuntimeException original = new DlRuntimeException("already wrapped");

        DlRuntimeException thrown = assertThrows(DlRuntimeException.class,
                () -> reporter.run(() -> {
                    throw original;
                }));

        assertSame(original, thrown, "an already-wrapped exception must not be wrapped twice");
    }

    @Test
    void letsErrorThrough() {
        Reporter reporter = new Reporter(EVERYTHING_IS_A_DISCONNECT);
        OutOfMemoryError original = new OutOfMemoryError("heap");

        OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
                () -> reporter.run(() -> {
                    throw original;
                }));

        assertSame(original, thrown,
                "an Error must propagate untouched, whatever the disconnect predicate says");
    }

    /** Stands in for a transport-specific disconnect exception. */
    private static final class MarkerException extends IOException {
        private static final long serialVersionUID = 1L;
    }
}
