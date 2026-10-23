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

import cloud.elastic.dlagent.transport.ClientDisconnectPredicate;
import org.apache.catalina.connector.ClientAbortException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The REST disconnect test must stay exactly
 * {@code instanceof ClientAbortException} -- no wider.
 * <p>
 * javap shows the inheritance chain:
 * {@code ClientAbortException extends
 * org.apache.coyote.BadRequestException extends java.io.IOException}. The
 * abandoned gRPC branch replaced the catch with {@code IOException}, which
 * looks harmless and is not: every S3 / HDFS read failure and every object
 * storage timeout is an IOException too, so they would all be reclassified
 * from {@code Failed} (ERROR, with stacktrace) to {@code Aborted} (WARN, no
 * stacktrace). That changes the REST path's observable behaviour, which must
 * not change, and it is invisible until
 * someone tries to debug a storage outage from the agent log.
 */
class SpringClientDisconnectPredicateTest {

    private final ClientDisconnectPredicate predicate = new SpringClientDisconnectPredicate();

    @Test
    void clientAbortIsADisconnect() {
        assertTrue(predicate.isClientDisconnect(new ClientAbortException("client closed")));
        assertTrue(predicate.isClientDisconnect(new ClientAbortException(new IOException("pipe"))));
    }

    @Test
    void plainIoFailuresAreNotDisconnects() {
        assertFalse(predicate.isClientDisconnect(new IOException("s3: connection reset by peer")),
                "a bare IOException is an object-storage failure, not a client disconnect");
        assertFalse(predicate.isClientDisconnect(new SocketException("broken pipe")));
        assertFalse(predicate.isClientDisconnect(new SocketTimeoutException("read timed out")));
        assertFalse(predicate.isClientDisconnect(new InterruptedIOException("interrupted")));
    }

    @Test
    void unrelatedThrowablesAreNotDisconnects() {
        assertFalse(predicate.isClientDisconnect(new IllegalStateException("bug")));
        assertFalse(predicate.isClientDisconnect(new RuntimeException(
                new ClientAbortException("wrapped, not thrown directly"))));
    }
}
