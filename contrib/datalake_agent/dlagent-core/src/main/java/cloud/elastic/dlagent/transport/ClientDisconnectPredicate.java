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

package cloud.elastic.dlagent.transport;

/**
 * Transport-neutral test for "the client went away".
 * <p>
 * dlagent-core must classify a failed request as either an abort (the client
 * closed the connection, e.g. a {@code LIMIT} query that stopped reading) or a
 * genuine failure (an S3 / HDFS read error, a timeout, ...), because the two
 * are logged at different levels and reported with different status strings.
 * Only the transport knows which throwable means "client gone", so each
 * composition root binds its own implementation:
 * <ul>
 *   <li>dlagent-rest: {@code t instanceof
 *       org.apache.catalina.connector.ClientAbortException} -- byte-for-byte
 *       what the code did before the three-module split.</li>
 *   <li>dlagent-grpc: {@code Status.fromThrowable(t).getCode()} in
 *       {@code {CANCELLED, DEADLINE_EXCEEDED}} (the concrete type observed is {@code io.grpc.InternalStatusRuntimeException}, so do
 *       not write {@code instanceof StatusRuntimeException}).</li>
 * </ul>
 * Deliberately <em>not</em> {@code catch (IOException)}:
 * {@code ClientAbortException extends BadRequestException extends IOException},
 * so widening to IOException would silently reclassify object-storage read
 * failures from {@code Failed}/ERROR/with-stack to {@code Aborted}/WARN/no-stack.
 * <p>
 * This predicate only <em>classifies</em> a throwable that has already been
 * caught. Producing the termination point in the first place is the transport's
 * job -- over gRPC, a cancelled client does not throw into the handler at all
 * and the server write loop has to poll
 * {@code ServerCallStreamObserver.isCancelled()}.
 */
public interface ClientDisconnectPredicate {

    /**
     * Tells whether the given throwable means the client closed the connection.
     *
     * @param t throwable caught while serving a request, never null
     * @return true if the client disconnected, false if this is a real failure
     */
    boolean isClientDisconnect(Throwable t);
}
