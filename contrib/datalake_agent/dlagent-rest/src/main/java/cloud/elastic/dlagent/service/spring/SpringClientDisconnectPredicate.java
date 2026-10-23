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
import org.springframework.stereotype.Component;

/**
 * REST implementation of {@link ClientDisconnectPredicate}.
 * <p>
 * Exactly the test the code carried inline before the three-module split:
 * {@code t instanceof ClientAbortException}, the exception embedded Tomcat
 * throws when the client stops reading. It is deliberately <em>not</em>
 * widened to {@code IOException} -- {@code ClientAbortException extends
 * BadRequestException extends IOException}, so widening would reclassify
 * every S3 / HDFS read failure from {@code Failed} (ERROR, with stacktrace)
 * to {@code Aborted} (WARN, no stacktrace).
 */
@Component
public class SpringClientDisconnectPredicate implements ClientDisconnectPredicate {

    @Override
    public boolean isClientDisconnect(Throwable t) {
        return t instanceof ClientAbortException;
    }
}
