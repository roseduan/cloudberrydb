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

package cloud.elastic.dlagent.service.rest;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Preconditions for the end-to-end tests in this package, which post to a real
 * agent over HTTP instead of driving the controller in-process.
 *
 * <p>They are guarded rather than left to fail because their dependencies --
 * a running agent, and behind it a gopher store, a hive metastore and an
 * object store -- are not present on a plain developer machine or in the
 * ordinary CI job. A suite that is red everywhere teaches people to ignore it,
 * which is how the earlier version of these tests came to assert nothing at
 * all for months.
 *
 * <p>Bring the agent up with {@code test/start-with-mock-parent.sh} (export
 * {@code LD_LIBRARY_PATH} to the thirdparty lib directory first, or its native
 * gopher client will not load); the storage side needs the compose stack the
 * request fixtures point at.
 */
final class AgentAvailability {

    /** Where start-with-mock-parent.sh puts the agent. */
    private static final String HEALTH_URL = "http://localhost:8080/actuator/health";

    /**
     * The gopher client socket the builtin fixtures configure. Its absence is
     * the cheapest reliable signal that no store is listening -- reaching the
     * catalog without it fails deep inside the native client.
     */
    private static final String GOPHER_SOCKET = "/tmp/gopher/builtin/connect";

    private AgentAvailability() {
    }

    /** Skips the calling test unless an agent answers on 8080. */
    static void assumeAgentRunning() {
        assumeTrue(agentResponds(),
                "no dlagent on " + HEALTH_URL
                        + "; start test/start-with-mock-parent.sh to run this test");
    }

    /** Skips the calling test unless the storage the fixtures point at is up. */
    static void assumeStorageRunning() {
        assumeTrue(Files.exists(Paths.get(GOPHER_SOCKET)),
                "no gopher store at " + GOPHER_SOCKET
                        + "; this test reaches a catalog and needs the compose stack");
    }

    private static boolean agentResponds() {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(HEALTH_URL).openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            connection.setRequestMethod("GET");
            return connection.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
