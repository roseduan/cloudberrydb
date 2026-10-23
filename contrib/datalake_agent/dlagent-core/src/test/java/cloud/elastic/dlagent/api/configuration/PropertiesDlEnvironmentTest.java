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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Properties-backed {@link DlEnvironment} used by the non-Spring
 * containers. Its job is narrow on purpose: dlagent-grpc's {@code DlConfig}
 * is the full parser, this only decodes an already-resolved value.
 */
class PropertiesDlEnvironmentTest {

    private static DlEnvironment environmentWith(String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return new PropertiesDlEnvironment(properties);
    }

    @Test
    void returnsTheDefaultWhenUnset() {
        DlEnvironment environment = new PropertiesDlEnvironment(new Properties());

        assertEquals(Boolean.TRUE,
                environment.getProperty("dlagent.metrics.partition.enabled", Boolean.class, Boolean.TRUE));
        assertEquals(Long.valueOf(1000L),
                environment.getProperty("dlagent.metrics.report-frequency", Long.class, 1000L));
    }

    @Test
    void convertsTheTypesCoreAsksFor() {
        assertEquals(Boolean.TRUE,
                environmentWith("k", "true").getProperty("k", Boolean.class, Boolean.FALSE));
        assertEquals(Boolean.FALSE,
                environmentWith("k", "false").getProperty("k", Boolean.class, Boolean.TRUE));
        assertEquals("v", environmentWith("k", "v").getProperty("k", String.class, "d"));
        assertEquals(Integer.valueOf(7), environmentWith("k", "7").getProperty("k", Integer.class, 0));
        assertEquals(Long.valueOf(9L), environmentWith("k", "9").getProperty("k", Long.class, 0L));
        assertEquals(Duration.ofMinutes(5),
                environmentWith("k", "PT5M").getProperty("k", Duration.class, Duration.ZERO));
    }

    @Test
    void trimsSurroundingWhitespace() {
        assertEquals(Long.valueOf(1000L),
                environmentWith("k", "  1000 ").getProperty("k", Long.class, 0L));
    }

    /**
     * A property that is set but undecodable must fail, not fall back to the
     * default: silently running on the default is how a misconfigured agent
     * looks healthy while doing the wrong thing.
     */
    @Test
    void failsOnAnUnconvertibleValue() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> environmentWith("dlagent.metrics.report-frequency", "soon")
                        .getProperty("dlagent.metrics.report-frequency", Long.class, 0L));
        assertTrue(thrown.getMessage().contains("dlagent.metrics.report-frequency"));
    }

    @Test
    void failsOnAnUnsupportedTargetType() {
        assertThrows(IllegalArgumentException.class,
                () -> environmentWith("k", "v").getProperty("k", Properties.class, null));
    }
}
