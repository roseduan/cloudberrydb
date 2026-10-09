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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Volume paths are authority-less ("hdfs:///warehouse/..."), so cleanup
 * requests can only reach the right cluster if fs.defaultFS is derived from
 * the volume options.  Getting it wrong leaves deleted-table files orphaned.
 */
class HdfsDefaultFsTest {

    private Map<String, String> cfg(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void hostAndSeparatePort() {
        assertEquals("hdfs://nn.example:8020",
                IcebergRestController.hdfsDefaultFs(cfg("namenodes", "nn.example", "port", "8020")));
    }

    @Test
    void portSplicedIntoNamenodesWins() {
        assertEquals("hdfs://nn.example:9000",
                IcebergRestController.hdfsDefaultFs(
                        cfg("namenodes", "nn.example:9000", "port", "8020")));
    }

    @Test
    void bareHostFallsBackToDefaultPort() {
        assertEquals("hdfs://nn.example:8020",
                IcebergRestController.hdfsDefaultFs(cfg("namenodes", "nn.example")));
    }

    @Test
    void emptyPortFallsBackToDefaultPort() {
        assertEquals("hdfs://nn.example:8020",
                IcebergRestController.hdfsDefaultFs(cfg("namenodes", "nn.example", "port", "")));
    }

    @Test
    void haNameserviceWinsOverNamenodes() {
        assertEquals("hdfs://mycluster",
                IcebergRestController.hdfsDefaultFs(
                        cfg("nameservices", "mycluster", "namenodes", "nn1.example", "port", "8020")));
    }

    @Test
    void namenodeListWithoutNameserviceLeavesDefaultFsAlone() {
        /* "hdfs://nn1:8020,nn2:8020" is not a URI Hadoop can parse. */
        assertNull(IcebergRestController.hdfsDefaultFs(cfg("namenodes", "nn1:8020,nn2:8020")));
        assertNull(IcebergRestController.hdfsDefaultFs(cfg("namenodes", "nn1,nn2", "port", "8020")));
    }

    @Test
    void namenodeListStillResolvesViaNameservice() {
        assertEquals("hdfs://mycluster",
                IcebergRestController.hdfsDefaultFs(
                        cfg("nameservices", "mycluster", "namenodes", "nn1:8020,nn2:8020")));
    }

    @Test
    void noNamenodeLeavesDefaultFsAlone() {
        assertNull(IcebergRestController.hdfsDefaultFs(cfg()));
        assertNull(IcebergRestController.hdfsDefaultFs(cfg("namenodes", "")));
        assertNull(IcebergRestController.hdfsDefaultFs(cfg("port", "8020")));
    }
}
