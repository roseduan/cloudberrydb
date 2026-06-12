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

package cloud.elastic.dlagent.api.model;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the legacy dual-track key normalization: sections written in
 * the new SQL-OPTION-named format are translated back to the legacy spellings
 * the generic-FDW transforms understand.
 */
public class BaseConfigurationFactoryNormalizeTest {

    /* The constructor only stores collaborators; normalization needs none. */
    private final BaseConfigurationFactory factory =
            new BaseConfigurationFactory(null, null, null);

    private static final Map<String, String> S3_MAP = ImmutableMap.<String, String>builder()
            .put("type", "fs.gopher.ufs_type")
            .put("endpoint", "fs.s3a.endpoint")
            .put("region", "fs.s3a.endpoint.region")
            .put("path_style_access", "fs.s3a.path.style.access")
            .put("access_key_id", "fs.s3a.access.key")
            .put("secret_access_key", "fs.s3a.secret.key")
            .build();

    private static final Map<String, String> HDFS_MAP = ImmutableMap.of(
            "hdfs_namenodes", "hdfs_namenode_host",
            "hdfs_port", "hdfs_namenode_port",
            "dfs_nameservices", "dfs.nameservices",
            "dfs_ha_namenodes", "dfs.ha.namenodes",
            "dfs_namenode_rpc_address", "dfs.namenode.rpc-address");

    @Test
    public void newS3KeysTranslateToLegacySpellings() {
        Map<String, Object> in = new HashMap<>();
        in.put("type", "s3");
        in.put("endpoint", "http://minio:9000");
        in.put("access_key_id", "ak");
        in.put("secret_access_key", "sk");
        in.put("fs.defaultFS", "s3a://");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "s3.conf", S3_MAP);

        assertEquals("s3", out.get("fs.gopher.ufs_type"));
        assertEquals("http://minio:9000", out.get("fs.s3a.endpoint"));
        assertEquals("ak", out.get("fs.s3a.access.key"));
        assertEquals("sk", out.get("fs.s3a.secret.key"));
        // untouched keys pass through; new keys are consumed
        assertEquals("s3a://", out.get("fs.defaultFS"));
        assertFalse(out.containsKey("type"));
        assertFalse(out.containsKey("endpoint"));
    }

    @Test
    public void newKeyWinsWhenBothSpellingsPresent() {
        Map<String, Object> in = new HashMap<>();
        in.put("endpoint", "http://new:9000");
        in.put("fs.s3a.endpoint", "http://old:9000");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "s3.conf", S3_MAP);

        assertEquals("http://new:9000", out.get("fs.s3a.endpoint"));
    }

    @Test
    public void legacyOnlySectionPassesThroughUnchanged() {
        Map<String, Object> in = new HashMap<>();
        in.put("fs.s3a.endpoint", "http://old:9000");
        in.put("fs.s3a.access.key", "ak");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "s3.conf", S3_MAP);

        assertEquals(in, out);
    }

    @Test
    public void hdfsNamenodesSpliceSplitsHostPort() {
        Map<String, Object> in = new HashMap<>();
        in.put("hdfs_namenodes", "192.168.1.10:8020");
        in.put("hdfs_port", "9000");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "gphdfs.conf", HDFS_MAP);

        assertEquals("192.168.1.10", out.get("hdfs_namenode_host"));
        // the spliced port wins over the separate hdfs_port key
        assertEquals("8020", out.get("hdfs_namenode_port"));
    }

    @Test
    public void hdfsFailoverProviderGetsNameserviceSuffix() {
        Map<String, Object> in = new HashMap<>();
        in.put("hdfs_namenodes", "mycluster");
        in.put("dfs_nameservices", "mycluster");
        in.put("dfs_ha_namenodes", "nn1,nn2");
        in.put("dfs_namenode_rpc_address", "10.0.0.1:9000,10.0.0.2:9000");
        in.put("dfs_client_failover_proxy_provider", "custom.Provider");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "gphdfs.conf", HDFS_MAP);

        assertEquals("mycluster", out.get("hdfs_namenode_host"));
        assertEquals("mycluster", out.get("dfs.nameservices"));
        assertEquals("nn1,nn2", out.get("dfs.ha.namenodes"));
        assertEquals("10.0.0.1:9000,10.0.0.2:9000", out.get("dfs.namenode.rpc-address"));
        assertEquals("custom.Provider", out.get("dfs.client.failover.proxy.provider.mycluster"));
        assertNull(out.get("dfs_client_failover_proxy_provider"));
    }

    @Test
    public void hiveUrlTranslatesToUris() {
        Map<String, Object> in = new HashMap<>();
        in.put("url", "thrift://hms:9083");

        Map<String, Object> out = factory.normalizeSiteKeys(in, "gphive.conf",
                ImmutableMap.of("url", "uris"));

        assertEquals("thrift://hms:9083", out.get("uris"));
        assertFalse(out.containsKey("url"));
    }
}
