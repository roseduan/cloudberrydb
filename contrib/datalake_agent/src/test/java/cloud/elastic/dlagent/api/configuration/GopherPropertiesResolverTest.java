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

import cloud.elastic.dlagent.api.model.iceberg.GopherRuntimeConfig;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GopherPropertiesResolverTest {

    private GopherConfigurationProperties baseline;
    private GopherPropertiesResolver resolver;

    @BeforeEach
    public void setUp() {
        baseline = new GopherConfigurationProperties();
        baseline.setEnabled(true);
        baseline.setWorkerPath("/baseline/worker");
        baseline.setConnectPath("/baseline/connect");
        baseline.setCacheStrategy("GOPHER_CACHE");
        baseline.setLogLevel("info");
        baseline.setLiboss2LogLevel("info");
        baseline.setBlockSize(null);  // suppress noise keys not relevant here
        baseline.setBufferSize(null);
        resolver = new GopherPropertiesResolver(baseline);
    }

    @Test
    public void baseline_returnsMapDerivedFromApplicationProperties() {
        Map<String, String> base = resolver.baseline();
        assertEquals("true", base.get("gopher.enabled"));
        assertEquals("/baseline/worker", base.get("gopher.worker_path"));
        assertEquals("/baseline/connect", base.get("gopher.connect_path"));
        assertEquals("GOPHER_CACHE", base.get("gopher.cache_strategy"));
    }

    @Test
    public void isEnabled_readsBaselineFlag() {
        assertTrue(resolver.isEnabled());
        baseline.setEnabled(false);
        assertFalse(resolver.isEnabled());
    }

    @Test
    public void isEnabledFromConfiguration_readsGopherEnabledKey() {
        Configuration conf = new Configuration(false);
        conf.set("gopher.enabled", "true");
        assertTrue(resolver.isEnabled(conf));
        conf.set("gopher.enabled", "false");
        assertFalse(resolver.isEnabled(conf));
    }

    @Test
    public void build_withNoVolumeOrRuntime_returnsBaselineOnly() {
        Map<String, String> out = resolver.build(null, null);
        assertEquals("/baseline/worker", out.get("gopher.worker_path"));
        assertNull(out.get("gopher.endpoint"));
        assertNull(out.get("gopher.bucket"));
    }

    @Test
    public void build_runtimeOverridesBaseline() {
        GopherRuntimeConfig runtime = new GopherRuntimeConfig();
        runtime.setWorkerPath("/req/worker");
        runtime.setConnectPath("/req/connect");
        runtime.setLogLevel("debug1");

        Map<String, String> out = resolver.build(null, runtime);

        assertEquals("/req/worker", out.get("gopher.worker_path"));
        assertEquals("/req/connect", out.get("gopher.connect_path"));
        assertEquals("debug1", out.get("gopher.log_level"));
        // unaffected baseline fields stay
        assertEquals("GOPHER_CACHE", out.get("gopher.cache_strategy"));
    }

    @Test
    public void build_s3VolumeMapsToS3aAndStripsEndpointScheme() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("s3");
        volume.setVolumeEndpoint("http://minio:9000");
        volume.setBucketName("foo");
        volume.setVolumeRegion("us-east-1");
        volume.setAccessKeyId("ak");
        volume.setSecretAccessKey("sk");
        volume.setPathStyleAccess(true);

        Map<String, String> out = resolver.build(volume, null);

        // ufs_type legacy hack mapping
        assertEquals("s3a", out.get("gopher.ufs_type"));
        // endpoint scheme stripped, useHttps derived
        assertEquals("minio:9000", out.get("gopher.endpoint"));
        assertEquals("false", out.get("gopher.useHttps"));
        // useVirtualHost is the negation of path_style_access
        assertEquals("true", out.get("gopher.path_style_access"));
        assertEquals("false", out.get("gopher.useVirtualHost"));
        // verbatim fields
        assertEquals("foo", out.get("gopher.bucket"));
        assertEquals("us-east-1", out.get("gopher.region"));
        assertEquals("ak", out.get("gopher.access_key"));
        assertEquals("sk", out.get("gopher.secret_key"));
    }

    @Test
    public void build_s3v2VolumeMapsToS3av2() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("s3v2");
        Map<String, String> out = resolver.build(volume, null);
        assertEquals("s3av2", out.get("gopher.ufs_type"));
    }

    @Test
    public void build_httpsEndpointSetsUseHttpsTrue() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("s3");
        volume.setVolumeEndpoint("https://s3.amazonaws.com");
        Map<String, String> out = resolver.build(volume, null);
        assertEquals("s3.amazonaws.com", out.get("gopher.endpoint"));
        assertEquals("true", out.get("gopher.useHttps"));
    }

    @Test
    public void build_otherVolumeTypesPassThroughVerbatim() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("oss");
        Map<String, String> out = resolver.build(volume, null);
        // not s3 or s3v2 -> no hack mapping
        assertEquals("oss", out.get("gopher.ufs_type"));
    }

    @Test
    public void build_hdfsVolumeMapsHdfsFieldsAndSkipsObjectStorageFields() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("hdfs");
        volume.setHdfsNamenodeHost("nn1.example.com");
        volume.setHdfsNamenodePort("8020");
        volume.setHdfsAuthMethod("kerberos");
        volume.setKrbPrincipal("hdfs/nn1.example.com@EXAMPLE.COM");
        volume.setIsHaSupported(false);

        // these are object-storage fields that must NOT leak into the hdfs map
        volume.setVolumeEndpoint("http://should-not-appear");
        volume.setBucketName("should-not-appear");

        Map<String, String> out = resolver.build(volume, null);

        assertEquals("hdfs", out.get("gopher.ufs_type"));
        assertEquals("nn1.example.com", out.get("gopher.name_node"));
        assertEquals("8020", out.get("gopher.port"));
        assertEquals("kerberos", out.get("gopher.auth_method"));
        assertEquals("hdfs/nn1.example.com@EXAMPLE.COM", out.get("gopher.krb_principal"));
        assertEquals("false", out.get("gopher.is_ha_supported"));
        // object-storage fields not populated for hdfs
        assertNull(out.get("gopher.endpoint"));
        assertNull(out.get("gopher.bucket"));
    }

    @Test
    public void build_hdfsHaVolumePopulatesAllHaKeys() {
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("hdfs");
        volume.setIsHaSupported(true);
        volume.setDfsNameservices("ns1");
        volume.setDfsHaNamenodes("nn1,nn2");
        volume.setDfsNamenodeRpcAddress("nn1.host:8020,nn2.host:8020");
        volume.setDfsClientFailoverProxyProvider("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        volume.setDfsClientUseDatanodeHostname(true);

        Map<String, String> out = resolver.build(volume, null);

        assertEquals("true", out.get("gopher.is_ha_supported"));
        assertEquals("ns1", out.get("gopher.dfs_nameservices"));
        assertEquals("nn1,nn2", out.get("gopher.dfs_ha_namenodes"));
        assertEquals("nn1.host:8020,nn2.host:8020", out.get("gopher.dfs_namenode_rpc_address"));
        assertEquals("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                out.get("gopher.dfs_client_failover_proxy_provider"));
        assertEquals("true", out.get("gopher.dfs_client_use_datanode_hostname"));
    }

    @Test
    public void applyTo_writesAllEntriesIntoHadoopConfiguration() {
        Configuration conf = new Configuration(false);
        VolumeInfo volume = new VolumeInfo();
        volume.setVolumeServerType("s3");
        volume.setVolumeEndpoint("http://minio:9000");
        volume.setBucketName("foo");

        Map<String, String> props = resolver.build(volume, null);
        resolver.applyTo(conf, props);

        assertEquals("minio:9000", conf.get("gopher.endpoint"));
        assertEquals("foo", conf.get("gopher.bucket"));
        assertEquals("s3a", conf.get("gopher.ufs_type"));
    }

    @Test
    public void applyTo_nullOrEmptyInputIsNoop() {
        Configuration conf = new Configuration(false);
        resolver.applyTo(conf, null);
        resolver.applyTo(null, resolver.baseline());
        // no exception, no side effects assert beyond not crashing
    }
}
