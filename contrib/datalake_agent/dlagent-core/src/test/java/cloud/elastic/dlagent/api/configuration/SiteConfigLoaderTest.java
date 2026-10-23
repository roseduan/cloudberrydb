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

import cloud.elastic.dlagent.api.model.iceberg.CatalogInfo;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SiteConfigLoaderTest {

    private final SiteConfigLoader loader = new SiteConfigLoader();

    @TempDir
    Path tempDir;

    @BeforeEach
    public void pointLoaderAtTempDir() {
        loader.setBaseDir(tempDir.toString());
    }

    private void writeSiteFile(String name, String content) throws IOException {
        Files.write(tempDir.resolve(name), content.getBytes(StandardCharsets.UTF_8));
    }

    // ---- empty server_name -------------------------------------------------

    @Test
    public void loadHiveSite_emptyServerNameReturnsEmptyInfo() {
        CatalogInfo info = loader.loadHiveSite(null);
        assertNotNull(info);
        assertNull(info.getServerName());
        assertNull(info.getHiveMetastoreUri());
    }

    @Test
    public void loadS3Site_emptyServerNameReturnsEmptyInfo() {
        VolumeInfo info = loader.loadS3Site("", "s3a://bucket/path");
        assertNotNull(info);
        assertNull(info.getServerName());
        assertNull(info.getVolumeEndpoint());
    }

    @Test
    public void loadHdfsSite_emptyServerNameReturnsEmptyInfo() {
        VolumeInfo info = loader.loadHdfsSite(null);
        assertNotNull(info);
        assertNull(info.getServerName());
    }

    // ---- hive site (keys = SQL OPTION names) -------------------------------

    @Test
    public void loadHiveSite_parsesSqlOptionKeys() throws IOException {
        writeSiteFile("gphive.conf",
                "myhive:\n" +
                "  url: thrift://metastore.example.com:9083\n" +
                "  username: gpadmin\n" +
                "  auth_method: kerberos\n" +
                "  krb_service_principal: hive/metastore@EXAMPLE.COM\n" +
                "  krb_client_principal: user/host@EXAMPLE.COM\n" +
                "  krb_client_keytab: /path/to/user.keytab\n" +
                "  hadoop_rpc_protection: privacy\n");

        CatalogInfo info = loader.loadHiveSite("myhive");
        assertEquals("myhive", info.getServerName());
        assertEquals("thrift://metastore.example.com:9083", info.getHiveMetastoreUri());
        assertEquals("gpadmin", info.getUsername());
        assertEquals("kerberos", info.getAuthMethod());
        assertEquals("hive/metastore@EXAMPLE.COM", info.getKrbServicePrincipal());
        assertEquals("user/host@EXAMPLE.COM", info.getKrbClientPrincipal());
        assertEquals("/path/to/user.keytab", info.getKrbClientKeytab());
        // unknown keys pass through
        assertEquals("privacy", info.getExtraProperties().get("hadoop_rpc_protection"));
        // typed keys are not duplicated into extras
        assertNull(info.getExtraProperties().get("url"));
    }

    @Test
    public void loadHiveSite_legacyUrisKeyFails() throws IOException {
        writeSiteFile("gphive.conf",
                "myhive:\n" +
                "  uris: thrift://metastore.example.com:9083\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadHiveSite("myhive"));
        assertTrue(e.getMessage().contains("\"uris\" (use \"url\")"), e.getMessage());
        assertTrue(e.getMessage().contains("gphive.conf"), e.getMessage());
        assertTrue(e.getMessage().contains("\"myhive\""), e.getMessage());
    }

    // ---- s3 site (keys = SQL OPTION names) ----------------------------------

    @Test
    public void loadS3Site_parsesSqlOptionKeys() throws IOException {
        writeSiteFile("s3.conf",
                "minio_prod:\n" +
                "  type: s3\n" +
                "  endpoint: http://192.168.50.30:9000\n" +
                "  region: us-east-1\n" +
                "  path_style_access: true\n" +
                "  access_key_id: admin\n" +
                "  secret_access_key: password\n" +
                "  fs.defaultFS: s3a://\n");

        // location arrives scheme-less on the wire (see parserBucketAndPrefix)
        VolumeInfo info = loader.loadS3Site("minio_prod", "/warehouse/db/tbl");
        assertEquals("minio_prod", info.getServerName());
        assertEquals("s3", info.getVolumeServerType());
        assertEquals("http://192.168.50.30:9000", info.getVolumeEndpoint());
        assertEquals("us-east-1", info.getVolumeRegion());
        assertEquals(Boolean.TRUE, info.getPathStyleAccess());
        assertEquals("admin", info.getAccessKeyId());
        assertEquals("password", info.getSecretAccessKey());
        // bucket falls back to the location-derived value
        assertEquals("warehouse", info.getBucketName());
        // unknown keys pass through (fs.defaultFS is not a legacy spelling)
        assertEquals("s3a://", info.getExtraProperties().get("fs.defaultFS"));
    }

    @Test
    public void loadS3Site_bucketNameKeyWinsOverLocation() throws IOException {
        writeSiteFile("s3.conf",
                "minio_prod:\n" +
                "  type: s3\n" +
                "  endpoint: http://192.168.50.30:9000\n" +
                "  bucket_name: frombucketkey\n");

        VolumeInfo info = loader.loadS3Site("minio_prod", "s3a://fromlocation/db/tbl");
        assertEquals("frombucketkey", info.getBucketName());
    }

    @Test
    public void loadS3Site_legacyKeysFailAggregated() throws IOException {
        writeSiteFile("s3.conf",
                "old_style:\n" +
                "  fs.s3a.endpoint: http://127.0.0.1:9000\n" +
                "  fs.s3a.access.key: admin\n" +
                "  fs.s3a.secret.key: password\n" +
                "  fs.gopher.ufs_type: s3\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadS3Site("old_style", null));
        String msg = e.getMessage();
        // all offending keys are reported in one error, each with its new name
        assertTrue(msg.contains("\"fs.s3a.endpoint\" (use \"endpoint\")"), msg);
        assertTrue(msg.contains("\"fs.s3a.access.key\" (use \"access_key_id\")"), msg);
        assertTrue(msg.contains("\"fs.s3a.secret.key\" (use \"secret_access_key\")"), msg);
        assertTrue(msg.contains("\"fs.gopher.ufs_type\" (use \"type\")"), msg);
        assertTrue(msg.contains("must match the SQL OPTION names"), msg);
    }

    @Test
    public void loadS3Site_legacyPrefixKeyFails() throws IOException {
        writeSiteFile("s3.conf",
                "old_style:\n" +
                "  type: s3\n" +
                "  fs.s3a.connection.maximum: 100\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadS3Site("old_style", null));
        assertTrue(e.getMessage().contains("\"fs.s3a.connection.maximum\""), e.getMessage());
    }

    // ---- hdfs site (keys = SQL OPTION names) --------------------------------

    @Test
    public void loadHdfsSite_parsesSqlOptionKeys() throws IOException {
        writeSiteFile("gphdfs.conf",
                "paa_cluster:\n" +
                "  hdfs_namenodes: 192.168.1.10\n" +
                "  hdfs_port: 9000\n" +
                "  hdfs_auth_method: kerberos\n" +
                "  krb_principal: gpadmin/master@REALM.COM\n" +
                "  krb_principal_keytab: /home/gpadmin/gpadmin.keytab\n" +
                "  krb_service_principal: hdfs/namenode@REALM.COM\n" +
                "  hadoop_rpc_protection: privacy\n" +
                "  data_transfer_protocol: true\n");

        VolumeInfo info = loader.loadHdfsSite("paa_cluster");
        assertEquals("paa_cluster", info.getServerName());
        assertEquals("hdfs", info.getVolumeServerType());
        assertEquals("192.168.1.10", info.getHdfsNamenodeHost());
        assertEquals("9000", info.getHdfsNamenodePort());
        assertEquals("kerberos", info.getHdfsAuthMethod());
        assertEquals("gpadmin/master@REALM.COM", info.getKrbPrincipal());
        assertEquals("/home/gpadmin/gpadmin.keytab", info.getKrbPrincipalKeytab());
        assertEquals("privacy", info.getHadoopRpcProtection());
        assertEquals("true", info.getDataTransferProtocol());
        // krb_service_principal has no VolumeInfo field; it passes through
        assertEquals("hdfs/namenode@REALM.COM",
                info.getExtraProperties().get("krb_service_principal"));
    }

    @Test
    public void loadHdfsSite_splicedPortWinsOverHdfsPort() throws IOException {
        writeSiteFile("gphdfs.conf",
                "paa_cluster:\n" +
                "  hdfs_namenodes: 192.168.1.10:8020\n" +
                "  hdfs_port: 9000\n");

        VolumeInfo info = loader.loadHdfsSite("paa_cluster");
        assertEquals("192.168.1.10", info.getHdfsNamenodeHost());
        assertEquals("8020", info.getHdfsNamenodePort());
    }

    @Test
    public void loadHdfsSite_parsesHaKeys() throws IOException {
        writeSiteFile("gphdfs.conf",
                "ha_cluster:\n" +
                "  hdfs_namenodes: mycluster\n" +
                "  hdfs_auth_method: simple\n" +
                "  is_ha_supported: true\n" +
                "  dfs_nameservices: mycluster\n" +
                "  dfs_ha_namenodes: nn1,nn2\n" +
                "  dfs_namenode_rpc_address: 192.168.1.10:9000,192.168.1.11:9000\n" +
                "  dfs_client_failover_proxy_provider: custom.ProviderClass\n" +
                "  dfs_client_use_datanode_hostname: true\n");

        VolumeInfo info = loader.loadHdfsSite("ha_cluster");
        assertEquals("mycluster", info.getHdfsNamenodeHost());
        assertNull(info.getHdfsNamenodePort());
        assertEquals(Boolean.TRUE, info.getIsHaSupported());
        assertEquals("mycluster", info.getDfsNameservices());
        assertEquals("nn1,nn2", info.getDfsHaNamenodes());
        assertEquals("192.168.1.10:9000,192.168.1.11:9000", info.getDfsNamenodeRpcAddress());
        assertEquals("custom.ProviderClass", info.getDfsClientFailoverProxyProvider());
        assertEquals(Boolean.TRUE, info.getDfsClientUseDatanodeHostname());
    }

    @Test
    public void loadHdfsSite_legacyKeysFail() throws IOException {
        writeSiteFile("gphdfs.conf",
                "old_cluster:\n" +
                "  hdfs_namenode_host: 192.168.1.10\n" +
                "  hdfs_namenode_port: 9000\n" +
                "  dfs.nameservices: mycluster\n" +
                "  dfs.ha.namenodes: nn1,nn2\n" +
                "  dfs.namenode.rpc-address: 192.168.1.10:9000\n" +
                "  dfs.client.failover.proxy.provider.mycluster: some.Provider\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadHdfsSite("old_cluster"));
        String msg = e.getMessage();
        assertTrue(msg.contains("\"hdfs_namenode_host\" (use \"hdfs_namenodes\")"), msg);
        assertTrue(msg.contains("\"hdfs_namenode_port\" (use \"hdfs_port\")"), msg);
        assertTrue(msg.contains("\"dfs.nameservices\" (use \"dfs_nameservices\")"), msg);
        assertTrue(msg.contains("\"dfs.ha.namenodes\" (use \"dfs_ha_namenodes\")"), msg);
        assertTrue(msg.contains("\"dfs.namenode.rpc-address\" (use \"dfs_namenode_rpc_address\")"), msg);
        assertTrue(msg.contains("\"dfs.client.failover.proxy.provider.mycluster\" (use \"dfs_client_failover_proxy_provider\")"), msg);
    }

    @Test
    public void loadHdfsSite_dottedDfsKeyFails() throws IOException {
        writeSiteFile("gphdfs.conf",
                "old_cluster:\n" +
                "  hdfs_namenodes: 192.168.1.10\n" +
                "  dfs.blocksize: 134217728\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadHdfsSite("old_cluster"));
        assertTrue(e.getMessage().contains("\"dfs.blocksize\""), e.getMessage());
    }

    // ---- missing section -----------------------------------------------------

    @Test
    public void loadS3Site_missingSectionFails() throws IOException {
        writeSiteFile("s3.conf", "other_section:\n  type: s3\n");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> loader.loadS3Site("absent", null));
        assertTrue(e.getMessage().contains("absent"), e.getMessage());
    }

    // ---- failover proxy defaults helper -----------------------------------

    @Test
    public void hdfsFailoverDefaults_addsDefaultProviderWhenAbsent() {
        VolumeInfo volume = new VolumeInfo();
        volume.setDfsNameservices("ns1");
        // no failover proxy provider set
        java.util.Map<String, String> defaults = loader.hdfsFailoverDefaults(volume);
        assertEquals("org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
                defaults.get("dfs.client.failover.proxy.provider.ns1"));
    }

    @Test
    public void hdfsFailoverDefaults_skipsWhenProviderAlreadySet() {
        VolumeInfo volume = new VolumeInfo();
        volume.setDfsNameservices("ns1");
        volume.setDfsClientFailoverProxyProvider("custom.ProviderClass");
        assertNull(loader.hdfsFailoverDefaults(volume).get("dfs.client.failover.proxy.provider.ns1"));
    }

    @Test
    public void hdfsFailoverDefaults_emptyWhenNoNameservices() {
        VolumeInfo volume = new VolumeInfo();
        assertEquals(0, loader.hdfsFailoverDefaults(volume).size());
    }
}
