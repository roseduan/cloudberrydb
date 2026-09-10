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

import cloud.elastic.dlagent.api.configuration.GopherPropertiesResolver;
import cloud.elastic.dlagent.api.configuration.S3FileIOPropertiesTransformer;
import cloud.elastic.dlagent.api.configuration.SiteConfigLoader;
import cloud.elastic.dlagent.api.model.iceberg.CatalogInfo;
import cloud.elastic.dlagent.api.model.iceberg.IcebergRequestConfig;
import cloud.elastic.dlagent.api.model.iceberg.VolumeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IcebergRequestConfigParserTest {

    private SiteConfigLoader siteLoader;
    private GopherPropertiesResolver gopherResolver;
    private S3FileIOPropertiesTransformer s3Transformer;
    private IcebergRequestConfigParser parser;

    @BeforeEach
    public void setUp() {
        siteLoader = Mockito.mock(SiteConfigLoader.class);
        gopherResolver = Mockito.mock(GopherPropertiesResolver.class);
        s3Transformer = Mockito.mock(S3FileIOPropertiesTransformer.class);
        parser = new IcebergRequestConfigParser(siteLoader, gopherResolver, s3Transformer);

        // sensible defaults for unmatched mocks
        when(siteLoader.loadHiveSite(ArgumentMatchers.<String>any())).thenReturn(new CatalogInfo());
        when(siteLoader.loadS3Site(ArgumentMatchers.<String>any(), ArgumentMatchers.<String>any())).thenReturn(new VolumeInfo());
        when(siteLoader.loadHdfsSite(ArgumentMatchers.<String>any())).thenReturn(new VolumeInfo());
        when(gopherResolver.isEnabled()).thenReturn(false);
        when(gopherResolver.build(any(), any())).thenReturn(new HashMap<>());
        when(s3Transformer.build(any())).thenReturn(new HashMap<>());
    }

    @Test
    public void parse_nullRequestReturnsEmptyConfig() {
        IcebergRequestConfig out = parser.parse(null);
        assertNotNull(out);
        assertNotNull(out.getCatalog());
        assertNotNull(out.getVolume());
        assertNotNull(out.getAdditional());
        assertNotNull(out.getGopherRuntime());
    }

    @Test
    public void parse_emptyRequestReturnsEmptyConfig() {
        IcebergRequestConfig out = parser.parse(new HashMap<>());
        assertNull(out.getCatalog().getServerType());
        assertNull(out.getVolume().getVolumeEndpoint());
        assertTrue(out.getFileIOProps().isEmpty());
    }

    // ---- catalog source selection -----------------------------------------

    @Test
    public void parse_catalogFromSqlOptionsWhenNoServerName() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> catalog = new HashMap<>();
        catalog.put("server_type", "hive");
        catalog.put("hive_metastore_uri", "thrift://localhost:9083");
        ic.put("IcebergCatalogConfig", catalog);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("hive", out.getCatalog().getServerType());
        assertEquals("thrift://localhost:9083", out.getCatalog().getHiveMetastoreUri());
        verify(siteLoader, never()).loadHiveSite(any());
    }

    @Test
    public void parse_catalogMergesSiteFileUnderSqlOptions() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> catalog = new HashMap<>();
        catalog.put("server_name", "hivecluster");
        catalog.put("server_type", "hive");
        ic.put("IcebergCatalogConfig", catalog);
        request.put("IcebergConfig", ic);

        CatalogInfo siteResult = new CatalogInfo();
        siteResult.setHiveMetastoreUri("thrift://from-site:9083");
        siteResult.setAuthMethod("kerberos");
        when(siteLoader.loadHiveSite("hivecluster")).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        // keys absent from SQL fall back to the conf section ...
        assertEquals("thrift://from-site:9083", out.getCatalog().getHiveMetastoreUri());
        assertEquals("kerberos", out.getCatalog().getAuthMethod());
        // ... while SQL OPTIONS sibling fields are KEPT (merge, not replace)
        assertEquals("hive", out.getCatalog().getServerType());
        verify(siteLoader, times(1)).loadHiveSite("hivecluster");
    }

    @Test
    public void parse_catalogSqlOptionWinsOverSiteFile() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> catalog = new HashMap<>();
        catalog.put("server_name", "hivecluster");
        catalog.put("hive_metastore_uri", "thrift://from-sql:9083");
        ic.put("IcebergCatalogConfig", catalog);
        request.put("IcebergConfig", ic);

        CatalogInfo siteResult = new CatalogInfo();
        siteResult.setHiveMetastoreUri("thrift://from-site:9083");
        when(siteLoader.loadHiveSite("hivecluster")).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("thrift://from-sql:9083", out.getCatalog().getHiveMetastoreUri());
    }

    // ---- volume source selection ------------------------------------------

    @Test
    public void parse_volumeFromSqlOptionsWhenNoServerName() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("volume_server_type", "s3");
        volume.put("volume_endpoint", "http://minio:9000");
        volume.put("bucket_name", "foo");
        volume.put("access_key_id", "ak");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("s3", out.getVolume().getVolumeServerType());
        assertEquals("http://minio:9000", out.getVolume().getVolumeEndpoint());
        assertEquals("foo", out.getVolume().getBucketName());
        assertEquals("ak", out.getVolume().getAccessKeyId());
        verify(siteLoader, never()).loadS3Site(any(), any());
        verify(siteLoader, never()).loadHdfsSite(any());
    }

    @Test
    public void parse_volumeMergesSiteFileUnderSqlOptions() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "myvolume");
        volume.put("volume_server_type", "s3");
        // a key written in SQL OPTIONS wins over the conf section ...
        volume.put("volume_endpoint", "http://from-sql");
        // ... and SQL-only keys survive conf-file mode (used to be dropped)
        volume.put("base_path", "/warehouse/");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);
        request.put("location", "s3a://mybucket/path");

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setVolumeEndpoint("http://from-site");
        siteResult.setAccessKeyId("site-ak");
        siteResult.setSecretAccessKey("site-sk");
        when(siteLoader.loadS3Site("myvolume", "s3a://mybucket/path")).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("http://from-sql", out.getVolume().getVolumeEndpoint());
        assertEquals("/warehouse/", out.getVolume().getBasePath());
        // keys absent from SQL fall back to the conf section
        assertEquals("site-ak", out.getVolume().getAccessKeyId());
        assertEquals("site-sk", out.getVolume().getSecretAccessKey());
        verify(siteLoader, times(1)).loadS3Site("myvolume", "s3a://mybucket/path");
        verify(siteLoader, never()).loadHdfsSite(any());
    }

    @Test
    public void parse_volumeExplicitFalseBoolOverridesSiteFile() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "myvolume");
        volume.put("volume_server_type", "s3");
        // explicitly false in SQL: must override a true in the conf section
        volume.put("path_style_access", false);
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setPathStyleAccess(true);
        when(siteLoader.loadS3Site(any(), any())).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals(Boolean.FALSE, out.getVolume().getPathStyleAccess());
    }

    @Test
    public void parse_volumeBoolAbsentFallsBackToSiteFile() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "myvolume");
        volume.put("volume_server_type", "s3");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setPathStyleAccess(true);
        when(siteLoader.loadS3Site(any(), any())).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals(Boolean.TRUE, out.getVolume().getPathStyleAccess());
    }

    @Test
    public void parse_volumeHdfsBodyKeysParseAndMerge() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "hdfsvolume");
        volume.put("volume_server_type", "hdfs");
        // wire keys equal the SQL OPTION names; spliced port wins
        volume.put("hdfs_namenodes", "192.168.1.10:8020");
        volume.put("hdfs_port", "9000");
        volume.put("hdfs_auth_method", "kerberos");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setHdfsAuthMethod("simple");
        siteResult.setKrbPrincipal("gpadmin@REALM.COM");
        when(siteLoader.loadHdfsSite("hdfsvolume")).thenReturn(siteResult);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("192.168.1.10", out.getVolume().getHdfsNamenodeHost());
        assertEquals("8020", out.getVolume().getHdfsNamenodePort());
        // SQL wins on conflict, conf fills the gaps
        assertEquals("kerberos", out.getVolume().getHdfsAuthMethod());
        assertEquals("gpadmin@REALM.COM", out.getVolume().getKrbPrincipal());
    }

    @Test
    public void parse_volumeFromHdfsSiteFileWhenServerTypeIsHdfs() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "hdfsvolume");
        volume.put("volume_server_type", "hdfs");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        parser.parse(request);

        verify(siteLoader, times(1)).loadHdfsSite("hdfsvolume");
        verify(siteLoader, never()).loadS3Site(any(), any());
    }

    /**
     * A conf-only HDFS volume must survive the flattening, not just the merge.
     *
     * <p>The test above proves the site file reaches the typed VolumeInfo.  The
     * load-table response is built from {@code toFlatPropertiesMap()}, though,
     * and that emitted none of the HDFS fields: the merged namenode was dropped
     * on the way out, so the segments got a response with nothing in it about a
     * namenode and every read and write failed with "resolved no namenode"
     * (issue #410).  A volume that spells its settings out in SQL hid the bug,
     * because those keys reach the response through the raw request map instead.
     *
     * <p>The expected keys are spelled literally: they are the wire contract the
     * C side parses, so a rename here has to be a deliberate edit to this test.
     */
    @Test
    public void parse_hdfsSiteValuesSurviveFlattening() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        /* Everything else is left in the conf section this names. */
        volume.put("server_name", "hdfsvolume");
        volume.put("volume_server_type", "hdfs");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setHdfsNamenodeHost("hadoop");
        siteResult.setHdfsNamenodePort("8020");
        siteResult.setHdfsAuthMethod("simple");
        siteResult.setHadoopRpcProtection("authentication");
        siteResult.setKrbPrincipal("nn/_HOST@REALM.COM");
        siteResult.setKrbPrincipalKeytab("/etc/security/nn.keytab");
        siteResult.setIsHaSupported(Boolean.TRUE);
        siteResult.setDfsNameservices("ns1");
        siteResult.setDfsClientUseDatanodeHostname(Boolean.TRUE);
        when(siteLoader.loadHdfsSite("hdfsvolume")).thenReturn(siteResult);

        Map<String, String> flat = parser.parse(request).toFlatPropertiesMap();

        /* The namenode is the load-bearing one: without it the segments fail. */
        assertEquals("hadoop", flat.get("IcebergVolumeConfig.hdfs_namenodes"));
        assertEquals("8020", flat.get("IcebergVolumeConfig.hdfs_port"));
        assertEquals("simple", flat.get("IcebergVolumeConfig.hdfs_auth_method"));
        assertEquals("authentication",
                     flat.get("IcebergVolumeConfig.hadoop_rpc_protection"));
        /* Auth and HA travel with it: a secured or HA cluster is unreachable
         * without them and they are equally absent from the SQL OPTIONS. */
        assertEquals("nn/_HOST@REALM.COM",
                     flat.get("IcebergVolumeConfig.krb_principal"));
        assertEquals("/etc/security/nn.keytab",
                     flat.get("IcebergVolumeConfig.krb_principal_keytab"));
        assertEquals("true", flat.get("IcebergVolumeConfig.is_ha_supported"));
        assertEquals("ns1", flat.get("IcebergVolumeConfig.dfs_nameservices"));
        assertEquals("true",
                     flat.get("IcebergVolumeConfig.dfs_client_use_datanode_hostname"));
    }

    /**
     * The port arrives on its own key even when the conf section spliced it
     * into the namenode value, because that is the pair the C side's
     * setGopherNamenode expects.
     */
    @Test
    public void parse_hdfsSplicedPortFlattensAsSeparateKey() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "hdfsvolume");
        volume.put("volume_server_type", "hdfs");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.applyHdfsNamenodes("nn.example.com:9820", null);
        when(siteLoader.loadHdfsSite("hdfsvolume")).thenReturn(siteResult);

        Map<String, String> flat = parser.parse(request).toFlatPropertiesMap();

        assertEquals("nn.example.com",
                     flat.get("IcebergVolumeConfig.hdfs_namenodes"));
        assertEquals("9820", flat.get("IcebergVolumeConfig.hdfs_port"));
    }

    /**
     * An S3 volume gains no HDFS keys.  The flattener writes only what the
     * typed object holds, so an absent field must stay absent rather than
     * arrive as an empty string the C side would then treat as set.
     */
    @Test
    public void parse_s3VolumeEmitsNoHdfsKeys() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("volume_server_type", "s3");
        volume.put("volume_endpoint", "http://minio:9000");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        Map<String, String> flat = parser.parse(request).toFlatPropertiesMap();

        assertEquals("http://minio:9000",
                     flat.get("IcebergVolumeConfig.volume_endpoint"));
        assertNull(flat.get("IcebergVolumeConfig.hdfs_namenodes"));
        assertNull(flat.get("IcebergVolumeConfig.hdfs_port"));
        assertNull(flat.get("IcebergVolumeConfig.hdfs_auth_method"));
    }

    /**
     * Both sides set the same S3 keys: SQL must win, per key, and the win must
     * survive into the flat map the load-table response is built from.
     *
     * <p>This is the case the review asked for and it belongs here rather than
     * in a smoke suite.  End to end it cannot be asserted at all: when a volume
     * names a section, {@code buildDataConfig} reads the whole DATA domain from
     * that section and ignores the inline options, so the agent's own FileIO
     * uses the conf value while the segments use the SQL one.  Staging a
     * section that disagrees therefore breaks the coordinator before the
     * segments are reached -- verified, it fails in
     * {@code GopherFileIO.initialize}.  The data plane's precedence is decided
     * here, in one place, and this is where it can be pinned.
     */
    @Test
    public void parse_volumeSqlOptionsWinPerKeyOverSiteFile() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "s3volume");
        volume.put("volume_server_type", "s3");
        volume.put("volume_endpoint", "http://minio:9000");
        volume.put("volume_region", "us-east-1");
        volume.put("bucket_name", "warehouse");
        volume.put("path_style_access", "true");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        /* The section disagrees on every one of those keys. */
        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setVolumeEndpoint("http://127.0.0.1:1");
        siteResult.setVolumeRegion("nonexistent-region");
        siteResult.setBucketName("nonexistent-bucket");
        siteResult.setPathStyleAccess(Boolean.FALSE);
        /* ...and supplies one key SQL left out, which must still come through. */
        siteResult.setSecretAccessKey("from-conf");
        when(siteLoader.loadS3Site(eq("s3volume"), any())).thenReturn(siteResult);

        Map<String, String> flat = parser.parse(request).toFlatPropertiesMap();

        assertEquals("http://minio:9000",
                     flat.get("IcebergVolumeConfig.volume_endpoint"));
        assertEquals("us-east-1", flat.get("IcebergVolumeConfig.volume_region"));
        assertEquals("warehouse", flat.get("IcebergVolumeConfig.bucket_name"));
        assertEquals("true", flat.get("IcebergVolumeConfig.path_style_access"));
        /* The gap-fill direction, in the same request as the overrides. */
        assertEquals("from-conf",
                     flat.get("IcebergVolumeConfig.secret_access_key"));
    }

    /**
     * path_style_access deserves its own case: it is a Boolean, so "unset" and
     * "false" are the same absence once it reaches C, and an explicit false is
     * what an operator writes to ask for virtual-host addressing.  A section
     * saying true must not turn that back on.
     */
    @Test
    public void parse_explicitFalsePathStyleSurvivesASiteFileSayingTrue() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("server_name", "s3volume");
        volume.put("volume_server_type", "s3");
        volume.put("path_style_access", "false");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        VolumeInfo siteResult = new VolumeInfo();
        siteResult.setPathStyleAccess(Boolean.TRUE);
        when(siteLoader.loadS3Site(eq("s3volume"), any())).thenReturn(siteResult);

        Map<String, String> flat = parser.parse(request).toFlatPropertiesMap();

        assertEquals("false", flat.get("IcebergVolumeConfig.path_style_access"));
    }

    // ---- gopher.enabled branch -------------------------------------------

    @Test
    public void parse_gopherEnabledRoutesToGopherResolver() {
        when(gopherResolver.isEnabled()).thenReturn(true);
        Map<String, String> gopherOut = new HashMap<>();
        gopherOut.put("gopher.endpoint", "minio:9000");
        when(gopherResolver.build(any(), any())).thenReturn(gopherOut);

        IcebergRequestConfig out = parser.parse(new HashMap<>());

        assertEquals("minio:9000", out.getFileIOProps().get("gopher.endpoint"));
        verify(gopherResolver, times(1)).build(any(), any());
        verify(s3Transformer, never()).build(any());
    }

    @Test
    public void parse_gopherDisabledRoutesToS3Transformer() {
        when(gopherResolver.isEnabled()).thenReturn(false);
        Map<String, String> s3Out = new HashMap<>();
        s3Out.put("s3.endpoint", "http://minio:9000");
        when(s3Transformer.build(any())).thenReturn(s3Out);

        IcebergRequestConfig out = parser.parse(new HashMap<>());

        assertEquals("http://minio:9000", out.getFileIOProps().get("s3.endpoint"));
        verify(s3Transformer, times(1)).build(any());
        verify(gopherResolver, never()).build(any(), any());
    }

    // ---- gopher runtime: new flat shape ----------------------------------

    @Test
    public void parse_gopherRuntimeFromNewFlatShape() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new LinkedHashMap<>();
        Map<String, Object> additional = new LinkedHashMap<>();
        Map<String, Object> fileIO = new LinkedHashMap<>();
        Map<String, Object> gopherConfig = new LinkedHashMap<>();
        Map<String, Object> common = new LinkedHashMap<>();
        common.put("worker_path", "/req/wp");
        common.put("connect_path", "/req/cp");
        common.put("log_level", "GOPHER_INFO");
        gopherConfig.put("common", common);
        fileIO.put("gopherConfig", gopherConfig);
        additional.put("fileIOConfig", fileIO);
        ic.put("IcebergAdditionalConfig", additional);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("/req/wp", out.getGopherRuntime().getWorkerPath());
        assertEquals("/req/cp", out.getGopherRuntime().getConnectPath());
        assertEquals("GOPHER_INFO", out.getGopherRuntime().getLogLevel());
    }

    // ---- gopher runtime: legacy wrapper shape ----------------------------

    @Test
    public void parse_gopherRuntimeFromLegacyWrapperShape() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new LinkedHashMap<>();
        Map<String, Object> additional = new LinkedHashMap<>();
        Map<String, Object> fileIO = new LinkedHashMap<>();
        Map<String, Object> gopherFileIO = new LinkedHashMap<>();
        Map<String, Object> gopherConfig = new LinkedHashMap<>();
        Map<String, Object> common = new LinkedHashMap<>();
        common.put("worker_path", "/legacy/wp");
        gopherConfig.put("common", common);
        gopherFileIO.put("gopherConfig", gopherConfig);
        fileIO.put("gopherFileIOConfig", gopherFileIO);
        additional.put("fileIOConfig", fileIO);
        ic.put("IcebergAdditionalConfig", additional);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("/legacy/wp", out.getGopherRuntime().getWorkerPath());
    }

    // ---- additional config ------------------------------------------------

    @Test
    public void parse_additionalSectionPopulatesTypedFields() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> additional = new HashMap<>();
        additional.put("totalSegment", "3");
        additional.put("splitSize", "67108864");
        additional.put("filterString", "id > 100");
        additional.put("tableIdentifier", "ns.table");
        ic.put("IcebergAdditionalConfig", additional);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("3", out.getAdditional().getTotalSegment());
        assertEquals("67108864", out.getAdditional().getSplitSize());
        assertEquals("id > 100", out.getAdditional().getFilterString());
        assertEquals("ns.table", out.getAdditional().getTableIdentifier());
    }

    // ---- user table properties --------------------------------------------

    @Test
    public void parseUserTableProperties_filtersOutInternalKeys() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put("write.format.default", "parquet");
        props.put("gopher.connect_path", "should-be-filtered");
        props.put("IcebergCatalogConfig.server_type", "should-be-filtered");
        props.put("buildInCatalog.table_exists", "should-be-filtered");
        request.put("properties", props);

        Map<String, String> out = parser.parseUserTableProperties(request);

        assertEquals("parquet", out.get("write.format.default"));
        assertNull(out.get("gopher.connect_path"));
        assertNull(out.get("IcebergCatalogConfig.server_type"));
        assertNull(out.get("buildInCatalog.table_exists"));
    }

    @Test
    public void parseUserTableProperties_handlesNullOrMissingProperties() {
        assertEquals(0, parser.parseUserTableProperties(null).size());
        assertEquals(0, parser.parseUserTableProperties(new HashMap<>()).size());
        Map<String, Object> request = new HashMap<>();
        request.put("properties", null);
        assertEquals(0, parser.parseUserTableProperties(request).size());
    }

    // ---- config_files / version fields -----------------------------------

    @Test
    public void parse_carriesLegacyTopLevelFields() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        ic.put("config_files", "s3.conf");
        ic.put("iceberg_config_version", "1");
        ic.put("set_catalog_default_impl", "true");
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("s3.conf", out.getConfigFiles());
        assertEquals("1", out.getIcebergConfigVersion());
        assertEquals("true", out.getSetCatalogDefaultImpl());
    }

    @Test
    public void parse_passesEqualVolumeInfoToResolverAndTransformer() {
        when(gopherResolver.isEnabled()).thenReturn(true);
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> volume = new HashMap<>();
        volume.put("volume_server_type", "s3");
        volume.put("volume_endpoint", "http://probe");
        ic.put("IcebergVolumeConfig", volume);
        request.put("IcebergConfig", ic);

        parser.parse(request);

        verify(gopherResolver).build(
                ArgumentMatchers.argThat(v -> v != null && "http://probe".equals(v.getVolumeEndpoint())),
                any());
    }

    @Test
    public void parse_emptyStringServerNameTreatedAsAbsent() {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> ic = new HashMap<>();
        Map<String, Object> catalog = new HashMap<>();
        catalog.put("server_name", "");
        catalog.put("server_type", "hive");
        ic.put("IcebergCatalogConfig", catalog);
        request.put("IcebergConfig", ic);

        IcebergRequestConfig out = parser.parse(request);

        assertEquals("hive", out.getCatalog().getServerType());
        verify(siteLoader, never()).loadHiveSite(any());
    }

    @Test
    public void parse_unrelatedTopLevelKeysIgnored() {
        Map<String, Object> request = new HashMap<>();
        request.put("namespace", "foo");
        request.put("name", "bar");
        request.put("schema", Collections.emptyMap());
        IcebergRequestConfig out = parser.parse(request);
        assertNotNull(out);
    }
}
