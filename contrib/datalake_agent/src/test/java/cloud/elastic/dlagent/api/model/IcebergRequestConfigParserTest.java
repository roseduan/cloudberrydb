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
