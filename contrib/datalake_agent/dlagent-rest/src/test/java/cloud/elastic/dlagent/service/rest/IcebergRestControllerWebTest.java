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

import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.configuration.GopherPropertiesResolver;
import cloud.elastic.dlagent.api.configuration.S3FileIOPropertiesTransformer;
import cloud.elastic.dlagent.api.configuration.SiteConfigLoader;
import cloud.elastic.dlagent.api.model.BaseConfigurationFactory;
import cloud.elastic.dlagent.api.model.IcebergRequestConfigParser;
import cloud.elastic.dlagent.service.iceberg.IcebergService;
import cloud.elastic.dlagent.service.iceberg.SchemaConverter;
import cloud.elastic.dlagent.service.spring.DlContextMdcLogEnhancerFilter;
import cloud.elastic.dlagent.service.spring.IcebergCatalogReleaseFilter;
import cloud.elastic.dlagent.service.iceberg.IcebergConfigurationBuilder;
import cloud.elastic.dlagent.service.iceberg.IcebergMetadataMapper;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;
import cloud.elastic.dlagent.service.iceberg.IcebergRequestContextFactory;
import cloud.elastic.dlagent.service.iceberg.MetadataFileCleaner;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Controller-level tests for {@link IcebergRestController}: routing, request
 * validation, response shaping and exception mapping, with the service layer
 * stubbed.
 *
 * <p>These exist because the pre-existing {@code *IntegrationTest} classes
 * could not do this job. They post to a live agent over HTTP, so they need a
 * hive metastore, a minio and a gopher store to be up; where those are absent
 * every request fails, and the assertions there were written loosely enough to
 * treat that as success. The result was a suite that stayed green whatever the
 * service did -- it had never once resolved a route, because the URLs it used
 * carried a {@code /namespace/{ns}/} segment the controller has never had (the
 * commit that introduced both, 6d7a7298dca, already mapped
 * {@code /{prefix}/tables/{table}/...}).
 *
 * <p>Everything asserted here needs no infrastructure, so it runs on any
 * machine and in CI. What genuinely needs the stack stays in the integration
 * classes, guarded so it skips rather than fails when the stack is absent.
 */
/*
 * Servlet filters are left out: the MDC and catalog-release filters carry
 * their own dependency graphs and neither participates in what is under test
 * here, which is the controller's routing and contract.  They have to be
 * excluded from the scan, not merely disabled with addFilters=false -- a
 * @WebMvcTest slice instantiates Filter beans regardless of whether it invokes
 * them.
 */
@WebMvcTest(controllers = IcebergRestController.class,
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.ASSIGNABLE_TYPE,
                classes = {DlContextMdcLogEnhancerFilter.class,
                        IcebergCatalogReleaseFilter.class}))
@AutoConfigureMockMvc(addFilters = false)
/*
 * The controller's collaborators are imported for real rather than mocked --
 * request parsing is precisely what a controller test should cover. Only the
 * service is stubbed, because that is the layer that would reach a catalog.
 */
@Import({IcebergRequestConfigParser.class, SchemaConverter.class,
        BaseConfigurationFactory.class,
        GopherPropertiesResolver.class, SiteConfigLoader.class,
        S3FileIOPropertiesTransformer.class,
        /*
         * The transport-neutral half of the controller, which owns request
         * parsing since the module split; only IcebergService beneath it is
         * stubbed, so this still tests what main wrote it to test.
         */
        IcebergOperations.class, IcebergRequestContextFactory.class,
        IcebergConfigurationBuilder.class, IcebergMetadataMapper.class,
        MetadataFileCleaner.class, JsonResponseWriter.class})
public class IcebergRestControllerWebTest {

    private static final String NAMESPACE = "default";
    private static final String TABLE = "web_test_table";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private IcebergService icebergService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The smallest body the controller accepts: a namespace, plus the config
     * envelope the request parser expects. Deliberately not the sprawling
     * fixture the integration tests build -- nothing here reaches storage, so
     * endpoints and credentials would only be decoration.
     */
    private String body(Map<String, Object> extra) throws Exception {
        Map<String, Object> catalogConfig = new LinkedHashMap<>();
        catalogConfig.put("server_type", "builtin");

        Map<String, Object> icebergConfig = new LinkedHashMap<>();
        icebergConfig.put("IcebergCatalogConfig", catalogConfig);

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("namespace", NAMESPACE);
        request.put("IcebergConfig", icebergConfig);
        if (extra != null) {
            request.putAll(extra);
        }
        return objectMapper.writeValueAsString(request);
    }

    private Map<String, Object> single(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    /* ---------------------------------------------------------------- routes */

    /**
     * The route the datalake_fdw client actually posts to. A 404 here means
     * the mapping moved and every consumer is broken -- which is exactly the
     * failure the old integration tests absorbed silently.
     */
    @Test
    public void tableExists_resolvesWithoutPrefix() throws Exception {
        when(icebergService.checkTableExists(eq(NAMESPACE), eq(TABLE), any(), any()))
                .thenReturn(true);

        mockMvc.perform(post("/api/v1/tables/{table}/exists", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(null)))
                .andExpect(status().isOk());
    }

    @Test
    public void tableExists_resolvesWithPrefix() throws Exception {
        when(icebergService.checkTableExists(eq(NAMESPACE), eq(TABLE), any(), any()))
                .thenReturn(true);

        mockMvc.perform(post("/api/v1/{prefix}/tables/{table}/exists", "cat1", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(null)))
                .andExpect(status().isOk());
    }

    /**
     * renameTable is unsupported at the catalog level and is not exposed over
     * REST at all. If a route is ever added, this fails and whoever added it
     * gets to write the real assertion.
     */
    @Test
    public void renameTable_hasNoRoute() throws Exception {
        mockMvc.perform(post("/api/v1/tables/{table}/rename", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(single("newName", "renamed"))))
                .andExpect(status().isNotFound());
    }

    /* ------------------------------------------------------------ validation */

    @Test
    public void tableExists_withoutNamespace_isRejected() throws Exception {
        String noNamespace = objectMapper.writeValueAsString(
                single("IcebergConfig", new LinkedHashMap<String, Object>()));

        mockMvc.perform(post("/api/v1/tables/{table}/exists", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(noNamespace))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.type").value("BadRequestException"));
    }

    @Test
    public void dropTable_withoutNamespace_isRejected() throws Exception {
        String noNamespace = objectMapper.writeValueAsString(
                single("IcebergConfig", new LinkedHashMap<String, Object>()));

        mockMvc.perform(post("/api/v1/tables/{table}/drop", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(noNamespace))
                .andExpect(status().isBadRequest());
    }

    /* -------------------------------------------------------------- responses */

    @Test
    public void dropTable_returnsNoContentOnSuccess() throws Exception {
        when(icebergService.dropTable(eq(NAMESPACE), eq(TABLE), anyBoolean(), any(), any()))
                .thenReturn(true);

        mockMvc.perform(post("/api/v1/tables/{table}/drop", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(single("purgeRequested", true))))
                .andExpect(status().isNoContent());
    }

    @Test
    public void getFragment_returnsServiceBody() throws Exception {
        String fragment = "{\"fragments\":[]}";
        when(icebergService.getTableFragment(eq(NAMESPACE), eq(TABLE), any(), any()))
                .thenReturn(fragment);

        mockMvc.perform(post("/api/v1/tables/{table}/getFragment", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(null)))
                .andExpect(status().isOk())
                .andExpect(content().string(fragment));
    }

    /* -------------------------------------------------------- error mapping */

    /**
     * The catalog's "no such table" has to reach the client as 404 rather than
     * as a 500 with a Java stack trace: datalake_fdw branches on it to decide
     * whether to create the table.
     */
    @Test
    public void noSuchTable_mapsToNotFound() throws Exception {
        when(icebergService.getTableFragment(eq(NAMESPACE), eq(TABLE), any(), any()))
                .thenThrow(new NoSuchTableException("Table does not exist: %s.%s", NAMESPACE, TABLE));

        mockMvc.perform(post("/api/v1/tables/{table}/getFragment", TABLE)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(null)))
                .andExpect(status().isNotFound());
    }
}
