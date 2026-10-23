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

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.api.model.FragmentDescription;
import cloud.elastic.dlagent.plugins.iceberg.utilities.SensitiveDataRedactor;
import cloud.elastic.dlagent.service.iceberg.IcebergOperations;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestCatalogEnvelopeTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void fragmentSerialization_hasSiblingEnvelopeForEmptyTable() throws Exception {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("s3.access-key-id", "vended-ak");
        config.put("s3.secret-access-key", "vended-sk");
        config.put("s3.session-token", "vended-token");
        IcebergRestCatalog.StageOneResult stage = new IcebergRestCatalog.StageOneResult(
                "s3://bucket/table/metadata/v1.metadata.json",
                "s3://bucket/table/", config, null, null);
        FragmentDescription description = new FragmentDescription(
                null, Collections.emptyList(), -1L);
        description.setCatalogProperties(stage.envelope(new Configuration(false)));

        JsonNode json = mapper.readTree(mapper.writeValueAsString(description));
        assertTrue(json.get("combinedTasks").isArray());
        assertEquals(0, json.get("combinedTasks").size());
        JsonNode envelope = json.get("catalog-properties");
        assertEquals(1, envelope.get("envelope-version").asInt());
        assertEquals("s3://bucket/table/", envelope.get("table-location").asText());
        assertEquals("s3://bucket/table/metadata/v1.metadata.json",
                envelope.get("metadata-location").asText());
        assertEquals("vended-token", envelope.get("config")
                .get("s3.session-token").asText());
    }

    @Test
    public void fragmentSerialization_omitsEnvelopeForPolarisCompatibility() throws Exception {
        FragmentDescription description = new FragmentDescription(
                null, Collections.emptyList(), -1L);
        JsonNode json = mapper.readTree(mapper.writeValueAsString(description));
        assertFalse(json.has("catalog-properties"));
    }

    @Test
    public void loadTableResponse_carriesWholeEnvelopeForTheWritePath() throws Exception {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("s3.access-key-id", "vended-ak");
        config.put("s3.secret-access-key", "vended-sk");
        IcebergRestCatalog.StageOneResult stage = new IcebergRestCatalog.StageOneResult(
                "s3://bucket/table/metadata/v3.metadata.json",
                "s3://bucket/table/", config, null, null);
        Map<String, Object> envelope = stage.envelope(new Configuration(false));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("table-location", "s3://stale/from-table-object/");
        IcebergOperations.putRestEnvelope(response, envelope);

        // The whole envelope, in the shape the segment parses -- an INSERT has
        // no fragments response to take it from.
        JsonNode json = mapper.readTree(mapper.writeValueAsString(response));
        JsonNode carried = json.get("catalog-properties");
        assertNotNull(carried);
        assertEquals(1, carried.get("envelope-version").asInt());
        assertEquals("s3://bucket/table/", carried.get("table-location").asText());
        assertEquals("s3://bucket/table/metadata/v3.metadata.json",
                carried.get("metadata-location").asText());
        assertEquals("vended-ak", carried.get("config").get("s3.access-key-id").asText());

        // ... and flattened, which is what the load-table parser reads. The
        // catalog's own locations win over whatever the caller had put there.
        assertEquals("s3://bucket/table/", json.get("table-location").asText());
        assertEquals(1, json.get("envelope-version").asInt());
    }

    @Test
    public void loadTableResponse_untouchedWhenNoEnvelopeWasVended() throws Exception {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("table-location", "s3://polaris/warehouse/db/t/");

        IcebergOperations.putRestEnvelope(response, null);

        JsonNode json = mapper.readTree(mapper.writeValueAsString(response));
        assertFalse(json.has("catalog-properties"));
        assertFalse(json.has("envelope-version"));
        assertEquals("s3://polaris/warehouse/db/t/", json.get("table-location").asText());
    }

    @Test
    public void recursiveLogRedactionMasksNestedTokenAndCredentialValues() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("storage-credentials", Arrays.asList(Collections.singletonMap(
                "config", Collections.singletonMap("s3.session-token", "TOKEN-PLAINTEXT"))));
        nested.put("client_credential", "CLIENT-PLAINTEXT");

        String rendered = String.valueOf(SensitiveDataRedactor.redact(nested));
        assertFalse(rendered.contains("TOKEN-PLAINTEXT"));
        assertFalse(rendered.contains("CLIENT-PLAINTEXT"));
        assertTrue(rendered.contains("**"));
    }

    @Test
    public void contractFixtures_areValidAndKeepFrozenKeys() throws Exception {
        for (String fixture : Arrays.asList(
                "envelope-fixtures/success.json",
                "envelope-fixtures/empty-table.json")) {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(fixture)) {
                assertNotNull(input, fixture);
                JsonNode root = mapper.readTree(input);
                JsonNode envelope = root.get("catalog-properties");
                assertEquals(1, envelope.get("envelope-version").asInt());
                assertTrue(envelope.hasNonNull("table-location"));
                assertTrue(envelope.hasNonNull("metadata-location"));
                assertTrue(root.get("combinedTasks").isArray());
            }
        }
    }
}
