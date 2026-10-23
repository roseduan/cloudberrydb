/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import cloud.elastic.dlagent.service.ServiceResult;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The REST path and the gRPC path reach the same bytes through two different
 * pieces of code: Spring's MappingJackson2HttpMessageConverter writes the
 * response for REST, while {@link JsonResponseWriter#writeBody} does it for
 * gRPC. They share one ObjectMapper instance (see
 * {@code DlConfiguration.objectMapper}); this test checks the remaining
 * question, which is whether those two code paths actually emit the same bytes
 * through it.
 *
 * <p>It exists because the design originally called for REST to be rewritten
 * to hand pre-serialized {@code byte[]} to Spring, precisely so there would be
 * one code path. Leaving REST's serialization untouched is the lower-risk
 * choice -- REST is the rollback path and may not drift -- but it only holds
 * if the converter adds nothing of its own. That is what is asserted here, on the body shapes the agent actually returns.
 */
public class SpringConverterByteEquivalenceTest {

    private final JsonResponseWriter writer = new JsonResponseWriter();
    private final MappingJackson2HttpMessageConverter converter =
            new MappingJackson2HttpMessageConverter(writer.objectMapper());

    private void assertSameBytes(Object body) throws Exception {
        MockHttpOutputMessage out = new MockHttpOutputMessage();
        converter.write(body, MediaType.APPLICATION_JSON, out);
        assertArrayEquals(writer.writeBody(body), out.getBodyAsBytes(),
                "Spring's converter and JsonResponseWriter disagreed for " + body
                        + " -- REST and gRPC would put different bytes on the wire. "
                        + "converter=" + out.getBodyAsString()
                        + " writer=" + new String(writer.writeBody(body), "UTF-8"));
    }

    @Test
    public void errorBodiesAreIdentical() throws Exception {
        // The shape every failed operation returns, from all five factories.
        assertSameBytes(ServiceResult.failure("boom", "BadRequestException", 400).getErrorResponse());
        assertSameBytes(ServiceResult.notFound("table", "db.t1").getErrorResponse());
        assertSameBytes(ServiceResult.conflict("table", "db.t1").getErrorResponse());
        assertSameBytes(ServiceResult.error("loadTable", "no such file").getErrorResponse());
        assertSameBytes(ServiceResult.badRequest("missing namespace").getErrorResponse());
    }

    @Test
    public void metadataShapedBodiesAreIdentical() throws Exception {
        // Nested maps and lists, the shape convertTableToMetadata produces.
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("id", 1);
        field.put("name", "c1");
        field.put("required", false);
        field.put("type", "long");

        List<Object> fields = new ArrayList<>();
        fields.add(field);

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "struct");
        schema.put("schema-id", 0);
        schema.put("fields", fields);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("format-version", 2);
        metadata.put("table-uuid", "b3a1f0e2-0000-4000-8000-000000000001");
        metadata.put("location", "s3a://bucket/db/t1");
        metadata.put("last-updated-ms", 1789109364432L);
        metadata.put("current-snapshot-id", 7146290312398765432L);
        metadata.put("schema", schema);
        metadata.put("properties", new HashMap<String, String>());
        metadata.put("snapshots", new ArrayList<>());
        assertSameBytes(metadata);
    }

    @Test
    public void nullsAndEmptiesAreIdentical() throws Exception {
        // The default property inclusion is USE_DEFAULTS, i.e. nulls ARE
        // written. If either path ever started dropping them the C parsers
        // would see a different document, so pin it here rather than trusting
        // the mapper config alone.
        Map<String, Object> withNulls = new LinkedHashMap<>();
        withNulls.put("present", "v");
        withNulls.put("absent", null);
        withNulls.put("emptyList", new ArrayList<>());
        withNulls.put("emptyMap", new HashMap<String, String>());
        assertSameBytes(withNulls);

        MockHttpOutputMessage out = new MockHttpOutputMessage();
        converter.write(withNulls, MediaType.APPLICATION_JSON, out);
        assertEquals("{\"present\":\"v\",\"absent\":null,\"emptyList\":[],\"emptyMap\":{}}",
                out.getBodyAsString(),
                "nulls or empty collections stopped being serialized");
    }

    @Test
    public void listBodiesAreIdentical() throws Exception {
        // listCatalogs / listNamespaces / planFileGroups return bare lists.
        assertSameBytes(Arrays.asList("a", "b"));
        assertSameBytes(new ArrayList<>());
    }

    /**
     * Pre-serialized bodies must NOT go through Jackson: a {@code byte[]} would
     * come back Base64-encoded and a {@code String} quote-wrapped and escaped.
     * {@code loadMetadataJson} returns {@code byte[] doc}, so this is a real
     * path, and both corruptions would be invisible to a test that only checks
     * the HTTP status.
     */
    @Test
    public void preSerializedBodiesPassThroughUntouched() {
        byte[] doc = "{\"format-version\":2}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertArrayEquals(doc, writer.writeBody(doc),
                "a byte[] body was re-serialized instead of passed through");

        String json = "{\"a\":1}";
        assertArrayEquals(json.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                writer.writeBody(json),
                "a String body was re-serialized instead of passed through");
    }

    /** A successful result with no data sends no body at all, not "null". */
    @Test
    public void absentBodyIsZeroBytes() {
        assertArrayEquals(new byte[0], writer.write(ServiceResult.success(null)),
                "an empty success body must be zero bytes, not the four bytes null");
    }
}
