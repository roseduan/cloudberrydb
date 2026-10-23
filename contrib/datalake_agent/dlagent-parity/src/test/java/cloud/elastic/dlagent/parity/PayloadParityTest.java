/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.service.ServiceResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the two transports make of an awkward request body, and of an awkward
 * response.
 *
 * <p>The request travels as an HTTP entity on one side and as the {@code bytes
 * payload} of a one-field protobuf message on the other, so "the same JSON"
 * has two very different journeys. These are the values where a difference
 * would be silent rather than loud.
 */
class PayloadParityTest {

    private static final Op OP = Op.LOAD_TABLE;

    /** Sends one body through both transports and returns what the operation saw. */
    private static void bothSee(String requestJson,
                                java.util.function.BiConsumer<RecordingOperations.Call,
                                                              RecordingOperations.Call> check)
            throws Exception {
        try (TransportHarness harness = new TransportHarness(() -> ServiceResult.success(null))) {
            harness.rest(OP, "cat", "t1", requestJson);
            RecordingOperations.Call rest = harness.operations().lastCall();

            harness.operations().reset();
            harness.grpc(OP, "cat", "t1", requestJson);
            RecordingOperations.Call grpc = harness.operations().lastCall();

            assertNotNull(rest, "the REST handler never called the operation");
            assertNotNull(grpc, "the gRPC handler never called the operation");
            check.accept(rest, grpc);
        }
    }

    private static void sameRequest(String requestJson) throws Exception {
        bothSee(requestJson, (rest, grpc) ->
                assertEquals(String.valueOf(rest.request), String.valueOf(grpc.request),
                        "the two transports parsed " + requestJson + " differently"));
    }

    @Test
    @DisplayName("an empty object")
    void emptyObject() throws Exception {
        sameRequest("{}");
    }

    @Test
    @DisplayName("a 64-bit snapshot id keeps every digit")
    void int64Precision() throws Exception {
        // 2^63-1 and a realistic snapshot id. A double would round both.
        String json = "{\"snapshotId\":9223372036854775807,\"other\":7264871234567890123}";
        bothSee(json, (rest, grpc) -> {
            assertEquals(String.valueOf(rest.request), String.valueOf(grpc.request));
            Object snapshot = rest.request.get("snapshotId");
            assertEquals(new BigInteger("9223372036854775807").longValue(),
                    ((Number) snapshot).longValue(),
                    "the snapshot id lost precision: " + snapshot);
        });
    }

    @Test
    @DisplayName("UTF-8: Chinese, an emoji and a surrogate pair")
    void utf8() throws Exception {
        sameRequest("{\"table\":\"数据表\",\"note\":\"🚀 ok\"}");
    }

    @Test
    @DisplayName("a null value is a null, not a missing key")
    void explicitNull() throws Exception {
        bothSee("{\"maybe\":null,\"present\":1}", (rest, grpc) -> {
            assertEquals(String.valueOf(rest.request), String.valueOf(grpc.request));
            assertTrue(rest.request.containsKey("maybe"),
                    "an explicit null must survive as a key: " + rest.request);
        });
    }

    @Test
    @DisplayName("numbers keep their type: int, long, double, boolean")
    void numberTypes() throws Exception {
        bothSee("{\"i\":7,\"l\":4294967296,\"d\":1.5,\"b\":true}", (rest, grpc) -> {
            assertEquals(String.valueOf(rest.request), String.valueOf(grpc.request));
            assertEquals(Integer.class, rest.request.get("i").getClass());
            assertEquals(Long.class, rest.request.get("l").getClass());
            assertEquals(Double.class, rest.request.get("d").getClass());
            assertEquals(Boolean.class, rest.request.get("b").getClass());
        });
    }

    @Test
    @DisplayName("nested objects and arrays")
    void nested() throws Exception {
        sameRequest("{\"a\":{\"b\":{\"c\":[1,2,{\"d\":\"e\"}]}},\"f\":[]}");
    }

    @Test
    @DisplayName("a body larger than gRPC's default 4MB message limit")
    void largeBody() throws Exception {
        StringBuilder big = new StringBuilder("{\"blob\":\"");
        for (int i = 0; i < 5 * 1024 * 1024 / 8; i++) {
            big.append("abcdefgh");
        }
        big.append("\"}");
        sameRequest(big.toString());
    }

    @Test
    @DisplayName("a response body with the same awkward values comes back identical")
    void awkwardResponse() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("snapshotId", 9223372036854775807L);
        body.put("name", "中文");
        body.put("empty", "");
        body.put("nil", null);
        body.put("nested", new LinkedHashMap<>(body));

        try (TransportHarness harness = new TransportHarness(() -> ServiceResult.success(body))) {
            ParityAssert.sameAnswer("awkward response",
                    harness.rest(OP, "cat", "t1", "{}"),
                    harness.grpc(OP, "cat", "t1", "{}"));
        }
    }

    /**
     * A body {@code @RequestBody Map} cannot bind -- missing, the literal
     * {@code null}, or not JSON -- is answered by REST through the controller
     * advice with the 500 error document. gRPC must answer with the same
     * status and the same document.
     *
     * <p>The one field that cannot match is {@code error.stack}: on the REST
     * side it is Spring's {@code HttpMessageNotReadableException}, whose first
     * line names the controller method, and the gRPC transport has neither.
     * Every other byte of the document is compared.
     */
    private static void sameRejection(String requestJson) throws Exception {
        try (TransportHarness harness = new TransportHarness(() -> ServiceResult.success(null))) {
            TransportHarness.Answer rest = harness.rest(OP, "cat", "t1", requestJson);
            TransportHarness.Answer grpc = harness.grpc(OP, "cat", "t1", requestJson);
            assertNull(harness.operations().lastCall(),
                    "an operation ran on the unreadable body '" + requestJson + "'");

            assertEquals(500, rest.status, "REST no longer answers this with 500: " + rest);
            assertEquals(rest.status, grpc.status,
                    "status differs for '" + requestJson + "'\n  rest: " + rest + "\n  grpc: " + grpc);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode restDoc = mapper.readTree(rest.body);
            JsonNode grpcDoc = mapper.readTree(grpc.body);
            assertNotNull(grpcDoc, "gRPC answered without a body: " + grpc);
            String restStack = ((ObjectNode) restDoc.get("error")).remove("stack").asText();
            String grpcStack = ((ObjectNode) grpcDoc.get("error")).remove("stack").asText();
            assertFalse(restStack.isEmpty() || grpcStack.isEmpty(), "a side lost its stack");

            assertArrayEquals(mapper.writeValueAsBytes(restDoc), mapper.writeValueAsBytes(grpcDoc),
                    "error document differs for '" + requestJson + "'\n  rest: " + rest.text()
                            + "\n  grpc: " + grpc.text());
        }
    }

    @Test
    @DisplayName("an empty body is rejected by both, with the same error document")
    void emptyBody() throws Exception {
        sameRejection("");
    }

    @Test
    @DisplayName("a literal null body is rejected by both, with the same error document")
    void nullBody() throws Exception {
        sameRejection("null");
    }

    @Test
    @DisplayName("a body that is not JSON is rejected by both, with the same error document")
    void notJson() throws Exception {
        sameRejection("not json at all");
    }

    @Test
    @DisplayName("a JSON body that is not an object is rejected by both, with the same error document")
    void notAnObject() throws Exception {
        sameRejection("[1]");
    }
}
