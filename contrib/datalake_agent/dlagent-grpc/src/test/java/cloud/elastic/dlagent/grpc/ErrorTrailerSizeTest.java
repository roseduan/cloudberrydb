package cloud.elastic.dlagent.grpc;

import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data plane's error document travels as a trailer, which has a size
 * ceiling a REST body does not. Over it the C client dropped the document and
 * the user saw only "metadata size exceeds limit".
 */
class ErrorTrailerSizeTest {

    private final JsonResponseWriter writer = new JsonResponseWriter();
    private final DataProxyGrpcService service =
            new DataProxyGrpcService(null, null, writer, new GrpcClientDisconnectPredicate());

    private static ServiceResult<Object> failure(int stackChars) {
        char[] stack = new char[stackChars];
        Arrays.fill(stack, 's');
        Map<String, Object> error = new HashMap<>();
        error.put("message", "table db.t not found");
        error.put("type", "NoSuchTableException");
        error.put("code", 404);
        error.put("stack", new String(stack));
        Map<String, Object> body = new HashMap<>();
        body.put("error", error);
        return ServiceResult.failureBody(body, 404);
    }

    @Test
    void typicalStackTraceIsSentUnchanged() {
        // 50 KB is a deep Iceberg/S3 trace: well over gRPC's default 16 KiB,
        // well under the cap.
        ServiceResult<Object> f = failure(50 * 1024);
        assertArrayEquals(writer.write(f), service.boundedDocument(f));
    }

    @Test
    void oversizedStackIsCutAndEverythingElseKept() throws Exception {
        ServiceResult<Object> f = failure(3 * 1024 * 1024);
        byte[] doc = service.boundedDocument(f);

        assertTrue(doc.length <= DataProxyGrpcService.MAX_ERROR_DOCUMENT_BYTES,
                "document is " + doc.length + " bytes");
        JsonNode error = writer.objectMapper().readTree(doc).get("error");
        assertEquals("table db.t not found", error.get("message").asText());
        assertEquals("NoSuchTableException", error.get("type").asText());
        assertEquals(404, error.get("code").asInt());
        assertTrue(error.get("stack").asText().startsWith("ssss"));
        assertTrue(error.get("stack").asText().endsWith(" characters)"));
        // The caller's result is untouched.
        assertEquals(3 * 1024 * 1024,
                ((String) ((Map<?, ?>) f.getErrorResponse().get("error")).get("stack")).length());
    }
}
