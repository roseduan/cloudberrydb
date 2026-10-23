/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.service.ServiceResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Every operation, every result shape: the two transports must answer the same
 * status and the same bytes.
 *
 * <p>24 operations x 5 result shapes. The shapes are the ones the operations
 * actually produce, and each has its own way of going wrong:
 *
 * <ul>
 * <li><b>a map body</b> -- the ordinary success;
 * <li><b>no body</b> -- {@code ResponseEntity.ok().build()}; gRPC has to send
 *     zero bytes rather than the four bytes of {@code "null"};
 * <li><b>a pre-serialized byte[]</b> -- loadMetadataJson; Jackson would
 *     Base64-encode it if the writer did not special-case it;
 * <li><b>an error document</b> -- must arrive WITH its status, not as a bare
 *     gRPC error, because the C client parses the document for its message;
 * <li><b>an escaped exception</b> -- mapped by IcebergExceptionMapper in core,
 *     so gRPC answers the same 404, 409 or 400 REST does rather than a
 *     blanket 500.
 * </ul>
 */
class OperationParityTest {

    private static final String REQUEST = "{\"namespace\":\"ns\",\"extra\":1}";

    private static Map<String, Object> payload() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "t1");
        body.put("rows", 42);
        body.put("ok", true);
        body.put("cols", Arrays.asList("a", "b"));
        return body;
    }

    /** Runs one operation through both transports and compares the answers. */
    private static void assertParity(Op op, Supplier<ServiceResult<Object>> answer) throws Exception {
        try (TransportHarness harness = new TransportHarness(answer)) {
            String table = op.takesTable() ? "t1" : null;

            TransportHarness.Answer rest = harness.rest(op, "cat", table, REQUEST);
            TransportHarness.Answer grpc = harness.grpc(op, "cat", table, REQUEST);

            ParityAssert.sameAnswer(op.name(), rest, grpc);
        }
    }

    @ParameterizedTest(name = "{0} -- a map body")
    @EnumSource(Op.class)
    @DisplayName("an ordinary success answers with identical bytes")
    void successWithBody(Op op) throws Exception {
        assertParity(op, () -> ServiceResult.success(payload()));
    }

    @ParameterizedTest(name = "{0} -- no body")
    @EnumSource(Op.class)
    @DisplayName("a success with no body sends zero bytes on both")
    void successWithoutBody(Op op) throws Exception {
        assertParity(op, () -> ServiceResult.success(null));
    }

    @ParameterizedTest(name = "{0} -- a pre-serialized byte[]")
    @EnumSource(Op.class)
    @DisplayName("an already-serialized body is not re-encoded")
    void preSerializedBody(Op op) throws Exception {
        byte[] raw = "{\"metadata\":\"already json\"}".getBytes(StandardCharsets.UTF_8);
        assertParity(op, () -> ServiceResult.success(raw));
    }

    @ParameterizedTest(name = "{0} -- an error document")
    @EnumSource(Op.class)
    @DisplayName("a failed operation carries its status AND its document")
    void errorDocument(Op op) throws Exception {
        assertParity(op, () -> ServiceResult.notFound("table", "t1"));
    }

    @ParameterizedTest(name = "{0} -- an escaped exception")
    @EnumSource(Op.class)
    @DisplayName("an exception maps to the same status and document on both")
    void escapedException(Op op) throws Exception {
        assertParity(op, () -> {
            throw new org.apache.iceberg.exceptions.NoSuchTableException("gone");
        });
    }
}
