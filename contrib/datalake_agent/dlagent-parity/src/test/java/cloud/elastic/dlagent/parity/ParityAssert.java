/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one place that decides what "the two transports answered the same" means.
 *
 * <p>THE DEFAULT IS RAW BYTES. Not "the same JSON", not "the same fields":
 * the C client reads a byte stream, so anything weaker would let a real
 * difference through. There is exactly ONE documented exception, below.
 *
 * <p>THE EXCEPTION: {@code error.stack}. The error document carries the Java
 * stack trace of whatever went wrong, and that stack genuinely differs between
 * the transports because the frames above the operation genuinely differ --
 * Spring's {@code InvocableHandlerMethod} on one side, grpc-java's
 * {@code ServerCalls} on the other. Requiring them to be equal would be
 * requiring the two transports to be the same code.
 *
 * <p>The exception is kept as narrow as it can be made:
 *
 * <ul>
 * <li>it applies only when BOTH bodies are JSON objects that have
 *     {@code error.stack};
 * <li>both stacks must be non-empty and must name the SAME exception class on
 *     their first line, so "one side lost the stack" and "the two sides
 *     reported different exceptions" both still fail;
 * <li>everything else in the document -- status, code, type, message,
 *     operation, responseBody -- is still compared byte for byte after the
 *     stack is removed.
 * </ul>
 *
 * <p>Any further normalisation belongs in a review, not in a helper: a rule
 * that had quietly ignored {@code code} would have hidden the 400-becomes-500
 * divergence that the polaris suite found on a live cluster.
 */
final class ParityAssert {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ParityAssert() {
    }

    static void sameAnswer(String what, TransportHarness.Answer rest,
                           TransportHarness.Answer grpc) throws Exception {
        sameAnswer(what, rest, grpc, false);
    }

    /**
     * As {@link #sameAnswer(String, TransportHarness.Answer, TransportHarness.Answer)}
     * for an operation that threw a java.lang.Error. Spring reports it wrapped
     * in its NestedServletException, a class the gRPC agent does not have, so
     * the first stack line is compared from the message on: the wrapper's
     * message, which names the Error, must still be the same.
     */
    static void sameAnswerForError(String what, TransportHarness.Answer rest,
                                   TransportHarness.Answer grpc) throws Exception {
        sameAnswer(what, rest, grpc, true);
    }

    private static void sameAnswer(String what, TransportHarness.Answer rest,
                                   TransportHarness.Answer grpc, boolean wrappedError)
            throws Exception {
        assertEquals(rest.status, grpc.status,
                what + ": status differs\n  rest: " + rest + "\n  grpc: " + grpc);

        if (java.util.Arrays.equals(rest.body, grpc.body)) {
            return;
        }

        JsonNode restBody = parseOrNull(rest.body);
        JsonNode grpcBody = parseOrNull(grpc.body);
        String restStack = stackOf(restBody);
        String grpcStack = stackOf(grpcBody);

        if (restStack == null || grpcStack == null) {
            // No stack involved: the bytes simply differ, and that is a finding.
            assertArrayEquals(rest.body, grpc.body,
                    what + ": body differs\n  rest: " + rest.text() + "\n  grpc: " + grpc.text());
            return;
        }

        assertTrue(!restStack.isEmpty() && !grpcStack.isEmpty(),
                what + ": one side reported an empty stack");
        if (wrappedError) {
            assertTrue(firstLine(restStack).startsWith(
                            "org.springframework.web.util.NestedServletException: "),
                    what + ": REST did not report the Error the way this assumes: "
                            + firstLine(restStack));
            assertEquals(afterClassName(firstLine(restStack)),
                    afterClassName(firstLine(grpcStack)),
                    what + ": the two transports reported different errors");
        } else {
            assertEquals(firstLine(restStack), firstLine(grpcStack),
                    what + ": the two transports reported different exceptions");
        }

        ((ObjectNode) restBody.get("error")).remove("stack");
        ((ObjectNode) grpcBody.get("error")).remove("stack");
        assertEquals(MAPPER.writeValueAsString(restBody), MAPPER.writeValueAsString(grpcBody),
                what + ": error document differs once the stack is set aside");
    }

    private static JsonNode parseOrNull(byte[] body) {
        try {
            return body == null || body.length == 0 ? null : MAPPER.readTree(body);
        }
        catch (Exception e) {
            return null;
        }
    }

    private static String stackOf(JsonNode body) {
        if (body == null || !body.isObject()) {
            return null;
        }
        JsonNode error = body.get("error");
        if (error == null || !error.isObject()) {
            return null;
        }
        JsonNode stack = error.get("stack");
        return stack == null || !stack.isTextual() ? null : stack.asText();
    }

    private static String afterClassName(String line) {
        int colon = line.indexOf(':');
        return colon < 0 ? "" : line.substring(colon);
    }

    /** "org.apache.iceberg.exceptions.NoSuchTableException: gone" */
    private static String firstLine(String stack) {
        int nl = stack.indexOf('\n');
        return (nl < 0 ? stack : stack.substring(0, nl)).trim();
    }
}
