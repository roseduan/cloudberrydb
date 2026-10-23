/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.parity;

import cloud.elastic.dlagent.api.exception.CatalogOperationException;
import cloud.elastic.dlagent.service.ServiceResult;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Failure, end to end: the status the C client will act on, and the document
 * it will read its message out of.
 *
 * <p>{@code IcebergExceptionMapperTest} in core pins the mapper. This pins
 * what actually comes out of each transport once the mapper has run, which is
 * not the same claim -- the divergence that made this necessary was in the
 * gRPC handler, not in the mapper: it answered 500 for everything because it
 * never consulted a mapper at all, while Spring's {@code @ControllerAdvice}
 * did. A live polaris run reported
 *
 * <pre>
 * rest: HTTP Status: 400, Error Type: CatalogOperationException
 * grpc: HTTP Status: 500, Error Type: InternalServerError
 * </pre>
 *
 * for the same failed CREATE CATALOG. The status is not cosmetic: the C side
 * turns it into an AGENT_CLI_ERROR_* code, and the FDW decides from that
 * whether a failure is the caller's fault or worth retrying.
 */
class ErrorParityTest {

    private static void assertSameFailure(String what, int expectedStatus,
                                          Supplier<ServiceResult<Object>> answer) throws Exception {
        try (TransportHarness harness = new TransportHarness(answer)) {
            TransportHarness.Answer rest = harness.rest(Op.LOAD_TABLE, "cat", "t1", "{}");
            TransportHarness.Answer grpc = harness.grpc(Op.LOAD_TABLE, "cat", "t1", "{}");

            ParityAssert.sameAnswer(what, rest, grpc);
            assertEquals(expectedStatus, rest.status,
                    what + ": both agree, but on the wrong status -- " + rest);
        }
    }

    /* ---- exceptions that escape the operation ------------------------ */

    @Test
    @DisplayName("NoSuchTableException is 404 on both")
    void noSuchTable() throws Exception {
        assertSameFailure("NoSuchTableException", 404, () -> {
            throw new NoSuchTableException("gone");
        });
    }

    @Test
    @DisplayName("NoSuchNamespaceException is 404 on both")
    void noSuchNamespace() throws Exception {
        assertSameFailure("NoSuchNamespaceException", 404, () -> {
            throw new NoSuchNamespaceException("gone");
        });
    }

    @Test
    @DisplayName("AlreadyExistsException is 409 on both")
    void alreadyExists() throws Exception {
        assertSameFailure("AlreadyExistsException", 409, () -> {
            throw new AlreadyExistsException("there");
        });
    }

    @Test
    @DisplayName("a catalog failure keeps the catalog server's own status")
    void catalogOperation() throws Exception {
        assertSameFailure("CatalogOperationException(400)", 400, () -> {
            throw new CatalogOperationException("createCatalog", 400,
                    "{\"error\":{\"code\":400}}", "overlapping locations");
        });
    }

    @Test
    @DisplayName("a 409 catalog failure gets its own wording, on both")
    void catalogAlreadyExists() throws Exception {
        assertSameFailure("CatalogOperationException(409)", 409, () -> {
            throw new CatalogOperationException("createCatalog", 409, "{}", "exists");
        });
    }

    @Test
    @DisplayName("a 401 catalog failure is an authentication failure, on both")
    void catalogUnauthenticated() throws Exception {
        assertSameFailure("CatalogOperationException(401)", 401, () -> {
            throw new CatalogOperationException("createCatalog", 401, "{}", "who are you");
        });
    }

    @Test
    @DisplayName("IllegalArgumentException is 400 on both, carrying its own message")
    void illegalArgument() throws Exception {
        assertSameFailure("IllegalArgumentException", 400, () -> {
            throw new IllegalArgumentException("bad option");
        });
    }

    @Test
    @DisplayName("anything else is 500 on both")
    void unexpected() throws Exception {
        assertSameFailure("IllegalStateException", 500, () -> {
            throw new IllegalStateException("boom");
        });
    }

    /**
     * A java.lang.Error -- a class missing from the classpath, a failed
     * static initialiser -- is answered like any other unexpected failure:
     * Spring wraps it and the controller advice turns it into the 500
     * document, so gRPC must too rather than ending the call with a bare
     * UNKNOWN and no body.
     */
    @Test
    @DisplayName("a java.lang.Error is the 500 document on both")
    void error() throws Exception {
        try (TransportHarness harness = new TransportHarness(() -> {
            throw new NoClassDefFoundError("org/apache/iceberg/Missing");
        })) {
            TransportHarness.Answer rest = harness.rest(Op.LOAD_TABLE, "cat", "t1", "{}");
            TransportHarness.Answer grpc = harness.grpc(Op.LOAD_TABLE, "cat", "t1", "{}");

            ParityAssert.sameAnswerForError("NoClassDefFoundError", rest, grpc);
            assertEquals(500, rest.status, "both agree, but on the wrong status -- " + rest);
        }
    }

    /* ---- failures the operation returns rather than throws ----------- */

    @Test
    @DisplayName("ServiceResult.notFound")
    void resultNotFound() throws Exception {
        assertSameFailure("ServiceResult.notFound", 404,
                () -> ServiceResult.notFound("table", "t1"));
    }

    @Test
    @DisplayName("ServiceResult.conflict")
    void resultConflict() throws Exception {
        assertSameFailure("ServiceResult.conflict", 409,
                () -> ServiceResult.conflict("table", "t1"));
    }

    @Test
    @DisplayName("ServiceResult.badRequest")
    void resultBadRequest() throws Exception {
        assertSameFailure("ServiceResult.badRequest", 400,
                () -> ServiceResult.badRequest("missing namespace"));
    }

    @Test
    @DisplayName("ServiceResult.error")
    void resultError() throws Exception {
        assertSameFailure("ServiceResult.error", 500,
                () -> ServiceResult.error("loadTable", "something broke"));
    }

    @Test
    @DisplayName("a 304 carries no body on either transport")
    void notModified() throws Exception {
        try (TransportHarness harness =
                     new TransportHarness(() -> ServiceResult.successWithStatus(null, 304))) {
            TransportHarness.Answer rest = harness.rest(Op.LOAD_TABLE, "cat", "t1", "{}");
            TransportHarness.Answer grpc = harness.grpc(Op.LOAD_TABLE, "cat", "t1", "{}");
            ParityAssert.sameAnswer("304", rest, grpc);
            assertEquals(304, rest.status);
            assertEquals(0, rest.body.length, "a 304 must carry no body");
        }
    }
}
