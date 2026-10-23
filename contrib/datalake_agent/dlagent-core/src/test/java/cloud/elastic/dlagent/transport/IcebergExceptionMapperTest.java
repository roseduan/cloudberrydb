/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import cloud.elastic.dlagent.api.exception.CatalogOperationException;
import cloud.elastic.dlagent.service.ServiceResult;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Each exception keeps the status and the document shape the REST transport
 * has always given it.
 *
 * <p>This is the test for a divergence found by running the polaris suite
 * under both transports: a failed CREATE CATALOG read
 * {@code HTTP Status: 400, Error Type: CatalogOperationException} under REST
 * and {@code HTTP Status: 500, Error Type: InternalServerError} under gRPC,
 * because the mapping lived in a Spring {@code @ControllerAdvice} that only
 * one of the two transports has.
 */
class IcebergExceptionMapperTest {

    @SuppressWarnings("unchecked")
    private static Map<String, Object> error(ServiceResult<Object> result) {
        Map<String, Object> body = result.getErrorResponse();
        assertNotNull(body, "an error result must carry a body");
        return (Map<String, Object>) body.get("error");
    }

    @Test
    @DisplayName("a missing table is 404 NoSuchTableException, not 500")
    void noSuchTable() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new NoSuchTableException("nope"));
        assertEquals(404, r.getStatusCode());
        assertEquals("The given table does not exist", error(r).get("message"));
        assertEquals("NoSuchTableException", error(r).get("type"));
        assertEquals(404, error(r).get("code"));
    }

    @Test
    @DisplayName("a missing namespace is 404 NoSuchNamespaceException")
    void noSuchNamespace() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new NoSuchNamespaceException("nope"));
        assertEquals(404, r.getStatusCode());
        assertEquals("NoSuchNamespaceException", error(r).get("type"));
    }

    @Test
    @DisplayName("an existing table is 409 AlreadyExistsException")
    void alreadyExists() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new AlreadyExistsException("there"));
        assertEquals(409, r.getStatusCode());
        assertEquals("AlreadyExistsException", error(r).get("type"));
    }

    @Test
    @DisplayName("a catalog failure keeps the catalog server's own status")
    void catalogOperationKeepsItsStatus() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new CatalogOperationException("createCatalog", 400,
                        "{\"error\":{\"code\":400}}", "overlapping locations"));

        assertEquals(400, r.getStatusCode());
        assertEquals("CatalogOperationException", error(r).get("type"));
        assertEquals(400, error(r).get("code"));
        assertEquals("createCatalog", error(r).get("operation"));
        assertEquals("{\"error\":{\"code\":400}}", error(r).get("responseBody"));
    }

    @Test
    @DisplayName("409, 401 and 403 catalog failures get their own wording")
    void catalogOperationSpecialCases() {
        assertEquals("CatalogAlreadyExistsException",
                error(IcebergExceptionMapper.toResult(
                        new CatalogOperationException("createCatalog", 409, "{}", "x")))
                        .get("type"));
        assertEquals("CatalogAuthenticationException",
                error(IcebergExceptionMapper.toResult(
                        new CatalogOperationException("createCatalog", 401, "{}", "x")))
                        .get("type"));
        assertEquals("CatalogAccessDeniedException",
                error(IcebergExceptionMapper.toResult(
                        new CatalogOperationException("createCatalog", 403, "{}", "x")))
                        .get("type"));
    }

    @Test
    @DisplayName("an IllegalArgumentException is 400, carrying its own message")
    void illegalArgument() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new IllegalArgumentException("bad option"));
        assertEquals(400, r.getStatusCode());
        assertEquals("bad option", error(r).get("message"));
        assertEquals("BadRequestException", error(r).get("type"));
    }

    @Test
    @DisplayName("anything else is still 500 InternalServerError")
    void fallback() {
        ServiceResult<Object> r = IcebergExceptionMapper.toResult(
                new IllegalStateException("boom"));
        assertEquals(500, r.getStatusCode());
        assertEquals("Internal Server Error", error(r).get("message"));
        assertEquals("InternalServerError", error(r).get("type"));
    }

    @Test
    @DisplayName("every document carries the stack the C side prints")
    void stackIsAlwaysPresent() {
        for (Exception e : new Exception[] {
                new NoSuchTableException("a"),
                new AlreadyExistsException("b"),
                new CatalogOperationException("op", 400, "{}", "c"),
                new IllegalArgumentException("d"),
                new IllegalStateException("e") }) {
            Object stack = error(IcebergExceptionMapper.toResult(e)).get("stack");
            assertTrue(stack instanceof String && !((String) stack).isEmpty(),
                    "missing stack for " + e.getClass().getSimpleName());
        }
    }
}
