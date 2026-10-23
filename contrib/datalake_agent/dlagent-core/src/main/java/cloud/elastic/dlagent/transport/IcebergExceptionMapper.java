/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.transport;

import cloud.elastic.dlagent.api.exception.CatalogOperationException;
import cloud.elastic.dlagent.service.ServiceResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Turns an exception that escaped an operation into the error document the C
 * client expects, for BOTH transports.
 *
 * <p>WHY THIS IS IN CORE. This mapping used to exist only as
 * {@code IcebergExceptionHandler}, a Spring {@code @ControllerAdvice} in
 * dlagent-rest, so only the REST transport had it. The gRPC transport's
 * catch-all produced a plain 500 for everything, and the difference is visible
 * to the user:
 *
 * <pre>
 * rest: HTTP Status: 400, Error Type: CatalogOperationException
 * grpc: HTTP Status: 500, Error Type: InternalServerError
 * </pre>
 *
 * on the very same failed CREATE CATALOG. It is not only cosmetic: the C side
 * turns the status into an {@code AGENT_CLI_ERROR_*} code, so "the catalog
 * already exists" (409) and "no such table" (404) both arrived as a generic
 * HTTP error, and the FDW decides what to retry from exactly that.
 *
 * <p>The bodies below are character-for-character the ones the REST handler
 * built -- same keys, same insertion into a HashMap, same stack-trace field --
 * because the REST path's response bytes may not change and the two
 * transports must emit the same bytes.
 * {@code IcebergExceptionHandler} now delegates here, which is what keeps them
 * from drifting apart again.
 *
 * <p>The {@code instanceof} chain reproduces Spring's most-specific-wins
 * dispatch, in the order the six {@code @ExceptionHandler} methods declared.
 */
@Slf4j
public final class IcebergExceptionMapper {

    private IcebergExceptionMapper() {
    }

    /**
     * The error document and status for an exception that escaped an operation.
     *
     * @param e the exception
     * @return a failed result carrying the REST-shaped error body
     */
    public static ServiceResult<Object> toResult(Exception e) {
        if (e instanceof NoSuchTableException) {
            log.error("Table not found: {}", e.getMessage(), e);
            return body(e, "The given table does not exist", "NoSuchTableException", 404);
        }
        if (e instanceof NoSuchNamespaceException) {
            log.error("Namespace not found: {}", e.getMessage(), e);
            return body(e, "The given namespace does not exist", "NoSuchNamespaceException", 404);
        }
        if (e instanceof CommitStateUnknownException) {
            /*
             * A commit whose reply was lost and whose outcome could not be
             * learned from the table either.  424 (not 5xx) on purpose: the
             * caller's transport retries a 5xx, and retrying a commit that may
             * already be applied is exactly what must not happen here.  In
             * this chain rather than only in the REST handler, so that gRPC
             * answers it with the same status: before, it would have fallen
             * through to the 500 below.
             */
            log.error("Commit state unknown: {}", e.getMessage(), e);
            return body(e, "Iceberg commit result unknown: the catalog stopped answering while a commit "
                    + "was in flight and the table state could not be re-read; the data may or may not have "
                    + "been committed \u2014 verify the table before re-running the statement",
                    "CommitStateUnknownException", 424);
        }
        if (e instanceof AlreadyExistsException) {
            log.error("Table already exists: {}", e.getMessage(), e);
            return body(e, "The given table already exists", "AlreadyExistsException", 409);
        }
        if (e instanceof CatalogOperationException) {
            return catalogOperation((CatalogOperationException) e);
        }
        if (e instanceof IllegalArgumentException) {
            log.error("Bad request: {}", e.getMessage(), e);
            return body(e, e.getMessage(), "BadRequestException", 400);
        }

        log.error("Internal server error: {}", e.getMessage(), e);
        return body(e, "Internal Server Error", "InternalServerError", 500);
    }

    /**
     * The catalog case carries two extra fields and takes its status from the
     * catalog server's own answer, so it cannot go through {@link #body}.
     */
    private static ServiceResult<Object> catalogOperation(CatalogOperationException e) {
        log.error("Catalog operation failed: {} - HTTP {}, Response: {}",
                  e.getOperation(), e.getHttpStatusCode(), e.getResponseBody(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();

        switch (e.getHttpStatusCode()) {
            case 409:
                error.put("message", "Catalog already exists");
                error.put("type", "CatalogAlreadyExistsException");
                break;
            case 401:
                error.put("message", "Authentication failed for catalog operation");
                error.put("type", "CatalogAuthenticationException");
                break;
            case 403:
                error.put("message", "Access denied for catalog operation");
                error.put("type", "CatalogAccessDeniedException");
                break;
            default:
                error.put("message", e.getMessage());
                error.put("type", "CatalogOperationException");
        }

        error.put("code", e.getHttpStatusCode());
        error.put("operation", e.getOperation());
        error.put("responseBody", e.getResponseBody());
        error.put("stack", stackTrace(e));
        errorResponse.put("error", error);

        return ServiceResult.failureBody(errorResponse, e.getHttpStatusCode());
    }

    private static ServiceResult<Object> body(Exception e, String message, String type, int code) {
        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("code", code);
        error.put("stack", stackTrace(e));
        errorResponse.put("error", error);

        return ServiceResult.failureBody(errorResponse, code);
    }

    /**
     * The full stack trace, which the C side reads out of the "stack" field and
     * prints under client_min_messages = log
     * ({@code agent_cli_wrapper_check_exec_error_json} in agent_c_api.cpp).
     */
    private static String stackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
