package cloud.elastic.dlagent.service.rest;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception handler for Iceberg REST API
 */
@ControllerAdvice(basePackages = "cloud.elastic.dlagent.service.rest")
@RequestMapping(produces = "application/json")
@Slf4j
public class IcebergExceptionHandler {

    /**
     * Handle NoSuchTableException
     */
    @ExceptionHandler(NoSuchTableException.class)
    public ResponseEntity<Map<String, Object>> handleNoSuchTableException(NoSuchTableException e) {
        log.error("Table not found: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", "The given table does not exist");
        error.put("type", "NoSuchTableException");
        error.put("code", 404);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
    }

    /**
     * Handle NoSuchNamespaceException
     */
    @ExceptionHandler(NoSuchNamespaceException.class)
    public ResponseEntity<Map<String, Object>> handleNoSuchNamespaceException(NoSuchNamespaceException e) {
        log.error("Namespace not found: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", "The given namespace does not exist");
        error.put("type", "NoSuchNamespaceException");
        error.put("code", 404);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
    }

    /**
     * A commit whose reply was lost and whose outcome could not be learned
     * from the table either.  424 (not 5xx) on purpose: the caller's transport
     * retries a 5xx, and retrying a commit that may already be applied is
     * exactly what must not happen here.
     */
    @ExceptionHandler(CommitStateUnknownException.class)
    public ResponseEntity<Map<String, Object>> handleCommitStateUnknown(CommitStateUnknownException e) {
        log.error("Commit state unknown: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", "Iceberg commit result unknown: the catalog stopped answering while a commit "
                + "was in flight and the table state could not be re-read; the data may or may not have "
                + "been committed — verify the table before re-running the statement");
        error.put("type", "CommitStateUnknownException");
        error.put("code", 424);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.FAILED_DEPENDENCY).body(errorResponse);
    }

    /**
     * Handle AlreadyExistsException
     */
    @ExceptionHandler(AlreadyExistsException.class)
    public ResponseEntity<Map<String, Object>> handleAlreadyExistsException(AlreadyExistsException e) {
        log.error("Table already exists: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", "The given table already exists");
        error.put("type", "AlreadyExistsException");
        error.put("code", 409);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
    }

    /**
     * Handle CatalogOperationException
     */
    @ExceptionHandler(CatalogOperationException.class)
    public ResponseEntity<Map<String, Object>> handleCatalogOperationException(CatalogOperationException e) {
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
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(e.getHttpStatusCode()).body(errorResponse);
    }

    /**
     * Handle IllegalArgumentException
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("Bad request: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", e.getMessage());
        error.put("type", "BadRequestException");
        error.put("code", 400);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    /**
     * Handle all other exceptions
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception e) {
        log.error("Internal server error: {}", e.getMessage(), e);

        Map<String, Object> errorResponse = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        error.put("message", "Internal Server Error");
        error.put("type", "InternalServerError");
        error.put("code", 500);
        error.put("stack", getStackTrace(e));
        errorResponse.put("error", error);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
