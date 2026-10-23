package cloud.elastic.dlagent.service.rest;

import cloud.elastic.dlagent.api.exception.CatalogOperationException;
import cloud.elastic.dlagent.service.ServiceResult;
import cloud.elastic.dlagent.transport.IcebergExceptionMapper;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

/**
 * Exception handler for Iceberg REST API.
 *
 * <p>The five methods below are Spring's dispatch table and nothing else: the
 * bodies and statuses come from {@link IcebergExceptionMapper} in core, so the
 * gRPC transport answers a failed operation exactly the same way. Before that
 * move this class WAS the mapping, and the gRPC side -- which cannot see a
 * {@code @ControllerAdvice} -- turned every one of these into a plain 500.
 */
@ControllerAdvice(basePackages = "cloud.elastic.dlagent.service.rest")
@RequestMapping(produces = "application/json")
public class IcebergExceptionHandler {

    @ExceptionHandler(NoSuchTableException.class)
    public ResponseEntity<Map<String, Object>> handleNoSuchTableException(NoSuchTableException e) {
        return toResponse(e);
    }

    @ExceptionHandler(NoSuchNamespaceException.class)
    public ResponseEntity<Map<String, Object>> handleNoSuchNamespaceException(NoSuchNamespaceException e) {
        return toResponse(e);
    }

    @ExceptionHandler(AlreadyExistsException.class)
    public ResponseEntity<Map<String, Object>> handleAlreadyExistsException(AlreadyExistsException e) {
        return toResponse(e);
    }

    @ExceptionHandler(CatalogOperationException.class)
    public ResponseEntity<Map<String, Object>> handleCatalogOperationException(CatalogOperationException e) {
        return toResponse(e);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(IllegalArgumentException e) {
        return toResponse(e);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception e) {
        return toResponse(e);
    }

    private ResponseEntity<Map<String, Object>> toResponse(Exception e) {
        ServiceResult<Object> result = IcebergExceptionMapper.toResult(e);
        return ResponseEntity.status(result.getStatusCode()).body(result.getErrorResponse());
    }
}
