package cloud.elastic.dlagent.service;

import java.util.Map;

/**
 * Generic result class for service operations
 */
public class ServiceResult<T> {
    private final boolean success;
    private final T data;
    private final Map<String, Object> errorResponse;
    private final int statusCode;
    private final Map<String, String> headers;

    private ServiceResult(boolean success, T data, Map<String, Object> errorResponse, int statusCode) {
        this(success, data, errorResponse, statusCode, java.util.Collections.emptyMap());
    }

    private ServiceResult(boolean success, T data, Map<String, Object> errorResponse, int statusCode,
                          Map<String, String> headers) {
        this.success = success;
        this.data = data;
        this.errorResponse = errorResponse;
        this.statusCode = statusCode;
        this.headers = headers;
    }

    public static <T> ServiceResult<T> success(T data) {
        return new ServiceResult<>(true, data, null, 200);
    }

    public static <T> ServiceResult<T> notFound(String resource, String identifier) {
        Map<String, Object> errorResponse = new java.util.HashMap<>();
        Map<String, Object> error = new java.util.HashMap<>();
        error.put("message", "The given " + resource + " does not exist: " + identifier);
        error.put("type", "NoSuchTableException");
        error.put("code", 404);
        errorResponse.put("error", error);

        return new ServiceResult<>(false, null, errorResponse, 404);
    }

    public static <T> ServiceResult<T> conflict(String resource, String identifier) {
        Map<String, Object> errorResponse = new java.util.HashMap<>();
        Map<String, Object> error = new java.util.HashMap<>();
        error.put("message", "The given " + resource + " already exists: " + identifier);
        error.put("type", "AlreadyExistsException");
        error.put("code", 409);
        errorResponse.put("error", error);

        return new ServiceResult<>(false, null, errorResponse, 409);
    }

    public static <T> ServiceResult<T> error(String operation, String errorMessage) {
        Map<String, Object> errorResponse = new java.util.HashMap<>();
        Map<String, Object> error = new java.util.HashMap<>();
        error.put("message", "Error in " + operation + ": " + errorMessage);
        error.put("type", "InternalServerError");
        error.put("code", 500);
        errorResponse.put("error", error);

        return new ServiceResult<>(false, null, errorResponse, 500);
    }

    public static <T> ServiceResult<T> badRequest(String message) {
        Map<String, Object> errorResponse = new java.util.HashMap<>();
        Map<String, Object> error = new java.util.HashMap<>();
        error.put("message", message);
        error.put("type", "BadRequestException");
        error.put("code", 400);
        errorResponse.put("error", error);

        return new ServiceResult<>(false, null, errorResponse, 400);
    }

    /**
     * An error result with the caller's own type and code.
     *
     * <p>The general case the four factories above are special cases of. The
     * body construction below is character-for-character the one the REST
     * controller's former {@code createErrorResponse(message, type, code)}
     * built -- same HashMap, same three keys, same insertion order -- because
     * the REST path's response bytes may not change and the two transports
     * must emit the same bytes.
     */
    public static <T> ServiceResult<T> failure(String message, String type, int code) {
        Map<String, Object> errorResponse = new java.util.HashMap<>();
        Map<String, Object> error = new java.util.HashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("code", code);
        errorResponse.put("error", error);

        return new ServiceResult<>(false, null, errorResponse, code);
    }

    public boolean isSuccess() {
        return success;
    }

    public T getData() {
        return data;
    }

    public Map<String, Object> getErrorResponse() {
        return errorResponse;
    }

    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Transport metadata to accompany the body; empty for almost everything.
     *
     * <p>Carries the {@code ETag} header five REST endpoints emit, so the
     * operation logic can live in core without losing it. That header is
     * dead weight for the real client -- {@code agent_cli}'s Response struct
     * carries no headers at all, as {@code loadMetadataJson} already notes in
     * the source -- but REST is the rollback path and may not change what it
     * puts on the wire, so it is carried here rather than dropped. The gRPC transport ignores it.
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * A failure whose error body the caller already built.
     *
     * <p>Fifteen endpoints construct the {@code {"error":{...}}} map inline
     * rather than calling a factory, and they need a way to hand over the map
     * they already have; rebuilding it through {@link #failure} would mean
     * editing fifteen bodies whose output must stay byte-for-byte unchanged.
     */
    public static <T> ServiceResult<T> failureBody(Map<String, Object> errorResponse, int statusCode) {
        return new ServiceResult<>(false, null, errorResponse, statusCode);
    }

    /** The same result plus one header. */
    public ServiceResult<T> withHeader(String name, String value) {
        Map<String, String> merged = new java.util.LinkedHashMap<>(headers);
        merged.put(name, value);
        return new ServiceResult<>(success, data, errorResponse, statusCode, merged);
    }

    /**
     * A success carrying no body and a status other than 200.
     *
     * <p>The one caller is the {@code If-None-Match} path, which answers 304
     * with an empty body. Folding it into {@link #success} would have meant
     * giving that method a status parameter every other caller ignores.
     */
    public static <T> ServiceResult<T> successWithStatus(T data, int statusCode) {
        return new ServiceResult<>(true, data, null, statusCode);
    }
}
