package cloud.elastic.dlagent.api.exception;

/**
 * Exception for catalog operation failures
 */
public class CatalogOperationException extends RuntimeException {
    private final int httpStatusCode;
    private final String responseBody;
    private final String operation;
    
    public CatalogOperationException(String operation, int httpStatusCode, String responseBody, String message) {
        super(message);
        this.operation = operation;
        this.httpStatusCode = httpStatusCode;
        this.responseBody = responseBody;
    }
    
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(super.getMessage());
        sb.append(" [Operation: ").append(operation);
        sb.append(", HTTP Status: ").append(httpStatusCode);
        if (responseBody != null && !responseBody.isEmpty()) {
            sb.append(", Response: ").append(responseBody);
        }
        sb.append("]");
        return sb.toString();
    }
    
    public int getHttpStatusCode() {
        return httpStatusCode;
    }
    
    public String getResponseBody() {
        return responseBody;
    }
    
    public String getOperation() {
        return operation;
    }
}
