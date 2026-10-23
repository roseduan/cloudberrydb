/*
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 */
package cloud.elastic.dlagent.grpc;

/**
 * Presents a {@link Throwable} that is not an {@link Exception} -- a
 * {@code NoClassDefFoundError}, {@code ExceptionInInitializerError},
 * {@code StackOverflowError} -- to {@code IcebergExceptionMapper}, which maps
 * exceptions.
 *
 * <p>The REST agent never hands such an Error to its controller advice
 * directly: Spring's dispatcher wraps it in a {@code NestedServletException}
 * first, and the advice's catch-all turns that into the 500 error document.
 * The wrapper here carries the message Spring builds, so the document's
 * {@code stack} names the same failure; only the wrapper's class differs,
 * Spring not being on this agent's classpath.
 */
final class UnhandledErrors {

    /** What Spring says when a handler, run on the request thread, threw. */
    static final String HANDLER_DISPATCH_FAILED = "Handler dispatch failed";

    /** What Spring says when a streamed response body, run asynchronously, threw. */
    static final String ASYNC_PROCESSING_FAILED = "Async processing failed";

    private UnhandledErrors() {
    }

    /**
     * @param t       what escaped the handler
     * @param context which of Spring's two messages REST would use
     * @return {@code t} itself if it is an Exception, else the wrapped Error
     */
    static Exception asException(Throwable t, String context) {
        if (t instanceof Exception) {
            return (Exception) t;
        }
        return new RuntimeException(context + "; nested exception is " + t, t);
    }
}
