package org.apache.cloudberry.iceberg.gateway.auth;

/**
 * Decides which databases a multi-database gateway may serve, for {@code GatewayFilter}. In
 * production this is {@link DatabasePoolRegistry}; tests substitute an in-memory one so the
 * routing is exercised without a PostgreSQL cluster.
 */
public interface DatabaseRouter {

    /** Thrown when a requested database cannot be served; carries the HTTP status to answer with. */
    final class UnservableDatabaseException extends RuntimeException {
        private final int status;
        private final String type;

        public UnservableDatabaseException(int status, String type, String message) {
            super(message);
            this.status = status;
            this.type = type;
        }

        public int status() {
            return status;
        }

        public String type() {
            return type;
        }
    }

    /** False = single-database mode: requests carry no prefix and use the default database. */
    boolean multiDb();

    /** Whether {@code pgRole} holds CONNECT on {@code db}; false for a database that does not exist. */
    boolean canConnect(String pgRole, String db);

    /** Ensures {@code db} can be served, or throws {@link UnservableDatabaseException}. */
    void requireServable(String db);
}
