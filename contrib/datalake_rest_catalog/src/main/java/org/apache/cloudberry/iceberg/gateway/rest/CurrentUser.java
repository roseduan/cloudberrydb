package org.apache.cloudberry.iceberg.gateway.rest;

/**
 * Per-request holder for the authenticated PG role, populated by {@link GatewayFilter} from the
 * JWT subject and read by {@link org.apache.cloudberry.iceberg.gateway.catalog.UserScopedCatalog} when the
 * (single, shared) REST adapter dispatches a call on the request thread.
 */
public final class CurrentUser {

    private static final ThreadLocal<String> PG_ROLE = new ThreadLocal<>();

    public static void set(String pgRole) {
        PG_ROLE.set(pgRole);
    }

    public static String get() {
        String role = PG_ROLE.get();
        if (role == null) {
            throw new IllegalStateException("No authenticated PG role bound to this request");
        }
        return role;
    }

    public static void clear() {
        PG_ROLE.remove();
    }

    private CurrentUser() {
    }
}
