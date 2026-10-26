package org.apache.cloudberry.iceberg.gateway.rest;

/**
 * Per-request holder for the database selected by the request's Iceberg REST {@code prefix},
 * populated by {@link GatewayFilter}. {@code null} means the request carried no prefix and is
 * served from the default database ({@code pg.jdbcUrl}).
 */
public final class CurrentDatabase {

    private static final ThreadLocal<String> DB = new ThreadLocal<>();

    public static void set(String db) {
        DB.set(db);
    }

    public static String get() {
        return DB.get();
    }

    public static void clear() {
        DB.remove();
    }

    private CurrentDatabase() {
    }
}
