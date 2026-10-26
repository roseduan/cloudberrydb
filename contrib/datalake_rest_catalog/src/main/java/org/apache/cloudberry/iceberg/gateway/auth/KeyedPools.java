package org.apache.cloudberry.iceberg.gateway.auth;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Create-once cache of one pool per database, kept apart from {@link DatabasePoolRegistry} so the
 * locking can be tested without a cluster.
 *
 * <p>Creating a pool means a real login plus catalog queries, which can stall for seconds (a
 * cloud database whose default warehouse is resuming, a connect timeout). The lock is therefore
 * per database: a slow first request to one database never holds up the first request to
 * another. A failed creation caches nothing, so the next request tries again.
 *
 * <p>The size cap is checked under the per-database lock only, so concurrent first requests to
 * different databases can overshoot it by at most the number of such requests in flight.
 */
final class KeyedPools<P> {

    private final ConcurrentHashMap<String, P> pools = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> creationLocks = new ConcurrentHashMap<>();
    private final int maxSize;

    KeyedPools(int maxSize) {
        this.maxSize = maxSize;
    }

    /** The pool for {@code key}, created by {@code create} on first use. */
    P get(String key, Function<String, P> create) {
        P existing = pools.get(key);
        if (existing != null) {
            return existing;
        }
        Object lock = creationLocks.computeIfAbsent(key, k -> new Object());
        synchronized (lock) {
            existing = pools.get(key);
            if (existing != null) {
                return existing;
            }
            if (pools.size() >= maxSize) {
                throw new DatabaseRouter.UnservableDatabaseException(503, "ServiceUnavailableException",
                        "Gateway already serves the maximum of " + maxSize + " databases");
            }
            P created = create.apply(key);
            pools.put(key, created);
            return created;
        }
    }

    Iterable<P> all() {
        return pools.values();
    }
}
