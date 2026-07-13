package org.apache.cloudberry.iceberg.gateway.auth;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Thread-safe, in-memory sliding-window rate limiter keyed by an arbitrary string (issue #382
 * C2 Task 3). Used to slow OAuth token-endpoint brute-force password guessing: each attempt
 * (successful or failed) is recorded against a key, and once a key has recorded {@code
 * maxAttempts} within the trailing {@code windowSeconds} it is rejected until older attempts age
 * out of the window.
 *
 * <p>The clock is injected as a {@link LongSupplier} (millis since epoch, like {@link
 * System#currentTimeMillis()}) so tests can drive window expiry deterministically without
 * wall-clock sleeps.
 *
 * <p><b>Bounded tracking map (issue #382 C2 review finding):</b> the key is {@code client_id +
 * "|" + remoteAddr}, and {@code client_id} is attacker-supplied and unvalidated. Pruning stale
 * <em>timestamps</em> inside {@code tryAcquire} does nothing for a key that is never revisited —
 * a single IP cycling through distinct bogus client_ids would otherwise grow the map without
 * bound, a memory-exhaustion DoS in the very feature meant to prevent DoS. To bound memory
 * regardless of key churn, {@code attempts} is a fixed-capacity LRU (access-order {@link
 * LinkedHashMap}): once the map holds more than {@code maxKeys} entries, the least-recently-used
 * key is evicted on the next insert, independent of whether its window has expired.
 *
 * <p><b>Locking:</b> a single {@code synchronized} monitor guards the whole map (LRU
 * reordering + prune + check + insert), rather than the previous {@code ConcurrentHashMap} +
 * per-key-deque striping scheme. The OAuth token endpoint is a low-QPS auth path, so one lock is
 * simpler and sufficient; it also makes the LRU eviction (which mutates the map itself, not just
 * a value) straightforward to reason about.
 *
 * <p><b>Accepted tradeoff (issue #382 C2 final review):</b> because eviction is purely
 * access-recency (LRU) and ignores window state, an attacker who churns more than {@code
 * maxKeys} distinct client_ids can evict — and thereby reset — an actively-limited key; at the
 * default {@code maxKeys=100000} this requires on the order of 100,000 distinct-key requests to
 * force a single eviction, which is judged acceptable given the alternative (unbounded memory).
 */
public final class RateLimiter {

    private final int maxAttempts;
    private final int windowSeconds;
    private final long windowMillis;
    private final int maxKeys;
    private final LongSupplier clockMillis;
    private final Map<String, Deque<Long>> attempts;

    public RateLimiter(int maxAttempts, int windowSeconds, int maxKeys, LongSupplier clockMillis) {
        this.maxAttempts = maxAttempts;
        this.windowSeconds = windowSeconds;
        this.windowMillis = windowSeconds * 1000L;
        this.maxKeys = maxKeys;
        this.clockMillis = clockMillis;
        this.attempts = new LinkedHashMap<>(16, 0.75f, /* accessOrder= */ true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Deque<Long>> eldest) {
                return size() > RateLimiter.this.maxKeys;
            }
        };
    }

    /** The trailing window size, in seconds — exposed so callers can populate a Retry-After header. */
    public int getWindowSeconds() {
        return windowSeconds;
    }

    /**
     * Records an attempt for {@code key} and reports whether it is within budget.
     *
     * @return false if {@code key} already has {@code maxAttempts} recorded within the trailing
     *     window (the attempt is still NOT counted in that case); true otherwise, in which case
     *     this call's timestamp is recorded.
     */
    public synchronized boolean tryAcquire(String key) {
        long now = clockMillis.getAsLong();
        long windowStart = now - windowMillis;

        Deque<Long> timestamps = attempts.computeIfAbsent(key, k -> new ArrayDeque<>());
        pruneExpired(timestamps, windowStart);
        if (timestamps.size() >= maxAttempts) {
            return false;
        }
        timestamps.addLast(now);
        return true;
    }

    /** Test/diagnostic hook: how many distinct keys are currently tracked (bounded by maxKeys). */
    synchronized int trackedKeyCount() {
        return attempts.size();
    }

    /** Drops timestamps older than the trailing window so the per-key deque doesn't grow unbounded. */
    private static void pruneExpired(Deque<Long> timestamps, long windowStart) {
        Iterator<Long> it = timestamps.iterator();
        while (it.hasNext()) {
            long ts = it.next();
            if (ts <= windowStart) {
                it.remove();
            } else {
                // Timestamps are inserted in increasing order, so once we hit one that's still
                // within the window, all subsequent ones are too.
                break;
            }
        }
    }
}
