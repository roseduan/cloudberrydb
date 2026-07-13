package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Offline unit tests for {@link RateLimiter} using an injected fake clock — no wall-clock
 * sleeps, so the window-expiry behavior is deterministic and fast.
 */
class RateLimiterTest {

    /** Generous bound: these tests care about window semantics, not eviction. */
    private static final int GENEROUS_MAX_KEYS = 1_000;

    @Test
    void allowsUpToMaxAttemptsThenRejects() {
        AtomicLong clock = new AtomicLong(0L);
        RateLimiter limiter = new RateLimiter(3, 60, GENEROUS_MAX_KEYS, clock::get);

        assertThat(limiter.tryAcquire("k")).isTrue();
        assertThat(limiter.tryAcquire("k")).isTrue();
        assertThat(limiter.tryAcquire("k")).isTrue();
        // 4th attempt within the same window must be rejected.
        assertThat(limiter.tryAcquire("k")).isFalse();
    }

    @Test
    void recoversAfterWindowSlidesPast() {
        AtomicLong clock = new AtomicLong(0L);
        RateLimiter limiter = new RateLimiter(2, 10, GENEROUS_MAX_KEYS, clock::get);

        assertThat(limiter.tryAcquire("k")).isTrue();
        assertThat(limiter.tryAcquire("k")).isTrue();
        assertThat(limiter.tryAcquire("k")).isFalse();

        // Advance the fake clock past the 10s window.
        clock.set(10_001L);
        assertThat(limiter.tryAcquire("k")).isTrue();
    }

    @Test
    void differentKeysAreIndependent() {
        AtomicLong clock = new AtomicLong(0L);
        RateLimiter limiter = new RateLimiter(1, 60, GENEROUS_MAX_KEYS, clock::get);

        assertThat(limiter.tryAcquire("a")).isTrue();
        assertThat(limiter.tryAcquire("a")).isFalse();
        // A different key has its own budget, unaffected by "a" being exhausted.
        assertThat(limiter.tryAcquire("b")).isTrue();
    }

    @Test
    void oldestAttemptExpiringLetsANewOneThrough() {
        AtomicLong clock = new AtomicLong(0L);
        RateLimiter limiter = new RateLimiter(2, 10, GENEROUS_MAX_KEYS, clock::get);

        assertThat(limiter.tryAcquire("k")).isTrue(); // t=0
        clock.set(5_000L);
        assertThat(limiter.tryAcquire("k")).isTrue(); // t=5s, window has [0,5]
        assertThat(limiter.tryAcquire("k")).isFalse(); // still within window, budget exhausted

        // Advance so the t=0 attempt falls outside the trailing 10s window but t=5s remains.
        clock.set(10_500L);
        assertThat(limiter.tryAcquire("k")).isTrue();
        // Now both remaining attempts (t=5s, t=10.5s) are within window -> budget exhausted again.
        assertThat(limiter.tryAcquire("k")).isFalse();
    }

    @Test
    void mapIsBoundedUnderKeyChurn() {
        // issue #382 C2 review finding: a single IP cycling distinct bogus client_ids must not
        // grow the tracking map without bound -- that's a memory-exhaustion DoS in the very
        // feature meant to prevent DoS. Assert the map never exceeds maxKeys no matter how many
        // distinct keys tryAcquire is called with.
        AtomicLong clock = new AtomicLong(0L);
        int maxKeys = 5;
        RateLimiter limiter = new RateLimiter(3, 60, maxKeys, clock::get);

        for (int i = 0; i < 50; i++) {
            limiter.tryAcquire("bogus-client-" + i);
            assertThat(limiter.trackedKeyCount()).isLessThanOrEqualTo(maxKeys);
        }
        assertThat(limiter.trackedKeyCount()).isLessThanOrEqualTo(maxKeys);
    }
}
