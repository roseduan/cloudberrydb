package org.apache.cloudberry.iceberg.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Pool creation is a real login that can stall; it must only ever block its own database. */
class KeyedPoolsTest {

    private final ExecutorService exec = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown() {
        exec.shutdownNow();
    }

    @Test
    void slowCreationOfOneDatabaseDoesNotBlockAnother() throws Exception {
        KeyedPools<String> pools = new KeyedPools<>(32);
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);

        Future<String> slow = exec.submit(() -> pools.get("slow_db", k -> {
            slowStarted.countDown();
            await(releaseSlow);
            return "pool:" + k;
        }));
        assertThat(slowStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // While slow_db is still being created, another database is served at once.
        Future<String> fast = exec.submit(() -> pools.get("fast_db", k -> "pool:" + k));
        assertThat(fast.get(5, TimeUnit.SECONDS)).isEqualTo("pool:fast_db");
        assertThat(slow.isDone()).isFalse();

        releaseSlow.countDown();
        assertThat(slow.get(5, TimeUnit.SECONDS)).isEqualTo("pool:slow_db");
    }

    @Test
    void concurrentFirstRequestsForOneDatabaseCreateItOnce() throws Exception {
        KeyedPools<String> pools = new KeyedPools<>(32);
        AtomicInteger creations = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        Future<?>[] calls = new Future<?>[8];
        for (int i = 0; i < calls.length; i++) {
            calls[i] = exec.submit(() -> {
                await(go);
                return pools.get("db", k -> "pool" + creations.incrementAndGet());
            });
        }
        go.countDown();
        for (Future<?> f : calls) {
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("pool1");
        }
        assertThat(creations.get()).isEqualTo(1);
    }

    @Test
    void failedCreationIsNotCachedSoTheNextRequestRetries() {
        KeyedPools<String> pools = new KeyedPools<>(32);
        assertThatThrownBy(() -> pools.get("db", k -> {
            throw new IllegalStateException("login failed");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(pools.get("db", k -> "pool")).isEqualTo("pool");
    }

    @Test
    void capRefusesNewDatabasesButStillServesKnownOnes() {
        KeyedPools<String> pools = new KeyedPools<>(2);
        pools.get("a", k -> "pa");
        pools.get("b", k -> "pb");
        assertThatThrownBy(() -> pools.get("c", k -> "pc"))
                .isInstanceOfSatisfying(DatabaseRouter.UnservableDatabaseException.class,
                        e -> assertThat(e.status()).isEqualTo(503));
        assertThat(pools.get("a", k -> "unused")).isEqualTo("pa");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
