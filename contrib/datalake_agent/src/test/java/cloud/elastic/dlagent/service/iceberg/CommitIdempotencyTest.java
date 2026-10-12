/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.service.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a resent commit must and must not do (issue #442).
 *
 * <p>The failure being guarded against is not a crash: it is a commit that ran
 * past the client's timeout, got resent, and landed twice -- leaving the table
 * with two snapshots holding the same rows and no error anywhere to say so.
 * These cases pin the three behaviours that prevent it and the one that must
 * not be prevented.
 */
public class CommitIdempotencyTest {

    @BeforeEach
    public void forgetPreviousTokens() {
        CommitIdempotency.reset();
    }

    /**
     * The case the whole mechanism exists for: the first attempt is still
     * committing when its resend arrives. The resend must not commit, and must
     * come back with the first one's metadata location -- that is the value the
     * client writes into its catalog, and two different ones would be two
     * different tables as far as it is concerned.
     */
    @Test
    public void aResendWaitsForTheFirstAttemptAndGetsItsAnswer() throws Exception {
        String token = "commit-in-flight";
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger commits = new AtomicInteger();
        Map<String, Object> answer =
                Collections.singletonMap("metadata-location", "s3://b/m/v7.json");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Map<String, Object>> first = pool.submit(() ->
                    CommitIdempotency.once(token, "commitAppend", () -> {
                        commits.incrementAndGet();
                        firstStarted.countDown();
                        releaseFirst.await(10, TimeUnit.SECONDS);
                        return answer;
                    }));

            firstStarted.await(10, TimeUnit.SECONDS);

            Future<Map<String, Object>> resend = pool.submit(() ->
                    CommitIdempotency.once(token, "commitAppend", () -> {
                        commits.incrementAndGet();
                        return Collections.singletonMap(
                                "metadata-location", "s3://b/m/v8.json");
                    }));

            releaseFirst.countDown();

            assertSame(answer, first.get(10, TimeUnit.SECONDS));
            assertSame(answer, resend.get(10, TimeUnit.SECONDS),
                       "the resend must return the first attempt's result");
            assertEquals(1, commits.get(), "the commit must have run once");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A resend that arrives after the first attempt already finished still has
     * to be recognized: the client's retry clock and the commit's duration are
     * independent, and a response lost on the way back looks exactly like this.
     */
    @Test
    public void aResendAfterCompletionStillGetsTheFirstAnswer() throws Exception {
        String token = "commit-finished";
        AtomicInteger commits = new AtomicInteger();
        Map<String, Object> answer =
                Collections.singletonMap("metadata-location", "s3://b/m/v9.json");

        assertSame(answer, CommitIdempotency.once(token, "commitAppend", () -> {
            commits.incrementAndGet();
            return answer;
        }));
        assertSame(answer, CommitIdempotency.once(token, "commitAppend", () -> {
            commits.incrementAndGet();
            return Collections.singletonMap("metadata-location", "s3://b/m/v10.json");
        }));
        assertEquals(1, commits.get());
    }

    /** Two genuinely different commits must not be confused for each other. */
    @Test
    public void differentTokensCommitIndependently() throws Exception {
        AtomicInteger commits = new AtomicInteger();

        Map<String, Object> one = CommitIdempotency.once("token-a", "commitAppend", () -> {
            commits.incrementAndGet();
            return Collections.singletonMap("metadata-location", "s3://b/m/a.json");
        });
        Map<String, Object> two = CommitIdempotency.once("token-b", "commitAppend", () -> {
            commits.incrementAndGet();
            return Collections.singletonMap("metadata-location", "s3://b/m/b.json");
        });

        assertEquals(2, commits.get());
        assertNotSame(one, two);
    }

    /**
     * A client that sends no token -- anything older than this change -- must
     * be left exactly as it was, including its ability to retry.
     */
    @Test
    public void withoutATokenEveryCallCommits() throws Exception {
        AtomicInteger commits = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            CommitIdempotency.once(null, "commitAppend", () -> {
                commits.incrementAndGet();
                return Collections.emptyMap();
            });
        }
        CommitIdempotency.once("", "commitAppend", () -> {
            commits.incrementAndGet();
            return Collections.emptyMap();
        });

        assertEquals(4, commits.get());
    }

    /**
     * A commit that failed may not have reached the catalog, so the token has
     * to be forgotten: the client asking again is how anyone finds out. Holding
     * the failure would turn a retryable error into a permanent one.
     */
    @Test
    public void aFailedCommitCanBeRetried() throws Exception {
        String token = "commit-that-failed";
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(IllegalStateException.class, () ->
                CommitIdempotency.once(token, "commitAppend", () -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("catalog unreachable");
                }));

        Map<String, Object> answer =
                Collections.singletonMap("metadata-location", "s3://b/m/v11.json");
        assertSame(answer, CommitIdempotency.once(token, "commitAppend", () -> {
            attempts.incrementAndGet();
            return answer;
        }));
        assertEquals(2, attempts.get());
    }
}
