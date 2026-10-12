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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes a commit safe to send twice (issue #442).
 *
 * <p>The client gives a commit five minutes and then resends the whole
 * request, up to three times. Before this, a commit that ran long was
 * indistinguishable from a new one, so the slow first attempt and its resend
 * could both reach the catalog and leave the table with two snapshots holding
 * the same rows. Every commit request now carries a token generated once per
 * attempt; the resend carries the same one, because it resends the same bytes.
 *
 * <p>So the second arrival does not commit. It waits on the first and returns
 * the first's answer -- the same metadata location, which is what the client
 * writes into its catalog. Waiting rather than answering "in progress" is the
 * point: the client has nothing useful to do with a "try later", and the
 * result is usually seconds away now that the commit no longer reads every
 * file it is committing.
 *
 * <p>Two things this deliberately does not do. It does not dedupe across
 * agent processes -- a restart loses the map, and the snapshot summary carries
 * the same token as a coarser backstop. And it does not suppress a retry after
 * a failed commit: a commit that threw did not necessarily reach the catalog,
 * and the client asking again is how it finds out.
 */
public final class CommitIdempotency {

    private static final Logger LOG = LoggerFactory.getLogger(CommitIdempotency.class);

    /**
     * How long a waiter blocks for the first attempt's answer.
     *
     * <p>Just under the client's own five-minute ceiling: waiting longer would
     * only produce an answer nobody is still listening for, and waiting much
     * less would send the waiter away empty while the commit it needs is still
     * running. A waiter that does give up fails its request rather than
     * committing -- a failed statement is recoverable, a duplicated snapshot
     * is not.
     */
    private static final long WAIT_SECONDS = 290;

    /**
     * How long a finished commit stays known. It has to outlast the client's
     * whole retry budget -- three attempts of five minutes -- so a resend that
     * arrives after the first attempt already finished still finds it.
     */
    private static final long RETAIN_MILLIS = TimeUnit.MINUTES.toMillis(20);

    /**
     * A ceiling on how many tokens are remembered, so a client that somehow
     * produces them without bound cannot grow the map indefinitely.
     */
    private static final int MAX_ENTRIES = 4096;

    private static final ConcurrentHashMap<String, Entry> INFLIGHT =
            new ConcurrentHashMap<>();

    private static final class Entry {
        final CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
        volatile long finishedAt;
    }

    /** What a commit endpoint does once the request has been understood. */
    @FunctionalInterface
    public interface Commit {
        Map<String, Object> run() throws Exception;
    }

    private CommitIdempotency() {
    }

    /**
     * Run a commit, or return the answer an identical one already produced.
     *
     * @param token the request's commit token, or null from a client that
     *              sends none -- in which case the commit simply runs
     * @param what  a short name for the operation, for logging
     * @param commit the commit itself
     */
    public static Map<String, Object> once(String token, String what, Commit commit)
            throws Exception {
        if (token == null || token.isEmpty()) {
            return commit.run();
        }

        Entry mine = new Entry();
        Entry existing = INFLIGHT.putIfAbsent(token, mine);

        if (existing != null) {
            return await(existing, token, what);
        }

        expireOldEntries();
        try {
            Map<String, Object> result = commit.run();

            mine.result.complete(result);
            mine.finishedAt = System.currentTimeMillis();
            return result;
        } catch (Exception e) {
            /*
             * Forget the token: this commit may not have reached the catalog,
             * and the client's next attempt has to be allowed to find out.
             */
            INFLIGHT.remove(token, mine);
            mine.result.completeExceptionally(e);
            throw e;
        }
    }

    private static Map<String, Object> await(Entry existing, String token, String what)
            throws Exception {
        LOG.info("{}: token {} already committing or committed, "
                 + "returning that result instead of committing again",
                 what, token);
        try {
            return existing.result.get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException(String.format(
                    "%s: commit %s is still running after %d seconds; "
                    + "refusing to commit it a second time",
                    what, token, WAIT_SECONDS), e);
        } catch (ExecutionException e) {
            /*
             * The first attempt failed.  Surface its failure rather than
             * committing here: the client's retry logic owns that decision,
             * and it has already been told this token is not a fresh attempt.
             */
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    /**
     * Drop tokens whose retry window has passed, and if the map is still at
     * its ceiling, the oldest finished ones. Unfinished entries are never
     * dropped -- something is waiting on them.
     */
    private static void expireOldEntries() {
        long cutoff = System.currentTimeMillis() - RETAIN_MILLIS;
        Iterator<Map.Entry<String, Entry>> it = INFLIGHT.entrySet().iterator();

        while (it.hasNext()) {
            Entry entry = it.next().getValue();

            if (entry.finishedAt != 0 && entry.finishedAt < cutoff) {
                it.remove();
            }
        }

        while (INFLIGHT.size() > MAX_ENTRIES) {
            String oldest = null;
            long oldestAt = Long.MAX_VALUE;

            for (Map.Entry<String, Entry> candidate : INFLIGHT.entrySet()) {
                long finishedAt = candidate.getValue().finishedAt;

                if (finishedAt != 0 && finishedAt < oldestAt) {
                    oldestAt = finishedAt;
                    oldest = candidate.getKey();
                }
            }
            if (oldest == null) {
                break;
            }
            INFLIGHT.remove(oldest);
        }
    }

    /** Test seam: forget every token. */
    static void reset() {
        INFLIGHT.clear();
    }
}
