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

package cloud.elastic.dlagent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Exits this JVM once {@code datalake_proxy} -- the background worker that
 * started it -- is gone.
 *
 * <p>{@code datalake_proxy.c} passes its own pid as {@code --parent.pid};
 * this polls {@code /proc/<pid>} and {@code /proc/<pid>/cmdline} and calls
 * {@code System.exit(0)} as soon as either says the parent is no longer the
 * datalake proxy. Without it a dead proxy leaves an orphan JVM behind, and the
 * restarted proxy starts another one, so the JVMs pile up -- which is why
 * this watchdog has to keep working on <em>both</em> jars.
 *
 * <p>WHY THIS IS IN CORE. As a Spring {@code @Component} with
 * {@code @Scheduled} and {@code @Value} in dlagent-rest, the gRPC jar would
 * have no watchdog at all. Only the scheduling is transport-specific, so the
 * logic lives here and each jar supplies its own ticker.
 *
 * <p>The polling belongs to the caller, not to this class: dlagent-rest keeps
 * its {@code @Scheduled(fixedRate = 1000)} shell so the REST jar's timing and
 * scheduler are untouched, and dlagent-grpc drives
 * {@link #checkOnce()} from its own ScheduledExecutorService.
 */
public class ParentProcessWatchdog {

    private static final Logger log = LoggerFactory.getLogger(ParentProcessWatchdog.class);

    /**
     * Set while the process is shutting down, so a tick that races with
     * shutdown does not call {@code System.exit(0)} and turn an orderly stop
     * into a watchdog kill.
     *
     * <p>DELIBERATE CHANGE, the only one in this move: the original field was
     * a non-volatile {@code static boolean} written by the shutdown thread and
     * read by the scheduler thread, i.e. a data race in which the reader was
     * not guaranteed to observe the flag at all -- exactly the case the flag
     * exists to handle. It is now a volatile instance field. This can only make
     * the guard more likely to be honoured, never less; the behaviour is
     * otherwise identical because there is one instance per process.
     */
    private volatile boolean shuttingDown = false;

    private final String parentPid;

    public ParentProcessWatchdog(String parentPid) {
        this.parentPid = parentPid;
    }

    /** Stop exiting on subsequent ticks; call from the shutdown hook. */
    public void shutdown() {
        shuttingDown = true;
    }

    /**
     * One poll. Calls {@code System.exit(0)} if the parent is gone -- by
     * design, and the reason this method never returns a value: there is no
     * caller left to report to.
     */
    public void checkOnce() {
        if (shuttingDown) {
            return;
        }

        Path processDirectory = Paths.get("/proc/" + parentPid);
        if (!Files.exists(processDirectory)) {
            log.info("exiting dlproxy due to parent process directory not exist");
            System.exit(0);
        }

        String filePath = "/proc/" + parentPid + "/cmdline";
        File file = new File(filePath);
        if (!file.exists()) {
            log.info("exiting dlproxy due to cmdline file of parent process not exist");
            System.exit(0);
        }

        boolean found = false;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String cmdLine = br.readLine();
            if (cmdLine.contains("datalake proxy")) {
                found = true;
            }
        } catch (Exception e) {
            log.error("failed to read command line file.", e);
        }

        if (!found) {
            log.info("exiting dlproxy due to command line file is corrupted");
            System.exit(0);
        }
    }
}
