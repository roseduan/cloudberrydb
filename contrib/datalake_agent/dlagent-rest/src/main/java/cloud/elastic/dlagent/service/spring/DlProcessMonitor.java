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

package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.service.ParentProcessWatchdog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Spring shell around {@link ParentProcessWatchdog}.
 *
 * <p>The watchdog logic lives in dlagent-core so the gRPC jar has it too: it
 * must keep working under both transports. What stays here is only what is
 * Spring: reading {@code parent.pid} from the environment and ticking once a
 * second. The rate, the scheduler and the {@code @PreDestroy} hook are the ones
 * this class always had, so the REST jar's behaviour is unchanged.
 */
@Component
public class DlProcessMonitor {

    @Value("${parent.pid}")
    String parentPid;

    private ParentProcessWatchdog watchdog;

    @PostConstruct
    void init() {
        watchdog = new ParentProcessWatchdog(parentPid);
    }

    @PreDestroy
    public void destroy() {
        if (watchdog != null) {
            watchdog.shutdown();
        }
    }

    @Scheduled(fixedRate = 1000)
    public void run() {
        if (watchdog != null) {
            watchdog.checkOnce();
        }
    }
}
