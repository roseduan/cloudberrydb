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

import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * A task decorator that gets a copy of the MDC context map in the web thread
 * context, and then sets the context map to the async thread context.
 */
@Component
public class DlContextMdcLogEnhancerDecorator implements TaskDecorator {

    /**
     * {@inheritDoc}
     */
    @Override
    public Runnable decorate(Runnable runnable) {
        // Web thread context: (Grab the current thread MDC data)
        Map<String, String> contextMap = MDC.getCopyOfContextMap();
        return () -> {
            try {
                // @Async thread context: (Restore the Web thread context's MDC data)
                MDC.setContextMap(contextMap);
                runnable.run();
            } finally {
                MDC.clear();
            }
        };
    }
}
