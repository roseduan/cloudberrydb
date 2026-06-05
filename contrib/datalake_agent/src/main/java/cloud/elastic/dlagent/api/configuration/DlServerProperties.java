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

package cloud.elastic.dlagent.api.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for dlagent.
 */
@ConfigurationProperties(prefix = DlServerProperties.PROPERTY_PREFIX)
public class DlServerProperties {
    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "dlagent";

    /**
     * Customizable settings for tomcat through dlagent
     */
    @Getter
    @Setter
    private Tomcat tomcat = new Tomcat();

    /**
     * Configurable task execution properties for async tasks (i.e Bridge Read)
     */
    @Getter
    @Setter
    private TaskExecutionProperties task = new TaskExecutionProperties();

    /**
     * Customizable settings for iceberg plugins through dlagent
     */
    @Getter
    @Setter
    private Iceberg iceberg = new Iceberg();

    @Getter
    @Setter
    public static class Tomcat {

        /**
         * Maximum number of headers allowed in the request
         */
        private int maxHeaderCount = 30000;

        /**
         * Whether upload requests will use the same read timeout as connectionTimeout
         */
        private boolean disableUploadTimeout = true; // default Tomcat setting

        /**
         * Timeout for reading data from upload requests, if disableUploadTimeout is set to false.
         */
        private Duration connectionUploadTimeout = Duration.ofMinutes(5); // 5 min is default Tomcat setting

    }

    @Getter
    @Setter
    public static class Iceberg {

        /**
         * Idle expiration (expireAfterAccess) for the cached hive catalogs in
         * IcebergCatalogWrapper.  An entry that has not been read for this
         * long is evicted and its underlying HMS connections are closed.
         */
        private Duration hiveCatalogCacheTtl = Duration.ofMinutes(5);

    }
}
