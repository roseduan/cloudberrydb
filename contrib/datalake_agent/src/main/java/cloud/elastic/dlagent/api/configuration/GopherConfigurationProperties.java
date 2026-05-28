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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for Gopher client.
 * <p>
 * These properties are loaded from application.properties/application.yml
 * with the prefix "gopher".
 * <p>
 */
@Component
@ConfigurationProperties(prefix = GopherConfigurationProperties.PROPERTY_PREFIX)
@Getter
@Setter
public class GopherConfigurationProperties {

    /**
     * The property prefix for all Gopher configuration properties.
     */
    public static final String PROPERTY_PREFIX = "gopher";

    /**
     * Whether Gopher mode is enabled.
     * <p>
     * When set to true, GopherFileIO and GopherFileSystem will be used.
     * When set to false or not set, original HadoopFileIO will be used.
     */
    private Boolean enabled = false;

    /**
     * Path to gopher_worker executable.
     */
    private String workerPath;

    /**
     * Path to gopher_connect socket or executable.
     */
    private String connectPath;

    /**
     * Path to gopher_connect_plasma executable.
     */
    private String connectPlasmaPath;

    /**
     * Cache strategy: GOPHER_CACHE or GOPHER_NOT_CACHE.
     */
    private String cacheStrategy = "GOPHER_CACHE";

    /**
     * Gopher log level: fatal, error, warn, info, debug1, debug2, debug3.
     */
    private String logLevel = "info";

    /**
     * liboss2 log severity: fatal, error, warn, info, debug1, debug2, debug3.
     */
    private String liboss2LogLevel = "info";

    /**
     * Block size for file operations in bytes.
     */
    private Long blockSize = 134217728L; // 128MB

    /**
     * Buffer size for I/O operations in bytes.
     */
    private Long bufferSize = 1048576L; // 1MB

    /**
     * Converts this configuration to a map with "gopher." prefixed keys.
     * <p>
     * This map can be directly used to populate Hadoop Configuration or
     * passed to GopherFileIO.
     *
     * @return Map of Gopher configuration properties
     */
    public Map<String, String> toGopherPropertiesMap() {
        Map<String, String> props = new HashMap<>();

        if (enabled != null) {
            props.put("gopher.enabled", String.valueOf(enabled));
        }
        if (workerPath != null) {
            props.put("gopher.worker_path", workerPath);
        }
        if (connectPath != null) {
            props.put("gopher.connect_path", connectPath);
        }
        if (connectPlasmaPath != null) {
            props.put("gopher.connect_plasma_path", connectPlasmaPath);
        }
        if (cacheStrategy != null) {
            props.put("gopher.cache_strategy", cacheStrategy);
        }
        if (logLevel != null) {
            props.put("gopher.log_level", logLevel);
        }
        if (liboss2LogLevel != null) {
            props.put("gopher.liboss2_log_level", liboss2LogLevel);
        }
        if (blockSize != null) {
            props.put("gopher.block_size", String.valueOf(blockSize));
        }
        if (bufferSize != null) {
            props.put("gopher.buffer_size", String.valueOf(bufferSize));
        }
        return props;
    }
}
