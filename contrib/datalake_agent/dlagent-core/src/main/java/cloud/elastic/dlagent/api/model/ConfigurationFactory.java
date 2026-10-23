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

package cloud.elastic.dlagent.api.model;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public interface ConfigurationFactory {
    /**
     * Configuration property that stores the server name
     */
    String DLAGENT_SERVER_NAME_PROPERTY = "dlagent.config.server.name";

    /**
     * Synthetic configuration property that stores the user so that is can be
     * used in config files for interpolation in other properties, for example
     * in JDBC when setting session authorization from a proxy user to the
     * end-user
     */
    String DLAGENT_SESSION_USER_PROPERTY = "dlagent.session.user";

    /**
     * Initializes a configuration object that applies server-specific configurations and
     * adds additional properties on top of it, if specified.
     *
     * @param configFiles name of the configuration
     * @param serverName name of the server
     * @param userName name of the user
     * @return configuration object
     */
    Configuration initConfiguration(String catalogType,
                                    String configFiles,
                                    String serverName,
                                    String userName,
                                    String location,
                                    Map<String, String> additionalProperties);

    void initIcebergConfigFormJson(RequestContext context);
}
