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

package cloud.elastic.dlagent.plugins.iceberg;

import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.HashMap;
import java.util.Map;

/** Polaris specialization of the common Iceberg REST catalog. */
public class IcebergPolarisCatalog extends IcebergRestCatalog {

    public IcebergPolarisCatalog(String warehouse, IcebergUtilities icebergUtilities,
                                 Configuration configuration,
                                 Map<String, String> gopherProperties) {
        super("polaris", warehouse, icebergUtilities, configuration, gopherProperties,
                requireConfig(configuration, "polaris_server_url"));
    }

    @Override
    protected Map<String, String> authProperties() {
        String clientId = requireConfig(configuration, "client_id");
        String clientSecret = requireConfig(configuration, "client_secret");
        String scope = FilePathUtils.unescapeString(
                configuration.get("scope", "PRINCIPAL_ROLE:ALL"));
        String realm = FilePathUtils.unescapeString(
                configuration.get("polaris_server_realm", "POLARIS"));

        Map<String, String> props = new HashMap<>();
        props.put(OAuth2Properties.CREDENTIAL, clientId + ":" + clientSecret);
        props.put(OAuth2Properties.SCOPE, scope);
        props.put("header.Polaris-Realm", realm);
        return props;
    }
}
