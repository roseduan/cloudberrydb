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

package cloud.elastic.dlagent.api.model.iceberg;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Catalog connection info parsed from the Iceberg request body.
 *
 * <p>Sourced exclusively from one of:
 * <ul>
 *   <li>SQL OPTIONS via {@code IcebergCatalogConfig.*} when {@code server_name} is absent</li>
 *   <li>{@code gphive.conf[server_name]} when {@code server_name} is given</li>
 * </ul>
 *
 * <p>The two sources are mutually exclusive: when {@code server_name} is present the
 * SQL OPTIONS fields are ignored. Kerberos fields here describe the catalog service
 * (e.g. Hive Metastore) authentication only; HDFS NameNode Kerberos lives on
 * {@link VolumeInfo}.
 */
@Getter
@Setter
@NoArgsConstructor
public class CatalogInfo {

    /** Catalog type: hive / hadoop / s3 / polaris. */
    private String serverType;

    /** When set, CatalogInfo is loaded from {@code gphive.conf[serverName]}; otherwise from SQL OPTIONS. */
    private String serverName;

    private String hiveMetastoreUri;
    private String username;

    /** simple / kerberos. */
    private String authMethod;

    /** Hive Metastore service principal (not HDFS NameNode). */
    private String krbServicePrincipal;
    private String krbClientPrincipal;
    private String krbClientKeytab;

    private String catalogName;
    private Boolean enableMetadataCache;
    private Integer metadataCacheTtl;
    private Boolean autoRefreshMetadata;
    private String warehouseLocationPrefix;

    /** Polaris-specific fields. */
    private String polarisServerUrl;
    /** Realm sent as the Polaris-Realm header; agent defaults to "POLARIS" when unset (issue #841). */
    private String polarisServerRealm;
    private String clientId;
    private String clientSecret;
    private String scope;

    /** Any extra catalog-related keys (pass-through for hadoop rpc protection etc.). */
    private Map<String, String> extraProperties = new HashMap<>();
}
