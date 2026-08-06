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

import cloud.elastic.dlagent.api.security.SecureLogin;
import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.buildin.DlIcebergBuildInCatalog;
import org.apache.iceberg.util.LocationUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;

/**
 * Implementation of IcebergCatalog for tables stored in BuildInCatalog.
 */
public class IcebergBuildInCatalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergBuildInCatalog.class);

    private DlIcebergBuildInCatalog buildInCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;
    private String catalogLocation;

    public IcebergBuildInCatalog(String catalogLocation,
                              IcebergUtilities icebergUtilities,
                              Configuration configuration,
                              SecureLogin secureLogin,
                              String serverName,
                              String configFile,
                              Map<String, String> gopherProperties,
                              Map<String, String> buildInCatalogProperties) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.catalogLocation = catalogLocation;

        createDefaultBuildInCatalog(catalogLocation, icebergUtilities, configuration,
                                    secureLogin, serverName, configFile, gopherProperties, buildInCatalogProperties);
    }

    public void createDefaultBuildInCatalog(String catalogLocation,
                                         IcebergUtilities icebergUtilities,
                                         Configuration configuration,
                                         SecureLogin secureLogin,
                                         String serverName,
                                         String configFile,
                                         Map<String, String> gopherProperties,
                                         Map<String, String> buildInCatalogProperties) {
        HiveConf conf = new HiveConf(configuration, IcebergBuildInCatalog.class);

        // Set hadoop temp directory
        conf.set("hadoop.tmp.dir", "/tmp/hadoop-" + System.getProperty("user.name"));
        conf.setBoolean(ConfigProperties.ENGINE_HIVE_ENABLED, true);

        buildInCatalog = new DlIcebergBuildInCatalog(secureLogin, serverName, configFile, buildInCatalogProperties);
        buildInCatalog.setConf(conf);

        Map<String, String> properties = icebergUtilities.composeCatalogProperties(this.configuration);
        if (catalogLocation != null) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogLocation);
        }
        if (gopherProperties != null) {
            for (Map.Entry<String, String> entry : gopherProperties.entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        properties.forEach((key, value) -> LOG.debug(" createDefaultBuildInCatalog properties {}: {}", key, value));

        buildInCatalog.initialize("IcebergBuildInCatalog", properties);
    }

    private String buildTableLocation(String warehouseLocation, String namespace, String tableName) {
        String warehouse = LocationUtil.stripTrailingSlash(warehouseLocation);
        if (warehouse.isEmpty()) {
            return String.format("%s/%s", namespace, tableName);
        } else {
            return String.format("%s/%s/%s", warehouse, namespace, tableName);
        }
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) {
        if (catalogLocation != null && !catalogLocation.isEmpty()) {
            if (location == null || location.isEmpty()) {
                if (identifier.namespace().length() == 0) {
                    throw new IllegalArgumentException(
                        "Table identifier must include a namespace: " + identifier);
                }
                String namespace = identifier.namespace().level(0);
                String tableName = identifier.name();
                location = buildTableLocation(catalogLocation, namespace, tableName);
            }
            return buildInCatalog.createTable(identifier, schema, spec, location,
                    IcebergUtilities.stripInternalProperties(properties));
        } else {
            /* No catalog location: let the catalog assign one, but still
             * forward the partition spec and properties. */
            return buildInCatalog.createTable(identifier, schema,
                    spec != null ? spec : PartitionSpec.unpartitioned(),
                    IcebergUtilities.stripInternalProperties(properties));
        }
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(tableName);
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> properties) throws Exception {
        Preconditions.checkState(tableId != null);
        return buildInCatalog.loadTable(tableId);
    }

    @Override
    public boolean dropTable(String tableName, boolean purge) {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(tableName);
        return buildInCatalog.dropTable(tableId, purge);
    }

    @Override
    public void renameTable(String tableName, String newTableName) {
        throw new UnsupportedOperationException("Iceberg accessor does not support renameTable operation.");
    }

    @Override
    public boolean createNamespace(String catalogName, String namespaceName, 
                                  Map<String, String> properties) throws Exception {
        throw new UnsupportedOperationException("BuildIn catalog does not support createNamespace operation.");
    }
}