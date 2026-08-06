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

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.exceptions.NoSuchTableException;
import com.google.common.base.Preconditions;
import cloud.elastic.dlagent.plugins.hudi.utilities.FilePathUtils;
import org.apache.iceberg.CatalogUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of IcebergCatalog for tables handled by Iceberg's Catalogs API.
 */
public class IcebergPolarisCatalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergPolarisCatalog.class);

    private RESTCatalog restCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;

    public IcebergPolarisCatalog(String warehouse, IcebergUtilities icebergUtilities,
                                 Configuration configuration, Map<String, String> gopherProperties) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.restCatalog = new RESTCatalog();

        String catalogName = icebergUtilities.extractWarehouseFromTableName(warehouse);
        String clientId = FilePathUtils.unescapeString(configuration.get("client_id","root"));
        String clientSecret = FilePathUtils.unescapeString(configuration.get("client_secret","secret"));
        String scope = FilePathUtils.unescapeString(configuration.get("scope","PRINCIPAL_ROLE:ALL"));
        String polarisServerUrl = FilePathUtils.unescapeString(configuration.get("polaris_server_url", "http://127.0.0.1:8181/api/catalog"));
        String realm = FilePathUtils.unescapeString(configuration.get("polaris_server_realm", "POLARIS"));
        String credential = clientId + ":" + clientSecret;

        // Polaris-specific REST keys are stamped last so an inbound gopher.*
        // entry with the same name cannot override them.
        Map<String, String> props = icebergUtilities.composeCatalogProperties(this.configuration);
        if (gopherProperties != null) {
            props.putAll(gopherProperties);
        }
        props.put(CatalogProperties.URI, polarisServerUrl);
        props.put(OAuth2Properties.CREDENTIAL, credential);
        props.put(OAuth2Properties.SCOPE, scope);
        props.put("header.Polaris-Realm", realm);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, catalogName);

        LOG.info("Polaris catalog properties: {}", props);

        CatalogUtil.configureHadoopConf(restCatalog, this.configuration);
        restCatalog.initialize("polaris", props);
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> tableProps) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(identifier.toString());
        // location is intentionally not forwarded: the Polaris REST catalog
        // assigns the table location itself.  The partition spec and table
        // properties must be forwarded, otherwise PARTITION BY tables are
        // silently created unpartitioned.
        return restCatalog.createTable(tableId, schema,
                spec != null ? spec : PartitionSpec.unpartitioned(),
                IcebergUtilities.stripInternalProperties(tableProps));
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        TableIdentifier tableId = icebergUtilities.getIcebergTableIdentifier(tableName);
        return loadTable(tableId, null, null);
    }

    @Override
    public Table loadTable(TableIdentifier tableId, String tableLocation,
                           Map<String, String> tableProps) throws Exception {
        Preconditions.checkState(tableId != null);
        final int MAX_ATTEMPTS = 5;
        final int SLEEP_MS = 500;
        int attempt = 0;
        while (attempt < MAX_ATTEMPTS) {
            try {
                Table table = restCatalog.loadTable(tableId);
                return table;
            } catch (NoSuchTableException e) {
                throw e;
            } catch (NullPointerException | UncheckedIOException e) {
                if (attempt == MAX_ATTEMPTS - 1) {
                    // Throw exception on last attempt.
                    throw e;
                }
                LOG.warn("Caught Exception during Iceberg table loading: {}: {}", tableId, e);
            }
            ++attempt;
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                // Ignored.
            }
        }
        // We shouldn't really get there, but to make the compiler happy:
        throw new Exception(
                String.format("Failed to load Iceberg table with id: %s", tableId));
    }

    @Override
    public boolean dropTable(String tableName, boolean purge) {
        throw new UnsupportedOperationException("Iceberg accessor does not support dropTable operation.");
    }

    @Override
    public void renameTable(String tableName, String newTableName) {
        throw new UnsupportedOperationException("Iceberg accessor does not support renameTable operation.");
    }

    @Override
    public boolean createNamespace(String catalogName, String namespaceName,
                                  Map<String, String> properties) throws Exception {
        restCatalog.createNamespace(
            org.apache.iceberg.catalog.Namespace.of(namespaceName),
            properties != null ? properties : new HashMap<String, String>()
        );
        return true;
    }
}

