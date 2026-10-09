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

import cloud.elastic.dlagent.api.utilities.Utilities;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.List;

import cloud.elastic.dlagent.plugins.iceberg.utilities.IcebergUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of IcebergCatalog for tables handled by Iceberg's Catalogs API.
 */
public class IcebergS3Catalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergS3Catalog.class);

    private HadoopCatalog hadoopCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;

    public IcebergS3Catalog(String catalogLocation, IcebergUtilities icebergUtilities, Configuration configuration, Map<String, String> gopherProperties) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.hadoopCatalog = new HadoopCatalog();

        // Hadoop's S3AFileSystem stages object uploads through hadoop.tmp.dir.
        // IcebergBuildInCatalog / IcebergHiveCatalog set this same key.
        if (configuration.get("hadoop.tmp.dir") == null) {
            configuration.set("hadoop.tmp.dir",
                "/tmp/hadoop-" + System.getProperty("user.name"));
        }

        Map<String, String> props = icebergUtilities.composeCatalogProperties(this.configuration);

        String fsPrefix = configuration.get("fs.prefix");
        if (fsPrefix == null) {
            LOG.warn("fs.prefix not set; falling back to empty prefix");
            fsPrefix = "";
        }
        String warehouseLocation = String.format("%s/%s", configuration.get("fs.defaultFS"), fsPrefix);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        LOG.info("warehouse location of iceberg hadoop-table {}", warehouseLocation);

        if (gopherProperties != null) {
            for (Map.Entry<String, String> entry : gopherProperties.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }

        hadoopCatalog.setConf(this.configuration);
        hadoopCatalog.initialize("", props);
    }

    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> tableProps) throws Exception {
        // Iceberg's path-based HadoopCatalog rejects any non-null custom
        // location (it enforces <warehouse>/<ns>/<table>). The volume URL
        // forwarded from IcebergRestController.createTable would trip that
        // check; pass null and let iceberg compute the standard layout from
        // CatalogProperties.WAREHOUSE_LOCATION (set in initialize).
        return hadoopCatalog.createTable(identifier, schema, spec, null,
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
        return hadoopCatalog.loadTable(tableId);
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
        throw new UnsupportedOperationException("S3 catalog does not support createNamespace operation.");
    }

    /**
     * Releases the FileIO iceberg's HadoopCatalog holds -- under gopher a
     * GopherFileIO with a native connection.  This catalog is built fresh per
     * request, so the interface's no-op default leaked one per write.
     */
    @Override
    public void close() {
        HadoopCatalog catalog = hadoopCatalog;

        hadoopCatalog = null;
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                LOG.warn("Failed to close S3 iceberg catalog", e);
            }
        }
    }
}

