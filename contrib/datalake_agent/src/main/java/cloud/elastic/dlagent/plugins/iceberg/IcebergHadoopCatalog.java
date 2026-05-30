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
import org.apache.iceberg.exceptions.NoSuchTableException;
import com.google.common.base.Preconditions;
import org.apache.iceberg.avro.Avro;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of IcebergCatalog for tables handled by Iceberg's Catalogs API.
 */
public class IcebergHadoopCatalog implements IcebergCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergHadoopCatalog.class);

    private HadoopCatalog hadoopCatalog;
    private IcebergUtilities icebergUtilities;
    private Configuration configuration;
    private boolean isUseGopherClient = true;

    public void createDefaultHadoopCatalog(String catalogLocation, IcebergUtilities icebergUtilities, Configuration configuration) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.hadoopCatalog = new HadoopCatalog();

        // Hadoop's S3AFileSystem stages object uploads through hadoop.tmp.dir
        // and HDFS clients use it for local intermediate buffers.
        if (configuration.get("hadoop.tmp.dir") == null) {
            configuration.set("hadoop.tmp.dir",
                "/tmp/hadoop-" + System.getProperty("user.name"));
        }

        Map<String, String> props = icebergUtilities.composeCatalogProperties(this.configuration);

        // Always pass the original URL (hdfs://namenode:port/..., s3a://bucket/...,
        // oss://bucket/...) into iceberg's HadoopCatalog.  The previous code rewrote
        // it to gopher://hdfs/... or gopher://<bucket>/... in the gopher-enabled
        // branch on the theory that hadoop would route gopher:// to GopherFileSystem
        // -- but the rewrite is unnecessary (IcebergUtilities.setupGopherConfiguration
        // already registers fs.hdfs.impl / fs.s3a.impl / fs.oss.impl as
        // GopherFileSystem, so hadoop dispatches the native scheme to it) AND
        // destructive (it loses the original scheme, host and port).  Iceberg writes
        // data file URLs into metadata.json relative to WAREHOUSE_LOCATION; keeping
        // the original scheme means readers/writers see a faithful URL all the way
        // through and GopherURI / native gopher can parse host+bucket+key from it
        // directly.
        String warehouseLocation = buildStandardURI(configuration.get("fs.defaultFS"), catalogLocation);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        LOG.info("warehouse location of iceberg hadoop-table {}", warehouseLocation);

        hadoopCatalog.setConf(this.configuration);
        hadoopCatalog.initialize("", props);
    }

    /**
     * Builds a standard URI by joining fs.defaultFS with the relative catalog
     * location, preserving the native scheme (hdfs://, s3a://, oss://...).
     * GopherFileSystem is wired in via fs.<scheme>.impl by
     * IcebergUtilities.setupGopherConfiguration, so we do not need to (and
     * should not) rewrite to a synthetic gopher:// scheme here.
     */
    private String buildStandardURI(String defaultFS, String catalogLocation) {
        if (defaultFS != null && !defaultFS.isEmpty()) {
            if (catalogLocation.startsWith("/")) {
                return defaultFS + catalogLocation;
            }
            return defaultFS + "/" + catalogLocation;
        }
        return catalogLocation;
    }

    public void createGopherHadoopCatalog(String catalogLocation,
                                          IcebergUtilities icebergUtilities,
                                          Configuration configuration,
                                          Map<String, String> gopherProperties) {
        this.icebergUtilities = icebergUtilities;
        this.configuration = configuration;
        this.hadoopCatalog = new HadoopCatalog();

        // See createDefaultHadoopCatalog for rationale.
        if (configuration.get("hadoop.tmp.dir") == null) {
            configuration.set("hadoop.tmp.dir",
                "/tmp/hadoop-" + System.getProperty("user.name"));
        }

        Map<String, String> props = icebergUtilities.composeCatalogProperties(this.configuration);

        // See createDefaultHadoopCatalog: keep the original native scheme; do not
        // rewrite to gopher://<...>.
        String warehouseLocation = buildStandardURI(configuration.get("fs.defaultFS"), catalogLocation);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);


        if (gopherProperties != null) {
            for (Map.Entry<String, String> entry : gopherProperties.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
        // Register GopherFileIO (matches IcebergUtilities.composeGopherCatalogProperties).
        // containsKey guard avoids overriding an explicit iceberg.io-impl=... pin.
        if (!props.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            props.put(CatalogProperties.FILE_IO_IMPL,
                      "org.cbdb.iceberg.gopher.client.GopherFileIO");
        }

        LOG.debug("iceberg hadoop catalog gopher properties: {}", gopherProperties);
        LOG.info("warehouse location of iceberg hadoop-table {}", warehouseLocation);

        hadoopCatalog.setConf(this.configuration);
        hadoopCatalog.initialize("", props);
    }

    public IcebergHadoopCatalog(String catalogLocation,
                                IcebergUtilities icebergUtilities,
                                Configuration configuration,
                                Map<String, String> gopherProperties) {
        if (isUseGopherClient) {
            createGopherHadoopCatalog(catalogLocation, icebergUtilities, configuration, gopherProperties);
        } else {
            createDefaultHadoopCatalog(catalogLocation, icebergUtilities, configuration);
        }
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
        // CatalogProperties.WAREHOUSE_LOCATION (set in initialize via the
        // fs.defaultFS + catalogLocation contract).
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
        final int MAX_ATTEMPTS = 5;
        final int SLEEP_MS = 500;
        int attempt = 0;
        while (attempt < MAX_ATTEMPTS) {
            try {
                return hadoopCatalog.loadTable(tableId);
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
        throw new UnsupportedOperationException("Hadoop catalog does not support createNamespace operation.");
    }
}

