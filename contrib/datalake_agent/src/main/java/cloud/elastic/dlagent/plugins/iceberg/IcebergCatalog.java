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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Interface for Iceberg catalogs. Only contains a minimal set of methods to make
 * it easy to add support for new Iceberg catalogs. Methods that can be implemented in a
 * catalog-agnostic way should be placed in IcebergUtil.
 */
public interface IcebergCatalog {
    /**
     * Creates an Iceberg table in this catalog.
     */
    Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) throws Exception;

    Table loadTable(String tableName) throws Exception;

    /**
     * The catalog's FileIO, obtained WITHOUT loading a table (and therefore without the
     * metadata.json GET that a table load implies). Only implemented where a catalog-wide
     * FileIO exists and a caller needs raw storage access -- see the loadMetadataJson
     * endpoint, which reads one known object and needs no table state.
     */
    default org.apache.iceberg.io.FileIO io() {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not expose a catalog-wide FileIO");
    }

    /**
     * Loads a native Iceberg table based on 'tableId' or 'tableLocation'.
     * @param tableId is the Iceberg table identifier to load the table via the catalog
     *     interface, e.g. HadoopCatalog.
     * @param tableLocation is the filesystem path to load the table via the HadoopTables
     *     interface.
     * @param properties provides information for table loading when Iceberg Catalogs
     *     is being used.
     */
    Table loadTable(TableIdentifier tableId, String tableLocation,
                    Map<String, String> properties) throws Exception;

    /**
     * Drops the table from this catalog.
     * If purge is true, delete all data and metadata files in the table.
     * Return true if the table was dropped, false if the table did not exist
     */
    boolean dropTable(String tableName, boolean purge);

    /**
     * Renames Iceberg table.
     * For HadoopTables, Iceberg does not supported 'renameTable' method
     * For HadoopCatalog, Iceberg implement 'renameTable' method with Exception threw
     */
    void renameTable(String tableName, String newTableName);

    /**
     * Creates a namespace in the specified catalog.
     * @param catalogName the catalog name
     * @param namespaceName the namespace name
     * @param properties namespace properties
     * @return true if namespace was created successfully
     */
    boolean createNamespace(String catalogName, String namespaceName,
                           Map<String, String> properties) throws Exception;
}

