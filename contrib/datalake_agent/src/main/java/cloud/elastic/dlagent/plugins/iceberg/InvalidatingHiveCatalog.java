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

import cloud.elastic.dlagent.api.model.RequestContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

/**
 * Catalog-level proxy that invalidates the wrapper's hive cache whenever a
 * delegated catalog call surfaces a connection-class failure.
 *
 * <p>Why a proxy and not try/catch at every service callsite?  catalog use is
 * spread across {@code IcebergServiceImpl} (5 sites) and {@code
 * IcebergMetadataFetcher} (~22 sites including the SELECT / append / commit /
 * statistics paths).  Wrapping the catalog itself means every present and
 * future call site picks up invalidation automatically, with no risk of a new
 * caller forgetting to wire up the eviction.
 *
 * <p>Only hive catalogs are wrapped; the s3 / hadoop / polaris / builtin
 * branches in {@link IcebergCatalogWrapper#getIcebergCatalog} build a fresh
 * catalog on every request and so don't suffer from this bug.
 */
final class InvalidatingHiveCatalog implements IcebergCatalog {

    private final IcebergHiveCatalog delegate;
    private final IcebergCatalogWrapper wrapper;
    private final RequestContext context;

    InvalidatingHiveCatalog(IcebergHiveCatalog delegate,
                            IcebergCatalogWrapper wrapper,
                            RequestContext context) {
        this.delegate = delegate;
        this.wrapper = wrapper;
        this.context = context;
    }

    /**
     * Deliberately does NOT close the delegate: the hive catalog is owned by
     * the wrapper's cache, which closes it on eviction and expects it to stay
     * usable across requests. Closing it here would tear down the shared HMS
     * client pool underneath every other request holding the same entry.
     */
    @Override
    public void close() {
    }

    @Override
    public Table createTable(TableIdentifier identifier,
                             Schema schema,
                             PartitionSpec spec,
                             String location,
                             Map<String, String> properties) {
        try {
            return delegate.createTable(identifier, schema, spec, location, properties);
        } catch (RuntimeException e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public Table loadTable(String tableName) throws Exception {
        try {
            return delegate.loadTable(tableName);
        } catch (Exception e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public Table loadTable(TableIdentifier tableId,
                           String tableLocation,
                           Map<String, String> properties) throws Exception {
        try {
            return delegate.loadTable(tableId, tableLocation, properties);
        } catch (Exception e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public org.apache.iceberg.io.FileIO io() throws Exception {
        try {
            return delegate.io();
        } catch (Exception e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public boolean dropTable(String tableName, boolean purge) {
        try {
            return delegate.dropTable(tableName, purge);
        } catch (RuntimeException e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public void renameTable(String tableName, String newTableName) {
        try {
            delegate.renameTable(tableName, newTableName);
        } catch (RuntimeException e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    @Override
    public boolean createNamespace(String catalogName,
                                   String namespaceName,
                                   Map<String, String> properties) throws Exception {
        try {
            return delegate.createNamespace(catalogName, namespaceName, properties);
        } catch (Exception e) {
            maybeInvalidate(e);
            throw e;
        }
    }

    private void maybeInvalidate(Throwable e) {
        if (IcebergCatalogWrapper.isConnectionFailure(e)) {
            wrapper.invalidateHiveCatalog(context);
        }
    }
}
